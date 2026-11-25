"""The Import page's writer: the latest snapshot of each chat of an uploaded tgstats.db into tg.snapshot and
tg.snapshot_member, exactly as the snapshot contract (design.md 5) writes them, so the reconciler applies them.

The whole file is checked first (tgaud.web.tgstats_file), outside the web connection; an invalid file writes
nothing. Then each admissible chat gets one transaction on the web connection, bounded in total by CHAT_SECONDS
(the fence's and the chat lock's waits included: lock_timeout and statement_timeout are set from the time left
before every statement):

    generation fence, tgaud.history.lock_chats(chat)      (migrations, forget-user and ingest take the same lock)
    the collection's uuid anywhere in tg.snapshot         -> skipped: already imported
    the chat a retired source (tg.chat_alias.old_chat_id) -> refused: migrated
    the chat not tracked                                  -> skipped: not tracked
    a snapshot of the chat with started_at >= this one's  -> skipped: not newer (the reconciler applies a chat's
                                                             snapshots in started_at order: never insert behind)
    INSERT tg.snapshot unfinished; COPY its members; drop rows of anyone tg.forget_user erased (whatever their
    seen_at: the collector's clock cannot prove a page came after the erasure) and clear erased inviters;
    finish it with fetched = the rows that survived

The checks run inside the locked transaction, so racing uploads, a migration or a forget-user cannot slip between
check and write. Failures are per chat: an earlier chat's commit stands, a later chat still runs. A commit whose
acknowledgement was lost is reported as unknown; uploading the same file again settles it by the uuid.
"""

from __future__ import annotations

import datetime as dt
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import psycopg

from tgaud import history
from tgaud.web import tgstats_file
from tgaud.web.app import OutOfTime, WebDb, db_error_line
from tgaud.web.tgstats_file import BadValue, Chat, FileError

log = logging.getLogger(__name__)

UTC = dt.timezone.utc
CHAT_SECONDS = 30.0  # one chat's transaction, waits included: well under WebDb's 75 s lock wait
MAX_MEMBERS = 200_000  # members of one snapshot: about 10 s of transaction (tests/tgaud/test_import_scale.py)

MEMBER_COLUMNS = ("snapshot_id", "user_id", "seen_at", "joined_date", "inviter_id", "via_request",
                  "subscription_until", "is_bot", "is_deleted", "is_premium", "username", "first_name", "last_name")
COPY_MEMBERS = f"COPY tg.snapshot_member ({', '.join(MEMBER_COLUMNS)}) FROM STDIN"

IMPORTED, SKIPPED, REFUSED, FAILED, UNKNOWN = "imported", "skipped", "refused", "failed", "unknown"
AGAIN = "upload the same file again"


@dataclass
class Outcome:
    """What happened to one chat of the file (one row of the result page)."""

    chat_id: int
    title: str | None
    status: str  # imported | skipped | refused | failed | unknown
    reason: str
    listed: int = 0  # member rows in the file's snapshot
    written: int | None = None  # rows that survived insertion (the snapshot's fetched)
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None
    snapshot_id: int | None = None
    snapshots: int = 1  # the file's snapshots of the chat (only the latest is imported)


@dataclass
class Report:
    outcomes: list[Outcome]
    current: bool  # the file has the new format's columns

    def count(self, status: str) -> int:
        return sum(o.status == status for o in self.outcomes)


class CommitUnknown(Exception):
    """The commit was sent but its result never came back: the snapshot may or may not be there."""


def run(database: WebDb, path: str | Path, *, store_names: bool, now: dt.datetime | None = None,
        ceiling: int | None = None, seconds: float | None = None) -> Report:
    """Check the uploaded file, then import its chats one transaction each; FileError when the whole file is
    refused (nothing written)."""
    now = now or dt.datetime.now(UTC)
    ceiling = MAX_MEMBERS if ceiling is None else ceiling
    with tgstats_file.Reader(path) as reader:
        checked = tgstats_file.check(reader, now=now, ceiling=ceiling)
        outcomes: dict[int, Outcome] = {c.chat_id: Outcome(c.chat_id, c.title, "", "", c.listed,
                                                            started_at=c.started_at, finished_at=c.finished_at,
                                                            snapshots=c.snapshots) for c in checked.chats}
        known = _known_chats(database, [c.chat_id for c in checked.chats])
        for chat in checked.chats:
            out = outcomes[chat.chat_id]
            title, tracked = known.get(chat.chat_id, (None, False))
            out.title = title or out.title
            if chat.refusal is not None:
                # nothing to write: an untracked chat says so first (the owner may never have meant to track it)
                if not tracked:
                    out.status, out.reason = SKIPPED, _NOT_TRACKED
                else:
                    out.status, out.reason = REFUSED, _refusal_text(chat.refusal)
                continue
            _import_chat(database, reader, chat, out, store_names=store_names,
                         seconds=CHAT_SECONDS if seconds is None else seconds)
    return Report([outcomes[c.chat_id] for c in checked.chats], checked.current)


_NOT_TRACKED = "not tracked: tgaud records no history for this chat (the bot is not an admin there)"


def _refusal_text(refusal: str) -> str:
    if refusal == "old format":
        return ("old format: this snapshot was collected by an older tgstats, which records only when the "
                "collection ended, so its sightings could contradict leaves tgaud saw meanwhile. Collect the chat "
                "again with this version (tgstats collect) and upload the new file")
    return refusal


def _known_chats(database: WebDb, chat_ids: list[int]) -> dict[int, tuple[str | None, bool]]:
    with database.read() as conn:
        rows = conn.execute("SELECT chat_id, title, tracked FROM tg.chat WHERE chat_id = ANY(%s::bigint[])",
                            (chat_ids,)).fetchall()
    return {int(r[0]): (r[1], bool(r[2])) for r in rows}


def _import_chat(database: WebDb, reader: tgstats_file.Reader, chat: Chat, out: Outcome, *, store_names: bool,
                 seconds: float) -> None:
    try:
        _write(database, reader, chat, out, store_names=store_names, seconds=seconds)
    except CommitUnknown as e:
        out.status, out.written, out.snapshot_id = UNKNOWN, None, None
        out.reason = (f"unknown: the connection was lost while committing ({db_error_line(e.__cause__ or e)}); "
                      f"{AGAIN}: if this chat was imported, it is skipped as already imported")
        log.warning("web import: chat %s: commit outcome unknown", chat.chat_id)
    except (OutOfTime, FileError, BadValue) as e:  # the file is read again inside the transaction's time
        out.status, out.written, out.snapshot_id = FAILED, None, None
        out.reason = f"failed: {e}; nothing was written for this chat; {AGAIN}"
        log.warning("web import: chat %s: %s", chat.chat_id, e)
    except psycopg.Error as e:
        out.status, out.written, out.snapshot_id = FAILED, None, None
        line = db_error_line(e)
        out.reason = f"failed: database error: {line}; nothing was written for this chat; {AGAIN}"
        log.warning("web import: chat %s: database error: %s", chat.chat_id, line)


def _write(database: WebDb, reader: tgstats_file.Reader, chat: Chat, out: Outcome, *, store_names: bool,
           seconds: float) -> None:
    s = chat.snapshot
    with database.write_within(seconds) as (conn, clock):
        reader.deadline = clock.end  # reading the file again counts against the same time
        clock.arm()
        history.lock_chats(conn, [chat.chat_id])
        clock.arm()
        dup = conn.execute("SELECT snapshot_id FROM tg.snapshot WHERE collect_uuid = %s",
                           (chat.uuid,)).fetchone()
        if dup is not None:
            out.status, out.reason, out.snapshot_id = SKIPPED, f"already imported (snapshot {dup[0]})", int(dup[0])
            return
        # a retired source before tracking: M1 deletes the old chat's row, and the owner should hear why
        alias = conn.execute("SELECT chat_id FROM tg.chat_alias WHERE old_chat_id = %s", (chat.chat_id,)).fetchone()
        if alias is not None:
            out.status = REFUSED
            out.reason = (f"migrated: this chat became the supergroup {alias[0]}, so its old id takes no new "
                          "snapshots; collect the supergroup and upload that file")
            return
        row = conn.execute("SELECT tracked FROM tg.chat WHERE chat_id = %s", (chat.chat_id,)).fetchone()
        if row is None or not row[0]:
            out.status, out.reason = SKIPPED, _NOT_TRACKED
            return
        newest = conn.execute("SELECT max(started_at) FROM tg.snapshot WHERE chat_id = %s",
                              (chat.chat_id,)).fetchone()[0]
        if newest is not None and chat.started_at <= newest:
            out.status = SKIPPED
            out.reason = (f"not newer: tgaud already has a snapshot of this chat started "
                          f"{newest.astimezone(UTC):%Y-%m-%d %H:%M:%S}Z, and snapshots are applied in start order")
            return
        clock.arm()
        sid = conn.execute("""INSERT INTO tg.snapshot (chat_id, started_at, method, reported_at_start, collect_uuid)
                              VALUES (%s, %s, %s, %s, %s) RETURNING snapshot_id""",
                           (chat.chat_id, chat.started_at, s.method, s.reported_start, chat.uuid)).fetchone()[0]
        clock.arm()
        with conn.cursor().copy(COPY_MEMBERS) as copy:
            for m in tgstats_file.members(reader, chat):
                names = (m.username, m.first_name, m.last_name) if store_names else (None, None, None)
                copy.write_row((sid, m.user_id, m.seen_at, m.joined_date, m.inviter_id, m.via_request,
                                m.subscription_until, m.is_bot, m.is_deleted, m.is_premium, *names))
        clock.arm()
        # anyone forget-user erased stays erased, whenever the page says it saw them (the trigger of 008 keeps a
        # row dated after the erasure: a collector's clock is no proof)
        conn.execute("""DELETE FROM tg.snapshot_member m WHERE m.snapshot_id = %s
                        AND EXISTS (SELECT 1 FROM tg.erasure e WHERE e.user_hash = tg.erasure_hash(m.user_id))""",
                     (sid,))
        clock.arm()
        conn.execute("""UPDATE tg.snapshot_member m SET inviter_id = NULL WHERE m.snapshot_id = %s
                        AND m.inviter_id IS NOT NULL
                        AND EXISTS (SELECT 1 FROM tg.erasure e WHERE e.user_hash = tg.erasure_hash(m.inviter_id))""",
                     (sid,))
        clock.arm()
        written = conn.execute("SELECT count(*) FROM tg.snapshot_member WHERE snapshot_id = %s", (sid,)).fetchone()[0]
        clock.arm()
        conn.execute("""UPDATE tg.snapshot SET finished_at = %s, reported_at_end = %s, fetched = %s, complete = %s
                        WHERE snapshot_id = %s""",
                     (chat.finished_at, s.reported_end, written, s.complete, sid))
        try:
            _commit(conn)
        except psycopg.Error as e:
            if conn.broken or conn.closed:
                raise CommitUnknown() from e
            raise
        out.status, out.snapshot_id, out.written = IMPORTED, int(sid), int(written)
        out.reason = f"imported: {written} members written" + (
            f" ({chat.listed - written} left out: erased by forget-user)" if written < chat.listed else "")
    log.info("web import: chat %s: snapshot %s, %s members", chat.chat_id, out.snapshot_id, out.written)


def _commit(conn: psycopg.Connection) -> None:
    conn.commit()


__all__ = ["run", "Report", "Outcome", "FileError", "CHAT_SECONDS", "MAX_MEMBERS"]
