"""The Import page's writer: the latest snapshot of each chat of an uploaded tgstats.db into tg.snapshot and
tg.snapshot_member, exactly as the snapshot contract (design.md 5) writes them, so the reconciler applies them.

The whole file is checked first (tgaud.web.tgstats_file), outside the web connection; an invalid file writes
nothing. Then each admissible chat gets one transaction on the web connection, bounded in total by CHAT_SECONDS
(the fence's and the chat lock's waits included: lock_timeout and statement_timeout are set from the time left
before every statement):

    generation fence, tgaud.history.lock_chats(chat)      (migrations, forget-user and ingest take the same lock)
    tg.lock_collection(uuid), then the collection's versions anywhere in tg.snapshot (collect_uuid or version_of,
    migration 012; the claim is the collect_uuid row's chat and source):
      a version finished when this one did               -> skipped: already imported (that version)
      claimed by another chat or source, or another start -> skipped: already imported
      this one finished before the newest version        -> skipped: older version
      otherwise a later version: the checks below, then written as a delta (see _write)
    the chat a retired source (tg.chat_alias.old_chat_id) -> refused: migrated
    the chat not tracked                                  -> skipped: not tracked
    a snapshot of the chat with started_at >= this one's  -> skipped: not newer (the reconciler applies a chat's
                                                             snapshots in started_at order: never insert behind);
                                                             a later version: the newest version must be the chat's
                                                             newest snapshot by (started_at, snapshot_id)
    INSERT tg.snapshot unfinished; COPY its members; drop rows of anyone tg.forget_user erased (whatever their
    seen_at: the collector's clock cannot prove a page came after the erasure) and clear erased inviters;
    finish it with fetched = the rows that survived

A later version is written the same way with version_of instead of collect_uuid and the claim's started_at (so it
sorts right after the newest version), less every member a stored version listed (one statement: their member rows
and the sighting runs that begin or end at a version, so any reconcile or prune state of theirs), and always
complete = false: it is not a whole listing and gives no absences. It adds nobody -> skipped, nothing written.

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
from tgaud.web.app import HttpError, OutOfTime, WebDb, db_error_line
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
    later_version: bool = False  # imported as the members a later version of a stored collection added


@dataclass
class Report:
    outcomes: list[Outcome]
    current: bool  # the file has the new format's columns

    def count(self, status: str) -> int:
        return sum(o.status == status for o in self.outcomes)

    @property
    def later_versions(self) -> int:
        return sum(o.status == IMPORTED and o.later_version for o in self.outcomes)


class CommitUnknown(Exception):
    """The commit was sent but its result never came back: the snapshot may or may not be there."""


@dataclass(frozen=True)
class Version:
    """A stored version of a collection: its first import (the claim) or a later version written as a delta."""

    snapshot_id: int
    chat_id: int
    source: int  # coalesce(source_chat_id, chat_id): the chat id it was taken under
    started_at: dt.datetime
    finished_at: dt.datetime | None
    claim: bool  # collect_uuid holds the uuid (the first import); else version_of does


def _versions(q: Callable[..., psycopg.Cursor], uuid) -> list[Version]:
    """Every stored version of the collection, by snapshot_id; the caller holds tg.lock_collection(uuid)."""
    rows = q("""SELECT snapshot_id, chat_id, coalesce(source_chat_id, chat_id), started_at, finished_at,
                       collect_uuid IS NOT NULL
                FROM tg.snapshot WHERE collect_uuid = %(u)s OR version_of = %(u)s ORDER BY snapshot_id""",
             {"u": uuid}).fetchall()
    return [Version(int(r[0]), int(r[1]), int(r[2]), r[3], r[4], bool(r[5])) for r in rows]


def _against_versions(chat: Chat, versions: list[Version]) -> tuple[str, int] | None:
    """Why the file's snapshot is skipped against the stored versions of its collection, with the snapshot named;
    None when it is a later version this chat may add (the chat's own checks still follow)."""
    for v in versions:
        if v.finished_at == chat.finished_at:  # the same version: a retry, whatever happened to the chat since
            return f"already imported (snapshot {v.snapshot_id})", v.snapshot_id
    claim = next((v for v in versions if v.claim), versions[0])
    if (claim.chat_id, claim.source) != (chat.chat_id, chat.chat_id) or claim.started_at != chat.started_at:
        return f"already imported (snapshot {claim.snapshot_id})", claim.snapshot_id
    newest = _newest(versions)
    if chat.finished_at < newest.finished_at:
        return (f"older version: tgaud has a later version of this collection (snapshot {newest.snapshot_id})",
                newest.snapshot_id)
    return None


def _newest(versions: list[Version]) -> Version:
    return max(versions, key=lambda v: (v.finished_at, v.snapshot_id))


# the members of the new snapshot that a stored version listed: their member rows (unapplied, half applied,
# applied, or what a prune left) and the sighting runs that begin or end at a version (the reconciler folds every
# row it applies into one, bots and deleted accounts included; pruning starts only after reconciled_at). One
# statement, one READ COMMITTED snapshot: a prune batch committing meanwhile cannot hide anyone. A run through a
# version ends at a version: the newest one is the chat's newest snapshot, and a delta never lists anyone again.
_DROP_LISTED = """
DELETE FROM tg.snapshot_member n WHERE n.snapshot_id = %(s)s AND (
  EXISTS (SELECT 1 FROM tg.snapshot_member o WHERE o.snapshot_id = ANY(%(v)s::bigint[]) AND o.user_id = n.user_id)
  OR EXISTS (SELECT 1 FROM tg.sighting x WHERE x.chat_id = %(c)s AND x.user_id = n.user_id
               AND (x.first_snapshot_id = ANY(%(v)s::bigint[]) OR x.last_snapshot_id = ANY(%(v)s::bigint[]))))"""


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
        refused = [c for c in checked.chats if c.refusal is not None]
        known = _known_chats(database, [c.chat_id for c in refused]) if refused else {}
        for chat in checked.chats:
            out = outcomes[chat.chat_id]
            if chat.refusal is not None:
                # nothing to write: an untracked chat says so first (the owner may never have meant to track it);
                # when tgaud's chat list could not be read, the refusal itself is still true
                title, tracked = known.get(chat.chat_id, (None, True))
                out.title = title or out.title
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
    """tgaud's title and tracking of the refused chats, for their rows of the result page; empty when it cannot be
    read now (nothing is written for these chats, so no failure of this read may cost the others their import)."""
    try:
        with database.read() as conn:
            rows = conn.execute("SELECT chat_id, title, tracked FROM tg.chat WHERE chat_id = ANY(%s::bigint[])",
                                (chat_ids,)).fetchall()
    except (psycopg.Error, HttpError) as e:
        log.warning("web import: tgaud's chats could not be read for the result page: %s",
                    e.message if isinstance(e, HttpError) else db_error_line(e))
        return {}
    known = {int(r[0]): (r[1], bool(r[2])) for r in rows}
    return {c: known.get(c, (None, False)) for c in chat_ids}


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
    except HttpError as e:  # the connection busy with another request for too long, or the service stopping
        out.status, out.written, out.snapshot_id = FAILED, None, None
        out.reason = f"failed: {e.message}; nothing was written for this chat; {AGAIN}"
        log.warning("web import: chat %s: %s", chat.chat_id, e.message)


def _write(database: WebDb, reader: tgstats_file.Reader, chat: Chat, out: Outcome, *, store_names: bool,
           seconds: float) -> None:
    s = chat.snapshot
    with database.write_within(seconds) as (conn, clock):
        reader.deadline = clock.end  # reading the file again counts against the same time

        def q(sql: str, args: tuple | dict = ()) -> psycopg.Cursor:
            clock.arm()  # every statement gets only what is left of the chat's time
            return conn.execute(sql, args)

        clock.arm()
        history.lock_chats(conn, [chat.chat_id])
        q("SELECT tg.lock_collection(%s)", (chat.uuid,))  # a claim in flight from any chat commits first
        versions = _versions(q, chat.uuid)
        if versions:
            skip = _against_versions(chat, versions)
            if skip is not None:
                out.status, (out.reason, out.snapshot_id) = SKIPPED, skip
                return
        # a retired source before tracking: M1 deletes the old chat's row, and the owner should hear why
        alias = q("SELECT chat_id FROM tg.chat_alias WHERE old_chat_id = %s", (chat.chat_id,)).fetchone()
        if alias is not None:
            out.status = REFUSED
            out.reason = (f"migrated: this chat became the supergroup {alias[0]}, so its old id takes no new "
                          "snapshots; collect the supergroup and upload that file")
            return
        row = q("SELECT tracked, title FROM tg.chat WHERE chat_id = %s", (chat.chat_id,)).fetchone()
        if row is not None and row[1]:
            out.title = row[1]
        if row is None or not row[0]:
            out.status, out.reason = SKIPPED, _NOT_TRACKED
            return
        newest = q("""SELECT snapshot_id, started_at FROM tg.snapshot WHERE chat_id = %s
                      ORDER BY started_at DESC, snapshot_id DESC LIMIT 1""", (chat.chat_id,)).fetchone()
        if newest is not None and (newest[0] != _newest(versions).snapshot_id if versions
                                   else chat.started_at <= newest[1]):
            out.status = SKIPPED
            out.reason = (f"not newer: tgaud already has a snapshot of this chat started "
                          f"{newest[1].astimezone(UTC):%Y-%m-%d %H:%M:%S}Z, and snapshots are applied in start order")
            return
        # a later version: the uuid goes to version_of (collect_uuid keeps one row per collection, 011)
        sid = q("""INSERT INTO tg.snapshot (chat_id, started_at, method, reported_at_start, collect_uuid, version_of)
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING snapshot_id""",
                (chat.chat_id, chat.started_at, s.method, s.reported_start, None if versions else chat.uuid,
                 chat.uuid if versions else None)).fetchone()[0]
        clock.arm()
        with conn.cursor().copy(COPY_MEMBERS) as copy:
            for m in tgstats_file.members(reader, chat):
                names = (m.username, m.first_name, m.last_name) if store_names else (None, None, None)
                copy.write_row((sid, m.user_id, m.seen_at, m.joined_date, m.inviter_id, m.via_request,
                                m.subscription_until, m.is_bot, m.is_deleted, m.is_premium, *names))
        if versions:
            q(_DROP_LISTED, {"s": sid, "v": [v.snapshot_id for v in versions], "c": chat.chat_id})
        # anyone forget-user erased stays erased, whenever the page says it saw them (the trigger of 008 keeps a
        # row dated after the erasure: a collector's clock is no proof)
        q("""DELETE FROM tg.snapshot_member m WHERE m.snapshot_id = %s
             AND EXISTS (SELECT 1 FROM tg.erasure e WHERE e.user_hash = tg.erasure_hash(m.user_id))""", (sid,))
        q("""UPDATE tg.snapshot_member m SET inviter_id = NULL WHERE m.snapshot_id = %s AND m.inviter_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM tg.erasure e WHERE e.user_hash = tg.erasure_hash(m.inviter_id))""", (sid,))
        written = q("SELECT count(*) FROM tg.snapshot_member WHERE snapshot_id = %s", (sid,)).fetchone()[0]
        if versions and written == 0:
            conn.rollback()  # nothing to add: the delta row and its rows go
            last = _newest(versions).snapshot_id
            out.status, out.snapshot_id = SKIPPED, last
            out.reason = f"already imported (snapshot {last}): the file adds no member"
            return
        # a later version is never a whole listing: it must grade partial and give no absences
        q("""UPDATE tg.snapshot SET finished_at = %s, reported_at_end = %s, fetched = %s, complete = %s
             WHERE snapshot_id = %s""", (chat.finished_at, s.reported_end, written,
                                         False if versions else s.complete, sid))
        clock.arm()  # the commit too
        try:
            _commit(conn)
        except psycopg.Error as e:
            if conn.broken or conn.closed:
                raise CommitUnknown() from e
            raise
        out.status, out.snapshot_id, out.written = IMPORTED, int(sid), int(written)
        if versions:
            claim = next((v for v in versions if v.claim), versions[0])
            out.later_version = True
            out.reason = (f"imported: a later version of the collection in snapshot {claim.snapshot_id}: "
                          f"{written} members it lacked written ({chat.listed} in the file)")
        else:
            out.reason = f"imported: {written} members written" + (
                f" ({chat.listed - written} left out: erased by forget-user)" if written < chat.listed else "")
    log.info("web import: chat %s: snapshot %s, %s members%s", chat.chat_id, out.snapshot_id, out.written,
             " (a later version)" if out.later_version else "")


def _commit(conn: psycopg.Connection) -> None:
    conn.commit()


__all__ = ["run", "Report", "Outcome", "FileError", "CHAT_SECONDS", "MAX_MEMBERS"]
