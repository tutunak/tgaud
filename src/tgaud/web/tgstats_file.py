"""An uploaded tgstats.db, read defensively and checked whole before the Import page writes anything.

The file comes from the owner's `tgstats collect` (src/tgstats/db.py), but it is read as untrusted input: opened
read-only and immutable (no journal, no lock, nothing written), with SQLite's defensive configuration, untrusted
schema, an authorizer that allows only reads of the known tables, a length limit on every value and a progress
deadline on every query. tgaud never imports the tgstats package; the file layout below is its own copy of the
contract (tests/tgaud/test_import.py checks it against tgstats).

Two kinds of problem:
  FileError       structural: not SQLite, missing or unexpected tables or columns, a view, trigger or virtual
                  table, or a value of the wrong type or shape in a row the import reads. The whole file is
                  refused before any write.
  Chat.refusal    a value error in one chat's latest snapshot (times out of order or in the future, a negative
                  count, a malformed id), an old-format snapshot, or one over the member ceiling. That chat is
                  refused, the others go on.

Per chat, the import takes the latest snapshot by started_at (an old-format snapshot by its taken_at), ties broken
by the new format, then the uuid: never the SQLite row id.
"""

from __future__ import annotations

import datetime as dt
import re
import sqlite3
import time
import unicodedata
import uuid as uuidlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote

UTC = dt.timezone.utc
MAGIC = b"SQLite format 3\x00"
FUTURE_SLACK = dt.timedelta(minutes=5)  # the collector's clock may run this far ahead of the server's
MAX_SNAPSHOTS = 100_000  # rows of the snapshots table read
MAX_CHATS = 2_000  # chats in one file
MAX_TEXT = 1_024  # characters of a name, username or title (Telegram's are far shorter)
VALUE_LIMIT = 64 * 1024  # SQLite's own limit on any one value read (bytes)
READ_SECONDS = 30.0  # the whole check of a file
METHODS = ("basic_group", "listing", "deep")

# The tgstats file layout (src/tgstats/db.py): the base tables and the snapshot contract's columns added later.
BASE = {
    "chats": ("id", "type", "title", "username"),
    "users": ("id", "username", "first_name", "last_name", "is_bot", "is_deleted"),
    "snapshots": ("id", "chat_id", "taken_at", "fetched", "reported", "complete"),
    "memberships": ("snapshot_id", "user_id"),
}
SNAPSHOT_COLUMNS = ("uuid", "started_at", "finished_at", "method", "reported_start", "reported_end")
MEMBERSHIP_COLUMNS = ("seen_at", "joined_date", "inviter_id", "via_request", "subscription_until", "is_bot",
                      "is_deleted", "is_premium", "username", "first_name", "last_name")
OPTIONAL_TABLES = ("sqlite_sequence",)  # SQLite's own, for AUTOINCREMENT
MEMBER_SQL = ("SELECT user_id, seen_at, joined_date, inviter_id, via_request, subscription_until, is_bot, is_deleted, "
              "is_premium, username, first_name, last_name FROM memberships WHERE snapshot_id = ? ORDER BY user_id")


class FileError(Exception):
    """The whole file is refused (shown on the page)."""


@dataclass(frozen=True)
class Snapshot:
    """One row of the file's snapshots table, typed; the contract fields are None for an old-format snapshot."""

    file_id: int
    chat_id: int
    legacy: bool
    taken_at: str
    fetched: int
    complete: bool
    uuid: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    method: str | None = None
    reported_start: int | None = None
    reported_end: int | None = None
    reported: int | None = None  # tgstats' own column: the larger of the two counts


@dataclass(frozen=True)
class Member:
    """One member row of a snapshot as the import writes it (tg.snapshot_member's columns, names cleaned)."""

    user_id: int
    seen_at: dt.datetime
    joined_date: dt.datetime | None
    inviter_id: int | None
    via_request: bool | None
    subscription_until: dt.datetime | None
    is_bot: bool
    is_deleted: bool
    is_premium: bool | None
    username: str | None
    first_name: str | None
    last_name: str | None


@dataclass
class Chat:
    """One chat of the file: its latest snapshot and what the check found."""

    chat_id: int
    title: str | None
    snapshot: Snapshot
    snapshots: int  # how many the file has for it
    listed: int = 0  # member rows of the latest snapshot
    refusal: str | None = None
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None
    uuid: uuidlib.UUID | None = None


@dataclass
class TgstatsFile:
    path: Path
    current: bool  # the file has the snapshot contract's columns (tgstats of this version opened it)
    chats: list[Chat] = field(default_factory=list)


# ---------------------------------------------------------------------------------------------------------------
# Opening
# ---------------------------------------------------------------------------------------------------------------


class Reader:
    """The file opened read-only and defensively; every query has the deadline of `seconds` from the open."""

    def __init__(self, path: str | Path, seconds: float = READ_SECONDS) -> None:
        self.path = Path(path)
        with open(self.path, "rb") as fh:
            head = fh.read(len(MAGIC))
        if not head:
            raise FileError("the file is empty: upload the tgstats.db that tgstats collect wrote")
        if head != MAGIC:
            raise FileError("this is not a SQLite file: upload the tgstats.db that tgstats collect wrote")
        self.deadline = time.monotonic() + seconds
        self.tables: set[str] = set()
        uri = f"file:{quote(str(self.path.resolve()))}?mode=ro&immutable=1"
        self.conn = sqlite3.connect(uri, uri=True, isolation_level=None, check_same_thread=False)
        try:
            self._harden()
        except BaseException:
            self.conn.close()
            raise

    def _harden(self) -> None:
        c = self.conn
        setconfig = getattr(c, "setconfig", None)  # Python 3.12+
        if setconfig is not None:
            for name, on in (("SQLITE_DBCONFIG_DEFENSIVE", True), ("SQLITE_DBCONFIG_TRUSTED_SCHEMA", False),
                             ("SQLITE_DBCONFIG_ENABLE_TRIGGER", False), ("SQLITE_DBCONFIG_ENABLE_VIEW", False),
                             ("SQLITE_DBCONFIG_ENABLE_FKEY", False)):
                op = getattr(sqlite3, name, None)
                if op is not None:
                    setconfig(op, on)
        for pragma in ("trusted_schema = OFF", "query_only = ON", "temp_store = MEMORY", "mmap_size = 0",
                       "cell_size_check = ON"):
            c.execute(f"PRAGMA {pragma}")
        setlimit = getattr(c, "setlimit", None)  # Python 3.11+
        if setlimit is not None:
            for name, value in (("SQLITE_LIMIT_LENGTH", VALUE_LIMIT), ("SQLITE_LIMIT_ATTACHED", 0),
                                ("SQLITE_LIMIT_COLUMN", 100), ("SQLITE_LIMIT_SQL_LENGTH", 4096)):
                op = getattr(sqlite3, name, None)
                if op is not None:
                    setlimit(op, value)
        c.set_progress_handler(self._late, 5_000)
        c.set_authorizer(self._authorize)

    def _late(self) -> int:
        return 1 if time.monotonic() > self.deadline else 0

    def _authorize(self, action: int, arg1: str | None, arg2: str | None, dbname: str | None,
                   source: str | None) -> int:
        if action == sqlite3.SQLITE_SELECT:
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_READ and arg1 in ("sqlite_master", *self.tables):
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_PRAGMA and arg1 == "table_xinfo":
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_FUNCTION and arg2 == "count":
            return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_DENY

    def query(self, sql: str, args: tuple = ()) -> sqlite3.Cursor:
        try:
            return self.conn.execute(sql, args)
        except sqlite3.Error as e:
            raise _read_error(self, e) from None

    def rows(self, cur: sqlite3.Cursor) -> Iterator[tuple]:
        try:
            yield from cur
        except sqlite3.Error as e:
            raise _read_error(self, e) from None

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Reader":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _read_error(reader: Reader, e: sqlite3.Error) -> FileError:
    text = str(e)
    if "interrupted" in text and time.monotonic() > reader.deadline:
        return FileError("reading the file took too long; it is too large or damaged")
    damaged = isinstance(e, sqlite3.DatabaseError) and not isinstance(e, sqlite3.OperationalError)
    if damaged or "not a database" in text or "malformed" in text:
        return FileError(f"the file is damaged or not a tgstats.db ({text})")
    if "too big" in text:
        return FileError("the file holds a value larger than any tgstats.db value")
    if "decode" in text.lower():
        return FileError("the file holds text that is not UTF-8")
    return FileError(f"the file could not be read ({text})")


# ---------------------------------------------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------------------------------------------


_VIRTUAL = re.compile(r"\s*CREATE\s+VIRTUAL\b", re.I)


def _check_schema(r: Reader) -> bool:
    """Exactly the tgstats tables with exactly their columns, nothing else that can run code; True when the file has
    the snapshot contract's columns."""
    entries = r.query("SELECT type, name, tbl_name, sql FROM sqlite_master").fetchall()
    tables: dict[str, str] = {}
    for kind, name, tbl, sql in entries:
        if kind == "table":
            if not isinstance(sql, str) or _VIRTUAL.match(sql):
                raise FileError(f"the file has a virtual table ({name}): not a tgstats.db")
            tables[name] = sql
        elif kind == "index":
            if tbl not in (*BASE, *OPTIONAL_TABLES):
                raise FileError(f"the file has an index on an unknown table ({tbl}): not a tgstats.db")
        else:
            raise FileError(f"the file has a {kind} ({name}): a tgstats.db has none")
    missing = [t for t in BASE if t not in tables]
    if len(missing) == len(BASE):
        raise FileError("the file is SQLite but has none of the tgstats tables: upload the tgstats.db that "
                        "tgstats collect wrote")
    if missing:
        raise FileError(f"the file lacks the tgstats table(s) {', '.join(missing)}")
    extra = sorted(set(tables) - set(BASE) - set(OPTIONAL_TABLES))
    if extra:
        raise FileError(f"the file has table(s) a tgstats.db does not have: {', '.join(extra)}")
    r.tables = set(tables)
    # table_xinfo, not table_info: the latter leaves out generated columns (hidden 2, 3; 1 = a virtual table's)
    info = {t: r.query(f"PRAGMA table_xinfo({t})").fetchall() for t in BASE}
    hidden = [f"{t}.{row[1]}" for t, rows in info.items() for row in rows if row[6]]
    if hidden:
        raise FileError(f"the file has generated or hidden column(s) ({', '.join(hidden)}): a tgstats.db has none")
    names = {t: {row[1] for row in rows} for t, rows in info.items()}
    current = set(SNAPSHOT_COLUMNS) <= names["snapshots"]
    want = {t: set(cols) for t, cols in BASE.items()}
    if current:
        want["snapshots"] |= set(SNAPSHOT_COLUMNS)
        want["memberships"] |= set(MEMBERSHIP_COLUMNS)
    for t in BASE:
        if names[t] != want[t]:
            odd = sorted(names[t] ^ want[t])
            raise FileError(f"the table {t} has unexpected or missing columns ({', '.join(odd)}): not a tgstats.db "
                            "of a known version")
    pk = {t: sorted((row[5], row[1]) for row in rows if row[5]) for t, rows in info.items()}
    if pk["snapshots"] != [(1, "id")] or pk["memberships"] != [(1, "snapshot_id"), (2, "user_id")]:
        raise FileError("the snapshots or memberships table has another key than a tgstats.db")
    return current


# ---------------------------------------------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------------------------------------------


def _typed(value: object, kinds: tuple[type, ...], what: str) -> object:
    if value is None and type(None) in kinds:
        return None
    if type(value) not in kinds:  # bool is no SQLite storage class; int, str, float, bytes are
        raise FileError(f"{what} has a value of the wrong type ({type(value).__name__})")
    return value


INT = (int,)
OPT_INT, OPT_TEXT = (int, type(None)), (str, type(None))


def _flag(value: object, what: str, optional: bool = False) -> bool | None:
    v = _typed(value, OPT_INT if optional else INT, what)
    if v is None:
        return None
    if v not in (0, 1):
        raise FileError(f"{what} is not 0 or 1")
    return bool(v)


def _text(value: object, what: str, limit: int = MAX_TEXT) -> str | None:
    v = _typed(value, OPT_TEXT, what)
    if v is None:
        return None
    if len(v) > limit:
        raise FileError(f"{what} is longer than any tgstats.db value")
    return v


def clean_name(value: str | None) -> str | None:
    """A name or username without control characters (C0, C1, DEL); None when nothing is left."""
    if value is None:
        return None
    out = "".join(ch for ch in value if unicodedata.category(ch) != "Cc")
    return out or None


class BadValue(Exception):
    """A value error in one snapshot (refuses that chat only)."""


def parse_time(value: str | None, what: str) -> dt.datetime | None:
    if value is None:
        return None
    try:
        t = dt.datetime.fromisoformat(value)
    except ValueError:
        raise BadValue(f"{what} is not a time ({value[:40]!r})") from None
    if t.tzinfo is None:
        raise BadValue(f"{what} has no time zone")
    try:
        return t.astimezone(UTC)
    except OverflowError:  # year 1 with a positive offset, year 9999 with a negative one
        raise BadValue(f"{what} is out of range ({value[:40]!r})") from None


def _snapshot(row: tuple, current: bool) -> Snapshot:
    file_id = _typed(row[0], INT, "snapshots.id")
    chat_id = _typed(row[1], INT, "snapshots.chat_id")
    taken_at = _text(row[2], "snapshots.taken_at", 64)
    if taken_at is None:
        raise FileError("snapshots.taken_at is empty")
    fetched = _typed(row[3], INT, "snapshots.fetched")
    reported = _typed(row[4], OPT_INT, "snapshots.reported")
    complete = _flag(row[5], "snapshots.complete")
    if not current:
        return Snapshot(file_id, chat_id, True, taken_at, fetched, bool(complete), reported=reported)
    u, started, finished, method = (_text(row[i], f"snapshots.{n}", 64) for i, n in
                                    zip(range(6, 10), SNAPSHOT_COLUMNS[:4]))
    rep_start = _typed(row[10], OPT_INT, "snapshots.reported_start")
    rep_end = _typed(row[11], OPT_INT, "snapshots.reported_end")
    legacy = u is None and started is None and finished is None and method is None
    return Snapshot(file_id, chat_id, legacy, taken_at, fetched, bool(complete), u, started, finished, method,
                    rep_start, rep_end, reported)


def _order_key(s: Snapshot) -> tuple:
    """Latest by started_at (an old snapshot by taken_at); at the same time a new-format one wins, then the uuid.
    BadValue when the time is unreadable: then no snapshot of the chat can be called the latest."""
    t = parse_time(s.taken_at, "taken_at") if s.legacy else parse_time(s.started_at, "started_at")
    if t is None:
        raise BadValue("started_at is missing")
    return (t, not s.legacy, (s.uuid or "").lower(), s.file_id)


def _canonical_uuid(value: str | None) -> uuidlib.UUID:
    if value is None:
        raise BadValue("the snapshot has no collection id")
    try:
        u = uuidlib.UUID(value)
    except ValueError:
        raise BadValue("the collection id is not a uuid") from None
    if str(u) != value.lower() or u.int == 0:
        raise BadValue("the collection id is not a well-formed uuid")
    return u


def _check_snapshot(chat: Chat, now: dt.datetime) -> None:
    s = chat.snapshot
    chat.uuid = _canonical_uuid(s.uuid)
    started = parse_time(s.started_at, "started_at")
    finished = parse_time(s.finished_at, "finished_at")
    if started is None or finished is None:
        raise BadValue("started_at or finished_at is missing")
    if s.method not in METHODS:
        raise BadValue(f"method must be one of {', '.join(METHODS)}")
    if finished < started:
        raise BadValue("finished_at is before started_at")
    if finished > now + FUTURE_SLACK:
        raise BadValue(f"finished_at {finished:%Y-%m-%d %H:%M:%S}Z is in the future (the server's time is "
                       f"{now:%Y-%m-%d %H:%M:%S}Z)")
    for n, what in ((s.reported_start, "the count before the listing"), (s.reported_end, "the count after it"),
                    (s.reported, "the reported count"), (s.fetched, "the fetched count")):
        if n is not None and not 0 <= n <= 2**31 - 1:
            raise BadValue(f"{what} is negative or too large")
    if s.complete and s.reported_end is None:  # tgstats marks a listing without its final count incomplete
        raise BadValue("the snapshot is marked complete but has no member count after the listing")
    if s.fetched != chat.listed:  # tgstats writes one member row per user it counts
        raise BadValue(f"the snapshot says it fetched {s.fetched} members, but the file has {chat.listed} member "
                       "rows for it")
    chat.started_at, chat.finished_at = started, finished


def _shape(row: tuple) -> tuple:
    """A member row with every storage class and shape checked (FileError: the whole file is refused): what this
    tgstats writes, is_bot and is_deleted never NULL (NULL would read as false and could undo a deleted account)."""
    return (_typed(row[0], INT, "memberships.user_id"), _text(row[1], "memberships.seen_at", 64),
            _text(row[2], "memberships.joined_date", 64), _typed(row[3], OPT_INT, "memberships.inviter_id"),
            _flag(row[4], "memberships.via_request", optional=True),
            _text(row[5], "memberships.subscription_until", 64), _flag(row[6], "memberships.is_bot"),
            _flag(row[7], "memberships.is_deleted"), _flag(row[8], "memberships.is_premium", optional=True),
            *(_text(row[i], f"memberships.{n}") for i, n in ((9, "username"), (10, "first_name"), (11, "last_name"))))


def _member(v: tuple, started: dt.datetime, finished: dt.datetime) -> Member:
    """A shaped member row checked against its snapshot's interval (BadValue: that chat is refused)."""
    user_id, seen_raw, joined_raw, inviter, via, until_raw, is_bot, is_deleted, is_premium, *names = v
    if not 0 < user_id <= 2**63 - 1:
        raise BadValue("a user id is not a positive number")
    seen = parse_time(seen_raw, "a member's seen_at")
    if seen is None:
        raise BadValue("a member row has no seen_at")
    if not started <= seen <= finished:
        raise BadValue(f"a member's seen_at {seen:%Y-%m-%d %H:%M:%S}Z is outside the collection "
                       f"({started:%Y-%m-%d %H:%M:%S}Z to {finished:%Y-%m-%d %H:%M:%S}Z)")
    if inviter is not None and not 0 < inviter <= 2**63 - 1:
        inviter = None  # no inviter (Telegram's 0)
    return Member(user_id, seen, parse_time(joined_raw, "a joined_date"), inviter, via,
                  parse_time(until_raw, "a subscription_until"), is_bot, is_deleted, is_premium,
                  *(clean_name(n) for n in names))


def member(row: tuple, started: dt.datetime, finished: dt.datetime) -> Member:
    """One member row, shaped (FileError) and checked against its snapshot's interval (BadValue)."""
    return _member(_shape(row), started, finished)


def _scan(reader: Reader, chat: Chat) -> BadValue | None:
    """Shape every member row of the chat's snapshot, whatever the chat's own refusal (a structural error refuses
    the whole file), and check their values while the chat is still admissible; returns the first value error."""
    values = chat.refusal is None and chat.started_at is not None and chat.finished_at is not None
    bad: BadValue | None = None
    for row in reader.rows(reader.query(MEMBER_SQL, (chat.snapshot.file_id,))):
        shaped = _shape(row)
        if values and bad is None:
            try:
                _member(shaped, chat.started_at, chat.finished_at)  # type: ignore[arg-type]
            except BadValue as e:
                bad = e
    return bad


# ---------------------------------------------------------------------------------------------------------------
# The whole file
# ---------------------------------------------------------------------------------------------------------------


def check(reader: Reader, *, now: dt.datetime, ceiling: int) -> TgstatsFile:
    """The file's chats with their latest snapshot, every row the import will write checked; FileError for the
    whole file. Nothing is kept of the member rows but their count (they are read again when written)."""
    current = _check_schema(reader)
    cols = "id, chat_id, taken_at, fetched, reported, complete" + (
        ", " + ", ".join(SNAPSHOT_COLUMNS) if current else "")
    count = reader.query("SELECT count(*) FROM snapshots").fetchone()[0]
    if count > MAX_SNAPSHOTS:
        raise FileError(f"the file has {count} snapshots, more than the {MAX_SNAPSHOTS} the import reads")
    by_chat: dict[int, list[Snapshot]] = {}
    for row in reader.rows(reader.query(f"SELECT {cols} FROM snapshots ORDER BY id")):
        s = _snapshot(row, current)
        if s.chat_id not in by_chat and len(by_chat) >= MAX_CHATS:
            raise FileError(f"the file has snapshots of more than {MAX_CHATS} chats, the most the import reads")
        by_chat.setdefault(s.chat_id, []).append(s)
    uuids = [s.uuid.lower() for ss in by_chat.values() for s in ss if s.uuid is not None]
    if len(uuids) != len(set(uuids)):
        raise FileError("two snapshots of the file have the same collection id")
    titles: dict[int, str | None] = {}
    for n, row in enumerate(reader.rows(reader.query("SELECT id, title FROM chats")), 1):
        if n > MAX_CHATS:
            raise FileError(f"the file has more than {MAX_CHATS} chats, the most the import reads")
        chat_id, title = _typed(row[0], INT, "chats.id"), clean_name(_text(row[1], "chats.title"))
        if chat_id in by_chat:  # only the titles of chats it will report
            titles[chat_id] = title
    out = TgstatsFile(reader.path, current)
    for chat_id in sorted(by_chat):
        snaps = by_chat[chat_id]
        try:
            latest, unreadable = max(snaps, key=_order_key), None
        except BadValue as e:
            latest, unreadable = snaps[-1], e
        chat = Chat(chat_id, titles.get(chat_id), latest, len(snaps))
        out.chats.append(chat)
        chat.listed = reader.query("SELECT count(*) FROM memberships WHERE snapshot_id = ?",
                                   (latest.file_id,)).fetchone()[0]
        if latest.legacy:  # an old-format snapshot's member rows are never read by the import
            chat.refusal = "old format" if unreadable is None else \
                f"bad times or values: {unreadable}, so the latest snapshot of this chat cannot be told"
            continue
        if unreadable is not None:
            chat.refusal = f"bad times or values: {unreadable}, so the latest snapshot of this chat cannot be told"
        else:
            try:
                _check_snapshot(chat, now)
            except BadValue as e:
                chat.refusal = f"bad times or values: {e}"
        if chat.refusal is None and chat.listed > ceiling:
            chat.refusal = (f"too large: {chat.listed} members in one snapshot, more than the {ceiling} one import "
                            "transaction takes")
        bad = _scan(reader, chat)
        if bad is not None and chat.refusal is None:
            chat.refusal = f"bad times or values: {bad}"
    return out


def members(reader: Reader, chat: Chat) -> Iterator[Member]:
    """The member rows of the chat's latest snapshot in user id order, checked (FileError, BadValue)."""
    assert chat.started_at is not None and chat.finished_at is not None
    for row in reader.rows(reader.query(MEMBER_SQL, (chat.snapshot.file_id,))):
        yield member(row, chat.started_at, chat.finished_at)
