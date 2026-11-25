"""SQLite storage: chats, users and snapshots with their memberships.

A snapshot written by this version also carries what tgaud's snapshot contract needs (docs/design/design.md 5):
the collection's random id, when it started and finished, Telegram's count before and after the listing, how it
ran, and for every member the observation of the page that first listed them. Files written by older versions get
those columns on open (all NULL for their snapshots) and read back as before; tgaud refuses to import such
snapshots, so they stay for the analysis commands only.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS chats (
    id        INTEGER PRIMARY KEY,
    type      TEXT NOT NULL,
    title     TEXT NOT NULL,
    username  TEXT
);
CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY,
    username    TEXT,
    first_name  TEXT,
    last_name   TEXT,
    is_bot      INTEGER NOT NULL DEFAULT 0,
    is_deleted  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS snapshots (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id   INTEGER NOT NULL REFERENCES chats(id),
    taken_at  TEXT NOT NULL,
    fetched   INTEGER NOT NULL,
    reported  INTEGER,
    complete  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS memberships (
    snapshot_id  INTEGER NOT NULL REFERENCES snapshots(id),
    user_id      INTEGER NOT NULL REFERENCES users(id),
    PRIMARY KEY (snapshot_id, user_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS snapshots_by_chat ON snapshots(chat_id, id);
"""

# Added on open to files of every version (nullable: old snapshots read back with NULLs). The snapshot contract's
# fields: the collection's id, its times and counts, and how it ran.
SNAPSHOT_COLUMNS = {
    "uuid": "TEXT",            # random id made when the collection started; tgaud's duplicate key
    "started_at": "TEXT",      # before the first count read
    "finished_at": "TEXT",     # after the last count read (or the interrupt); taken_at is the same moment
    "method": "TEXT",          # basic_group | listing | deep: the path the run took
    "reported_start": "INTEGER",  # Telegram's member count before the listing
    "reported_end": "INTEGER",    # and after it; NULL when that read failed or was interrupted
}
# Per member, the observation of the page that first listed them (the users table keeps the latest profile).
MEMBERSHIP_COLUMNS = {
    "seen_at": "TEXT",          # when that page arrived
    "joined_date": "TEXT",      # participant.date; NULL for creators
    "inviter_id": "INTEGER",
    "via_request": "INTEGER",
    "subscription_until": "TEXT",
    "is_bot": "INTEGER",
    "is_deleted": "INTEGER",
    "is_premium": "INTEGER",
    "username": "TEXT",
    "first_name": "TEXT",
    "last_name": "TEXT",
}
METHODS = ("basic_group", "listing", "deep")

# A snapshot is incomplete when fetched falls short of what Telegram reported by
# more than this share: member counts drift a little while a listing runs.
INCOMPLETE_TOLERANCE = 0.02


class DatabaseNotFound(Exception):
    """The database file does not exist (analysis commands never create it)."""


@dataclass(frozen=True)
class ChatRow:
    id: int
    type: str
    title: str
    username: str | None


@dataclass(frozen=True)
class SnapshotRow:
    id: int
    chat_id: int
    taken_at: str
    fetched: int
    reported: int | None
    complete: bool
    uuid: str | None = None  # None: written by an older version (no snapshot contract fields)
    started_at: str | None = None
    finished_at: str | None = None
    method: str | None = None
    reported_start: int | None = None
    reported_end: int | None = None


@dataclass(frozen=True)
class UserRow:
    id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    is_bot: bool = False
    is_deleted: bool = False

    @property
    def name(self) -> str:
        return " ".join(p for p in (self.first_name, self.last_name) if p)


@dataclass(frozen=True)
class Member:
    """One member as one page listed them: the profile then, and the participant's fields where its type has them."""

    user: UserRow
    seen_at: str
    joined_date: str | None = None
    inviter_id: int | None = None
    via_request: bool | None = None
    subscription_until: str | None = None
    is_premium: bool | None = None

    @property
    def id(self) -> int:
        return self.user.id


@dataclass(frozen=True)
class Collection:
    """What the snapshot contract needs about one collection of a chat."""

    uuid: str
    started_at: str
    finished_at: str
    method: str
    reported_start: int | None
    reported_end: int | None


def is_incomplete(fetched: int, reported: int | None) -> bool:
    """True when Telegram reported clearly more members than we got."""
    if reported is None:
        return False
    return (reported - fetched) > reported * INCOMPLETE_TOLERANCE


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(path: str | Path, create: bool = True) -> sqlite3.Connection:
    path = Path(path)
    if not create and not path.exists():
        raise DatabaseNotFound(f"no database at {path}: run `tgstats collect` first")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    try:
        migrate(conn)
    except sqlite3.OperationalError:
        if create:
            raise
        # a read-only old file: the analysis commands read only the original columns
    return conn


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def migrate(conn: sqlite3.Connection) -> list[str]:
    """Add the snapshot contract's columns a file lacks, in one transaction; returns the columns added."""
    added: list[str] = []
    missing = []
    for table, columns in (("snapshots", SNAPSHOT_COLUMNS), ("memberships", MEMBERSHIP_COLUMNS)):
        have = _columns(conn, table)
        missing += [(table, column, kind) for column, kind in columns.items() if column not in have]
    if missing or not _has_uuid_index(conn):
        with conn:
            for table, column, kind in missing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {kind}")
                added.append(f"{table}.{column}")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS snapshots_by_uuid ON snapshots(uuid)")
    return added


def _has_uuid_index(conn: sqlite3.Connection) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'snapshots_by_uuid'"
                        ).fetchone() is not None


def upsert_chat(conn: sqlite3.Connection, chat: ChatRow) -> None:
    conn.execute(
        "INSERT INTO chats (id, type, title, username) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET type=excluded.type, title=excluded.title, "
        "username=excluded.username",
        (chat.id, chat.type, chat.title, chat.username),
    )


def _flag(value: bool | None) -> int | None:
    return None if value is None else int(value)


def add_snapshot(
    conn: sqlite3.Connection,
    chat: ChatRow,
    users: Iterable[UserRow | Member],
    reported: int | None,
    taken_at: str | None = None,
    complete: bool | None = None,
    collection: Collection | None = None,
) -> SnapshotRow:
    """Store one full snapshot of a chat atomically and return it.

    `users` are plain profiles or, from a collection, the members as first observed (one per user: the first one
    wins, as tgaud's ON CONFLICT DO NOTHING). `complete` overrides the verdict from the counts (an interrupted run
    is always incomplete). With `collection`, taken_at is its finished_at.
    """
    members: dict[int, UserRow | Member] = {}
    for u in users:
        members.setdefault(u.id, u)
    fetched = len(members)
    if complete is None:
        complete = not is_incomplete(fetched, reported)
    taken_at = collection.finished_at if collection is not None else taken_at or utcnow()
    c = collection
    with conn:
        upsert_chat(conn, chat)
        profiles = [m.user if isinstance(m, Member) else m for m in members.values()]
        conn.executemany(
            "INSERT INTO users (id, username, first_name, last_name, is_bot, is_deleted) "
            "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
            "username=excluded.username, first_name=excluded.first_name, "
            "last_name=excluded.last_name, is_bot=excluded.is_bot, is_deleted=excluded.is_deleted",
            [(u.id, u.username, u.first_name, u.last_name, int(u.is_bot), int(u.is_deleted)) for u in profiles],
        )
        cur = conn.execute(
            "INSERT INTO snapshots (chat_id, taken_at, fetched, reported, complete, uuid, started_at, finished_at, "
            "method, reported_start, reported_end) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (chat.id, taken_at, fetched, reported, int(complete), *(
                (c.uuid, c.started_at, c.finished_at, c.method, c.reported_start, c.reported_end) if c
                else (None,) * 6)),
        )
        snapshot_id = cur.lastrowid
        rows = []
        for m in members.values():
            if isinstance(m, Member):
                u = m.user
                rows.append((snapshot_id, u.id, m.seen_at, m.joined_date, m.inviter_id, _flag(m.via_request),
                             m.subscription_until, int(u.is_bot), int(u.is_deleted), _flag(m.is_premium),
                             u.username, u.first_name, u.last_name))
            else:
                rows.append((snapshot_id, m.id) + (None,) * 11)
        conn.executemany(
            "INSERT OR IGNORE INTO memberships (snapshot_id, user_id, seen_at, joined_date, inviter_id, via_request, "
            "subscription_until, is_bot, is_deleted, is_premium, username, first_name, last_name) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    if c is None:
        return SnapshotRow(snapshot_id, chat.id, taken_at, fetched, reported, complete)
    return SnapshotRow(snapshot_id, chat.id, taken_at, fetched, reported, complete, c.uuid, c.started_at,
                       c.finished_at, c.method, c.reported_start, c.reported_end)


def _snapshot(row: sqlite3.Row) -> SnapshotRow:
    keys = row.keys()
    extra = [row[k] if k in keys else None for k in SNAPSHOT_COLUMNS]
    return SnapshotRow(
        row["id"], row["chat_id"], row["taken_at"], row["fetched"], row["reported"], bool(row["complete"]), *extra
    )


def list_chats(conn: sqlite3.Connection) -> list[ChatRow]:
    rows = conn.execute("SELECT id, type, title, username FROM chats ORDER BY title, id")
    return [ChatRow(r["id"], r["type"], r["title"], r["username"]) for r in rows]


def latest_snapshots(conn: sqlite3.Connection) -> dict[int, SnapshotRow]:
    """The newest snapshot of every chat that has one, keyed by chat id."""
    rows = conn.execute(
        "SELECT s.* FROM snapshots s JOIN "
        "(SELECT chat_id, MAX(id) AS id FROM snapshots GROUP BY chat_id) m ON s.id = m.id"
    )
    return {r["chat_id"]: _snapshot(r) for r in rows}


def latest_snapshot(conn: sqlite3.Connection, chat_id: int) -> SnapshotRow | None:
    row = conn.execute("SELECT * FROM snapshots WHERE chat_id = ? ORDER BY id DESC LIMIT 1", (chat_id,)).fetchone()
    return _snapshot(row) if row else None


def snapshot_members(
    conn: sqlite3.Connection, snapshot_id: int, include_bots: bool = False
) -> dict[int, UserRow]:
    """Members of a snapshot by user id; bots and deleted accounts only on request."""
    sql = (
        "SELECT u.* FROM memberships m JOIN users u ON u.id = m.user_id WHERE m.snapshot_id = ?"
    )
    if not include_bots:
        sql += " AND u.is_bot = 0 AND u.is_deleted = 0"
    return {
        r["id"]: UserRow(
            r["id"], r["username"], r["first_name"], r["last_name"], bool(r["is_bot"]), bool(r["is_deleted"])
        )
        for r in conn.execute(sql, (snapshot_id,))
    }


def observations(conn: sqlite3.Connection, snapshot_id: int) -> dict[int, Member]:
    """The members of a snapshot as their first page observed them (written by this version; NULLs for older)."""
    out: dict[int, Member] = {}
    for r in conn.execute("SELECT * FROM memberships WHERE snapshot_id = ? ORDER BY user_id", (snapshot_id,)):
        user = UserRow(r["user_id"], r["username"], r["first_name"], r["last_name"], bool(r["is_bot"]),
                       bool(r["is_deleted"]))
        out[r["user_id"]] = Member(user, r["seen_at"], r["joined_date"], r["inviter_id"],
                                   None if r["via_request"] is None else bool(r["via_request"]),
                                   r["subscription_until"], None if r["is_premium"] is None else bool(r["is_premium"]))
    return out
