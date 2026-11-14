"""SQLite storage: chats, users and snapshots with their memberships."""

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
    return conn


def upsert_chat(conn: sqlite3.Connection, chat: ChatRow) -> None:
    conn.execute(
        "INSERT INTO chats (id, type, title, username) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET type=excluded.type, title=excluded.title, "
        "username=excluded.username",
        (chat.id, chat.type, chat.title, chat.username),
    )


def add_snapshot(
    conn: sqlite3.Connection,
    chat: ChatRow,
    users: Iterable[UserRow],
    reported: int | None,
    taken_at: str | None = None,
) -> SnapshotRow:
    """Store one full snapshot of a chat atomically and return it."""
    users = list(users)
    fetched = len({u.id for u in users})
    complete = not is_incomplete(fetched, reported)
    taken_at = taken_at or utcnow()
    with conn:
        upsert_chat(conn, chat)
        conn.executemany(
            "INSERT INTO users (id, username, first_name, last_name, is_bot, is_deleted) "
            "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
            "username=excluded.username, first_name=excluded.first_name, "
            "last_name=excluded.last_name, is_bot=excluded.is_bot, is_deleted=excluded.is_deleted",
            [(u.id, u.username, u.first_name, u.last_name, int(u.is_bot), int(u.is_deleted)) for u in users],
        )
        cur = conn.execute(
            "INSERT INTO snapshots (chat_id, taken_at, fetched, reported, complete) VALUES (?, ?, ?, ?, ?)",
            (chat.id, taken_at, fetched, reported, int(complete)),
        )
        snapshot_id = cur.lastrowid
        conn.executemany(
            "INSERT OR IGNORE INTO memberships (snapshot_id, user_id) VALUES (?, ?)",
            [(snapshot_id, u.id) for u in users],
        )
    return SnapshotRow(snapshot_id, chat.id, taken_at, fetched, reported, complete)


def _snapshot(row: sqlite3.Row) -> SnapshotRow:
    return SnapshotRow(
        row["id"], row["chat_id"], row["taken_at"], row["fetched"], row["reported"], bool(row["complete"])
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
