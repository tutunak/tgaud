import pytest

from tgstats import db
from tgstats.db import ChatRow, UserRow


def user(uid, bot=False, deleted=False):
    return UserRow(uid, f"u{uid}", f"First{uid}", f"Last{uid}", bot, deleted)


class Fixture:
    """Builds a database of chats and snapshots from plain member-id lists."""

    def __init__(self, conn):
        self.conn = conn
        self.clock = 0

    def chat(self, cid, title, members, reported=None, username=None, kind="supergroup", users=None):
        self.clock += 1
        chat = ChatRow(cid, kind, title, username)
        rows = users if users is not None else [user(m) for m in members]
        return db.add_snapshot(
            self.conn, chat, rows, len(members) if reported is None else reported,
            taken_at=f"2026-01-01T00:00:{self.clock:02d}+00:00",
        )


@pytest.fixture
def conn():
    c = db.connect(":memory:")
    yield c
    c.close()


@pytest.fixture
def fx(conn):
    return Fixture(conn)
