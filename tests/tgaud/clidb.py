"""Support for the CLI tests: a fixture database with the rollups run, and a way to run `tgaud` commands on it.

    report_db   (session) a copy of the history fixture with R1 and R2 committed at "now" = 2026-11-23 12:00 UTC;
                read-only tests use it as it is
    fresh_db    (per test) a copy of report_db, for tests that write
    cli         bind(dsn) -> Cli; `Cli.run(*argv)` runs tgaud.cli.main with the database in the environment and
                returns Out(code, out, err); `Cli.rows(*argv)` runs a report with --csv and parses stdout
"""

from __future__ import annotations

import csv
import datetime as dt
import io
from dataclasses import dataclass

import psycopg
import psycopg.conninfo
import psycopg.sql
import pytest

from tgaud import cli, rollups
from tgaud.jobs import audience

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 23, 12, tzinfo=UTC)
NOW_ARG = "2026-11-23T12:00:00+00:00"
A, B, C, D = -1001000000001, -1001000000002, -1001000000003, -1001000000004
AD = [A, B, C, D]
TOKEN = "5000:TEST-token_abcdefghijklmnop"  # the fake Bot API's bot id is 5000, as in the fixture's bot_state
LINK_KEY = "ab" * 32


def copy_db(pg, source_dsn: str) -> str:
    """A template copy of a database (no session may be open on the source); returns the tenant DSN."""
    name = f"{pg.tenant}_x{len(pg.databases) + 1}"
    src = psycopg.conninfo.conninfo_to_dict(source_dsn)["dbname"]
    with pg.admin() as a:
        a.execute(psycopg.sql.SQL("CREATE DATABASE {} TEMPLATE {} OWNER {}").format(
            psycopg.sql.Identifier(name), psycopg.sql.Identifier(src), psycopg.sql.Identifier(pg.tenant)))
        pg.databases.append(name)
        a.execute(psycopg.sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(psycopg.sql.Identifier(name)))
    return pg.tenant_dsn(name)


def run_rollups(dsn: str, as_of: dt.datetime = NOW) -> None:
    with psycopg.connect(dsn) as c:
        chats = [r[0] for r in c.execute("SELECT chat_id FROM tg.chat WHERE tracked ORDER BY chat_id")]
        rollups.refresh_chat_days(c, chats, as_of)
        c.commit()
        audience.refresh(c, as_of)


@pytest.fixture(scope="session")
def report_db(pg, history_fixture) -> str:
    dsn = copy_db(pg, history_fixture)
    run_rollups(dsn)
    return dsn


@pytest.fixture
def fresh_db(pg, report_db) -> str:
    return copy_db(pg, report_db)


@dataclass
class Out:
    code: int
    out: str
    err: str


class Cli:
    def __init__(self, pg, dsn: str, monkeypatch, capsys) -> None:
        self.pg, self.dsn, self.capsys = pg, dsn, capsys
        name = psycopg.conninfo.conninfo_to_dict(dsn)["dbname"]
        monkeypatch.setenv("TGAUD_DATABASE_URL", pg.tenant_url(name))
        monkeypatch.setenv("TGAUD_DATABASE_PASSWORD", pg.tenant_password)
        monkeypatch.setenv("TGAUD_BOT_TOKEN", TOKEN)
        monkeypatch.setenv("TGAUD_LINK_HMAC_KEY", LINK_KEY)

    def run(self, *argv: str) -> Out:
        self.capsys.readouterr()
        code = cli.main(list(argv))
        captured = self.capsys.readouterr()
        return Out(code, captured.out, captured.err)

    def rows(self, *argv: str, as_of: str | None = NOW_ARG) -> list[dict[str, str]]:
        """Run a report with --csv at the fixture's "now" and parse its stdout; the command must succeed."""
        extra = ["--as-of", as_of] if as_of else []
        out = self.run(*argv, *extra, "--csv")
        assert out.code == 0, out.err
        return list(csv.DictReader(io.StringIO(out.out)))


@pytest.fixture
def cli_for(pg, monkeypatch, capsys):
    def bind(dsn: str) -> Cli:
        return Cli(pg, dsn, monkeypatch, capsys)

    return bind


def ts(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text).replace(tzinfo=UTC)


class Builder:
    """Hand-written history for the small cases: chats, people, events and the spells the rebuild rules give."""

    def __init__(self, conn: psycopg.Connection) -> None:
        self.c = conn

    def chat(self, chat_id: int, title: str, chat_type: str = "supergroup", tracked: bool = True,
             username: str | None = None) -> int:
        self.c.execute("INSERT INTO tg.chat (chat_id, chat_type, title, username, tracked, bot_status, first_seen_at) "
                       "VALUES (%s, %s, %s, %s, %s, 'administrator', '2026-01-01+00')",
                       (chat_id, chat_type, title, username, tracked))
        self.c.execute("INSERT INTO tg.coverage (chat_id, period) VALUES (%s, tstzrange('2026-01-01+00', NULL))",
                       (chat_id,))
        return chat_id

    def user(self, user_id: int, **cols) -> int:
        self.c.execute("INSERT INTO tg.tg_user (user_id, username, first_seen_at, last_seen_at) "
                       "VALUES (%s, %s, '2026-01-01+00', '2026-11-23+00') ON CONFLICT DO NOTHING",
                       (user_id, cols.get("username")))
        return user_id

    def event(self, chat: int, user: int, at: dt.datetime, old: str, new: str, update_id: int,
              actor: int | None = None) -> int:
        """An event; update_id counts within the chat (the stored id is offset per chat, so the update identity
        (update_id, event_at) stays unique across chats while the order inside a chat is the one given)."""
        self.user(user)
        return self.c.execute(
            "INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status, actor_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING event_id",
            (chat, user, at, update_id + abs(chat) % 100_000 * 10_000, old, new, actor)).fetchone()[0]

    def spell(self, chat: int, user: int, known_from: dt.datetime, *, join_ev: int | None = None,
              left_at: dt.datetime | None = None, left_ev: int | None = None, reason: str = "left") -> int:
        """An exact spell when join_ev is given, else an inferred start (E6, E9); closed when left_ev is given."""
        exact = join_ev is not None
        return self.c.execute(
            "INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, join_event_id, known_from, "
            "join_route, last_confirmed_at, end_source, end_reason, left_at, left_event_id, left_after, left_before) "
            "VALUES (%(c)s, %(u)s, %(src)s, %(j)s, %(je)s, %(k)s, %(route)s, %(k)s, %(es)s, %(er)s, %(l)s, %(le)s, %(l)s, %(l)s) "
            "RETURNING membership_id",
            {"c": chat, "u": user, "src": "event" if exact else "inferred", "j": known_from if exact else None,
             "je": join_ev, "k": known_from, "route": "unattributed" if exact else None,
             "es": "event" if left_ev else None, "er": reason if left_ev else None,
             "l": left_at if left_ev else None, "le": left_ev}).fetchone()[0]

    def stay(self, chat: int, user: int, joined: dt.datetime, left: dt.datetime | None = None, *,
             update_id: int, left_update_id: int | None = None, reason: str = "left", actor: int = 9002) -> int:
        """join (and leave) events and their exact spell; a kick is done by `actor`."""
        join_ev = self.event(chat, user, joined, "left", "member", update_id)
        left_ev = None
        if left is not None:
            left_ev = self.event(chat, user, left, "member", "kicked" if reason == "kicked" else "left",
                                 left_update_id or update_id + 1, actor=actor if reason == "kicked" else None)
        return self.spell(chat, user, joined, join_ev=join_ev, left_at=left, left_ev=left_ev, reason=reason)
