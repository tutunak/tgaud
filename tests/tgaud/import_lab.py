"""Support for the Import page tests: synthetic tgstats.db files written by the tgstats code itself, a tgaud
database with tracked chats, a web app and a listener on it, and multipart bodies.

    TgstatsFile(path)   .snapshot(chat, {user: seen}, started, ...) writes a new-format snapshot through
                        tgstats.db.add_snapshot (the collector's own writer); .legacy(...) one as older versions
                        wrote it; .raw (the sqlite3 connection) for files tgstats would never write
    World(dsn)          the tgaud side: tracked chats A, B and C, untracked U, bot_state caught up; reads back
                        snapshots, members and spells
    Lab(pg, dsn, tmp)   World + the web app on its own fenced connection (as serve runs it), a spool directory,
                        `run(path)` (the importer), `post(body)` (POST /import through the WSGI app),
                        `reconcile()` (the reconciler job through the scheduler's real step loop)
    multipart(data)     (content type, body) of the page's form
"""

from __future__ import annotations

import datetime as dt
import io
import sqlite3
import threading
import uuid as uuidlib
from pathlib import Path
from wsgiref.util import setup_testing_defaults

import psycopg
from test_web import HOST, PROXY_TOKEN, Page

from tgaud import config, db, history, metrics
from tgaud.jobs import JobContext, Scheduler, reconciler
from tgaud.web import WebApp, WebDb, importer
from tgstats import db as tgdb

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 11, 20, 9, 0, tzinfo=UTC)
A, B, U, C = -1007000000001, -1007000000002, -1007000000003, -1007000000004  # A, B, C tracked; U not
BOUNDARY = "----tgaudTestBoundary7MA4YWxkTrZu0gW"


def at(minutes: float = 0) -> dt.datetime:
    return T0 + dt.timedelta(minutes=minutes)


def iso(t: dt.datetime) -> str:
    return t.astimezone(UTC).isoformat(timespec="microseconds")


class TgstatsFile:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.raw = tgdb.connect(self.path)

    def snapshot(self, chat: int, members: dict[int, dt.datetime | dict], started: dt.datetime, *,
                 finished: dt.datetime | None = None, reported: int | None = None, reported_end: int | None = -1,
                 complete: bool | None = None, method: str = "listing", uuid: str | None = None,
                 title: str = "file title", names: bool = True) -> str:
        """A new-format snapshot: {user: seen_at, or {seen_at, joined_date, inviter_id, ..., first_name}}."""
        rows = []
        for u, spec in members.items():
            spec = {"seen_at": spec} if isinstance(spec, dt.datetime) else dict(spec)
            user = tgdb.UserRow(u, spec.get("username", f"user{u}" if names else None),
                                spec.get("first_name", f"First{u}" if names else None),
                                spec.get("last_name"), spec.get("is_bot", False), spec.get("is_deleted", False))
            rows.append(tgdb.Member(user, iso(spec["seen_at"]),
                                    iso(spec["joined_date"]) if spec.get("joined_date") else None,
                                    spec.get("inviter_id"), spec.get("via_request"),
                                    iso(spec["subscription_until"]) if spec.get("subscription_until") else None,
                                    spec.get("is_premium", False)))
        finished = finished or max([started, *(dt.datetime.fromisoformat(r.seen_at) for r in rows)])
        start = len(rows) if reported is None else reported
        end = start if reported_end == -1 else reported_end
        u = uuid or str(uuidlib.uuid4())
        tgdb.add_snapshot(self.raw, tgdb.ChatRow(chat, "supergroup", title, None), rows,
                          max((c for c in (start, end) if c is not None), default=None), complete=complete,
                          collection=tgdb.Collection(u, iso(started), iso(finished), method, start, end))
        return u

    def legacy(self, chat: int, users: list[int], taken_at: dt.datetime) -> int:
        """A snapshot as tgstats before the snapshot contract wrote it (no uuid, times or observations)."""
        return tgdb.add_snapshot(self.raw, tgdb.ChatRow(chat, "channel", "old", None),
                                 [tgdb.UserRow(u, None, f"F{u}", None) for u in users], len(users),
                                 taken_at=taken_at.isoformat(timespec="seconds")).id

    def close(self) -> Path:
        self.raw.close()
        return self.path


def legacy_file(path: Path, chats: dict[int, list[int]]) -> Path:
    """A file exactly as the older tgstats wrote it: the base tables only, never migrated."""
    raw = sqlite3.connect(path)
    raw.executescript(tgdb.SCHEMA)
    for i, (chat, users) in enumerate(chats.items()):
        raw.execute("INSERT INTO chats VALUES (?, 'channel', ?, NULL)", (chat, f"old {i}"))
        raw.executemany("INSERT OR IGNORE INTO users (id, first_name) VALUES (?, ?)", [(u, f"F{u}") for u in users])
        sid = raw.execute("INSERT INTO snapshots (chat_id, taken_at, fetched, reported, complete) VALUES (?, ?, ?, ?, 0)",
                          (chat, f"2026-11-14T1{i}:00:00+00:00", len(users), len(users) + 5)).lastrowid
        raw.executemany("INSERT INTO memberships VALUES (?, ?)", [(sid, u) for u in users])
    raw.commit()
    raw.close()
    return path


class World:
    """The tgaud database of a test (committed: the web app has its own connection)."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        with psycopg.connect(dsn) as c:
            c.execute("SELECT tg.ensure_partitions('member_event', 'month', '2026-01-01', '2027-07-01')")
            c.execute("""INSERT INTO tg.bot_state (bot_id, allowed_updates, caught_up_at)
                         VALUES (5000, '{chat_member}', '2100-01-01+00') ON CONFLICT (bot_id) DO NOTHING""")
            for chat, tracked in ((A, True), (B, True), (C, True), (U, False)):
                self.chat(c, chat, tracked)

    @staticmethod
    def chat(c: psycopg.Connection, chat_id: int, tracked: bool = True, title: str | None = None) -> None:
        c.execute("""INSERT INTO tg.chat (chat_id, chat_type, title, tracked, bot_status, first_seen_at)
                     VALUES (%s, 'supergroup', %s, %s, 'administrator', '2026-01-01+00') ON CONFLICT DO NOTHING""",
                  (chat_id, title or f"tgaud {chat_id}", tracked))

    def q(self, sql: str, *args) -> list[tuple]:
        with psycopg.connect(self.dsn) as c:
            return c.execute(sql, args).fetchall()

    def snapshots(self, chat: int | None = None) -> list[dict]:
        with psycopg.connect(self.dsn) as c:
            cur = c.execute("SELECT * FROM tg.snapshot WHERE %(c)s::bigint IS NULL OR chat_id = %(c)s "
                            "ORDER BY snapshot_id", {"c": chat})
            return [dict(zip([d.name for d in cur.description], r)) for r in cur.fetchall()]

    def members(self, snapshot_id: int) -> dict[int, dict]:
        with psycopg.connect(self.dsn) as c:
            cur = c.execute("SELECT * FROM tg.snapshot_member WHERE snapshot_id = %s ORDER BY user_id", (snapshot_id,))
            return {r[1]: dict(zip([d.name for d in cur.description], r)) for r in cur.fetchall()}


def multipart(data: bytes, *, name: str = "file", filename: str = "tgstats.db",
              boundary: str = BOUNDARY) -> tuple[str, bytes]:
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\n"
            "Content-Type: application/octet-stream\r\n\r\n").encode() + data + f"\r\n--{boundary}--\r\n".encode()
    return f"multipart/form-data; boundary={boundary}", body


def config_for(pg, dsn: str, **env: str) -> config.Config:
    import psycopg.conninfo

    from clidb import LINK_KEY, TOKEN
    name = psycopg.conninfo.conninfo_to_dict(dsn)["dbname"]
    return config.load({"TGAUD_DATABASE_URL": pg.tenant_url(name), "TGAUD_DATABASE_PASSWORD": pg.tenant_password,
                        "TGAUD_BOT_TOKEN": TOKEN, "TGAUD_LINK_HMAC_KEY": LINK_KEY,
                        "TGAUD_WEB_LISTEN": "127.0.0.1:8097", "TGAUD_WEB_PROXY_TOKEN": PROXY_TOKEN,
                        "TGAUD_ACTIVATION_ID": "act-import", **env})


class Lab(World):
    def __init__(self, pg, dsn: str, tmp: Path, **env: str) -> None:
        super().__init__(dsn)
        self.tmp = Path(tmp)
        self.cfg = config_for(pg, dsn, **env)
        with psycopg.connect(dsn) as c:
            generation = db.claim_generation(c)
        self.ctx = JobContext(config=self.cfg, api=None, health=metrics.Health(), stop=threading.Event(),
                              generation=generation)
        self.database = WebDb(self.connect, fence=self.ctx.fence)
        self.spool = self.tmp / "spool"
        self.spool.mkdir(exist_ok=True)
        self.app = WebApp(self.cfg, self.database, PROXY_TOKEN.encode(), spool_dir=str(self.spool))
        self._files = 0

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(self.cfg.dsn("web"), autocommit=False)

    def file(self, name: str | None = None) -> TgstatsFile:
        self._files += 1
        return TgstatsFile(self.tmp / (name or f"tgstats-{self._files}.db"))

    def run(self, path: Path, **kw) -> importer.Report:
        return importer.run(self.database, path, store_names=self.cfg.store_names, **kw)

    def outcomes(self, report: importer.Report) -> dict[int, tuple[str, int | None]]:
        return {o.chat_id: (o.status, o.written) for o in report.outcomes}

    def post(self, body: bytes, content_type: str | None = None, *, path: str = "/import",
             origin: str | None = f"https://{HOST}", token: str | None = PROXY_TOKEN) -> Page:
        """POST through the WSGI app (the listener's transport is tested in test_import_listener.py)."""
        if content_type is None:
            content_type, body = multipart(body)
        environ: dict = {}
        setup_testing_defaults(environ)
        environ.update({"REQUEST_METHOD": "POST", "PATH_INFO": path, "QUERY_STRING": "", "HTTP_HOST": HOST,
                        "wsgi.input": io.BytesIO(body), "CONTENT_LENGTH": str(len(body)),
                        "CONTENT_TYPE": content_type, "HTTP_X_FORWARDED_PROTO": "https"})
        if token is not None:
            environ["HTTP_X_TGAUD_PROXY_TOKEN"] = token
        if origin is not None:
            environ["HTTP_ORIGIN"] = origin
        got: dict = {}

        def start_response(status, hdrs):
            got["status"], got["headers"] = int(status.split(" ", 1)[0]), dict(hdrs)

        out = b"".join(self.app(environ, start_response))
        return Page(got["status"], got["headers"], out)

    def upload(self, path: Path) -> Page:
        return self.post(Path(path).read_bytes())

    def reconcile(self, limit: int = 500) -> int:
        """The reconciler job through the scheduler's step loop (fenced, committed per step) until it is idle."""
        job = reconciler.make(self.ctx)
        s = Scheduler("worker", [job], lambda: psycopg.connect(self.dsn), self.ctx)
        s.conn = s.connect()
        try:
            for n in range(limit):
                s.run_once()
                if job._next_look > 0:  # its last step found no work
                    return n
        finally:
            s._discard()
        raise AssertionError("the reconciler never ran out of work")

    def spells(self, chat: int, user: int) -> list[history.Spell]:
        from history_world import COLS
        rows = self.q(f"SELECT {COLS} FROM tg.membership WHERE chat_id = %s AND user_id = %s "
                      "ORDER BY known_from", chat, user)
        return [history.Spell(*(v.astimezone(UTC) if isinstance(v, dt.datetime) else v for v in r)) for r in rows]

    def event(self, chat: int, user: int, t: dt.datetime, old: str, new: str) -> None:
        """A chat_member event as ingest stores it, and the pair rebuilt (ingest's incremental rule)."""
        with psycopg.connect(self.dsn) as c:
            history.lock_chats(c, [chat])
            uid = c.execute("SELECT coalesce(max(update_id), 800000) + 1 FROM tg.member_event").fetchone()[0]
            c.execute("INSERT INTO tg.tg_user (user_id, first_seen_at, last_seen_at) VALUES (%s, %s, %s) "
                      "ON CONFLICT DO NOTHING", (user, t, t))
            c.execute("INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status) "
                      "VALUES (%s, %s, %s, %s, %s, %s)", (chat, user, t, uid, old, new))
            history.rebuild_pairs(c, [(chat, user)])

    def close(self) -> None:
        self.database.close()


def migrate_chat(dsn: str, old: int, new: int, migrated_at: dt.datetime, *, new_tracked: bool,
                 hold: threading.Event | None = None, locked: threading.Event | None = None) -> None:
    """A migration as ingest applies it (jobs.ingest._migrate): both chats locked, the alias, then M1 (the new chat
    tracked: tg.move_chat_history and a rebuild into it) or M2 (the old id's pairs rebuilt, closed by the
    migration). With `hold`, it waits for that event after taking the locks (and sets `locked`)."""
    with psycopg.connect(dsn) as c:
        history.lock_chats(c, [old, new])
        if locked is not None:
            locked.set()
        if hold is not None:
            assert hold.wait(30)
        World.chat(c, new, new_tracked)
        c.execute("INSERT INTO tg.chat_alias (old_chat_id, chat_id, migrated_at, moved_at) VALUES (%s, %s, %s, NULL)",
                  (old, new, migrated_at))
        users = [r[0] for r in c.execute(
            "SELECT user_id FROM tg.member_event WHERE chat_id = %(c)s UNION SELECT user_id FROM tg.sighting "
            "WHERE chat_id = %(c)s UNION SELECT user_id FROM tg.membership WHERE chat_id = %(c)s", {"c": old})]
        if new_tracked:
            c.execute("SELECT tg.move_chat_history(%s, %s, %s)", (old, new, migrated_at))
            history.rebuild_pairs(c, [(new, u) for u in sorted(users)])
        else:
            history.rebuild_pairs(c, [(old, u) for u in sorted(users)])
