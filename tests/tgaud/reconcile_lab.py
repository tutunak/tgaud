"""The reconciler on the test database: snapshots written the way the collect script writes them (role
collect_writer, design.md 5), and the job's units run until there is no work.

`Lab` extends history_world.World (events, probes, gaps) with `collect` and `run`. It never commits unless asked
(`run(commit=True)`, one transaction per unit as the scheduler runs them).
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone

import psycopg

from history_world import CHAT, COLS, World, at  # noqa: F401 (re-exported for the tests)
from tgaud import config, history, metrics
from tgaud.jobs import JobContext
from tgaud.jobs.reconciler import Reconciler

CAUGHT_UP = datetime(2100, 1, 1, tzinfo=timezone.utc)  # ingest has drained the queue after every snapshot

SIGHTING_COLS = ("chat_id, user_id, first_seen_at, first_snapshot_id, last_seen_at, last_snapshot_id, times_seen, "
                 "joined_date, interior_seen_at")


def job(env: dict | None = None) -> Reconciler:
    ctx = JobContext(config=config.load(env or {}), api=None, health=metrics.Health(), stop=threading.Event())
    return Reconciler(ctx)


class Lab(World):
    def __init__(self, conn: psycopg.Connection, chat_id: int = CHAT, *, caught_up: datetime | None = CAUGHT_UP,
                 env: dict | None = None, **kw) -> None:
        super().__init__(conn, chat_id, **kw)
        conn.execute("""INSERT INTO tg.bot_state (bot_id, allowed_updates, caught_up_at) VALUES (5000, '{chat_member}', %s)
                        ON CONFLICT (bot_id) DO UPDATE SET caught_up_at = excluded.caught_up_at""", (caught_up,))
        self.job = job(env)

    def collect(self, started: datetime, listed: dict[int, tuple] | None = None, *, finished: datetime | None = None,
                reported: int | None = None, reported_end: int | None = None, fetched: int | None = None,
                complete: bool = True, done: bool = True, chat: int | None = None, rows: list[dict] | None = None,
                method: str = "listing") -> int:
        """A snapshot as the collect script writes it: {user: (seen_at, participant date)} or full `rows`."""
        chat = chat or self.chat_id
        rows = list(rows or [])
        for u, spec in (listed or {}).items():
            seen, hint = (spec, None) if isinstance(spec, datetime) else spec
            rows.append({"user_id": u, "seen_at": seen, "joined_date": hint})
        c = self.c
        c.execute("SET LOCAL ROLE collect_writer")
        try:
            sid = c.execute("""INSERT INTO tg.snapshot (chat_id, started_at, method, reported_at_start)
                               VALUES (%s, %s, %s, %s) RETURNING snapshot_id""",
                            (chat, started, method, len(rows) if reported is None else reported)).fetchone()[0]
            for r in rows:
                c.execute("""INSERT INTO tg.snapshot_member (snapshot_id, user_id, seen_at, joined_date, inviter_id,
                                                             is_bot, is_deleted, is_premium, username, first_name,
                                                             last_name)
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                          (sid, r["user_id"], r["seen_at"], r.get("joined_date"), r.get("inviter_id"),
                           r.get("is_bot", False), r.get("is_deleted", False), r.get("is_premium"),
                           r.get("username"), r.get("first_name"), r.get("last_name")))
            if done:
                last = max([started, *(r["seen_at"] for r in rows)])
                n = len(rows) if fetched is None else fetched
                end = reported_end if reported_end is not None else len(rows) if reported is None else reported
                c.execute("""UPDATE tg.snapshot SET finished_at = %s, reported_at_end = %s, fetched = %s, complete = %s
                             WHERE snapshot_id = %s""", (finished or last, end, n, complete, sid))
        finally:
            c.execute("RESET ROLE")
        return sid

    def fixed_chunk(self, n: int) -> None:
        """Chunks of exactly n users (no adapting to the measured time)."""
        self.job.chunk = n
        self.job._adapt = lambda size, elapsed: None

    def run(self, *, commit: bool = False, limit: int = 5000, stop_after=None) -> list[str]:
        """Run the job's units until none has work (or `stop_after(units)` says stop); returns the units run."""
        units: list[str] = []
        while True:
            unit = self.job.run_unit(self.c)
            if commit:
                self.c.commit()
            if unit is None:
                return units
            units.append(unit)
            assert len(units) < limit, units[-20:]
            if stop_after and stop_after(units):
                return units

    # --- reads ------------------------------------------------------------------------------------------------
    def sightings(self, user: int | None = None) -> list[tuple]:
        where, args = ("WHERE chat_id = %s", (self.chat_id,)) if user is None else \
            ("WHERE chat_id = %s AND user_id = %s", (self.chat_id, user))
        rows = self.c.execute(f"SELECT {SIGHTING_COLS} FROM tg.sighting {where} ORDER BY user_id, first_seen_at",
                              args).fetchall()
        return [tuple(_utc(v) for v in r) for r in rows]

    def snapshot(self, sid: int) -> dict:
        cur = self.c.execute("SELECT * FROM tg.snapshot WHERE snapshot_id = %s", (sid,))
        row = cur.fetchone()
        return dict(zip([d.name for d in cur.description], row)) if row else {}

    def pending(self, chat: int | None = None) -> list[tuple]:
        return self.c.execute("""SELECT user_id, reason, snapshot_id, absent_since FROM tg.probe
                                 WHERE chat_id = %s AND outcome = 'pending' ORDER BY user_id""",
                              (chat or self.chat_id,)).fetchall()

    def totals(self, sid: int) -> dict:
        cur = self.c.execute("""SELECT count(*) AS chunks, sum(listed) AS listed, sum(seeded) AS seeded,
                                       sum(missed_joins) AS missed_joins, sum(confirmed) AS confirmed,
                                       sum(absent_candidates) AS absent_candidates, sum(probes_queued) AS probes_queued,
                                       sum(conflicts) AS conflicts
                                FROM tg.reconcile_run WHERE snapshot_id = %s""", (sid,))
        return {d.name: (int(v) if v is not None else 0) for d, v in zip(cur.description, cur.fetchone())}

    def all_pairs(self) -> list[tuple[int, int]]:
        return self.c.execute("""SELECT chat_id, user_id FROM tg.member_event UNION SELECT chat_id, user_id
                                 FROM tg.sighting UNION SELECT chat_id, user_id FROM tg.probe
                                 UNION SELECT chat_id, user_id FROM tg.membership""").fetchall()

    def rebuild_all(self) -> history.RebuildResult:
        return history.rebuild_pairs(self.c, self.all_pairs())


def _utc(v):
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc)
    if isinstance(v, list):
        return [_utc(x) for x in v]
    return v
