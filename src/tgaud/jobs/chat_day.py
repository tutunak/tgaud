"""Job `chat_day` (batch thread): R1, stats.chat_day recomputed from the earliest dirty day (or 2 days back),
every 5 minutes (design.md 8, D13). The SQL is block R1 of queries.sql; tgaud.rollups runs it.

A cycle is a list of steps, each one transaction that commits by itself:

  * the plan (cheap): which tracked chat needs how many days;
  * one set-based step for every chat whose oldest target day is more than FULL_DAYS back (a cold start, far-back
    dirty days), because a pass per chat over a big history costs more than one pass over all of it;
  * then one step per chat, so a step stays short and the batch thread's other jobs are not starved.

Every step of a cycle recomputes "today" from the cycle's as_of. The last step also writes the stats.rollup_run
row (as_of = the cycle's, row_count = rows written in the cycle) and, after its commit, sets
tgaud_rollup_as_of_timestamp_seconds{rollup="chat_day"}: a cycle that did not finish publishes nothing, so
the gauge ages and the alert (> 20 min) fires. A failing step is rolled back (the chat keeps its old rows and its
dirty marker), goes to the end of the queue, and the job retries after RETRY seconds. Markers are consumed only
for what a step recomputed: a marker written meanwhile has a new version and survives (tgaud.rollups).
"""

from __future__ import annotations

import datetime as dt
import logging

import psycopg

from tgaud import metrics, rollups
from tgaud.jobs import Job, JobContext

log = logging.getLogger(__name__)

NAME = "chat_day"
INTERVAL = 300.0  # seconds between cycle starts (design.md 8)
RETRY = 60.0  # seconds before the next step after a failure
FULL_DAYS = rollups.FULL_DAYS

PUBLISH_RUN = ("INSERT INTO stats.rollup_run (rollup, started_at, finished_at, as_of, row_count) "
               "VALUES ('chat_day', %(started)s, clock_timestamp(), %(as_of)s, %(rows)s)")


class ChatDayJob:
    name = NAME

    def __init__(self, ctx: JobContext, *, interval: float = INTERVAL, retry: float = RETRY) -> None:
        self.ctx = ctx
        self.interval = interval
        self.retry = retry
        self._next_cycle = 0.0  # due at once after a start
        self._not_before = 0.0  # after a failure
        self._queue: list[list[int]] = []  # steps of the running cycle
        self._as_of: dt.datetime | None = None
        self._started: dt.datetime | None = None
        self._rows = 0

    @property
    def pending(self) -> int:
        """Steps left in the running cycle."""
        return len(self._queue)

    def due(self, now: float) -> bool:
        if now < self._not_before:
            return False
        return bool(self._queue) or now >= self._next_cycle

    def step(self, conn: psycopg.Connection) -> None:
        now = self.ctx.clock()
        if self._queue and self._as_of is not None and now - self._as_of.timestamp() > 3 * self.interval:
            log.warning("chat_day: the cycle of %s did not finish (%d steps left): planning again",
                        self._as_of.isoformat(), len(self._queue))
            self._queue = []
        try:
            if not self._queue:
                self._plan(conn, now)
            else:
                self._run_step(conn)
        except Exception:
            conn.rollback()
            if len(self._queue) > 1:
                self._queue.append(self._queue.pop(0))
            self._not_before = now + self.retry
            raise

    def _plan(self, conn: psycopg.Connection, now: float) -> None:
        self._as_of = dt.datetime.fromtimestamp(now, dt.timezone.utc)
        self._started = self._as_of
        self._rows = 0
        today = self._as_of.date()
        targets = rollups.plan_chat_days(conn, self._as_of)
        self._next_cycle = now + self.interval
        far = [c for c, d in targets.items() if (today - d).days > FULL_DAYS]
        near = [[c] for c in targets if c not in far]
        self._queue = ([far] if far else []) + near
        conn.commit()
        if not self._queue:  # nothing tracked: the empty rollup is as fresh as any
            self._publish(conn)

    def _run_step(self, conn: psycopg.Connection) -> None:
        assert self._as_of is not None
        chats = self._queue[0]
        res = rollups.refresh_chat_days(conn, chats, self._as_of)
        self._rows += res.rows
        last = len(self._queue) == 1
        if last:
            conn.execute(PUBLISH_RUN, {"started": self._started, "as_of": self._as_of, "rows": self._rows})
        conn.commit()
        self._queue.pop(0)
        log.debug("chat_day: %s step, %d chats, %d rows, %d markers cleared, %d steps left",
                  res.mode, res.chats, res.rows, res.markers_cleared, len(self._queue))
        if last:
            metrics.ROLLUP_AS_OF.labels(NAME).set(self._as_of.timestamp())

    def _publish(self, conn: psycopg.Connection) -> None:
        self.ctx.fence(conn)  # a second transaction of this step
        conn.execute(PUBLISH_RUN, {"started": self._started, "as_of": self._as_of, "rows": 0})
        conn.commit()
        metrics.ROLLUP_AS_OF.labels(NAME).set(self._as_of.timestamp())


def make(ctx: JobContext) -> Job:
    return ChatDayJob(ctx)
