"""Job `audience` (batch thread): R2, tg.user_chats, tg.chat_set and the audience rollups (stats.overlap,
unique_per_chat, chats_per_user), hourly (design.md 8, D13). The SQL is block R2 of queries.sql; tgaud.rollups
runs it.

`refresh(conn)` is the callable: it runs R2 once, in one REPEATABLE READ transaction that it commits itself,
and returns an AudienceResult. The CLI's forget-user calls it after erasing someone; the job calls it hourly
(and at start) with the scheduler's generation fence as the first statement of that transaction. Every
published row carries the `as_of` it was asked for; the stats.rollup_run row 'audience' is written in the same
transaction, so a failing run leaves the previous rollup and its as_of in place. After the commit the job sets
tgaud_rollup_as_of_timestamp_seconds{rollup="audience"}.

The step is one transaction of up to 24-34 s at full scale (verify.md 5, the budget is minutes); it is the
only unit that cannot be cut, because the three projections must describe one user_chats.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable

import psycopg
from psycopg import IsolationLevel

from tgaud import metrics, rollups
from tgaud.jobs import Job, JobContext

log = logging.getLogger(__name__)

NAME = "audience"
INTERVAL = 3600.0  # seconds between runs (design.md 8)
RETRY = 300.0  # seconds before the next try after a failure


def refresh(conn: psycopg.Connection, as_of: dt.datetime | None = None, *,
            fence: Callable[[psycopg.Connection], None] | None = None) -> rollups.AudienceResult:
    """R2 in its own REPEATABLE READ transaction, committed here. The connection must not be in autocommit.
    An open transaction of the caller (the scheduler's fence read) is committed first: the isolation level can
    only be set between transactions. `fence`, if given, runs first in the new transaction (the generation
    fence of a job step); a failure rolls the transaction back and re-raises."""
    if conn.autocommit:
        raise ValueError("audience.refresh needs a connection that is not in autocommit mode")
    as_of = as_of or dt.datetime.now(dt.timezone.utc)
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        conn.commit()
    conn.isolation_level = IsolationLevel.REPEATABLE_READ
    try:
        if fence is not None:
            fence(conn)
        result = rollups.refresh_audience(conn, as_of)
        conn.commit()
        return result
    except BaseException:
        conn.rollback()
        raise
    finally:
        if conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE:  # not on a lost connection
            conn.isolation_level = None


class AudienceJob:
    name = NAME

    def __init__(self, ctx: JobContext, *, interval: float = INTERVAL, retry: float = RETRY) -> None:
        self.ctx = ctx
        self.interval = interval
        self.retry = retry
        self._next = 0.0  # due at once after a start

    def due(self, now: float) -> bool:
        return now >= self._next

    def step(self, conn: psycopg.Connection) -> None:
        now = self.ctx.clock()
        as_of = dt.datetime.fromtimestamp(now, dt.timezone.utc)
        try:
            result = refresh(conn, as_of, fence=self.ctx.fence)
        except Exception:
            self._next = now + self.retry
            raise
        self._next = now + self.interval
        metrics.ROLLUP_AS_OF.labels(NAME).set(as_of.timestamp())
        log.info("audience: %d pairs from %d chat sets (%s refresh)", result.pairs, result.chat_sets,
                 result.refreshed)


def make(ctx: JobContext) -> Job:
    return AudienceJob(ctx)
