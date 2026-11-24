"""Job `partitions` (batch thread): R4, tg.maintain_partitions, at start and daily (design.md 7), and the gauge
tgaud_partitions_ahead_days{table}.

Creates member_event months and raw_update / update_receipt days ahead and drops raw data past its retention.
The gauge is measured right after each run (days from the run's time to the end of the last partition): it
stays at that value until the next run, which is what the alert (< 7) is meant to see; a run that stops
happening is caught by tgaud_job_last_success_timestamp_seconds{job="partitions"}. DDL waits for locks at
most LOCK_TIMEOUT: a long transaction on a partition fails the run (retried after RETRY) instead of
queueing every writer behind the DROP.
"""

from __future__ import annotations

import datetime as dt
import logging

import psycopg

from tgaud import metrics, sql
from tgaud.jobs import Job, JobContext

log = logging.getLogger(__name__)

NAME = "partitions"
INTERVAL = 86400.0  # seconds between runs (design.md 7: at start and daily)
RETRY = 300.0
LOCK_TIMEOUT = "10s"
TABLES = ("member_event", "raw_update", "update_receipt")

# The end of the last partition of each parent, read from the partition bounds (the same parsing as
# tg.drop_old_partitions).
AHEAD = """
SELECT parent.relname,
       max((regexp_match(pg_get_expr(c.relpartbound, c.oid), 'TO \\(''([^'']+)''\\)'))[1]::timestamptz) AS covered_to
FROM pg_inherits i
JOIN pg_class parent ON parent.oid = i.inhparent
JOIN pg_namespace n ON n.oid = parent.relnamespace AND n.nspname = 'tg'
JOIN pg_class c ON c.oid = i.inhrelid
WHERE parent.relname = ANY (%(tables)s)
GROUP BY parent.relname
"""


def maintain(conn: psycopg.Connection, as_of: dt.datetime) -> dict[str, float]:
    """Run R4 at `as_of` in the caller's transaction (never commits); returns the days ahead per table."""
    conn.execute("SELECT set_config('lock_timeout', %s, true)", (LOCK_TIMEOUT,))
    res = sql.run(conn, "R4", as_of=as_of)
    log.info("partitions: %s", ", ".join(f"{p} +{c}/-{d}" for p, c, d in res.rows))
    ahead = {t: 0.0 for t in TABLES}
    for table, covered_to in conn.execute(AHEAD, {"tables": list(TABLES)}):
        ahead[table] = round((covered_to - as_of).total_seconds() / 86400, 2)
    return ahead


class PartitionsJob:
    name = NAME

    def __init__(self, ctx: JobContext, *, interval: float = INTERVAL, retry: float = RETRY) -> None:
        self.ctx = ctx
        self.interval = interval
        self.retry = retry
        self._next = 0.0

    def due(self, now: float) -> bool:
        return now >= self._next

    def step(self, conn: psycopg.Connection) -> None:
        now = self.ctx.clock()
        try:
            ahead = maintain(conn, dt.datetime.fromtimestamp(now, dt.timezone.utc))
            conn.commit()
        except Exception:
            self._next = now + self.retry
            raise
        self._next = now + self.interval
        for table, days in ahead.items():
            metrics.PARTITIONS_AHEAD_DAYS.labels(table).set(days)


def make(ctx: JobContext) -> Job:
    return PartitionsJob(ctx)
