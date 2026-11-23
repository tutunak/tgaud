"""Job `partitions` (batch thread): tg.maintain_partitions at start and daily, and the gauge
tgaud_partitions_ahead_days (design.md 7, R4); filled by the task that owns it (rollups or ingest, see
docs/tgaud.md).

Core stub: `make` returns a job that is never due. The scheduler contract is in tgaud.jobs: `due(now)` cheap and
without I/O, `step(conn)` a bounded piece of work (under 5 s) that commits, idempotent, the transaction opened
by the scheduler's generation fence (call `ctx.fence(conn)` again after each extra commit).
"""

from __future__ import annotations

from tgaud.jobs import Job, JobContext, StubJob

NAME = "partitions"


def make(ctx: JobContext) -> Job:
    return StubJob(NAME)
