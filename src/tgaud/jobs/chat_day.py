"""Job `chat_day` (batch thread). Filled by the rollups task: R1, stats.chat_day from the earliest dirty day, every
5 minutes (design.md 8).

Core stub: `make` returns a job that is never due. The scheduler contract is in tgaud.jobs: `due(now)` cheap and
without I/O, `step(conn)` a bounded piece of work (under 5 s) that commits, idempotent, the transaction opened
by the scheduler's generation fence (call `ctx.fence(conn)` again after each extra commit).
"""

from __future__ import annotations

from tgaud.jobs import Job, JobContext, StubJob

NAME = "chat_day"


def make(ctx: JobContext) -> Job:
    return StubJob(NAME)
