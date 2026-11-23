"""Job `audience` (batch thread). Filled by the rollups task: R2, tg.user_chats, tg.chat_set and the audience
rollups, hourly (design.md 8).

Core stub: `make` returns a job that is never due. The scheduler contract is in tgaud.jobs: `due(now)` cheap and
without I/O, `step(conn)` a bounded piece of work (under 5 s) that commits, idempotent, the transaction opened
by the scheduler's generation fence (call `ctx.fence(conn)` again after each extra commit).
"""

from __future__ import annotations

from tgaud.jobs import Job, JobContext, StubJob

NAME = "audience"


def make(ctx: JobContext) -> Job:
    return StubJob(NAME)
