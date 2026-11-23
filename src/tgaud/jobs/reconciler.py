"""Job `reconciler` (worker thread). Filled by the reconcile task: applies finished snapshots, probe answers, gaps
(G1) and migrations, and rebuilds spells (design.md 4-6).

Core stub: `make` returns a job that is never due. The scheduler contract is in tgaud.jobs: `due(now)` cheap and
without I/O, `step(conn)` a bounded piece of work (under 5 s) that commits, idempotent, the transaction opened
by the scheduler's generation fence (call `ctx.fence(conn)` again after each extra commit).
"""

from __future__ import annotations

from tgaud.jobs import Job, JobContext, StubJob

NAME = "reconciler"


def make(ctx: JobContext) -> Job:
    return StubJob(NAME)
