"""A worker job harness for the prober and sampler tests: a migrated database with a bot_state row and a claimed
generation, the fake Bot API, a fake clock, and `tick` that runs a job the way tgaud.jobs.Scheduler does (fence,
step, commit) whenever it is due."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg
import pytest

from botapi_fake import FakeBotApi
from history_world import World
from tgaud import config, db, metrics
from tgaud.jobs import Job, JobContext


@pytest.fixture(autouse=True)
def no_closed_log_streams():
    """An earlier test's setup_logging can leave a root handler on pytest's closed stderr capture; the jobs log at
    INFO, which would print 'Logging error' tracebacks. Import this fixture into a test module to drop them."""
    root = logging.getLogger()
    for h in list(root.handlers):
        if isinstance(h, logging.StreamHandler) and getattr(h.stream, "closed", False):
            root.removeHandler(h)
    yield


class Clock:
    def __init__(self, t: datetime | float) -> None:
        self.t = t.timestamp() if isinstance(t, datetime) else float(t)
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self.t

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.t += seconds

    def set(self, t: datetime) -> None:
        with self._lock:
            self.t = t.timestamp()

    @property
    def now(self) -> datetime:
        return datetime.fromtimestamp(self(), timezone.utc)


@dataclass
class Env:
    dsn: str
    ctx: JobContext
    fake: FakeBotApi
    clock: Clock
    conn: psycopg.Connection  # the worker's connection
    w: World  # evidence, on its own connection; commit with w.c.commit()

    def tick(self, job: Job) -> bool:
        """One scheduler visit: fence, step, commit when the job is due."""
        if not job.due(self.clock()):
            return False
        self.ctx.fence(self.conn)
        job.step(self.conn)
        self.conn.commit()
        return True

    def run(self, job: Job, seconds: float, dt: float = 0.05) -> None:
        end = self.clock() + seconds
        while self.clock() < end:
            self.tick(job)
            self.clock.advance(dt)


def make_env(dsn: str, start: datetime, *, rate: float = 5.0) -> Env:
    with psycopg.connect(dsn) as c:
        c.execute("INSERT INTO tg.bot_state (bot_id, allowed_updates) VALUES (5000, '{chat_member}')")
        c.commit()
        gen = db.claim_generation(c)
    fake = FakeBotApi()
    clock = Clock(start)
    cfg = config.load({"TGAUD_PROBE_RATE": str(rate)})
    ctx = JobContext(config=cfg, api=fake.api(), health=metrics.Health(), stop=threading.Event(), generation=gen,
                     clock=clock)
    world = World(psycopg.connect(dsn))
    world.c.commit()  # its partitions and chat: never hold their locks against the job's connection
    return Env(dsn, ctx, fake, clock, psycopg.connect(dsn), world)
