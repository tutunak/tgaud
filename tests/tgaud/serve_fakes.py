"""Job modules for serve subprocess tests, installed into sys.modules under the real job names, so the normal
`tgaud serve` wiring (tgaud.jobs.load_job_module) picks them up. TEST_MODE is a comma-separated list:

    poll           ingest: commit offset 1, then long-poll getUpdates (the test's FakeBotServer holds or
                   trickles it), marking health after each successful poll and commit
    fail_head      ingest: polls succeed, but the update's transaction always fails (a stuck head update)
    block_startup  ingest.startup: SELECT pg_sleep(60) on the ingest connection
    stuck_startup  ingest.startup: blocks in Python and ignores the stop (only the hard deadline ends it)
    real_ingest    keep the real tgaud.jobs.ingest (its identity guards); the other modes then don't apply to it
    stalled_log    once serve logs "serving as generation", every log record blocks forever (a full stderr pipe)
    long_batch     batch chat_day: step 1 publishes a stats.rollup_run row; step 2 deletes it and sleeps in SQL
    heartbeat      worker reconciler: due every 0.2 s, a trivial transaction
"""

from __future__ import annotations

import datetime as dt
import logging
import sys
import threading
import time
import types

from tgaud.jobs import JobContext, StubJob


def _ingest(modes: set[str]) -> types.ModuleType:
    m = types.ModuleType("tgaud.jobs.ingest")

    def identify(ctx: JobContext, conn) -> None:
        conn.execute("INSERT INTO tg.bot_state (bot_id, allowed_updates) VALUES (5000, '{chat_member}') "
                     "ON CONFLICT DO NOTHING")
        conn.commit()

    def startup(ctx: JobContext, conn) -> None:
        if "block_startup" in modes:
            conn.execute("SELECT pg_sleep(60)")
        if "stuck_startup" in modes:
            print("startup stuck", flush=True)
            time.sleep(60)

    def run(ctx: JobContext, conn) -> None:
        if "poll" in modes or "fail_head" in modes:
            ctx.fence(conn)
            conn.execute("UPDATE tg.bot_state SET next_offset = 1")
            conn.commit()
            ctx.health.mark_commit()
        while not ctx.stop.is_set():
            if "poll" in modes or "fail_head" in modes:
                ctx.api.get_updates(1, timeout=1)
                ctx.health.mark_poll_ok()
                ctx.fence(conn)
                if "fail_head" in modes:
                    try:
                        conn.execute("UPDATE tg.bot_state SET next_offset = 2")
                        conn.execute("SELECT 1/0")  # the head update fails every time
                    except Exception:
                        conn.rollback()
                        time.sleep(0.2)
                        continue
                conn.commit()
                ctx.health.mark_commit()
            else:
                ctx.stop.wait(0.5)

    m.identify = identify
    m.startup = startup
    m.run = run
    return m


class Heartbeat:
    name = "reconciler"

    def __init__(self) -> None:
        self.next = 0.0

    def due(self, now: float) -> bool:
        return now >= self.next

    def step(self, conn) -> None:
        conn.execute("SELECT 1")
        self.next = time.time() + 0.2


class LongBatch:
    name = "chat_day"

    def __init__(self) -> None:
        self.steps = 0

    def due(self, now: float) -> bool:
        return self.steps < 2

    def step(self, conn) -> None:
        self.steps += 1
        if self.steps == 1:
            now = dt.datetime.now(dt.UTC)
            conn.execute("INSERT INTO stats.rollup_run (rollup, started_at, finished_at, as_of, row_count) "
                         "VALUES ('chat_day', %s, %s, %s, 7)", (now, now, now))
            return
        conn.execute("DELETE FROM stats.rollup_run")
        conn.execute("SELECT pg_sleep(60)")


def _job_module(name: str, factory) -> types.ModuleType:
    m = types.ModuleType(f"tgaud.jobs.{name}")
    m.make = lambda ctx: factory()
    return m


class _StalledSink(logging.Handler):
    """Blocks every record after the service started, like a stderr pipe nobody reads."""

    def __init__(self) -> None:
        super().__init__()
        self.stalled = False

    def emit(self, record: logging.LogRecord) -> None:
        if self.stalled:
            threading.Event().wait()
        if record.getMessage().startswith("serving as generation"):
            print("logging stalls now", flush=True)
            self.stalled = True


def _stall_logging() -> None:
    from tgaud import config

    original = config.setup_logging

    def setup(*args, **kwargs):
        original(*args, **kwargs)
        logging.getLogger().addHandler(_StalledSink())

    config.setup_logging = setup


def install(mode: str) -> None:
    modes = {x for x in mode.split(",") if x}
    if "stalled_log" in modes:
        _stall_logging()
    if "real_ingest" not in modes:
        sys.modules["tgaud.jobs.ingest"] = _ingest(modes)
    if "heartbeat" in modes:
        sys.modules["tgaud.jobs.reconciler"] = _job_module("reconciler", Heartbeat)
    if "long_batch" in modes:
        sys.modules["tgaud.jobs.chat_day"] = _job_module("chat_day", LongBatch)
    for name in ("prober", "sampler", "audience", "partitions"):
        sys.modules.setdefault(f"tgaud.jobs.{name}", _job_module(name, lambda n=name: StubJob(n)))
