"""The process model of `tgaud serve` (the brief's "jobs.py"; a package so each job has its own module).

    ingest thread   tgaud.jobs.ingest: startup(ctx, conn) once, then run(ctx, conn), one long-running loop on
                    the ingest connection (which holds the service lock)
    worker thread   Scheduler over tgaud.jobs.reconciler, .prober, .sampler
    batch thread    Scheduler over tgaud.jobs.chat_day, .audience, .partitions

Each scheduled job module exports `make(ctx: JobContext) -> Job`. A Job has `name`, `due(now) -> bool` and
`step(conn) -> None`: a bounded piece of work (target under 5 s) that commits. The scheduler runs due jobs
round-robin on its own connection; before every step it opens the transaction with the generation fence
(`ctx.fence(conn)`), and commits after it. A step that commits more than once calls `ctx.fence(conn)` again
as the first statement of each further transaction. A failing step is logged, counted in
`tgaud_job_failures_total{job}` and rolled back (its connection discarded and reopened when it was lost); the
other jobs go on. Steps must be idempotent: a commit whose outcome is unknown after a lost connection is
retried. Writers of evidence or spells take `tgaud.history.lock_chats` before reading what they rebuild from.

Service ownership (tgaud.db): serve takes SERVE_LOCK on the ingest connection (a second serve exits 2), then
increments tg.bot_state.serve_generation; a job transaction whose fence sees another value raises
StaleGeneration and the process exits 1, so after a takeover an old process commits nothing.

Shutdown: the SIGTERM/SIGINT handler only sets a flag; the C-level handler also writes to a wakeup pipe, so
the Supervisor thread (started first thing in `tgaud serve`) notices within 100 ms even while the main thread
is blocked. It then sets `ctx.stop`, cancels in-flight SQL and closes the Bot API client, each in a daemon
thread, and calls os._exit(1) if the process is still alive 9 s after the signal (systemd waits 10 s).
Correctness never depends on a graceful stop: every unit of work is a transaction, and an uncommitted one rolls
back when the process dies.
"""

from __future__ import annotations

import importlib
import logging
import os
import select
import signal
import sys
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from types import ModuleType
from typing import Protocol

import psycopg

from tgaud import db, metrics
from tgaud.botapi import BotApi
from tgaud.config import Config

log = logging.getLogger(__name__)

WORKER_JOBS = ("reconciler", "prober", "sampler")
BATCH_JOBS = ("chat_day", "audience", "partitions")
STOP_DEADLINE = 9.0  # seconds from SIGTERM to os._exit
HEALTH_WINDOW = 180.0


class Job(Protocol):
    name: str

    def due(self, now: float) -> bool: ...

    def step(self, conn: psycopg.Connection) -> None: ...


class StartupError(Exception):
    """Raised by ingest.startup to stop serve before anything starts; `code` is the exit code (2 = identity
    or configuration mismatch, per the deploy contract)."""

    def __init__(self, message: str, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class JobContext:
    """What every job gets. `generation` is set once serve owns the service (before any thread starts)."""

    config: Config
    api: BotApi | None
    health: metrics.Health
    stop: threading.Event
    generation: int | None = None
    clock: Callable[[], float] = time.time
    _fatal: Callable[[int, str], None] = field(default=lambda code, reason: None, repr=False)

    def fence(self, conn: psycopg.Connection) -> None:
        """First statement of every job transaction: raises db.StaleGeneration after a takeover."""
        if self.generation is None:
            raise RuntimeError("the serve generation is not claimed yet")
        db.check_generation(conn, self.generation)

    def fatal(self, code: int, reason: str) -> None:
        """Stop the whole process with this exit code (ingest lost its connection, a takeover happened)."""
        self._fatal(code, reason)


class StubJob:
    """A job whose task has not filled it in yet: never due."""

    def __init__(self, name: str) -> None:
        self.name = name

    def due(self, now: float) -> bool:
        return False

    def step(self, conn: psycopg.Connection) -> None:
        raise NotImplementedError(f"job {self.name} is a stub")


class Scheduler(threading.Thread):
    """Runs due jobs round-robin on its own connection until ctx.stop is set."""

    def __init__(self, name: str, jobs: Sequence[Job], connect: Callable[[], psycopg.Connection],
                 ctx: JobContext, *, idle: float = 1.0, max_backoff: float = 30.0) -> None:
        super().__init__(name=name, daemon=True)
        self.jobs = list(jobs)
        self.connect = connect
        self.ctx = ctx
        self.idle = idle
        self.max_backoff = max_backoff
        self.conn: psycopg.Connection | None = None
        self._next = 0

    def run(self) -> None:
        backoff = 1.0
        try:
            while not self.ctx.stop.is_set():
                if self.conn is None:
                    try:
                        self.conn = self.connect()
                    except Exception as e:
                        log.warning("%s: cannot connect to the database (%s); retry in %.0f s",
                                    self.name, type(e).__name__, backoff)
                        self.ctx.stop.wait(backoff)
                        backoff = min(backoff * 2, self.max_backoff)
                        continue
                    backoff = 1.0
                if not self.run_once():
                    self.ctx.stop.wait(self.idle)
        finally:
            self._discard()

    def run_once(self) -> bool:
        """One round over the jobs starting after the last one; True when a step ran."""
        ran = False
        n = len(self.jobs)
        start = self._next
        self._next = (self._next + 1) % n if n else 0
        for k in range(n):
            if self.ctx.stop.is_set() or self.conn is None:
                break
            job = self.jobs[(start + k) % n]
            try:
                due = job.due(self.ctx.clock())
            except Exception:
                log.exception("%s: job %s: due() failed", self.name, job.name)
                metrics.JOB_FAILURES.labels(job.name).inc()
                continue
            if due:
                ran = True
                self._step(job)
        return ran

    def _step(self, job: Job) -> None:
        conn = self.conn
        assert conn is not None
        started = time.monotonic()
        try:
            self.ctx.fence(conn)
            job.step(conn)
            conn.commit()
        except db.StaleGeneration as e:
            log.error("%s: %s; stopping", self.name, e)
            self._rollback_or_discard()
            self.ctx.fatal(1, str(e))
            return
        except Exception as e:
            if self.ctx.stop.is_set():
                log.info("%s: job %s interrupted by shutdown (%s)", self.name, job.name, type(e).__name__)
            else:
                log.exception("%s: job %s step failed", self.name, job.name)
            metrics.JOB_FAILURES.labels(job.name).inc()
            if conn.closed or conn.broken or isinstance(e, psycopg.OperationalError):
                self._discard()
            else:
                self._rollback_or_discard()
            return
        metrics.DB_TRANSACTION_SECONDS.labels(job.name).observe(time.monotonic() - started)
        metrics.JOB_LAST_SUCCESS.labels(job.name).set(time.time())

    def _rollback_or_discard(self) -> None:
        conn = self.conn
        if conn is None:
            return
        try:
            conn.rollback()
        except Exception:
            self._discard()

    def _discard(self) -> None:
        conn, self.conn = self.conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


class Supervisor:
    """Notices SIGTERM/SIGINT (or a fatal request) within 100 ms, runs the stop actions, and ends the process
    with os._exit(1) when it is still alive `deadline` seconds later. Install it from the main thread first."""

    def __init__(self, deadline: float = STOP_DEADLINE) -> None:
        self.deadline = deadline
        self.signalled = False  # written by the signal handler only: a plain attribute, no lock
        self.requested: tuple[int, str] | None = None
        self.stop_started: float | None = None
        self._actions: list[Callable[[], None]] = []
        self._actions_lock = threading.Lock()
        self._r, self._w = os.pipe()
        os.set_blocking(self._r, False)
        os.set_blocking(self._w, False)
        self.thread = threading.Thread(target=self._loop, name="supervisor", daemon=True)

    def install(self) -> "Supervisor":
        signal.signal(signal.SIGTERM, self._handler)
        signal.signal(signal.SIGINT, self._handler)
        signal.set_wakeup_fd(self._w, warn_on_full_buffer=False)
        self.thread.start()
        return self

    def _handler(self, signum: int, frame: object) -> None:
        self.signalled = True

    @property
    def stopping(self) -> bool:
        return self.stop_started is not None

    def request(self, code: int, reason: str) -> None:
        """From any thread except a signal handler: stop with this exit code."""
        if self.requested is None:
            self.requested = (code, reason)
        try:
            os.write(self._w, b"\0")
        except BlockingIOError:
            pass  # the pipe is full: the supervisor wakes up anyway

    def add_stop_action(self, action: Callable[[], None]) -> None:
        """Run `action` (in a daemon thread) when the stop begins; at once if it already began."""
        with self._actions_lock:
            self._actions.append(action)
            started = self.stop_started is not None
        if started:
            _in_background(action)

    def _loop(self) -> None:
        while True:
            ready: list = []
            try:
                ready, _, _ = select.select([self._r], [], [], 0.1)
                if ready:
                    while os.read(self._r, 512):
                        pass
            except BlockingIOError:
                pass
            except OSError:
                time.sleep(0.1)
            if ready and self.requested is None:
                self.signalled = True  # the wakeup byte came from the C-level signal handler
            if self.stop_started is None and (self.signalled or self.requested is not None):
                self.stop_started = time.monotonic()
                why = self.requested[1] if self.requested else "signal"
                log.info("stopping (%s)", why)
                with self._actions_lock:
                    actions = list(self._actions)
                for action in actions:
                    _in_background(action)
            if self.stop_started is not None and time.monotonic() - self.stop_started >= self.deadline:
                log.error("still running %.0f s after the stop began: exiting now", self.deadline)
                for h in logging.getLogger().handlers:
                    try:
                        h.flush()
                    except Exception:
                        pass
                os._exit(1)


def _in_background(action: Callable[[], None]) -> None:
    def run() -> None:
        try:
            action()
        except Exception as e:
            log.debug("stop action failed: %s", type(e).__name__)

    threading.Thread(target=run, name="stop-action", daemon=True).start()


def _cancel(conns: Sequence[psycopg.Connection | None]) -> None:
    """Cancel whatever each connection is running (best effort, one failure does not stop the others)."""
    for conn in conns:
        if conn is None or conn.closed:
            continue
        try:
            cancel = getattr(conn, "cancel_safe", None)
            if cancel is not None:
                cancel(timeout=3.0)
            else:
                conn.cancel()
        except Exception as e:
            log.debug("cancel failed: %s", type(e).__name__)


def load_job_module(name: str) -> ModuleType:
    """tgaud.jobs.<name>; tests replace a job by putting their module into sys.modules under that name."""
    return importlib.import_module(f"{__name__}.{name}")


def serve(config: Config, supervisor: Supervisor, *, health_window: float | None = None) -> int:
    """Run the service until a stop; returns the exit code (0 after a signal, 1 after a fatal error,
    2 when another serve runs or startup refuses, 3 when the schema is behind)."""
    from tgaud.metrics import Health, MetricsServer

    exit_code: list[int] = []
    stop = threading.Event()

    def fatal(code: int, reason: str) -> None:
        if not exit_code:
            exit_code.append(code)
        stop.set()
        supervisor.request(code, reason)

    supervisor.add_stop_action(stop.set)
    health = Health(window=health_window or HEALTH_WINDOW, activation_id=config.activation_id)
    api = BotApi(str(config.bot_token.reveal()), config.bot_api_url) if config.bot_token else None
    if api is not None:
        supervisor.add_stop_action(api.close)
    ctx = JobContext(config=config, api=api, health=health, stop=stop, _fatal=fatal)

    conns: list[psycopg.Connection] = []
    schedulers: list[Scheduler] = []
    threads: list[threading.Thread] = []
    supervisor.add_stop_action(lambda: _cancel([*conns, *(s.conn for s in schedulers)]))

    try:
        ingest_conn = db.connect(config, "ingest")
    except psycopg.Error as e:
        log.error("cannot connect to the database: %s", e)
        return 1
    conns.append(ingest_conn)
    try:
        status = db.schema_status(ingest_conn)
        if status.state == "behind":
            log.error("the database lacks migrations %s of this image: run `tgaud migrate` first",
                      ", ".join(f"{v:03d}" for v in status.missing))
            return 3
        if status.changed:
            log.error("applied migrations %s differ from this image's files", status.changed)
            return 1
        if status.state == "ahead":
            log.warning("the database has newer migrations %s than this image (a rollback?); continuing",
                        status.extra)
        if not db.try_serve_lock(ingest_conn):
            log.error("another tgaud serve runs against this database (or a maintenance command): exiting")
            return 2
        ingest = load_job_module("ingest")
        generation = db.claim_generation(ingest_conn)
        try:
            ingest.startup(ctx, ingest_conn)
        except StartupError as e:
            log.error("%s", e)
            return e.code
        if generation is None:
            generation = db.claim_generation(ingest_conn)
        if generation is None:
            log.error("ingest startup left no tg.bot_state row: cannot claim the service")
            return 1
        ctx.generation = generation
        log.info("serving as generation %d", generation)
        if stop.is_set():
            return exit_code[0] if exit_code else 0

        try:
            server = MetricsServer(config.metrics_listen.host, config.metrics_listen.port, health).start()
        except OSError as e:
            log.error("cannot listen on %s: %s", config.metrics_listen, e)
            return 1
        supervisor.add_stop_action(server.close)
        # The web UI (phase 2) starts here when config.web_listen is set; it calls
        # config.web_proxy_token_or_fail() first.

        for name, jobs in (("worker", WORKER_JOBS), ("batch", BATCH_JOBS)):
            made = [load_job_module(j).make(ctx) for j in jobs]
            schedulers.append(Scheduler(name, made, lambda n=name: db.connect(config, n), ctx))

        def run_ingest() -> None:
            try:
                ingest.run(ctx, ingest_conn)
            except db.StaleGeneration as e:
                fatal(1, str(e))
            except Exception as e:
                if not stop.is_set():
                    log.exception("ingest failed")
                    fatal(1, f"ingest failed: {type(e).__name__}")
                return
            if not stop.is_set():
                fatal(1, "ingest returned")

        ingest_thread = threading.Thread(target=run_ingest, name="ingest", daemon=True)
        threads.append(ingest_thread)
        ingest_thread.start()
        for s in schedulers:
            s.start()
        while not stop.wait(0.2):
            pass
        end = time.monotonic() + STOP_DEADLINE - 3.0  # a commit in flight finishes; a stuck read is abandoned
        for t in [ingest_thread, *schedulers]:
            t.join(max(0.0, end - time.monotonic()))
        return exit_code[0] if exit_code else 0
    except psycopg.Error as e:
        if stop.is_set():
            return exit_code[0] if exit_code else 0
        log.error("database error during startup: %s", e)
        return 1
    finally:
        # A connection whose thread is still inside it is left to the process exit: closing it under a
        # running statement would free libpq state that thread still uses. The server rolls it back.
        if not any(t.is_alive() for t in threads):
            for c in conns:
                _close_in_background(c)


def _close_in_background(conn: psycopg.Connection) -> None:
    t = threading.Thread(target=conn.close, name="close", daemon=True)
    t.start()
    t.join(1.0)


def main_serve(config: Config, supervisor: Supervisor) -> int:
    code = serve(config, supervisor)
    sys.stdout.flush()
    return code
