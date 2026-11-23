"""The scheduler, the service lock, the generation fence and maintenance, in process against the test database."""

import threading
import time

import psycopg
import pytest

from tgaud import config, db, metrics
from tgaud.jobs import JobContext, Scheduler, StubJob


class Recorder:
    """A job that is always due and runs `body(conn)`."""

    def __init__(self, name, body=None, log=None):
        self.name = name
        self.body = body
        self.log = log if log is not None else []

    def due(self, now):
        return True

    def step(self, conn):
        self.log.append(self.name)
        if self.body:
            self.body(conn)


@pytest.fixture
def svc(migrated_dsn):
    """A database with a bot_state row and a claimed generation; returns (dsn, ctx, fatal calls)."""
    with psycopg.connect(migrated_dsn) as c:
        c.execute("INSERT INTO tg.bot_state (bot_id, allowed_updates) VALUES (5000, '{chat_member}')")
        c.execute("CREATE TABLE public.job_log (job text, n int)")
        c.commit()
        gen = db.claim_generation(c)
    fatal = []
    stop = threading.Event()

    def on_fatal(code, reason):
        fatal.append((code, reason))
        stop.set()

    cfg = config.load({})
    ctx = JobContext(config=cfg, api=None, health=metrics.Health(), stop=stop, generation=gen, _fatal=on_fatal)
    return migrated_dsn, ctx, fatal


def write(name):
    def body(conn):
        conn.execute("INSERT INTO public.job_log VALUES (%s, 1)", (name,))
    return body


def rows(dsn):
    with psycopg.connect(dsn) as c:
        return [r[0] for r in c.execute("SELECT job FROM public.job_log ORDER BY job").fetchall()]


def test_round_robin_over_due_jobs_and_commit(svc):
    dsn, ctx, _ = svc
    log = []
    jobs = [Recorder("a", write("a"), log), Recorder("b", write("b"), log), StubJob("never")]
    s = Scheduler("t", jobs, lambda: psycopg.connect(dsn), ctx)
    s.conn = s.connect()
    assert s.run_once() is True
    assert s.run_once() is True
    assert log == ["a", "b", "b", "a"]  # each round starts one job later
    assert rows(dsn) == ["a", "a", "b", "b"]
    assert metrics.REGISTRY.get_sample_value("tgaud_job_last_success_timestamp_seconds", {"job": "a"}) > 0
    assert metrics.REGISTRY.get_sample_value("tgaud_db_transaction_seconds_count", {"job": "a"}) >= 2
    s._discard()


def test_a_failing_step_is_rolled_back_counted_and_the_others_go_on(svc):
    dsn, ctx, fatal = svc

    def fail(conn):
        conn.execute("INSERT INTO public.job_log VALUES ('bad', 1)")
        raise ValueError("step failed")

    before = metrics.REGISTRY.get_sample_value("tgaud_job_failures_total", {"job": "bad"}) or 0
    s = Scheduler("t", [Recorder("bad", fail), Recorder("good", write("good"))], lambda: psycopg.connect(dsn), ctx)
    s.conn = s.connect()
    s.run_once()
    s.run_once()
    assert rows(dsn) == ["good", "good"]
    assert metrics.REGISTRY.get_sample_value("tgaud_job_failures_total", {"job": "bad"}) == before + 2
    assert s.conn is not None and not fatal
    s._discard()


def test_a_lost_connection_is_discarded_and_reopened(svc, pg):
    dsn, ctx, fatal = svc
    killed = []

    def lose_connection(conn):
        conn.execute("INSERT INTO public.job_log VALUES ('lost', 1)")
        if not killed:
            killed.append(conn.info.backend_pid)
            with pg.admin() as a:
                a.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
            conn.execute("SELECT 1")  # the connection is gone now

    jobs = [Recorder("lost", lose_connection), Recorder("other", write("other"))]
    s = Scheduler("t", jobs, lambda: psycopg.connect(dsn), ctx, idle=0.05)
    s.start()
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and rows(dsn).count("other") < 2:
        time.sleep(0.1)
    ctx.stop.set()
    s.join(10)
    assert not s.is_alive()
    got = rows(dsn)
    assert got.count("other") >= 2
    assert got.count("lost") >= 1  # later rounds, on the new connection, commit; the killed write is absent
    assert not fatal


def test_a_stale_reconnecting_worker_runs_no_step_after_a_takeover(svc, pg):
    dsn, ctx, fatal = svc
    log = []
    s = Scheduler("t", [Recorder("job", write("job"), log)], lambda: psycopg.connect(dsn), ctx, idle=0.05)
    s.conn = s.connect()
    s.run_once()
    assert log == ["job"]
    # the old worker loses its connection while a new serve takes over
    with pg.admin() as a:
        a.execute("SELECT pg_terminate_backend(%s)", (s.conn.info.backend_pid,))
    with psycopg.connect(dsn) as other:
        assert db.claim_generation(other) == ctx.generation + 1
    s._discard()
    s.start()  # reconnects, fences, finds the new generation
    s.join(20)
    assert not s.is_alive()
    assert log == ["job"]
    assert rows(dsn) == ["job"]
    assert fatal and fatal[0][0] == 1 and "took over" in fatal[0][1]


def test_takeover_waits_for_in_flight_job_transactions(svc):
    dsn, ctx, _ = svc
    with psycopg.connect(dsn) as job, psycopg.connect(dsn) as other:
        ctx.fence(job)  # an in-flight job transaction
        job.execute("INSERT INTO public.job_log VALUES ('inflight', 1)")
        out = {}
        t = threading.Thread(target=lambda: out.setdefault("gen", db.claim_generation(other)))
        t.start()
        time.sleep(0.5)
        assert t.is_alive(), "the takeover's FOR UPDATE must wait for the job transaction"
        job.commit()
        t.join(10)
        assert out["gen"] == ctx.generation + 1
        with pytest.raises(db.StaleGeneration):
            ctx.fence(job)
        job.rollback()
    assert rows(dsn) == ["inflight"]


def test_the_fence_never_blocks_ingest_offset_updates(svc):
    dsn, ctx, _ = svc
    with psycopg.connect(dsn) as job, psycopg.connect(dsn) as ingest:
        ctx.fence(job)  # a long batch transaction holds the fence
        ingest.execute("SET lock_timeout = '2s'")
        ctx.fence(ingest)
        ingest.execute("UPDATE tg.bot_state SET next_offset = next_offset + 1")
        ingest.commit()
        job.rollback()


def test_serve_lock_and_maintenance(svc):
    dsn, ctx, _ = svc
    with psycopg.connect(dsn) as serve, psycopg.connect(dsn) as second, psycopg.connect(dsn) as maint:
        assert db.try_serve_lock(serve) is True
        assert db.try_serve_lock(second) is False
        with pytest.raises(db.MaintenanceBlocked, match="stop the service first"):
            with db.maintenance(maint):
                pass
        serve.execute("SELECT pg_advisory_unlock(%s, %s)", db.SERVE_LOCK)
        serve.commit()
        with db.maintenance(maint):
            assert db.try_serve_lock(second) is False  # held by maintenance
            with pytest.raises(db.StaleGeneration):
                ctx.fence(serve)  # maintenance bumped the generation
            serve.rollback()
        assert db.try_serve_lock(second) is True


def test_claim_generation_without_a_row(migrated_dsn):
    with psycopg.connect(migrated_dsn) as c:
        assert db.claim_generation(c) is None
        with pytest.raises(db.StaleGeneration):
            db.check_generation(c, 1)
