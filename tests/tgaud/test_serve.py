"""`tgaud serve` as a subprocess: exit codes, the single serve, /health, and SIGTERM within 10 s."""

import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import psycopg
import pytest
from botapi_fake import TOKEN, FakeBotServer

RUNNER = Path(__file__).with_name("serve_runner.py")
KEY = "cd" * 32


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Serve:
    def __init__(self, pg, dsn, fake, mode="", tmp_path=None, **env):
        self.port = free_port()
        dbname = psycopg.conninfo.conninfo_to_dict(dsn)["dbname"]
        base = {k: v for k, v in os.environ.items() if not k.startswith("TGAUD_")}
        base.update({
            "TGAUD_DATABASE_URL": pg.tenant_url(dbname),
            "TGAUD_DATABASE_PASSWORD": pg.tenant_password,
            "TGAUD_BOT_TOKEN": TOKEN,
            "TGAUD_LINK_HMAC_KEY": KEY,
            "TGAUD_BOT_API_URL": fake.url if fake else "http://127.0.0.1:1",
            "TGAUD_METRICS_LISTEN": f"127.0.0.1:{self.port}",
            "TGAUD_ACTIVATION_ID": "act-test",
            "TEST_MODE": mode,
        })
        base.update(env)
        self.log = (tmp_path or Path("/tmp")) / f"serve-{self.port}.log"
        self.out = open(self.log, "w")
        self.proc = subprocess.Popen([sys.executable, str(RUNNER)], env=base, stdout=self.out,
                                     stderr=subprocess.STDOUT)

    def health(self):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/health", timeout=2) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()
        except OSError:
            return None, ""

    def metrics(self):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/metrics", timeout=2) as r:
                return r.read().decode()
        except OSError:
            return ""

    def wait_for(self, predicate, timeout=30.0, what="condition"):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"serve exited {self.proc.returncode} waiting for {what}:\n{self.text()}")
            if predicate():
                return
            time.sleep(0.1)
        raise AssertionError(f"timed out waiting for {what}:\n{self.text()}")

    def sigterm_and_time(self):
        started = time.monotonic()
        self.proc.send_signal(signal.SIGTERM)
        try:
            code = self.proc.wait(15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            raise AssertionError(f"serve still alive 15 s after SIGTERM:\n{self.text()}")
        return code, time.monotonic() - started

    def text(self):
        self.out.flush()
        return self.log.read_text()

    def close(self):
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(10)
        self.out.close()


@pytest.fixture
def serves():
    started = []
    yield started
    for s in started:
        s.close()


@pytest.fixture
def fake():
    with FakeBotServer() as server:
        yield server


def start(serves, *args, **kw):
    s = Serve(*args, **kw)
    serves.append(s)
    return s


def backend_running(dsn, application, query_part):
    with psycopg.connect(dsn, autocommit=True) as c:
        return c.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name = %s AND state = 'active' "
                         "AND query LIKE %s", (application, f"%{query_part}%")).fetchone()[0] > 0


def offset(dsn):
    with psycopg.connect(dsn, autocommit=True) as c:
        return c.execute("SELECT next_offset FROM tg.bot_state").fetchone()[0]


def test_healthy_serve_and_a_second_serve_exits_2(pg, migrated_dsn, fake, serves, tmp_path):
    first = start(serves, pg, migrated_dsn, fake, "poll,heartbeat", tmp_path)
    first.wait_for(lambda: first.health()[0] == 200, what="/health 200")
    status, body = first.health()
    assert body == "ok activation=act-test\n"
    second = start(serves, pg, migrated_dsn, fake, "poll", tmp_path)
    assert second.proc.wait(15) == 2
    assert "another tgaud serve" in second.text()
    assert first.proc.poll() is None
    assert 'tgaud_job_last_success_timestamp_seconds{job="reconciler"}' in first.metrics()
    code, took = first.sigterm_and_time()
    assert code == 0 and took < 10


def test_schema_behind_exits_3(pg, empty_dsn, fake, serves, tmp_path):
    s = start(serves, pg, empty_dsn, fake, "", tmp_path)
    assert s.proc.wait(30) == 3
    assert "tgaud migrate" in s.text()


def test_config_error_exits_2(pg, migrated_dsn, serves, tmp_path):
    s = start(serves, pg, migrated_dsn, None, "", tmp_path, TGAUD_LINK_HMAC_KEY="")
    assert s.proc.wait(30) == 2
    assert "TGAUD_LINK_HMAC_KEY" in s.text()


def test_health_503_while_the_head_update_keeps_failing(pg, migrated_dsn, fake, serves, tmp_path):
    s = start(serves, pg, migrated_dsn, fake, "fail_head,heartbeat", tmp_path, TEST_HEALTH_WINDOW="2")
    s.wait_for(lambda: 'job="reconciler"' in s.metrics(), what="background job success")
    time.sleep(2.5)
    status, body = s.health()
    assert status == 503 and "ingest commit" in body and "activation=act-test" in body
    assert "getUpdates" not in body.split("activation")[0].replace("ingest commit", "")  # polls do succeed
    assert offset(migrated_dsn) == 1  # still on the failing update
    s.sigterm_and_time()


def test_sigterm_during_a_held_long_poll(pg, migrated_dsn, fake, serves, tmp_path):
    fake.hold("getUpdates", seconds=None, times=50)
    s = start(serves, pg, migrated_dsn, fake, "poll", tmp_path)
    s.wait_for(lambda: fake.calls_of("getUpdates"), what="a long poll")
    code, took = s.sigterm_and_time()
    assert took < 10, s.text()
    assert offset(migrated_dsn) == 1


def test_sigterm_during_a_trickling_response(pg, migrated_dsn, fake, serves, tmp_path):
    fake.trickle_body("getUpdates", interval=0.2, times=50)
    s = start(serves, pg, migrated_dsn, fake, "poll", tmp_path)
    s.wait_for(lambda: fake.calls_of("getUpdates"), what="a trickling poll")
    time.sleep(1.0)
    code, took = s.sigterm_and_time()
    assert took < 10, s.text()
    assert offset(migrated_dsn) == 1


def test_sigterm_during_a_long_batch_statement(pg, migrated_dsn, fake, serves, tmp_path):
    s = start(serves, pg, migrated_dsn, fake, "long_batch", tmp_path)
    s.wait_for(lambda: backend_running(migrated_dsn, "tgaud-batch", "pg_sleep"), what="the long statement")
    code, took = s.sigterm_and_time()
    assert took < 10, s.text()
    with psycopg.connect(migrated_dsn, autocommit=True) as c:
        assert c.execute("SELECT row_count FROM stats.rollup_run").fetchall() == [(7,)]  # the published rollup


def test_sigterm_during_a_blocked_startup(pg, migrated_dsn, fake, serves, tmp_path):
    s = start(serves, pg, migrated_dsn, fake, "block_startup", tmp_path)
    s.wait_for(lambda: backend_running(migrated_dsn, "tgaud-ingest", "pg_sleep"), what="the blocked startup")
    code, took = s.sigterm_and_time()
    assert took < 10, s.text()
    assert offset(migrated_dsn) == 0


def test_the_hard_deadline_ends_a_stuck_process(pg, migrated_dsn, fake, serves, tmp_path):
    s = start(serves, pg, migrated_dsn, fake, "stuck_startup", tmp_path)
    s.wait_for(lambda: "startup stuck" in s.text(), what="the stuck startup")
    code, took = s.sigterm_and_time()
    assert code == 1 and 8.5 < took < 10, (code, took, s.text())
    assert "exiting now" in s.text()
