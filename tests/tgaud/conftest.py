"""PostgreSQL for the tgaud tests.

With TGAUD_TEST_ADMIN_URL (a superuser or CREATEROLE+CREATEDB login), the tests use that server. Without it,
they start docker.io/library/postgres:18.6-alpine3.24 (local image) with podman on a free 127.0.0.1 port with
the production limits, and remove it at the end.

Per test session: one tenant login like ~/projects/ansible/postgresql makes it (LOGIN NOSUPERUSER NOCREATEDB
NOCREATEROLE NOREPLICATION NOBYPASSRLS INHERIT, CONNECTION LIMIT 10), with a unique name, so several worktrees
can test at once. Every database is created by the admin, owned by the tenant, PUBLIC revoked; admin.sql runs
as the admin (the roles bi_reader and collect_writer are cluster-wide and made only if missing); migrations
run as the tenant. Everything is dropped at the end (the shared roles stay).

Fixtures:
    pg                 the PgServer of the session (admin DSN, tenant, make_db(), psql())
    tgaud_dsn          tenant DSN of a migrated database shared by the session (don't commit test data)
    empty_dsn          tenant DSN of a fresh, unmigrated database (per test)
    migrated_dsn       tenant DSN of a fresh, migrated database (per test)
    history_fixture    tenant DSN of a migrated database with tests/fixtures/history_fixture.sql loaded the way
                       docs/design/evidence/run-fixture.sh does (session; read it inside a transaction you roll
                       back)
psql runs from the local binary if there is one, else from the postgres image with podman (--network=host).

TGAUD_TEST_NO_DATABASE=1 skips every test that needs the server (the Forgejo CI test job: its runner's job network
is the host's, so a service is not reachable by name). The local `mise run ci` runs them all.
"""

from __future__ import annotations

import os
import secrets
import shutil
import socket
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
import pytest
from psycopg import sql as psql_sql

from tgaud import db

IMAGE = "docker.io/library/postgres:18.6-alpine3.24"
SERVER_SETTINGS = ["-c", "shared_buffers=64MB", "-c", "work_mem=2MB", "-c", "maintenance_work_mem=32MB",
                   "-c", "autovacuum_work_mem=16MB", "-c", "effective_cache_size=256MB",
                   "-c", "max_parallel_workers=0", "-c", "max_parallel_workers_per_gather=0",
                   "-c", "max_parallel_maintenance_workers=0"]
REPO = Path(__file__).resolve().parents[2]
SQL_DIR = REPO / "src" / "tgaud" / "sql"
FIXTURE_SQL = REPO / "tests" / "fixtures" / "history_fixture.sql"


@dataclass
class PgServer:
    admin_dsn: str
    tenant: str
    tenant_password: str
    workdir: Path
    container: str | None = None
    databases: list[str] = field(default_factory=list)

    @property
    def _admin_info(self) -> dict:
        return psycopg.conninfo.conninfo_to_dict(self.admin_dsn)

    @property
    def host(self) -> str:
        return str(self._admin_info.get("host") or "127.0.0.1")

    @property
    def port(self) -> int:
        return int(self._admin_info.get("port") or 5432)

    def tenant_dsn(self, dbname: str, **extra: str) -> str:
        return psycopg.conninfo.make_conninfo(host=self.host, port=str(self.port), dbname=dbname,
                                              user=self.tenant, password=self.tenant_password, **extra)

    def tenant_url(self, dbname: str) -> str:
        """The contract's URL form (no password): pass TGAUD_DATABASE_PASSWORD separately."""
        return f"postgresql://{self.tenant}@{self.host}:{self.port}/{dbname}?sslmode=disable"

    def admin(self, dbname: str = "postgres") -> psycopg.Connection:
        return psycopg.connect(self.admin_dsn, dbname=dbname, autocommit=True)

    def make_db(self, migrated: bool = True) -> str:
        """A new database owned by the tenant (PUBLIC revoked), admin.sql applied, optionally migrated."""
        name = f"{self.tenant}_{len(self.databases) + 1}"
        with self.admin() as a:
            a.execute(psql_sql.SQL("CREATE DATABASE {} OWNER {}").format(
                psql_sql.Identifier(name), psql_sql.Identifier(self.tenant)))
            self.databases.append(name)
            a.execute(psql_sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(psql_sql.Identifier(name)))
        for attempt in range(3):
            # roles are cluster-wide: a parallel session may create them at the same moment
            r = self.psql(name, SQL_DIR / "admin.sql", as_admin=True, variables={"tenant": self.tenant}, check=False)
            if r.returncode == 0:
                break
            if attempt == 2 or "already exists" not in r.stderr and "duplicate key" not in r.stderr:
                raise RuntimeError(f"admin.sql failed: {r.stderr}")
            time.sleep(0.5)
        dsn = self.tenant_dsn(name)
        if migrated:
            with psycopg.connect(dsn) as c:
                db.migrate(c)
        return dsn

    def psql(self, dbname: str, file: Path, *, as_admin: bool = False, variables: dict[str, str] | None = None,
             check: bool = True) -> subprocess.CompletedProcess:
        """Run a psql script (ON_ERROR_STOP) as the tenant or the admin."""
        info = self._admin_info
        user = str(info.get("user") or "postgres") if as_admin else self.tenant
        password = str(info.get("password") or "") if as_admin else self.tenant_password
        local = self.workdir / file.name
        shutil.copyfile(file, local)
        args = ["-X", "-q", "-v", "ON_ERROR_STOP=1", "-h", self.host, "-p", str(self.port), "-U", user,
                "-d", dbname]
        for k, v in (variables or {}).items():
            args += ["-v", f"{k}={v}"]
        env = dict(os.environ, PGPASSWORD=password)
        if shutil.which("psql"):
            cmd = ["psql", *args, "-f", str(local)]
        else:
            cmd = ["podman", "run", "--rm", "--network=host", "-e", "PGPASSWORD",
                   "-v", f"{self.workdir}:/work:ro,Z", IMAGE, "psql", *args, "-f", f"/work/{file.name}"]
        r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
        if check and r.returncode != 0:
            raise RuntimeError(f"psql {file.name} failed ({r.returncode}): {r.stderr}")
        return r


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(dsn: str, timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            with psycopg.connect(dsn, connect_timeout=3) as c:
                c.execute("SELECT 1")
                return
        except psycopg.Error:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.5)


@pytest.fixture(scope="session")
def pg(tmp_path_factory: pytest.TempPathFactory) -> Iterator[PgServer]:
    if os.environ.get("TGAUD_TEST_NO_DATABASE"):
        pytest.skip("TGAUD_TEST_NO_DATABASE is set: no PostgreSQL in this run, the database tests are skipped")
    tag = secrets.token_hex(4)
    container = None
    admin_dsn = os.environ.get("TGAUD_TEST_ADMIN_URL", "").strip()
    if not admin_dsn:
        port = _free_port()
        password = secrets.token_hex(12)
        container = f"tgaud-test-{tag}"
        subprocess.run(["podman", "run", "-d", "--rm", "--name", container, "-e", f"POSTGRES_PASSWORD={password}",
                        "-p", f"127.0.0.1:{port}:5432", "--memory", "512m", IMAGE, *SERVER_SETTINGS],
                       check=True, capture_output=True, text=True)
        admin_dsn = f"postgresql://postgres:{password}@127.0.0.1:{port}/postgres"
    server = PgServer(admin_dsn, f"tgaud_t_{tag}", secrets.token_hex(16), tmp_path_factory.mktemp("psql"),
                      container)
    try:
        _wait_ready(admin_dsn)
        with server.admin() as a:
            a.execute(psql_sql.SQL(
                "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS "
                "INHERIT CONNECTION LIMIT 10").format(psql_sql.Identifier(server.tenant),
                                                       psql_sql.Literal(server.tenant_password)))
        yield server
    finally:
        try:
            with server.admin() as a:
                for name in reversed(server.databases):
                    a.execute(psql_sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(psql_sql.Identifier(name)))
                a.execute(psql_sql.SQL("DROP ROLE IF EXISTS {}").format(psql_sql.Identifier(server.tenant)))
        finally:
            if container:
                subprocess.run(["podman", "rm", "-f", container], capture_output=True)


@pytest.fixture(scope="session")
def tgaud_dsn(pg: PgServer) -> str:
    return pg.make_db(migrated=True)


@pytest.fixture
def empty_dsn(pg: PgServer) -> str:
    return pg.make_db(migrated=False)


@pytest.fixture
def migrated_dsn(pg: PgServer) -> str:
    return pg.make_db(migrated=True)


@pytest.fixture(scope="session")
def history_fixture(pg: PgServer) -> str:
    dsn = pg.make_db(migrated=True)
    name = psycopg.conninfo.conninfo_to_dict(dsn)["dbname"]
    r = pg.psql(name, FIXTURE_SQL)
    noise = [line for line in r.stderr.splitlines()
             if line.strip() and "permission denied to vacuum" not in line and "skipping" not in line]
    assert not noise, "\n".join(noise)
    return dsn
