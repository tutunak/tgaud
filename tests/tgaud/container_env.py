"""The outside world of scripts/container-e2e.sh: a test database (not migrated: the image's `migrate` does that) and
the fake Bot API, both on the host's loopback, which the container reaches at the address given as argv[1] (pasta
--map-host-loopback). Never the real Telegram.

Prints `KEY=VALUE` lines (the container's environment, then HEALTH_PORT, a free host port to publish /metrics on),
then `ready`, and waits until stdin closes. Then it checks what the service did (getMe and getUpdates reached the
fake, the serve generation was claimed, a poll was committed), prints the result, drops the database and its
tenant login (and the PostgreSQL container when it started one) and exits 1 if a check failed.

The database is made the way tests/tgaud/conftest.py makes it: TGAUD_TEST_ADMIN_URL when set, else a podman
postgres:18.6 with the production limits.
"""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
import tempfile
from pathlib import Path

import psycopg
from psycopg import sql as psql_sql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from botapi_fake import FakeBotServer  # noqa: E402
from conftest import IMAGE, SERVER_SETTINGS, PgServer, _free_port, _wait_ready  # noqa: E402

TOKEN = "5000:TEST-token_abcdefghijklmnop"  # the fake's bot id is 5000
LINK_KEY = "cd" * 32


def main(host_alias: str) -> int:
    tag = secrets.token_hex(4)
    container = None
    admin_dsn = os.environ.get("TGAUD_TEST_ADMIN_URL", "").strip()
    if not admin_dsn:
        port = _free_port()
        password = secrets.token_hex(12)
        container = f"tgaud-e2e-pg-{tag}"
        subprocess.run(["podman", "run", "-d", "--rm", "--name", container, "-e", f"POSTGRES_PASSWORD={password}",
                        "-p", f"127.0.0.1:{port}:5432", "--memory", "512m", IMAGE, *SERVER_SETTINGS],
                       check=True, capture_output=True, text=True)
        admin_dsn = f"postgresql://postgres:{password}@127.0.0.1:{port}/postgres"
    failures: list[str] = []
    with tempfile.TemporaryDirectory(prefix="tgaud-e2e-") as workdir:
        server = PgServer(admin_dsn, f"tgaud_e2e_{tag}", secrets.token_hex(16), Path(workdir), container)
        fake = FakeBotServer(token=TOKEN)
        try:
            _wait_ready(admin_dsn)
            with server.admin() as a:
                a.execute(psql_sql.SQL(
                    "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS "
                    "INHERIT CONNECTION LIMIT 10").format(psql_sql.Identifier(server.tenant),
                                                           psql_sql.Literal(server.tenant_password)))
            dsn = server.make_db(migrated=False)
            dbname = psycopg.conninfo.conninfo_to_dict(dsn)["dbname"]
            fake_port = fake.httpd.server_address[1]
            url = f"postgresql://{server.tenant}@{host_alias}:{server.port}/{dbname}?sslmode=disable"
            env = {
                "TGAUD_DATABASE_URL": url,
                "TGAUD_DATABASE_PASSWORD": server.tenant_password,
                "TGAUD_BOT_TOKEN": TOKEN,
                "TGAUD_LINK_HMAC_KEY": LINK_KEY,
                "TGAUD_BOT_API_URL": f"http://{host_alias}:{fake_port}",
                "TGAUD_ACTIVATION_ID": f"e2e-{tag}",
                "HEALTH_PORT": str(_free_port()),
            }
            for k, v in env.items():
                print(f"{k}={v}")
            print("ready", flush=True)
            sys.stdin.read()  # the script closes it when the container is gone

            methods = {c.method for c in fake.calls}
            for method in ("getMe", "getUpdates"):
                if method not in methods:
                    failures.append(f"serve never called {method} on the fake Bot API")
            with psycopg.connect(dsn) as c:
                row = c.execute("SELECT serve_generation, last_poll_ok_at IS NOT NULL FROM tg.bot_state").fetchone()
            if row is None or row[0] != 1 or not row[1]:
                failures.append(f"tg.bot_state after serve: {row} (want generation 1 and a committed poll)")
            print(f"fake Bot API: {len(fake.calls)} calls ({', '.join(sorted(methods))}); bot_state {row}", flush=True)
        except Exception as e:
            failures.append(f"{type(e).__name__}: {e}")
        finally:
            fake.close()
            try:
                with server.admin() as a:
                    for name in server.databases:
                        a.execute(psql_sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                            psql_sql.Identifier(name)))
                    a.execute(psql_sql.SQL("DROP ROLE IF EXISTS {}").format(psql_sql.Identifier(server.tenant)))
            finally:
                if container:
                    subprocess.run(["podman", "rm", "-f", container], capture_output=True)
    for f in failures:
        print(f"container-e2e: {f}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
