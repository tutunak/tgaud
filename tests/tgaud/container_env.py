"""The outside world of scripts/container-e2e.sh: a test database (not migrated: the image's `migrate` does that) and
the fake Bot API. Never the real Telegram. The container reaches the host's 127.0.0.1 at the address given as argv[1]
(pasta --map-host-loopback); the fake listens there, and so does a relay to the database, which forwards to the
server over TCP at the address this helper itself uses (any host address, another loopback address; never a Unix
socket), so the container needs no route of its own to it.

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
import socket
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

import psycopg
from psycopg import sql as psql_sql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from botapi_fake import FakeBotServer  # noqa: E402
from conftest import IMAGE, SERVER_SETTINGS, PgServer, _free_port, _wait_ready  # noqa: E402

TOKEN = "5000:TEST-token_abcdefghijklmnop"  # the fake's bot id is 5000
LINK_KEY = "cd" * 32


class Relay:
    """Forwards each TCP connection to 127.0.0.1:<port> to the database server at (host, port) over TCP.

    Only TCP targets: a Unix socket would let PostgreSQL authenticate this process's account (peer), and the relay's
    listener is open to every local account, so it is refused before anything listens."""

    def __init__(self, host: str, port: int) -> None:
        if not host or host.startswith("/") or host.startswith("@"):
            raise ValueError(f"the database host {host!r} is a Unix socket: give TGAUD_TEST_ADMIN_URL a TCP host")
        self.target = (host, port)
        self.sock = socket.create_server(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        threading.Thread(target=self._accept, name="db-relay", daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                client, _ = self.sock.accept()
            except OSError:
                return  # closed
            threading.Thread(target=self._serve, args=(client,), daemon=True).start()

    def _serve(self, client: socket.socket) -> None:
        try:
            upstream = socket.create_connection(self.target, timeout=10)
        except OSError:
            client.close()
            return
        upstream.settimeout(None)
        for src, dst in ((client, upstream), (upstream, client)):
            threading.Thread(target=self._pipe, args=(src, dst), daemon=True).start()

    @staticmethod
    def _pipe(src: socket.socket, dst: socket.socket) -> None:
        try:
            while data := src.recv(65536):
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for s in (src, dst):  # either side ending ends both
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                s.close()

    def close(self) -> None:
        self.sock.close()


def main(host_alias: str) -> int:
    tag = secrets.token_hex(4)
    container = None
    failures: list[str] = []
    try:  # from the moment a PostgreSQL container may exist, it is removed whatever fails
        admin_dsn = os.environ.get("TGAUD_TEST_ADMIN_URL", "").strip()
        if not admin_dsn:
            port = _free_port()
            password = secrets.token_hex(12)
            container = f"tgaud-e2e-pg-{tag}"
            subprocess.run(["podman", "run", "-d", "--rm", "--name", container, "-e", f"POSTGRES_PASSWORD={password}",
                            "-p", f"127.0.0.1:{port}:5432", "--memory", "512m", IMAGE, *SERVER_SETTINGS],
                           check=True, capture_output=True, text=True)
            admin_dsn = f"postgresql://postgres:{password}@127.0.0.1:{port}/postgres"
        with tempfile.TemporaryDirectory(prefix="tgaud-e2e-") as workdir:
            server = PgServer(admin_dsn, f"tgaud_e2e_{tag}", secrets.token_hex(16), Path(workdir), container)
            failures += run(server, admin_dsn, host_alias, tag)
    except subprocess.CalledProcessError as e:  # its message would carry the arguments, the password among them
        failures.append(f"podman run of the test database failed ({e.returncode}): {(e.stderr or '').strip()}")
    except Exception as e:
        failures.append(f"{type(e).__name__}: {e}")
    finally:
        if container:
            subprocess.run(["podman", "rm", "-f", container], capture_output=True)
    for f in failures:
        print(f"container-e2e: {f}", file=sys.stderr)
    return 1 if failures else 0


def run(server: PgServer, admin_dsn: str, host_alias: str, tag: str) -> list[str]:
    """Make the database, the fake and the relay, hand them to the script, check what serve did; returns the
    failures. Drops only what it made, whatever fails."""
    failures: list[str] = []
    fake = relay = None
    role_created = False
    try:
        relay = Relay(server.host, server.port)  # refuses a Unix socket before anything else exists
        fake = FakeBotServer(token=TOKEN)
        _wait_ready(admin_dsn)
        with server.admin() as a:
            a.execute(psql_sql.SQL(
                "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS "
                "INHERIT CONNECTION LIMIT 10").format(psql_sql.Identifier(server.tenant),
                                                       psql_sql.Literal(server.tenant_password)))
        role_created = True  # from here on the role is this run's to drop, never before (a name collision)
        dsn = server.make_db(migrated=False)
        dbname = psycopg.conninfo.conninfo_to_dict(dsn)["dbname"]
        fake_port = fake.httpd.server_address[1]
        url = f"postgresql://{server.tenant}@{host_alias}:{relay.port}/{dbname}?sslmode=disable"
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
        for made in (fake, relay):
            if made is not None:
                made.close()
        if server.databases or role_created:
            try:
                with server.admin() as a:
                    for name in server.databases:
                        a.execute(psql_sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                            psql_sql.Identifier(name)))
                    if role_created:
                        a.execute(psql_sql.SQL("DROP ROLE {}").format(psql_sql.Identifier(server.tenant)))
            except Exception as e:
                failures.append(f"cleanup: {type(e).__name__}: {e}")
    return failures


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
