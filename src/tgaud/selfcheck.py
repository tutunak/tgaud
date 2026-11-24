"""`tgaud --selfcheck`: an offline check of a built image (deploy contract: --network=none, read-only root).

It parses the config from the environment, filling dummy values only for unset variables (the secrets and
TGAUD_DATABASE_URL; an explicitly empty value is validated as serve would), lexes every bundled migration and
admin.sql, parses every block of queries.sql, imports every module of the package, compiles every web template,
and prints one line
starting `selfcheck ok:`. It never connects anywhere: socket
creation and connects are refused for its duration, so a module that touches the network fails the check.
"""

from __future__ import annotations

import contextlib
import importlib
import os
import pkgutil
import socket
import sys
from collections.abc import Iterator, Mapping
from importlib.resources.abc import Traversable

DUMMY_ENV = {
    "TGAUD_DATABASE_URL": "postgresql://selfcheck@127.0.0.1:1/selfcheck",
    "TGAUD_DATABASE_PASSWORD": "selfcheck",
    "TGAUD_BOT_TOKEN": "1:selfcheck",
    "TGAUD_LINK_HMAC_KEY": "00" * 32,
    "TGAUD_WEB_PROXY_TOKEN": "selfcheck",
}


class SelfcheckError(RuntimeError):
    pass


@contextlib.contextmanager
def _no_network() -> Iterator[None]:
    def refuse(*args, **kwargs):
        raise SelfcheckError("the selfcheck tried to open a network connection")

    saved = (socket.socket.connect, socket.socket.connect_ex, socket.create_connection, socket.getaddrinfo)
    socket.socket.connect = refuse  # type: ignore[method-assign]
    socket.socket.connect_ex = refuse  # type: ignore[method-assign]
    socket.create_connection = refuse  # type: ignore[assignment]
    socket.getaddrinfo = refuse  # type: ignore[assignment]
    try:
        yield
    finally:
        socket.socket.connect, socket.socket.connect_ex, socket.create_connection, socket.getaddrinfo = saved


def run(env: Mapping[str, str] | None = None, root: Traversable | None = None) -> str:
    """Returns the `selfcheck ok: ...` line or raises SelfcheckError (or the underlying error)."""
    from tgaud import config, db, sql, sqlscript

    env = dict(os.environ if env is None else env)
    # Dummies only for unset variables: an explicitly empty value is checked like serve would check it.
    dummies = [k for k in DUMMY_ENV if k not in env]
    for k in dummies:
        env[k] = DUMMY_ENV[k]
    with _no_network():
        cfg = config.load(env, need=config.NEEDS)
        if cfg.web_listen is not None:
            cfg.web_proxy_token_or_fail()
        cfg.dsn("selfcheck")

        root = root or db.sql_root()
        migrations = db.load_migrations(root)
        if not migrations:
            raise SelfcheckError("no migrations bundled")
        for m in migrations:
            m.statements()
        sqlscript.parse((root / "admin.sql").read_text("utf-8"), "admin.sql")
        qf = sql.parse((root / "queries.sql").read_text("utf-8"), "queries.sql")
        for block_id in qf.ids():
            for b in qf.blocks_for(block_id):
                _render(b.items)

        import tgaud

        modules = 0
        for info in pkgutil.walk_packages(tgaud.__path__, "tgaud."):
            importlib.import_module(info.name)
            modules += 1
        templates = _web_templates()
    dummy_note = f" (dummy {', '.join(dummies)})" if dummies else ""
    return (f"selfcheck ok: config ok{dummy_note}, {len(migrations)} migrations (latest "
            f"{migrations[-1].version:03d}), {len(qf.ids())} query blocks, {modules} modules, "
            f"{templates} web templates")


def _web_templates() -> int:
    """Compile every template of the web UI and find its stylesheet (both are package data of the wheel)."""
    from tgaud.web import pages

    env = pages.environment()
    names = env.list_templates()
    if not names:
        raise SelfcheckError("no web templates bundled")
    for name in names:
        env.get_template(name)
    if pages.static_file("style.css") is None:
        raise SelfcheckError("the web stylesheet is not bundled")
    return len(names)


def _render(items) -> None:
    from tgaud import sql

    for item in items:
        if isinstance(item, sql.If):
            _render(item.then)
            _render(item.otherwise)
        else:
            item.query()
            if item.explainable:
                item.query("EXPLAIN (ANALYZE, BUFFERS)")


def main() -> int:
    try:
        line = run()
    except Exception as e:
        print(f"selfcheck failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    print(line)
    return 0
