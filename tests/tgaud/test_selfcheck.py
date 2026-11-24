import shutil
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from tgaud import db, selfcheck

SQL = Path(__file__).resolve().parents[2] / "src" / "tgaud" / "sql"


def test_selfcheck_offline_with_no_environment(monkeypatch):
    def no_socket(*args, **kwargs):
        raise AssertionError("the selfcheck opened a socket")

    monkeypatch.setattr(socket.socket, "__init__", no_socket)
    line = selfcheck.run(env={})
    assert line.startswith("selfcheck ok: config ok (dummy TGAUD_DATABASE_URL")
    image = db.load_migrations()  # other tasks add migrations: count the image's files
    assert f"{len(image)} migrations (latest {image[-1].version:03d}), 37 query blocks" in line


def test_selfcheck_uses_real_values_when_present():
    env = {"TGAUD_DATABASE_URL": "postgresql://tgaud@db:5432/tgaud", "TGAUD_DATABASE_PASSWORD": "pw",
           "TGAUD_BOT_TOKEN": "1:abc", "TGAUD_LINK_HMAC_KEY": "ef" * 32, "TGAUD_WEB_PROXY_TOKEN": "t"}
    assert selfcheck.run(env=env).startswith("selfcheck ok: config ok, ")
    with pytest.raises(Exception, match="TGAUD_LINK_HMAC_KEY"):
        selfcheck.run(env=env | {"TGAUD_LINK_HMAC_KEY": "zz"})


def test_selfcheck_cli_in_an_empty_environment():
    r = subprocess.run([sys.executable, "-c", "import sys; from tgaud.cli import main; sys.exit(main(['--selfcheck']))"],
                       env={}, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    assert r.stdout.startswith("selfcheck ok:")


@pytest.mark.parametrize("file,breakage", [
    ("queries.sql", "\nSELECT :'only' IN ('all', '99') AS run \\gset\n\\if :run\nSELECT 'unterminated;\n\\endif\n"),
    ("queries.sql", "\nSELECT :'only' IN ('all', '98') AS run \\gset\n\\if :run\nSELECT :nosuchvar;\n\\endif\n"),
    ("migrations/002_shared.sql", "\n\\set x 1\n"),
    ("migrations/003_bad.sql", "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1;\n"),
    ("admin.sql", "\nSELECT 'oops;\n"),
])
def test_selfcheck_fails_on_a_broken_sql_file(tmp_path, file, breakage):
    root = tmp_path / "sql"
    shutil.copytree(SQL, root)
    with open(root / file, "a") as f:
        f.write(breakage)
    with pytest.raises(Exception):
        selfcheck.run(env={}, root=root)


@pytest.mark.parametrize("env", [{"TGAUD_BOT_TOKEN": ""}, {"TGAUD_BOT_API_URL": "http://127.0.0.1:abc"},
                                 {"TGAUD_DATABASE_URL": ""}, {"TGAUD_LINK_HMAC_KEY": " "}])
def test_selfcheck_checks_explicit_values_even_when_empty(env):
    with pytest.raises(Exception, match="TGAUD_"):
        selfcheck.run(env=env)
