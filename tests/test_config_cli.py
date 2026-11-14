import os
import sqlite3
import stat

import pytest

from tgstats import cli
from tgstats.config import ConfigError, load_config, load_db_path, parse_env_file


def test_env_vars_and_defaults(tmp_path):
    cfg = load_config(environ={"TG_API_ID": "12", "TG_API_HASH": "abc"}, cwd=tmp_path)
    assert (cfg.api_id, cfg.api_hash) == (12, "abc")
    assert (cfg.session_path, cfg.db_path) == ("tgstats.session", "tgstats.db")


def test_dotenv_file_and_precedence(tmp_path):
    (tmp_path / ".env").write_text(
        "# comment\nTG_API_ID=5\nexport TG_API_HASH='quoted'\nTG_DB=from_env_file.db\nTG_SESSION=\"s.session\"\n"
    )
    cfg = load_config(environ={}, cwd=tmp_path)
    assert (cfg.api_id, cfg.api_hash, cfg.db_path, cfg.session_path) == (5, "quoted", "from_env_file.db", "s.session")
    cfg = load_config(environ={"TG_API_ID": "9", "TG_DB": "real.db"}, cwd=tmp_path)
    assert cfg.api_id == 9 and cfg.db_path == "real.db"
    cfg = load_config("flag.session", "flag.db", environ={"TG_DB": "real.db"}, cwd=tmp_path)
    assert (cfg.session_path, cfg.db_path) == ("flag.session", "flag.db")
    assert parse_env_file(tmp_path / "missing") == {}


@pytest.mark.parametrize(
    "environ,needle",
    [({}, "TG_API_ID"), ({"TG_API_ID": "1"}, "TG_API_HASH"), ({"TG_API_ID": "x", "TG_API_HASH": "h"}, "number")],
)
def test_config_errors_are_one_line_and_say_where_to_get_credentials(tmp_path, environ, needle):
    with pytest.raises(ConfigError) as err:
        load_config(environ=environ, cwd=tmp_path)
    msg = str(err.value)
    assert needle in msg and "my.telegram.org" in msg and "\n" not in msg


def test_db_path_needs_no_credentials(tmp_path):
    assert load_db_path(environ={}, cwd=tmp_path) == "tgstats.db"
    assert load_db_path("x.db", environ={}, cwd=tmp_path) == "x.db"


def test_chats_without_api_id_prints_one_line_error(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TG_API_ID", raising=False)
    monkeypatch.delenv("TG_API_HASH", raising=False)
    assert cli.main(["chats"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    lines = captured.err.strip().splitlines()
    assert len(lines) == 1 and "TG_API_ID" in lines[0] and "my.telegram.org" in lines[0]


@pytest.fixture
def populated(tmp_path, fx):
    path = tmp_path / "t.db"
    fx.chat(1, "Alpha", [1, 2, 3], username="alpha", reported=3)
    fx.chat(2, "Beta", [3, 4], reported=10)  # incomplete
    fx.conn.commit()
    disk = sqlite3.connect(path)
    fx.conn.backup(disk)
    disk.close()
    return str(path)


def test_unique_cli_output_warning_and_csv(populated, tmp_path, capsys):
    csv_path = tmp_path / "u.csv"
    assert cli.main(["unique", "--db", populated, "--chat", "@alpha", "--list", "--csv", str(csv_path)]) == 0
    out, err = capsys.readouterr()
    table_part = out.split("\n\n")[0]
    assert "Compared chats: 2" in table_part and "Alpha (1)" in table_part and "Beta" not in table_part
    assert "WARNING" in err and "'Beta'" in err
    assert "1\t@u1\tFirst1 Last1" in out
    rows = csv_path.read_text().splitlines()
    assert rows[0] == "chat_id,chat_title,user_id,username,first_name,last_name"
    assert rows[1:] == ["1,Alpha,1,u1,First1,Last1", "1,Alpha,2,u2,First2,Last2"]


def test_intersect_cli(populated, capsys):
    assert cli.main(["intersect", "Alpha", "Beta", "--db", populated, "--list"]) == 0
    out, err = capsys.readouterr()
    assert "Common users: 1" in out and "33.3% of A" in out and "50.0% of B" in out
    assert "Jaccard index: 0.250" in out
    assert "3\t@u3" in out
    assert "lower bound" in err


def test_overlap_cli_and_errors(populated, tmp_path, capsys):
    assert cli.main(["overlap", "--db", populated]) == 0
    out, err = capsys.readouterr()
    assert "1 = Alpha (1) (3 members)" in out and "lower bound" in err
    assert cli.main(["intersect", "Alpha", "Alpha", "--db", populated]) == 2
    assert cli.main(["unique", "--db", populated, "--among", "Nope"]) == 2
    assert cli.main(["unique", "--db", str(tmp_path / "missing.db")]) == 2
    assert not (tmp_path / "missing.db").exists()


def _mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def test_new_session_file_is_owner_only_from_creation(tmp_path):
    old = os.umask(0o022)
    try:
        cfg = load_config(str(tmp_path / "s.session"), None, environ={"TG_API_ID": "1", "TG_API_HASH": "h"}, cwd=tmp_path)
        client = cli.make_client(cfg)  # Telethon opens/creates the session here, before any login
    finally:
        os.umask(old)
    assert _mode(tmp_path / "s.session") == 0o600
    client.session.close()


def test_existing_world_readable_session_is_tightened_before_telethon_opens_it(tmp_path):
    path = tmp_path / "old.session"
    path.write_text("")
    path.chmod(0o644)
    cfg = load_config(str(path), None, environ={"TG_API_ID": "1", "TG_API_HASH": "h"}, cwd=tmp_path)
    cli.make_client(cfg).session.close()
    assert _mode(path) == 0o600


def test_failed_login_leaves_session_owner_only(tmp_path, monkeypatch):
    from telethon import TelegramClient

    async def boom(self, *a, **k):
        raise RuntimeError("code entry interrupted")

    monkeypatch.setattr(TelegramClient, "start", boom)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TG_API_ID", "1")
    monkeypatch.setenv("TG_API_HASH", "h")
    old = os.umask(0o022)
    try:
        with pytest.raises(RuntimeError):
            cli.main(["login", "--session", str(tmp_path / "x.session")])
    finally:
        os.umask(old)
    assert _mode(tmp_path / "x.session") == 0o600


def test_session_protection_failure_is_reported_not_ignored(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TG_API_ID", "1")
    monkeypatch.setenv("TG_API_HASH", "h")
    (tmp_path / "dir.session").mkdir()  # not a file: cannot be secured
    assert cli.main(["login", "--session", str(tmp_path / "dir.session")]) == 1
    assert "cannot secure the session file" in capsys.readouterr().err


@pytest.mark.parametrize("command", ["chats", "collect"])
def test_bad_delay_range_is_a_one_line_error(tmp_path, monkeypatch, capsys, command):
    monkeypatch.chdir(tmp_path)
    assert cli.main([command, "--min-delay-ms", "5000", "--max-delay-ms", "1000"]) == 2
    err = capsys.readouterr().err
    assert "--min-delay-ms" in err and err.count("\n") == 1


def test_delay_defaults_are_1_to_5_seconds():
    args = cli.build_parser().parse_args(["collect"])
    assert cli.delay_range(args) == (1000, 5000)
