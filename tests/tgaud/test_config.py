import logging

import psycopg.conninfo
import pytest

from tgaud import config

TOKEN = "123456:SECRET-bot-token_xyz"
KEY = "ab" * 32
PW = "db-pass-WORD-77"
WEB_TOKEN = "proxy-token-secret-99"


def full_env(**extra):
    env = {
        "TGAUD_DATABASE_URL": "postgresql://tgaud@db.example:5433/tgaud?sslmode=disable",
        "TGAUD_DATABASE_PASSWORD": PW,
        "TGAUD_BOT_TOKEN": TOKEN,
        "TGAUD_LINK_HMAC_KEY": KEY,
        "TGAUD_ACTIVATION_ID": "act-1",
    }
    env.update(extra)
    return env


def test_defaults_and_parsing():
    cfg = config.load(full_env(), need=config.NEEDS)
    assert str(cfg.metrics_listen) == "0.0.0.0:9108"
    assert cfg.web_listen is None
    assert cfg.store_names is True and cfg.log_level == "INFO"
    assert cfg.bot_api_url == "https://api.telegram.org"
    assert cfg.activation_id == "act-1"
    info = psycopg.conninfo.conninfo_to_dict(cfg.dsn("worker"))
    assert info["password"] == PW and info["application_name"] == "tgaud-worker"
    assert info["host"] == "db.example" and info["port"] == "5433" and info["sslmode"] == "disable"
    cfg2 = config.load(full_env(TGAUD_STORE_NAMES="false", TGAUD_LOG_LEVEL="debug",
                                TGAUD_METRICS_LISTEN="127.0.0.1:19108", TGAUD_BOT_API_URL="http://127.0.0.1:8081/"))
    assert cfg2.store_names is False and cfg2.log_level == "DEBUG"
    assert (cfg2.metrics_listen.host, cfg2.metrics_listen.port) == ("127.0.0.1", 19108)
    assert cfg2.bot_api_url == "http://127.0.0.1:8081"


def test_password_env_overrides_the_url_password():
    cfg = config.load(full_env(TGAUD_DATABASE_URL="postgresql://tgaud:in-url-pw@h/tgaud"))
    assert psycopg.conninfo.conninfo_to_dict(cfg.dsn("x"))["password"] == PW
    assert "in-url-pw" not in repr(cfg) and "in-url-pw" not in (cfg.database_url or "")
    env = full_env(TGAUD_DATABASE_URL="postgresql://tgaud:in-url-pw@h/tgaud")
    del env["TGAUD_DATABASE_PASSWORD"]
    cfg = config.load(env)
    assert psycopg.conninfo.conninfo_to_dict(cfg.dsn("x"))["password"] == "in-url-pw"
    assert "in-url-pw" not in repr(cfg)


def test_needs_and_validation_errors_name_variables_not_values():
    with pytest.raises(config.ConfigError) as e:
        config.load({}, need=config.NEEDS)
    msg = str(e.value)
    for name in ("TGAUD_DATABASE_URL", "TGAUD_BOT_TOKEN", "TGAUD_LINK_HMAC_KEY"):
        assert name in msg
    config.load({}, need=())  # nothing needed: fine
    bad = full_env(TGAUD_LINK_HMAC_KEY="abcd", TGAUD_BOT_TOKEN="not a token " + TOKEN,
                   TGAUD_DATABASE_URL="mysql://u:" + PW + "@h/d", TGAUD_METRICS_LISTEN="nope",
                   TGAUD_STORE_NAMES="maybe", TGAUD_LOG_LEVEL="loud", TGAUD_BOT_API_URL="ftp://x")
    with pytest.raises(config.ConfigError) as e:
        config.load(bad)
    msg = str(e.value)
    assert msg.count(";") >= 6
    for secret in (TOKEN, PW, "abcd"):
        assert secret not in msg
    with pytest.raises(config.ConfigError) as e:
        config.load(full_env(TGAUD_DATABASE_URL=f"postgresql://u:{PW}@h/d?nosuchoption=1"))
    assert PW not in str(e.value)


def test_secrets_never_in_repr_or_str():
    cfg = config.load(full_env(TGAUD_WEB_PROXY_TOKEN=WEB_TOKEN), need=config.NEEDS)
    text = repr(cfg) + str(cfg) + repr(cfg.bot_token) + str(cfg.link_hmac_key)
    for secret in (TOKEN, PW, KEY, WEB_TOKEN, bytes.fromhex(KEY).decode("latin-1")):
        assert secret not in text
    assert "Secret('***')" in text
    with pytest.raises(TypeError):
        import pickle

        pickle.dumps(cfg.bot_token)


def test_web_listener_parsing_and_conditional_proxy_token():
    for raw in (None, ""):
        env = full_env() if raw is None else full_env(TGAUD_WEB_LISTEN=raw)
        cfg = config.load(env, need={"database"})
        assert cfg.web_listen is None
        with pytest.raises(config.ConfigError, match="off"):
            cfg.web_proxy_token_or_fail()
    # a web-enabled env file without the token: migrate and botcheck still load
    web = full_env(TGAUD_WEB_LISTEN="0.0.0.0:8097")
    cfg = config.load(web, need={"database"})
    assert str(cfg.web_listen) == "0.0.0.0:8097"
    assert config.load(web, need={"bot"}).bot_token is not None
    with pytest.raises(config.ConfigError, match="TGAUD_WEB_PROXY_TOKEN"):
        cfg.web_proxy_token_or_fail()
    cfg = config.load(full_env(TGAUD_WEB_LISTEN="0.0.0.0:8097", TGAUD_WEB_PROXY_TOKEN=WEB_TOKEN))
    assert cfg.web_proxy_token_or_fail().reveal() == WEB_TOKEN


def test_migrate_and_botcheck_run_with_a_web_env_file_without_the_token(monkeypatch, capsys):
    from botapi_fake import FakeBotServer

    from tgaud import cli

    with FakeBotServer(token=TOKEN) as fake:
        env = {"TGAUD_BOT_TOKEN": TOKEN, "TGAUD_BOT_API_URL": fake.url, "TGAUD_WEB_LISTEN": "0.0.0.0:8097"}
        for k in list(config.os.environ):
            if k.startswith("TGAUD_"):
                monkeypatch.delenv(k)
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        assert cli.main(["botcheck"]) == 0
        assert "bot id 5000" in capsys.readouterr().out
        # migrate gets as far as connecting (no database here): a connection failure, not a config error
        monkeypatch.setenv("TGAUD_DATABASE_URL", "postgresql://nobody@127.0.0.1:1/none?connect_timeout=1")
        assert cli.main(["migrate"]) == 1
        captured = capsys.readouterr()
        assert "configuration" not in captured.err and "TGAUD_WEB_PROXY_TOKEN" not in captured.err


def test_link_key_fingerprint_and_digest():
    cfg = config.load(full_env())
    fp = cfg.link_key_fingerprint()
    assert len(fp) == 64 and KEY not in fp
    assert fp == config.load(full_env()).link_key_fingerprint()
    assert fp != config.load(full_env(TGAUD_LINK_HMAC_KEY="cd" * 32)).link_key_fingerprint()
    assert len(cfg.link_digest("https://t.me/+abc")) == 32


def test_log_redaction(capsys):
    cfg = config.load(full_env(TGAUD_WEB_PROXY_TOKEN=WEB_TOKEN))
    config.setup_logging(cfg)
    log = logging.getLogger("tgaud.test")
    log.warning("url https://api.telegram.org/bot%s/getMe pw=%s key=%s web=%s", TOKEN, PW, KEY, WEB_TOKEN)
    try:
        raise RuntimeError(f"boom {TOKEN}\nsecond line")
    except RuntimeError:
        log.exception("failed")
    err = capsys.readouterr().err
    for secret in (TOKEN, PW, KEY, WEB_TOKEN):
        assert secret not in err
    assert err.count("\n") == 2  # one line per record, the traceback folded into its line
    assert logging.getLogger("httpx").level == logging.WARNING


def test_percent_encoded_password_key_never_shown():
    url = f"postgresql://tgaud@h:5432/tgaud?%70assword={PW}&sslmode=disable"
    env = full_env(TGAUD_DATABASE_URL=url)
    del env["TGAUD_DATABASE_PASSWORD"]
    cfg = config.load(env)
    assert PW not in repr(cfg) and PW not in cfg.database_url
    assert cfg.database_url == "postgresql://tgaud@h:5432/tgaud?sslmode=disable"
    assert psycopg.conninfo.conninfo_to_dict(cfg.dsn("x"))["password"] == PW  # still used to connect
    for bad in (f"postgresql://tgaud@h/tgaud?%70assword={PW}&bogus=1", f"postgresql://u:{PW}@h:x/d",
                f"postgres://tgaud@h/tgaud?%70assword={PW}&sslmode=nonsense%"):
        with pytest.raises(config.ConfigError) as e:
            config.load(full_env(TGAUD_DATABASE_URL=bad))
        assert PW not in str(e.value) and "TGAUD_DATABASE_URL" in str(e.value)


@pytest.mark.parametrize("url", ["http://127.0.0.1:abc", "http://127.0.0.1:0", "http://127.0.0.1:70000",
                                 "https://h/x#frag", "http://[::1]:x", "https://"])
def test_bot_api_url_is_fully_validated(url):
    with pytest.raises(config.ConfigError, match="TGAUD_BOT_API_URL"):
        config.load(full_env(TGAUD_BOT_API_URL=url))


def test_botcheck_with_a_bad_api_url_fails_cleanly(monkeypatch, capsys):
    from tgaud import cli

    for k in list(config.os.environ):
        if k.startswith("TGAUD_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("TGAUD_BOT_TOKEN", TOKEN)
    monkeypatch.setenv("TGAUD_BOT_API_URL", "http://127.0.0.1:abc")
    assert cli.main(["botcheck"]) == 1
    err = capsys.readouterr().err
    assert "TGAUD_BOT_API_URL" in err and "Traceback" not in err and TOKEN not in err
