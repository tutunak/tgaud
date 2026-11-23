"""Configuration from the environment: every variable of the deploy contract (docs/deploy-contract.md).

`load(env, need=...)` parses and validates everything once and returns a frozen Config. Secrets are held in
`Secret` objects whose repr and str are `Secret('***')`, so a Config can be printed or logged; error messages
name the variable, never its value. `need` says which of the optional-by-command values the caller requires:

    serve        need={"database", "bot", "link_key"}
    migrate      need={"database"}
    botcheck     need={"bot"}
    link create  need={"database", "bot", "link_key"}

`TGAUD_WEB_LISTEN` is parsed here (empty = web UI off), but the proxy token is required only by the code
that starts the web listener: it calls `config.web_proxy_token_or_fail()`.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import psycopg.conninfo

NEEDS = frozenset({"database", "bot", "link_key"})
LINK_KEY_FINGERPRINT_LABEL = b"tgaud link key fingerprint v1"
_LISTEN = re.compile(r"^(?P<host>\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9.\-]*):(?P<port>[0-9]{1,5})$")
_BOOL = {"true": True, "1": True, "yes": True, "on": True, "false": False, "0": False, "no": False, "off": False}
_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")


class ConfigError(ValueError):
    """Invalid or missing configuration; the message lists every problem, never a secret value."""


class Secret:
    """A secret string or bytes value. Only `reveal()` returns it."""

    __slots__ = ("_value",)

    def __init__(self, value: str | bytes) -> None:
        self._value = value

    def reveal(self) -> str | bytes:
        return self._value

    def __repr__(self) -> str:
        return "Secret('***')"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Secret) and hmac.compare_digest(_as_bytes(self._value), _as_bytes(other._value))

    def __hash__(self) -> int:
        return hash("Secret")

    def __reduce__(self):  # never pickled with its value
        raise TypeError("Secret cannot be pickled")


def _as_bytes(v: str | bytes) -> bytes:
    return v.encode() if isinstance(v, str) else v


@dataclass(frozen=True)
class Listen:
    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class Config:
    database_url: str | None  # the URL without any password
    database_password: Secret | None
    bot_token: Secret | None
    link_hmac_key: Secret | None  # bytes
    web_proxy_token: Secret | None
    metrics_listen: Listen
    web_listen: Listen | None  # None = web UI off
    store_names: bool
    log_level: str
    bot_api_url: str
    activation_id: str
    _conninfo: Mapping[str, str] = field(default_factory=dict, repr=False, compare=False)

    def dsn(self, job: str) -> str:
        """libpq conninfo for one job's connection: password, application_name tgaud-<job>, keepalives."""
        if self.database_url is None:
            raise ConfigError("TGAUD_DATABASE_URL is required")
        params = dict(self._conninfo)
        params.setdefault("connect_timeout", "10")
        # A process cut off from the server loses its sessions (and its advisory locks) within ~1 min.
        params.setdefault("keepalives", "1")
        params.setdefault("keepalives_idle", "30")
        params.setdefault("keepalives_interval", "10")
        params.setdefault("keepalives_count", "3")
        params["application_name"] = f"tgaud-{job}"
        if self.database_password is not None:
            params["password"] = str(self.database_password.reveal())
        return psycopg.conninfo.make_conninfo(**params)

    def secrets(self) -> list[str]:
        """Every secret value as text, for the log redaction filter."""
        out: list[str] = []
        for s in (self.database_password, self.bot_token, self.web_proxy_token):
            if s is not None and s.reveal():
                out.append(str(s.reveal()))
        if self.link_hmac_key is not None:
            out.append(bytes(self.link_hmac_key.reveal()).hex())  # type: ignore[arg-type]
        return out

    def link_key_fingerprint(self) -> str:
        """HMAC of a fixed label with the link key: stored by serve to detect a changed key, never the key."""
        key = self._link_key()
        return hmac.new(key, LINK_KEY_FINGERPRINT_LABEL, hashlib.sha256).hexdigest()

    def link_digest(self, invite_link: str) -> bytes:
        """HMAC-SHA256 of a bot-made invite link (tg.invite_link.link_digest); the link itself is never stored."""
        return hmac.new(self._link_key(), invite_link.encode(), hashlib.sha256).digest()

    def _link_key(self) -> bytes:
        if self.link_hmac_key is None:
            raise ConfigError("TGAUD_LINK_HMAC_KEY is required")
        return bytes(self.link_hmac_key.reveal())  # type: ignore[arg-type]

    def web_proxy_token_or_fail(self) -> Secret:
        """For the code that starts the web listener: the proxy token is required when the web UI listens."""
        if self.web_listen is None:
            raise ConfigError("the web UI is off (TGAUD_WEB_LISTEN is empty)")
        if self.web_proxy_token is None:
            raise ConfigError("TGAUD_WEB_PROXY_TOKEN is required when TGAUD_WEB_LISTEN is set")
        return self.web_proxy_token


def load(env: Mapping[str, str] | None = None, need: Iterable[str] = ()) -> Config:
    """Parse and validate the environment once; raises ConfigError listing every problem."""
    env = os.environ if env is None else env
    need = frozenset(need)
    unknown = need - NEEDS
    if unknown:
        raise ValueError(f"unknown needs: {sorted(unknown)}")
    problems: list[str] = []

    def get(name: str) -> str:
        return env.get(name, "").strip()

    def secret(name: str) -> Secret | None:
        v = env.get(name, "")
        v = v.strip()
        return Secret(v) if v else None

    # database
    database_url: str | None = None
    conninfo: dict[str, str] = {}
    password = secret("TGAUD_DATABASE_PASSWORD")
    raw_url = get("TGAUD_DATABASE_URL")
    if raw_url:
        parsed = _parse_database_url(raw_url, problems)
        if parsed is not None:
            conninfo, url_password, database_url = parsed
            if password is None and url_password:
                password = Secret(url_password)
    elif "database" in need:
        problems.append("TGAUD_DATABASE_URL is required")

    bot_token = secret("TGAUD_BOT_TOKEN")
    if bot_token is None and "bot" in need:
        problems.append("TGAUD_BOT_TOKEN is required")
    elif bot_token is not None and not re.fullmatch(r"[0-9]+:[A-Za-z0-9_\-]+", str(bot_token.reveal())):
        problems.append("TGAUD_BOT_TOKEN is not a Bot API token (digits, a colon, then letters, digits, _ or -)")

    link_key: Secret | None = None
    raw_key = get("TGAUD_LINK_HMAC_KEY")
    if raw_key:
        try:
            key = bytes.fromhex(raw_key)
        except ValueError:
            problems.append("TGAUD_LINK_HMAC_KEY must be hex")
        else:
            if len(key) < 32:
                problems.append(f"TGAUD_LINK_HMAC_KEY must be at least 32 bytes (64 hex digits), got {len(key)}")
            else:
                link_key = Secret(key)
    elif "link_key" in need:
        problems.append("TGAUD_LINK_HMAC_KEY is required")

    web_proxy_token = secret("TGAUD_WEB_PROXY_TOKEN")

    metrics_listen = _listen("TGAUD_METRICS_LISTEN", get("TGAUD_METRICS_LISTEN") or "0.0.0.0:9108", problems)
    raw_web = get("TGAUD_WEB_LISTEN")
    web_listen = _listen("TGAUD_WEB_LISTEN", raw_web, problems) if raw_web else None

    store_names = True
    raw_store = get("TGAUD_STORE_NAMES")
    if raw_store:
        if raw_store.lower() not in _BOOL:
            problems.append("TGAUD_STORE_NAMES must be true or false")
        else:
            store_names = _BOOL[raw_store.lower()]

    log_level = (get("TGAUD_LOG_LEVEL") or "INFO").upper()
    if log_level == "WARN":
        log_level = "WARNING"
    if log_level not in _LEVELS:
        problems.append(f"TGAUD_LOG_LEVEL must be one of {', '.join(_LEVELS)}")

    bot_api_url = (get("TGAUD_BOT_API_URL") or "https://api.telegram.org").rstrip("/")
    u = urlsplit(bot_api_url)
    if u.scheme not in ("http", "https") or not u.hostname or u.username or u.password or u.query:
        problems.append("TGAUD_BOT_API_URL must be an http(s) URL without credentials or query")

    activation_id = env.get("TGAUD_ACTIVATION_ID", "").strip()
    if any(ch in activation_id for ch in "\r\n"):
        problems.append("TGAUD_ACTIVATION_ID must be one line")

    if problems:
        raise ConfigError("; ".join(problems))
    return Config(
        database_url=database_url,
        database_password=password,
        bot_token=bot_token,
        link_hmac_key=link_key,
        web_proxy_token=web_proxy_token,
        metrics_listen=metrics_listen,
        web_listen=web_listen,
        store_names=store_names,
        log_level=log_level,
        bot_api_url=bot_api_url,
        activation_id=activation_id,
        _conninfo=conninfo,
    )


def _parse_database_url(raw: str, problems: list[str]) -> tuple[dict[str, str], str | None, str] | None:
    """(conninfo without password, the URL's password, the URL without password) or None after a problem."""
    if not (raw.startswith("postgresql://") or raw.startswith("postgres://")):
        problems.append("TGAUD_DATABASE_URL must start with postgresql://")
        return None
    try:
        info = psycopg.conninfo.conninfo_to_dict(raw)
    except psycopg.ProgrammingError:
        # libpq's message may quote parts of the URL; it is not shown.
        problems.append(f"TGAUD_DATABASE_URL is not a valid PostgreSQL URL: {_redact_url(raw)}")
        return None
    url_password = info.pop("password", None)
    if not info.get("dbname"):
        problems.append("TGAUD_DATABASE_URL must name a database")
    clean = {k: str(v) for k, v in info.items() if v is not None}
    return clean, (str(url_password) if url_password else None), _redact_url(raw, keep_user=True)


def _redact_url(raw: str, keep_user: bool = True) -> str:
    """The URL with any password removed (user@host stays)."""
    m = re.match(r"^(?P<scheme>[a-z]+://)(?P<auth>[^@/]*@)?(?P<rest>.*)$", raw)
    if not m:
        return "<unparseable>"
    auth = m.group("auth") or ""
    if auth:
        user = auth[:-1].split(":", 1)[0]
        auth = f"{user}@" if keep_user and user else ""
    rest = re.sub(r"(?i)([?&]password=)[^&]*", r"\1***", m.group("rest"))
    return m.group("scheme") + auth + rest


def _listen(name: str, raw: str, problems: list[str]) -> Listen:
    m = _LISTEN.match(raw)
    if not m or not 0 < int(m.group("port")) < 65536:
        problems.append(f"{name} must be host:port, e.g. 0.0.0.0:9108")
        return Listen("0.0.0.0", 9108)
    host = m.group("host").strip("[]") or "0.0.0.0"
    return Listen(host, int(m.group("port")))


class RedactingFormatter(logging.Formatter):
    """One line per record (journald), with every known secret replaced by ***."""

    def __init__(self, secrets: Iterable[str] = (), fmt: str | None = None) -> None:
        super().__init__(fmt or "%(levelname)s %(name)s: %(message)s")
        self.secrets = sorted({s for s in secrets if s}, key=len, reverse=True)

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        for s in self.secrets:
            text = text.replace(s, "***")
        return text.replace("\r", "").replace("\n", " | ")


def setup_logging(config: Config | None, level: str | None = None) -> None:
    """Logs to stderr, one line per record, secrets redacted; httpx request lines (they carry the token in
    the URL) are kept at WARNING."""
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler()
    handler.setFormatter(RedactingFormatter(config.secrets() if config else ()))
    root.addHandler(handler)
    root.setLevel(level or (config.log_level if config else "INFO"))
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
