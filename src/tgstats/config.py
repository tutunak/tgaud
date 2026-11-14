"""Settings from the environment and an optional .env file in the working directory."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_SESSION = "tgstats.session"
DEFAULT_DB = "tgstats.db"
CREDENTIALS_URL = "https://my.telegram.org"


class ConfigError(Exception):
    """A setting is missing or malformed; the message is shown to the user as is."""


@dataclass(frozen=True)
class Config:
    api_id: int
    api_hash: str
    session_path: str
    db_path: str


def parse_env_file(path: Path) -> dict[str, str]:
    """Parse KEY=VALUE lines; ignores blanks, comments and a leading `export`."""
    values: dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return values
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, _, value = line.partition("=")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def _lookup(environ: dict[str, str], dotenv: dict[str, str], name: str) -> str | None:
    value = environ.get(name)
    if value is None or value == "":
        value = dotenv.get(name)
    return value if value else None


def load_db_path(
    cli_db: str | None = None,
    environ: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> str:
    """Database path only; analysis commands need no Telegram credentials."""
    environ = dict(os.environ) if environ is None else environ
    dotenv = parse_env_file((cwd or Path.cwd()) / ".env")
    return cli_db or _lookup(environ, dotenv, "TG_DB") or DEFAULT_DB


def load_config(
    cli_session: str | None = None,
    cli_db: str | None = None,
    environ: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> Config:
    """Full config for commands that talk to Telegram. Real env vars beat .env."""
    environ = dict(os.environ) if environ is None else environ
    dotenv = parse_env_file((cwd or Path.cwd()) / ".env")

    raw_id = _lookup(environ, dotenv, "TG_API_ID")
    raw_hash = _lookup(environ, dotenv, "TG_API_HASH")
    where = f"get them at {CREDENTIALS_URL} (API development tools) and set them in the environment or a .env file"
    if raw_id is None:
        raise ConfigError(f"TG_API_ID is not set: {where}.")
    if raw_hash is None:
        raise ConfigError(f"TG_API_HASH is not set: {where}.")
    try:
        api_id = int(raw_id)
    except ValueError:
        raise ConfigError(f"TG_API_ID must be a number (got {raw_id!r}): see {CREDENTIALS_URL}.") from None

    return Config(
        api_id=api_id,
        api_hash=raw_hash,
        session_path=cli_session or _lookup(environ, dotenv, "TG_SESSION") or DEFAULT_SESSION,
        db_path=cli_db or _lookup(environ, dotenv, "TG_DB") or DEFAULT_DB,
    )
