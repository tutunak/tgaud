"""The `tgaud` command (deploy contract, Commands):

    tgaud serve         the service; 3 when the schema is behind, 2 on a config error or another running serve
    tgaud migrate       apply pending migrations; prints the version; 0 ok, 1 failure
    tgaud --selfcheck   offline check of config, SQL files and modules; prints `selfcheck ok: ...`
    tgaud botcheck      getMe with TGAUD_BOT_TOKEN, prints the bot id; no database
    tgaud <report> ...  report commands registered by the modules of tgaud.commands (cli task)

Report commands register with `register(name, help, add_arguments, run)`: `add_arguments(parser)` adds their
options, `run(args) -> int` does the work (0 ok, 1 failure; argparse exits 2 on a usage error). They load
their own config, e.g. `tgaud.config.load(need={"database"})`, and connect with `tgaud.db.connect(config, "cli")`.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from tgaud import config as config_mod

log = logging.getLogger("tgaud")


@dataclass(frozen=True)
class Command:
    name: str
    help: str
    add_arguments: Callable[[argparse.ArgumentParser], None]
    run: Callable[[argparse.Namespace], int]


COMMANDS: dict[str, Command] = {}
BUILTIN = ("serve", "migrate", "botcheck")


def register(name: str, help: str, add_arguments: Callable[[argparse.ArgumentParser], None],
             run: Callable[[argparse.Namespace], int]) -> Command:
    """Add a report command; called by the modules of tgaud.commands when imported."""
    if name in BUILTIN or name in COMMANDS:
        raise ValueError(f"command {name!r} is already registered")
    cmd = Command(name, help, add_arguments, run)
    COMMANDS[name] = cmd
    return cmd


def build_parser() -> argparse.ArgumentParser:
    from tgaud import commands

    commands.load_all()
    p = argparse.ArgumentParser(prog="tgaud", description="Telegram Bot API audience statistics service")
    p.add_argument("--selfcheck", action="store_true",
                   help="offline check: config, every SQL file, every module; prints 'selfcheck ok: ...'")
    sub = p.add_subparsers(dest="command", metavar="COMMAND")
    sub.add_parser("serve", help="run the service: ingest, worker, batch, /metrics and /health")
    sub.add_parser("migrate", help="apply pending database migrations and print the schema version")
    sub.add_parser("botcheck", help="call getMe with TGAUD_BOT_TOKEN and print the bot id (no database)")
    for cmd in COMMANDS.values():
        sp = sub.add_parser(cmd.name, help=cmd.help)
        cmd.add_arguments(sp)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    supervisor = None
    if argv[:1] == ["serve"]:
        # The supervisor first, before any config parsing, schema check or startup (tgaud.jobs); argparse
        # still checks the arguments (`serve --help`, a typo) before anything touches the database.
        from tgaud.jobs import Supervisor

        supervisor = Supervisor().install()
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.selfcheck:
        if args.command:
            parser.error("--selfcheck takes no command")
        from tgaud import selfcheck

        return selfcheck.main()
    if args.command is None:
        parser.print_help(sys.stderr)
        return 2
    if args.command == "migrate":
        return cmd_migrate()
    if args.command == "botcheck":
        return cmd_botcheck()
    if args.command == "serve":
        if supervisor is None:  # e.g. `tgaud --some-flag serve`: install it now, still before any I/O
            from tgaud.jobs import Supervisor

            supervisor = Supervisor().install()
        return cmd_serve(supervisor)
    config_mod.setup_logging(None)
    try:
        return COMMANDS[args.command].run(args)
    except config_mod.ConfigError as e:
        print(f"tgaud {args.command}: configuration: {e}", file=sys.stderr)
        return 1


def cmd_serve(supervisor) -> int:
    from tgaud import jobs

    try:
        cfg = config_mod.load(need={"database", "bot", "link_key"})
    except config_mod.ConfigError as e:
        config_mod.setup_logging(None)
        log.error("configuration: %s", e)
        return 2
    config_mod.setup_logging(cfg)
    try:
        return jobs.main_serve(cfg, supervisor)
    except Exception:
        log.exception("serve failed")
        return 1


def cmd_migrate() -> int:
    import psycopg

    from tgaud import db

    try:
        cfg = config_mod.load(need={"database"})
    except config_mod.ConfigError as e:
        print(f"tgaud migrate: configuration: {e}", file=sys.stderr)
        return 1
    config_mod.setup_logging(cfg)
    try:
        with db.connect(cfg, "migrate") as conn:
            result = db.migrate(conn)
    except (psycopg.Error, db.MigrationError) as e:
        log.error("migrate failed: %s", e)
        return 1
    applied = ", ".join(f"{v:03d}" for v in result.applied) or "none"
    print(f"schema at {result.version:03d} (applied now: {applied})" if result.version else "schema empty")
    return 0


def cmd_botcheck() -> int:
    from tgaud.botapi import ApiError, BotApi

    try:
        cfg = config_mod.load(need={"bot"})
    except config_mod.ConfigError as e:
        print(f"tgaud botcheck: configuration: {e}", file=sys.stderr)
        return 1
    config_mod.setup_logging(cfg)
    try:
        with BotApi(str(cfg.bot_token.reveal()), cfg.bot_api_url) as api:
            me = api.get_me()
    except ApiError as e:
        log.error("botcheck failed: %s", e)
        return 1
    print(f"bot id {me['id']} (@{me.get('username', '')})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
