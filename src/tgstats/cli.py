"""Command line interface: login, chats, collect, unique, intersect, overlap."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import os
import sys
from pathlib import Path
from typing import Any, AsyncIterator, Sequence

from . import __version__, analysis, collector, db
from .analysis import AnalysisError
from .config import Config, ConfigError, load_config, load_db_path
from .db import DatabaseNotFound, UserRow


class CliError(Exception):
    """A runtime problem reported as one line; exit status 1."""


# ---------------------------------------------------------------- output helpers


def table(header: Sequence[str], rows: Sequence[Sequence[str]], right: Sequence[int] = ()) -> str:
    cells = [list(map(str, r)) for r in rows]
    widths = [max(len(str(h)), *(len(r[i]) for r in cells)) if cells else len(str(h)) for i, h in enumerate(header)]

    def fmt(row: Sequence[str]) -> str:
        parts = [str(v).rjust(w) if i in right else str(v).ljust(w) for i, (v, w) in enumerate(zip(row, widths))]
        return "  ".join(parts).rstrip()

    return "\n".join([fmt(header), fmt(["-" * w for w in widths]), *(fmt(r) for r in cells)])


def label(chat: Any) -> str:
    return f"{chat.title} ({chat.id})"


def user_line(user: UserRow) -> str:
    handle = f"@{user.username}" if user.username else "-"
    return f"{user.id}\t{handle}\t{user.name or '-'}"


def write_users_csv(path: str, rows: Sequence[tuple[Any, UserRow]] | Sequence[UserRow], with_chat: bool) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        header = ["user_id", "username", "first_name", "last_name"]
        writer.writerow((["chat_id", "chat_title"] + header) if with_chat else header)
        for item in rows:
            chat, user = item if with_chat else (None, item)  # type: ignore[misc]
            fields = [user.id, user.username or "", user.first_name or "", user.last_name or ""]
            writer.writerow(([chat.id, chat.title] + fields) if with_chat else fields)


def sorted_users(members: dict[int, UserRow], ids: Sequence[int] | frozenset[int]) -> list[UserRow]:
    return sorted((members[i] for i in ids), key=lambda u: u.id)


def warn(text: str | None) -> None:
    if text:
        print(text, file=sys.stderr)


def bots_note(include_bots: bool) -> None:
    if not include_bots:
        print("Bots and deleted accounts are excluded (use --include-bots to keep them).")


# ---------------------------------------------------------------- telegram plumbing


def _prepare_session(session_path: str) -> None:
    """Make the session file owner-only before Telethon opens it.

    The session is a login for the account. Create it empty with mode 0600 (SQLite
    accepts an empty file) or tighten an existing one; failures are raised, not hidden.
    """
    path = session_path if session_path.endswith(".session") else session_path + ".session"
    try:
        os.close(os.open(path, os.O_RDWR | os.O_CREAT, 0o600))
        os.chmod(path, 0o600)
    except OSError as exc:
        raise CliError(f"cannot secure the session file {path}: {exc}") from exc


def make_client(cfg: Config, delay: tuple[int, int] | None = None) -> Any:
    """A Telegram client; with a delay range, every request it sends waits a random time in it first."""
    _prepare_session(cfg.session_path)
    # flood_sleep_threshold=0: every FloodWaitError reaches the collector, which prints it
    client = collector.PacedTelegramClient(cfg.session_path, cfg.api_id, cfg.api_hash, flood_sleep_threshold=0)
    if delay is not None:
        client.pacer = collector.Pacer(*delay)
    return client


def delay_range(args: argparse.Namespace) -> tuple[int, int]:
    low, high = args.min_delay_ms, args.max_delay_ms
    if not 0 <= low <= high:
        raise ConfigError(f"need 0 <= --min-delay-ms <= --max-delay-ms, got {low} and {high}")
    return low, high


@contextlib.asynccontextmanager
async def connected_client(cfg: Config, delay: tuple[int, int]) -> AsyncIterator[Any]:
    """A logged-in client whose requests are paced by a random delay in the given ms range."""
    print(f"Pausing {delay[0]}-{delay[1]} ms at random before each Telegram request.", file=sys.stderr)
    client = make_client(cfg, delay)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise CliError("not logged in: run `tgstats login` first")
        yield client
    finally:
        await client.disconnect()


def describe_user(me: Any) -> str:
    name = " ".join(p for p in (me.first_name, me.last_name) if p) or "(no name)"
    handle = f" @{me.username}" if me.username else ""
    return f"{name}{handle} (id {me.id})"


# ---------------------------------------------------------------- commands


def cmd_login(args: argparse.Namespace) -> int:
    cfg = load_config(args.session, args.db)

    async def run() -> None:
        client = make_client(cfg)
        try:
            await client.start()  # asks for phone, code and 2FA password when needed
            me = await client.get_me()
            print(f"Logged in as {describe_user(me)}")
            print(f"Session saved to {cfg.session_path}: it grants full access to this account, keep it private.")
        finally:
            await client.disconnect()

    asyncio.run(run())
    return 0


def cmd_chats(args: argparse.Namespace) -> int:
    delay = delay_range(args)
    cfg = load_config(args.session, args.db)

    async def run() -> list[tuple[collector.AdminChat, int | None]]:
        async with connected_client(cfg, delay) as client:
            chats = await collector.list_admin_chats(client)
            out = []
            for chat in chats:
                try:
                    reported = await collector.reported_count(client, chat)
                except Exception:
                    reported = None
                out.append((chat, reported))
            return out

    found = asyncio.run(run())
    last: dict[int, str] = {}
    if Path(cfg.db_path).exists():
        conn = db.connect(cfg.db_path)
        last = {cid: s.taken_at for cid, s in db.latest_snapshots(conn).items()}
        conn.close()
    if not found:
        print("No chats where this account is creator or admin.")
        return 0
    rows = [
        [c.id, c.type, c.title, f"@{c.username}" if c.username else "-", "?" if n is None else n, last.get(c.id, "never")]
        for c, n in found
    ]
    print(table(["ID", "TYPE", "TITLE", "USERNAME", "MEMBERS", "LAST COLLECTED"], rows, right=[0, 4]))
    return 0


def cmd_collect(args: argparse.Namespace) -> int:
    delay = delay_range(args)
    if args.expand_at < 0:
        raise ConfigError(f"--expand-at must be 0 or more, got {args.expand_at}")
    cfg = load_config(args.session, args.db)
    conn = db.connect(cfg.db_path)

    async def run() -> list[collector.CollectResult]:
        async with connected_client(cfg, delay) as client:
            return await collector.collect(
                client, conn, args.chats or None, deep=args.deep, expand_at=args.expand_at
            )

    try:
        results = asyncio.run(run())
    finally:
        conn.close()
    failed = [r for r in results if r.error]
    short = [r for r in results if not r.error and not r.complete]
    print(f"Done: {len(results) - len(failed)} collected, {len(failed)} failed.")
    if short:
        print(
            "Incomplete snapshots: "
            + ", ".join(r.chat.title for r in short)
            + ". Telegram returned fewer members than reported; see the README caveat."
        )
        if not args.deep:
            print("To search for more members, run `tgstats collect --deep CHAT`; it is slow (hours for a big chat).")
    return 1 if failed else 0


def open_db(args: argparse.Namespace) -> Any:
    return db.connect(load_db_path(args.db), create=False)


def cmd_unique(args: argparse.Namespace) -> int:
    conn = open_db(args)
    try:
        sets = analysis.load_sets(conn, args.among, args.include_bots)
    finally:
        conn.close()
    results = analysis.unique_per_chat(sets)
    shown = results
    if args.chat:
        target = analysis.resolve_chat(args.chat, [s.chat for s in sets])
        shown = [r for r in results if r.chat_set.chat.id == target.id]
        if not shown:
            raise AnalysisError(f"chat {args.chat!r} is not among the compared chats")

    warn(analysis.incomplete_warning(sets, "unique"))
    rows = [
        [label(r.chat_set.chat), len(r.unique), r.total, f"{r.percent:.1f}%", r.chat_set.snapshot.taken_at]
        for r in shown
    ]
    print(f"Compared chats: {len(sets)}")
    print(table(["CHAT", "UNIQUE", "TOTAL", "UNIQUE %", "SNAPSHOT"], rows, right=[1, 2, 3]))
    bots_note(args.include_bots)
    if args.list:
        for r in shown:
            print(f"\n{label(r.chat_set.chat)}: {len(r.unique)} unique")
            for user in sorted_users(r.chat_set.members, r.unique):
                print(user_line(user))
    if args.csv:
        write_users_csv(
            args.csv,
            [(r.chat_set.chat, u) for r in shown for u in sorted_users(r.chat_set.members, r.unique)],
            with_chat=True,
        )
        print(f"Wrote {args.csv}")
    return 0


def cmd_intersect(args: argparse.Namespace) -> int:
    conn = open_db(args)
    try:
        sets = analysis.load_sets(conn, [args.chat_a, args.chat_b], args.include_bots)
    finally:
        conn.close()
    if len(sets) != 2:
        raise AnalysisError("CHAT_A and CHAT_B are the same chat; pick two different ones")
    a, b = sets
    result = analysis.intersect(a, b)

    warn(analysis.incomplete_warning(sets, "intersect"))
    print(f"A: {label(a.chat)}: {len(a.members)} members")
    print(f"B: {label(b.chat)}: {len(b.members)} members")
    print(f"Common users: {len(result.common)}")
    print(f"  {result.percent_of_a:.1f}% of A")
    print(f"  {result.percent_of_b:.1f}% of B")
    print(f"Jaccard index: {result.jaccard:.3f}")
    bots_note(args.include_bots)
    users = sorted_users(a.members, result.common)
    if args.list:
        print()
        for user in users:
            print(user_line(user))
    if args.csv:
        write_users_csv(args.csv, users, with_chat=False)
        print(f"Wrote {args.csv}")
    return 0


def cmd_overlap(args: argparse.Namespace) -> int:
    conn = open_db(args)
    try:
        sets = analysis.load_sets(conn, args.among, args.include_bots)
    finally:
        conn.close()
    matrix = analysis.overlap_matrix(sets)

    warn(analysis.incomplete_warning(sets, "overlap"))
    for i, s in enumerate(sets, 1):
        print(f"{i:>3} = {label(s.chat)} ({len(s.members)} members)")
    print("\nCommon users per pair (diagonal: the chat's own member count)")
    header = [""] + [str(i) for i in range(1, len(sets) + 1)]
    rows = [[str(i)] + [str(n) for n in row] for i, row in enumerate(matrix, 1)]
    print(table(header, rows, right=range(1, len(header))))
    bots_note(args.include_bots)
    return 0


# ---------------------------------------------------------------- parser


def build_parser() -> argparse.ArgumentParser:
    db_flag = argparse.ArgumentParser(add_help=False)
    db_flag.add_argument("--db", metavar="PATH", help="SQLite database (env TG_DB, default tgstats.db)")
    tg_flags = argparse.ArgumentParser(add_help=False, parents=[db_flag])
    tg_flags.add_argument("--session", metavar="PATH", help="session file (env TG_SESSION, default tgstats.session)")
    bots = argparse.ArgumentParser(add_help=False)
    bots.add_argument("--include-bots", action="store_true", help="keep bots and deleted accounts in the analysis")
    out_flags = argparse.ArgumentParser(add_help=False)
    out_flags.add_argument("--list", action="store_true", help="print the users (id, @username, name)")
    out_flags.add_argument("--csv", metavar="FILE", help="write the users to a CSV file")
    pace = argparse.ArgumentParser(add_help=False)
    pace.add_argument(
        "--min-delay-ms", type=int, default=collector.MIN_DELAY_MS, metavar="MS",
        help=f"shortest random pause before each Telegram request (default {collector.MIN_DELAY_MS})",
    )
    pace.add_argument(
        "--max-delay-ms", type=int, default=collector.MAX_DELAY_MS, metavar="MS",
        help=f"longest random pause before each Telegram request (default {collector.MAX_DELAY_MS})",
    )

    parser = argparse.ArgumentParser(
        prog="tgstats",
        description="Collect the members of the Telegram chats you administer and compare their audiences.",
        epilog="Needs TG_API_ID and TG_API_HASH (https://my.telegram.org) in the environment or a .env file.",
    )
    parser.add_argument("--version", action="version", version=f"tgstats {__version__}")
    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    p = sub.add_parser("login", parents=[tg_flags], help="log in to Telegram (phone, code, 2FA password)")
    p.set_defaults(func=cmd_login)

    p = sub.add_parser("chats", parents=[tg_flags, pace], help="list chats where you are creator or admin")
    p.set_defaults(func=cmd_chats)

    p = sub.add_parser("collect", parents=[tg_flags, pace], help="fetch member lists and store a new snapshot")
    p.add_argument("chats", nargs="*", metavar="CHAT", help="numeric id, @username or exact title (default: all admin chats)")
    p.add_argument(
        "--deep",
        action="store_true",
        help="when the listing is short, also search members by one character, then by two (Latin, digits, Cyrillic), and merge",
    )
    p.add_argument(
        "--expand-at",
        type=int,
        default=collector.DEFAULT_EXPAND_AT,
        metavar="N",
        help="with --deep: search two-character queries only for first characters whose single search "
        f"returned at least N members (default {collector.DEFAULT_EXPAND_AT}; 0 searches them all)",
    )
    p.set_defaults(func=cmd_collect)

    p = sub.add_parser("unique", parents=[db_flag, bots, out_flags], help="members who are in no other compared chat")
    p.add_argument("--among", nargs="+", metavar="CHAT", help="compare only these chats (default: every collected chat)")
    p.add_argument("--chat", metavar="CHAT", help="show only this chat")
    p.set_defaults(func=cmd_unique)

    p = sub.add_parser("intersect", parents=[db_flag, bots, out_flags], help="users in both of two chats")
    p.add_argument("chat_a", metavar="CHAT_A")
    p.add_argument("chat_b", metavar="CHAT_B")
    p.set_defaults(func=cmd_intersect)

    p = sub.add_parser("overlap", parents=[db_flag, bots], help="matrix of pairwise common-user counts")
    p.add_argument("--among", nargs="+", metavar="CHAT", help="compare only these chats (default: every collected chat)")
    p.set_defaults(func=cmd_overlap)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (ConfigError, AnalysisError, DatabaseNotFound) as exc:
        print(f"tgstats: error: {exc}", file=sys.stderr)
        return 2
    except CliError as exc:
        print(f"tgstats: error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except OSError as exc:
        print(f"tgstats: error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
