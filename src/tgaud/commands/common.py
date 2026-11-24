"""Plumbing of the report commands: connection, chat and user resolution, report header, table and CSV output.

Every report runs one block of queries.sql through tgaud.sql in one REPEATABLE READ read-only transaction, so
the header (as-of time, chats in scope, coverage, gap flags) and the table describe the same data. The header
goes to stdout before the table; with --csv the table is the only thing on stdout and the header goes to stderr.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import datetime as dt
import functools
import re
import sys
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, TextIO

import psycopg
from psycopg import IsolationLevel

from tgaud import config as config_mod
from tgaud import db, sql

UTC = dt.timezone.utc

# Columns that hold a chat id: the table shows "title (id)" beside them unless the result names its chats itself.
CHAT_COLUMNS = frozenset({"chat_id", "chat_a", "chat_b", "from_chat", "to_chat", "channel_id", "group_id"})


class CliError(Exception):
    """A problem with the request or the data; shown as one line, exit status 1."""


def command(name: str) -> Callable:
    """Wrap run(args) -> int of a report command: CliError and database errors become one line and status 1."""

    def deco(fn: Callable[[argparse.Namespace], int]) -> Callable[[argparse.Namespace], int]:
        @functools.wraps(fn)
        def wrapper(args: argparse.Namespace) -> int:
            try:
                return fn(args)
            except CliError as e:
                print(f"tgaud {name}: {e}", file=sys.stderr)
            except db.MaintenanceBlocked as e:
                print(f"tgaud {name}: {e}", file=sys.stderr)
            except (ValueError, TypeError) as e:  # tgaud.sql refuses a bad parameter
                print(f"tgaud {name}: {e}", file=sys.stderr)
            except psycopg.Error as e:
                print(f"tgaud {name}: database error: {str(e).strip().splitlines()[0] if str(e).strip() else e!r}",
                      file=sys.stderr)
            except BrokenPipeError:
                try:
                    sys.stdout.close()
                except Exception:
                    pass
            return 1

        return wrapper

    return deco


# ------------------------------------------------------------------------------------------------------
# Connection
# ------------------------------------------------------------------------------------------------------


@contextlib.contextmanager
def session(*, need: Sequence[str] = ("database",), read_only: bool = True
            ) -> Iterator[tuple[psycopg.Connection, config_mod.Config]]:
    """Config, a connection as job `cli`, and a schema check. Reports are read-only and REPEATABLE READ."""
    cfg = config_mod.load(need=set(need))
    try:
        conn = db.connect(cfg, "cli")
    except psycopg.Error as e:
        raise CliError(f"cannot connect to the database: {str(e).strip().splitlines()[0]}") from None
    try:
        status = db.schema_status(conn)
        if status.state == "behind":
            raise CliError("the database schema is behind this image: run `tgaud migrate` first "
                           f"(missing migrations {', '.join(f'{v:03d}' for v in status.missing)})")
        if read_only:
            conn.read_only = True
            conn.isolation_level = IsolationLevel.REPEATABLE_READ
        yield conn, cfg
    finally:
        with contextlib.suppress(psycopg.Error):
            conn.rollback()
        conn.close()


# ------------------------------------------------------------------------------------------------------
# Arguments
# ------------------------------------------------------------------------------------------------------


def number(text: str) -> Decimal:
    try:
        d = Decimal(text)
    except InvalidOperation:
        raise argparse.ArgumentTypeError(f"{text!r} is not a number") from None
    if not d.is_finite() or d < 0:
        raise argparse.ArgumentTypeError(f"{text!r} is not a non-negative number")
    return d


def positive_int(text: str) -> int:
    try:
        n = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r} is not an integer") from None
    if n < 1:
        raise argparse.ArgumentTypeError(f"{text!r} must be at least 1")
    return n


def parse_time(text: str, option: str = "time") -> dt.datetime:
    """`now`, an ISO date (00:00 UTC) or an ISO date and time; without an offset it is UTC."""
    raw = text.strip()
    if raw.lower() == "now":
        return dt.datetime.now(UTC)
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            return dt.datetime.combine(dt.date.fromisoformat(raw), dt.time(), UTC)
        value = dt.datetime.fromisoformat(raw)
    except ValueError:
        raise CliError(f"{option}: {text!r} is not a date or a time (use 2026-11-23 or 2026-11-23T12:00:00+00:00)") from None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def add_among(p: argparse.ArgumentParser) -> None:
    p.add_argument("--among", action="extend", nargs="+", metavar="CHAT", default=[],
                   help="limit the report to these chats (id, @username or title; repeatable; the default is "
                        "every tracked chat). Put positional arguments before --among.")


def add_as_of(p: argparse.ArgumentParser) -> None:
    p.add_argument("--as-of", metavar="TIME", help="report time (default: now); a date means 00:00 UTC")


def add_csv(p: argparse.ArgumentParser) -> None:
    p.add_argument("--csv", action="store_true", help="write the table as CSV to stdout (the header goes to stderr)")


# ------------------------------------------------------------------------------------------------------
# Chats and users
# ------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Chat:
    id: int
    title: str
    username: str | None
    tracked: bool


def describe(chat: Chat) -> str:
    handle = f" @{chat.username}" if chat.username else ""
    return f"{chat.id}{handle} {chat.title!r}"


def load_chats(conn: psycopg.Connection) -> list[Chat]:
    rows = conn.execute("SELECT chat_id, title, username, tracked FROM tg.chat ORDER BY chat_id").fetchall()
    return [Chat(*r) for r in rows]


def resolve_chat(ref: str, chats: Sequence[Chat]) -> Chat:
    """Match a numeric id (marked or bare), an @username or an exact title; ambiguity is an error."""
    ref = ref.strip()
    matches: list[Chat] = []
    if ref.startswith("@"):
        name = ref[1:].lower()
        matches = [c for c in chats if c.username and c.username.lower() == name]
    else:
        try:
            n = int(ref)
        except ValueError:
            n = None
        if n is not None:
            ids = {n, -n, -(10**12 + n)}  # the marked id (-100123...) as well as the bare channel / group id
            matches = [c for c in chats if c.id in ids]
        if not matches:
            matches = [c for c in chats if c.title == ref]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        known = "; ".join(describe(c) for c in chats if c.tracked) or "none"
        raise CliError(f"unknown chat {ref!r}; tracked chats: {known}")
    raise CliError(f"ambiguous chat {ref!r}; candidates: {'; '.join(describe(c) for c in matches)}; "
                   "use the numeric id")


def resolve_tracked(ref: str, chats: Sequence[Chat]) -> Chat:
    chat = resolve_chat(ref, chats)
    if not chat.tracked:
        raise CliError(f"chat {describe(chat)} is not tracked")
    return chat


def resolve_scope(refs: Sequence[str], chats: Sequence[Chat]) -> list[int] | None:
    """The chat ids of --among (tracked chats, in the order given, no repeats); None = every tracked chat."""
    if not refs:
        return None
    out: list[int] = []
    for ref in refs:
        chat = resolve_tracked(ref, chats)
        if chat.id not in out:
            out.append(chat.id)
    return out


def resolve_user(conn: psycopg.Connection, ref: str) -> int:
    """A user id, or an @username looked up in tg.tg_user (the latest username; none when names are not kept)."""
    ref = ref.strip()
    if ref.startswith("@"):
        rows = conn.execute("SELECT user_id, username FROM tg.tg_user WHERE lower(username) = lower(%s) "
                            "ORDER BY user_id", (ref[1:],)).fetchall()
        if not rows:
            raise CliError(f"no user with username {ref!r} (names are only stored when TGAUD_STORE_NAMES is on)")
        if len(rows) > 1:
            raise CliError(f"ambiguous username {ref!r}; candidates: {', '.join(str(r[0]) for r in rows)}; "
                           "use the numeric id")
        return int(rows[0][0])
    try:
        return int(ref)
    except ValueError:
        raise CliError(f"{ref!r} is neither a user id nor an @username") from None


# ------------------------------------------------------------------------------------------------------
# Report header
# ------------------------------------------------------------------------------------------------------

_COVERAGE = """
SELECT c.chat_id, c.title, s.sampled_at, s.reported, s.known_open, s.suspect_open,
       round(100.0 * s.known_open / nullif(s.reported, 0), 1) AS coverage_pct,
       CASE WHEN %(from)s::timestamptz IS NOT NULL
            THEN tg.window_flags(c.chat_id, tstzrange(%(from)s::timestamptz, %(to)s::timestamptz, %(bounds)s)) END AS flags
FROM tg.chat c
LEFT JOIN LATERAL (SELECT x.* FROM tg.count_sample x
                    WHERE x.chat_id = c.chat_id AND x.sampled_at <= %(as_of)s::timestamptz
                    ORDER BY x.sampled_at DESC LIMIT 1) s ON true
WHERE c.chat_id = ANY (%(scope)s::bigint[])
ORDER BY c.chat_id
"""


@dataclass(frozen=True)
class Window:
    start: dt.datetime
    end: dt.datetime
    point: bool = False  # an instant: tstzrange [t, t]


def coverage_rows(conn: psycopg.Connection, scope: Sequence[int], as_of: dt.datetime,
                  window: Window | None) -> list[dict[str, Any]]:
    """Per chat: the latest count sample at or before as_of (Telegram's count, our known / suspect spells), the
    share we know, and tg.window_flags over the window (None = clean, or no window)."""
    if window is not None and window.start > window.end:
        raise CliError(f"the window starts ({window.start.isoformat()}) after it ends ({window.end.isoformat()}): "
                       "check --since and --as-of")
    cur = conn.execute(_COVERAGE, {"from": window.start if window else None, "to": window.end if window else None,
                                   "bounds": "[]" if window and window.point else "[)",
                                   "as_of": as_of, "scope": list(scope)})
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def when(value: dt.datetime | None) -> str:
    return "-" if value is None else value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def print_header(out: TextIO, conn: psycopg.Connection, *, title: str, as_of: dt.datetime, scope: Sequence[int],
                 window: Window | None = None, rollup_as_of: dt.datetime | None = None, rollup: bool = False,
                 notes: Sequence[str] = ()) -> None:
    """What every report says before its table: the title, the as-of time, the window, the chats in scope with
    their coverage (our known members / Telegram's count, suspect spells) and the gap flags of the window."""
    rows = coverage_rows(conn, scope, as_of, window)
    print(f"== {title}", file=out)
    print(f"as of {when(as_of)}", file=out)
    if rollup:
        print(f"audience rollup as of {when(rollup_as_of)}" if rollup_as_of
              else "audience rollup: not run yet (the service refreshes it hourly)", file=out)
    if window is not None:
        span = when(window.start) if window.point else f"{when(window.start)} .. {when(window.end)}"
        print(f"window {span}", file=out)
    else:
        print("window: none (current state)", file=out)
    for note in notes:
        print(note, file=out)
    print(f"chats in scope: {len(scope)}", file=out)
    for r in rows:  # every chat: a low or missing coverage must never drop out of the header
        if r["reported"] is None:
            cov = "no count sample yet"
        else:
            pct = f" ({r['coverage_pct']} %)" if r["coverage_pct"] is not None else ""
            cov = f"known {r['known_open']} of {r['reported']} reported{pct}, suspect {r['suspect_open']}"
        flags = f"; window flags: {r['flags']}" if r["flags"] else ""
        print(f"  {r['title']} ({r['chat_id']}): {cov}{flags}", file=out)
    print(file=out)


def rollup_as_of(conn: psycopg.Connection) -> dt.datetime | None:
    return conn.execute("SELECT max(as_of) FROM stats.rollup_run WHERE rollup = 'audience'").fetchone()[0]


# ------------------------------------------------------------------------------------------------------
# Tables and CSV
# ------------------------------------------------------------------------------------------------------


def cell(value: Any) -> str:
    """A value as a table cell."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, dt.datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, (list, tuple)):
        return ",".join(cell(v) for v in value)
    return str(value)


def csv_cell(value: Any) -> str:
    """A value as a CSV field: NULL is empty, booleans true / false, times ISO 8601 in UTC."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dt.datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return ",".join(csv_cell(v) for v in value)
    return str(value)


def _numeric(values: Sequence[Any]) -> bool:
    present = [v for v in values if v is not None]
    return bool(present) and all(isinstance(v, (int, float, Decimal)) and not isinstance(v, bool) for v in present)


def render_table(columns: Sequence[str], rows: Sequence[Sequence[Any]], titles: dict[int, str] | None = None) -> str:
    """A text table: numbers right-aligned, chat id columns shown as "title (id)" when the result has no title
    column of its own."""
    own_titles = any("title" in c or c == "channel" for c in columns)
    cells: list[list[str]] = []
    right: list[bool] = []
    for i, name in enumerate(columns):
        col = [r[i] for r in rows]
        decorate = titles is not None and not own_titles and name in CHAT_COLUMNS
        if decorate:
            cells.append([("" if v is None else f"{titles.get(int(v), '?')} ({v})") for v in col])
            right.append(False)
        else:
            cells.append([cell(v) for v in col])
            right.append(_numeric(col))
    widths = [max([len(columns[i])] + [len(c) for c in cells[i]]) for i in range(len(columns))]

    def line(parts: Sequence[str]) -> str:
        return "  ".join(p.rjust(w) if right[i] else p.ljust(w) for i, (p, w) in enumerate(zip(parts, widths))).rstrip()

    out = [line(list(columns)), line(["-" * w for w in widths])]
    out += [line([cells[i][n] for i in range(len(columns))]) for n in range(len(rows))]
    return "\n".join(out)


def emit(result: sql.Result, args: argparse.Namespace, titles: dict[int, str] | None = None,
         out: TextIO | None = None) -> None:
    """Print a result as CSV (--csv) or as a table with a row count."""
    out = out or sys.stdout
    if getattr(args, "csv", False):
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(result.columns)
        for row in result.rows:
            writer.writerow([csv_cell(v) for v in row])
        return
    if not result.rows:
        print("(no rows)", file=out)
        return
    print(render_table(result.columns, result.rows, titles), file=out)
    print(f"({len(result.rows)} rows)", file=out)


def header_stream(args: argparse.Namespace) -> TextIO:
    return sys.stderr if getattr(args, "csv", False) else sys.stdout


def titles_of(chats: Sequence[Chat]) -> dict[int, str]:
    return {c.id: c.title for c in chats}
