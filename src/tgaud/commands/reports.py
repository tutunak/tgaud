"""The report commands that run one block of queries.sql (design.md 8 and 10): overlap, unique, chats-per-user,
user, growth, short-stays, serial, bursts, sources, cohorts, tenure, rejoiners, flow, discussion, requests,
raids, activity, post-leaves, asof, drift, coverage.

Options carry the names of the block's psql variables (queries.sql header) and its defaults: an option that is
not given leaves the header default in force. `--among CHAT...` is the scope, `--as-of` the report time,
`--csv` writes the table as CSV.
"""

from __future__ import annotations

import argparse
import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg

from tgaud import cli, sql
from tgaud.commands import common
from tgaud.commands.common import CliError, Window

UTC = common.UTC

# option -> (flag, argparse kwargs); the dest is the psql variable name
OPTIONS: dict[str, tuple[str, dict[str, Any]]] = {
    "since": ("--since", dict(metavar="TIME", help="start of the history window (default: as of minus 90 days)")),
    "hours": ("--hours", dict(type=common.number, metavar="H",
                              help="a short stay / quick exit is an exit within H hours (default 48)")),
    "flow_days": ("--flow-days", dict(type=common.positive_int, metavar="N",
                                      help="joined A, then B within N days (default 7)")),
    "burst_minutes": ("--burst-minutes", dict(type=common.positive_int, metavar="N",
                                              help="joins to several chats within N minutes (default 10)")),
    "burst_users": ("--burst-users", dict(type=common.positive_int, metavar="N",
                                          help="report a burst from N users (default 20)")),
    "raid_min_joins": ("--raid-min-joins", dict(type=common.positive_int, metavar="N",
                                                help="a minute is a raid from N joins (and 10x the baseline; default 20)")),
    "raid_hours": ("--raid-hours", dict(type=common.positive_int, metavar="H",
                                        help="how far back to look for raids (default 24)")),
    "post_hours": ("--post-hours", dict(type=common.positive_int, metavar="H",
                                        help="leaves within H hours after a post (default 6)")),
    "active_days": ("--active-days", dict(type=common.positive_int, metavar="N",
                                          help="active = wrote in the last N days (default 30)")),
}


@dataclass(frozen=True)
class Spec:
    name: str
    help: str
    block: str
    params: tuple[str, ...] = ()
    rollup: bool = False  # reads the audience rollup (R2): no window, its own as-of time
    window: str = "since"  # since | raid | day | none


PLAIN: tuple[Spec, ...] = (
    Spec("overlap", "overlap of every pair of chats: shared members, share of each, Jaccard (item 1)", "1",
         rollup=True, window="none"),
    Spec("unique", "unique (exclusive) members per chat in the scope, and the distinct total (item 3)", "3",
         rollup=True, window="none"),
    Spec("chats-per-user", "how many people are in 1, 2, 3 ... of the chats in scope (item 2)", "2a",
         rollup=True, window="none"),
    Spec("growth", "daily joins, exits, net, corrections and the residual against Telegram's count (item 9)",
         "9a", ("since",)),
    Spec("short-stays", "short stays per chat and week outside campaign windows (item 5)", "5", ("since", "hours")),
    Spec("serial", "people with 2+ short stays: prize hunters (item 6)", "6a", ("since", "hours")),
    Spec("bursts", "many people joining the same two chats within minutes (item 6)", "6b",
         ("since", "burst_minutes", "burst_users")),
    Spec("sources", "joins and 7/30-day retention per link, campaign, request, folder, admin add (item 8)", "8",
         ("since",)),
    Spec("cohorts", "retention of each join week at 7, 30 and 90 days (item 7)", "7b", ("since",)),
    Spec("tenure", "how long people stayed before they left: median, p90, share within H (item 7)", "7a",
         ("since", "hours")),
    Spec("rejoiners", "people who came back to a chat they had left (item 10)", "10a", ("since",)),
    Spec("flow", "does chat A feed chat B: joined A, then B within N days (item 10)", "10b", ("since", "flow_days")),
    Spec("discussion", "a channel against its discussion group: overlap and who writes (item 11)", "11",
         ("active_days",), rollup=True, window="none"),
    Spec("requests", "join-request funnel per chat and week (item 12)", "12", ("since",)),
    Spec("raids", "minutes with joins far above the baseline, and the share kicked within an hour (item 13)", "13",
         ("raid_min_joins", "raid_hours"), window="raid"),
    Spec("activity", "active vs silent members of groups; did short-stayers write (item 14)", "14",
         ("since", "hours", "active_days")),
    Spec("post-leaves", "leaves within N hours after each channel post, against the span before (item 15)", "15",
         ("since", "post_hours")),
    Spec("drift", "count samples of the last 24 hours: mismatch and residual (item 9)", "9b", window="day"),
)


# ------------------------------------------------------------------------------------------------------
# The common runner
# ------------------------------------------------------------------------------------------------------


def _ts(conn: psycopg.Connection, text: str) -> dt.datetime:
    return conn.execute("SELECT %s::timestamptz", (text,)).fetchone()[0]


def _scope_ids(text: str) -> list[int]:
    body = text.strip("{}")
    return [int(x) for x in body.split(",")] if body else []


def run_report(conn: psycopg.Connection, args: argparse.Namespace, block: str, chats: list[common.Chat], *,
               scope: list[int] | None, params: dict[str, Any], window: Callable[[dt.datetime, dt.datetime], Window | None],
               rollup: bool = False, notes: tuple[str, ...] = (), title: str | None = None,
               header_scope: Callable[[psycopg.Connection, list[int]], list[int]] | None = None) -> sql.Result:
    """Header, then the block: both in the caller's transaction, with one as-of time and one scope."""
    params = dict(params)
    if getattr(args, "as_of", None):
        params["as_of"] = common.parse_time(args.as_of, "--as-of").isoformat()
    if scope is not None:
        params["scope"] = scope
    vals = sql.values(conn, **params)
    as_of = _ts(conn, vals["as_of_ts"])
    since = _ts(conn, vals["since_ts"])
    params["as_of"] = vals["as_of_ts"]  # frozen: header and table agree
    in_scope = scope if scope is not None else _scope_ids(vals["scope_ids"])
    if header_scope is not None:  # the chats the table reads beyond the scope (a channel's discussion group)
        in_scope = header_scope(conn, in_scope)
    ra = common.rollup_as_of(conn) if rollup else None
    common.print_header(common.header_stream(args), conn, title=title or sql.title(block), as_of=as_of, scope=in_scope,
                        window=window(as_of, since), rollup_as_of=ra, rollup=rollup, notes=notes)
    result = sql.run(conn, block, **params)
    common.emit(result, args, common.titles_of(chats))
    return result


def _supplied(args: argparse.Namespace, names: tuple[str, ...]) -> dict[str, Any]:
    return {n: getattr(args, n) for n in names if getattr(args, n, None) is not None}


def _since_window(as_of: dt.datetime, since: dt.datetime) -> Window:
    return Window(since, as_of)


def _activity_window(conn: psycopg.Connection, as_of: dt.datetime, days: int) -> Window:
    """The period the activity counts of block 11 cover, from the same UTC day boundaries as its SQL: the last
    `days` UTC days up to last_day, which is the day of as_of when the data is current (as_of is not before the
    last update ingest committed; the window then ends at as_of) and the day before when it is not (the day of
    as_of is partly over; the window ends at that day's end)."""
    horizon = conn.execute("SELECT max(last_poll_ok_at) FROM tg.bot_state").fetchone()[0]
    today = as_of.astimezone(UTC).date()
    current = horizon is None or as_of >= horizon
    last_day = today if current else today - dt.timedelta(days=1)
    start = dt.datetime.combine(last_day - dt.timedelta(days=days - 1), dt.time(), UTC)
    end = as_of if current else dt.datetime.combine(last_day + dt.timedelta(days=1), dt.time(), UTC)
    return Window(start, end)


def _with_discussion_groups(conn: psycopg.Connection, scope: list[int]) -> list[int]:
    """The scope and the tracked discussion groups linked to its channels (item 11 reads both)."""
    groups = conn.execute("SELECT g.chat_id FROM tg.chat ch JOIN tg.chat g ON g.chat_id = ch.linked_chat_id AND g.tracked "
                          "WHERE ch.chat_type = 'channel' AND ch.tracked AND ch.chat_id = ANY (%s::bigint[])",
                          (scope,)).fetchall()
    return sorted(set(scope) | {r[0] for r in groups})


def _make_plain(spec: Spec) -> tuple[Callable[[argparse.ArgumentParser], None], Callable[[argparse.Namespace], int]]:
    def add(p: argparse.ArgumentParser) -> None:
        common.add_among(p)
        common.add_as_of(p)
        for name in spec.params:
            flag, kwargs = OPTIONS[name]
            p.add_argument(flag, dest=name, default=None, **kwargs)
        common.add_csv(p)

    @common.command(spec.name)
    def run(args: argparse.Namespace) -> int:
        with common.session() as (conn, _cfg):
            chats = common.load_chats(conn)
            scope = common.resolve_scope(args.among, chats)
            params = _supplied(args, spec.params)
            if "since" in params:
                params["since"] = common.parse_time(params["since"], "--since").isoformat()
            header_scope = None
            if spec.block == "11":  # the table also reads each channel's discussion group and its activity
                header_scope = _with_discussion_groups
                days = params.get("active_days", 30)
                window = lambda as_of, _since: _activity_window(conn, as_of, days)  # noqa: E731
            elif spec.window == "raid":
                hours = params.get("raid_hours", 24)
                window = lambda as_of, _since: Window(as_of - dt.timedelta(hours=hours), as_of)  # noqa: E731
            elif spec.window == "day":
                window = lambda as_of, _since: Window(as_of - dt.timedelta(hours=24), as_of)  # noqa: E731
            elif spec.window == "none":
                window = lambda as_of, _since: None  # noqa: E731
            else:
                window = _since_window
            run_report(conn, args, spec.block, chats, scope=scope, params=params, window=window, rollup=spec.rollup,
                       header_scope=header_scope)
        return 0

    return add, run


# ------------------------------------------------------------------------------------------------------
# user, asof, coverage
# ------------------------------------------------------------------------------------------------------


def _add_user(p: argparse.ArgumentParser) -> None:
    p.add_argument("user", metavar="ID|@NAME", help="a user id, or an @username (when names are stored)")
    common.add_as_of(p)
    common.add_csv(p)


@common.command("user")
def _run_user(args: argparse.Namespace) -> int:
    with common.session() as (conn, _cfg):
        chats = common.load_chats(conn)
        user_id = common.resolve_user(conn, args.user)
        run_report(conn, args, "2b", chats, scope=None, params={"user": user_id},
                   window=lambda as_of, since: None, notes=(f"user {user_id}",),
                   title="one user: every chat and every stay")
    return 0


def _add_asof(p: argparse.ArgumentParser) -> None:
    p.add_argument("chat", metavar="CHAT", help="chat id, @username or title")
    p.add_argument("date", metavar="DATE", help="the moment: 2026-11-07 (00:00 UTC) or 2026-11-07T12:00:00+00:00")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--list", action="store_true", help="the members at that moment, one row each (item 16b)")
    mode.add_argument("--eod", action="store_true",
                      help="members at the end of that UTC day, from the chat_day rollup (item 16d)")
    mode.add_argument("--matrix", action="store_true",
                      help="the overlap of every pair of --among chats (default: all tracked) at that moment, a "
                           "batch of about a minute (item 16c); CHAT is then only checked, not used")
    common.add_among(p)
    common.add_as_of(p)
    common.add_csv(p)


@common.command("asof")
def _run_asof(args: argparse.Namespace) -> int:
    with common.session() as (conn, _cfg):
        chats = common.load_chats(conn)
        chat = common.resolve_tracked(args.chat, chats)
        day = common.parse_time(args.date, "DATE")
        params: dict[str, Any] = {"day": day.isoformat()}
        scope: list[int] | None = [chat.id]
        window: Callable[[dt.datetime, dt.datetime], Window | None] = lambda as_of, since: Window(day, day, point=True)  # noqa: E731
        if args.matrix:
            block, scope = "16c", common.resolve_scope(args.among, chats)
        elif args.eod:
            block = "16d"
            start = day.replace(hour=0, minute=0, second=0, microsecond=0)
            window = lambda as_of, since: Window(start, start + dt.timedelta(days=1))  # noqa: E731
        else:
            block = "16b" if args.list else "16a"
            params["chat"] = chat.id
        run_report(conn, args, block, chats, scope=scope, params=params, window=window)
    return 0


_COVERAGE_TABLE = """
SELECT c.chat_id, c.title, c.chat_type, c.bot_status,
       (SELECT lower(v.period) FROM tg.coverage v WHERE v.chat_id = c.chat_id AND v.period @> %(as_of)s::timestamptz) AS admin_since,
       s.sampled_at, s.reported, s.known_open, s.suspect_open, s.reported - s.known_open AS mismatch,
       round(100.0 * s.known_open / nullif(s.reported, 0), 1) AS coverage_pct,
       (SELECT max(n.finished_at) FROM tg.snapshot n
         WHERE n.chat_id = c.chat_id AND n.quality = 'good' AND n.finished_at <= %(as_of)s::timestamptz) AS last_good_snapshot,
       (SELECT count(*) FROM tg.gap g WHERE (g.chat_id = c.chat_id OR g.chat_id IS NULL)
           AND g.period @> %(as_of)s::timestamptz) AS open_gaps,
       (SELECT count(*) FROM tg.probe p WHERE p.chat_id = c.chat_id AND p.outcome = 'pending'
           AND p.queued_at <= %(as_of)s::timestamptz) AS probes_pending,
       tg.window_flags(c.chat_id, tstzrange(%(since)s::timestamptz, %(as_of)s::timestamptz)) AS flags
FROM tg.chat c
LEFT JOIN LATERAL (SELECT x.* FROM tg.count_sample x
                    WHERE x.chat_id = c.chat_id AND x.sampled_at <= %(as_of)s::timestamptz
                    ORDER BY x.sampled_at DESC LIMIT 1) s ON true
WHERE c.chat_id = ANY (%(scope)s::bigint[])
ORDER BY c.chat_id
"""

_GAPS = """
SELECT g.gap_id, g.chat_id, c.title, lower(g.period) AS gap_from, upper(g.period) AS gap_to, g.reason, g.confirmed
FROM tg.gap g LEFT JOIN tg.chat c ON c.chat_id = g.chat_id
WHERE (g.chat_id = ANY (%(scope)s::bigint[]) OR g.chat_id IS NULL)
  AND g.period && tstzrange(%(since)s::timestamptz, %(as_of)s::timestamptz)
ORDER BY lower(g.period), g.gap_id
"""


def _add_coverage(p: argparse.ArgumentParser) -> None:
    common.add_among(p)
    common.add_as_of(p)
    p.add_argument("--since", metavar="TIME", help="start of the window for the gap flags (default: as of minus 90 days)")
    p.add_argument("--gaps", action="store_true", help="list the gaps (lost or doubtful periods) of the window instead")
    common.add_csv(p)


@common.command("coverage")
def _run_coverage(args: argparse.Namespace) -> int:
    with common.session() as (conn, _cfg):
        chats = common.load_chats(conn)
        scope = common.resolve_scope(args.among, chats)
        params: dict[str, Any] = {}
        if args.as_of:
            params["as_of"] = common.parse_time(args.as_of, "--as-of").isoformat()
        if args.since:
            params["since"] = common.parse_time(args.since, "--since").isoformat()
        if scope is not None:
            params["scope"] = scope
        vals = sql.values(conn, **params)
        as_of, since = _ts(conn, vals["as_of_ts"]), _ts(conn, vals["since_ts"])
        in_scope = scope if scope is not None else _scope_ids(vals["scope_ids"])
        if since > as_of:
            raise CliError("--since is after --as-of")
        common.print_header(common.header_stream(args), conn, title="coverage: when the bot was admin, drift, gaps",
                            as_of=as_of, scope=in_scope, window=Window(since, as_of))
        cur = conn.execute(_GAPS if args.gaps else _COVERAGE_TABLE,
                           {"as_of": as_of, "since": since, "scope": in_scope})
        result = sql.Result([d.name for d in cur.description], cur.fetchall())
        common.emit(result, args, common.titles_of(chats))
    return 0


# ------------------------------------------------------------------------------------------------------


for _spec in PLAIN:
    _add, _run = _make_plain(_spec)
    cli.register(_spec.name, _spec.help, _add, _run)
cli.register("user", "one user's chats and stays by id or @username (item 2)", _add_user, _run_user)
cli.register("asof", "who was in CHAT at DATE: counts, the list, the day-end count or the overlap matrix (item 16)",
             _add_asof, _run_asof)
cli.register("coverage", "per chat: when the bot was admin, Telegram's count against ours, suspect spells, gaps",
             _add_coverage, _run_coverage)
