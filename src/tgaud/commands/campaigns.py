"""Owner campaigns and bot links: `campaign add|list|report ID` and `link create` (design.md 2 items 5-6, 8).

A campaign entered here is an annotation (origin `annotation`): a name, a window [starts, draw] and the chats it
covers; Telegram's own giveaways are observations written by the bot. `campaign report` is analytics item 4: per
chat, per person (`--people`), or the entrant list (`--entrants`). `link create` asks the Bot API for a named
invite link, prints it once and stores only its HMAC digest and name (D8); the link is never logged.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from typing import Any

import psycopg

from tgaud import cli, sql
from tgaud.botapi import ApiError, BotApi
from tgaud.commands import common, reports
from tgaud.commands.common import CliError, Window
from tgaud.config import Config

NAME_MAX = 32  # Telegram: invite link names are 1-32 characters
KINDS = ("giveaway", "ad", "other")  # tg_giveaway is an observation, written by the bot


def make_api(cfg: Config) -> BotApi:
    """The Bot API client of `link create`; tests replace this with the fake."""
    return BotApi(str(cfg.bot_token.reveal()), cfg.bot_api_url)


# ------------------------------------------------------------------------------------------------------
# campaign
# ------------------------------------------------------------------------------------------------------


def _add_campaign(p: argparse.ArgumentParser) -> None:
    sub = p.add_subparsers(dest="campaign_command", metavar="add|list|report", required=True)

    a = sub.add_parser("add", help="enter an owner campaign (giveaway, ad): name, window and chats")
    a.add_argument("name", metavar="NAME")
    a.add_argument("--chat", action="extend", nargs="+", metavar="CHAT", required=True,
                   help="the chats it covers (id, @username or title)")
    a.add_argument("--starts", required=True, metavar="TIME", help="start of the entry window")
    a.add_argument("--draw", required=True, metavar="TIME", help="end of the entry window (the draw)")
    a.add_argument("--kind", choices=KINDS, default="giveaway")
    a.add_argument("--only-new-members", action="store_true",
                   help="only first-time joiners are eligible (returners are reported apart)")
    a.add_argument("--winners", type=common.positive_int, metavar="N", help="number of winners")
    a.add_argument("--note", metavar="TEXT")

    ls = sub.add_parser("list", help="the campaigns entered or observed")
    common.add_as_of(ls)
    common.add_csv(ls)

    r = sub.add_parser("report", help="giveaway joiners of a campaign: per chat, per person or the entrant list")
    r.add_argument("campaign", metavar="ID", type=int)
    view = r.add_mutually_exclusive_group()
    view.add_argument("--people", action="store_true", help="counted once per person across the campaign's chats")
    view.add_argument("--entrants", action="store_true", help="one row per chat and person (for --csv)")
    r.add_argument("--hours", type=common.number, metavar="H", help="quick exit = an exit within H hours (default 48)")
    common.add_as_of(r)
    common.add_csv(r)


@common.command("campaign")
def _run_campaign(args: argparse.Namespace) -> int:
    return {"add": _campaign_add, "list": _campaign_list, "report": _campaign_report}[args.campaign_command](args)


def _campaign_add(args: argparse.Namespace) -> int:
    starts = common.parse_time(args.starts, "--starts")
    draw = common.parse_time(args.draw, "--draw")
    if starts >= draw:
        raise CliError("--starts must be before --draw")
    name = args.name.strip()
    if not name:
        raise CliError("the campaign needs a name")
    with common.session(read_only=False) as (conn, _cfg):
        chats = common.load_chats(conn)
        picked: list[common.Chat] = []
        for ref in args.chat:
            chat = common.resolve_tracked(ref, chats)
            if chat not in picked:
                picked.append(chat)
        campaign_id = conn.execute(
            "INSERT INTO tg.campaign (name, kind, origin, starts_at, draw_at, only_new_members, winner_count, note) "
            "VALUES (%s, %s, 'annotation', %s, %s, %s, %s, %s) RETURNING campaign_id",
            (name, args.kind, starts, draw, args.only_new_members, args.winners, args.note)).fetchone()[0]
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO tg.campaign_chat (campaign_id, chat_id) VALUES (%s, %s)",
                            [(campaign_id, c.id) for c in picked])
        conn.commit()
    print(f"campaign {campaign_id} {name!r}: {common.when(starts)} .. {common.when(draw)}, "
          f"{len(picked)} chat{'s' if len(picked) != 1 else ''}: " + ", ".join(f"{c.title} ({c.id})" for c in picked))
    return 0


_LIST = """
SELECT c.campaign_id, c.name, c.kind, c.origin, c.starts_at, c.draw_at, c.only_new_members, c.winner_count,
       coalesce(string_agg(coalesce(ch.title, 'chat') || ' (' || cc.chat_id || ')', ', ' ORDER BY cc.chat_id), '') AS chats
FROM tg.campaign c
LEFT JOIN tg.campaign_chat cc ON cc.campaign_id = c.campaign_id
LEFT JOIN tg.chat ch ON ch.chat_id = cc.chat_id
GROUP BY c.campaign_id ORDER BY c.campaign_id
"""


def _campaign_list(args: argparse.Namespace) -> int:
    with common.session() as (conn, _cfg):
        chats = common.load_chats(conn)
        as_of = common.parse_time(args.as_of, "--as-of") if args.as_of else conn.execute("SELECT now()").fetchone()[0]
        in_scope = [r[0] for r in conn.execute(
            "SELECT DISTINCT cc.chat_id FROM tg.campaign_chat cc JOIN tg.chat c ON c.chat_id = cc.chat_id "
            "ORDER BY cc.chat_id")]
        common.print_header(common.header_stream(args), conn, title="campaigns", as_of=as_of, scope=in_scope)
        cur = conn.execute(_LIST)
        common.emit(sql.Result([d.name for d in cur.description], cur.fetchall()), args, common.titles_of(chats))
    return 0


def _campaign_report(args: argparse.Namespace) -> int:
    with common.session() as (conn, _cfg):
        chats = common.load_chats(conn)
        row = conn.execute("SELECT name, starts_at, draw_at, only_new_members FROM tg.campaign WHERE campaign_id = %s",
                           (args.campaign,)).fetchone()
        if row is None:
            raise CliError(f"no campaign {args.campaign}")
        name, starts, draw, only_new = row
        known = {c.id for c in chats}
        scope = [r[0] for r in conn.execute("SELECT chat_id FROM tg.campaign_chat WHERE campaign_id = %s ORDER BY chat_id",
                                            (args.campaign,)) if r[0] in known]
        hours = args.hours if args.hours is not None else 48
        params: dict[str, Any] = {"campaign": args.campaign}
        if args.hours is not None:
            params["hours"] = args.hours
        block = "4c" if args.entrants else "4b" if args.people else "4a"

        def window(as_of: dt.datetime, _since: dt.datetime) -> Window | None:
            end = min(draw + dt.timedelta(hours=float(hours)), as_of)
            return Window(starts, end) if starts < end else None

        notes = [f"campaign {args.campaign} {name!r}: {common.when(starts)} .. {common.when(draw)}"
                 + (", only new members are eligible" if only_new else "")]
        reports.run_report(conn, args, block, chats, scope=scope, params=params, window=window,
                           notes=tuple(notes), title=f"campaign report, {sql.title(block)}")
    return 0


# ------------------------------------------------------------------------------------------------------
# link create
# ------------------------------------------------------------------------------------------------------


def _add_link(p: argparse.ArgumentParser) -> None:
    sub = p.add_subparsers(dest="link_command", metavar="create", required=True)
    c = sub.add_parser("create", help="make a named invite link with the bot; the link is printed once")
    c.add_argument("--chat", required=True, metavar="CHAT", help="id, @username or title")
    c.add_argument("--name", required=True, help=f"the link's name (1-{NAME_MAX} characters): the campaign label")
    c.add_argument("--campaign", type=int, metavar="ID", help="attribute the joins through this link to the campaign")


@common.command("link")
def _run_link(args: argparse.Namespace) -> int:
    name = args.name.strip()
    if not 1 <= len(name) <= NAME_MAX:
        raise CliError(f"--name must be 1-{NAME_MAX} characters (Telegram's limit), not {len(name)}")
    with common.session(need=("database", "bot", "link_key"), read_only=False) as (conn, cfg):
        chats = common.load_chats(conn)
        chat = common.resolve_tracked(args.chat, chats)
        if args.campaign is not None:
            row = conn.execute("SELECT 1 FROM tg.campaign WHERE campaign_id = %s", (args.campaign,)).fetchone()
            if row is None:
                raise CliError(f"no campaign {args.campaign}")
            if conn.execute("SELECT 1 FROM tg.campaign_chat WHERE campaign_id = %s AND chat_id = %s",
                            (args.campaign, chat.id)).fetchone() is None:
                print(f"tgaud link: note: {chat.title} ({chat.id}) is not one of campaign {args.campaign}'s chats",
                      file=sys.stderr)
        state = conn.execute("SELECT bot_id, link_key_fingerprint FROM tg.bot_state").fetchone()
        conn.rollback()
        if state is not None:
            bot_id, fingerprint = state
            token_bot = str(cfg.bot_token.reveal()).split(":", 1)[0]
            if token_bot != str(bot_id):
                raise CliError(f"TGAUD_BOT_TOKEN belongs to bot {token_bot}, but the service records bot {bot_id}")
            if fingerprint is not None and fingerprint != cfg.link_key_fingerprint():
                raise CliError("TGAUD_LINK_HMAC_KEY is not the key the service recorded: joins through this link "
                               "could not be attributed")
        try:
            with make_api(cfg) as api:
                made = api.create_chat_invite_link(chat.id, name=name)
        except ApiError as e:
            raise CliError(f"createChatInviteLink failed: {e}") from None
        link = made.get("invite_link") if isinstance(made, dict) else None
        if not isinstance(link, str) or not link:
            raise CliError("createChatInviteLink returned no invite_link")
        try:
            creator = (made.get("creator") or {}).get("id")
            link_id = conn.execute(
                "INSERT INTO tg.invite_link (chat_id, link_digest, creator_id, name, is_primary, creates_join_request, "
                "campaign_id) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING invite_link_id",
                (chat.id, cfg.link_digest(link), creator if creator is not None else (state[0] if state else None),
                 made.get("name") or name, bool(made.get("is_primary", False)),
                 bool(made.get("creates_join_request", False)), args.campaign)).fetchone()[0]
            conn.commit()
        except psycopg.Error as e:
            # The link exists at Telegram and cannot be shown again: print it, then fail.
            print(link)
            raise CliError("the link was created at Telegram (printed above) but could not be stored, so joins "
                           f"through it will not be attributed: {str(e).strip().splitlines()[0]}") from None
    print(link)
    print(f"stored: invite link {link_id} '{name}' for {chat.title} ({chat.id})"
          + (f", campaign {args.campaign}" if args.campaign is not None else "")
          + "; only its HMAC digest and name are kept, so this is the only time the link is shown",
          file=sys.stderr)
    return 0


cli.register("campaign", "owner campaigns: add, list, report ID (items 4 and 8)", _add_campaign, _run_campaign)
cli.register("link", "bot-made invite links: create --chat --name [--campaign]", _add_link, _run_link)
