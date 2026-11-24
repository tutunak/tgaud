"""The two commands that write evidence: `probe CHAT USER` and `forget-user ID` (design.md 6 and 11).

`probe` queues a manual probe (reason `manual`, the top priority of the prober's queue) and returns: the prober
of the running service asks Telegram and the answer becomes evidence. `forget-user` erases one person
everywhere (tg.forget_user) while the service is stopped, then refreshes the audience rollup (R2) before it
reports success.
"""

from __future__ import annotations

import argparse
import sys

import psycopg

from tgaud import cli, db, history
from tgaud.commands import common
from tgaud.commands.common import CliError

# ------------------------------------------------------------------------------------------------------
# probe
# ------------------------------------------------------------------------------------------------------


def _add_probe(p: argparse.ArgumentParser) -> None:
    p.add_argument("chat", metavar="CHAT", help="chat id, @username or title")
    p.add_argument("user", metavar="USER", help="a user id, or an @username (when names are stored)")


@common.command("probe")
def _run_probe(args: argparse.Namespace) -> int:
    with common.session(read_only=False) as (conn, _cfg):
        chat = common.resolve_tracked(args.chat, common.load_chats(conn))
        user_id = common.resolve_user(conn, args.user)
        history.lock_chats(conn, [chat.id])
        # a person the service has not seen yet can still be asked about (the probe row references tg_user)
        conn.execute("INSERT INTO tg.tg_user (user_id, first_seen_at, last_seen_at) VALUES (%s, now(), now()) "
                     "ON CONFLICT (user_id) DO NOTHING", (user_id,))
        pending = conn.execute("SELECT probe_id, reason FROM tg.probe WHERE chat_id = %s AND user_id = %s "
                               "AND outcome = 'pending' FOR UPDATE", (chat.id, user_id)).fetchone()
        if pending is not None and pending[1] == "manual":
            conn.commit()
            print(f"probe {pending[0]} for user {user_id} in {chat.title} ({chat.id}) is already queued as manual")
            return 0
        if pending is not None:
            # one pending probe per pair: the manual one takes its place; the old row stays as a cancelled record
            conn.execute("UPDATE tg.probe SET outcome = 'cancelled' WHERE probe_id = %s", (pending[0],))
        probe_id = conn.execute("INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (%s, %s, 'manual') "
                                "RETURNING probe_id", (chat.id, user_id)).fetchone()[0]
        conn.commit()
    replaced = f" (replacing {pending[1]} probe {pending[0]})" if pending is not None else ""
    print(f"queued manual probe {probe_id} for user {user_id} in {chat.title} ({chat.id}){replaced}; "
          "the prober of the running service answers it")
    return 0


# ------------------------------------------------------------------------------------------------------
# forget-user
# ------------------------------------------------------------------------------------------------------

# every chat the person appears in, as subject or in a secondary reference (these chats are locked)
_CHATS_OF_USER = """
SELECT chat_id FROM tg.membership WHERE user_id = %(u)s
UNION SELECT chat_id FROM tg.member_event WHERE user_id = %(u)s OR actor_id = %(u)s
UNION SELECT chat_id FROM tg.sighting WHERE user_id = %(u)s
UNION SELECT chat_id FROM tg.probe WHERE user_id = %(u)s
UNION SELECT chat_id FROM tg.join_request WHERE user_id = %(u)s OR resolved_by = %(u)s
UNION SELECT chat_id FROM tg.activity_daily WHERE user_id = %(u)s
UNION SELECT chat_id FROM tg.invite_link WHERE creator_id = %(u)s
UNION SELECT s.chat_id FROM tg.snapshot s
       WHERE EXISTS (SELECT 1 FROM tg.snapshot_member m WHERE m.snapshot_id = s.snapshot_id
                      AND (m.user_id = %(u)s OR m.inviter_id = %(u)s))
UNION SELECT chat_id FROM tg.raw_update
       WHERE user_id = %(u)s OR jsonb_path_exists(payload, '$.** ? (@.id == $uid)', jsonb_build_object('uid', %(u)s::bigint))
ORDER BY 1
"""

_COUNTS = """
SELECT (SELECT count(*) FROM tg.membership WHERE user_id = %(u)s),
       (SELECT count(*) FROM tg.member_event WHERE user_id = %(u)s),
       (SELECT count(*) FROM tg.member_event WHERE actor_id = %(u)s),
       (SELECT count(*) FROM tg.sighting WHERE user_id = %(u)s) + (SELECT count(*) FROM tg.probe WHERE user_id = %(u)s)
         + (SELECT count(*) FROM tg.join_request WHERE user_id = %(u)s)
         + (SELECT count(*) FROM tg.activity_daily WHERE user_id = %(u)s)
         + (SELECT count(*) FROM tg.snapshot_member WHERE user_id = %(u)s),
       (SELECT count(*) FROM tg.tg_user WHERE user_id = %(u)s)
"""


def _add_forget(p: argparse.ArgumentParser) -> None:
    p.add_argument("user", metavar="ID", help="the user id to erase everywhere (an @username is looked up first)")
    p.add_argument("--yes", action="store_true", help="do not ask for confirmation")


def _confirm(prompt: str) -> bool:
    print(prompt, end="", file=sys.stderr, flush=True)
    answer = sys.stdin.readline().strip().lower()
    return answer in ("y", "yes")


@common.command("forget-user")
def _run_forget(args: argparse.Namespace) -> int:
    with common.session(read_only=False) as (conn, _cfg):
        user_id = common.resolve_user(conn, args.user)
        with db.maintenance(conn):  # fails fast while `tgaud serve` runs
            spells, events, as_actor, other, known = conn.execute(_COUNTS, {"u": user_id}).fetchone()
            chat_ids = [r[0] for r in conn.execute(_CHATS_OF_USER, {"u": user_id})]
            conn.rollback()
            print(f"user {user_id}: {spells} stays, {events} events, {as_actor} events as the acting admin, "
                  f"{other} other rows, in {len(chat_ids)} chats" + ("" if known else " (no profile row)"),
                  file=sys.stderr)
            if not args.yes and not _confirm(f"Erase user {user_id} everywhere? This cannot be undone. [y/N] "):
                print("not confirmed: nothing changed", file=sys.stderr)
                return 1
            try:
                history.lock_chats(conn, chat_ids)
                conn.execute("SELECT tg.forget_user(%s)", (user_id,))
                conn.commit()
            except psycopg.Error:
                conn.rollback()
                raise
            # The materialized user_chats and the sets built from it still name the person: refresh R2 now.
            from tgaud.jobs import audience

            try:
                refreshed = audience.refresh(conn)
            except Exception as e:  # noqa: BLE001
                raise CliError(f"user {user_id} is erased from the history, but the audience rollup refresh failed "
                               f"({str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__}); "
                               "tg.user_chats may still list the id: run this command again (it is safe to repeat)") from None
    print(f"erased user {user_id}: {spells} stays, {events} events, {other} other rows; audience rollup "
          f"refreshed as of {common.when(refreshed.as_of)}")
    return 0


cli.register("probe", "queue a manual getChatMember check of USER in CHAT at the top priority", _add_probe, _run_probe)
cli.register("forget-user", "erase one person everywhere (the service must be stopped)", _add_forget, _run_forget)
