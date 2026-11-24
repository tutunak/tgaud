"""Ingest (its own thread, on the connection that holds the service lock): design.md 7, D1, D14.

serve calls, in this order, with the service lock held:

    identify(ctx, conn) the identity guards of the deploy contract, before the generation is claimed: getMe's
                        bot id and ctx.config.link_key_fingerprint() against tg.bot_state; on a mismatch raise
                        tgaud.jobs.StartupError (exit 2) having written nothing. On the first start (no row) it
                        records both. It leaves exactly one tg.bot_state row and commits or rolls back.
                        No other writes here: a refused start must not change the database, not even the
                        serve generation, which serve claims only after this returns.
    startup(ctx, conn)  once, after the claim: partitions; allowed_updates (an updates_not_allowed gap when
                        the stored list lacked chat_member, closed by the first poll); the rights check of
                        every tracked chat except the old group of a migration, and of the destination of
                        every migration still in M2 (getChatMember(chat, bot) -> bot_status, coverage,
                        not_admin / rights_lost gaps, the chat gauges, M1 when the bot is admin there); queues a
                        getChat metadata fetch for each. Every transaction starts with ctx.fence(conn).
    run(ctx, conn)      the long-polling loop until ctx.stop is set.

The loop. Before every getUpdates: when tg.bot_state.last_update_at is older than 6 days (or unset), the offset
is omitted (ids restart after a week without updates, F§1) until an update commits. Then
getUpdates(offset, timeout=POLL_TIMEOUT, limit=100, allowed_updates), and per update one transaction:

    fence -> tg.update_receipt ON CONFLICT DO NOTHING (seen: skip the body) -> lock_chats -> the body by type
    -> tg.bot_state (next_offset = update_id + 1, last_update_*, last_poll_ok_at; caught_up_at = the poll's
    send time in the transaction of the response's last update when the response had fewer than 100) -> commit

An empty response commits last_poll_ok_at and caught_up_at in a transaction of its own. A service_down gap
is recorded in the transaction that advances last_poll_ok_at when the previous durable value is more than
24 h old (a restart after a long stop, or a long run of failed polls), and a suspected one when an update's lag
passed 20 h; the open stays of the affected chats are left to the reconciler (G1). Metrics and health marks
happen only after a commit.

Failures: input that does not validate (parse_update checks every field the bodies write, bigint ranges
included) goes through the malformed path in one transaction (a receipt with update_type 'malformed' dated at
the receive time, the redacted payload in tg.raw_update when it is a membership update with a known chat id,
the offset), counted as tgaud_ingest_failures_total{stage="parse"}; the loop never wedges on one bad update. A
database error rolls back and retries the same update with backoff (stage "db") and never advances the
offset; a lost connection ends ingest (serve exits 1). Any other exception (a program error in a body or a
rebuild) rolls back and ends ingest, so the restarted service retries the valid update rather than
acknowledging it without its data. An entry whose update_id can't be read ends its response there: the
updates before it commit, neither the offset nor caught_up_at passes it, it is counted (stage "parse") and
the same offset is polled again after a backoff. Strings are made storable before parsing (NUL and unpaired
surrogates become U+FFFD); a NaN or Infinity fails validation and is stored as null in the raw payload.

Bot rights: my_chat_member events and rights checks set the bot's status as of their time. A queued event
older than the last check (processed after the startup check) is replayed at its own time for coverage and
gaps, and the check's observation is applied again from the check's time, so the interval in between keeps its
history. A failed check stays due (tg.chat.rights_due_at, with backoff) and marks the chat with a suspected
not_admin gap (confirmed false) until a check or a newer event resolves it.

Privacy (design.md 11): never bio, user_chat_id or message text; a full invite link made by this bot is stored
only as HMAC-SHA256 with TGAUD_LINK_HMAC_KEY (tg.invite_link.link_digest, and a digest marker in raw payloads);
names are dropped when TGAUD_STORE_NAMES is off. Updates about an erased person (tg.is_erased) dated before
the erasure are committed as a receipt only; an erased actor, inviter or resolver is stored as NULL (with
member_event.actor_is_other kept) and replaced in raw payloads.

Chats: only channels, groups and supergroups carry audience data; any other chat (a private chat, e.g. a user
blocking the bot) commits the receipt (without the chat id) and the offset only. Every chat id is routed through
tg.chat_alias rows whose moved_at is set (M1 done); an alias still waiting for M1 (moved_at NULL, the new chat
not tracked) routes nothing. Migrations (migrate_to_chat_id in the old group, migrate_from_chat_id in the new
supergroup, in either order and repeated) record the alias once; M1 (tg.move_chat_history, then a rebuild of the
moved pairs) runs when the new chat is tracked, now or when it becomes tracked later; until then M2 holds.

getChat metadata and due rights checks are fetched between polls (tg.chat.metadata_due_at, rights_due_at: first
seen, became tracked, every tracked chat at startup), retried with backoff until they succeed; the update that
queued them never waits for them.
"""

from __future__ import annotations

import copy
import datetime as dt
import logging
import math
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from tgaud import db, history, metrics
from tgaud.botapi import ALLOWED_UPDATES, POLL_TIMEOUT, ApiError
from tgaud.jobs import JobContext, StartupError

log = logging.getLogger(__name__)

LIMIT = 100
STALE_OFFSET = dt.timedelta(days=6)
SERVICE_DOWN = dt.timedelta(hours=24)
LAG_SUSPECT = dt.timedelta(hours=20)
SHORT_STAY = dt.timedelta(hours=48)
MIN_EMPTY_POLL = 1.0  # seconds: an empty answer faster than this (a broken proxy, a fake) is not hammered
MAX_BACKOFF = 60.0
METADATA_BATCH = 5
METADATA_BACKOFF = (30, 120, 600, 1800, 3600)  # seconds after the 1st, 2nd, ... failed getChat

MEMBERSHIP_TYPES = ("chat_member", "my_chat_member", "chat_join_request")
AUDIENCE_CHATS = ("channel", "group", "supergroup")
STATUSES = ("creator", "administrator", "member", "restricted", "left", "kicked")
ADMIN = ("creator", "administrator")
EXIT_KINDS = ("leave", "kick", "restrict_out")
EVENT_LABELS = ("join", "leave", "kick", "restrict_out", "ban_out", "unban")  # anything else: "role"
POST_KINDS = ("text", "photo", "video", "animation", "audio", "document", "poll", "giveaway")
# Service messages carry one of these keys; they are not someone writing (activity) nor a channel post.
SERVICE_KEYS = frozenset((
    "new_chat_members", "left_chat_member", "new_chat_title", "new_chat_photo", "delete_chat_photo",
    "group_chat_created", "supergroup_chat_created", "channel_chat_created", "message_auto_delete_timer_changed",
    "migrate_to_chat_id", "migrate_from_chat_id", "pinned_message", "successful_payment", "refunded_payment",
    "users_shared", "chat_shared", "connected_website", "write_access_allowed", "passport_data",
    "proximity_alert_triggered", "boost_added", "chat_background_set", "forum_topic_created",
    "forum_topic_edited", "forum_topic_closed", "forum_topic_reopened", "general_forum_topic_hidden",
    "general_forum_topic_unhidden", "giveaway_created", "giveaway_winners", "giveaway_completed",
    "video_chat_scheduled", "video_chat_started", "video_chat_ended", "video_chat_participants_invited",
    "web_app_data", "gift", "unique_gift", "paid_message_price_changed", "direct_message_price_changed",
    "checklist_tasks_done", "checklist_tasks_added", "suggested_post_approved", "suggested_post_declined",
    "suggested_post_paid", "suggested_post_refunded", "suggested_post_approval_failed",
))
DIGEST_MARK = "hmac-sha256:"
USER_KEYS = ("from", "user", "creator")  # keys whose object is a person in membership payloads
MEMBER_KEYS = ("new_chat_member", "old_chat_member")  # a ChatMember: holds the person at `user`
NAME_KEYS = ("first_name", "last_name", "username")
# ChatInviteLink (Bot API 10.5): a link in a raw payload keeps only these fields, each only with its type
LINK_FIELDS = {
    "invite_link": str, "creator": dict, "creates_join_request": bool, "is_primary": bool, "is_revoked": bool,
    "name": str, "expire_date": int, "member_limit": int, "pending_join_request_count": int,
    "subscription_period": int, "subscription_price": int,
}


class Malformed(ValueError):
    """The update's body cannot be parsed."""


# ------------------------------------------------------------------------------------------------------------
# Parsing (pure Python: nothing here touches the database)
# ------------------------------------------------------------------------------------------------------------


UNSTORABLE = re.compile("[\x00\ud800-\udfff]")  # NUL and surrogates: PostgreSQL text and jsonb refuse them


def _clean(v: Any) -> Any:
    """The JSON value with every string (keys included) storable: NUL and unpaired surrogates (a JSON escape like
    \\ud800 survives decoding) replaced with U+FFFD. Applied to each update before parsing, so the raw payload is
    clean before its jsonb write and the text columns too. Numbers stay as sent: a NaN or Infinity must fail
    validation (redact makes it null for the jsonb write)."""
    if isinstance(v, str):
        return UNSTORABLE.sub("\ufffd", v)
    if isinstance(v, dict):
        return {_clean(k): _clean(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_clean(x) for x in v]
    return v


BIGINT = (-(2 ** 63), 2 ** 63 - 1)
INT4 = (-(2 ** 31), 2 ** 31 - 1)
UPDATE_IDS = (0, 2 ** 63 - 2)  # next_offset = update_id + 1 must fit a bigint


def _int(v: Any, what: str, bounds: tuple[int, int] = BIGINT) -> int:
    """An integer that fits the database column (bigint unless said otherwise)."""
    if isinstance(v, bool) or not isinstance(v, int):
        raise Malformed(f"{what} is not an integer")
    if not bounds[0] <= v <= bounds[1]:
        raise Malformed(f"{what} is out of range")
    return v


def _opt_int(v: Any, bounds: tuple[int, int] = INT4) -> int | None:
    """An optional integer; one that is absent, not an integer or out of range is None."""
    if isinstance(v, bool) or not isinstance(v, int) or not bounds[0] <= v <= bounds[1]:
        return None
    return v


def _id_at(obj: Any, *path: str) -> int | None:
    """The id found at `path` inside a (possibly malformed) payload, when it is a valid bigint."""
    for key in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return _opt_int(obj, BIGINT)


def _opt_str(v: Any) -> str | None:
    return v if isinstance(v, str) and v else None


def _opt_bool(v: Any) -> bool | None:
    return v if isinstance(v, bool) else None


def _ts(v: Any, what: str) -> dt.datetime:
    n = _int(v, what)
    if n <= 0 or n > 32503680000:  # year 3000
        raise Malformed(f"{what} is out of range")
    return dt.datetime.fromtimestamp(n, dt.UTC)


def _until(v: Any) -> dt.datetime | None:
    """until_date: absent or 0 = forever / none."""
    if v is None or v == 0:
        return None
    return _ts(v, "until_date")


def _dict(v: Any, what: str) -> dict:
    if not isinstance(v, dict):
        raise Malformed(f"{what} is missing")
    return v


@dataclass(frozen=True)
class User:
    id: int
    is_bot: bool
    is_premium: bool | None
    language_code: str | None
    username: str | None
    first_name: str | None
    last_name: str | None


@dataclass(frozen=True)
class Chat:
    id: int
    type: str
    title: str | None
    username: str | None


@dataclass(frozen=True)
class Member:
    status: str
    user: User
    is_member: bool | None
    until_date: dt.datetime | None
    can_invite_users: bool | None
    can_restrict_members: bool | None


@dataclass(frozen=True)
class Link:
    invite_link: str
    creator: User | None
    name: str | None
    is_primary: bool | None
    creates_join_request: bool | None
    subscription_price: int | None


@dataclass
class Parsed:
    update_id: int
    type: str
    date: dt.datetime
    chat: Chat | None
    inner: dict | None  # the update's object (ChatMemberUpdated, ChatJoinRequest, Message)
    sender: User | None = None  # `from`
    member: Member | None = None
    old: Member | None = None
    link: Link | None = None
    via_join_request: bool = False
    via_folder: bool = False
    message_id: int | None = None
    migration: tuple[int, int] | None = None  # (old chat id, new chat id) of a migration service message
    giveaway: Giveaway | None = None


@dataclass(frozen=True)
class Giveaway:
    draw_at: dt.datetime
    winner_count: int | None
    only_new_members: bool
    chats: tuple[int, ...]


def parse_giveaway(v: Any) -> Giveaway:
    g = _dict(v, "giveaway")
    chats = g.get("chats") or []
    if not isinstance(chats, list):
        raise Malformed("giveaway.chats is not a list")
    return Giveaway(draw_at=_ts(g.get("winners_selection_date"), "giveaway.winners_selection_date"),
                    winner_count=_opt_int(g.get("winner_count")), only_new_members=g.get("only_new_members") is True,
                    chats=tuple(_int(_dict(c, "giveaway.chats[]").get("id"), "giveaway.chats[].id") for c in chats))


def parse_user(v: Any, what: str = "user") -> User:
    d = _dict(v, what)
    return User(id=_int(d.get("id"), f"{what}.id"), is_bot=d.get("is_bot") is True,
                is_premium=True if d.get("is_premium") is True else None,
                language_code=_opt_str(d.get("language_code")), username=_opt_str(d.get("username")),
                first_name=_opt_str(d.get("first_name")), last_name=_opt_str(d.get("last_name")))


def parse_chat(v: Any) -> Chat:
    d = _dict(v, "chat")
    kind = d.get("type")
    if not isinstance(kind, str):
        raise Malformed("chat.type is missing")
    return Chat(id=_int(d.get("id"), "chat.id"), type=kind, title=_opt_str(d.get("title")),
                username=_opt_str(d.get("username")))


def parse_member(v: Any, what: str) -> Member:
    d = _dict(v, what)
    status = d.get("status")
    if status not in STATUSES:
        raise Malformed(f"{what}.status {status!r} is unknown")
    is_member = _opt_bool(d.get("is_member")) if status == "restricted" else None
    return Member(status=status, user=parse_user(d.get("user"), f"{what}.user"), is_member=is_member,
                  until_date=_until(d.get("until_date")),
                  can_invite_users=_opt_bool(d.get("can_invite_users")),
                  can_restrict_members=_opt_bool(d.get("can_restrict_members")))


def parse_link(v: Any, bot_id: int) -> Link | None:
    """A ChatInviteLink. Its creator is required (Bot API); a link this bot did not make must be masked by
    Telegram, so an unmasked one of another creator is refused rather than stored as text."""
    if v is None:
        return None
    d = _dict(v, "invite_link")
    text = d.get("invite_link")
    if not isinstance(text, str) or not text:
        raise Malformed("invite_link.invite_link is missing")
    creator = parse_user(d.get("creator"), "invite_link.creator")
    if creator.id != bot_id and not is_masked(text):
        raise Malformed("invite_link of another creator is not masked")
    return Link(invite_link=text, creator=creator, name=_opt_str(d.get("name")),
                is_primary=_opt_bool(d.get("is_primary")),
                creates_join_request=_opt_bool(d.get("creates_join_request")),
                subscription_price=_opt_int(d.get("subscription_price")))


def _identified(raw: Any) -> bool:
    """An entry of a getUpdates response whose update_id can be read (and stored with its successor)."""
    return isinstance(raw, dict) and _opt_int(raw.get("update_id"), UPDATE_IDS) is not None


def update_type_of(raw: dict) -> str | None:
    keys = [k for k in raw if k != "update_id"]
    return keys[0] if len(keys) == 1 and isinstance(keys[0], str) else None


def parse_update(raw: dict, received: dt.datetime, bot_id: int) -> Parsed:
    """The update's fields this service uses, validated for the database; raises Malformed (or a
    TypeError/KeyError on odd shapes). `received` dates an update of a type that was not asked for and has no
    date. Everything the bodies write is checked here, so a body fails only on a database or program error."""
    update_id = _int(raw.get("update_id"), "update_id", UPDATE_IDS)
    kind = update_type_of(raw)
    if kind is None:
        raise Malformed("an update must carry exactly one object")
    inner = raw[kind]
    if not isinstance(inner, dict):
        raise Malformed(f"{kind} is not an object")
    if kind in ("chat_member", "my_chat_member"):
        p = Parsed(update_id, kind, _ts(inner.get("date"), "date"), parse_chat(inner.get("chat")), inner,
                   sender=parse_user(inner.get("from"), "from"),
                   member=parse_member(inner.get("new_chat_member"), "new_chat_member"),
                   old=parse_member(inner.get("old_chat_member"), "old_chat_member"),
                   link=parse_link(inner.get("invite_link"), bot_id),
                   via_join_request=inner.get("via_join_request") is True,
                   via_folder=inner.get("via_chat_folder_invite_link") is True)
        if p.member.user.id != p.old.user.id:
            raise Malformed("old and new chat member are different users")
        return p
    if kind == "chat_join_request":
        return Parsed(update_id, kind, _ts(inner.get("date"), "date"), parse_chat(inner.get("chat")), inner,
                      sender=parse_user(inner.get("from"), "from"),
                      link=parse_link(inner.get("invite_link"), bot_id))
    if kind in ("message", "channel_post"):
        sender = parse_user(inner["from"], "from") if "from" in inner else None
        chat = parse_chat(inner.get("chat"))
        migration = None
        if "migrate_to_chat_id" in inner:
            migration = chat.id, _int(inner["migrate_to_chat_id"], "migrate_to_chat_id")
        elif "migrate_from_chat_id" in inner:
            migration = _int(inner["migrate_from_chat_id"], "migrate_from_chat_id"), chat.id
        if migration is not None and migration[0] == migration[1]:
            raise Malformed("a migration to the same chat id")
        return Parsed(update_id, kind, _ts(inner.get("date"), "date"), chat, inner, sender=sender,
                      message_id=_int(inner.get("message_id"), "message_id"), migration=migration,
                      giveaway=parse_giveaway(inner["giveaway"]) if "giveaway" in inner else None)
    # a type that was not asked for: only its date, when it has one
    date = inner.get("date")
    chat = inner.get("chat")
    return Parsed(update_id, kind, _ts(date, "date") if date is not None else received,
                  parse_chat(chat) if chat is not None else None, None)


def is_masked(link: str) -> bool:
    """Telegram replaces the second half of other admins' links with an ellipsis (F§4)."""
    return "…" in link or "..." in link


def post_kind(msg: dict) -> str:
    for k in POST_KINDS:
        if k in msg:
            return k
    return "other"


def _is_user_dict(d: dict) -> bool:
    return isinstance(d.get("id"), int) and "is_bot" in d


def _expected_shape(holder: str | None, key: Any, value: Any) -> bool:
    """Whether a value has the shape redaction reads at its position in a membership payload. A person and a
    member are objects; invite_link is a ChatInviteLink (an object recognized by its text at invite_link) or the
    link's text; inside one of them (`holder`) only a member's `user` and a link's `creator` are objects, the
    rest scalars, and a link keeps only the ChatInviteLink fields, each with its type; a name is a string. Other
    positions hold anything."""
    if holder == "link":
        kind = LINK_FIELDS.get(key)
        return kind is not None and isinstance(value, kind) and not (kind is int and isinstance(value, bool))
    if holder is not None and isinstance(value, (dict, list)):
        return (holder, key) in (("member", "user"), ("link", "creator")) and isinstance(value, dict)
    if key in USER_KEYS or key in MEMBER_KEYS:
        return isinstance(value, dict)
    if key == "invite_link":
        return isinstance(value, str) or isinstance(value, dict) and isinstance(value.get("invite_link"), str)
    if key in NAME_KEYS:
        return isinstance(value, str)
    return True


# ------------------------------------------------------------------------------------------------------------
# The service entry points
# ------------------------------------------------------------------------------------------------------------


def identify(ctx: JobContext, conn: psycopg.Connection) -> None:
    if ctx.api is None:
        raise StartupError("ingest needs TGAUD_BOT_TOKEN")
    bot_id = int(ctx.api.get_me()["id"])
    fingerprint = ctx.config.link_key_fingerprint()
    rows = conn.execute("SELECT bot_id, link_key_fingerprint FROM tg.bot_state").fetchall()
    conn.rollback()
    if len(rows) > 1:
        raise StartupError(f"tg.bot_state has {len(rows)} rows; expected one bot")
    if rows:
        known_bot, known_fp = rows[0]
        if known_bot != bot_id:
            raise StartupError(f"TGAUD_BOT_TOKEN belongs to bot {bot_id}, but this database records bot "
                               f"{known_bot}: refusing to start")
        if known_fp is not None and known_fp != fingerprint:
            raise StartupError("TGAUD_LINK_HMAC_KEY differs from the key this database was started with "
                               "(it would re-key every bot invite link): refusing to start")
        if known_fp is not None:
            return
    # first start, or a row from before the fingerprint was recorded: both checks passed
    conn.execute("""INSERT INTO tg.bot_state (bot_id, allowed_updates, link_key_fingerprint) VALUES (%s, %s, %s)
                    ON CONFLICT (bot_id) DO UPDATE SET link_key_fingerprint = excluded.link_key_fingerprint
                    WHERE tg.bot_state.link_key_fingerprint IS NULL""",
                 (bot_id, list(ALLOWED_UPDATES), fingerprint))
    conn.commit()


def startup(ctx: JobContext, conn: psycopg.Connection) -> None:
    Ingest(ctx, conn).startup()


def run(ctx: JobContext, conn: psycopg.Connection) -> None:
    Ingest(ctx, conn).run()


# ------------------------------------------------------------------------------------------------------------
# The ingest worker
# ------------------------------------------------------------------------------------------------------------


@dataclass
class Effects:
    """What to tell the metrics and logs once the transaction committed."""

    actions: list[Callable[[], None]] = field(default_factory=list)

    def add(self, action: Callable[[], None]) -> None:
        self.actions.append(action)

    def apply(self) -> None:
        for a in self.actions:
            try:
                a()
            except Exception:  # a metric must never break ingest
                log.exception("ingest: a post-commit action failed")


class Ingest:
    def __init__(self, ctx: JobContext, conn: psycopg.Connection, *, poll_timeout: int = POLL_TIMEOUT) -> None:
        self.ctx = ctx
        self.conn = conn
        self.poll_timeout = poll_timeout
        self.bot_id: int | None = None
        self.store_names = ctx.config.store_names
        self._partitions: set[tuple[str, dt.date]] = set()
        self.backoff_base = 1.0  # first wait after a failed poll or database error (seconds); doubles
        self.min_empty_poll = MIN_EMPTY_POLL
        self._poll_backoff = self.backoff_base
        # an updates_not_allowed gap waits for the first poll with the full list; None = not read yet (startup
        # and run use separate instances: the polling one reads it from the database)
        self._updates_gap_open: bool | None = None
        # test hook: called inside each update (or empty poll) transaction just before the commit, with the
        # update id (None for an empty poll)
        self.before_commit: Callable[[int | None], None] | None = None

    # -- time --------------------------------------------------------------------------------------------

    def now(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self.ctx.clock(), dt.UTC)

    # -- transactions ------------------------------------------------------------------------------------

    def _bot_id(self) -> int:
        if self.bot_id is None:
            row = self.conn.execute("SELECT bot_id FROM tg.bot_state").fetchone()
            self.conn.rollback()
            if row is None:
                raise StartupError("tg.bot_state has no row: identify did not run")
            self.bot_id = int(row[0])
        return self.bot_id

    def _transaction(self, body: Callable[[Effects], Any], *, retry: bool = True) -> Any:
        """Run body in one fenced transaction and commit; a database error rolls back and (with retry) tries
        again with backoff until it succeeds or the stop is set (then returns None). A lost connection, a
        takeover (StaleGeneration) and anything that is not a database error propagate."""
        backoff = self.backoff_base
        while True:
            started = time.monotonic()
            effects = Effects()
            try:
                self.ctx.fence(self.conn)
                result = body(effects)
                self.conn.commit()
            except db.StaleGeneration:
                self._rollback()
                raise
            except psycopg.Error as e:
                self._rollback()
                if self.conn.closed or self.conn.broken:
                    raise
                metrics.INGEST_FAILURES.labels("db").inc()
                if not retry or self.ctx.stop.is_set():
                    raise
                log.warning("ingest: database error (%s: %s); retry in %.0f s", type(e).__name__,
                            str(e).splitlines()[0] if str(e) else "", backoff)
                if self.ctx.stop.wait(backoff):
                    return None
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue
            except BaseException:
                self._rollback()
                raise
            metrics.DB_TRANSACTION_SECONDS.labels("ingest").observe(time.monotonic() - started)
            effects.apply()
            return result if result is not None else True

    def _rollback(self) -> None:
        self._partitions.clear()  # partitions created in the rolled-back transaction are gone
        try:
            self.conn.rollback()
        except psycopg.Error:
            pass

    def _ensure_partitions(self, table: str, step: str, at: dt.datetime) -> None:
        day = at.date() if step == "day" else at.date().replace(day=1)
        if (table, day) in self._partitions:
            return
        start = dt.datetime.combine(day, dt.time(), dt.UTC)
        self.conn.execute("SELECT tg.ensure_partitions(%s, %s, %s, %s)",
                          (table, step, start, start + dt.timedelta(days=1)))
        self._partitions.add((table, day))

    # -- startup -----------------------------------------------------------------------------------------

    def startup(self) -> None:
        bot_id = self._bot_id()
        now = self.now()

        def prepare(fx: Effects) -> None:
            conn = self.conn
            conn.execute("SELECT * FROM tg.maintain_partitions(%s)", (now,))
            allowed, last_poll = conn.execute("SELECT allowed_updates, last_poll_ok_at FROM tg.bot_state "
                                              "WHERE bot_id = %s FOR NO KEY UPDATE", (bot_id,)).fetchone()
            if "chat_member" not in (allowed or []):
                # updates sent before this start lacked chat_member: those joins and leaves are lost (D6). The
                # gap runs from the first coverage (or the last poll) until the first poll with the full list.
                open_gap = conn.execute("""SELECT 1 FROM tg.gap WHERE chat_id IS NULL AND reason = 'updates_not_allowed'
                                           AND upper_inf(period)""").fetchone()
                if open_gap is None:
                    conn.execute(
                        """INSERT INTO tg.gap (chat_id, period, reason, confirmed, detected_at, note)
                           SELECT NULL, tstzrange(least(coalesce(min(lower(period)), %(last)s, %(now)s), %(now)s), NULL),
                                  'updates_not_allowed', true, %(now)s, %(note)s FROM tg.coverage""",
                        dict(last=last_poll, now=now, note=f"allowed_updates was {allowed}: chat_member missing"))
            conn.execute("UPDATE tg.bot_state SET allowed_updates = %s, updated_at = now() WHERE bot_id = %s",
                         (list(ALLOWED_UPDATES), bot_id))
            # every tracked chat except the old group of a migration: a rights check now, then getChat; and the
            # destination of every migration still in M2, even one an earlier check found the bot not admin in
            # (its promotion update may have expired during an outage): admin there now means M1
            conn.execute("""UPDATE tg.chat c SET rights_due_at = %(now)s, metadata_due_at = %(now)s
                            WHERE (c.tracked OR EXISTS (SELECT 1 FROM tg.chat_alias a
                                                        WHERE a.chat_id = c.chat_id AND a.moved_at IS NULL))
                              AND NOT EXISTS (SELECT 1 FROM tg.chat_alias a WHERE a.old_chat_id = c.chat_id)""",
                         dict(now=now))
            self._updates_gap_open = conn.execute(
                """SELECT EXISTS (SELECT 1 FROM tg.gap WHERE chat_id IS NULL AND reason = 'updates_not_allowed'
                                  AND upper_inf(period))""").fetchone()[0]

        self._transaction(prepare)
        self.refresh_rights(limit=None)

    def refresh_rights(self, limit: int | None = METADATA_BATCH) -> int:
        """The rights check of chats whose tg.chat.rights_due_at has passed (startup, first seen, a failed
        check's retry); returns how many succeeded."""
        if self.ctx.api is None:
            return 0
        due = self.conn.execute(
            """SELECT c.chat_id, c.rights_attempts FROM tg.chat c
               WHERE c.rights_due_at <= %s AND NOT EXISTS (SELECT 1 FROM tg.chat_alias a WHERE a.old_chat_id = c.chat_id)
               ORDER BY c.rights_due_at, c.chat_id LIMIT %s""", (self.now(), limit)).fetchall()
        self.conn.rollback()
        ok = 0
        for chat_id, attempts in due:
            if self.ctx.stop.is_set():
                break
            ok += bool(self.check_rights(chat_id, attempts))
        return ok

    def check_rights(self, chat_id: int, attempts: int = 0) -> bool:
        """getChatMember(chat, bot) -> bot status, rights, coverage and gaps of one chat. A transient failure
        keeps the check due (with backoff) and marks the chat's admin status unknown with a suspected gap until a
        check or a newer event resolves it."""
        assert self.ctx.api is not None
        bot_id = self._bot_id()
        member: Member | None = None
        failure = None
        try:
            member = parse_member(self.ctx.api.get_chat_member(chat_id, bot_id), "chat_member")
        except ApiError as e:
            text = (e.description or "").lower()
            if e.outcome in ("400", "403") and "upgraded" not in text:
                # chat not found, bot kicked or not a member: the bot is not admin there
                member = Member("kicked" if "kick" in text else "left", User(bot_id, True, None, None, None, None, None),
                                None, None, None, None)
            elif "upgraded" in text:
                failure = "the group was upgraded to a supergroup (its migration message decides)"
            else:
                failure = e.outcome
        except (Malformed, KeyError, TypeError) as e:
            failure = f"unexpected member ({e})"
        at = self.now()
        if member is None:
            log.warning("ingest: rights check of chat %s failed (%s); retried later", chat_id, failure)
            wait = METADATA_BACKOFF[min(attempts, len(METADATA_BACKOFF) - 1)]

            def later(fx: Effects) -> None:
                history.lock_chats(self.conn, [chat_id])
                row = self.conn.execute(
                    """UPDATE tg.chat SET rights_due_at = %s, rights_attempts = rights_attempts + 1,
                              rights_failed_at = coalesce(rights_failed_at, %s)
                       WHERE chat_id = %s RETURNING tracked""",
                    (at + dt.timedelta(seconds=wait), at, chat_id)).fetchone()
                if row is None or not row[0]:
                    return
                covered = self.conn.execute(
                    """SELECT 1 FROM tg.gap WHERE chat_id = %(c)s AND reason IN ('not_admin', 'rights_lost')
                       AND lower(period) <= %(at)s AND (upper_inf(period) OR upper(period) > %(at)s)""",
                    dict(c=chat_id, at=at)).fetchone()
                if covered is None:
                    self.conn.execute(
                        """INSERT INTO tg.gap (chat_id, period, reason, confirmed, detected_at, note)
                           VALUES (%s, tstzrange(%s, NULL), 'not_admin', false, %s, %s)""",
                        (chat_id, at, at, f"rights check failed ({failure}): admin status unknown"))

            self._transaction(later)
            return False

        def body(fx: Effects) -> None:
            pending = self._pending_aliases(chat_id)
            history.lock_chats(self.conn, [chat_id, *(o for o, _ in pending)])
            if self.conn.execute("SELECT 1 FROM tg.chat WHERE chat_id = %s", (chat_id,)).fetchone() is None:
                return
            self._set_bot_state(chat_id, at, member, "rights_check", fx)
            self.conn.execute("UPDATE tg.chat SET rights_due_at = NULL, rights_attempts = 0, rights_failed_at = NULL "
                              "WHERE chat_id = %s", (chat_id,))
            self._move_pending(chat_id, pending)

        self._transaction(body)
        return True

    # -- the loop ----------------------------------------------------------------------------------------

    def run(self) -> None:
        self._bot_id()
        while not self.ctx.stop.is_set():
            self.refresh_rights()
            self.refresh_metadata()
            if self.ctx.stop.is_set():
                return
            self.poll_once()

    def _offset(self) -> int | None:
        """The offset for the next getUpdates: None while the last committed update is older than 6 days."""
        row = self.conn.execute("SELECT next_offset, last_update_at FROM tg.bot_state WHERE bot_id = %s",
                                (self._bot_id(),)).fetchone()
        self.conn.rollback()
        next_offset, last_update_at = row
        if last_update_at is None or self.now() - last_update_at > STALE_OFFSET:
            return None
        return int(next_offset)

    def poll_once(self) -> int | None:
        """One getUpdates and its updates; returns how many updates were committed (None after a failed
        poll)."""
        assert self.ctx.api is not None
        offset = self._offset()
        sent = self.now()
        started = time.monotonic()
        try:
            updates = self.ctx.api.get_updates(offset, timeout=self.poll_timeout, limit=LIMIT,
                                               allowed_updates=ALLOWED_UPDATES)
            if not isinstance(updates, list):
                raise ApiError("getUpdates", "other", "result is not a list")
        except ApiError as e:
            if self.ctx.stop.is_set():
                return None
            wait = e.retry_after if e.outcome == "429" and e.retry_after else self._poll_backoff
            log.warning("ingest: getUpdates failed (%s: %s); retry in %.0f s", e.outcome, e.description, wait)
            self._poll_backoff = min(self._poll_backoff * 2, MAX_BACKOFF)
            self.ctx.stop.wait(wait)
            return None
        except Exception:
            if self.ctx.stop.is_set():  # the client was closed by the shutdown
                return None
            raise
        # Telegram sends a response in update_id order: an entry whose update_id can't be read ends it there, so
        # neither the offset nor caught_up_at passes it, and the next poll (after the backoff) asks again from
        # the same offset. While Telegram keeps sending it ingest stalls, visibly: evidence may be missing.
        usable = next((i for i, raw in enumerate(updates) if not _identified(raw)), len(updates))
        if usable == len(updates):
            self._poll_backoff = self.backoff_base
        received = self.now()
        self.ctx.health.mark_poll_ok()
        if not updates:
            def empty(fx: Effects) -> None:
                self._advance(fx, received, caught_up=sent)
                self._hook(None)

            if self._transaction(empty) is not None:
                self.ctx.health.mark_commit()
            elapsed = time.monotonic() - started
            if elapsed < self.min_empty_poll:
                self.ctx.stop.wait(self.min_empty_poll - elapsed)
            return 0
        done = 0
        short = len(updates) < LIMIT and usable == len(updates)
        for i, raw in enumerate(updates[:usable]):
            if self.ctx.stop.is_set():
                return done
            last = i == len(updates) - 1
            if self.handle(raw, received, sent if (last and short) else None):
                done += 1
                self.ctx.health.mark_commit()
            elif self.ctx.stop.is_set():
                return done
        if usable < len(updates):
            metrics.INGEST_FAILURES.labels("parse").inc()
            wait = self._poll_backoff
            log.error("ingest: entry %d of a getUpdates response has no valid update_id; nothing from it on is "
                      "acknowledged (offset and caught_up_at stay); polled again in %.0f s", usable, wait)
            self._poll_backoff = min(self._poll_backoff * 2, MAX_BACKOFF)
            self.ctx.stop.wait(wait)
        return done

    # -- one update --------------------------------------------------------------------------------------

    def handle(self, raw: Any, received: dt.datetime, caught_up: dt.datetime | None) -> bool:
        """Process one update in one transaction; True when it committed."""
        if not _identified(raw):  # poll_once never passes one: it ends the response there
            metrics.INGEST_FAILURES.labels("parse").inc()
            log.error("ingest: an update without a valid update_id was not processed")
            return False
        raw = _clean(raw)
        try:
            parsed = parse_update(raw, received, self._bot_id())
        except (Malformed, KeyError, TypeError, ValueError, AttributeError) as e:
            return self._malformed(raw, received, caught_up, f"{type(e).__name__}: {e}")
        try:
            ok = self._transaction(lambda fx: self._apply(fx, parsed, raw, received, caught_up))
        except Malformed as e:  # input the parser let through (rolled back): recorded, never a wedge
            return self._malformed(raw, received, caught_up, f"{type(e).__name__}: {e}")
        # Anything else (a program error in a body or a rebuild) propagates after the rollback: ingest ends and
        # the restarted service retries the valid update, instead of acknowledging it without its data.
        return ok is not None

    def _malformed(self, raw: dict, received: dt.datetime, caught_up: dt.datetime | None, why: str) -> bool:
        update_id = int(raw["update_id"])
        kind = update_type_of(raw)
        inner = raw.get(kind) if kind else None
        chat_id = None
        if isinstance(inner, dict) and isinstance(inner.get("chat"), dict) \
                and inner["chat"].get("type") in AUDIENCE_CHATS:
            chat_id = _id_at(inner, "chat", "id")
        log.error("ingest: update %s (%s) does not parse (%s): recorded as malformed", update_id, kind, why)

        def body(fx: Effects) -> None:
            self._ensure_partitions("update_receipt", "day", received)
            self.conn.execute("""INSERT INTO tg.update_receipt (update_id, update_date, update_type, chat_id, received_at)
                                 VALUES (%s, %s, 'malformed', %s, %s) ON CONFLICT DO NOTHING""",
                              (update_id, received, chat_id, received))
            # the chat id read whatever chat.type says (the queue only matches a pending destination)
            notice_chat = _id_at(inner, "chat", "id")
            if kind in ("chat_member", "my_chat_member") and notice_chat is not None:
                self._queue_m2_check(notice_chat, received)
            if kind in MEMBERSHIP_TYPES and chat_id is not None and isinstance(inner, dict):
                subject = _subject_id(kind, inner)
                # the erasure cutoff by the payload's date; when that is unusable, anyone ever erased counts
                try:
                    dated = _ts(inner.get("date"), "date")
                except Malformed:
                    dated = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
                erased = self._erased(_payload_user_ids(inner), dated)
                if subject not in erased:
                    try:
                        payload = self.redact(inner, erased)
                    except Exception as e:  # a shape the redactor cannot read: the receipt alone is kept
                        log.error("ingest: malformed update %s cannot be redacted (%s); raw payload not kept",
                                  update_id, type(e).__name__)
                    else:
                        self._raw_update(update_id, received, kind, chat_id, subject, inner, received, erased,
                                         payload=payload)
            self._advance(fx, received, update_id=update_id, update_date=received, caught_up=caught_up)
            fx.add(lambda: metrics.INGEST_FAILURES.labels("parse").inc())
            fx.add(lambda: metrics.UPDATES.labels("malformed").inc())
            self._hook(update_id)

        return self._transaction(body) is not None

    def _hook(self, update_id: int | None) -> None:
        if self.before_commit is not None:
            self.before_commit(update_id)

    def _apply(self, fx: Effects, p: Parsed, raw: dict, received: dt.datetime,
               caught_up: dt.datetime | None) -> None:
        conn = self.conn
        in_scope = p.chat is not None and p.chat.type in AUDIENCE_CHATS
        self._ensure_partitions("update_receipt", "day", p.date)
        seen = conn.execute("""INSERT INTO tg.update_receipt (update_id, update_date, update_type, chat_id, received_at)
                               VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING 1""",
                            (p.update_id, p.date, p.type, p.chat.id if in_scope else None, received)).fetchone() is None
        if not seen and in_scope:
            self._body(fx, p, raw, received)
        if not seen and p.chat is not None and p.type in ("chat_member", "my_chat_member"):
            # after the body (M1 may have just run), and whatever chat.type says: the queue only matches a
            # pending destination
            self._queue_m2_check(p.chat.id, received)
        self._advance(fx, received, update_id=p.update_id, update_date=p.date, caught_up=caught_up)
        lag = max(0.0, (received - p.date).total_seconds())
        fx.add(lambda: metrics.UPDATES.labels(p.type).inc())
        fx.add(lambda: metrics.UPDATE_LAG.labels(p.type).observe(lag))
        self._hook(p.update_id)

    def _advance(self, fx: Effects, received: dt.datetime, *, update_id: int | None = None,
                 update_date: dt.datetime | None = None, caught_up: dt.datetime | None = None) -> None:
        """tg.bot_state after an update (or an empty poll), and the service_down gaps."""
        bot_id = self._bot_id()
        if update_id is None:
            row = self.conn.execute(
                """WITH old AS (SELECT last_poll_ok_at FROM tg.bot_state WHERE bot_id = %(bot)s)
                   UPDATE tg.bot_state SET last_poll_ok_at = greatest(last_poll_ok_at, %(recv)s),
                          caught_up_at = CASE WHEN %(cu)s::timestamptz IS NULL THEN caught_up_at
                                              ELSE greatest(caught_up_at, %(cu)s) END,
                          updated_at = now()
                   WHERE bot_id = %(bot)s RETURNING (SELECT last_poll_ok_at FROM old)""",
                dict(bot=bot_id, recv=received, cu=caught_up)).fetchone()
        else:
            row = self.conn.execute(
                """WITH old AS (SELECT last_poll_ok_at FROM tg.bot_state WHERE bot_id = %(bot)s)
                   UPDATE tg.bot_state SET next_offset = %(next)s::bigint, last_update_id = %(id)s::bigint,
                          last_update_at = %(date)s, last_poll_ok_at = greatest(last_poll_ok_at, %(recv)s),
                          caught_up_at = CASE WHEN %(cu)s::timestamptz IS NULL THEN caught_up_at
                                              ELSE greatest(caught_up_at, %(cu)s) END,
                          updated_at = now()
                   WHERE bot_id = %(bot)s RETURNING (SELECT last_poll_ok_at FROM old)""",
                dict(bot=bot_id, id=update_id, next=update_id + 1, date=update_date, recv=received,
                     cu=caught_up)).fetchone()
        previous = row[0] if row else None
        if self._updates_gap_open is None:
            self._updates_gap_open = self.conn.execute(
                """SELECT EXISTS (SELECT 1 FROM tg.gap WHERE chat_id IS NULL AND reason = 'updates_not_allowed'
                                  AND upper_inf(period))""").fetchone()[0]
        if self._updates_gap_open:  # this poll sent the full allowed_updates
            self.conn.execute("""UPDATE tg.gap SET period = tstzrange(lower(period), greatest(lower(period), %s))
                                 WHERE chat_id IS NULL AND reason = 'updates_not_allowed' AND upper_inf(period)""",
                              (received,))
            fx.add(lambda: setattr(self, "_updates_gap_open", False))
        if previous is not None and received - previous > SERVICE_DOWN:
            self.conn.execute("""INSERT INTO tg.gap (chat_id, period, reason, confirmed, detected_at, note)
                                 VALUES (NULL, tstzrange(%s, %s), 'service_down', true, %s, %s)""",
                              (previous, received, received, "no successful getUpdates for longer than 24 h"))
            lost = (received - previous - SERVICE_DOWN).total_seconds()
            fx.add(lambda: metrics.LOST_SECONDS.inc(lost))
            log.warning("ingest: no successful poll since %s: service_down gap recorded", previous.isoformat())
        if update_date is not None and received - update_date > LAG_SUSPECT:
            hit = self.conn.execute("""SELECT 1 FROM tg.gap WHERE chat_id IS NULL AND reason = 'service_down'
                                       AND period && tstzrange(%s, %s) LIMIT 1""",
                                    (update_date, received)).fetchone()
            if hit is None:
                self.conn.execute("""INSERT INTO tg.gap (chat_id, period, reason, confirmed, detected_at, note)
                                     VALUES (NULL, tstzrange(%s, %s), 'service_down', false, %s, %s)""",
                                  (update_date, received, received, "update lag passed 20 h"))

    # -- bodies ------------------------------------------------------------------------------------------

    def _route(self, chat_id: int) -> int:
        row = self.conn.execute("SELECT chat_id FROM tg.chat_alias WHERE old_chat_id = %s AND moved_at IS NOT NULL",
                                (chat_id,)).fetchone()
        return int(row[0]) if row else chat_id

    def _erased(self, ids: Iterable[int | None], at: dt.datetime) -> set[int]:
        ids = sorted({i for i in ids if i is not None})
        if not ids:
            return set()
        rows = self.conn.execute("SELECT u FROM unnest(%s::bigint[]) u WHERE tg.is_erased(u, %s)",
                                 (ids, at)).fetchall()
        return {int(r[0]) for r in rows}

    def _body(self, fx: Effects, p: Parsed, raw: dict, received: dt.datetime) -> None:
        conn = self.conn
        if p.type not in (*MEMBERSHIP_TYPES, "message", "channel_post"):
            return
        assert p.chat is not None and p.inner is not None
        subject = self._subject(p)
        secondary = [p.sender.id if p.sender else None, p.link.creator.id if p.link and p.link.creator else None]
        erased = self._erased([subject, *secondary], p.date)
        if subject is not None and subject in erased:
            return  # evidence from before the person's erasure: the receipt only
        # writers of evidence lock every chat the transaction touches, once, in ascending order
        raw_chat = p.chat.id
        migration = p.migration
        pending: list[tuple[int, dt.datetime]] = []
        if p.type == "my_chat_member":
            pending = self._pending_aliases(raw_chat)
        if p.type in ("chat_member", "my_chat_member") or migration is not None:
            lock = {raw_chat, self._route(raw_chat), *(o for o, _ in pending)}
            if migration is not None:
                lock |= {migration[0], migration[1]}
            history.lock_chats(conn, lock)
        chat_id = self._route(raw_chat)
        if chat_id == raw_chat:
            self._upsert_chat(p.chat, received)
        if p.type == "chat_member":
            self._chat_member(fx, p, chat_id, raw_chat, erased, received)
        elif p.type == "my_chat_member":
            self._my_chat_member(fx, p, chat_id, raw_chat, pending, erased, received)
        elif p.type == "chat_join_request":
            self._join_request(fx, p, chat_id, raw_chat, erased, received)
        elif p.type == "message":
            self._message(fx, p, chat_id, erased, received)
        elif p.type == "channel_post":
            self._channel_post(fx, p, chat_id)

    def _queue_m2_check(self, chat_id: int, received: dt.datetime) -> None:
        """A chat_member or my_chat_member update from the destination of a migration still in M2 (any receipt:
        stored, receipt-only for an erased person, or malformed): the bot may be admin there by now with its
        promotion update lost, so a rights check decides M1. Chat-level: nothing about the person is stored."""
        self.conn.execute("""UPDATE tg.chat c SET rights_due_at = coalesce(c.rights_due_at, %s)
                             WHERE c.chat_id = %s AND EXISTS (SELECT 1 FROM tg.chat_alias a
                                                              WHERE a.chat_id = c.chat_id AND a.moved_at IS NULL)""",
                          (received, chat_id))

    def _pending_aliases(self, chat_id: int) -> list[tuple[int, dt.datetime]]:
        """Migrations into this chat whose history waits for M1 (old chat id, migrated_at)."""
        return [(int(r[0]), r[1]) for r in self.conn.execute(
            "SELECT old_chat_id, migrated_at FROM tg.chat_alias WHERE chat_id = %s AND moved_at IS NULL "
            "ORDER BY old_chat_id", (chat_id,)).fetchall()]

    def _move_pending(self, chat_id: int, pending: list[tuple[int, dt.datetime]]) -> None:
        """M1 for migrations into `chat_id` once it is tracked (the caller locked every chat involved)."""
        if not pending:
            return
        tracked = self.conn.execute("SELECT tracked FROM tg.chat WHERE chat_id = %s", (chat_id,)).fetchone()
        if tracked and tracked[0]:
            for old, migrated_at in pending:
                self._move(old, chat_id, migrated_at)

    def _subject(self, p: Parsed) -> int | None:
        if p.type == "chat_member" and p.member:
            return p.member.user.id
        if p.type == "chat_join_request" and p.sender:
            return p.sender.id
        # a message's sender is not its subject: an erased sender loses only the activity count
        # (_message), while chat-level evidence (a migration, a giveaway) stores no sender and stays
        return None

    def _upsert_chat(self, chat: Chat, received: dt.datetime) -> None:
        title = chat.title or str(chat.id)
        self.conn.execute(
            """INSERT INTO tg.chat AS c (chat_id, chat_type, title, username, metadata_due_at, rights_due_at)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (chat_id) DO UPDATE SET chat_type = excluded.chat_type, title = excluded.title,
                      username = excluded.username, updated_at = now()
               WHERE (c.chat_type, c.title, c.username) IS DISTINCT FROM
                     (excluded.chat_type, excluded.title, excluded.username)""",
            (chat.id, chat.type, title, chat.username, received, received))

    def _upsert_user(self, u: User, at: dt.datetime) -> None:
        names = (u.username, u.first_name, u.last_name) if self.store_names else (None, None, None)
        self.conn.execute(
            """INSERT INTO tg.tg_user AS t (user_id, is_bot, is_premium, language_code, username, first_name,
                                            last_name, first_seen_at, last_seen_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (user_id) DO UPDATE SET
                 first_seen_at = least(t.first_seen_at, excluded.first_seen_at),
                 last_seen_at = greatest(t.last_seen_at, excluded.last_seen_at),
                 is_bot = CASE WHEN excluded.last_seen_at >= t.last_seen_at THEN excluded.is_bot ELSE t.is_bot END,
                 is_premium = CASE WHEN excluded.last_seen_at >= t.last_seen_at THEN excluded.is_premium
                                   ELSE t.is_premium END,
                 language_code = CASE WHEN excluded.last_seen_at >= t.last_seen_at THEN excluded.language_code
                                      ELSE t.language_code END,
                 username = CASE WHEN excluded.last_seen_at >= t.last_seen_at THEN excluded.username
                                 ELSE t.username END,
                 first_name = CASE WHEN excluded.last_seen_at >= t.last_seen_at THEN excluded.first_name
                                   ELSE t.first_name END,
                 last_name = CASE WHEN excluded.last_seen_at >= t.last_seen_at THEN excluded.last_name
                                  ELSE t.last_name END""",
            (u.id, u.is_bot, u.is_premium, u.language_code, *names, at, at))

    def _is_bot_link(self, link: Link) -> bool:
        return link.creator is not None and link.creator.id == self._bot_id()

    def _upsert_link(self, chat_id: int, link: Link, at: dt.datetime, erased: set[int]) -> int:
        creator = link.creator.id if link.creator and link.creator.id not in erased else None
        common = (chat_id, creator, link.name, link.is_primary, link.creates_join_request, link.subscription_price,
                  at, at)
        update = """name = coalesce(excluded.name, l.name), is_primary = coalesce(excluded.is_primary, l.is_primary),
                    creates_join_request = coalesce(excluded.creates_join_request, l.creates_join_request),
                    subscription_price = coalesce(excluded.subscription_price, l.subscription_price),
                    first_seen_at = least(l.first_seen_at, excluded.first_seen_at),
                    last_seen_at = greatest(l.last_seen_at, excluded.last_seen_at)"""
        if self._is_bot_link(link):
            digest = self.ctx.config.link_digest(link.invite_link)
            row = self.conn.execute(
                f"""INSERT INTO tg.invite_link AS l (chat_id, creator_id, name, is_primary, creates_join_request,
                                                     subscription_price, first_seen_at, last_seen_at, link_digest)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chat_id, link_digest) WHERE link_digest IS NOT NULL DO UPDATE SET {update},
                       creator_id = coalesce(l.creator_id, excluded.creator_id)
                    RETURNING invite_link_id""", (*common, digest)).fetchone()
        else:
            row = self.conn.execute(
                f"""INSERT INTO tg.invite_link AS l (chat_id, creator_id, name, is_primary, creates_join_request,
                                                     subscription_price, first_seen_at, last_seen_at, masked_link)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chat_id, masked_link, creator_id) WHERE link_digest IS NULL DO UPDATE SET {update}
                    RETURNING invite_link_id""", (*common, link.invite_link)).fetchone()
        return int(row[0])

    def _chat_member(self, fx: Effects, p: Parsed, chat_id: int, raw_chat: int, erased: set[int],
                     received: dt.datetime) -> None:
        conn = self.conn
        assert p.member and p.old and p.sender
        user = p.member.user
        self._upsert_user(user, p.date)
        link_id = self._upsert_link(chat_id, p.link, p.date, erased) if p.link else None
        actor_is_other = p.sender.id != user.id
        actor_id = p.sender.id if actor_is_other and p.sender.id not in erased else None
        self._ensure_partitions("member_event", "month", p.date)
        row = conn.execute(
            """INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, received_at, old_status,
                                            old_is_member, new_status, new_is_member, actor_id, actor_is_other,
                                            invite_link_id, via_join_request, via_folder_link, old_until_date,
                                            new_until_date, source_chat_id)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (update_id, event_at) DO NOTHING RETURNING event_id, kind""",
            (chat_id, user.id, p.date, p.update_id, received, p.old.status, p.old.is_member, p.member.status,
             p.member.is_member, actor_id, actor_is_other, link_id, p.via_join_request, p.via_folder,
             p.old.until_date, p.member.until_date, raw_chat if raw_chat != chat_id else None)).fetchone()
        if row is not None:
            event_id, kind = row
            label = kind if kind in EVENT_LABELS else "role"
            chat = str(chat_id)
            at = p.date.timestamp()
            fx.add(lambda: metrics.CHAT_EVENTS.labels(chat, label).inc())
            fx.add(lambda: metrics.LAST_TRANSITION.labels(chat).set(at))
            # An approved request: a direct one (via_join_request, no link) or one through a link that needs
            # approval (the join carries that link, still with creates_join_request). A pending request for
            # another link, or a self-join after the link stopped asking for approval, stays pending.
            linked_approval = link_id is not None and p.link is not None and p.link.creates_join_request is True
            if kind == "join" and (p.via_join_request or linked_approval):
                resolver = p.sender.id if actor_is_other and p.sender.id not in erased else None
                hit = conn.execute(
                    """UPDATE tg.join_request SET outcome = 'approved', resolved_at = %s, resolved_by = %s
                       WHERE join_request_id = (SELECT join_request_id FROM tg.join_request
                                                WHERE chat_id = %s AND user_id = %s AND outcome IS NULL
                                                  AND requested_at <= %s
                                                  AND invite_link_id IS NOT DISTINCT FROM %s
                                                ORDER BY requested_at DESC LIMIT 1)
                       RETURNING 1""",
                    (p.date, resolver, chat_id, user.id, p.date, None if p.via_join_request else link_id)).fetchone()
                if hit is not None:
                    fx.add(lambda: metrics.CHAT_JOIN_REQUESTS.labels(chat, "approved").inc())
            history.rebuild_pairs(conn, [(chat_id, user.id)])
            if kind in EXIT_KINDS:
                # a short stay is the rebuilt spell this exit closed, from its exact join (a migration echo
                # absorbed by E3 is not a join)
                spell = conn.execute("""SELECT joined_at FROM tg.membership
                                        WHERE chat_id = %s AND user_id = %s AND left_event_id = %s""",
                                     (chat_id, user.id, event_id)).fetchone()
                if spell is not None and spell[0] is not None and p.date - spell[0] <= SHORT_STAY:
                    fx.add(lambda: metrics.CHAT_SHORT_STAYS.labels(chat).inc())
        self._raw_update(p.update_id, p.date, p.type, raw_chat, user.id, p.inner, received, erased)

    def _my_chat_member(self, fx: Effects, p: Parsed, chat_id: int, raw_chat: int,
                        pending: list[tuple[int, dt.datetime]], erased: set[int], received: dt.datetime) -> None:
        assert p.member and p.sender
        subject = p.sender.id if p.sender.id not in erased else None
        # The old group of a migration ended at the migration: news about the bot there changes nothing.
        old_group = self.conn.execute("SELECT 1 FROM tg.chat_alias WHERE old_chat_id = %s", (raw_chat,)).fetchone()
        if old_group is None:
            if p.member.user.id != self._bot_id():
                log.warning("ingest: my_chat_member about user %s, not this bot; ignored", p.member.user.id)
            else:
                if self._set_bot_state(chat_id, p.date, p.member, "my_chat_member", fx):
                    self.conn.execute("UPDATE tg.chat SET metadata_due_at = %s, metadata_attempts = 0 "
                                      "WHERE chat_id = %s", (received, chat_id))
                self._move_pending(chat_id, pending)
        self._raw_update(p.update_id, p.date, p.type, raw_chat, subject, p.inner, received, erased)

    def _set_bot_state(self, chat_id: int, at: dt.datetime, member: Member, source: str, fx: Effects) -> bool:
        """The bot's status and rights in one chat as of `at` (an event's date or a check's time): tg.chat,
        coverage, not_admin / rights_lost gaps. News older than the chat's last observation (a queued update
        processed after the startup check) is replayed at its own time for coverage and gaps, and the newer
        observation is then applied again from its time: the current state stays the observed one and the
        history keeps what happened in between. The old group of a migration is left as the migration closed
        it. Returns True when the chat became tracked."""
        conn = self.conn
        row = conn.execute(
            """SELECT c.tracked, c.rights_checked_at, c.bot_status, c.can_invite_users, c.can_restrict_members,
                      EXISTS (SELECT 1 FROM tg.chat_alias a WHERE a.old_chat_id = c.chat_id), c.rights_failed_at
               FROM tg.chat c WHERE c.chat_id = %s FOR UPDATE""", (chat_id,)).fetchone()
        if row is None or row[5]:
            return False
        was_tracked, checked_at, cur_status, cur_invite, cur_restrict, _, failed_at = row
        status = member.status
        admin = status in ADMIN
        can_invite = True if status == "creator" else (bool(member.can_invite_users) if admin else None)
        can_restrict = True if status == "creator" else (bool(member.can_restrict_members) if admin else None)
        tracked = bool(was_tracked or admin)
        if checked_at is None or at >= checked_at:
            # a pending retry after a failed check stays until a check succeeds or news from at least the
            # failed observation's time resolves it
            resolves = source == "rights_check" or failed_at is None or at >= failed_at
            conn.execute("""UPDATE tg.chat SET bot_status = %(st)s, can_invite_users = %(ci)s,
                                   can_restrict_members = %(cr)s, rights_checked_at = %(at)s, tracked = %(tr)s,
                                   rights_due_at = CASE WHEN %(res)s THEN NULL ELSE rights_due_at END,
                                   rights_attempts = CASE WHEN %(res)s THEN 0 ELSE rights_attempts END,
                                   rights_failed_at = CASE WHEN %(res)s THEN NULL ELSE rights_failed_at END,
                                   updated_at = now() WHERE chat_id = %(c)s""",
                         dict(st=status, ci=can_invite, cr=can_restrict, at=at, tr=tracked, res=resolves, c=chat_id))
            self._rights_history(chat_id, at, status, can_invite, can_restrict, tracked, source)
            chat = str(chat_id)
            fx.add(lambda: metrics.CHAT_BOT_ADMIN.labels(chat).set(1 if admin else 0))
            fx.add(lambda: metrics.CHAT_PERMISSION_OK.labels(chat, "invite_users").set(1 if can_invite else 0))
            fx.add(lambda: metrics.CHAT_PERMISSION_OK.labels(chat, "restrict_members").set(1 if can_restrict else 0))
        else:
            conn.execute("UPDATE tg.chat SET tracked = %s, updated_at = now() WHERE chat_id = %s", (tracked, chat_id))
            self._rights_history(chat_id, at, status, can_invite, can_restrict, tracked, source)
            self._rights_history(chat_id, checked_at, cur_status, cur_invite, cur_restrict, tracked, "rights_check")
        return tracked and not was_tracked

    def _rights_history(self, chat_id: int, at: dt.datetime, status: str, can_invite: bool | None,
                        can_restrict: bool | None, tracked: bool, source: str) -> None:
        """Coverage and rights gaps from `at` on, for a bot in `status` with these rights."""
        conn = self.conn
        admin = status in ADMIN
        if admin:
            open_from = conn.execute("SELECT lower(period) FROM tg.coverage WHERE chat_id = %s AND upper_inf(period)",
                                     (chat_id,)).fetchone()
            if open_from is None:
                conn.execute("""INSERT INTO tg.coverage (chat_id, period, source)
                                SELECT %(c)s, tstzrange(greatest(%(at)s, max(upper(period))), NULL), %(s)s
                                FROM tg.coverage WHERE chat_id = %(c)s""", dict(c=chat_id, at=at, s=source))
            elif open_from[0] > at:  # admin since earlier than the open period says (a replayed event)
                conn.execute("""UPDATE tg.coverage SET period = tstzrange(greatest(%(at)s,
                                    (SELECT max(upper(x.period)) FROM tg.coverage x
                                     WHERE x.chat_id = %(c)s AND NOT upper_inf(x.period))), NULL)
                                WHERE chat_id = %(c)s AND upper_inf(period)""", dict(c=chat_id, at=at))
        else:  # not admin from `at`: a period covering it ends there (also one a replay's re-check closed later)
            conn.execute("DELETE FROM tg.coverage WHERE chat_id = %s AND upper_inf(period) AND lower(period) >= %s",
                         (chat_id, at))
            conn.execute("""UPDATE tg.coverage SET period = tstzrange(lower(period), %(at)s)
                            WHERE chat_id = %(c)s AND lower(period) < %(at)s
                              AND (upper_inf(period) OR upper(period) > %(at)s)""", dict(c=chat_id, at=at))
        if not tracked:
            return
        want = None if (admin and can_invite and can_restrict) else ("rights_lost" if admin else "not_admin")
        # what no longer holds at `at` ends there: rights gaps of another reason, and every suspected one
        conn.execute("""UPDATE tg.gap SET period = tstzrange(lower(period), greatest(lower(period), %(at)s))
                        WHERE chat_id = %(c)s AND reason IN ('not_admin', 'rights_lost')
                          AND lower(period) <= %(at)s AND (upper_inf(period) OR upper(period) > %(at)s)
                          AND (reason IS DISTINCT FROM %(want)s OR NOT confirmed)""",
                     dict(c=chat_id, at=at, want=want))
        if want is None:
            return
        held = conn.execute("""SELECT 1 FROM tg.gap WHERE chat_id = %(c)s AND reason = %(want)s AND confirmed
                               AND lower(period) <= %(at)s AND (upper_inf(period) OR upper(period) > %(at)s)""",
                            dict(c=chat_id, at=at, want=want)).fetchone()
        if held is not None:
            return
        # a replayed event: the same gap observed later started here
        later = conn.execute("""SELECT gap_id FROM tg.gap WHERE chat_id = %s AND reason = %s AND confirmed
                                AND lower(period) > %s ORDER BY lower(period) LIMIT 1""",
                             (chat_id, want, at)).fetchone()
        if later is not None:
            conn.execute("UPDATE tg.gap SET period = tstzrange(%s, upper(period)) WHERE gap_id = %s", (at, later[0]))
            # G1 handled this gap from its old start: the reconciler starts it over (the caller holds the chat
            # lock, so no reconcile chunk of this chat runs in between)
            conn.execute("DELETE FROM tg.gap_progress WHERE gap_id = %s", (later[0],))
            return
        missing = [r for r, ok in (("can_invite_users", can_invite), ("can_restrict_members", can_restrict)) if not ok]
        note = f"{source}: bot is {status}" if want == "not_admin" else f"{source}: missing {', '.join(missing)}"
        conn.execute("""INSERT INTO tg.gap (chat_id, period, reason, confirmed, detected_at, note)
                        VALUES (%s, tstzrange(%s, NULL), %s, true, %s, %s)""", (chat_id, at, want, self.now(), note))

    def _join_request(self, fx: Effects, p: Parsed, chat_id: int, raw_chat: int, erased: set[int],
                      received: dt.datetime) -> None:
        assert p.sender
        self._upsert_user(p.sender, p.date)
        link_id = self._upsert_link(chat_id, p.link, p.date, erased) if p.link else None
        row = self.conn.execute(
            """INSERT INTO tg.join_request (chat_id, user_id, requested_at, update_id, invite_link_id)
               VALUES (%s, %s, %s, %s, %s) ON CONFLICT (chat_id, user_id, requested_at) DO NOTHING RETURNING 1""",
            (chat_id, p.sender.id, p.date, p.update_id, link_id)).fetchone()
        if row is not None:
            chat = str(chat_id)
            fx.add(lambda: metrics.CHAT_JOIN_REQUESTS.labels(chat, "received").inc())
        self._raw_update(p.update_id, p.date, p.type, raw_chat, p.sender.id, p.inner, received, erased)

    def _message(self, fx: Effects, p: Parsed, chat_id: int, erased: set[int], received: dt.datetime) -> None:
        msg = p.inner
        assert msg is not None and p.chat is not None
        if p.migration is not None:
            self._migrate(p.migration[0], p.migration[1], p.date, p.chat, received)
            return
        if p.chat.type not in ("group", "supergroup"):
            return
        if p.giveaway is not None:
            self._giveaway(chat_id, p)
        if SERVICE_KEYS.intersection(msg):
            return
        day = p.date.date()
        kind = _attribution(msg, p.chat)
        if kind is not None:
            self.conn.execute(
                """INSERT INTO tg.unattributed_daily AS a (chat_id, day, sender_kind, messages) VALUES (%s, %s, %s, 1)
                   ON CONFLICT (chat_id, day, sender_kind) DO UPDATE SET messages = a.messages + 1""",
                (chat_id, day, kind))
        elif p.sender is not None and p.sender.id not in erased:  # an erased writer: dated before the erasure
            self._upsert_user(p.sender, p.date)
            self.conn.execute(
                """INSERT INTO tg.activity_daily AS a (chat_id, user_id, day, messages) VALUES (%s, %s, %s, 1)
                   ON CONFLICT (chat_id, user_id, day) DO UPDATE SET messages = a.messages + 1""",
                (chat_id, p.sender.id, day))

    def _channel_post(self, fx: Effects, p: Parsed, chat_id: int) -> None:
        msg = p.inner
        assert msg is not None and p.chat is not None
        if p.chat.type != "channel":
            return
        if p.giveaway is not None:
            self._giveaway(chat_id, p)
        if SERVICE_KEYS.intersection(msg):
            return
        self.conn.execute("""INSERT INTO tg.post (chat_id, message_id, posted_at, kind) VALUES (%s, %s, %s, %s)
                             ON CONFLICT (chat_id, message_id) DO NOTHING""",
                          (chat_id, p.message_id, p.date, post_kind(msg)))

    def _giveaway(self, chat_id: int, p: Parsed) -> None:
        g = p.giveaway
        assert g is not None
        if g.draw_at <= p.date:
            log.warning("ingest: giveaway in chat %s draws before it starts; not recorded", chat_id)
            return
        row = self.conn.execute(
            """INSERT INTO tg.campaign (name, kind, origin, starts_at, draw_at, only_new_members, winner_count,
                                        post_chat_id, post_message_id)
               VALUES (%s, 'tg_giveaway', 'observation', %s, %s, %s, %s, %s, %s)
               ON CONFLICT (post_chat_id, post_message_id) DO NOTHING RETURNING campaign_id""",
            (f"Telegram giveaway {chat_id}/{p.message_id}", p.date, g.draw_at, g.only_new_members, g.winner_count,
             chat_id, p.message_id)).fetchone()
        if row is None:
            return
        for c in sorted(set(g.chats) or {chat_id}):
            self.conn.execute("INSERT INTO tg.campaign_chat (campaign_id, chat_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                              (row[0], c))

    # -- migrations (D2, M1/M2) --------------------------------------------------------------------------

    def _migrate(self, old: int, new: int, at: dt.datetime, chat: Chat, received: dt.datetime) -> None:
        """A migration service message (either one, either order, repeated): the alias once; M1 when the new
        chat is tracked, else M2 (the old history stays under the old id and is closed by the rebuild)."""
        conn = self.conn
        alias = conn.execute("SELECT chat_id, moved_at FROM tg.chat_alias WHERE old_chat_id = %s",
                             (old,)).fetchone()
        if alias is not None:
            if alias[0] != new:
                log.warning("ingest: chat %s migrated to %s, but the alias says %s; ignored", old, new, alias[0])
                return
            if alias[1] is not None:
                return  # M1 done
        title = chat.title or str(new)
        conn.execute("""INSERT INTO tg.chat (chat_id, chat_type, title, metadata_due_at)
                        VALUES (%s, 'supergroup', %s, %s) ON CONFLICT (chat_id) DO NOTHING""", (new, title, received))
        # whether the bot is admin in the destination decides M1: ask, unless its rights are known (also for a
        # row the collector made); a lost promotion update must not leave the history in M2
        conn.execute("""UPDATE tg.chat SET rights_due_at = coalesce(rights_due_at, %s)
                        WHERE chat_id = %s AND rights_checked_at IS NULL""", (received, new))
        if alias is None:
            conn.execute("INSERT INTO tg.chat_alias (old_chat_id, chat_id, migrated_at, moved_at) "
                         "VALUES (%s, %s, %s, NULL)", (old, new, at))
            # the old group ends at the migration: its coverage and its rights gaps end there too
            conn.execute("DELETE FROM tg.coverage WHERE chat_id = %s AND upper_inf(period) AND lower(period) >= %s",
                         (old, at))
            conn.execute("UPDATE tg.coverage SET period = tstzrange(lower(period), %s) "
                         "WHERE chat_id = %s AND upper_inf(period)", (at, old))
            conn.execute("""UPDATE tg.gap SET period = tstzrange(lower(period), greatest(lower(period), %s))
                            WHERE chat_id = %s AND upper_inf(period) AND reason IN ('not_admin', 'rights_lost')""",
                         (at, old))
            # and no rights check or getChat asks about it again
            conn.execute("UPDATE tg.chat SET rights_due_at = NULL, metadata_due_at = NULL WHERE chat_id = %s", (old,))
            log.info("ingest: chat %s migrated to %s", old, new)
        migrated_at = conn.execute("SELECT migrated_at FROM tg.chat_alias WHERE old_chat_id = %s",
                                   (old,)).fetchone()[0]
        tracked = conn.execute("SELECT tracked FROM tg.chat WHERE chat_id = %s", (new,)).fetchone()
        if tracked and tracked[0]:
            self._move(old, new, migrated_at)
        elif alias is None:
            # M2: the rebuild closes the old chat's open spells by migration (censoring, never an exit)
            history.rebuild_pairs(conn, [(old, u) for u in self._pair_users(old)])

    def _pair_users(self, chat_id: int) -> list[int]:
        rows = self.conn.execute(
            """SELECT user_id FROM tg.member_event WHERE chat_id = %(c)s
               UNION SELECT user_id FROM tg.sighting WHERE chat_id = %(c)s
               UNION SELECT user_id FROM tg.probe WHERE chat_id = %(c)s
               UNION SELECT user_id FROM tg.membership WHERE chat_id = %(c)s
               ORDER BY 1""", dict(c=chat_id)).fetchall()
        return [int(r[0]) for r in rows]

    def _move(self, old: int, new: int, migrated_at: dt.datetime) -> None:
        """M1: the old chat's history moves to the new id (once), then its pairs are rebuilt."""
        users = sorted(set(self._pair_users(old)) | set(self._pair_users(new)))
        moved = self.conn.execute("SELECT tg.move_chat_history(%s, %s, %s)", (old, new, migrated_at)).fetchone()[0]
        log.info("ingest: history of chat %s moved to %s (%s events)", old, new, moved)
        history.rebuild_pairs(self.conn, [(new, u) for u in users])

    # -- raw membership updates --------------------------------------------------------------------------

    def _raw_update(self, update_id: int, update_date: dt.datetime, kind: str, chat_id: int, user_id: int | None,
                    inner: dict | None, received: dt.datetime, erased: set[int], payload: dict | None = None) -> None:
        if kind not in MEMBERSHIP_TYPES or inner is None:
            return
        if payload is None:
            payload = self.redact(inner, erased)
        self._ensure_partitions("raw_update", "day", update_date)
        self.conn.execute(
            """INSERT INTO tg.raw_update (update_id, update_date, update_type, chat_id, user_id, received_at, payload)
               VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
            (update_id, update_date, kind, chat_id, user_id if user_id not in erased else None, received,
             Jsonb(payload)))

    def redact(self, inner: dict, erased: set[int] = frozenset()) -> dict:  # type: ignore[assignment]
        """The payload as stored in tg.raw_update: no bio or user_chat_id anywhere, a full invite link (one
        this bot made, or any link Telegram did not mask) replaced by its digest marker, erased people replaced,
        names and usernames of people dropped when store_names is off, a value of another shape at a sensitive
        position (a person, a member, invite_link, a name) dropped, a link object keeping only the ChatInviteLink
        fields, and NaN or Infinity stored as null."""
        bot_id = self._bot_id()
        digest = self.ctx.config.link_digest

        def walk(v: Any, key: str | None = None) -> Any:
            if isinstance(v, list):
                return [walk(x) for x in v]
            if isinstance(v, float) and not math.isfinite(v):
                return None  # NaN and Infinity: Python's JSON decoder accepts them, jsonb does not
            if not isinstance(v, dict):
                return v
            # a person: by its position in the payload (even when the object is incomplete) or by its shape
            person = key in USER_KEYS or _is_user_dict(v)
            uid = v.get("id")
            if person and isinstance(uid, int) and uid in erased:
                return {"erased": True}
            # the link position wins over a user-like shape: a link keeps only its typed fields
            holder = "link" if key == "invite_link" else "person" if person else "member" if key in MEMBER_KEYS \
                else None
            out: dict[str, Any] = {}
            for k, x in v.items():
                if k in ("bio", "user_chat_id"):
                    continue
                if not self.store_names and person and k in NAME_KEYS:
                    continue
                if not _expected_shape(holder, k, x):
                    # a sensitive position holding something else (an array, a scalar): dropped, not walked,
                    # so redaction never depends on the container shape
                    continue
                if k == "invite_link" and isinstance(x, str):
                    # a link's text, in a ChatInviteLink or (malformed) directly in the update: only a link
                    # Telegram masked for another creator stays as text
                    creator = v.get("creator")
                    by_bot = isinstance(creator, dict) and creator.get("id") == bot_id
                    out[k] = DIGEST_MARK + digest(x).hex() if by_bot or not is_masked(x) else x
                    continue
                out[k] = walk(x, k)
            return out

        return walk(copy.deepcopy(inner))

    # -- getChat metadata --------------------------------------------------------------------------------

    def refresh_metadata(self, limit: int = METADATA_BATCH) -> int:
        """Fetch getChat for up to `limit` due chats; returns how many succeeded."""
        if self.ctx.api is None:
            return 0
        now = self.now()
        due = self.conn.execute("""SELECT c.chat_id, c.metadata_attempts FROM tg.chat c
                                   WHERE c.metadata_due_at <= %s AND (c.tracked OR c.rights_checked_at IS NULL)
                                     AND NOT EXISTS (SELECT 1 FROM tg.chat_alias a WHERE a.old_chat_id = c.chat_id)
                                   ORDER BY c.metadata_due_at, c.chat_id LIMIT %s""",
                                (now, limit)).fetchall()
        self.conn.rollback()
        ok = 0
        for chat_id, attempts in due:
            if self.ctx.stop.is_set():
                break
            try:
                info = self.ctx.api.get_chat(chat_id)
                if not isinstance(info, dict):
                    raise ApiError("getChat", "other", "result is not an object")
            except ApiError as e:
                wait = METADATA_BACKOFF[min(attempts, len(METADATA_BACKOFF) - 1)]
                if e.outcome == "429" and e.retry_after:
                    wait = max(wait, e.retry_after)
                log.warning("ingest: getChat %s failed (%s); retry in %d s", chat_id, e.outcome, wait)
                due_at = self.now() + dt.timedelta(seconds=wait)
                self._transaction(lambda fx, c=chat_id, d=due_at: self.conn.execute(
                    "UPDATE tg.chat SET metadata_due_at = %s, metadata_attempts = metadata_attempts + 1 "
                    "WHERE chat_id = %s", (d, c)))
                continue
            info = _clean(info)
            kind = info.get("type") if info.get("type") in AUDIENCE_CHATS else None
            fields = dict(
                c=chat_id, kind=kind, title=_opt_str(info.get("title")), username=_opt_str(info.get("username")),
                linked=info.get("linked_chat_id") if isinstance(info.get("linked_chat_id"), int) else None,
                forum=_opt_bool(info.get("is_forum")), jbr=_opt_bool(info.get("join_by_request")),
                hidden=_opt_bool(info.get("has_hidden_members")))
            self._transaction(lambda fx, f=fields: self.conn.execute(
                """UPDATE tg.chat SET chat_type = coalesce(%(kind)s, chat_type), title = coalesce(%(title)s, title),
                          username = %(username)s, linked_chat_id = %(linked)s, is_forum = %(forum)s,
                          join_by_request = %(jbr)s, has_hidden_members = %(hidden)s,
                          metadata_due_at = NULL, metadata_attempts = 0, updated_at = now()
                   WHERE chat_id = %(c)s""", f))
            ok += 1
        return ok


def _attribution(msg: dict, chat: Chat | None) -> str | None:
    """The unattributed kind of a group message, or None when a user wrote it."""
    if msg.get("is_automatic_forward") is True:
        return "automatic_forward"
    sender_chat = msg.get("sender_chat")
    if isinstance(sender_chat, dict):
        return "anonymous_admin" if chat is not None and sender_chat.get("id") == chat.id else "sender_chat"
    return None


def _subject_id(kind: str, inner: dict) -> int | None:
    """The subject user id of a (possibly malformed) membership payload, when it can be read."""
    if kind in ("chat_join_request", "my_chat_member"):
        return _id_at(inner, "from", "id")
    return _id_at(inner, "new_chat_member", "user", "id")


USER_POSITIONS = (("from",), ("new_chat_member", "user"), ("old_chat_member", "user"), ("invite_link", "creator"))


def _payload_user_ids(inner: dict) -> list[int]:
    """Every person's id at the user positions of a membership payload, read even when it is malformed."""
    ids = [_id_at(inner, *path, "id") for path in USER_POSITIONS]
    return [i for i in ids if i is not None]

