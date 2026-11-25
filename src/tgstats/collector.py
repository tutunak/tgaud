"""Fetch the member lists of admin chats from Telegram and store them as snapshots."""

from __future__ import annotations

import asyncio
import random
import sqlite3
import string
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable
from uuid import uuid4

from telethon import TelegramClient, errors, utils
from telethon.tl import types
from telethon.tl.functions.channels import GetFullChannelRequest, GetParticipantsRequest
from telethon.tl.functions.messages import GetFullChatRequest

from . import db
from .analysis import resolve_chat
from .db import ChatRow, Member, UserRow

PAGE_SIZE = 200
MAX_FLOOD_RETRIES = 20
# random pause before every request, in whole milliseconds, so requests don't come at a fixed rate
MIN_DELAY_MS = 1000
MAX_DELAY_MS = 5000
# --deep searches by single characters, then by pairs of characters. Two groups, never mixed in a pair:
# L = Latin letters and digits, C = Cyrillic letters.
LATIN_CHARS = string.ascii_lowercase + string.digits
CYRILLIC_CHARS = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
DEEP_GROUPS = (LATIN_CHARS, CYRILLIC_CHARS)
DEEP_QUERIES = list(LATIN_CHARS + CYRILLIC_CHARS)
# a single character is expanded into two-character queries when its search returned at least this many members
DEFAULT_EXPAND_AT = 100

Sleep = Callable[[float], Awaitable[None]]
Out = Callable[[str], None]
Clock = Callable[[], datetime]


def wall_clock() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class AdminChat:
    """A chat where the account is creator or admin, with its Telethon entity."""

    id: int
    type: str  # group | supergroup | channel
    title: str
    username: str | None
    entity: Any

    @property
    def row(self) -> ChatRow:
        return ChatRow(self.id, self.type, self.title, self.username)


@dataclass
class CollectResult:
    chat: AdminChat
    fetched: int = 0
    reported: int | None = None
    complete: bool = True
    error: str | None = None


@dataclass
class Listing:
    """One collection of one chat as it goes (tgaud's snapshot contract, docs/design/design.md 5): its random id,
    when it started, the count read before it, the first observation of every member, the path it took, and the
    first error it went on after (a kept listing that saw an error is incomplete whatever the counts say).

    Times come from `clock` but never go back: a wall-clock step between two pages must not date a page before
    the start or before an earlier page."""

    clock: Clock = wall_clock
    uuid: str = field(default_factory=lambda: str(uuid4()))
    members: dict[int, Member] = field(default_factory=dict)
    method: str = "listing"
    error: str | None = None
    reported_start: int | None = None
    started_at: str = ""
    _last: datetime | None = None

    def now(self) -> str:
        t = self.clock().astimezone(timezone.utc)
        if self._last is not None and t < self._last:
            t = self._last
        self._last = t
        return t.isoformat(timespec="microseconds")

    def start(self) -> None:
        self.started_at = self.now()

    def add(self, rows: dict[int, Member]) -> None:
        """Merge one page: a member already listed keeps the first page's observation."""
        for uid, row in rows.items():
            self.members.setdefault(uid, row)

    def failed(self, what: str) -> None:
        if self.error is None:
            self.error = what

    def finish(self, reported_end: int | None) -> db.Collection:
        return db.Collection(self.uuid, self.started_at, self.now(), self.method, self.reported_start, reported_end)


class Pacer:
    """Waits a random MIN..MAX whole milliseconds; run before every Telegram request."""

    def __init__(
        self,
        min_ms: int = MIN_DELAY_MS,
        max_ms: int = MAX_DELAY_MS,
        sleep: Sleep = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        if not 0 <= min_ms <= max_ms:
            raise ValueError(f"need 0 <= min delay <= max delay, got {min_ms}..{max_ms} ms")
        self.min_ms, self.max_ms = min_ms, max_ms
        self._sleep = sleep
        self._rng = rng or random.Random()

    async def wait(self) -> None:
        await self._sleep(self._rng.randint(self.min_ms, self.max_ms) / 1000)


class PacedTelegramClient(TelegramClient):
    """A TelegramClient that runs its pacer before every request it sends.

    It hooks `_call`, where every request ends up: explicit `client(request)` calls
    and the ones Telethon sends itself (iter_dialogs, the authorization check).
    Not paced: the connection handshake, and Telethon's own retries inside `_call`
    after a server error (it waits a fixed 2 s between those).
    """

    pacer: Pacer | None = None

    async def _call(self, sender: Any, request: Any, ordered: bool = False, flood_sleep_threshold: Any = None) -> Any:
        if self.pacer is not None:
            await self.pacer.wait()
        return await super()._call(sender, request, ordered=ordered, flood_sleep_threshold=flood_sleep_threshold)


def chat_type(entity: Any) -> str | None:
    if isinstance(entity, types.Chat):
        return None if getattr(entity, "deactivated", False) else "group"
    if isinstance(entity, types.Channel):
        return "supergroup" if entity.megagroup else "channel"
    return None


def _is_admin(entity: Any) -> bool:
    return bool(getattr(entity, "creator", False) or getattr(entity, "admin_rights", None))


async def _flood_wait(exc: errors.FloodWaitError, attempt: int, sleep: Sleep, out: Out) -> None:
    if attempt == MAX_FLOOD_RETRIES:
        raise exc
    out(f"  Telegram asks to wait {exc.seconds}s (flood wait); sleeping, then continuing")
    await sleep(exc.seconds + 1)


async def list_admin_chats(
    client: Any, sleep: Sleep = asyncio.sleep, out: Out = print
) -> list[AdminChat]:
    """Admin chats from the dialog list; a flood wait restarts the listing, without duplicates."""
    found: dict[int, AdminChat] = {}
    for attempt in range(MAX_FLOOD_RETRIES + 1):
        try:
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                kind = chat_type(entity)
                if kind is None or not _is_admin(entity):
                    continue
                chat_id = utils.get_peer_id(entity)
                found.setdefault(
                    chat_id,
                    AdminChat(chat_id, kind, entity.title, getattr(entity, "username", None), entity),
                )
            break
        except errors.FloodWaitError as exc:
            await _flood_wait(exc, attempt, sleep, out)
    return list(found.values())


async def call(
    client: Any, request: Any, sleep: Sleep = asyncio.sleep, out: Out = print
) -> Any:
    """Send a request, sleeping through FloodWaitError and retrying."""
    for attempt in range(MAX_FLOOD_RETRIES + 1):
        try:
            return await client(request)
        except errors.FloodWaitError as exc:
            await _flood_wait(exc, attempt, sleep, out)
    raise AssertionError("unreachable")


def _user_row(user: Any) -> UserRow:
    return UserRow(
        id=user.id,
        username=getattr(user, "username", None),
        first_name=getattr(user, "first_name", None),
        last_name=getattr(user, "last_name", None),
        is_bot=bool(getattr(user, "bot", False)),
        is_deleted=bool(getattr(user, "deleted", False)),
    )


def _participant_user_id(participant: Any) -> int | None:
    if isinstance(participant, types.ChannelParticipantLeft):
        return None
    if isinstance(participant, types.ChannelParticipantBanned) and participant.left:
        return None  # restricted or banned and no longer in the chat: not a member
    user_id = getattr(participant, "user_id", None)
    if user_id is None:
        peer = getattr(participant, "peer", None)
        user_id = getattr(peer, "user_id", None)
    return user_id


def _when(value: Any) -> str | None:
    if not isinstance(value, datetime):
        return None
    return (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).astimezone(timezone.utc).isoformat()


def _member(participant: Any, user: Any, seen_at: str) -> Member:
    """The observation of one participant on one page. Telethon 1.45: `date` (the join date) on ChannelParticipant,
    -Self, -Admin, -Banned and Chat-/ChatParticipantAdmin, none on the creators (joined_date NULL, design.md 5);
    `inviter_id` on -Self, -Admin and the Chat ones (0 = none); `via_request` on -Self only;
    `subscription_until_date` on ChannelParticipant and -Self."""
    inviter = getattr(participant, "inviter_id", None)
    via = getattr(participant, "via_request", None)
    return Member(
        user=_user_row(user),
        seen_at=seen_at,
        joined_date=_when(getattr(participant, "date", None)),
        inviter_id=inviter if isinstance(inviter, int) and inviter > 0 else None,
        via_request=bool(via) if isinstance(participant, types.ChannelParticipantSelf) else None,
        subscription_until=_when(getattr(participant, "subscription_until_date", None)),
        is_premium=bool(getattr(user, "premium", False)),
    )


def _rows_from(participants: Iterable[Any], users: Iterable[Any], seen_at: str) -> dict[int, Member]:
    """The members one page lists, observed at `seen_at` (a user listed twice on the page: the first entry)."""
    by_id = {u.id: u for u in users if hasattr(u, "id")}
    rows: dict[int, Member] = {}
    for p in participants:
        uid = _participant_user_id(p)
        if uid is not None and uid in by_id and uid not in rows:
            rows[uid] = _member(p, by_id[uid], seen_at)
    return rows


async def reported_count(
    client: Any, chat: AdminChat, sleep: Sleep = asyncio.sleep, out: Out = print
) -> int | None:
    """The member count Telegram reports for the chat now: asked for every time, never the dialog list's copy."""
    entity = chat.entity
    if isinstance(entity, types.Channel):
        full = await call(client, GetFullChannelRequest(entity), sleep, out)
        return getattr(full.full_chat, "participants_count", None)
    full = await call(client, GetFullChatRequest(entity.id), sleep, out)
    for c in getattr(full, "chats", None) or []:
        if getattr(c, "id", None) == entity.id:
            return getattr(c, "participants_count", None)
    return None


async def _search_members(
    client: Any, entity: Any, query: str, sleep: Sleep, out: Out, listing: Listing
) -> int:
    """Page through one channel participant listing until Telegram returns no more.

    Every page is merged into the listing as it arrives (dated when it arrived), so an interrupt keeps what was
    found; returns how many distinct members this one query alone returned.
    """
    found: set[int] = set()
    offset = 0
    while True:
        result = await call(
            client,
            GetParticipantsRequest(
                entity, types.ChannelParticipantsSearch(query), offset, PAGE_SIZE, hash=0
            ),
            sleep,
            out,
        )
        seen_at = listing.now()
        participants = getattr(result, "participants", None)
        if not participants:
            break
        rows = _rows_from(participants, result.users, seen_at)
        found.update(rows)
        listing.add(rows)
        offset += len(participants)
    return len(found)


async def _basic_group_members(
    client: Any, entity: Any, sleep: Sleep, out: Out, listing: Listing
) -> None:
    listing.method = "basic_group"
    full = await call(client, GetFullChatRequest(entity.id), sleep, out)
    seen_at = listing.now()
    participants = getattr(full.full_chat.participants, "participants", None)
    if participants is None:
        raise RuntimeError("Telegram did not return the member list of this group (no rights?)")
    listing.add(_rows_from(participants, full.users, seen_at))


def pair_queries(first: str) -> list[str]:
    """The two-character queries for one first character: it paired with each character of its group."""
    for group in DEEP_GROUPS:
        if first in group:
            return [first + second for second in group]
    raise ValueError(f"{first!r} is in no deep-search group")


async def _deep_search(
    client: Any,
    entity: Any,
    listing: Listing,
    reported: int | None,
    expand_at: int,
    sleep: Sleep,
    out: Out,
) -> None:
    """Search by single characters, then, while still short, by character pairs; merge into the listing. A failed
    query stops the search and keeps what was found, and the listing is incomplete from then on."""
    members = listing.members
    listing.method = "deep"

    def short() -> bool:
        return db.is_incomplete(len(members), reported)

    def stopped(query: str, exc: Exception) -> None:
        out(f"  deep search stopped at {query!r}: {type(exc).__name__}: {exc}")
        listing.failed(f"the deep search query {query!r} failed: {type(exc).__name__}: {exc}")

    out(f"  listing is short ({len(members)} of {reported}); searching by single characters")
    returned: dict[str, int] = {}
    for query in DEEP_QUERIES:
        if not short():
            return
        before = len(members)
        try:
            found = await _search_members(client, entity, query, sleep, out, listing)
        except Exception as exc:  # keep what we already have
            stopped(query, exc)
            return
        returned[query] = found
        out(f"  deep {query!r}: +{len(members) - before} new, {len(members)} total")
    if not short():
        return

    # Anyone matched by "cx" is also matched by "c", so a single-character search that returned fewer
    # members than any search cap cannot be hiding anyone its two-character queries would find.
    # Only characters whose search returned at least `expand_at` members are expanded.
    expanded = [c for c in DEEP_QUERIES if returned[c] >= expand_at]
    total = sum(len(pair_queries(c)) for c in expanded)
    out(
        f"  still short ({len(members)} of {reported}); searching by two characters: {total} queries "
        f"for {len(expanded)} first characters, {len(DEEP_QUERIES) - len(expanded)} skipped "
        f"(their single search returned fewer than {expand_at})"
    )
    for first in expanded:
        before, ran = len(members), 0
        for query in pair_queries(first):
            if not short():
                break
            try:
                await _search_members(client, entity, query, sleep, out, listing)
            except Exception as exc:  # keep what we already have
                stopped(query, exc)
                return
            ran += 1
        out(f"  deep '{first}?': {ran} queries, +{len(members) - before} new, {len(members)} total")
        if not short():
            return


async def fetch_members(
    client: Any,
    chat: AdminChat,
    reported: int | None,
    deep: bool = False,
    sleep: Sleep = asyncio.sleep,
    out: Out = print,
    expand_at: int = DEFAULT_EXPAND_AT,
    listing: Listing | None = None,
) -> Listing:
    """The members of a chat, added to `listing` (a new one by default) as they are found."""
    listing = Listing() if listing is None else listing
    if not isinstance(chat.entity, types.Channel):
        await _basic_group_members(client, chat.entity, sleep, out, listing)
        return listing

    await _search_members(client, chat.entity, "", sleep, out, listing)
    if deep and db.is_incomplete(len(listing.members), reported):
        await _deep_search(client, chat.entity, listing, reported, expand_at, sleep, out)
    return listing


def _larger(*counts: int | None) -> int | None:
    """The count to compare fetched with: the larger of the two reads (as tgaud grades), None without any."""
    return max((c for c in counts if c is not None), default=None)


def _keep_partial(conn: sqlite3.Connection, chat: AdminChat, listing: Listing, out: Out) -> None:
    """After an interrupt: store the members found so far as an incomplete snapshot, if it beats the latest."""
    latest = db.latest_snapshot(conn, chat.id)
    found = len(listing.members)
    if latest is not None and found <= latest.fetched:
        why = f"{found} members found, not more than the {latest.fetched} in the latest snapshot"
    elif found == 0:
        why = "no members found yet"
    else:
        snapshot = db.add_snapshot(conn, chat.row, listing.members.values(), listing.reported_start,
                                   complete=False, collection=listing.finish(None))
        shown = "?" if listing.reported_start is None else listing.reported_start
        out(f"{chat.title}: interrupted; saved a partial snapshot, fetched {snapshot.fetched} of {shown} reported, INCOMPLETE")
        return
    out(f"{chat.title}: interrupted; partial result discarded: {why}")


async def collect_chat(
    client: Any,
    conn: sqlite3.Connection,
    chat: AdminChat,
    deep: bool = False,
    sleep: Sleep = asyncio.sleep,
    out: Out = print,
    expand_at: int = DEFAULT_EXPAND_AT,
    clock: Clock = wall_clock,
) -> CollectResult:
    """Collect one chat and store a snapshot. Never raises for a per-chat failure.

    The snapshot records what tgaud's snapshot contract needs: a random id, started_at before the first count
    read, Telegram's count before and after the listing, the path taken, finished_at, and each member as the first
    page that listed them saw them. A failed final count keeps the listing, with reported_end NULL and
    complete false. An interrupt (Ctrl-C cancels the task) keeps the members found so far, then propagates.
    """
    result = CollectResult(chat)
    listing = Listing(clock)
    listing.start()
    try:
        listing.reported_start = await reported_count(client, chat, sleep, out)
        await fetch_members(client, chat, listing.reported_start, deep, sleep, out, expand_at, listing)
        try:
            reported_end = await reported_count(client, chat, sleep, out)
        except Exception as exc:  # the listing stands; without the second count it cannot be complete
            reported_end = None
            out(f"  the member count after the listing failed: {type(exc).__name__}: {exc}")
            listing.failed(f"the final member count failed: {type(exc).__name__}: {exc}")
        reported = _larger(listing.reported_start, reported_end)
        snapshot = db.add_snapshot(conn, chat.row, listing.members.values(), reported,
                                   complete=False if listing.error else None,
                                   collection=listing.finish(reported_end))
    except asyncio.CancelledError:
        try:
            _keep_partial(conn, chat, listing, out)
        except Exception as exc:  # the interrupt still wins
            out(f"{chat.title}: interrupted; could not store the partial result: {type(exc).__name__}: {exc}")
        raise
    except Exception as exc:
        result.error = f"{type(exc).__name__}: {exc}"
        out(f"{chat.title}: FAILED, {result.error}")
        return result

    result.fetched, result.reported, result.complete = snapshot.fetched, reported, snapshot.complete
    shown = "?" if reported is None else reported
    if snapshot.complete:
        status = "ok"
    elif listing.error:
        status = "INCOMPLETE (a request failed during the collection, see above)"
    else:
        status = "INCOMPLETE (Telegram returned fewer members than reported)"
    out(f"{chat.title}: fetched {snapshot.fetched} of {shown} reported, {status}")
    return result


async def collect(
    client: Any,
    conn: sqlite3.Connection,
    refs: list[str] | None = None,
    deep: bool = False,
    sleep: Sleep = asyncio.sleep,
    out: Out = print,
    expand_at: int = DEFAULT_EXPAND_AT,
    clock: Clock = wall_clock,
) -> list[CollectResult]:
    """Collect every admin chat, or only the ones named by refs, one chat at a time."""
    admin_chats = await list_admin_chats(client, sleep, out)
    if refs:
        chosen: list[AdminChat] = []
        for ref in refs:
            chat = resolve_chat(ref, admin_chats)
            if chat not in chosen:
                chosen.append(chat)  # type: ignore[arg-type]
    else:
        chosen = admin_chats
    if not chosen:
        out("no chats where this account is creator or admin")
    results = []
    for chat in chosen:
        out(f"collecting {chat.title} ({chat.type}, id {chat.id})")
        results.append(await collect_chat(client, conn, chat, deep, sleep, out, expand_at, clock))
    return results
