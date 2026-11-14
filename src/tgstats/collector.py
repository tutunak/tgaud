"""Fetch the member lists of admin chats from Telegram and store them as snapshots."""

from __future__ import annotations

import asyncio
import random
import sqlite3
import string
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable

from telethon import TelegramClient, errors, utils
from telethon.tl import types
from telethon.tl.functions.channels import GetFullChannelRequest, GetParticipantsRequest
from telethon.tl.functions.messages import GetFullChatRequest

from . import db
from .analysis import resolve_chat
from .db import ChatRow, UserRow

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
    user_id = getattr(participant, "user_id", None)
    if user_id is None:
        peer = getattr(participant, "peer", None)
        user_id = getattr(peer, "user_id", None)
    return user_id


def _rows_from(participants: Iterable[Any], users: Iterable[Any]) -> dict[int, UserRow]:
    by_id = {u.id: u for u in users if hasattr(u, "id")}
    rows: dict[int, UserRow] = {}
    for p in participants:
        uid = _participant_user_id(p)
        if uid is not None and uid in by_id:
            rows[uid] = _user_row(by_id[uid])
    return rows


async def reported_count(
    client: Any, chat: AdminChat, sleep: Sleep = asyncio.sleep, out: Out = print
) -> int | None:
    """The member count Telegram reports for the chat."""
    entity = chat.entity
    if isinstance(entity, types.Channel):
        full = await call(client, GetFullChannelRequest(entity), sleep, out)
        return getattr(full.full_chat, "participants_count", None)
    return getattr(entity, "participants_count", None)


async def _search_members(
    client: Any, entity: Any, query: str, sleep: Sleep, out: Out, into: dict[int, UserRow]
) -> dict[int, UserRow]:
    """Page through one channel participant listing until Telegram returns no more.

    Every page is merged into `into` as it arrives, so an interrupt keeps what was found; the
    return value is what this one query alone returned.
    """
    members: dict[int, UserRow] = {}
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
        participants = getattr(result, "participants", None)
        if not participants:
            break
        rows = _rows_from(participants, result.users)
        members.update(rows)
        into.update(rows)
        offset += len(participants)
    return members


async def _basic_group_members(
    client: Any, entity: Any, sleep: Sleep, out: Out
) -> dict[int, UserRow]:
    full = await call(client, GetFullChatRequest(entity.id), sleep, out)
    participants = getattr(full.full_chat.participants, "participants", None)
    if participants is None:
        raise RuntimeError("Telegram did not return the member list of this group (no rights?)")
    return _rows_from(participants, full.users)


def pair_queries(first: str) -> list[str]:
    """The two-character queries for one first character: it paired with each character of its group."""
    for group in DEEP_GROUPS:
        if first in group:
            return [first + second for second in group]
    raise ValueError(f"{first!r} is in no deep-search group")


async def _deep_search(
    client: Any,
    entity: Any,
    members: dict[int, UserRow],
    reported: int | None,
    expand_at: int,
    sleep: Sleep,
    out: Out,
) -> None:
    """Search by single characters, then, while still short, by character pairs; merge into `members`."""

    def short() -> bool:
        return db.is_incomplete(len(members), reported)

    out(f"  listing is short ({len(members)} of {reported}); searching by single characters")
    returned: dict[str, int] = {}
    for query in DEEP_QUERIES:
        if not short():
            return
        before = len(members)
        try:
            found = await _search_members(client, entity, query, sleep, out, members)
        except Exception as exc:  # keep what we already have
            out(f"  deep search stopped at {query!r}: {type(exc).__name__}: {exc}")
            return
        returned[query] = len(found)
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
                await _search_members(client, entity, query, sleep, out, members)
            except Exception as exc:  # keep what we already have
                out(f"  deep search stopped at {query!r}: {type(exc).__name__}: {exc}")
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
    into: dict[int, UserRow] | None = None,
) -> dict[int, UserRow]:
    """The members of a chat. With `into`, members are added to that dict as they are found."""
    members = {} if into is None else into
    if not isinstance(chat.entity, types.Channel):
        members.update(await _basic_group_members(client, chat.entity, sleep, out))
        return members

    await _search_members(client, chat.entity, "", sleep, out, members)
    if deep and db.is_incomplete(len(members), reported):
        await _deep_search(client, chat.entity, members, reported, expand_at, sleep, out)
    return members


def _keep_partial(
    conn: sqlite3.Connection, chat: AdminChat, members: dict[int, UserRow], reported: int | None, out: Out
) -> None:
    """After an interrupt: store the members found so far as an incomplete snapshot, if it beats the latest."""
    latest = db.latest_snapshot(conn, chat.id)
    found = len(members)
    if latest is not None and found <= latest.fetched:
        why = f"{found} members found, not more than the {latest.fetched} in the latest snapshot"
    elif found == 0:
        why = "no members found yet"
    else:
        snapshot = db.add_snapshot(conn, chat.row, members.values(), reported, complete=False)
        shown = "?" if reported is None else reported
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
) -> CollectResult:
    """Collect one chat and store a snapshot. Never raises for a per-chat failure.

    An interrupt (Ctrl-C cancels the task) keeps the members found so far, then propagates.
    """
    result = CollectResult(chat)
    members: dict[int, UserRow] = {}
    reported: int | None = None
    try:
        reported = await reported_count(client, chat, sleep, out)
        await fetch_members(client, chat, reported, deep, sleep, out, expand_at, members)
        snapshot = db.add_snapshot(conn, chat.row, members.values(), reported)
    except asyncio.CancelledError:
        try:
            _keep_partial(conn, chat, members, reported, out)
        except Exception as exc:  # the interrupt still wins
            out(f"{chat.title}: interrupted; could not store the partial result: {type(exc).__name__}: {exc}")
        raise
    except Exception as exc:
        result.error = f"{type(exc).__name__}: {exc}"
        out(f"{chat.title}: FAILED, {result.error}")
        return result

    result.fetched, result.reported, result.complete = snapshot.fetched, reported, snapshot.complete
    shown = "?" if reported is None else reported
    status = "ok" if snapshot.complete else "INCOMPLETE (Telegram returned fewer members than reported)"
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
        results.append(await collect_chat(client, conn, chat, deep, sleep, out, expand_at))
    return results
