"""Fetch the member lists of admin chats from Telegram and store them as snapshots."""

from __future__ import annotations

import asyncio
import sqlite3
import string
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable

from telethon import errors, utils
from telethon.tl import types
from telethon.tl.functions.channels import GetFullChannelRequest, GetParticipantsRequest
from telethon.tl.functions.messages import GetFullChatRequest

from . import db
from .analysis import resolve_chat
from .db import ChatRow, UserRow

PAGE_SIZE = 200
MAX_FLOOD_RETRIES = 20
# --deep searches by single characters: Latin letters, digits, Cyrillic letters
DEEP_QUERIES = list(string.ascii_lowercase + string.digits + "абвгдеёжзийклмнопрстуфхцчшщъыьэюя")

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


def chat_type(entity: Any) -> str | None:
    if isinstance(entity, types.Chat):
        return None if getattr(entity, "deactivated", False) else "group"
    if isinstance(entity, types.Channel):
        return "supergroup" if entity.megagroup else "channel"
    return None


def _is_admin(entity: Any) -> bool:
    return bool(getattr(entity, "creator", False) or getattr(entity, "admin_rights", None))


async def list_admin_chats(client: Any) -> list[AdminChat]:
    chats = []
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        kind = chat_type(entity)
        if kind is None or not _is_admin(entity):
            continue
        chats.append(
            AdminChat(utils.get_peer_id(entity), kind, entity.title, getattr(entity, "username", None), entity)
        )
    return chats


async def call(
    client: Any, request: Any, sleep: Sleep = asyncio.sleep, out: Out = print
) -> Any:
    """Send a request, sleeping through FloodWaitError and retrying."""
    for attempt in range(MAX_FLOOD_RETRIES + 1):
        try:
            return await client(request)
        except errors.FloodWaitError as exc:
            if attempt == MAX_FLOOD_RETRIES:
                raise
            out(f"  Telegram asks to wait {exc.seconds}s (flood wait); sleeping, then continuing")
            await sleep(exc.seconds + 1)
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
    client: Any, entity: Any, query: str, sleep: Sleep, out: Out
) -> dict[int, UserRow]:
    """Page through one channel participant listing until Telegram returns no more."""
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
        members.update(_rows_from(participants, result.users))
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


async def fetch_members(
    client: Any,
    chat: AdminChat,
    reported: int | None,
    deep: bool = False,
    sleep: Sleep = asyncio.sleep,
    out: Out = print,
) -> dict[int, UserRow]:
    if not isinstance(chat.entity, types.Channel):
        return await _basic_group_members(client, chat.entity, sleep, out)

    members = await _search_members(client, chat.entity, "", sleep, out)
    if deep and db.is_incomplete(len(members), reported):
        out(f"  listing is short ({len(members)} of {reported}); searching by single characters")
        for query in DEEP_QUERIES:
            if not db.is_incomplete(len(members), reported):
                break
            try:
                found = await _search_members(client, chat.entity, query, sleep, out)
            except Exception as exc:  # keep what the listing already gave us
                out(f"  deep search stopped at {query!r}: {type(exc).__name__}: {exc}")
                break
            before = len(members)
            members.update(found)
            out(f"  deep {query!r}: +{len(members) - before} new, {len(members)} total")
    return members


async def collect_chat(
    client: Any,
    conn: sqlite3.Connection,
    chat: AdminChat,
    deep: bool = False,
    sleep: Sleep = asyncio.sleep,
    out: Out = print,
) -> CollectResult:
    """Collect one chat and store a snapshot. Never raises for a per-chat failure."""
    result = CollectResult(chat)
    try:
        reported = await reported_count(client, chat, sleep, out)
        members = await fetch_members(client, chat, reported, deep, sleep, out)
        snapshot = db.add_snapshot(conn, chat.row, members.values(), reported)
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
) -> list[CollectResult]:
    """Collect every admin chat, or only the ones named by refs, one chat at a time."""
    admin_chats = await list_admin_chats(client)
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
        results.append(await collect_chat(client, conn, chat, deep, sleep, out))
    return results
