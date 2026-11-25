"""A fake Telethon client: answers the few requests the collector sends, offline."""

import asyncio
import signal
from datetime import datetime, timezone
from types import SimpleNamespace

from telethon import errors
from telethon.tl import types
from telethon.tl.functions.channels import GetFullChannelRequest, GetParticipantsRequest
from telethon.tl.functions.messages import GetFullChatRequest

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def tg_user(uid, bot=False, deleted=False, first=None, premium=None, last=None):
    return types.User(
        id=uid, first_name=first or f"Name{uid}", last_name=last, username=f"user{uid}", bot=bot, deleted=deleted,
        premium=premium,
    )


def channel(cid, title, megagroup=False, creator=True, admin=False, username=None):
    return types.Channel(
        id=cid, title=title, photo=types.ChatPhotoEmpty(), date=NOW, megagroup=megagroup,
        broadcast=not megagroup, creator=creator or None, username=username,
        admin_rights=types.ChatAdminRights() if admin else None,
    )


def basic_group(cid, title, count, creator=True):
    return types.Chat(
        id=cid, title=title, photo=types.ChatPhotoEmpty(), participants_count=count,
        date=NOW, version=1, creator=creator or None,
    )


class FakeClient:
    """members: {entity.id: [User]}; per-chat options live in `opts`.

    opts[id]: page (max participants per response), cap (max returned by the
    empty-query listing), search_cap (max returned by a non-empty query), reported
    (what GetFullChannel, or GetFullChat's chat, says; a list gives one value per count
    read, and an exception in it is raised by that read), error (exception to raise on
    GetFullChannel/GetParticipants), participant (user -> the participant object a
    listing returns for that user; default ChannelParticipant joined at NOW).

    Interrupts (Ctrl-C) happen instead of answering a request: on GetFullChannel
    (interrupt_on_full), on given 1-based GetParticipants call numbers
    (interrupt_on_calls) or on given search queries (interrupt_on_queries). The
    interrupt cancels the running task, or with sigint=True sends a real SIGINT,
    which under asyncio.run cancels the task and then raises KeyboardInterrupt.
    """

    def __init__(
        self, dialogs, members, opts=None, flood_on=(), dialog_floods=0,
        interrupt_on_full=False, interrupt_on_calls=(), interrupt_on_queries=(), sigint=False, fail_on_queries=None,
        fail_on_pages=None,
    ):
        self.dialogs = dialogs
        self.members = members
        self.opts = opts or {}
        self.flood_on = set(flood_on)  # 1-based GetParticipants call numbers that hit a flood wait
        self.dialog_floods = dialog_floods  # GetDialogs flood waits, each hit after the first dialog
        self.interrupt_on_full = interrupt_on_full
        self.interrupt_on_calls = set(interrupt_on_calls)
        self.interrupt_on_queries = set(interrupt_on_queries)
        self.sigint = sigint
        self.fail_on_queries = fail_on_queries or {}  # search query -> exception to raise
        self.fail_on_pages = fail_on_pages or {}  # (search query, offset) -> exception to raise
        self.participant_calls = 0
        self.requests = []

    async def _interrupt(self):
        if self.sigint:
            signal.raise_signal(signal.SIGINT)
        else:
            asyncio.current_task().cancel()
        await asyncio.sleep(3600)  # the cancellation arrives here

    def iter_dialogs(self):
        async def gen():
            for i, entity in enumerate(self.dialogs):
                if i == 1 and self.dialog_floods:
                    self.dialog_floods -= 1
                    raise errors.FloodWaitError(None, capture=12)
                yield SimpleNamespace(entity=entity)

        return gen()

    def _reported(self, opt, default):
        value = opt.get("reported", default)
        if isinstance(value, list):
            value = value.pop(0) if len(value) > 1 else value[0]
        if isinstance(value, BaseException):
            raise value
        return value

    async def __call__(self, request):
        self.requests.append(request)
        if isinstance(request, GetFullChatRequest):
            users = self.members[request.chat_id]
            opt = self.opts.get(request.chat_id, {})
            make = opt.get("participant", lambda u: types.ChatParticipant(user_id=u.id, inviter_id=0, date=NOW))
            parts = [make(u) for u in users]
            chat = types.Chat(id=request.chat_id, title="g", photo=types.ChatPhotoEmpty(),
                              participants_count=self._reported(opt, len(users)), date=NOW, version=1)
            return SimpleNamespace(
                full_chat=SimpleNamespace(participants=SimpleNamespace(participants=parts)), users=users,
                chats=[chat],
            )
        cid = request.channel.id
        opt = self.opts.get(cid, {})
        if opt.get("error"):
            raise opt["error"]
        users = self.members[cid]
        if isinstance(request, GetFullChannelRequest):
            if self.interrupt_on_full:
                await self._interrupt()
            return SimpleNamespace(
                full_chat=SimpleNamespace(participants_count=self._reported(opt, len(users)))
            )
        assert isinstance(request, GetParticipantsRequest)
        self.participant_calls += 1
        if self.participant_calls in self.flood_on:
            raise errors.FloodWaitError(request, capture=30)
        query = request.filter.q.lower()
        if query in self.fail_on_queries:
            raise self.fail_on_queries[query]
        if (query, request.offset) in self.fail_on_pages:
            raise self.fail_on_pages[(query, request.offset)]
        if self.participant_calls in self.interrupt_on_calls or query in self.interrupt_on_queries:
            await self._interrupt()
        if query:
            hits = [u for u in users if u.first_name.lower().startswith(query)]
            hits = hits[: opt.get("search_cap", len(hits))]
        else:
            hits = users[: opt.get("cap", len(users))]
        page = hits[request.offset : request.offset + min(request.limit, opt.get("page", 200))]
        make = opt.get("participant", lambda u: types.ChannelParticipant(user_id=u.id, date=NOW))
        return SimpleNamespace(
            participants=[make(u) for u in page],
            users=page,
            count=len(hits),
        )
