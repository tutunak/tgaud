"""A fake Telethon client: answers the few requests the collector sends, offline."""

from datetime import datetime, timezone
from types import SimpleNamespace

from telethon import errors
from telethon.tl import types
from telethon.tl.functions.channels import GetFullChannelRequest, GetParticipantsRequest
from telethon.tl.functions.messages import GetFullChatRequest

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def tg_user(uid, bot=False, deleted=False, first=None):
    return types.User(
        id=uid, first_name=first or f"Name{uid}", username=f"user{uid}", bot=bot, deleted=deleted
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
    empty-query listing), reported (what GetFullChannel says), error (exception
    to raise on GetFullChannel/GetParticipants).
    """

    def __init__(self, dialogs, members, opts=None, flood_on=(), dialog_floods=0):
        self.dialogs = dialogs
        self.members = members
        self.opts = opts or {}
        self.flood_on = set(flood_on)  # 1-based GetParticipants call numbers that hit a flood wait
        self.dialog_floods = dialog_floods  # GetDialogs flood waits, each hit after the first dialog
        self.participant_calls = 0
        self.requests = []

    def iter_dialogs(self):
        async def gen():
            for i, entity in enumerate(self.dialogs):
                if i == 1 and self.dialog_floods:
                    self.dialog_floods -= 1
                    raise errors.FloodWaitError(None, capture=12)
                yield SimpleNamespace(entity=entity)

        return gen()

    async def __call__(self, request):
        self.requests.append(request)
        if isinstance(request, GetFullChatRequest):
            users = self.members[request.chat_id]
            parts = [types.ChatParticipant(user_id=u.id, inviter_id=0, date=NOW) for u in users]
            return SimpleNamespace(
                full_chat=SimpleNamespace(participants=SimpleNamespace(participants=parts)), users=users
            )
        cid = request.channel.id
        opt = self.opts.get(cid, {})
        if opt.get("error"):
            raise opt["error"]
        users = self.members[cid]
        if isinstance(request, GetFullChannelRequest):
            return SimpleNamespace(
                full_chat=SimpleNamespace(participants_count=opt.get("reported", len(users)))
            )
        assert isinstance(request, GetParticipantsRequest)
        self.participant_calls += 1
        if self.participant_calls in self.flood_on:
            raise errors.FloodWaitError(request, capture=30)
        query = request.filter.q.lower()
        if query:
            hits = [u for u in users if u.first_name.lower().startswith(query)]
        else:
            hits = users[: opt.get("cap", len(users))]
        page = hits[request.offset : request.offset + min(request.limit, opt.get("page", 200))]
        return SimpleNamespace(
            participants=[types.ChannelParticipant(user_id=u.id, date=NOW) for u in page],
            users=page,
            count=len(hits),
        )
