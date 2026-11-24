"""The Bot API ingest loop (tgaud.jobs.ingest) in process: the fake Bot API, a fresh test database per test, and a
fake clock. Payloads use Bot API 10.5 field names (design.md 7, F§1-F§10)."""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import threading

import psycopg
import pytest
from botapi_fake import BOT_ID, FakeBotApi

from tgaud import config, db, history, metrics, sql
from tgaud.jobs import JobContext, StartupError
from tgaud.jobs import ingest

KEY = "ab" * 32
T0 = dt.datetime(2026, 11, 24, 12, 0, tzinfo=dt.UTC)
CHANNEL = {"id": -1001000000001, "title": "Owner channel", "type": "channel", "username": "ownerchan"}
GROUP = {"id": -1001000000002, "title": "Owner discussion", "type": "supergroup"}
BASIC = {"id": -4000000077, "title": "Old basic group", "type": "group"}
NEW_SG = {"id": -1001000000077, "title": "Old basic group", "type": "supergroup"}
ADMIN = 9001
BIO = "BIO-SECRET-do-not-store"
TEXT = "MESSAGE-TEXT-do-not-store"
USER_CHAT_ID = 777000123456
BOT_LINK = "https://t.me/+BOTLINKfulltoken123"
MASKED_LINK = "https://t.me/+AbCd…"


def ts(at: dt.datetime) -> int:
    return int(at.timestamp())


class Clock:
    def __init__(self, at: dt.datetime) -> None:
        self.t = at.timestamp()

    def __call__(self) -> float:
        return self.t

    def advance(self, **kw) -> None:
        self.t += dt.timedelta(**kw).total_seconds()

    @property
    def now(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self.t, dt.UTC)


class Crash(BaseException):
    """A crash between the body and the commit (not an Exception, so nothing in ingest handles it)."""


# ------------------------------------------------------------------------------------------------------------
# payloads
# ------------------------------------------------------------------------------------------------------------


def user(uid: int, **kw) -> dict:
    return {"id": uid, "is_bot": False, "first_name": f"Name{uid}", "last_name": f"Surname{uid}",
            "username": f"handle{uid}", "language_code": "en", **kw}


BOT_USER = {"id": BOT_ID, "is_bot": True, "first_name": "tgaud test", "username": "tgaud_test_bot"}


def member(uid: int, status: str, **kw) -> dict:
    return {"user": user(uid) if uid != BOT_ID else BOT_USER, "status": status, **kw}


def admin_rights(**kw) -> dict:
    rights = dict(can_be_edited=False, is_anonymous=False, can_manage_chat=True, can_delete_messages=True,
                  can_manage_video_chats=True, can_restrict_members=True, can_promote_members=False,
                  can_change_info=True, can_invite_users=True, can_post_stories=False, can_edit_stories=False,
                  can_delete_stories=False, can_post_messages=True, can_edit_messages=True)
    rights.update(kw)
    return rights


def chat_member(upd: int, uid: int, at: dt.datetime, old="left", new="member", *, chat=CHANNEL, actor=None,
                link=None, old_kw=None, new_kw=None, **kw) -> dict:
    body = {"chat": chat, "from": user(actor or uid), "date": ts(at),
            "old_chat_member": member(uid, old, **(old_kw or {})),
            "new_chat_member": member(uid, new, **(new_kw or {}))}
    if link is not None:
        body["invite_link"] = link
    body.update(kw)
    return {"update_id": upd, "chat_member": body}


def my_chat_member(upd: int, at: dt.datetime, new="administrator", old="left", *, chat=CHANNEL, actor=ADMIN,
                   **rights) -> dict:
    new_member = member(BOT_ID, new, **(admin_rights(**rights) if new == "administrator" else {}))
    return {"update_id": upd, "my_chat_member": {
        "chat": chat, "from": user(actor), "date": ts(at),
        "old_chat_member": member(BOT_ID, old), "new_chat_member": new_member}}


def bot_link(name="giveaway-nov") -> dict:
    return {"invite_link": BOT_LINK, "creator": BOT_USER, "creates_join_request": False, "is_primary": False,
            "is_revoked": False, "name": name}


def masked_link() -> dict:
    return {"invite_link": MASKED_LINK, "creator": user(ADMIN), "creates_join_request": True,
            "is_primary": False, "is_revoked": False, "name": "admin link"}


def join_request(upd: int, uid: int, at: dt.datetime, *, chat=CHANNEL, link=None) -> dict:
    body = {"chat": chat, "from": user(uid), "user_chat_id": USER_CHAT_ID, "date": ts(at), "bio": BIO}
    if link is not None:
        body["invite_link"] = link
    return {"update_id": upd, "chat_join_request": body}


def message(upd: int, at: dt.datetime, *, chat=GROUP, uid=None, mid=1, **kw) -> dict:
    body = {"message_id": mid, "date": ts(at), "chat": chat, **kw}
    if uid is not None:
        body["from"] = user(uid)
    return {"update_id": upd, "message": body}


def channel_post(upd: int, at: dt.datetime, mid: int, **kw) -> dict:
    return {"update_id": upd, "channel_post": {"message_id": mid, "date": ts(at), "chat": CHANNEL,
                                               "sender_chat": CHANNEL, **kw}}


# ------------------------------------------------------------------------------------------------------------
# harness
# ------------------------------------------------------------------------------------------------------------


def sample(name: str, **labels) -> float:
    return metrics.REGISTRY.get_sample_value(name, labels) or 0.0


class Harness:
    def __init__(self, dsn: str, *, clock: Clock | None = None, fake: FakeBotApi | None = None,
                 store_names: bool = True, key: str = KEY) -> None:
        self.dsn = dsn
        self.clock = clock or Clock(T0)
        self.fake = fake or FakeBotApi()
        self.cfg = config.load({"TGAUD_LINK_HMAC_KEY": key, "TGAUD_STORE_NAMES": str(store_names).lower()})
        self.conn: psycopg.Connection | None = None
        self.ing: ingest.Ingest | None = None

    def start(self) -> "Harness":
        if self.conn is not None:
            self.conn.close()
        self.conn = psycopg.connect(self.dsn)
        self.ctx = JobContext(config=self.cfg, api=self.fake.api(), health=metrics.Health(),
                              stop=threading.Event(), clock=self.clock)
        ingest.identify(self.ctx, self.conn)
        self.ctx.generation = db.claim_generation(self.conn)
        self.ing = ingest.Ingest(self.ctx, self.conn)
        self.ing.min_empty_poll = 0.0
        self.ing.backoff_base = 0.0
        self.ing.startup()
        return self

    restart = start

    def poll(self, *updates: dict) -> int | None:
        self.fake.updates(list(updates))
        return self.ing.poll_once()

    def q(self, query: str, *args) -> list[tuple]:
        with psycopg.connect(self.dsn, autocommit=True) as c:
            return c.execute(query, args or None).fetchall()

    def one(self, query: str, *args):
        rows = self.q(query, *args)
        assert len(rows) == 1, rows
        return rows[0] if len(rows[0]) > 1 else rows[0][0]

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()


@pytest.fixture
def h(migrated_dsn):
    harness = Harness(migrated_dsn).start()
    yield harness
    harness.close()


@pytest.fixture
def make(migrated_dsn):
    made: list[Harness] = []

    def factory(**kw) -> Harness:
        harness = Harness(migrated_dsn, **kw)
        made.append(harness)
        return harness

    yield factory
    for harness in made:
        harness.close()


def digest(link: str, key: str = KEY) -> bytes:
    return hmac.new(bytes.fromhex(key), link.encode(), hashlib.sha256).digest()


def tracked_channel(h: Harness, upd: int = 1, chat=CHANNEL, at=None) -> None:
    assert h.poll(my_chat_member(upd, at or h.clock.now - dt.timedelta(hours=1), chat=chat)) == 1


# ------------------------------------------------------------------------------------------------------------
# chat_member
# ------------------------------------------------------------------------------------------------------------


def test_a_join_through_a_bot_link_stores_event_user_link_receipt_and_raw(h):
    tracked_channel(h)
    at = T0 - dt.timedelta(minutes=5)
    joins = sample("tgaud_chat_events_total", chat=str(CHANNEL["id"]), kind="join")
    assert h.poll(chat_member(10, 42, at, link=bot_link())) == 1

    ev = h.one("""SELECT chat_id, user_id, event_at, update_id, old_status, new_status, kind, actor_id,
                         actor_is_other, via_join_request, via_folder_link, source_chat_id, invite_link_id
                  FROM tg.member_event""")
    assert ev[:12] == (CHANNEL["id"], 42, at, 10, "left", "member", "join", None, False, False, False, None)
    link = h.one("SELECT invite_link_id, chat_id, masked_link, link_digest, made_by_bot, creator_id, name "
                 "FROM tg.invite_link")
    assert link == (ev[12], CHANNEL["id"], None, digest(BOT_LINK), True, BOT_ID, "giveaway-nov")
    assert h.one("SELECT is_bot, language_code, username, first_name, last_name, first_seen_at FROM tg.tg_user "
                 "WHERE user_id = 42") == (False, "en", "handle42", "Name42", "Surname42", at)
    assert h.one("SELECT update_type, chat_id, update_date FROM tg.update_receipt WHERE update_id = 10") == (
        "chat_member", CHANNEL["id"], at)
    payload = h.one("SELECT payload FROM tg.raw_update WHERE update_id = 10")
    assert payload["invite_link"]["invite_link"] == ingest.DIGEST_MARK + digest(BOT_LINK).hex()
    assert payload["new_chat_member"]["user"]["id"] == 42
    assert h.one("SELECT next_offset, last_update_id, last_update_at FROM tg.bot_state") == (11, 10, at)
    assert sample("tgaud_chat_events_total", chat=str(CHANNEL["id"]), kind="join") == joins + 1
    assert sample("tgaud_last_transition_timestamp_seconds", chat=str(CHANNEL["id"])) == at.timestamp()


def test_masked_links_keep_their_text_and_are_one_row_per_link(h):
    tracked_channel(h)
    h.poll(chat_member(10, 42, T0, link=masked_link()), chat_member(11, 43, T0, link=masked_link()))
    assert h.q("SELECT masked_link, link_digest, creator_id, name, creates_join_request FROM tg.invite_link") == [
        (MASKED_LINK, None, ADMIN, "admin link", True)]
    assert h.one("SELECT count(DISTINCT invite_link_id) FROM tg.member_event") == 1


def test_kick_by_an_admin_restriction_and_folder_join(h):
    tracked_channel(h, chat=GROUP)
    until = T0 + dt.timedelta(days=3)
    h.poll(chat_member(10, 42, T0 - dt.timedelta(hours=3), chat=GROUP, via_chat_folder_invite_link=True),
           chat_member(11, 42, T0 - dt.timedelta(hours=2), "member", "restricted", chat=GROUP, actor=ADMIN,
                       new_kw={"is_member": True, "until_date": ts(until), "can_send_messages": False}),
           chat_member(12, 42, T0 - dt.timedelta(hours=1), "restricted", "kicked", chat=GROUP, actor=ADMIN,
                       old_kw={"is_member": True, "until_date": ts(until)}, new_kw={"until_date": 0}))
    rows = h.q("""SELECT update_id, kind, old_is_member, new_is_member, actor_id, actor_is_other, via_folder_link,
                         old_until_date, new_until_date FROM tg.member_event ORDER BY update_id""")
    assert rows == [
        (10, "join", None, None, None, False, True, None, None),
        (11, "restrict", None, True, ADMIN, True, False, None, until),
        (12, "kick", True, None, ADMIN, True, False, until, None),
    ]
    # the admin is not an audience member
    assert h.q("SELECT user_id FROM tg.tg_user") == [(42,)]


def fake_rebuild(conn, pairs):
    """A stand-in for the history task's rebuild (a stub in this base): one spell per join .. exit from the
    events, a join while already in absorbed (as E3 absorbs a migration echo)."""
    for chat, user in pairs:
        conn.execute("DELETE FROM tg.membership WHERE chat_id = %s AND user_id = %s", (chat, user))
        start = None
        for eid, at, kind in conn.execute("""SELECT event_id, event_at, kind FROM tg.member_event
                                             WHERE chat_id = %s AND user_id = %s ORDER BY event_at, update_id""",
                                          (chat, user)).fetchall():
            if kind == "join" and start is None:
                start = (eid, at)
            elif kind in ("leave", "kick", "restrict_out") and start is not None:
                conn.execute(
                    """INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, join_event_id, known_from,
                                                  join_route, last_confirmed_at, end_source, end_reason, left_at,
                                                  left_event_id, left_after, left_before)
                       VALUES (%s, %s, 'event', %s, %s, %s, 'unattributed', %s, 'event', 'left', %s, %s, %s, %s)""",
                    (chat, user, start[1], start[0], start[1], start[1], at, eid, at, at))
                start = None
    return history.RebuildResult()


def test_short_stays_come_from_the_rebuilt_spell_this_exit_closed(h, monkeypatch):
    monkeypatch.setattr(history, "rebuild_pairs", fake_rebuild)
    tracked_channel(h)
    chat = str(CHANNEL["id"])
    before = sample("tgaud_chat_short_stays_total", chat=chat)
    h.poll(chat_member(10, 42, T0 - dt.timedelta(hours=30)),
           chat_member(11, 42, T0 - dt.timedelta(hours=2), "member", "left"),
           chat_member(12, 43, T0 - dt.timedelta(hours=1), "member", "left"))  # 43: no join seen
    assert sample("tgaud_chat_short_stays_total", chat=chat) == before + 1
    # a member of three days with a migration-style echo join 3 h ago: the stay is not short
    h.poll(chat_member(13, 44, T0 - dt.timedelta(days=3)), chat_member(14, 44, T0 - dt.timedelta(hours=3)),
           chat_member(15, 44, T0 - dt.timedelta(hours=1), "member", "left"))
    assert sample("tgaud_chat_short_stays_total", chat=chat) == before + 1
    assert sample("tgaud_chat_events_total", chat=chat, kind="leave") >= 3


def test_join_requests_are_approved_by_the_matching_join(h):
    tracked_channel(h)
    req_at = T0 - dt.timedelta(hours=2)
    approved = T0 - dt.timedelta(hours=1)
    approval_link = dict(masked_link(), name="approval link", creates_join_request=True)
    other_link = dict(approval_link, invite_link="https://t.me/+ZzYy…", name="other link")
    h.poll(join_request(10, 42, req_at),                          # a direct request, no link
           join_request(11, 43, req_at, link=approval_link),       # through a link that needs approval
           join_request(12, 44, req_at, link=other_link))
    rows = h.q("SELECT user_id, outcome, invite_link_id IS NOT NULL FROM tg.join_request ORDER BY user_id")
    assert rows == [(42, None, False), (43, None, True), (44, None, True)]
    raw = h.one("SELECT payload FROM tg.raw_update WHERE update_id = 10")
    assert "bio" not in raw and "user_chat_id" not in raw
    h.poll(chat_member(13, 42, approved, actor=ADMIN, via_join_request=True),   # Telegram: direct, no link
           chat_member(14, 43, approved, actor=ADMIN, link=approval_link),      # Telegram: the link, no flag
           chat_member(15, 44, approved, actor=ADMIN, link=approval_link))      # requested via another link
    assert h.q("SELECT user_id, outcome, resolved_at, resolved_by FROM tg.join_request ORDER BY user_id") == [
        (42, "approved", approved, ADMIN), (43, "approved", approved, ADMIN), (44, None, None, None)]
    assert h.q("SELECT user_id, via_join_request, actor_is_other FROM tg.member_event ORDER BY user_id") == [
        (42, True, True), (43, False, True), (44, False, True)]


# ------------------------------------------------------------------------------------------------------------
# my_chat_member: tracking, coverage and gaps
# ------------------------------------------------------------------------------------------------------------


def test_my_chat_member_tracks_the_chat_and_keeps_coverage_and_gaps(h):
    t1, t2, t3, t4 = (T0 - dt.timedelta(hours=n) for n in (4, 3, 2, 1))
    h.poll(my_chat_member(1, t1))
    assert h.one("SELECT tracked, bot_status, can_invite_users, can_restrict_members, metadata_due_at IS NOT NULL "
                 "FROM tg.chat") == (True, "administrator", True, True, True)
    assert h.q("SELECT lower(period), upper(period) FROM tg.coverage") == [(t1, None)]
    h.poll(my_chat_member(2, t2, old="administrator", can_invite_users=False))
    assert h.q("SELECT reason, lower(period), upper(period) FROM tg.gap") == [("rights_lost", t2, None)]
    assert sample("tgaud_chat_permission_ok", chat=str(CHANNEL["id"]), right="invite_users") == 0
    h.poll(my_chat_member(3, t3, "member", "administrator"))
    assert h.q("SELECT reason, lower(period), upper(period) FROM tg.gap ORDER BY lower(period)") == [
        ("rights_lost", t2, t3), ("not_admin", t3, None)]
    assert h.q("SELECT lower(period), upper(period) FROM tg.coverage") == [(t1, t3)]
    assert sample("tgaud_chat_bot_admin", chat=str(CHANNEL["id"])) == 0
    h.poll(my_chat_member(4, t4, "administrator", "member"))
    assert h.q("SELECT reason, upper(period) FROM tg.gap ORDER BY lower(period)") == [("rights_lost", t3), ("not_admin", t4)]
    assert h.q("SELECT lower(period), upper(period) FROM tg.coverage ORDER BY lower(period)") == [(t1, t3), (t4, None)]
    assert sample("tgaud_chat_bot_admin", chat=str(CHANNEL["id"])) == 1
    raw = h.q("SELECT update_type, user_id FROM tg.raw_update ORDER BY update_id")
    assert raw == [("my_chat_member", ADMIN)] * 4


def test_a_private_block_commits_only_the_receipt_then_a_join_after_a_restart(h):
    blocked = {"update_id": 5, "my_chat_member": {
        "chat": {"id": 42, "type": "private", "first_name": "Name42", "username": "handle42"},
        "from": user(42), "date": ts(T0 - dt.timedelta(minutes=9)),
        "old_chat_member": member(BOT_ID, "member"), "new_chat_member": member(BOT_ID, "kicked", until_date=0)}}
    assert h.poll(blocked) == 1
    assert h.q("SELECT update_type, chat_id FROM tg.update_receipt") == [("my_chat_member", None)]
    for table in ("tg.chat", "tg.tg_user", "tg.raw_update", "tg.gap", "tg.coverage"):
        assert h.q(f"SELECT count(*) FROM {table}") == [(0,)], table
    h.restart()
    tracked_channel(h, upd=6)
    h.poll(chat_member(7, 42, T0 - dt.timedelta(minutes=1)))
    assert h.q("SELECT update_id FROM tg.member_event") == [(7,)]
    assert h.one("SELECT next_offset FROM tg.bot_state") == 8


# ------------------------------------------------------------------------------------------------------------
# message and channel_post
# ------------------------------------------------------------------------------------------------------------


def test_group_messages_count_activity_and_unattributed_senders_never_text(h):
    tracked_channel(h, chat=GROUP)
    at = T0 - dt.timedelta(minutes=30)
    h.poll(message(10, at, uid=42, mid=1, text=TEXT),
           message(11, at, uid=42, mid=2, photo=[{"file_id": "x", "file_unique_id": "y", "width": 1, "height": 1}],
                   caption=TEXT),
           message(12, at, uid=43, mid=3, text=TEXT),
           message(13, at, uid=777000, mid=4, sender_chat=CHANNEL, is_automatic_forward=True, text=TEXT),
           message(14, at, uid=1087968824, mid=5, sender_chat=GROUP, text=TEXT),
           message(15, at, uid=136817688, mid=6, sender_chat={"id": -1009999, "type": "channel", "title": "x"},
                   text=TEXT),
           message(16, at, uid=44, mid=7, new_chat_members=[user(44)]))
    assert h.q("SELECT user_id, day, messages FROM tg.activity_daily ORDER BY user_id") == [
        (42, at.date(), 2), (43, at.date(), 1)]
    assert h.q("SELECT sender_kind, messages FROM tg.unattributed_daily ORDER BY sender_kind") == [
        ("anonymous_admin", 1), ("automatic_forward", 1), ("sender_chat", 1)]
    assert h.q("SELECT update_type FROM tg.raw_update") == [("my_chat_member",)]  # messages are never raw
    assert h.one("SELECT count(*) FROM tg.update_receipt WHERE update_type = 'message'") == 7


def test_channel_posts_and_an_observed_giveaway(h):
    tracked_channel(h)
    at = T0 - dt.timedelta(hours=1)
    draw = T0 + dt.timedelta(days=2)
    giveaway = {"chats": [CHANNEL, GROUP], "winners_selection_date": ts(draw), "winner_count": 3,
                "only_new_members": True, "prize_description": TEXT}
    post = channel_post(11, at, 501, giveaway=giveaway)
    h.poll(channel_post(10, at, 500, text=TEXT), post)
    h.poll(dict(post, update_id=12))  # the same post again (a new update id): one campaign
    assert h.q("SELECT message_id, posted_at, kind FROM tg.post ORDER BY message_id") == [
        (500, at, "text"), (501, at, "giveaway")]
    camp = h.one("""SELECT campaign_id, kind, origin, starts_at, draw_at, only_new_members, winner_count,
                           post_chat_id, post_message_id FROM tg.campaign""")
    assert camp[1:] == ("tg_giveaway", "observation", at, draw, True, 3, CHANNEL["id"], 501)
    assert h.q("SELECT chat_id FROM tg.campaign_chat ORDER BY chat_id") == [(GROUP["id"],), (CHANNEL["id"],)]


# ------------------------------------------------------------------------------------------------------------
# idempotency, crash safety, offsets, failures
# ------------------------------------------------------------------------------------------------------------


def test_the_same_update_twice_is_one_receipt_and_one_event(h):
    tracked_channel(h)
    upd = chat_member(10, 42, T0)
    h.poll(upd)
    h.poll(upd)
    assert h.q("SELECT count(*) FROM tg.update_receipt WHERE update_id = 10") == [(1,)]
    assert h.q("SELECT count(*) FROM tg.member_event") == [(1,)]


def test_join_leave_join_in_one_second_are_three_events(h):
    tracked_channel(h)
    h.poll(chat_member(10, 42, T0), chat_member(11, 42, T0, "member", "left"), chat_member(12, 42, T0))
    assert h.q("SELECT update_id, kind FROM tg.member_event ORDER BY update_id") == [
        (10, "join"), (11, "leave"), (12, "join")]


def test_a_crash_before_the_commit_processes_the_update_again_once(h):
    tracked_channel(h)
    batch = [chat_member(10, 42, T0), chat_member(11, 43, T0)]

    def crash(update_id):
        if update_id == 11:
            raise Crash()

    h.ing.before_commit = crash
    with pytest.raises(Crash):
        h.poll(*batch)
    assert h.q("SELECT update_id FROM tg.member_event") == [(10,)]
    assert h.one("SELECT next_offset FROM tg.bot_state") == 11
    h.restart()
    h.poll(*batch)  # Telegram re-delivers from the committed offset; 10 would be dropped by the receipt anyway
    assert h.fake.calls_of("getUpdates")[-1].params["offset"] == 11
    assert h.q("SELECT update_id FROM tg.member_event ORDER BY 1") == [(10,), (11,)]
    assert h.q("SELECT count(*) FROM tg.update_receipt WHERE update_type = 'chat_member'") == [(2,)]


def test_a_warm_process_omits_the_offset_after_six_days_of_silence(h):
    tracked_channel(h)
    h.poll(chat_member(1000, 42, T0))
    h.poll()
    assert h.fake.calls_of("getUpdates")[-1].params["offset"] == 1001
    h.clock.advance(days=6, hours=1)
    h.poll()
    assert "offset" not in h.fake.calls_of("getUpdates")[-1].params
    # an empty first response, then a lower id: committed, and from then on the offset is used again
    h.poll(chat_member(7, 43, h.clock.now - dt.timedelta(seconds=5)))
    assert "offset" not in h.fake.calls_of("getUpdates")[-1].params
    assert h.one("SELECT next_offset FROM tg.bot_state") == 8
    h.poll()
    assert h.fake.calls_of("getUpdates")[-1].params["offset"] == 8
    assert h.q("SELECT update_id FROM tg.member_event ORDER BY 1") == [(7,), (1000,)]


def test_a_restart_before_the_restarted_id_commits_omits_the_offset_again(h):
    tracked_channel(h)
    h.poll(chat_member(1000, 42, T0))
    h.clock.advance(days=7)
    h.ing.before_commit = lambda update_id: (_ for _ in ()).throw(Crash()) if update_id == 3 else None
    with pytest.raises(Crash):
        h.poll(chat_member(3, 43, h.clock.now))
    h.restart()
    h.poll(chat_member(3, 43, h.clock.now))
    assert all("offset" not in c.params for c in h.fake.calls_of("getUpdates")[-2:])
    assert h.one("SELECT next_offset FROM tg.bot_state") == 4


def test_a_malformed_update_is_recorded_and_the_next_join_commits_once(h):
    tracked_channel(h)
    failures = sample("tgaud_ingest_failures_total", stage="parse")
    bad = chat_member(10, 42, T0, link=bot_link(), bio=BIO)
    bad["chat_member"]["new_chat_member"]["status"] = "banned"
    no_date = chat_member(11, 43, T0)
    del no_date["chat_member"]["date"]
    good = chat_member(12, 44, T0)
    assert h.poll(bad, no_date, good) == 3
    assert sample("tgaud_ingest_failures_total", stage="parse") == failures + 2
    receipts = h.q("SELECT update_id, update_type, update_date, chat_id FROM tg.update_receipt "
                   "WHERE update_id >= 10 ORDER BY update_id")
    assert receipts == [(10, "malformed", T0, CHANNEL["id"]), (11, "malformed", T0, CHANNEL["id"]),
                        (12, "chat_member", T0, CHANNEL["id"])]
    raw = h.one("SELECT payload FROM tg.raw_update WHERE update_id = 10")
    assert "bio" not in raw
    assert raw["invite_link"]["invite_link"].startswith(ingest.DIGEST_MARK)
    assert h.q("SELECT update_id FROM tg.member_event") == [(12,)]
    assert h.one("SELECT next_offset FROM tg.bot_state") == 13


def test_a_database_error_rolls_back_and_does_not_advance(h):
    tracked_channel(h)
    failures = sample("tgaud_ingest_failures_total", stage="db")
    attempts = []

    def fail(update_id):
        if update_id == 10:
            attempts.append(1)
            if len(attempts) >= 3:
                h.ctx.stop.set()
            h.conn.execute("SELECT 1/0")

    h.ing.before_commit = fail
    with pytest.raises(psycopg.errors.DivisionByZero):
        h.poll(chat_member(10, 42, T0))
    assert len(attempts) == 3
    assert sample("tgaud_ingest_failures_total", stage="db") == failures + 3
    assert h.one("SELECT next_offset FROM tg.bot_state") == 2
    assert h.q("SELECT count(*) FROM tg.member_event") == [(0,)]
    h.ctx.stop.clear()
    h.ing.before_commit = lambda update_id: None
    h.poll(chat_member(10, 42, T0))
    assert h.one("SELECT next_offset FROM tg.bot_state") == 11
    assert h.q("SELECT count(*) FROM tg.member_event") == [(1,)]


def test_caught_up_is_the_send_time_after_the_last_update_of_a_short_response(h):
    tracked_channel(h)
    first = h.one("SELECT caught_up_at FROM tg.bot_state")
    assert first == h.clock.now
    finished = first + dt.timedelta(seconds=30)  # a snapshot the reconciler may apply once caught_up_at passes it
    with psycopg.connect(h.dsn, autocommit=True) as c:
        c.execute("INSERT INTO tg.snapshot (chat_id, started_at, finished_at, method, fetched, complete) "
                  "VALUES (%s, %s, %s, 'listing', 0, true)", (CHANNEL["id"], first, finished))
    h.clock.advance(minutes=1)
    sent = h.clock.now
    h.ing.before_commit = lambda update_id: (_ for _ in ()).throw(Crash()) if update_id == 11 else None
    with pytest.raises(Crash):
        h.poll(chat_member(10, 42, T0), chat_member(11, 43, T0))
    assert h.one("SELECT caught_up_at FROM tg.bot_state") == first  # the old watermark stays
    assert h.one("SELECT count(*) FROM tg.snapshot WHERE reconciled_at IS NULL AND finished_at > %s", first) == 1
    h.restart()
    h.clock.advance(minutes=1)
    resent = h.clock.now
    h.poll(chat_member(11, 43, T0))
    assert h.one("SELECT caught_up_at FROM tg.bot_state") == resent > sent
    # a full response (100 updates) is not caught up
    h.clock.advance(minutes=1)
    h.poll(*[message(100 + i, T0, uid=50, mid=i, chat=GROUP, text="x") for i in range(100)])
    assert h.one("SELECT caught_up_at FROM tg.bot_state") == resent


def test_service_down_after_25_hours_of_failed_polls_is_atomic_with_the_watermark(h):
    h.poll()
    watermark = h.one("SELECT last_poll_ok_at FROM tg.bot_state")
    assert watermark == T0
    for _ in range(25):
        h.clock.advance(hours=1)
        h.fake.error("getUpdates", 502, "Bad Gateway")
        assert h.ing.poll_once() is None
    assert h.one("SELECT last_poll_ok_at FROM tg.bot_state") == watermark
    h.ing.before_commit = lambda update_id: (_ for _ in ()).throw(Crash())
    with pytest.raises(Crash):
        h.poll()
    assert h.one("SELECT last_poll_ok_at FROM tg.bot_state") == watermark
    assert h.q("SELECT count(*) FROM tg.gap") == [(0,)]
    lost = sample("tgaud_lost_seconds_total")
    h.restart()
    h.poll()
    now = h.clock.now
    assert h.one("SELECT last_poll_ok_at FROM tg.bot_state") == now
    assert h.q("SELECT chat_id, reason, confirmed, lower(period), upper(period) FROM tg.gap") == [
        (None, "service_down", True, watermark, now)]
    assert sample("tgaud_lost_seconds_total") == lost + 3600


def test_a_lag_past_20_hours_records_a_suspected_gap(h):
    tracked_channel(h)
    old = T0 - dt.timedelta(hours=21)
    h.poll(chat_member(10, 42, old))
    assert h.q("SELECT reason, confirmed, lower(period), upper(period) FROM tg.gap") == [
        ("service_down", False, old, T0)]
    h.poll(chat_member(11, 43, old + dt.timedelta(minutes=1)))  # overlaps it: no second gap
    assert h.q("SELECT count(*) FROM tg.gap") == [(1,)]


# ------------------------------------------------------------------------------------------------------------
# startup
# ------------------------------------------------------------------------------------------------------------


def test_startup_checks_the_rights_of_every_tracked_chat(make):
    h = make()
    h.start()
    with psycopg.connect(h.dsn, autocommit=True) as c:
        for chat in (CHANNEL, GROUP, BASIC):
            c.execute("INSERT INTO tg.chat (chat_id, chat_type, title, tracked, bot_status) "
                      "VALUES (%s, %s, %s, true, 'administrator')", (chat["id"], chat["type"], chat["title"]))
    answers = {CHANNEL["id"]: {"ok": member(BOT_ID, "administrator", **admin_rights())},
               GROUP["id"]: {"ok": member(BOT_ID, "administrator", **admin_rights(can_restrict_members=False))},
               BASIC["id"]: {"err": (403, "Forbidden: bot was kicked from the group chat")}}
    for chat_id in sorted(answers):
        a = answers[chat_id]
        if "ok" in a:
            h.fake.ok("getChatMember", a["ok"])
        else:
            h.fake.error("getChatMember", *a["err"])
    h.restart()
    calls = h.fake.calls_of("getChatMember")
    assert sorted(c.params["chat_id"] for c in calls) == sorted(answers)
    assert all(c.params["user_id"] == BOT_ID for c in calls)
    rows = h.q("SELECT chat_id, bot_status, can_invite_users, can_restrict_members, rights_checked_at, "
               "metadata_due_at FROM tg.chat ORDER BY chat_id")
    assert [r[:4] for r in rows] == [(GROUP["id"], "administrator", True, False),
                                     (CHANNEL["id"], "administrator", True, True),
                                     (BASIC["id"], "kicked", None, None)]
    assert all(r[4] == T0 and r[5] == T0 for r in rows)
    assert h.q("SELECT chat_id, reason, lower(period), upper(period) FROM tg.gap ORDER BY chat_id") == [
        (GROUP["id"], "rights_lost", T0, None), (BASIC["id"], "not_admin", T0, None)]
    assert h.q("SELECT chat_id, lower(period), upper(period), source FROM tg.coverage ORDER BY chat_id") == [
        (GROUP["id"], T0, None, "rights_check"), (CHANNEL["id"], T0, None, "rights_check")]
    assert sample("tgaud_chat_permission_ok", chat=str(GROUP["id"]), right="restrict_members") == 0
    assert sample("tgaud_chat_bot_admin", chat=str(BASIC["id"])) == 0
    assert h.one("SELECT allowed_updates FROM tg.bot_state") == list(ingest.ALLOWED_UPDATES)


def test_metadata_is_fetched_after_the_discovery_commits_and_retried(h):
    tracked_channel(h, upd=1)
    h.clock.advance(seconds=1)
    tracked_channel(h, upd=2, chat=GROUP)
    h.fake.error("getChat", 500, "Internal Server Error")  # the first fetch (the channel, due first) fails once
    channel_info = dict(CHANNEL, linked_chat_id=GROUP["id"], join_by_request=False, has_hidden_members=False,
                        accent_color_id=0, max_reaction_count=11, accepted_gift_types={})
    group_info = dict(GROUP, linked_chat_id=CHANNEL["id"], join_by_request=True, has_hidden_members=True,
                      is_forum=False, accent_color_id=0, max_reaction_count=11, accepted_gift_types={})
    h.fake.respond("getChat", lambda p: channel_info if p["chat_id"] == CHANNEL["id"] else group_info)
    assert h.ing.refresh_metadata() == 1
    assert h.one("SELECT metadata_attempts, linked_chat_id FROM tg.chat WHERE chat_id = %s", CHANNEL["id"]) == (1, None)
    assert h.one("SELECT join_by_request, has_hidden_members, linked_chat_id FROM tg.chat WHERE chat_id = %s",
                 GROUP["id"]) == (True, True, CHANNEL["id"])
    with psycopg.connect(h.dsn) as c:
        assert sql.run(c, "11").rows == []  # the pair is not known yet
    assert h.ing.refresh_metadata() == 0  # not due yet
    h.clock.advance(seconds=31)
    assert h.ing.refresh_metadata() == 1
    assert h.one("SELECT metadata_due_at, metadata_attempts, linked_chat_id FROM tg.chat WHERE chat_id = %s",
                 CHANNEL["id"]) == (None, 0, GROUP["id"])
    with psycopg.connect(h.dsn) as c:
        r = sql.run(c, "11")
        assert [(row[0], row[2]) for row in r.rows] == [(CHANNEL["id"], GROUP["id"])]


# ------------------------------------------------------------------------------------------------------------
# migrations (M1 / M2)
# ------------------------------------------------------------------------------------------------------------


def migrate_to(upd, at):
    return message(upd, at, chat=BASIC, uid=ADMIN, mid=90, migrate_to_chat_id=NEW_SG["id"])


def migrate_from(upd, at):
    return message(upd, at, chat=NEW_SG, uid=ADMIN, mid=1, migrate_from_chat_id=BASIC["id"])


@pytest.mark.parametrize("order", ["to_first", "from_first"])
def test_a_migration_into_a_tracked_supergroup_moves_the_history_once(h, order):
    t = T0 - dt.timedelta(hours=5)
    tracked_channel(h, upd=1, chat=BASIC, at=t)
    h.poll(chat_member(2, 42, t + dt.timedelta(minutes=1), chat=BASIC),
           message(3, t + dt.timedelta(minutes=2), chat=BASIC, uid=42, text=TEXT))
    mig = T0 - dt.timedelta(hours=1)
    h.poll(my_chat_member(4, mig, chat=NEW_SG))  # the bot is admin in the new supergroup
    first, second = (migrate_to(5, mig), migrate_from(6, mig)) if order == "to_first" else (
        migrate_from(5, mig), migrate_to(6, mig))
    h.poll(first)
    h.restart()  # a stop after the first commit
    h.poll(second, dict(first, update_id=7))
    assert h.q("SELECT old_chat_id, chat_id, migrated_at, moved_at IS NOT NULL FROM tg.chat_alias") == [
        (BASIC["id"], NEW_SG["id"], mig, True)]
    assert h.q("SELECT chat_id, source_chat_id FROM tg.member_event") == [(NEW_SG["id"], BASIC["id"])]
    assert h.q("SELECT chat_id, messages FROM tg.activity_daily") == [(NEW_SG["id"], 1)]
    assert h.q("SELECT chat_id FROM tg.chat ORDER BY chat_id") == [(NEW_SG["id"],)]
    # a late update of the old id is routed to the new chat
    h.poll(chat_member(8, 43, mig - dt.timedelta(minutes=30), chat=BASIC))
    assert h.one("SELECT chat_id, source_chat_id FROM tg.member_event WHERE update_id = 8") == (
        NEW_SG["id"], BASIC["id"])


def test_m2_keeps_the_old_history_until_the_new_chat_is_tracked(h):
    t = T0 - dt.timedelta(hours=5)
    tracked_channel(h, upd=1, chat=BASIC, at=t)
    h.poll(chat_member(2, 42, t + dt.timedelta(minutes=1), chat=BASIC))
    mig = T0 - dt.timedelta(hours=2)
    h.poll(migrate_to(3, mig))
    assert h.q("SELECT old_chat_id, chat_id, moved_at FROM tg.chat_alias") == [(BASIC["id"], NEW_SG["id"], None)]
    assert h.q("SELECT chat_id, tracked FROM tg.chat ORDER BY chat_id") == [(NEW_SG["id"], False), (BASIC["id"], True)]
    assert h.q("SELECT upper(period) FROM tg.coverage WHERE chat_id = %s", BASIC["id"]) == [(mig,)]
    h.restart()
    h.poll(migrate_from(4, mig))
    # a late exit of the old id stays in the old history
    late = mig - dt.timedelta(minutes=10)
    h.poll(chat_member(5, 42, late, "member", "left", chat=BASIC))
    assert h.q("SELECT update_id, chat_id, source_chat_id FROM tg.member_event ORDER BY update_id") == [
        (2, BASIC["id"], None), (5, BASIC["id"], None)]
    # the bot becomes admin in the new supergroup: M1 moves the history once
    h.poll(my_chat_member(6, T0 - dt.timedelta(minutes=30), chat=NEW_SG))
    assert h.q("SELECT update_id, chat_id, source_chat_id FROM tg.member_event ORDER BY update_id") == [
        (2, NEW_SG["id"], BASIC["id"]), (5, NEW_SG["id"], BASIC["id"])]
    assert h.one("SELECT moved_at IS NOT NULL FROM tg.chat_alias") is True
    h.poll(my_chat_member(7, T0 - dt.timedelta(minutes=20), "administrator", "administrator", chat=NEW_SG))
    assert h.q("SELECT count(*) FROM tg.member_event") == [(2,)]


def test_move_chat_history_is_idempotent_and_merges_overlapping_coverage(migrated_dsn):
    old, new = -400001, -1004000000001
    a, b, c = (T0 - dt.timedelta(days=n) for n in (10, 5, 1))
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute("SELECT tg.ensure_partitions('member_event', 'month', %s, %s)", (a, T0))
        conn.execute("INSERT INTO tg.chat (chat_id, chat_type, title, tracked) VALUES (%s, 'group', 'o', true), "
                     "(%s, 'supergroup', 'n', true)", (old, new))
        conn.execute("INSERT INTO tg.tg_user (user_id, first_seen_at, last_seen_at) VALUES (1, %s, %s)", (a, a))
        conn.execute("INSERT INTO tg.coverage (chat_id, period) VALUES (%s, tstzrange(%s, %s)), (%s, tstzrange(%s, NULL))",
                     (old, a, c, new, b))
        conn.execute("INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (%s, 1, 'manual'), (%s, 1, 'manual')",
                     (old, new))
        conn.execute("INSERT INTO tg.invite_link (chat_id, masked_link, creator_id) VALUES (%s, 'm…', 7), (%s, 'm…', 7)",
                     (old, new))
        conn.execute("""INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status,
                                                     invite_link_id, actor_is_other)
                        SELECT %s, 1, %s, 1, 'left', 'member', invite_link_id, false FROM tg.invite_link
                        WHERE chat_id = %s""", (old, b, old))
        conn.execute("INSERT INTO tg.chat_alias (old_chat_id, chat_id, migrated_at) VALUES (%s, %s, %s)", (old, new, c))
        assert conn.execute("SELECT tg.move_chat_history(%s, %s, %s)", (old, new, c)).fetchone()[0] == 1
        assert conn.execute("SELECT tg.move_chat_history(%s, %s, %s)", (old, new, c)).fetchone()[0] == 0
        assert conn.execute("SELECT lower(period), upper(period) FROM tg.coverage").fetchall() == [(a, None)]
        assert conn.execute("SELECT chat_id, outcome FROM tg.probe ORDER BY probe_id").fetchall() == [
            (new, "cancelled"), (new, "pending")]
        link = conn.execute("SELECT invite_link_id FROM tg.invite_link").fetchall()
        assert len(link) == 1
        assert conn.execute("SELECT invite_link_id FROM tg.member_event").fetchall() == link
        assert conn.execute("SELECT moved_at IS NOT NULL FROM tg.chat_alias").fetchone()[0] is True
        conn.rollback()


# ------------------------------------------------------------------------------------------------------------
# privacy, erasure, identity
# ------------------------------------------------------------------------------------------------------------


def test_nothing_private_is_stored_after_a_full_run(make):
    h = make(store_names=False).start()
    tracked_channel(h)
    tracked_channel(h, upd=2, chat=GROUP)
    at = T0 - dt.timedelta(minutes=20)
    bad = join_request(9, 41, at, link=bot_link())
    bad["chat_join_request"]["date"] = "yesterday"
    h.poll(join_request(10, 42, at, link=bot_link()), chat_member(11, 42, at, actor=ADMIN, via_join_request=True,
                                                                   link=bot_link()),
           chat_member(12, 43, at, link=masked_link()), bad,
           message(13, at, uid=42, text=TEXT, caption=TEXT), channel_post(14, at, 7, text=TEXT),
           chat_member(15, 44, at, chat=GROUP, link=bot_link("second")))
    assert h.one("SELECT username, first_name, last_name FROM tg.tg_user WHERE user_id = 42") == (None, None, None)
    tables = h.q("""SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname IN ('tg', 'stats') AND c.relkind IN ('r', 'p', 'm') AND NOT c.relispartition
                      AND (c.relkind <> 'm' OR c.relispopulated)""")
    secrets = [BIO, TEXT, str(USER_CHAT_ID), BOT_LINK, "BOTLINKfulltoken", "user_chat_id", "Name42", "Surname42",
               "handle42", "Name43", "Name41"]
    found = []
    for (table,) in tables:
        for (row,) in h.q(f"SELECT t::text FROM {table} t"):
            found += [(table, s) for s in secrets if s in row]
    assert found == []
    assert h.q("SELECT count(*) FROM tg.raw_update") == [(7,)]  # 2 my_chat_member, 1 + 3 + the malformed request


def test_erased_people_dated_before_the_erasure_leave_only_the_receipt(h):
    tracked_channel(h)
    with psycopg.connect(h.dsn, autocommit=True) as c:
        c.execute("INSERT INTO tg.erasure (user_hash, erased_at) VALUES (tg.erasure_hash(42), %s), "
                  "(tg.erasure_hash(%s), %s)", (T0, ADMIN, T0))
    before = T0 - dt.timedelta(minutes=10)
    h.poll(chat_member(10, 42, before), join_request(11, 42, before), message(12, before, uid=42, text="hi"))
    assert h.q("SELECT count(*) FROM tg.update_receipt WHERE update_id BETWEEN 10 AND 12") == [(3,)]
    for table in ("tg.member_event", "tg.tg_user", "tg.join_request", "tg.activity_daily"):
        assert h.q(f"SELECT count(*) FROM {table}") == [(0,)], table
    assert h.q("SELECT count(*) FROM tg.raw_update WHERE update_id >= 10") == [(0,)]
    # an older update where the erased admin is only the actor: stored without the admin's id
    h.poll(chat_member(13, 43, before, "member", "kicked", actor=ADMIN, link=masked_link()))
    assert h.one("SELECT actor_id, actor_is_other, kind FROM tg.member_event") == (None, True, "kick")
    assert h.one("SELECT creator_id FROM tg.invite_link") is None
    raw = h.one("SELECT payload FROM tg.raw_update WHERE update_id = 13")
    assert raw["from"] == {"erased": True} and raw["invite_link"]["creator"] == {"erased": True}
    assert "Name9001" not in str(raw)
    # after the erasure: new data
    h.clock.advance(minutes=5)
    h.poll(chat_member(14, 42, T0 + dt.timedelta(minutes=1)))
    assert h.q("SELECT user_id FROM tg.member_event WHERE update_id = 14") == [(42,)]


@pytest.mark.parametrize("change", ["bot", "key"])
def test_identity_guards_refuse_another_bot_or_key_before_writing(make, change):
    h = make().start()
    h.close()
    before = h.q("SELECT * FROM tg.bot_state")
    other = make(key="cd" * 32) if change == "key" else make(fake=FakeBotApi(bot_id=BOT_ID + 1))
    other.conn = psycopg.connect(other.dsn)
    ctx = JobContext(config=other.cfg, api=other.fake.api(), health=metrics.Health(), stop=threading.Event(),
                     clock=other.clock)
    with pytest.raises(StartupError, match="refusing to start"):
        ingest.identify(ctx, other.conn)
    assert h.q("SELECT * FROM tg.bot_state") == before


def test_run_polls_until_the_stop(h):
    tracked_channel(h)
    h.fake.updates([chat_member(10, 42, T0)])
    t = threading.Thread(target=ingest.run, args=(h.ctx, h.conn), daemon=True)
    h.ing.min_empty_poll = 0.0
    t.start()
    deadline = dt.datetime.now() + dt.timedelta(seconds=10)
    while dt.datetime.now() < deadline and not h.q("SELECT 1 FROM tg.member_event"):
        threading.Event().wait(0.05)
    h.ctx.stop.set()
    t.join(5)
    assert not t.is_alive()
    assert h.q("SELECT update_id FROM tg.member_event") == [(10,)]
    assert h.ctx.health.check()[0] == 200


# ------------------------------------------------------------------------------------------------------------
# review round 1 regressions
# ------------------------------------------------------------------------------------------------------------


def erase(h: Harness, *ids: int, at: dt.datetime = T0) -> None:
    with psycopg.connect(h.dsn, autocommit=True) as c:
        for i in ids:
            c.execute("INSERT INTO tg.erasure (user_hash, erased_at) VALUES (tg.erasure_hash(%s), %s)", (i, at))


def test_malformed_payloads_drop_erased_people_and_names_by_position(make):
    h = make(store_names=False).start()
    tracked_channel(h)
    erase(h, ADMIN, 47)
    before = T0 - dt.timedelta(minutes=10)
    # an old member update (bad status) whose actor and link creator are the erased admin, and whose member
    # object lacks is_bot
    bad = chat_member(10, 42, before, actor=ADMIN, link=masked_link())
    bad["chat_member"]["new_chat_member"]["status"] = "banned"
    del bad["chat_member"]["new_chat_member"]["user"]["is_bot"]
    # a request without a usable date: every erasure counts (47 is the subject: no raw at all)
    undated = join_request(11, 47, before)
    undated["chat_join_request"]["date"] = "soon"
    undated_other = join_request(12, 48, before, link=masked_link())
    undated_other["chat_join_request"]["date"] = None
    h.poll(bad, undated, undated_other)
    assert h.q("SELECT update_id, update_type FROM tg.update_receipt WHERE update_id >= 10 ORDER BY 1") == [
        (10, "malformed"), (11, "malformed"), (12, "malformed")]
    raw = dict(h.q("SELECT update_id, payload FROM tg.raw_update WHERE update_id >= 10"))
    assert set(raw) == {10, 12}
    assert raw[10]["from"] == {"erased": True} and raw[10]["invite_link"]["creator"] == {"erased": True}
    assert raw[10]["new_chat_member"]["user"] == {"id": 42, "language_code": "en"}
    assert raw[12]["invite_link"]["creator"] == {"erased": True}
    assert "Name48" not in str(raw[12]) and str(USER_CHAT_ID) not in str(raw[12])


def test_a_link_without_a_creator_or_an_unmasked_foreign_link_is_malformed(h):
    tracked_channel(h)
    no_creator = bot_link()
    del no_creator["creator"]
    foreign_full = dict(masked_link(), invite_link="https://t.me/+FOREIGNfulltoken")
    h.poll(chat_member(10, 42, T0, link=no_creator), chat_member(11, 43, T0, link=foreign_full),
           chat_member(12, 44, T0))
    assert h.q("SELECT update_id, update_type FROM tg.update_receipt WHERE update_id >= 10 ORDER BY 1") == [
        (10, "malformed"), (11, "malformed"), (12, "chat_member")]
    assert h.q("SELECT count(*) FROM tg.invite_link") == [(0,)]
    dump = str(h.q("SELECT payload FROM tg.raw_update"))
    assert BOT_LINK not in dump and "FOREIGNfulltoken" not in dump
    assert h.q("SELECT update_id FROM tg.member_event") == [(12,)]


def test_a_program_error_in_a_body_rolls_back_and_propagates(h, monkeypatch):
    tracked_channel(h)

    def broken(conn, pairs):
        raise RuntimeError("rebuild bug")

    monkeypatch.setattr(history, "rebuild_pairs", broken)
    with pytest.raises(RuntimeError, match="rebuild bug"):
        h.poll(chat_member(10, 42, T0))
    assert h.q("SELECT count(*) FROM tg.update_receipt WHERE update_id = 10") == [(0,)]
    assert h.q("SELECT count(*) FROM tg.member_event") == [(0,)]
    assert h.one("SELECT next_offset FROM tg.bot_state") == 2
    monkeypatch.undo()
    h.restart()
    h.poll(chat_member(10, 42, T0))
    assert h.q("SELECT update_id FROM tg.member_event") == [(10,)]
    assert h.q("SELECT update_type FROM tg.update_receipt WHERE update_id = 10") == [("chat_member",)]


def test_ids_outside_bigint_are_malformed_not_a_database_wedge(h):
    tracked_channel(h)
    huge_chat = chat_member(10, 42, T0, chat=dict(CHANNEL, id=-(2 ** 70)))
    huge_user = chat_member(11, 2 ** 64, T0)
    h.poll(huge_chat, huge_user, chat_member(12, 44, T0))
    assert h.q("SELECT update_id, update_type, chat_id FROM tg.update_receipt WHERE update_id >= 10 ORDER BY 1") == [
        (10, "malformed", None), (11, "malformed", CHANNEL["id"]), (12, "chat_member", CHANNEL["id"])]
    assert h.q("SELECT update_id FROM tg.member_event") == [(12,)]
    assert h.one("SELECT next_offset FROM tg.bot_state") == 13


def window_flags(h: Harness, chat: int, lo: dt.datetime, hi: dt.datetime):
    return h.one("SELECT tg.window_flags(%s, tstzrange(%s, %s))", chat, lo, hi)


@pytest.mark.parametrize("crash_between", [False, True])
def test_queued_rights_changes_replay_at_their_times_after_the_startup_check(h, crash_between):
    start = T0 - dt.timedelta(hours=2)
    tracked_channel(h, at=start)
    e1, e2 = T0 + dt.timedelta(minutes=1), T0 + dt.timedelta(minutes=3)
    # the service stops; meanwhile the bot loses admin (e1) and gets it back (e2); the restart checks at T0+6m
    h.clock.advance(minutes=6)
    check = h.clock.now
    h.fake.ok("getChatMember", member(BOT_ID, "administrator", **admin_rights()))
    h.restart()
    demoted = my_chat_member(2, e1, "member", "administrator")
    promoted = my_chat_member(3, e2, "administrator", "member")
    if crash_between:
        h.ing.before_commit = lambda update_id: (_ for _ in ()).throw(Crash()) if update_id == 3 else None
        with pytest.raises(Crash):
            h.poll(demoted, promoted)
        h.clock.advance(minutes=1)
        h.fake.ok("getChatMember", member(BOT_ID, "administrator", **admin_rights()))
        h.restart()
        h.poll(promoted)
    else:
        h.poll(demoted, promoted)
    assert h.q("SELECT lower(period), upper(period) FROM tg.coverage ORDER BY lower(period)") == [
        (start, e1), (e2, None)]
    assert h.q("SELECT reason, confirmed, lower(period), upper(period) FROM tg.gap") == [
        ("not_admin", True, e1, e2)]
    assert window_flags(h, CHANNEL["id"], e1, e2) is not None
    assert window_flags(h, CHANNEL["id"], e2, check) is None
    assert h.one("SELECT bot_status, rights_checked_at >= %s FROM tg.chat", check) == ("administrator", True)


def test_a_promotion_and_demotion_while_stopped_replay_after_a_not_admin_check(h):
    start = T0 - dt.timedelta(hours=2)
    removed = T0 - dt.timedelta(hours=1)
    tracked_channel(h, at=start)
    h.poll(my_chat_member(2, removed, "member", "administrator"))
    e1, e2 = T0 + dt.timedelta(minutes=1), T0 + dt.timedelta(minutes=3)
    h.clock.advance(minutes=6)
    check = h.clock.now
    h.fake.ok("getChatMember", member(BOT_ID, "member"))
    h.restart()
    h.poll(my_chat_member(3, e1, "administrator", "member"), my_chat_member(4, e2, "member", "administrator"))
    assert h.q("SELECT lower(period), upper(period) FROM tg.coverage ORDER BY lower(period)") == [
        (start, removed), (e1, e2)]
    assert h.q("SELECT reason, lower(period), upper(period) FROM tg.gap ORDER BY lower(period)") == [
        ("not_admin", removed, e1), ("not_admin", e2, None)]
    assert h.one("SELECT bot_status, rights_checked_at FROM tg.chat") == ("member", check)


def test_a_failed_startup_check_is_retried_and_doubted_until_resolved(make):
    h = make().start()
    tracked_channel(h)
    h.clock.advance(hours=25)  # an outage: the bot was removed meanwhile, the update is gone
    h.fake.error("getChatMember", 500, "Internal Server Error")
    h.restart()
    failed_at = h.clock.now
    assert h.one("SELECT bot_status, rights_attempts, rights_due_at > %s FROM tg.chat", failed_at) == (
        "administrator", 1, True)
    assert h.q("SELECT reason, confirmed, lower(period), upper(period) FROM tg.gap WHERE chat_id IS NOT NULL") == [
        ("not_admin", False, failed_at, None)]
    h.poll()
    assert window_flags(h, CHANNEL["id"], failed_at, failed_at + dt.timedelta(seconds=10)) is not None
    assert h.ing.refresh_rights() == 0  # not due yet
    h.clock.advance(seconds=31)
    h.fake.error("getChatMember", 403, "Forbidden: bot was kicked from the channel chat")
    assert h.ing.refresh_rights() == 1
    resolved = h.clock.now
    assert h.one("SELECT bot_status, rights_due_at, rights_attempts FROM tg.chat") == ("kicked", None, 0)
    assert h.q("""SELECT reason, confirmed, lower(period), upper(period) FROM tg.gap WHERE chat_id IS NOT NULL
                  ORDER BY lower(period)""") == [
        ("not_admin", False, failed_at, resolved), ("not_admin", True, resolved, None)]
    assert h.q("SELECT upper(period) FROM tg.coverage") == [(resolved,)]


def test_a_previous_list_without_chat_member_records_updates_not_allowed(make):
    h = make().start()
    tracked_channel(h, at=T0 - dt.timedelta(days=3))
    with psycopg.connect(h.dsn, autocommit=True) as c:
        c.execute("UPDATE tg.bot_state SET allowed_updates = '{message,my_chat_member}'")
    h.clock.advance(hours=1)
    h.restart()
    assert h.q("SELECT chat_id, reason, lower(period), upper(period) FROM tg.gap "
               "WHERE reason = 'updates_not_allowed'") == [(None, "updates_not_allowed", T0 - dt.timedelta(days=3), None)]
    assert h.one("SELECT allowed_updates FROM tg.bot_state") == list(ingest.ALLOWED_UPDATES)
    h.clock.advance(seconds=5)
    h.poll()
    assert h.q("SELECT upper(period) FROM tg.gap WHERE reason = 'updates_not_allowed'") == [(h.clock.now,)]
    h.restart()  # the full list is stored now: no new gap
    h.poll()
    assert h.q("SELECT count(*) FROM tg.gap WHERE reason = 'updates_not_allowed'") == [(1,)]


def test_m2_restart_never_checks_the_old_group_and_m1_moves_no_open_gap(h):
    t = T0 - dt.timedelta(hours=5)
    tracked_channel(h, upd=1, chat=BASIC, at=t)
    mig = T0 - dt.timedelta(hours=2)
    h.poll(migrate_to(2, mig))
    # the bot was promoted in the supergroup before the restart (its update is still queued)
    h.fake.respond("getChatMember", lambda params: member(BOT_ID, "administrator", **admin_rights()))
    h.restart()
    assert BASIC["id"] not in [c.params["chat_id"] for c in h.fake.calls_of("getChatMember")]
    assert h.one("SELECT rights_due_at, metadata_due_at FROM tg.chat WHERE chat_id = %s", NEW_SG["id"])[0] is None
    h.poll(my_chat_member(3, T0 - dt.timedelta(minutes=30), chat=NEW_SG))
    assert h.q("SELECT chat_id FROM tg.chat") == [(NEW_SG["id"],)]
    assert h.q("SELECT count(*) FROM tg.gap WHERE upper_inf(period)") == [(0,)]
    assert h.q("SELECT lower(period), upper(period) FROM tg.coverage ORDER BY 1") == [
        (t, mig), (T0 - dt.timedelta(minutes=30), None)]


def test_an_erased_sender_keeps_chat_level_evidence(h):
    t = T0 - dt.timedelta(hours=5)
    tracked_channel(h, upd=1, chat=BASIC, at=t)
    tracked_channel(h, upd=2, chat=GROUP, at=t)
    erase(h, ADMIN)
    before = T0 - dt.timedelta(hours=1)
    draw = T0 + dt.timedelta(days=1)
    h.poll(migrate_to(3, before),
           message(4, before, chat=GROUP, uid=ADMIN, mid=8, text=TEXT,
                   giveaway={"chats": [GROUP], "winners_selection_date": ts(draw), "winner_count": 1}),
           message(5, before, chat=GROUP, uid=ADMIN, mid=9, text=TEXT))
    assert h.q("SELECT old_chat_id, chat_id FROM tg.chat_alias") == [(BASIC["id"], NEW_SG["id"])]
    assert h.q("SELECT post_chat_id, post_message_id FROM tg.campaign") == [(GROUP["id"], 8)]
    assert h.q("SELECT count(*) FROM tg.activity_daily") == [(0,)]
    assert h.q("SELECT count(*) FROM tg.tg_user") == [(0,)]


def test_move_chat_history_keeps_what_only_the_source_rows_know(migrated_dsn):
    old, new = -400002, -1004000000002
    a, b, c = (T0 - dt.timedelta(days=n) for n in (10, 5, 1))
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute("SELECT tg.ensure_partitions('member_event', 'month', %s, %s)", (a, T0))
        conn.execute("INSERT INTO tg.chat (chat_id, chat_type, title, tracked) VALUES (%s, 'group', 'o', true), "
                     "(%s, 'supergroup', 'n', true)", (old, new))
        conn.execute("INSERT INTO tg.tg_user (user_id, first_seen_at, last_seen_at) VALUES (1, %s, %s)", (a, a))
        camp = conn.execute("""INSERT INTO tg.campaign (name, kind, origin, starts_at, draw_at)
                               VALUES ('owner', 'giveaway', 'annotation', %s, %s) RETURNING campaign_id""",
                            (a, b)).fetchone()[0]
        conn.execute("""INSERT INTO tg.invite_link (chat_id, masked_link, creator_id, name, campaign_id)
                        VALUES (%s, 'm…', 7, 'owner label', %s), (%s, 'm…', 7, NULL, NULL)""", (old, camp, new))
        conn.execute("""INSERT INTO tg.join_request (chat_id, user_id, requested_at, outcome, resolved_at, resolved_by)
                        VALUES (%s, 1, %s, 'approved', %s, 7), (%s, 1, %s, NULL, NULL, NULL)""", (old, b, c, new, b))
        # rollup inputs older than any event: a count sample and activity of the old id
        conn.execute("INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, net_events) "
                     "VALUES (%s, %s, 10, 9, 0)", (old, a))
        conn.execute("INSERT INTO tg.activity_daily (chat_id, user_id, day, messages) VALUES (%s, 1, %s, 2)",
                     (old, (a + dt.timedelta(days=1)).date()))
        conn.execute("""INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status,
                                                     actor_is_other) VALUES (%s, 1, %s, 1, 'left', 'member', false)""",
                     (old, b))
        conn.execute("INSERT INTO tg.rollup_dirty (chat_id, from_day) VALUES (%s, %s)", (new, c.date()))
        conn.execute("SELECT tg.move_chat_history(%s, %s, %s)", (old, new, c))
        assert conn.execute("SELECT chat_id, name, campaign_id FROM tg.invite_link").fetchall() == [
            (new, "owner label", camp)]
        assert conn.execute("SELECT chat_id, outcome, resolved_at, resolved_by FROM tg.join_request").fetchall() == [
            (new, "approved", c, 7)]
        assert conn.execute("SELECT from_day FROM tg.rollup_dirty WHERE chat_id = %s", (new,)).fetchone()[0] == a.date()
        conn.rollback()


# ------------------------------------------------------------------------------------------------------------
# review round 2 regressions
# ------------------------------------------------------------------------------------------------------------


def test_updates_not_allowed_closes_through_the_serve_entry_points(make):
    h = make().start()
    tracked_channel(h, at=T0 - dt.timedelta(hours=1))
    with psycopg.connect(h.dsn, autocommit=True) as c:
        c.execute("UPDATE tg.bot_state SET allowed_updates = '{message}'")
    h.close()
    conn = psycopg.connect(h.dsn)
    ctx = JobContext(config=h.cfg, api=h.fake.api(), health=metrics.Health(), stop=threading.Event(), clock=h.clock)
    ingest.identify(ctx, conn)
    ctx.generation = db.claim_generation(conn)
    ingest.startup(ctx, conn)  # one Ingest instance ...
    assert h.q("SELECT upper_inf(period) FROM tg.gap WHERE reason = 'updates_not_allowed'") == [(True,)]
    t = threading.Thread(target=ingest.run, args=(ctx, conn), daemon=True)  # ... and another one polls
    t.start()
    deadline = dt.datetime.now() + dt.timedelta(seconds=10)
    while dt.datetime.now() < deadline and h.q("SELECT 1 FROM tg.gap WHERE reason = 'updates_not_allowed' "
                                               "AND upper_inf(period)"):
        threading.Event().wait(0.05)
    ctx.stop.set()
    t.join(5)
    conn.close()
    assert h.q("SELECT upper_inf(period) FROM tg.gap WHERE reason = 'updates_not_allowed'") == [(False,)]


def test_unhashable_ids_and_a_scalar_invite_link_in_malformed_payloads(h):
    tracked_channel(h)
    odd_from = chat_member(10, 42, T0)
    odd_from["chat_member"]["from"]["id"] = [1, 2]
    odd_user = chat_member(11, 43, T0)
    odd_user["chat_member"]["new_chat_member"]["user"]["id"] = {"x": 1}
    scalar_link = chat_member(12, 44, T0)
    scalar_link["chat_member"]["invite_link"] = BOT_LINK
    h.poll(odd_from, odd_user, scalar_link, chat_member(13, 45, T0))
    assert h.q("SELECT update_id, update_type FROM tg.update_receipt WHERE update_id >= 10 ORDER BY 1") == [
        (10, "malformed"), (11, "malformed"), (12, "malformed"), (13, "chat_member")]
    assert h.q("SELECT update_id FROM tg.member_event") == [(13,)]
    raw = h.one("SELECT payload FROM tg.raw_update WHERE update_id = 12")
    assert raw["invite_link"] == ingest.DIGEST_MARK + digest(BOT_LINK).hex()
    for table in ("tg.raw_update", "tg.invite_link", "tg.update_receipt", "tg.member_event"):
        assert not [r for (r,) in h.q(f"SELECT t::text FROM {table} t") if "BOTLINKfulltoken" in r], table


def test_an_event_older_than_a_failed_check_keeps_the_retry(make):
    h = make().start()
    tracked_channel(h, at=T0 - dt.timedelta(hours=1))
    h.clock.advance(hours=1)
    h.fake.error("getChatMember", 500, "Internal Server Error")
    h.restart()
    failed_at = h.clock.now
    # a queued rights change from before the failed check: newer than the last good check, older than the failure
    h.poll(my_chat_member(2, failed_at - dt.timedelta(minutes=10), "administrator", "administrator",
                          can_change_info=False))
    assert h.one("SELECT rights_due_at IS NOT NULL, rights_attempts, rights_failed_at FROM tg.chat") == (
        True, 1, failed_at)
    assert h.q("SELECT confirmed, upper_inf(period) FROM tg.gap WHERE chat_id IS NOT NULL") == [(False, True)]
    # an event from the failure on resolves it
    h.clock.advance(seconds=5)
    h.poll(my_chat_member(3, h.clock.now, "administrator", "administrator"))
    assert h.one("SELECT rights_due_at, rights_attempts, rights_failed_at FROM tg.chat") == (None, 0, None)
    assert h.q("SELECT confirmed, upper(period) FROM tg.gap WHERE chat_id IS NOT NULL") == [(False, h.clock.now)]


@pytest.mark.parametrize("crash", [False, True])
def test_moving_a_gap_start_earlier_restarts_its_g1_progress(make, crash):
    h = make().start()
    tracked_channel(h, at=T0 - dt.timedelta(hours=1))
    h.clock.advance(minutes=6)
    check = h.clock.now
    h.fake.ok("getChatMember", member(BOT_ID, "member"))
    h.restart()
    gap_id, lower = h.one("SELECT gap_id, lower(period) FROM tg.gap WHERE reason = 'not_admin'")
    assert lower == check
    with psycopg.connect(h.dsn, autocommit=True) as c:
        c.execute("INSERT INTO tg.gap_progress (gap_id, chat_id, last_user_id, done) VALUES (%s, %s, 100, true)",
                  (gap_id, CHANNEL["id"]))
    demoted = my_chat_member(2, T0 + dt.timedelta(minutes=1), "member", "administrator")
    if crash:
        h.ing.before_commit = lambda update_id: (_ for _ in ()).throw(Crash())
        with pytest.raises(Crash):
            h.poll(demoted)
        assert h.one("SELECT lower(period) FROM tg.gap WHERE gap_id = %s", gap_id) == check
        assert h.q("SELECT done FROM tg.gap_progress") == [(True,)]
        h.ing.before_commit = None
    h.poll(demoted)
    assert h.one("SELECT lower(period) FROM tg.gap WHERE gap_id = %s", gap_id) == T0 + dt.timedelta(minutes=1)
    assert h.q("SELECT count(*) FROM tg.gap_progress") == [(0,)]


@pytest.mark.parametrize("collector_row", [False, True])
def test_a_migration_queues_the_destinations_rights_check_and_m1_follows(h, collector_row):
    tracked_channel(h, upd=1, chat=BASIC, at=T0 - dt.timedelta(hours=5))
    h.poll(chat_member(2, 42, T0 - dt.timedelta(hours=4), chat=BASIC))
    if collector_row:
        with psycopg.connect(h.dsn, autocommit=True) as c:
            c.execute("INSERT INTO tg.chat (chat_id, chat_type, title) VALUES (%s, 'supergroup', 'from collect')",
                      (NEW_SG["id"],))
    h.poll(migrate_to(3, T0 - dt.timedelta(hours=2)))
    assert h.one("SELECT moved_at FROM tg.chat_alias") is None
    assert h.one("SELECT rights_due_at IS NOT NULL FROM tg.chat WHERE chat_id = %s", NEW_SG["id"]) is True
    # the promotion update was lost; the check finds the bot admin there
    h.fake.ok("getChatMember", member(BOT_ID, "administrator", **admin_rights()))
    assert h.ing.refresh_rights() == 1
    assert h.one("SELECT moved_at IS NOT NULL FROM tg.chat_alias") is True
    assert h.q("SELECT chat_id, source_chat_id FROM tg.member_event") == [(NEW_SG["id"], BASIC["id"])]


def test_a_self_join_after_the_link_stopped_asking_for_approval_leaves_the_request(h):
    tracked_channel(h)
    approval_link = dict(masked_link(), name="approval link", creates_join_request=True)
    h.poll(join_request(10, 43, T0 - dt.timedelta(hours=2), link=approval_link))
    edited = dict(approval_link, creates_join_request=False)
    h.poll(chat_member(11, 43, T0 - dt.timedelta(hours=1), link=edited))  # a self-join: from = the user
    assert h.q("SELECT outcome FROM tg.join_request") == [(None,)]


def test_update_ids_at_and_past_the_int4_limit_commit(h):
    tracked_channel(h)
    h.poll(chat_member(2 ** 31 - 1, 42, T0))
    assert h.one("SELECT next_offset, last_update_id FROM tg.bot_state") == (2 ** 31, 2 ** 31 - 1)
    h.poll(chat_member(2 ** 62, 43, T0))
    assert h.one("SELECT next_offset FROM tg.bot_state") == 2 ** 62 + 1
    assert h.q("SELECT count(*) FROM tg.member_event") == [(2,)]


def test_a_migration_to_the_same_id_is_malformed(h):
    tracked_channel(h, chat=BASIC)
    h.poll(message(10, T0, chat=BASIC, uid=ADMIN, mid=91, migrate_to_chat_id=BASIC["id"]),
           chat_member(11, 42, T0, chat=BASIC))
    assert h.q("SELECT update_id, update_type FROM tg.update_receipt WHERE update_id >= 10 ORDER BY 1") == [
        (10, "malformed"), (11, "chat_member")]
    assert h.q("SELECT count(*) FROM tg.chat_alias") == [(0,)]
    assert h.q("SELECT update_id FROM tg.member_event") == [(11,)]


def test_move_chat_history_marks_utc_days_whatever_the_session_time_zone(migrated_dsn):
    old, new = -400003, -1004000000003
    late = dt.datetime(2026, 11, 10, 23, 45, tzinfo=dt.UTC)  # 00:45 on the 11th in Belgrade
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute("SET TIME ZONE 'Europe/Belgrade'")
        conn.execute("INSERT INTO tg.chat (chat_id, chat_type, title, tracked) VALUES (%s, 'group', 'o', true), "
                     "(%s, 'supergroup', 'n', true)", (old, new))
        conn.execute("INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, net_events) "
                     "VALUES (%s, %s, 10, 9, 0)", (old, late))
        conn.execute("SELECT tg.move_chat_history(%s, %s, %s)", (old, new, late + dt.timedelta(days=5)))
        assert conn.execute("SELECT from_day FROM tg.rollup_dirty WHERE chat_id = %s", (new,)).fetchone()[0] == (
            dt.date(2026, 11, 10))
        conn.rollback()


# ------------------------------------------------------------------------------------------------------------
# review round 3 regressions
# ------------------------------------------------------------------------------------------------------------


def test_an_unreadable_update_id_ends_the_response_there_and_is_polled_again(h):
    tracked_channel(h)
    caught_up = h.one("SELECT caught_up_at FROM tg.bot_state")
    failures = sample("tgaud_ingest_failures_total", stage="parse")
    unreadable = chat_member(11, 43, T0)
    unreadable["update_id"] = "eleven"
    h.clock.advance(minutes=1)
    # one short response: a join, the unreadable entry, a valid join after it
    assert h.poll(chat_member(10, 42, T0), unreadable, chat_member(12, 44, T0)) == 1
    assert h.q("SELECT update_id FROM tg.member_event") == [(10,)]
    assert h.one("SELECT next_offset, caught_up_at FROM tg.bot_state") == (11, caught_up)
    assert h.q("SELECT count(*) FROM tg.update_receipt WHERE update_id > 10") == [(0,)]
    assert sample("tgaud_ingest_failures_total", stage="parse") == failures + 1
    # Telegram sends it again from the same offset: still nothing past it
    h.clock.advance(minutes=1)
    assert h.poll(unreadable, chat_member(12, 44, T0)) == 0
    assert h.fake.calls_of("getUpdates")[-1].params["offset"] == 11
    assert h.one("SELECT next_offset, caught_up_at FROM tg.bot_state") == (11, caught_up)
    assert sample("tgaud_ingest_failures_total", stage="parse") == failures + 2
    # once the response reads, it commits and the watermark moves
    h.clock.advance(minutes=1)
    sent = h.clock.now
    assert h.poll(chat_member(12, 44, T0)) == 1
    assert h.one("SELECT next_offset, caught_up_at FROM tg.bot_state") == (13, sent)
    assert h.q("SELECT update_id FROM tg.member_event ORDER BY 1") == [(10,), (12,)]


def without_is_bot(uid: int) -> dict:
    u = user(uid)
    del u["is_bot"]
    return u


def test_unexpected_shapes_at_sensitive_positions_are_dropped_from_malformed_payloads(make):
    h = make(store_names=False).start()
    tracked_channel(h)
    link_in_array = chat_member(10, 42, T0, link=[bot_link()])
    from_in_array = chat_member(11, 43, T0, actor=ADMIN)
    from_in_array["chat_member"]["from"] = [without_is_bot(ADMIN)]
    from_scalar = join_request(12, 44, T0)
    from_scalar["chat_join_request"]["from"] = "Name44 Surname44"
    user_in_array = chat_member(13, 45, T0)
    user_in_array["chat_member"]["new_chat_member"]["user"] = [without_is_bot(45)]
    text_in_array = chat_member(14, 46, T0, link=dict(bot_link(), invite_link=[BOT_LINK]))
    # containers nested inside a link or a member, which hold only scalars besides `creator` and `user`
    text_in_object = chat_member(15, 47, T0, link=dict(bot_link(), invite_link={"url": BOT_LINK}))
    person_in_member = chat_member(16, 48, T0, "left", "banned", new_kw={"extra": [without_is_bot(49)]})
    # an object at invite_link without the link's text is not a ChatInviteLink: dropped whole
    link_without_text = chat_member(17, 51, T0, link={"url": BOT_LINK, "creator": BOT_USER, "name": "x"})
    # a valid join whose link carries a field a ChatInviteLink does not have: the raw payload drops it
    extra_field = chat_member(18, 50, T0, link=dict(masked_link(), url=BOT_LINK))
    h.poll(link_in_array, from_in_array, from_scalar, user_in_array, text_in_array, text_in_object,
           person_in_member, link_without_text, extra_field)
    assert h.q("SELECT update_id, update_type FROM tg.update_receipt WHERE update_id >= 10 ORDER BY 1") == [
        *((u, "malformed") for u in range(10, 18)), (18, "chat_member")]
    raw = dict(h.q("SELECT update_id, payload FROM tg.raw_update WHERE update_id >= 10"))
    assert set(raw) == set(range(10, 19))
    assert "invite_link" not in raw[10] and "from" not in raw[11] and "from" not in raw[12]
    assert "user" not in raw[13]["new_chat_member"] and "extra" not in raw[16]["new_chat_member"]
    assert all("invite_link" not in raw[u] for u in (14, 15, 17))  # no link text: not a ChatInviteLink
    assert raw[18]["invite_link"] == {k: v for k, v in masked_link().items() if k != "creator"} | {
        "creator": {"id": ADMIN, "is_bot": False, "language_code": "en"}}
    tables = h.q("""SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname IN ('tg', 'stats') AND c.relkind IN ('r', 'p', 'm') AND NOT c.relispartition
                      AND (c.relkind <> 'm' OR c.relispopulated)""")
    secrets = [BOT_LINK, "BOTLINKfulltoken", "Name9001", "Surname9001", "handle9001", "Name44", "Name45",
               "Surname45", "handle45", "Name49", "handle49"]
    found = []
    for (table,) in tables:
        for (row,) in h.q(f"SELECT t::text FROM {table} t"):
            found += [(table, s) for s in secrets if s in row]
    assert found == []
    assert h.q("SELECT update_id FROM tg.member_event") == [(18,)]


def surrogate_and_nul(bad: dict) -> None:
    bad["chat_member"]["new_chat_member"]["status"] = "banned"
    bad["chat_member"]["new_chat_member"]["custom_title"] = "x\ud800y\x00z"  # sent as the escapes \ud800, \u0000
    bad["chat_member"]["\udc00"] = True


def nan_expiry(bad: dict) -> None:
    # the kick's only invalid field is its expiry (json.dumps sends NaN and Infinity, as json.loads reads them);
    # read as null it would be a valid kick without expiry
    bad["chat_member"]["new_chat_member"]["until_date"] = float("nan")
    bad["chat_member"]["extra"] = float("-inf")  # a field nothing reads


def infinity_date(bad: dict) -> None:
    bad["chat_member"]["date"] = float("inf")


STORED_AS = {
    surrogate_and_nul: lambda p: p["new_chat_member"]["custom_title"] == "x\ufffdy\ufffdz" and p["\ufffd"] is True,
    nan_expiry: lambda p: p["new_chat_member"]["until_date"] is None and p["extra"] is None,
    infinity_date: lambda p: p["date"] is None,
}


@pytest.mark.parametrize("spoil", list(STORED_AS))
def test_values_postgresql_cannot_store_in_a_malformed_update_commit_once(h, spoil):
    tracked_channel(h)
    bad = chat_member(10, 42, T0, "member", "kicked")
    spoil(bad)
    db_failures = sample("tgaud_ingest_failures_total", stage="db")
    attempts = []

    def fail_once(update_id):  # a genuine database failure in the malformed path still rolls back and retries
        if update_id == 10 and not attempts:
            attempts.append(1)
            h.conn.execute("SELECT 1/0")

    h.ing.before_commit = fail_once
    guard = threading.Timer(15, h.ctx.stop.set)  # a wedge ends the poll (and fails the test) instead of hanging
    guard.start()
    try:
        assert h.poll(bad, chat_member(11, 43, T0)) == 2
    finally:
        guard.cancel()
    assert sample("tgaud_ingest_failures_total", stage="db") == db_failures + 1
    assert h.q("SELECT update_id, update_type FROM tg.update_receipt WHERE update_id >= 10 ORDER BY 1") == [
        (10, "malformed"), (11, "chat_member")]
    assert STORED_AS[spoil](h.one("SELECT payload FROM tg.raw_update WHERE update_id = 10"))
    assert h.q("SELECT update_id FROM tg.member_event") == [(11,)]
    assert h.one("SELECT next_offset FROM tg.bot_state") == 12


@pytest.mark.parametrize("trigger", ["restart", "chat_member", "my_chat_member", "erased_subject", "malformed",
                                     "my_chat_member_admin"])
def test_an_m2_destination_once_checked_as_not_admin_is_checked_again(make, trigger):
    h = make().start()
    tracked_channel(h, upd=1, chat=BASIC, at=T0 - dt.timedelta(hours=5))
    h.poll(chat_member(2, 42, T0 - dt.timedelta(hours=4), chat=BASIC))
    h.poll(migrate_to(3, T0 - dt.timedelta(hours=2)))
    h.fake.ok("getChatMember", member(BOT_ID, "member"))
    assert h.ing.refresh_rights() == 1  # the destination's check: the bot is not admin there yet
    checked = h.clock.now
    assert h.one("SELECT tracked, bot_status, rights_due_at FROM tg.chat WHERE chat_id = %s", NEW_SG["id"]) == (
        False, "member", None)
    # the bot is promoted there and that update is lost; a check now finds it admin
    h.fake.respond("getChatMember", lambda params: member(BOT_ID, "administrator", **admin_rights()))
    if trigger == "restart":
        h.clock.advance(hours=26)  # the outage the promotion update expired in
        h.restart()
        assert BASIC["id"] not in [c.params["chat_id"] for c in h.fake.calls_of("getChatMember")]
    elif trigger == "my_chat_member_admin":
        # news of the promotion itself: M1 runs in its transaction and no check is left due
        h.clock.advance(minutes=10)
        h.poll(my_chat_member(4, h.clock.now - dt.timedelta(minutes=1), chat=NEW_SG))
        assert h.one("SELECT rights_due_at FROM tg.chat WHERE chat_id = %s", NEW_SG["id"]) is None
    else:
        h.clock.advance(minutes=10)
        at = h.clock.now - dt.timedelta(minutes=1)
        news = chat_member(4, 43, at, chat=NEW_SG)
        if trigger == "my_chat_member":  # queued from before the first check, so it says nothing about now
            news = my_chat_member(4, checked - dt.timedelta(minutes=1), "member", "left", chat=NEW_SG)
        elif trigger == "erased_subject":  # dated before the person's erasure: a receipt only
            erase(h, 43, at=h.clock.now)
        elif trigger == "malformed":  # a valid chat id, an unreadable date
            news["chat_member"]["date"] = "soon"
        # the check is queued in the update's transaction: a crash before its commit queues nothing
        h.ing.before_commit = lambda update_id: (_ for _ in ()).throw(Crash()) if update_id == 4 else None
        with pytest.raises(Crash):
            h.poll(news)
        assert h.q("SELECT count(*) FROM tg.update_receipt WHERE update_id = 4") == [(0,)]
        assert h.one("SELECT rights_due_at FROM tg.chat WHERE chat_id = %s", NEW_SG["id"]) is None
        h.ing.before_commit = None
        assert h.poll(news) == 1
        assert h.one("SELECT moved_at FROM tg.chat_alias") is None
        assert h.ing.refresh_rights() == 1
    assert h.one("SELECT moved_at IS NOT NULL FROM tg.chat_alias") is True
    assert h.one("SELECT tracked FROM tg.chat WHERE chat_id = %s", NEW_SG["id"]) is True
    assert h.one("SELECT chat_id, source_chat_id FROM tg.member_event WHERE update_id = 2") == (
        NEW_SG["id"], BASIC["id"])
    assert h.q("SELECT count(*) FROM tg.member_event WHERE chat_id = %s", BASIC["id"]) == [(0,)]
    assert h.ing.refresh_rights() == 0  # nothing left due: M1 ran once
    if trigger in ("erased_subject", "malformed"):  # each path keeps only its usual rows (erased: the receipt)
        assert h.q("SELECT update_type, chat_id FROM tg.update_receipt WHERE update_id = 4") == [
            ("chat_member" if trigger == "erased_subject" else "malformed", NEW_SG["id"])]
        for table in ("tg.member_event", "tg.tg_user"):
            assert h.q(f"SELECT count(*) FROM {table} WHERE user_id = 43") == [(0,)], table
        assert h.q("SELECT count(*) FROM tg.raw_update WHERE update_id = 4") == [(0 if trigger == "erased_subject"
                                                                                  else 1,)]
