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

from tgaud import config, db, metrics, sql
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


def test_short_stays_are_counted_for_exits_within_48_hours_of_an_exact_join(h):
    tracked_channel(h)
    chat = str(CHANNEL["id"])
    before = sample("tgaud_chat_short_stays_total", chat=chat)
    h.poll(chat_member(10, 42, T0 - dt.timedelta(hours=30)), chat_member(11, 42, T0 - dt.timedelta(hours=2), "member", "left"),
           chat_member(12, 43, T0 - dt.timedelta(hours=1), "member", "left"))  # 43: no join seen
    assert sample("tgaud_chat_short_stays_total", chat=chat) == before + 1
    assert sample("tgaud_chat_events_total", chat=chat, kind="leave") >= 2


def test_join_request_then_its_approval(h):
    tracked_channel(h)
    req_at = T0 - dt.timedelta(hours=2)
    h.poll(join_request(10, 42, req_at, link=masked_link()))
    jr = h.one("SELECT chat_id, user_id, requested_at, update_id, outcome, invite_link_id IS NOT NULL FROM tg.join_request")
    assert jr == (CHANNEL["id"], 42, req_at, 10, None, True)
    raw = h.one("SELECT payload FROM tg.raw_update WHERE update_id = 10")
    assert "bio" not in raw and "user_chat_id" not in raw
    approved = T0 - dt.timedelta(hours=1)
    h.poll(chat_member(11, 42, approved, actor=ADMIN, via_join_request=True, link=masked_link()))
    assert h.one("SELECT outcome, resolved_at, resolved_by FROM tg.join_request") == ("approved", approved, ADMIN)
    assert h.one("SELECT via_join_request, actor_is_other FROM tg.member_event") == (True, True)


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
