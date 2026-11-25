"""The snapshot contract fields of `tgstats collect` (docs/design/design.md 5) and old files.

A collection records its random id, started_at before the first count read, Telegram's count before and after the
listing, the path it took, finished_at, and every member as the first page that listed them saw them. A kept
listing that saw an error is incomplete whatever the counts say. Files of older versions migrate on open and read
back as before.
"""

import asyncio
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fakes import NOW, FakeClient, basic_group, channel, tg_user
from telethon import errors
from telethon.tl import types
from test_collector import Recorder, run

from tgstats import analysis, collector, db

CHAT1, CHAT2 = -1000000000001, -1000000000002
T0 = datetime(2026, 11, 20, 9, 0, tzinfo=timezone.utc)


class Ticks:
    """A clock that moves one second per reading, so every page gets its own time."""

    def __init__(self, start=T0):
        self.t = start - timedelta(seconds=1)

    def __call__(self):
        self.t += timedelta(seconds=1)
        return self.t


def collect(client, conn, deep=False, clock=None, **kw):
    rec = Recorder()
    results = run(collector.collect(client, conn, None, deep=deep, sleep=rec.sleep, out=rec.out,
                                    clock=clock or Ticks(), **kw))
    return results, rec


def iso(t):
    return t.isoformat(timespec="microseconds")


OLD_SCHEMA = """
CREATE TABLE chats (id INTEGER PRIMARY KEY, type TEXT NOT NULL, title TEXT NOT NULL, username TEXT);
CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT,
                    is_bot INTEGER NOT NULL DEFAULT 0, is_deleted INTEGER NOT NULL DEFAULT 0);
CREATE TABLE snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL REFERENCES chats(id),
                        taken_at TEXT NOT NULL, fetched INTEGER NOT NULL, reported INTEGER, complete INTEGER NOT NULL);
CREATE TABLE memberships (snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
                          user_id INTEGER NOT NULL REFERENCES users(id), PRIMARY KEY (snapshot_id, user_id))
                          WITHOUT ROWID;
CREATE INDEX snapshots_by_chat ON snapshots(chat_id, id);
INSERT INTO chats VALUES (-1000000000001, 'channel', 'Old', NULL), (-1000000000002, 'channel', 'Other', NULL);
INSERT INTO users VALUES (1, 'u1', 'A', NULL, 0, 0), (2, 'u2', 'B', NULL, 0, 0), (3, NULL, 'C', NULL, 1, 0);
INSERT INTO snapshots (chat_id, taken_at, fetched, reported, complete)
  VALUES (-1000000000001, '2026-11-14T10:00:00+00:00', 3, 3, 1), (-1000000000002, '2026-11-14T11:00:00+00:00', 2, 9, 0);
INSERT INTO memberships VALUES (1, 1), (1, 2), (1, 3), (2, 2), (2, 3);
"""


def test_an_old_file_opens_migrates_and_reads_back(tmp_path):
    path = tmp_path / "old.db"
    raw = sqlite3.connect(path)
    raw.executescript(OLD_SCHEMA)
    raw.close()

    conn = db.connect(path, create=False)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(snapshots)")}
    assert set(db.SNAPSHOT_COLUMNS) <= cols
    assert set(db.MEMBERSHIP_COLUMNS) <= {r[1] for r in conn.execute("PRAGMA table_info(memberships)")}
    latest = db.latest_snapshots(conn)
    assert latest[CHAT1] == db.SnapshotRow(1, CHAT1, "2026-11-14T10:00:00+00:00", 3, 3, True)
    assert latest[CHAT1].uuid is None and latest[CHAT1].started_at is None  # old snapshots: NULLs
    assert all(m.seen_at is None for m in db.observations(conn, 1).values())
    sets = analysis.load_sets(conn)
    assert {s.chat.title: set(s.members) for s in sets} == {"Old": {1, 2}, "Other": {2}}  # bot 3 left out
    assert analysis.overlap_matrix(sets) == [[2, 1], [1, 1]]
    conn.close()
    assert db.migrate(db.connect(path)) == []  # once migrated, opening again changes nothing


def test_a_read_only_old_file_still_reads(tmp_path):
    path = tmp_path / "old.db"
    raw = sqlite3.connect(path)
    raw.executescript(OLD_SCHEMA)
    raw.close()
    path.chmod(0o444)
    try:
        conn = db.connect(path, create=False)
        assert db.latest_snapshots(conn)[CHAT1].fetched == 3
        conn.close()
    finally:
        path.chmod(0o644)


def test_a_new_collect_stores_the_contract_fields_and_each_observation(conn):
    users = [tg_user(1, premium=True, last="L"), tg_user(2, bot=True), tg_user(3, deleted=True)]
    joined = datetime(2025, 5, 1, 8, 30, tzinfo=timezone.utc)
    until = datetime(2027, 1, 1, tzinfo=timezone.utc)
    kinds = {
        1: types.ChannelParticipantSelf(user_id=1, inviter_id=77, date=joined, via_request=True,
                                        subscription_until_date=until),
        2: types.ChannelParticipantAdmin(user_id=2, promoted_by=5, date=joined, admin_rights=types.ChatAdminRights(),
                                         inviter_id=78),
        3: types.ChannelParticipantCreator(user_id=3, admin_rights=types.ChatAdminRights()),
    }
    client = FakeClient([channel(1, "C", megagroup=True)], {1: users},
                        {1: {"page": 2, "reported": [3, 4], "participant": lambda u: kinds[u.id]}})
    results, rec = collect(client, conn)
    snap = db.latest_snapshots(conn)[CHAT1]
    uuid.UUID(snap.uuid)  # a well-formed random id
    # clock readings: start 09:00:00, page 1 at :01, page 2 at :02, the empty page at :03, finish at :04
    assert (snap.started_at, snap.finished_at, snap.taken_at) == (iso(T0), iso(T0 + timedelta(seconds=4)),
                                                                  iso(T0 + timedelta(seconds=4)))
    assert (snap.method, snap.reported_start, snap.reported_end, snap.reported) == ("listing", 3, 4, 4)
    assert (snap.fetched, snap.complete) == (3, False)  # 3 of the larger count 4
    obs = db.observations(conn, snap.id)
    assert obs[1] == db.Member(db.UserRow(1, "user1", "Name1", "L", False, False), iso(T0 + timedelta(seconds=1)),
                               joined.isoformat(), 77, True, until.isoformat(), True)
    assert obs[2] == db.Member(db.UserRow(2, "user2", "Name2", None, True, False), iso(T0 + timedelta(seconds=1)),
                               joined.isoformat(), 78, None, None, False)
    assert obs[3] == db.Member(db.UserRow(3, "user3", "Name3", None, False, True), iso(T0 + timedelta(seconds=2)),
                               None, None, None, None, False)  # a creator has no join date


def test_the_two_counts_are_read_fresh_and_the_larger_decides_completeness(conn):
    users = [tg_user(i) for i in range(1, 11)]
    client = FakeClient([channel(1, "C", megagroup=True)], {1: users}, {1: {"reported": [10, 12]}})
    results, _ = collect(client, conn)
    snap = db.latest_snapshots(conn)[CHAT1]
    assert (snap.reported_start, snap.reported_end, snap.reported, snap.complete) == (10, 12, 12, False)
    assert sum(isinstance(r, collector.GetFullChannelRequest) for r in client.requests) == 2


def test_a_basic_group_reads_both_counts_fresh_not_the_dialog_copy(conn):
    members = [tg_user(1), tg_user(2), tg_user(3)]
    group = basic_group(7, "Small", 40)  # the dialog list's count is stale
    client = FakeClient([group], {7: members}, {7: {"reported": [3, 3]}})
    results, _ = collect(client, conn)
    snap = db.latest_snapshots(conn)[-7]
    assert (snap.method, snap.reported_start, snap.reported_end, snap.complete) == ("basic_group", 3, 3, True)
    assert [type(r).__name__ for r in client.requests] == ["GetFullChatRequest"] * 3  # count, members, count
    obs = db.observations(conn, snap.id)
    assert obs[1].joined_date == NOW.isoformat() and obs[1].inviter_id is None  # inviter 0 = none


def test_a_failed_final_count_keeps_the_listing_incomplete_without_an_end_count(conn):
    users = [tg_user(i) for i in range(1, 5)]
    client = FakeClient([channel(1, "C", megagroup=True)], {1: users},
                        {1: {"reported": [4, errors.RPCError(None, "INTERNAL", 500)]}})
    results, rec = collect(client, conn)
    snap = db.latest_snapshots(conn)[CHAT1]
    assert (snap.fetched, snap.reported_start, snap.reported_end, snap.complete) == (4, 4, None, False)
    assert results[0].error is None and not results[0].complete
    assert "the member count after the listing failed" in rec.text and "a request failed" in rec.text


@pytest.mark.parametrize("start", [1, None], ids=["start-count", "no-start-count"])
def test_an_answer_without_the_final_count_keeps_the_listing_incomplete(conn, start):
    """review 1: a successful GetFullChannel whose optional participants_count is absent is no count either."""
    client = FakeClient([channel(1, "C", megagroup=True)], {1: [tg_user(1)]}, {1: {"reported": [start, None]}})
    results, rec = collect(client, conn)
    snap = db.latest_snapshots(conn)[CHAT1]
    assert (snap.fetched, snap.reported_start, snap.reported_end, snap.complete) == (1, start, None, False)
    assert not results[0].complete and "no member count after the listing" in rec.text


def test_one_user_listed_on_two_pages_keeps_the_first_pages_observation(conn):
    first = tg_user(5, first="Before")
    later = tg_user(5, first="After", premium=True)
    users = [first, tg_user(6), tg_user(7), tg_user(8), later]  # the listing shifted: user 5 again on page 2
    client = FakeClient([channel(1, "C", megagroup=True)], {1: users}, {1: {"page": 3, "reported": 4}})
    collect(client, conn)
    snap = db.latest_snapshots(conn)[CHAT1]
    obs = db.observations(conn, snap.id)
    assert snap.fetched == 4 and set(obs) == {5, 6, 7, 8}
    assert (obs[5].seen_at, obs[5].user.first_name, obs[5].is_premium) == (iso(T0 + timedelta(seconds=1)),
                                                                           "Before", False)
    assert obs[8].seen_at == iso(T0 + timedelta(seconds=2))


def test_a_deep_search_failing_on_a_late_page_after_the_threshold_is_incomplete(conn):
    # the listing returns 90 of 100; the search for 'a' brings all 100 by its second page, and its last page fails
    users = [tg_user(i, first=f"a{i}") for i in range(1, 101)]
    client = FakeClient([channel(1, "C", megagroup=True)], {1: users},
                        {1: {"cap": 90, "page": 50, "reported": 100}},
                        fail_on_pages={("a", 100): errors.RPCError(None, "TIMEOUT", 500)})
    results, rec = collect(client, conn, deep=True)
    snap = db.latest_snapshots(conn)[CHAT1]
    assert snap.fetched == 100 and not db.is_incomplete(snap.fetched, snap.reported)  # the counts say complete
    assert (snap.complete, snap.method, results[0].complete) == (False, "deep", False)
    assert "deep search stopped at 'a'" in rec.text


def test_the_same_user_in_two_chats_keeps_each_chats_own_observation(conn):
    clock = Ticks()
    one = FakeClient([channel(1, "One")], {1: [tg_user(9, first="Early")]})
    collect(one, conn, clock=clock)
    two = FakeClient([channel(2, "Two")], {2: [tg_user(9, first="Late", premium=True)]})
    collect(two, conn, clock=clock)
    latest = db.latest_snapshots(conn)
    a, b = db.observations(conn, latest[CHAT1].id)[9], db.observations(conn, latest[CHAT2].id)[9]
    assert (a.user.first_name, a.is_premium, b.user.first_name, b.is_premium) == ("Early", False, "Late", True)
    assert a.seen_at < b.seen_at
    assert latest[CHAT1].uuid != latest[CHAT2].uuid
    assert conn.execute("SELECT first_name FROM users WHERE id = 9").fetchone()[0] == "Late"  # the latest profile


def test_an_interrupted_collect_stores_the_contract_fields_with_no_end_count(conn):
    users = [tg_user(i, first=f"{'abcd'[i - 1]}1") for i in range(1, 5)]
    client = FakeClient([channel(1, "C", megagroup=True), channel(2, "Next")], {1: users, 2: [tg_user(50)]},
                        {1: {"page": 2}}, interrupt_on_calls={3})
    rec = Recorder()
    with pytest.raises(asyncio.CancelledError):
        run(collector.collect(client, conn, None, sleep=rec.sleep, out=rec.out, clock=Ticks()))
    snap = db.latest_snapshots(conn)[CHAT1]
    assert (snap.fetched, snap.complete, snap.reported_start, snap.reported_end) == (4, False, 4, None)
    assert snap.method == "listing" and snap.started_at == iso(T0) and snap.finished_at == iso(T0 + timedelta(seconds=3))
    assert sorted(m.seen_at for m in db.observations(conn, snap.id).values()) == [iso(T0 + timedelta(seconds=1))] * 2 \
        + [iso(T0 + timedelta(seconds=2))] * 2
    assert CHAT2 not in db.latest_snapshots(conn)


def test_page_times_never_go_back_when_the_wall_clock_steps(conn):
    readings = iter([T0, T0 + timedelta(seconds=5), T0 - timedelta(hours=1), T0 + timedelta(seconds=7),
                     T0 + timedelta(seconds=8)])
    client = FakeClient([channel(1, "C")], {1: [tg_user(i) for i in range(1, 5)]}, {1: {"page": 2}})
    collect(client, conn, clock=lambda: next(readings))
    snap = db.latest_snapshots(conn)[CHAT1]
    seen = sorted({m.seen_at for m in db.observations(conn, snap.id).values()})
    assert seen == [iso(T0 + timedelta(seconds=5))]  # the second page's reading went back: it keeps the first's
    assert snap.started_at <= seen[0] <= snap.finished_at


def test_a_banned_participant_who_left_is_not_a_member(conn):
    left = types.ChannelParticipantBanned(peer=types.PeerUser(2), kicked_by=5, date=NOW,
                                          banned_rights=types.ChatBannedRights(until_date=None), left=True)
    restricted = types.ChannelParticipantBanned(peer=types.PeerUser(3), kicked_by=5, date=NOW,
                                                banned_rights=types.ChatBannedRights(until_date=None))
    parts = {1: types.ChannelParticipant(user_id=1, date=NOW), 2: left, 3: restricted}
    client = FakeClient([channel(1, "C", megagroup=True)], {1: [tg_user(1), tg_user(2), tg_user(3)]},
                        {1: {"participant": lambda u: parts[u.id], "reported": 2}})
    collect(client, conn)
    obs = db.observations(conn, db.latest_snapshots(conn)[CHAT1].id)
    assert set(obs) == {1, 3} and obs[3].joined_date == NOW.isoformat()
