import asyncio

import pytest
from telethon import errors

from fakes import FakeClient, basic_group, channel, tg_user
from tgstats import collector, db


def run(coro):
    return asyncio.run(coro)


class Recorder:
    def __init__(self):
        self.sleeps = []
        self.lines = []

    async def sleep(self, seconds):
        self.sleeps.append(seconds)

    def out(self, line):
        self.lines.append(line)

    @property
    def text(self):
        return "\n".join(self.lines)


def collect(client, conn, refs=None, deep=False):
    rec = Recorder()
    results = run(collector.collect(client, conn, refs, deep=deep, sleep=rec.sleep, out=rec.out))
    return results, rec


def stored_ids(conn, chat_id):
    snap = db.latest_snapshots(conn)[chat_id]
    return set(db.snapshot_members(conn, snap.id, include_bots=True)), snap


def test_lists_only_admin_chats_with_types(conn):
    dialogs = [
        channel(1, "Broadcast"),
        channel(2, "Super", megagroup=True, creator=False, admin=True),
        basic_group(3, "Small", 1),
        channel(4, "Not mine", creator=False),
        tg_user(9),
    ]
    chats = run(collector.list_admin_chats(FakeClient(dialogs, {})))
    assert [(c.id, c.type, c.title) for c in chats] == [
        (-1000000000001, "channel", "Broadcast"),
        (-1000000000002, "supergroup", "Super"),
        (-3, "group", "Small"),
    ]


def test_paginates_until_telegram_returns_nothing(conn):
    users = [tg_user(i) for i in range(1, 8)]
    client = FakeClient([channel(1, "C", megagroup=True)], {1: users}, {1: {"page": 3}})
    results, rec = collect(client, conn)
    assert results[0].fetched == 7 and results[0].error is None
    ids, snap = stored_ids(conn, -1000000000001)
    assert ids == set(range(1, 8))
    # pages of 3, 3, 1 and a final empty page
    assert client.participant_calls == 4
    offsets = [r.offset for r in client.requests if hasattr(r, "offset")]
    assert offsets == [0, 3, 6, 7]


def test_records_fetched_versus_reported(conn):
    users = [tg_user(i) for i in range(1, 11)]
    client = FakeClient(
        [channel(1, "Big", megagroup=True), channel(2, "Full")],
        {1: users, 2: users[:4]},
        {1: {"cap": 4, "reported": 10}},
    )
    results, rec = collect(client, conn)
    big, full = results
    assert (big.fetched, big.reported, big.complete) == (4, 10, False)
    assert (full.fetched, full.reported, full.complete) == (4, 4, True)
    _, snap = stored_ids(conn, -1000000000001)
    assert (snap.fetched, snap.reported, snap.complete) == (4, 10, False)
    assert "fetched 4 of 10 reported, INCOMPLETE" in rec.text
    assert "fetched 4 of 4 reported, ok" in rec.text


def test_flood_wait_is_slept_through_and_collection_continues(conn):
    users = [tg_user(i) for i in range(1, 8)]
    client = FakeClient([channel(1, "C", megagroup=True)], {1: users}, {1: {"page": 3}}, flood_on={2})
    results, rec = collect(client, conn)
    assert rec.sleeps == [31]  # requested 30s + 1
    assert "wait" in rec.text and "30s" in rec.text
    assert results[0].error is None and results[0].fetched == 7
    ids, _ = stored_ids(conn, -1000000000001)
    assert ids == set(range(1, 8))


def test_repeated_flood_wait_eventually_gives_up(conn):
    client = FakeClient([channel(1, "C")], {1: [tg_user(1)]}, flood_on=set(range(1, 100)))
    results, rec = collect(client, conn)
    assert results[0].error and "FloodWait" in results[0].error
    assert len(rec.sleeps) == collector.MAX_FLOOD_RETRIES


def test_one_failing_chat_does_not_stop_the_next(conn):
    client = FakeClient(
        [channel(1, "Private"), channel(2, "Fine")],
        {1: [], 2: [tg_user(5), tg_user(6)]},
        {1: {"error": errors.ChatAdminRequiredError(None)}},
    )
    results, rec = collect(client, conn)
    bad, good = results
    assert bad.error and "ChatAdminRequiredError" in bad.error
    assert good.error is None and good.fetched == 2
    assert "Private: FAILED" in rec.text
    snaps = db.latest_snapshots(conn)
    assert -1000000000001 not in snaps and -1000000000002 in snaps


def test_named_chats_only_and_unknown_is_an_error(conn):
    from tgstats.analysis import AnalysisError

    client = FakeClient(
        [channel(1, "One", username="one"), channel(2, "Two")], {1: [tg_user(1)], 2: [tg_user(2)]}
    )
    results, _ = collect(client, conn, ["@one"])
    assert [r.chat.title for r in results] == ["One"]
    assert set(db.latest_snapshots(conn)) == {-1000000000001}
    with pytest.raises(AnalysisError, match="unknown"):
        collect(client, conn, ["Nope"])


def test_basic_group_and_bots_deleted_are_stored_flagged(conn):
    members = [tg_user(1), tg_user(2, bot=True), tg_user(3, deleted=True)]
    client = FakeClient([basic_group(7, "Small", 3)], {7: members})
    results, _ = collect(client, conn)
    assert results[0].fetched == 3 and results[0].complete
    rows = {
        r["id"]: (r["is_bot"], r["is_deleted"])
        for r in conn.execute("SELECT * FROM users")
    }
    assert rows == {1: (0, 0), 2: (1, 0), 3: (0, 1)}
    assert conn.execute("SELECT username, first_name FROM users WHERE id = 1").fetchone()[:] == ("user1", "Name1")


def test_each_collect_adds_a_new_snapshot(conn):
    client = FakeClient([channel(1, "C")], {1: [tg_user(1)]})
    collect(client, conn)
    client.members[1] = [tg_user(1), tg_user(2)]
    collect(client, conn)
    assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 2
    ids, snap = stored_ids(conn, -1000000000001)
    assert ids == {1, 2} and snap.fetched == 2


def test_deep_search_merges_results_past_the_cap(conn):
    # first names start with a..h so a single-character search finds each user
    users = [tg_user(i, first=f"{'abcdefgh'[i - 1]}user") for i in range(1, 9)]
    client = FakeClient([channel(1, "C", megagroup=True)], {1: users}, {1: {"cap": 3, "reported": 8}})

    results, rec = collect(client, conn)
    assert (results[0].fetched, results[0].complete) == (3, False)

    results, rec = collect(client, conn, deep=True)
    assert (results[0].fetched, results[0].complete) == (8, True)
    assert "deep" in rec.text
    ids, snap = stored_ids(conn, -1000000000001)
    assert ids == set(range(1, 9)) and snap.complete


def test_deep_is_skipped_when_listing_is_complete(conn):
    client = FakeClient([channel(1, "C")], {1: [tg_user(1)]})
    collect(client, conn, deep=True)
    assert client.participant_calls == 2  # one page, one empty page; no searches


def test_flood_wait_during_chat_discovery_is_slept_through_without_duplicates(conn):
    client = FakeClient(
        [channel(1, "One"), channel(2, "Two")],
        {1: [tg_user(1)], 2: [tg_user(2)]},
        dialog_floods=2,
    )
    results, rec = collect(client, conn)
    assert rec.sleeps == [13, 13]
    assert "12s" in rec.text and "wait" in rec.text
    assert [r.chat.title for r in results] == ["One", "Two"]
    assert all(r.error is None for r in results)
    assert set(db.latest_snapshots(conn)) == {-1000000000001, -1000000000002}
