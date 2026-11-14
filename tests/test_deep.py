"""collect --deep: the two-character pass, pruning, and what an interrupt keeps."""

import asyncio
import contextlib
import string

import pytest
from fakes import FakeClient, channel, tg_user
from test_collector import Recorder, run

from tgstats import cli, collector, db

CHAT1, CHAT2 = -1000000000001, -1000000000002
LATIN = string.ascii_lowercase + string.digits
CYRILLIC = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"


def collect(client, conn, deep=True, **kwargs):
    rec = Recorder()
    results = run(collector.collect(client, conn, None, deep=deep, sleep=rec.sleep, out=rec.out, **kwargs))
    return results, rec


def queries(client):
    return [r.filter.q for r in client.requests if hasattr(r, "filter")]


def pair_queries_run(client):
    seen = []
    for q in queries(client):
        if len(q) == 2 and (not seen or seen[-1] != q):
            seen.append(q)
    return seen


def named(*names):
    return [tg_user(i, first=name) for i, name in enumerate(names, 1)]


def capped_chat():
    """Six users a listing and a search each return only part of; the pairs aa, ab, ac find all of them."""
    users = named("aa1", "aa2", "ab1", "ab2", "ac1", "ac2")
    return users, {1: {"cap": 1, "search_cap": 2, "reported": 6}}


def stored(conn, chat_id=CHAT1):
    snap = db.latest_snapshots(conn)[chat_id]
    return set(db.snapshot_members(conn, snap.id, include_bots=True)), snap


def test_group_sizes_and_query_counts():
    assert (len(LATIN), len(CYRILLIC)) == (36, 33)
    assert collector.DEEP_QUERIES == list(LATIN + CYRILLIC)
    assert len(collector.DEEP_QUERIES) == 69
    assert len(collector.DEEP_QUERIES) + 36 * 36 + 33 * 33 == 69 + 2385


def test_pair_pass_is_skipped_when_the_single_pass_completes(conn):
    client = FakeClient([channel(1, "C", megagroup=True)], {1: named("a1", "b1", "c1")}, {1: {"cap": 1}})
    results, rec = collect(client, conn)
    assert (results[0].fetched, results[0].complete) == (3, True)
    assert not [q for q in queries(client) if len(q) == 2]
    assert "two characters" not in rec.text


def test_pairs_stay_in_one_group_in_the_stated_order_and_zero_runs_the_full_grid(conn):
    client = FakeClient(
        [channel(1, "C", megagroup=True)], {1: named("_x")}, {1: {"reported": 10**6}}
    )
    results, rec = collect(client, conn, expand_at=0)
    expected = [a + b for a in LATIN for b in LATIN] + [a + b for a in CYRILLIC for b in CYRILLIC]
    assert len(expected) == 1296 + 1089
    assert pair_queries_run(client) == expected
    assert results[0].complete is False
    assert "2385 queries for 69 first characters, 0 skipped" in rec.text


@pytest.mark.parametrize(
    "expand_at,expanded", [(3, ["a"]), (2, ["a", "b"]), (4, []), (collector.DEFAULT_EXPAND_AT, [])]
)
def test_only_characters_at_or_above_expand_at_are_expanded(conn, expand_at, expanded):
    # a single search returns 3 members for 'a', 2 for 'b', none for the rest
    client = FakeClient(
        [channel(1, "C", megagroup=True)],
        {1: named("a1", "a2", "a3", "b1", "b2")},
        {1: {"cap": 1, "reported": 1000}},
    )
    results, rec = collect(client, conn, expand_at=expand_at)
    pairs = pair_queries_run(client)
    assert pairs == [f + c for f in expanded for c in LATIN]
    skipped = 69 - len(expanded)
    assert f"{36 * len(expanded)} queries for {len(expanded)} first characters, {skipped} skipped" in rec.text
    # one summary line per expanded character, not one per query
    assert [line for line in rec.lines if "?'" in line] == [
        f"  deep '{f}?': 36 queries, +0 new, 5 total" for f in expanded
    ]
    assert len(rec.lines) < 90


def test_default_expand_at_is_100():
    assert collector.DEFAULT_EXPAND_AT == 100
    args = cli.build_parser().parse_args(["collect"])
    assert args.expand_at == 100
    assert cli.build_parser().parse_args(["collect", "--expand-at", "0"]).expand_at == 0


def test_stops_as_soon_as_the_snapshot_is_complete(conn):
    users, opts = capped_chat()
    client = FakeClient([channel(1, "C", megagroup=True)], {1: users}, opts)
    results, rec = collect(client, conn, expand_at=2)
    assert (results[0].fetched, results[0].complete) == (6, True)
    assert pair_queries_run(client) == ["aa", "ab", "ac"]  # no 'ad'
    assert "deep 'a?': 3 queries, +4 new, 6 total" in rec.text
    ids, snap = stored(conn)
    assert ids == set(range(1, 7)) and snap.complete


def test_cyrillic_pairs_are_searched_after_the_latin_ones(conn):
    users = named("аб1", "аб2", "ав1")
    client = FakeClient(
        [channel(1, "C", megagroup=True)], {1: users}, {1: {"cap": 1, "search_cap": 2, "reported": 3}}
    )
    results, _ = collect(client, conn, expand_at=2)
    assert results[0].complete
    assert pair_queries_run(client)[-3:] == ["аа", "аб", "ав"]
    assert all(not any(ch in CYRILLIC for ch in q) for q in pair_queries_run(client)[:-3])


def test_a_failing_query_stops_the_deep_search_and_keeps_what_was_found(conn):
    users, opts = capped_chat()
    client = FakeClient(
        [channel(1, "C", megagroup=True)], {1: users}, opts, fail_on_queries={"ac": RuntimeError("boom")}
    )
    results, rec = collect(client, conn, expand_at=2)
    assert "deep search stopped at 'ac': RuntimeError: boom" in rec.text
    assert pair_queries_run(client) == ["aa", "ab", "ac"]
    assert (results[0].error, results[0].fetched, results[0].complete) == (None, 4, False)
    assert stored(conn)[0] == {1, 2, 3, 4}


# ---------------------------------------------------------------- interrupts


def seed(conn, count):
    """An earlier complete snapshot of chat 1 with `count` members."""
    users, _ = capped_chat()
    collect(FakeClient([channel(1, "C", megagroup=True)], {1: users[:count]}), conn, deep=False)
    return db.latest_snapshots(conn)[CHAT1]


def interrupted(conn, **client_kwargs):
    """Run a collect of two chats that is interrupted in the first; return (client, recorder)."""
    users, opts = capped_chat()
    client = FakeClient(
        [channel(1, "C", megagroup=True), channel(2, "Next")],
        {1: users, 2: [tg_user(50)]},
        opts,
        **client_kwargs,
    )
    with pytest.raises(asyncio.CancelledError):
        collect(client, conn, expand_at=2)
    return client


def test_interrupt_in_the_pair_pass_keeps_an_incomplete_partial_snapshot(conn, capsys):
    client = interrupted(conn, interrupt_on_queries={"ac"})
    ids, snap = stored(conn)
    assert ids == {1, 2, 3, 4}  # aa1 aa2 ab1 ab2, found before 'ac'
    assert (snap.fetched, snap.reported, snap.complete) == (4, 6, False)
    assert CHAT2 not in db.latest_snapshots(conn)
    assert all(getattr(r, "channel", None) is None or r.channel.id == 1 for r in client.requests)


def test_interrupt_message_says_a_partial_snapshot_was_saved(conn):
    users, opts = capped_chat()
    client = FakeClient([channel(1, "C", megagroup=True)], {1: users}, opts, interrupt_on_queries={"ac"})
    rec = Recorder()
    with pytest.raises(asyncio.CancelledError):
        run(collector.collect(client, conn, None, deep=True, sleep=rec.sleep, out=rec.out, expand_at=2))
    assert "C: interrupted; saved a partial snapshot, fetched 4 of 6 reported, INCOMPLETE" in rec.lines


def test_interrupted_snapshot_is_incomplete_even_when_the_counts_say_complete(conn):
    users = named("a1", "b1", "c1", "d1")
    client = FakeClient(
        [channel(1, "C", megagroup=True)], {1: users}, {1: {"page": 2}}, interrupt_on_calls={3}
    )
    with pytest.raises(asyncio.CancelledError):
        collect(client, conn, deep=False)
    ids, snap = stored(conn)
    assert ids == {1, 2, 3, 4} and (snap.fetched, snap.reported) == (4, 4)
    assert snap.complete is False


def test_interrupt_in_the_count_request_stores_nothing(conn):
    client = interrupted(conn, interrupt_on_full=True)
    assert db.latest_snapshots(conn) == {}
    assert queries(client) == []


def test_interrupt_in_the_count_request_leaves_the_earlier_snapshot_the_latest(conn):
    before = seed(conn, 6)
    client = interrupted(conn, interrupt_on_full=True)
    assert db.latest_snapshots(conn)[CHAT1] == before
    assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 1
    assert CHAT2 not in db.latest_snapshots(conn)


@pytest.mark.parametrize("earlier,kept", [(6, False), (2, False), (1, True)])
def test_partial_result_is_stored_only_when_it_beats_the_latest_snapshot(conn, earlier, kept):
    before = seed(conn, earlier)
    rec = Recorder()
    users, opts = capped_chat()
    # the listing gives aa1, the search for 'a' aa2; the interrupt comes at 'b' with 2 members found
    client = FakeClient(
        [channel(1, "C", megagroup=True), channel(2, "Next")],
        {1: users, 2: [tg_user(50)]},
        opts,
        interrupt_on_queries={"b"},
    )
    with pytest.raises(asyncio.CancelledError):
        run(collector.collect(client, conn, None, deep=True, sleep=rec.sleep, out=rec.out, expand_at=2))
    latest = db.latest_snapshots(conn)[CHAT1]
    if kept:
        assert latest.id != before.id and (latest.fetched, latest.complete) == (2, False)
        assert "saved a partial snapshot, fetched 2 of 6" in rec.text
    else:
        assert latest == before
        assert "partial result discarded" in rec.text and f"the {earlier} in the latest snapshot" in rec.text
    assert CHAT2 not in db.latest_snapshots(conn)


# ---------------------------------------------------------------- command line


@pytest.fixture
def cli_env(tmp_path, monkeypatch):
    monkeypatch.setenv("TG_API_ID", "1")
    monkeypatch.setenv("TG_API_HASH", "h")
    db_path = tmp_path / "t.db"

    def run_cli(client, *argv):
        @contextlib.asynccontextmanager
        async def connected(cfg, delay):
            yield client

        monkeypatch.setattr(cli, "connected_client", connected)
        return cli.main(["collect", "--db", str(db_path), "--session", str(tmp_path / "s"), *argv])

    run_cli.db_path = db_path
    return run_cli


def test_negative_expand_at_is_a_one_line_usage_error(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TG_API_ID", raising=False)
    assert cli.main(["collect", "--deep", "--expand-at", "-1"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    lines = captured.err.strip().splitlines()
    assert len(lines) == 1 and "--expand-at" in lines[0]


def test_collect_help_shows_expand_at(capsys):
    with pytest.raises(SystemExit):
        cli.main(["collect", "--help"])
    assert "--expand-at N" in capsys.readouterr().out


def test_expand_at_reaches_the_collector(cli_env):
    client = FakeClient(
        [channel(1, "C", megagroup=True)], {1: named("_x")}, {1: {"reported": 1000}}
    )
    assert cli_env(client, "--deep", "--expand-at", "0") == 0
    assert len(pair_queries_run(client)) == 2385


@pytest.mark.parametrize(
    "kwargs,earlier,saved",
    [
        pytest.param({"interrupt_on_queries": {"ac"}}, None, 4, id="pair-pass"),
        pytest.param({"interrupt_on_full": True}, None, None, id="count-request"),
        pytest.param({"interrupt_on_queries": {"b"}}, 6, None, id="fewer-than-latest"),
    ],
)
def test_ctrl_c_exits_130_and_later_chats_are_not_collected(cli_env, capsys, kwargs, earlier, saved):
    users, opts = capped_chat()
    if earlier:
        conn = db.connect(cli_env.db_path)
        seed(conn, earlier)
        conn.close()
    client = FakeClient(
        [channel(1, "C", megagroup=True), channel(2, "Next")],
        {1: users, 2: [tg_user(50)]},
        opts,
        sigint=True,
        **kwargs,
    )
    assert cli_env(client, "--deep", "--expand-at", "2") == 130
    captured = capsys.readouterr()
    assert "interrupted" in captured.err
    assert "Done:" not in captured.out
    assert all(getattr(r, "channel", None) is None or r.channel.id == 1 for r in client.requests)
    conn = db.connect(cli_env.db_path)
    latest = db.latest_snapshots(conn)
    assert CHAT2 not in latest
    if saved:
        assert (latest[CHAT1].fetched, latest[CHAT1].complete) == (saved, False)
        assert "saved a partial snapshot, fetched 4 of 6" in captured.out
    elif earlier:
        assert latest[CHAT1].fetched == earlier and "partial result discarded" in captured.out
    else:
        assert latest == {} and "partial result discarded" in captured.out
    conn.close()


def test_hint_to_use_deep_appears_only_for_an_incomplete_snapshot_without_deep(cli_env, capsys):
    short = FakeClient([channel(1, "C", megagroup=True)], {1: named("_x", "_y", "_z")}, {1: {"cap": 1}})
    assert cli_env(short) == 0
    out = capsys.readouterr().out
    assert "Incomplete snapshots: C" in out
    assert "tgstats collect --deep CHAT" in out and "slow" in out

    whole = FakeClient([channel(1, "C", megagroup=True)], {1: named("_x", "_y", "_z")})
    assert cli_env(whole) == 0
    out = capsys.readouterr().out
    assert "Incomplete" not in out and "--deep" not in out

    # with --deep it already searched: no hint even though the snapshot stays incomplete
    short = FakeClient([channel(1, "C", megagroup=True)], {1: named("_x", "_y", "_z")}, {1: {"cap": 1}})
    assert cli_env(short, "--deep") == 0
    out = capsys.readouterr().out
    assert "Incomplete snapshots: C" in out and "tgstats collect --deep CHAT" not in out
