import pytest

from tgstats import analysis, db
from tgstats.analysis import AnalysisError
from conftest import user


def by_title(results):
    return {r.chat_set.chat.title: r for r in results}


def test_unique_default_compares_every_collected_chat(fx, conn):
    fx.chat(1, "A", [1, 2, 3, 4])
    fx.chat(2, "B", [3, 4, 5])
    fx.chat(3, "C", [4, 6])
    res = by_title(analysis.unique_per_chat(analysis.load_sets(conn)))
    assert res["A"].unique == {1, 2} and res["A"].total == 4
    assert res["B"].unique == {5} and res["B"].total == 3
    assert res["C"].unique == {6} and res["C"].total == 2
    assert res["A"].percent == 50.0


def test_unique_with_among_ignores_other_chats(fx, conn):
    fx.chat(1, "A", [1, 2, 3, 4])
    fx.chat(2, "B", [3, 4, 5])
    fx.chat(3, "C", [1, 2, 5])
    res = by_title(analysis.unique_per_chat(analysis.load_sets(conn, ["A", "B"])))
    assert set(res) == {"A", "B"}
    assert res["A"].unique == {1, 2}
    assert res["B"].unique == {5}
    full = by_title(analysis.unique_per_chat(analysis.load_sets(conn)))
    assert full["A"].unique == set()


def test_unique_single_chat_is_everyone(fx, conn):
    fx.chat(1, "A", [1, 2])
    (res,) = analysis.unique_per_chat(analysis.load_sets(conn))
    assert res.unique == {1, 2}


def test_intersection_counts_percentages_and_jaccard(fx, conn):
    fx.chat(1, "A", [1, 2, 3, 4])
    fx.chat(2, "B", [3, 4, 5, 6, 7, 8])
    a, b = analysis.load_sets(conn, ["A", "B"])
    r = analysis.intersect(a, b)
    assert r.common == {3, 4}
    assert r.percent_of_a == 50.0
    assert r.percent_of_b == pytest.approx(100 / 3)
    assert r.jaccard == pytest.approx(2 / 8)


def test_intersection_of_empty_chats_does_not_divide_by_zero(fx, conn):
    fx.chat(1, "A", [])
    fx.chat(2, "B", [])
    a, b = analysis.load_sets(conn, ["A", "B"])
    r = analysis.intersect(a, b)
    assert (r.percent_of_a, r.percent_of_b, r.jaccard) == (0.0, 0.0, 0.0)


def test_overlap_matrix(fx, conn):
    fx.chat(1, "A", [1, 2, 3])
    fx.chat(2, "B", [2, 3, 4, 5])
    fx.chat(3, "C", [9])
    sets = analysis.load_sets(conn)
    assert [s.chat.title for s in sets] == ["A", "B", "C"]
    assert analysis.overlap_matrix(sets) == [[3, 2, 0], [2, 4, 0], [0, 0, 1]]


def test_latest_snapshot_is_used_and_old_ones_kept(fx, conn):
    fx.chat(1, "A", [1, 2, 3])
    fx.chat(1, "A", [2, 3, 4, 5])
    fx.chat(2, "B", [5])
    assert conn.execute("SELECT COUNT(*) FROM snapshots WHERE chat_id = 1").fetchone()[0] == 2
    sets = analysis.load_sets(conn)
    a = next(s for s in sets if s.chat.title == "A")
    assert a.ids == {2, 3, 4, 5}
    assert a.snapshot.fetched == 4
    res = by_title(analysis.unique_per_chat(sets))
    assert res["A"].unique == {2, 3, 4}


def test_bots_and_deleted_excluded_by_default(fx, conn):
    users = [user(1), user(2, bot=True), user(3, deleted=True), user(4)]
    fx.chat(1, "A", [1, 2, 3, 4], users=users)
    fx.chat(2, "B", [2, 3, 9], users=[user(2, bot=True), user(3, deleted=True), user(9)])
    sets = analysis.load_sets(conn)
    res = by_title(analysis.unique_per_chat(sets))
    assert res["A"].total == 2 and res["A"].unique == {1, 4}
    assert res["B"].unique == {9}
    assert analysis.intersect(*sets).common == set()

    sets = analysis.load_sets(conn, include_bots=True)
    res = by_title(analysis.unique_per_chat(sets))
    assert res["A"].total == 4 and res["A"].unique == {1, 4}
    assert analysis.intersect(*sets).common == {2, 3}


def test_no_collected_chats_is_an_error(conn):
    with pytest.raises(AnalysisError, match="collect"):
        analysis.load_sets(conn)


def test_among_chat_without_snapshot_is_an_error(fx, conn):
    fx.chat(1, "A", [1])
    db.upsert_chat(conn, db.ChatRow(2, "group", "B", None))
    with pytest.raises(AnalysisError, match="no snapshot"):
        analysis.load_sets(conn, ["A", "B"])


def test_complete_snapshots_give_no_warning(fx, conn):
    fx.chat(1, "A", [1, 2], reported=2)
    fx.chat(2, "B", [1], reported=1)
    sets = analysis.load_sets(conn)
    for kind in ("unique", "intersect", "overlap"):
        assert analysis.incomplete_warning(sets, kind) is None


def test_incomplete_warning_names_the_chat_and_says_what_it_means(fx, conn):
    fx.chat(1, "Complete", [1, 2], reported=2)
    fx.chat(2, "Short chat", [1], reported=50)
    sets = analysis.load_sets(conn)
    assert [s.chat.title for s in analysis.incomplete_sets(sets)] == ["Short chat"]

    unique = analysis.incomplete_warning(sets, "unique")
    assert "Short chat" in unique and "Complete" not in unique
    assert "fetched 1 of 50" in unique
    assert "either direction" in unique and "even for a chat whose own snapshot is complete" in unique

    inter = analysis.incomplete_warning(sets, "intersect")
    assert "lower bound" in inter and "Jaccard" in inter and "either direction" in inter

    over = analysis.incomplete_warning(sets, "overlap")
    assert "lower bound" in over


def test_incomplete_warning_names_every_incomplete_chat(fx, conn):
    fx.chat(1, "X", [1], reported=9)
    fx.chat(2, "Y", [1], reported=9)
    text = analysis.incomplete_warning(analysis.load_sets(conn), "overlap")
    assert "'X'" in text and "'Y'" in text


def test_missing_member_makes_other_chat_look_unique(fx, conn):
    # truth A=B={1,2}; B's snapshot misses user 2 -> 1 "unique" user in A, truth is 0
    fx.chat(1, "A", [1, 2], reported=2)
    fx.chat(2, "B", [1], reported=2)
    sets = analysis.load_sets(conn)
    res = by_title(analysis.unique_per_chat(sets))
    assert res["A"].unique == {2}
    assert [s.chat.title for s in analysis.incomplete_sets(sets)] == ["B"]
    assert "'B'" in analysis.incomplete_warning(sets, "unique")


def test_both_truncated_overstates_overlap(fx, conn):
    # truth A={1,2,3}, B={1,2,4}: real 66.7% and Jaccard 0.5; fetched {1,2} for both
    fx.chat(1, "A", [1, 2], reported=3)
    fx.chat(2, "B", [1, 2], reported=3)
    a, b = analysis.load_sets(conn)
    r = analysis.intersect(a, b)
    assert r.percent_of_a == 100.0 and r.jaccard == 1.0
    assert not a.snapshot.complete and not b.snapshot.complete


def test_is_incomplete_threshold():
    assert not db.is_incomplete(98, 100)
    assert db.is_incomplete(97, 100)
    assert not db.is_incomplete(5, None)
    assert not db.is_incomplete(10, 10)
    assert db.is_incomplete(1, 2)
