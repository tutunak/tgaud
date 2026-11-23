"""The queries.sql loader: every block loads, 1-16 run on the history fixture, 1, 2 and 4 give verify.md 4."""

from decimal import Decimal

import psycopg
import pytest
from psycopg import pq

from tgaud import sql, sqlscript

A, B, C, D = -1001000000001, -1001000000002, -1001000000003, -1001000000004
NOW = "2026-11-23 12:00+00"
AD = [A, B, C, D]
# The parameters of docs/design/evidence/40-run-queries.sh, per block.
RUNS = [
    ("1", dict(scope=AD)), ("2a", {}), ("2a", dict(scope=AD)), ("2b", dict(user=1001)), ("2b", dict(user=1006)),
    ("3", dict(scope=AD)), ("3", dict(scope=[A, B])), ("4", {}), ("5", dict(scope=AD, since="2026-08-01")),
    ("6", dict(scope=AD, since="2026-08-01", burst_users=2)), ("7a", dict(scope=AD)),
    ("7b", dict(scope=[A, B], since="2026-08-01")), ("8", dict(scope=[A], since="2026-08-01")),
    ("9a", dict(scope=[A, B], since="2026-08-01")), ("9b", dict(scope=AD)), ("10", dict(since="2026-08-01")),
    ("11", {}), ("12", dict(since="2026-08-01")), ("13", dict(raid_min_joins=5)), ("14", dict(scope=AD)),
    ("15", dict(scope=AD)), ("16a", dict(chat=A, day="2026-11-07 00:00+00")),
    ("16a", dict(chat=A, day="2026-07-15 00:00+00")), ("16b", dict(chat=B, day="2026-11-07 00:00+00")),
    ("16c", dict(scope=AD, day="2026-11-07 00:00+00")), ("16d", dict(scope=AD, day="2026-11-07 00:00+00")),
]


@pytest.fixture
def fx(history_fixture):
    """The fixture database in a transaction that is rolled back, with R1 and R2 built inside it."""
    with psycopg.connect(history_fixture) as c:
        sql.run(c, "R1", as_of=NOW)
        sql.run(c, "R2", as_of=NOW)
        yield c
        c.rollback()


def test_every_block_id_loads():
    ids = sql.ids()
    assert len(ids) == 37
    assert {"1", "2", "2a", "2b", "4a", "16c", "16d", "R1", "R2", "R3", "R4"} <= set(ids)
    for block_id in ids:
        if block_id in ("R1", "R2"):
            with pytest.raises(sql.BranchingBlock):
                sql.block(block_id)
            continue
        stmts = sql.block(block_id)
        assert stmts, block_id
        for s in stmts:
            assert s.head.split(" ")[0] not in ("BEGIN", "COMMIT")
            s.query()
    assert [s.head for s in sql.block("16c")] == ["SET LOCAL", "WITH S"]
    assert len(sql.block("2")) == 2 and len(sql.block("16")) == 5


def test_placeholders_never_carry_values():
    s = sql.block("16a")[0]
    q = sqlscript.strip_comments(s.query())
    assert ":'" not in q and ":chat" not in q and "%(v" in q
    params = s.bind(sql.load().defaults | {"day_ts": "x", "as_of_ts": "y", "scope_ids": "{}", "since_ts": "z"})
    assert all(name.startswith("v") for name in params)
    assert all(type(v).__name__ in ("str", "Int8", "Int4", "Decimal") for v in params.values())


@pytest.mark.parametrize("block_id,params", RUNS, ids=[f"{b}-{i}" for i, (b, _) in enumerate(RUNS)])
def test_analytics_blocks_run_on_the_fixture(fx, block_id, params):
    res = sql.run(fx, block_id, as_of=NOW, **params)
    assert res.columns
    assert fx.info.transaction_status == pq.TransactionStatus.INTRANS


def _by_pair(rows):
    return {(r["chat_a"], r["chat_b"]): r for r in rows}


def test_block_1_overlap_matches_verify(fx):
    res = sql.run(fx, "1", as_of=NOW, scope=AD)
    rows = _by_pair(res.dicts())
    assert len(rows) == 16
    shared = {pair: rows[pair]["shared"] for pair in [(A, B), (A, C), (A, D), (B, C), (B, D), (C, D)]}
    assert shared == {(A, B): 4, (A, C): 2, (A, D): 1, (B, C): 3, (B, D): 1, (C, D): 1}
    assert [rows[(x, x)]["shared"] for x in (A, B, C, D)] == [10, 6, 5, 2]
    ab = rows[(A, B)]
    assert (ab["pct_of_a"], ab["pct_of_b"], ab["jaccard"]) == (Decimal("40.00"), Decimal("66.67"), Decimal("0.3333"))
    assert ab["coverage_a"] == Decimal("80.00") and ab["suspect_a"] == 0


def test_block_2_chats_per_user_matches_verify(fx):
    all_tracked = sql.run(fx, "2a", as_of=NOW).rows
    assert [(k, n) for k, n, *_ in all_tracked] == [(1, 20), (2, 3), (3, 1), (4, 1)]
    scoped = sql.run(fx, "2a", as_of=NOW, scope=AD).rows
    assert [(k, n) for k, n, *_ in scoped] == [(1, 10), (2, 3), (3, 1), (4, 1)]
    u1001 = sql.run(fx, "2b", as_of=NOW, user=1001).dicts()
    assert len(u1001) == 4 and {r["now"] for r in u1001} == {"in"}
    assert sorted(r["start_source"] for r in u1001) == ["event", "event", "snapshot", "snapshot"]
    assert sum(r["joined_hint"] is not None for r in u1001) == 2
    u1006 = sql.run(fx, "2b", as_of=NOW, user=1006).dicts()
    assert [(r["chat_id"], r["now"]) for r in u1006] == [(A, "out"), (A, "in")]
    assert u1006[1]["link"] == "G1 giveaway A"


def test_block_4_giveaways_match_verify(fx):
    per_chat = {(r["campaign_id"], r["chat_id"]): r for r in sql.run(fx, "4a", as_of=NOW).dicts()}
    a = per_chat[(1, A)]
    assert [a[k] for k in ("entrants", "first_time", "returners", "quick_exit", "in_at_draw", "exit_after_draw",
                           "still_in")] == [6, 5, 1, 3, 4, 2, 2]
    b = per_chat[(1, B)]
    assert [b[k] for k in ("entrants", "quick_exit", "quick_exit_kicked", "quick_exit_unknown", "in_at_draw",
                           "in_at_draw_unknown", "exit_after_draw_unknown", "still_in")] == [3, 1, 1, 1, 1, 1, 1, 1]
    c = per_chat[(2, C)]
    assert [c[k] for k in ("entrants", "quick_exit", "in_at_draw", "still_in")] == [2, 1, 1, 1]
    persons = {r["campaign_id"]: r for r in sql.run(fx, "4b", as_of=NOW).dicts()}
    g1 = persons[1]
    assert [g1[k] for k in ("persons", "multi_chat", "first_time", "quick_exit", "in_at_draw", "exit_after_draw",
                            "still_in")] == [8, 1, 7, 4, 5, 2, 3]
    assert len(sql.run(fx, "4c", as_of=NOW).rows) == 11
    # block "4" runs 4a, 4b and 4c; the last is the entrant list
    assert sql.run(fx, "4", as_of=NOW).columns[:3] == ["campaign_id", "chat_id", "user_id"]
    assert len(sql.run(fx, "4", as_of=NOW, all_results=True)) == 3


def test_16c_returns_the_matrix_and_leaves_the_callers_transaction_open(fx):
    res = sql.run(fx, "16c", as_of=NOW, scope=AD, day="2026-11-07 00:00+00")
    m = {}
    for r in res.dicts():
        m[(r["chat_a"], r["chat_b"])] = m[(r["chat_b"], r["chat_a"])] = r["shared"]
    assert len(res.rows) == 10
    assert [m[(x, x)] for x in (A, B, C, D)] == [11, 6, 5, 4]
    assert (m[(A, B)], m[(A, C)], m[(B, C)], m[(A, D)]) == (4, 2, 3, 1)
    assert fx.info.transaction_status == pq.TransactionStatus.INTRANS
    assert fx.execute("SHOW work_mem").fetchone()[0] == "64MB"  # SET LOCAL lasts until the caller ends it


def test_16d_from_chat_day_matches_verify(fx):
    eod = {r["chat_id"]: r for r in sql.run(fx, "16d", as_of=NOW, scope=AD, day="2026-11-07 00:00+00").dicts()}
    assert [eod[x]["members_eod"] for x in (A, B, C, D)] == [12, 7, 5, 4]
    assert eod[B]["unsure_eod"] == 1


def test_explain_is_an_option_and_never_bound(fx):
    plan = sql.run(fx, "1", as_of=NOW, scope=AD, explain="EXPLAIN (COSTS OFF)")
    assert plan.columns == ["QUERY PLAN"] and plan.rows
    for bad in ("EXPLAIN; DROP TABLE tg.chat", "SELECT 1;", "EXPLAIN (ANALYZE, BUFFERS); --"):
        with pytest.raises(ValueError):
            sql.run(fx, "1", as_of=NOW, explain=bad)


def test_values_are_bound_not_formatted(fx):
    with pytest.raises(psycopg.errors.InvalidDatetimeFormat):
        sql.run(fx, "16a", as_of="2026-11-23'; DROP TABLE tg.chat; --", chat=A)
    fx.rollback()
    for bad in (dict(scope="{1}; DROP TABLE tg.chat"), dict(chat="1 OR 1=1"), dict(hours="48; --"),
                dict(only="1"), dict(nope=1)):
        with pytest.raises((ValueError, TypeError)):
            sql.run(fx, "16a", as_of=NOW, **bad)
    assert fx.execute("SELECT count(*) FROM tg.chat").fetchone()[0] > 0


def test_default_report_on_an_empty_schema_returns_no_rows(tgaud_dsn):
    with psycopg.connect(tgaud_dsn) as c:
        res = sql.run(c, "1")
        assert res.rows == [] and res.columns[:3] == ["chat_a", "title_a", "chat_b"]
        assert sql.values(c)["scope_ids"] == "{}"
        c.rollback()
