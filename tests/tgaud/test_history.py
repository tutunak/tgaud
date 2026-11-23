import threading
import time

import psycopg
import pytest

from tgaud import history


def test_history_fixture_loads(history_fixture):
    with psycopg.connect(history_fixture) as c:
        counts = dict(c.execute("""
            SELECT 'snapshots', count(*) FROM tg.snapshot UNION ALL
            SELECT 'graded', count(*) FROM tg.snapshot WHERE quality IS NOT NULL UNION ALL
            SELECT 'tracked', count(*) FROM tg.chat WHERE tracked UNION ALL
            SELECT 'aliases', count(*) FROM tg.chat_alias UNION ALL
            SELECT 'actor_mismatch', count(*) FROM tg.member_event WHERE actor_is_other <> (actor_id IS NOT NULL)
        """).fetchall())
        assert counts["snapshots"] == 15
        assert counts["graded"] == 15
        assert counts["tracked"] == 18  # A-D and H01..H14; the old H13 group was moved into its supergroup
        assert counts["aliases"] == 1
        assert counts["actor_mismatch"] == 0  # the 002 trigger set actor_is_other for the fixture's inserts
        assert c.execute("SELECT count(*) FROM tg.membership").fetchone()[0] > 30


def test_lock_chats_orders_and_dedups(tgaud_dsn):
    with psycopg.connect(tgaud_dsn) as c:
        assert history.lock_chats(c, [3, -1, 3, 2]) == [-1, 2, 3]
        held = c.execute("""SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()
                             AND objsubid = 1""").fetchone()[0]
        assert held == 3
        c.rollback()


def test_lock_chats_refuses_autocommit(tgaud_dsn):
    with psycopg.connect(tgaud_dsn, autocommit=True) as c:
        with pytest.raises(ValueError):
            history.lock_chats(c, [1])


def test_overlapping_lock_sets_in_any_order_do_not_deadlock(tgaud_dsn):
    errors: list[BaseException] = []
    done: list[str] = []
    barrier = threading.Barrier(2)

    def worker(name: str, ids: list[int]) -> None:
        try:
            with psycopg.connect(tgaud_dsn) as c:
                c.execute("SET lock_timeout = '20s'")
                barrier.wait()
                for _ in range(20):
                    history.lock_chats(c, ids)
                    time.sleep(0.005)
                    c.commit()
                done.append(name)
        except BaseException as e:  # a deadlock would surface as psycopg.errors.DeadlockDetected
            errors.append(e)

    threads = [threading.Thread(target=worker, args=("a", [1, 2, 3, 4])),
               threading.Thread(target=worker, args=("b", [4, 3, 2, 1]))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert not errors, errors
    assert sorted(done) == ["a", "b"]


def test_rebuild_pairs_stub_is_empty(tgaud_dsn):
    with psycopg.connect(tgaud_dsn) as c:
        assert history.rebuild_pairs(c, [(1, 2)]) == history.RebuildResult(0, 0, 0)
