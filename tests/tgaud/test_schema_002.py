"""The shared schema additions of migration 002."""

import datetime as dt
import threading
import time

import psycopg
import pytest

CHAT = -1009000000001


@pytest.fixture
def c(tgaud_dsn):
    """A tenant connection in a transaction that is rolled back at the end."""
    with psycopg.connect(tgaud_dsn) as conn:
        conn.execute("SELECT * FROM tg.ensure_partitions('member_event', 'month', '2026-01-01', '2027-01-01')")
        conn.execute("INSERT INTO tg.chat (chat_id, chat_type, title, tracked) VALUES (%s, 'channel', 't', true)",
                     (CHAT,))
        conn.execute("""INSERT INTO tg.tg_user (user_id, first_seen_at, last_seen_at)
                        SELECT u, now(), now() FROM generate_series(1, 5) u""")
        yield conn
        conn.rollback()


def _event(c, update_id, user_id, actor_id=None, actor_is_other=None):
    cols = "chat_id, user_id, event_at, update_id, old_status, new_status, actor_id"
    vals = [CHAT, user_id, dt.datetime(2026, 9, 1, tzinfo=dt.UTC), update_id, "left", "member", actor_id]
    if actor_is_other is not None:
        cols += ", actor_is_other"
        vals.append(actor_is_other)
    ph = ", ".join(["%s"] * len(vals))
    return c.execute(f"INSERT INTO tg.member_event ({cols}) VALUES ({ph}) RETURNING actor_is_other", vals).fetchone()[0]


def test_actor_is_other_is_set_at_insert_and_survives_erasing_the_actor(c):
    assert _event(c, 1, 1, actor_id=2) is True
    assert _event(c, 2, 1, actor_id=None) is False
    assert _event(c, 3, 1, actor_id=None, actor_is_other=True) is True
    c.execute("UPDATE tg.member_event SET actor_id = NULL WHERE update_id = 1")
    assert c.execute("SELECT actor_is_other FROM tg.member_event WHERE update_id = 1").fetchone()[0] is True


def test_bot_state_columns(c):
    c.execute("INSERT INTO tg.bot_state (bot_id, allowed_updates) VALUES (5000, '{chat_member}')")
    row = c.execute("SELECT serve_generation, caught_up_at, link_key_fingerprint FROM tg.bot_state").fetchone()
    assert row == (0, None, None)
    comment = c.execute("SELECT col_description('tg.bot_state'::regclass, attnum) FROM pg_attribute "
                        "WHERE attrelid = 'tg.bot_state'::regclass AND attname = 'caught_up_at'").fetchone()[0]
    assert comment.startswith("when a getUpdates last returned fewer updates than its limit")


def test_chat_alias_moved_at_is_nullable_without_default(c):
    row = c.execute("""SELECT is_nullable, column_default FROM information_schema.columns
                       WHERE table_schema = 'tg' AND table_name = 'chat_alias' AND column_name = 'moved_at'""").fetchone()
    assert row == ("YES", None)
    c.execute("INSERT INTO tg.chat_alias (old_chat_id, chat_id, migrated_at) VALUES (-5, %s, now())", (CHAT,))
    assert c.execute("SELECT moved_at FROM tg.chat_alias WHERE old_chat_id = -5").fetchone()[0] is None


def _snapshot(c, **cols):
    base = dict(chat_id=CHAT, started_at=dt.datetime(2026, 9, 1, tzinfo=dt.UTC), method="listing")
    base.update(cols)
    names = ", ".join(base)
    ph = ", ".join(["%s"] * len(base))
    return c.execute(f"INSERT INTO tg.snapshot ({names}) VALUES ({ph}) RETURNING snapshot_id",
                     list(base.values())).fetchone()[0]


def test_a_grade_may_exist_without_reconciled_at_but_not_the_reverse(c):
    done = dict(finished_at=dt.datetime(2026, 9, 1, 1, tzinfo=dt.UTC), fetched=1, complete=True)
    _snapshot(c, quality="good", applied_through_user_id=100, **done)
    c.execute("SAVEPOINT s")
    with pytest.raises(psycopg.errors.CheckViolation, match="reconciled_needs_grade"):
        _snapshot(c, reconciled_at=dt.datetime.now(dt.UTC), **done)
    c.execute("ROLLBACK TO s")


def test_reconcile_chunks_and_gap_progress(c):
    c.execute("""INSERT INTO tg.reconcile_run (chat_id, chunk, from_user_id, through_user_id, listed)
                 VALUES (%s, 1, 1, 500, 500), (%s, 2, 501, 900, 400)""", (CHAT, CHAT))
    gap = c.execute("INSERT INTO tg.gap (chat_id, period, reason) VALUES (%s, tstzrange(now() - interval '2 days', "
                    "now()), 'service_down') RETURNING gap_id", (CHAT,)).fetchone()[0]
    c.execute("INSERT INTO tg.gap_progress (gap_id, chat_id, last_user_id) VALUES (%s, %s, 42)", (gap, CHAT))
    c.execute("UPDATE tg.gap_progress SET done = true WHERE gap_id = %s", (gap,))
    c.execute("DELETE FROM tg.gap WHERE gap_id = %s", (gap,))
    assert c.execute("SELECT count(*) FROM tg.gap_progress").fetchone()[0] == 0


def test_mark_dirty_keeps_the_earliest_day_and_bumps_the_version(c):
    v1 = c.execute("SELECT tg.mark_dirty(%s, '2026-09-10')", (CHAT,)).fetchone()[0]
    v2 = c.execute("SELECT tg.mark_dirty(%s, '2026-09-12')", (CHAT,)).fetchone()[0]
    v3 = c.execute("SELECT tg.mark_dirty(%s, '2026-09-05')", (CHAT,)).fetchone()[0]
    assert v1 < v2 < v3
    assert c.execute("SELECT from_day::text FROM tg.rollup_dirty WHERE chat_id = %s", (CHAT,)).fetchone()[0] == "2026-09-05"
    # a direct upsert (as tg.move_chat_history and tg.forget_user do) bumps it too
    c.execute("""INSERT INTO tg.rollup_dirty (chat_id, from_day) VALUES (%s, '2026-09-01')
                 ON CONFLICT (chat_id) DO UPDATE SET from_day = least(tg.rollup_dirty.from_day, excluded.from_day)""",
              (CHAT,))
    v4 = c.execute("SELECT version FROM tg.rollup_dirty WHERE chat_id = %s", (CHAT,)).fetchone()[0]
    assert v4 > v3


def test_is_erased_covers_evidence_from_before_the_erasure_only(c):
    c.execute("INSERT INTO tg.erasure (user_hash, erased_at) VALUES (tg.erasure_hash(7), '2026-10-01+00')")
    assert c.execute("SELECT tg.is_erased(7, '2026-09-30+00')").fetchone()[0] is True
    assert c.execute("SELECT tg.is_erased(7, '2026-10-01+00')").fetchone()[0] is True
    assert c.execute("SELECT tg.is_erased(7, '2026-10-02+00')").fetchone()[0] is False
    assert c.execute("SELECT tg.is_erased(8, '2026-09-30+00')").fetchone()[0] is False
    assert c.execute("SELECT length(tg.erasure_hash(7)), tg.erasure_hash(7) = tg.erasure_hash(7)").fetchone() == (32, True)


# ---- the finished-snapshot guard, as collect_writer on two connections -------------------------------------


@pytest.fixture
def collector(tgaud_dsn):
    """Two collect_writer connections and a chat; cleaned up after."""
    setup = psycopg.connect(tgaud_dsn, autocommit=True)
    setup.execute("INSERT INTO tg.chat (chat_id, chat_type, title) VALUES (%s, 'channel', 'snap') "
                  "ON CONFLICT DO NOTHING", (CHAT,))
    conns = [psycopg.connect(tgaud_dsn) for _ in range(2)]
    for conn in conns:
        conn.execute("SET ROLE collect_writer")
        conn.execute("SET lock_timeout = '20s'")
        conn.commit()
    yield conns
    for conn in conns:
        conn.close()
    setup.execute("DELETE FROM tg.snapshot WHERE chat_id = %s", (CHAT,))
    setup.execute("DELETE FROM tg.chat WHERE chat_id = %s", (CHAT,))
    setup.close()


def _new_snapshot(conn) -> int:
    sid = conn.execute("INSERT INTO tg.snapshot (chat_id, started_at, method, reported_at_start) "
                       "VALUES (%s, now(), 'listing', 2) RETURNING snapshot_id", (CHAT,)).fetchone()[0]
    conn.commit()
    return sid


PAGE = ("INSERT INTO tg.snapshot_member (snapshot_id, user_id, seen_at) "
        "SELECT %s, u, now() FROM generate_series(1, 2) u ON CONFLICT DO NOTHING")
FINISH = "UPDATE tg.snapshot SET finished_at = now(), reported_at_end = 2, fetched = 2, complete = true WHERE snapshot_id = %s"


def _in_thread(fn):
    out: dict = {}

    def run():
        try:
            out["value"] = fn()
        except BaseException as e:
            out["error"] = e

    t = threading.Thread(target=run)
    t.start()
    return t, out


def test_page_first_makes_the_finalizer_wait_and_the_page_is_kept(collector, tgaud_dsn):
    page, final = collector
    sid = _new_snapshot(page)
    page.execute(PAGE, (sid,))  # uncommitted page
    t, out = _in_thread(lambda: (final.execute(FINISH, (sid,)), final.commit()))
    time.sleep(1.0)
    assert t.is_alive(), "the finalizer must wait for the uncommitted page"
    page.commit()
    t.join(20)
    assert "error" not in out, out
    with psycopg.connect(tgaud_dsn) as check:
        assert check.execute("SELECT count(*) FROM tg.snapshot_member WHERE snapshot_id = %s", (sid,)).fetchone()[0] == 2


def test_finalizer_first_makes_a_page_wait_and_fail(collector, tgaud_dsn):
    page, final = collector
    sid = _new_snapshot(page)
    final.execute(FINISH, (sid,))  # uncommitted finalizer
    t, out = _in_thread(lambda: (page.execute(PAGE, (sid,)), page.commit()))
    time.sleep(1.0)
    assert t.is_alive(), "the page must wait for the uncommitted finalizer"
    final.commit()
    t.join(20)
    assert isinstance(out.get("error"), psycopg.errors.ObjectNotInPrerequisiteState), out
    page.rollback()
    with psycopg.connect(tgaud_dsn) as check:
        assert check.execute("SELECT count(*) FROM tg.snapshot_member WHERE snapshot_id = %s", (sid,)).fetchone()[0] == 0


def test_collector_recipe_as_collect_writer(collector, tgaud_dsn):
    """design.md 5 step 3 says ON CONFLICT (snapshot_id, user_id) DO NOTHING; collect_writer may only INSERT,
    and a conflict target needs SELECT on its columns, so the recipe is the target-free form (docs/tgaud.md)."""
    page, _ = collector
    sid = _new_snapshot(page)
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        page.execute("INSERT INTO tg.snapshot_member (snapshot_id, user_id, seen_at) VALUES (%s, 1, now()) "
                     "ON CONFLICT (snapshot_id, user_id) DO NOTHING", (sid,))
    page.rollback()
    page.execute(PAGE, (sid,))
    page.commit()
    page.execute(PAGE, (sid,))  # a replayed page stores nothing new
    page.commit()
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        page.execute("SELECT count(*) FROM tg.snapshot_member")
    page.rollback()
    with psycopg.connect(tgaud_dsn) as check:
        assert check.execute("SELECT count(*) FROM tg.snapshot_member WHERE snapshot_id = %s", (sid,)).fetchone()[0] == 2
