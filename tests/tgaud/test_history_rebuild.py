"""rebuild_pairs on the database: the golden fixture, the snapshot boundary, pruning, sighting runs, late probe
answers, migrations, dirty markers, idempotence and the serialization with other writers (design.md 4-6)."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from tgaud import history

COLS = ", ".join(history.SPELL_COLUMNS)
ALL_PAIRS = """
SELECT chat_id, user_id FROM tg.member_event UNION SELECT chat_id, user_id FROM tg.sighting
UNION SELECT chat_id, user_id FROM tg.probe UNION SELECT chat_id, user_id FROM tg.membership"""


def spells(c, where: str = "true", params=()) -> list[tuple]:
    return c.execute(f"SELECT {COLS} FROM tg.membership WHERE {where} ORDER BY {COLS}", params).fetchall()


def test_golden_fixture_rebuilds_the_hand_written_spells(history_fixture):
    with psycopg.connect(history_fixture) as c:
        hand = spells(c)
        assert len(hand) > 30
        c.execute("DELETE FROM tg.membership")
        pairs = c.execute(ALL_PAIRS).fetchall()
        result = history.rebuild_pairs(c, pairs)
        rebuilt = spells(c)
        by_pair = lambda rows: {(r[0], r[1], r[6]): r for r in rows}  # noqa: E731 (chat, user, known_from)
        h, r = by_pair(hand), by_pair(rebuilt)
        assert set(h) == set(r), (sorted(set(h) - set(r)), sorted(set(r) - set(h)))
        diffs = [(k, col, h[k][i], r[k][i]) for k in sorted(h) for i, col in enumerate(history.SPELL_COLUMNS)
                 if h[k][i] != r[k][i]]
        assert not diffs, "\n".join(map(str, diffs))
        assert result.opened == len(hand) and result.closed == 0 and result.changed == 0
        # idempotent: a second call over every pair writes nothing and marks nothing dirty
        versions = c.execute("SELECT chat_id, version FROM tg.rollup_dirty ORDER BY chat_id").fetchall()
        assert history.rebuild_pairs(c, pairs) == history.RebuildResult()
        assert c.execute("SELECT chat_id, version FROM tg.rollup_dirty ORDER BY chat_id").fetchall() == versions
        c.rollback()


# --- scenarios on a fresh chat (session database, rolled back) ------------------------------------------------

from history_world import CHAT, World, at  # noqa: E402

DAY = 60 * 24
H1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
H2 = datetime(2026, 9, 20, tzinfo=timezone.utc)


@pytest.fixture
def world(tgaud_dsn):
    with psycopg.connect(tgaud_dsn) as c:
        yield World(c)
        c.rollback()


def test_rebuild_without_evidence_changes_nothing(world):
    assert history.rebuild_pairs(world.c, []) == history.RebuildResult()
    assert world.rebuild([world.user(1)]) == history.RebuildResult()
    assert world.spells(1) == []


def test_rebuild_refuses_autocommit(tgaud_dsn):
    with psycopg.connect(tgaud_dsn, autocommit=True) as c:
        with pytest.raises(ValueError, match="autocommit"):
            history.rebuild_pairs(c, [(CHAT, 1)])


def test_rebuild_works_with_any_row_factory(tgaud_dsn):
    from psycopg.rows import dict_row
    with psycopg.connect(tgaud_dsn, row_factory=dict_row) as c:
        w = World(c)
        w.join(1, at(0))
        assert w.rebuild() == history.RebuildResult(opened=1)
        c.rollback()


def test_counts_and_dirty_markers(world):
    w = world
    w.join(1, at(0))
    assert w.rebuild() == history.RebuildResult(opened=1)
    marker = w.c.execute("SELECT from_day, version FROM tg.rollup_dirty WHERE chat_id = %s", (CHAT,)).fetchone()
    assert marker[0] == at(0).date()
    assert w.rebuild() == history.RebuildResult()  # nothing changed: nothing written, nothing marked
    assert w.c.execute("SELECT from_day, version FROM tg.rollup_dirty WHERE chat_id = %s", (CHAT,)).fetchone() == marker
    w.c.execute("UPDATE tg.rollup_dirty SET from_day = '2026-10-01' WHERE chat_id = %s", (CHAT,))
    w.leave(1, at(DAY * 40))
    assert w.rebuild() == history.RebuildResult(closed=1)
    assert w.c.execute("SELECT from_day FROM tg.rollup_dirty WHERE chat_id = %s", (CHAT,)).fetchone()[0] == at(0).date()
    w.event(1, at(DAY * 2), "member", "administrator")  # L moves inside the closed spell
    assert w.rebuild() == history.RebuildResult(changed=1)
    w.join(1, at(DAY * 41))
    assert w.rebuild() == history.RebuildResult(opened=1)
    assert [s.last_confirmed_at for s in w.spells(1)] == [at(DAY * 2), at(DAY * 41)]


def test_ingest_style_calls_match_one_full_rebuild(world):
    """The bot rebuilds one pair per update; the result equals one rebuild of all evidence at the end."""
    w = world
    for t, kind in [(0, "join"), (DAY, "leave"), (2 * DAY, "join")]:
        getattr(w, kind)(2, at(t))
        w.rebuild([2])
    incremental = w.spells(2)
    w.c.execute("DELETE FROM tg.membership WHERE chat_id = %s", (CHAT,))
    w.rebuild([2])
    assert w.spells(2) == incremental and len(incremental) == 2


# --- the reconcile boundary (plan review 2) ----------------------------------------------------------------

def test_mid_reconcile_listed_pair_is_never_absent(world):
    w = world
    for u in (5, 10, 20, 30):
        w.join(u, at(0))
    # graded good, chunk 1 applied users <= 10; 20 is listed but not folded yet; 5 and 30 are not listed
    sid = w.snapshot(at(DAY), {10: (at(DAY + 1), None), 20: (at(DAY + 2), None)}, through=10)
    w.rebuild()
    suspect = {u: w.spells(u)[0].suspect_since for u in (5, 10, 20, 30)}
    assert suspect == {5: at(DAY), 10: None, 20: None, 30: None}
    ev = history.load_evidence(w.c, [(CHAT, 20), (CHAT, 30)])
    assert ev[(CHAT, 20)].absences == [] and ev[(CHAT, 30)].absences == []
    w.finish_reconcile(sid, [20])
    w.rebuild()
    suspect = {u: w.spells(u)[0].suspect_since for u in (5, 10, 20, 30)}
    assert suspect == {5: at(DAY), 10: None, 20: None, 30: at(DAY)}


def test_absent_pair_of_a_committed_chunk_stays_suspect_across_restart_and_finalization(migrated_dsn):
    with psycopg.connect(migrated_dsn) as c:
        w = World(c)
        w.join(5, at(0))
        w.join(10, at(0))
        sid = w.snapshot(at(DAY), {10: (at(DAY + 1), None)}, through=10)
        w.rebuild()
        c.commit()
    with psycopg.connect(migrated_dsn) as c:  # a restart: a new process, a new connection
        w = World(c)
        w.users |= {5, 10}
        assert w.rebuild() == history.RebuildResult()
        assert w.spells(5)[0].suspect_since == at(DAY)
        w.finish_reconcile(sid)
        assert w.rebuild() == history.RebuildResult()
        assert w.spells(5)[0].suspect_since == at(DAY)
        c.commit()


# --- pruning, run extension and the sighting representation (plan review 2, design review 1) ----------------

def test_replay_is_identical_after_pruning_with_a_late_join_between_interior_points(world):
    w = world
    for k in range(4):
        w.snapshot(at(k * 7 * DAY), {3: (at(k * 7 * DAY + 10), H1)})
    w.join(3, at(8 * DAY))  # arrives late: between the 2nd and 3rd point of the run
    w.rebuild()
    before = w.spells(3)
    assert [(s.start_source, s.known_from, s.left_after, s.left_before) for s in before] == [
        ("snapshot", at(10), at(7 * DAY + 10), at(8 * DAY)), ("event", at(8 * DAY), None, None)]
    assert before[1].last_confirmed_at == at(21 * DAY + 10)
    assert w.prune() == 4
    assert w.rebuild() == history.RebuildResult()
    assert w.spells(3) == before


def test_absence_with_a_pending_probe_is_suspect_after_pruning(world):
    w = world
    w.snapshot(at(0), {4: (at(10), H1), 6: (at(11), H1)})
    sid = w.snapshot(at(7 * DAY), {6: (at(7 * DAY + 11), H1)})
    w.c.execute("""INSERT INTO tg.probe (chat_id, user_id, reason, snapshot_id, absent_since)
                   VALUES (%s, 4, 'snapshot_absent', %s, %s)""", (CHAT, sid, at(7 * DAY)))
    w.rebuild()
    assert w.spells(4)[0].suspect_since == at(7 * DAY)
    w.prune()
    assert w.rebuild() == history.RebuildResult()
    assert w.spells(4)[0].suspect_since == at(7 * DAY)


def test_middle_page_beyond_grace_after_an_exit_is_S4_before_and_after_pruning_and_extension(world):
    w = world
    exit_at = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
    w.join(7, at(-60))
    w.snapshot(exit_at - timedelta(minutes=60), {7: (exit_at - timedelta(minutes=50), H1)})
    w.leave(7, exit_at)
    # the snapshot started inside G after the exit, but the user's page came after G
    w.snapshot(exit_at + timedelta(minutes=5), {7: (exit_at + timedelta(minutes=15), H1)})
    w.snapshot(exit_at + timedelta(days=7), {7: (exit_at + timedelta(days=7, minutes=3), H1)})
    w.rebuild()
    expected = [(s.start_source, s.known_from, s.end_source) for s in w.spells(7)]
    assert expected == [("event", at(-60), "event"), ("snapshot", exit_at + timedelta(minutes=15), None)]
    assert w.c.execute("SELECT times_seen, cardinality(interior_seen_at) FROM tg.sighting WHERE user_id = 7"
                       ).fetchone() == (3, 1)  # one run across the exit; the middle page is interior
    w.prune()
    w.snapshot(exit_at + timedelta(days=14), {7: (exit_at + timedelta(days=14, minutes=3), H1)})
    w.rebuild()
    assert [(s.start_source, s.known_from, s.end_source) for s in w.spells(7)] == expected


def test_page_just_inside_grace_stays_closed_and_one_at_grace_reopens(world):
    w = world
    exit_at = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
    for u, page in [(8, exit_at + timedelta(minutes=9, seconds=59, milliseconds=800)), (9, exit_at + history.GRACE)]:
        w.join(u, at(-60))
        w.leave(u, exit_at)
    w.snapshot(exit_at - timedelta(minutes=1), {8: (exit_at + timedelta(minutes=9, seconds=59, milliseconds=800), H1),
                                                9: (exit_at + history.GRACE, H1)})
    w.rebuild()
    assert len(w.spells(8)) == 1 and w.spells(8)[0].left_at == exit_at  # S3: stays closed
    assert [s.known_from for s in w.spells(9)] == [at(-60), exit_at + history.GRACE]  # S4: exactly at G
    later = exit_at + timedelta(days=7, minutes=3)
    w.snapshot(exit_at + timedelta(days=7), {8: (later, H1), 9: (later, H1)})
    w.prune()
    w.rebuild()
    # the S3 page stays ignored inside the extended run; the next listing a week later is the missed rejoin
    assert [(s.known_from, s.end_source) for s in w.spells(8)] == [(at(-60), "event"), (later, None)]
    assert [s.known_from for s in w.spells(9)] == [at(-60), exit_at + history.GRACE]


def test_out_of_order_partial_listing(world):
    w = world
    t = datetime(2026, 9, 2, 0, 0, tzinfo=timezone.utc)
    # a partial listing started first and finished last; its page for 11 came after the good listing's page
    w.snapshot(t, {11: (t + timedelta(minutes=110), H1)}, finished=t + timedelta(minutes=120), good=False)
    w.snapshot(t + timedelta(minutes=60), {11: (t + timedelta(minutes=70), H1)}, finished=t + timedelta(minutes=90))
    runs = w.c.execute("SELECT first_seen_at, times_seen FROM tg.sighting WHERE user_id = 11 ORDER BY first_snapshot_id"
                       ).fetchall()
    assert runs == [(t + timedelta(minutes=110), 1), (t + timedelta(minutes=70), 1)]  # an earlier page: a new run
    w.rebuild()
    (s,) = w.spells(11)
    assert (s.start_source, s.known_from, s.last_confirmed_at, s.suspect_since) == (
        "snapshot", t + timedelta(minutes=70), t + timedelta(minutes=110), None)
    w.leave(11, t + timedelta(minutes=105))  # left between the pages, 5 min before the partial page (S3)
    w.rebuild()
    (s,) = w.spells(11)
    assert (s.known_from, s.last_confirmed_at, s.left_at) == (
        t + timedelta(minutes=70), t + timedelta(minutes=70), t + timedelta(minutes=105))


def test_same_page_time_in_two_runs_commits_and_leaves_the_gap_uncovered(world):
    w = world
    t = datetime(2026, 9, 3, 10, 0, tzinfo=timezone.utc)
    w.join(12, t - timedelta(hours=1))
    w.join(13, t - timedelta(hours=1))
    page = t + timedelta(minutes=30)
    s1 = w.snapshot(t, {12: (page, H1)}, finished=t + timedelta(minutes=40))
    s2 = w.snapshot(t + timedelta(minutes=10), {13: (t + timedelta(minutes=12), H1)},
                    finished=t + timedelta(minutes=40))
    s3 = w.snapshot(t + timedelta(minutes=20), {12: (page, H1)}, finished=t + timedelta(minutes=40))
    rows = w.c.execute("SELECT first_snapshot_id, first_seen_at FROM tg.sighting WHERE user_id = 12 ORDER BY 1"
                       ).fetchall()
    assert rows == [(s1, page), (s3, page)]
    ev = history.load_evidence(w.c, [(CHAT, 12)])[(CHAT, 12)]
    assert [a.snapshot_id for a in ev.absences] == [s2]  # the good snapshot between the runs is not covered
    trace: list = []
    history.replay(ev, trace)
    assert [r for r, _ in trace] == ["E1", "A1", "S2", "S2"]
    w.rebuild()
    before = (w.c.execute("SELECT * FROM tg.sighting WHERE user_id = 12 ORDER BY first_snapshot_id").fetchall(),
              w.spells(12))
    for sid in (s1, s3):  # a retried chunk folds the same rows again
        w.fold(sid, 12)
    runs = w.c.execute("SELECT * FROM tg.sighting WHERE user_id = 12 ORDER BY first_snapshot_id").fetchall()
    assert runs == before[0]
    assert w.rebuild() == history.RebuildResult() and w.spells(12) == before[1]


def test_each_spell_keeps_its_own_hint_after_a_late_exit_extension_and_pruning(world):
    w = world
    w.snapshot(at(0), {14: (at(10), H1)})
    w.snapshot(at(20 * DAY), {14: (at(20 * DAY + 10), H2)})  # rejoined meanwhile: Telegram's date changed
    assert w.c.execute("SELECT count(*) FROM tg.sighting WHERE user_id = 14").fetchone()[0] == 2  # split by hint
    w.rebuild()
    assert [(s.known_from, s.joined_hint) for s in w.spells(14)] == [(at(10), H1)]
    w.leave(14, at(10 * DAY))  # the exit between the listings arrives late
    w.rebuild()
    expected = [(at(10), H1, "event"), (at(20 * DAY + 10), H2, None)]
    assert [(s.known_from, s.joined_hint, s.end_source) for s in w.spells(14)] == expected
    w.snapshot(at(27 * DAY), {14: (at(27 * DAY + 10), H2)})
    w.prune()
    w.rebuild()
    assert [(s.known_from, s.joined_hint, s.end_source) for s in w.spells(14)] == expected
    assert w.spells(14)[1].last_confirmed_at == at(27 * DAY + 10)


def test_run_extension_one_two_three_equals_a_fresh_run(world):
    w = world
    pages = [at(10), at(7 * DAY + 10), at(14 * DAY + 10)]
    states = []
    for k, p in enumerate(pages):
        w.snapshot(at(k * 7 * DAY), {15: (p, H1)})
        states.append(w.c.execute("SELECT first_seen_at, interior_seen_at, last_seen_at, times_seen FROM tg.sighting "
                                  "WHERE user_id = 15").fetchone())
    assert states == [(pages[0], [], pages[0], 1), (pages[0], [], pages[1], 2), (pages[0], [pages[1]], pages[2], 3)]
    w.rebuild()
    extended = w.spells(15)
    w.c.execute("DELETE FROM tg.membership WHERE user_id = 15")
    w.c.execute("DELETE FROM tg.sighting WHERE user_id = 15")
    first, last = w.c.execute("SELECT min(snapshot_id), max(snapshot_id) FROM tg.snapshot WHERE chat_id = %s",
                              (CHAT,)).fetchone()
    w.c.execute("""INSERT INTO tg.sighting (chat_id, user_id, first_seen_at, first_snapshot_id, last_seen_at,
                                            last_snapshot_id, times_seen, joined_date, interior_seen_at)
                   VALUES (%s, 15, %s, %s, %s, %s, 3, %s, %s)""",
                (CHAT, pages[0], first, pages[2], last, H1, [pages[1]]))
    w.rebuild()
    assert w.spells(15) == extended and extended[0].last_confirmed_at == pages[2]


def test_sighting_points_check(world):
    w = world
    sid = w.snapshot(at(0), {})
    w.user(16)
    ins = """INSERT INTO tg.sighting (chat_id, user_id, first_seen_at, first_snapshot_id, last_seen_at,
                                      last_snapshot_id, times_seen, interior_seen_at)
             VALUES (%s, 16, %s, %s, %s, %s, %s, %s::timestamptz[])"""
    good = [(at(1), at(1), 1, "{}"), (at(2), at(3), 2, "{}"), (at(4), at(6), 3, "{%s}" % at(5).isoformat())]
    for first, last, n, interior in good:
        with w.c.transaction():
            w.c.execute(ins, (CHAT, first, sid, last, sid, n, interior))
            w.c.execute("DELETE FROM tg.sighting WHERE user_id = 16")
    bad = [(at(1), at(3), 3, "{}"), (at(1), at(3), 3, "{NULL}"), (at(1), at(4), 4, "{{%s},{%s}}" % (at(2), at(3))),
           (at(1), at(3), 3, "[0:0]={%s}" % at(2)), (at(1), at(2), 1, "{}"), (at(1), at(1), 1, "{%s}" % at(1)),
           (at(1), at(3), 2, "{%s}" % at(2))]
    for first, last, n, interior in bad:
        with pytest.raises(psycopg.errors.CheckViolation):
            with w.c.transaction():
                w.c.execute(ins, (CHAT, first, sid, last, sid, n, interior))


def test_probe_sent_after_answer_is_rejected(world):
    with pytest.raises(psycopg.errors.CheckViolation):
        with world.c.transaction():
            world.probe(17, at(0), "in", sent=at(1))


# --- probes that straddle a transition (plan review 3, design review 1) ---------------------------------------

def test_probe_answer_counts_unless_an_event_lies_in_its_interval(world):
    w = world
    j = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
    w.join(18, j)
    sent = j + timedelta(milliseconds=800)  # the join is dated 12:00:00, the call went out after it
    w.probe(18, sent + timedelta(milliseconds=300), "out", sent=sent)
    w.rebuild()
    (s,) = w.spells(18)
    assert (s.end_source, s.left_before) == ("probe", sent + timedelta(milliseconds=300))
    w.join(19, j)
    w.probe(19, j + timedelta(seconds=1), "out", sent=j - timedelta(seconds=1))  # the join falls inside: ignored
    w.probe(19, None, "error")  # P4: a failed call is no evidence
    w.rebuild()
    (s,) = w.spells(19)
    assert s.end_source is None


# --- migrations ---------------------------------------------------------------------------------------------

def test_M1_move_chat_history_then_rebuild_absorbs_the_echoes(world):
    w = world
    old = -4009
    w.add_chat(old, "group")
    m = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
    w.join(21, at(0), chat=old)
    w.join(22, at(DAY), chat=old)
    history.rebuild_pairs(w.c, [(old, 21), (old, 22)])
    w.join(21, m + timedelta(seconds=5))  # join echoes in the new supergroup
    w.join(22, m + timedelta(seconds=5))
    w.leave(22, m + timedelta(days=5))
    w.c.execute("SELECT tg.move_chat_history(%s, %s, %s)", (old, CHAT, m))
    w.rebuild([21, 22])
    (a,) = w.spells(21)
    assert (a.joined_at, a.last_confirmed_at, a.end_source) == (at(0), m + timedelta(seconds=5), None)
    (b,) = w.spells(22)
    assert (b.joined_at, b.left_at) == (at(DAY), m + timedelta(days=5))
    assert w.c.execute("SELECT count(*) FROM tg.membership WHERE chat_id = %s", (old,)).fetchone()[0] == 0


def test_M2_migration_to_an_untracked_chat_closes_by_migration(world):
    w = world
    new = -1009000000077
    w.add_chat(new)
    m = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
    w.join(23, at(0))
    w.join(24, at(0))
    assert w.rebuild() == history.RebuildResult(opened=2)
    w.c.execute("INSERT INTO tg.chat_alias (old_chat_id, chat_id, migrated_at, moved_at) VALUES (%s, %s, %s, NULL)",
                (CHAT, new, m))
    assert w.rebuild() == history.RebuildResult(closed=2)
    assert [(s.end_source, s.end_reason, s.left_after, s.left_before) for s in w.spells(23)] == [
        ("migration", "migrated", at(0), m)]
    w.leave(24, m - timedelta(seconds=30))  # a late old-id exit dated before the migration closes exactly
    w.rebuild()
    assert [(s.end_source, s.left_at) for s in w.spells(24)] == [("event", m - timedelta(seconds=30))]


# --- serialization with other writers (plan review 1) ---------------------------------------------------------

@pytest.mark.parametrize("writer_locks", [True, False])
def test_concurrent_writer_waits_and_the_result_equals_a_fresh_replay(migrated_dsn, monkeypatch, writer_locks):
    with psycopg.connect(migrated_dsn) as c:
        w = World(c)
        w.join(25, at(0))
        c.commit()
    paused, resume = threading.Event(), threading.Event()
    real = history.build_spells

    def slow(ev, *rest):
        if threading.current_thread().name == "A":
            paused.set()
            assert resume.wait(30)
        return real(ev, *rest)

    monkeypatch.setattr(history, "build_spells", slow)
    errors: list[BaseException] = []

    def a() -> None:
        try:
            with psycopg.connect(migrated_dsn) as c:
                history.rebuild_pairs(c, [(CHAT, 25)])
                c.commit()
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    ta = threading.Thread(target=a, name="A")
    ta.start()
    assert paused.wait(30)  # A holds the lock and has read the evidence
    with psycopg.connect(migrated_dsn) as c:
        w = World(c)
        w.users.add(25)
        c.execute("SET lock_timeout = '30s'")
        waited: list[bool] = []

        def release_later() -> None:
            # A goes on only once B waits for A's chat lock (an advisory lock of this database not granted)
            with psycopg.connect(migrated_dsn, autocommit=True) as probe:
                for _ in range(300):
                    if probe.execute("""SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
                                          AND database = (SELECT oid FROM pg_database
                                                           WHERE datname = current_database())""").fetchone()[0]:
                        waited.append(True)
                        break
                    threading.Event().wait(0.05)
            resume.set()

        threading.Thread(target=release_later).start()
        if writer_locks:
            history.lock_chats(c, [CHAT])  # waits for A
        w.leave(25, at(DAY))
        if not writer_locks:
            c.commit()  # evidence committed while A still holds its stale read
        history.rebuild_pairs(c, [(CHAT, 25)])  # waits for A, then reads A's spells and the new event
        c.commit()
        ta.join(30)
        assert not errors, errors
        assert waited == [True]
        got = w.spells(25)
        c.execute("DELETE FROM tg.membership WHERE chat_id = %s", (CHAT,))
        history.rebuild_pairs(c, [(CHAT, 25)])
        assert w.spells(25) == got and [(s.end_source, s.left_at) for s in got] == [("event", at(DAY))]
        c.rollback()


# --- review 1: times compare as instants in any session zone ---------------------------------------------------

FALL_JOIN = datetime(2026, 10, 25, 0, 55, tzinfo=timezone.utc)  # 02:55 CEST
FALL_EXIT = datetime(2026, 10, 25, 1, 5, tzinfo=timezone.utc)  # 02:05 CET, after the fall-back at 01:00Z


@pytest.mark.parametrize("set_zone", ["SET TimeZone = 'Europe/Belgrade'", "SET LOCAL TimeZone = 'Europe/Belgrade'"])
def test_fall_back_in_a_local_session_gives_one_closed_stay_and_keeps_the_zone(tgaud_dsn, set_zone):
    with psycopg.connect(tgaud_dsn) as c:
        w = World(c)
        c.execute(set_zone)
        w.join(31, FALL_JOIN)
        w.leave(31, FALL_EXIT)
        assert w.rebuild([31]) == history.RebuildResult(opened=1)
        assert c.execute("SHOW TimeZone").fetchone()[0] == "Europe/Belgrade"  # the caller's zone is back
        (s,) = w.spells(31)
        assert (s.joined_at, s.left_at, s.end_source) == (FALL_JOIN, FALL_EXIT, "event")
        assert w.rebuild([31]) == history.RebuildResult()  # a second call in the local zone changes nothing
        c.execute("SET LOCAL TimeZone = 'UTC'")
        assert w.rebuild([31]) == history.RebuildResult()  # nor one in UTC
        ev = history.load_evidence(c, [(CHAT, 31)])[(CHAT, 31)]
        assert [e.event_at.tzinfo for e in ev.events] == [timezone.utc, timezone.utc]
        c.rollback()


def test_zone_is_restored_after_a_python_error_and_by_rollback_after_a_database_error(migrated_dsn, monkeypatch):
    with psycopg.connect(migrated_dsn) as c:  # commits: a database of its own
        c.execute("SET TimeZone = 'Europe/Belgrade'")
        w = World(c)
        w.join(32, FALL_JOIN)
        c.commit()

        def boom(ev, *rest):
            raise RuntimeError("injected in the replay")

        monkeypatch.setattr(history, "build_spells", boom)
        with pytest.raises(RuntimeError, match="injected"):
            w.rebuild([32])
        assert c.execute("SHOW TimeZone").fetchone()[0] == "Europe/Belgrade"  # transaction usable, zone back
        c.rollback()
        monkeypatch.undo()

        real = history._load_chunk

        def fail(conn, pairs):
            real(conn, pairs)
            conn.execute("SELECT 1 / 0")

        monkeypatch.setattr(history, "_load_chunk", fail)
        with pytest.raises(psycopg.errors.DivisionByZero):
            w.rebuild([32])
        c.rollback()
        assert c.execute("SHOW TimeZone").fetchone()[0] == "Europe/Belgrade"


def test_golden_fixture_in_a_local_session_zone(history_fixture):
    with psycopg.connect(history_fixture) as c:
        hand = [history.Spell(*(v.astimezone(timezone.utc) if isinstance(v, datetime) else v for v in r))
                for r in spells(c)]
        c.execute("SET LOCAL TimeZone = 'America/New_York'")
        c.execute("DELETE FROM tg.membership")
        pairs = c.execute(ALL_PAIRS).fetchall()
        history.rebuild_pairs(c, pairs)
        rebuilt = [history.Spell(*(v.astimezone(timezone.utc) if isinstance(v, datetime) else v for v in r))
                   for r in spells(c)]
        assert sorted(rebuilt) == sorted(hand)
        c.execute("SET LOCAL TimeZone = 'UTC'")
        assert history.rebuild_pairs(c, pairs) == history.RebuildResult()
        c.rollback()


# --- review 1: a sighting counts only once its snapshot is applied to the user ---------------------------------

def test_singleton_folded_above_the_cursor_is_not_evidence_until_the_cursor_passes(world):
    w = world
    # in one reconcile transaction: graded, cursor 4, but user 8's run already folded (ahead of the cursor)
    sid = w.snapshot(at(0), {3: (at(5), H1), 8: (at(6), H1)}, through=4, fold_through="all")
    assert w.c.execute("SELECT count(*) FROM tg.sighting WHERE user_id = 8").fetchone()[0] == 1
    w.rebuild([3, 8])
    assert w.spells(8) == [] and len(w.spells(3)) == 1
    assert history.load_evidence(w.c, [(CHAT, 8)])[(CHAT, 8)].sightings == []
    with w.c.transaction(force_rollback=True):  # a chunk that advances the cursor and is interrupted
        w.c.execute("UPDATE tg.snapshot SET applied_through_user_id = 8 WHERE snapshot_id = %s", (sid,))
        w.rebuild([8])
        assert [s.start_source for s in w.spells(8)] == ["snapshot"]
    assert w.rebuild([8]) == history.RebuildResult() and w.spells(8) == []  # rolled back: as before
    w.c.execute("UPDATE tg.snapshot SET applied_through_user_id = 8 WHERE snapshot_id = %s", (sid,))
    assert w.rebuild([8]) == history.RebuildResult(opened=1)
    w.finish_reconcile(sid)
    assert w.rebuild([3, 8]) == history.RebuildResult()  # finalization changes nothing


def test_extension_above_the_cursor_keeps_the_applied_points(world):
    w = world
    w.snapshot(at(0), {9: (at(5), H1)})
    w.rebuild([9])
    assert w.spells(9)[0].last_confirmed_at == at(5)
    # the next listing is mid-apply with cursor 4; user 9's run is extended ahead of the cursor
    sid = w.snapshot(at(7 * DAY), {9: (at(7 * DAY + 5), H1)}, through=4, fold_through="all")
    assert w.c.execute("SELECT times_seen FROM tg.sighting WHERE user_id = 9").fetchone()[0] == 2
    assert w.rebuild([9]) == history.RebuildResult()
    assert w.spells(9)[0].last_confirmed_at == at(5)
    w.c.execute("UPDATE tg.snapshot SET applied_through_user_id = 9 WHERE snapshot_id = %s", (sid,))
    assert w.rebuild([9]) == history.RebuildResult(changed=1)
    assert w.spells(9)[0].last_confirmed_at == at(7 * DAY + 5)
    w.c.execute("UPDATE tg.snapshot SET applied_through_user_id = NULL WHERE snapshot_id = %s", (sid,))
    w.rebuild([9])
    assert w.spells(9)[0].last_confirmed_at == at(5)  # a graded snapshot with nothing applied yet
    w.finish_reconcile(sid)
    w.rebuild([9])
    assert w.spells(9)[0].last_confirmed_at == at(7 * DAY + 5)


# --- review 2: a snapshot graded rejected is no evidence at any position of a run (conductor) --------------------

REJ_DAY = datetime(2026, 9, 5, tzinfo=timezone.utc)


def hm(h: int, m: int = 0) -> datetime:
    return REJ_DAY + timedelta(hours=h, minutes=m)


def test_rejected_interior_listing_is_no_evidence_after_pruning(world):
    w = world
    w.join(40, hm(9))
    sids = [w.snapshot(hm(k), {40: (hm(k, 1), H1)}, finished=hm(k, 20)) for k in (10, 11, 12)]
    w.leave(40, hm(10, 30))  # the exact exit falls between the first and the middle listing
    w.rebuild([40])
    assert [(s.known_from, s.end_source) for s in w.spells(40)] == [(hm(9), "event"), (hm(11, 1), None)]
    assert w.c.execute("SELECT times_seen FROM tg.sighting WHERE user_id = 40").fetchone()[0] == 3
    w.prune()
    w.c.execute("UPDATE tg.rollup_dirty SET from_day = '2026-12-01' WHERE chat_id = %s", (CHAT,))
    version = w.c.execute("SELECT version FROM tg.rollup_dirty WHERE chat_id = %s", (CHAT,)).fetchone()[0]
    with w.c.transaction(force_rollback=True):  # the owner's rejection, interrupted: nothing changes
        w.reject(sids[1])
        w.rebuild([40])
        assert w.spells(40)[1].known_from == hm(12, 1)
    assert w.rebuild([40]) == history.RebuildResult()
    assert w.spells(40)[1].known_from == hm(11, 1)
    w.reject(sids[1])
    assert w.rebuild([40]) == history.RebuildResult(opened=1, changed=1)  # the rejoin moves to the next listing
    assert [(s.known_from, s.end_source) for s in w.spells(40)] == [(hm(9), "event"), (hm(12, 1), None)]
    from_day, new_version = w.c.execute("SELECT from_day, version FROM tg.rollup_dirty WHERE chat_id = %s",
                                        (CHAT,)).fetchone()
    assert from_day == REJ_DAY.date() and new_version > version
    assert w.rebuild([40]) == history.RebuildResult()


def test_rejected_end_point_stays_out_when_the_run_is_extended(world):
    w = world
    w.join(41, hm(9))
    w.snapshot(hm(10), {41: (hm(10, 1), H1)}, finished=hm(10, 20))
    w.leave(41, hm(10, 30))
    s2 = w.snapshot(hm(11), {41: (hm(11, 1), H1)}, finished=hm(11, 20))
    w.reject(s2)
    w.rebuild([41])
    assert [(s.known_from, s.end_source) for s in w.spells(41)] == [(hm(9), "event")]
    w.snapshot(hm(12), {41: (hm(12, 1), H1)}, finished=hm(12, 20))  # extends the run: s2's point is now interior
    assert w.c.execute("SELECT times_seen FROM tg.sighting WHERE user_id = 41").fetchone()[0] == 3
    w.rebuild([41])
    assert [(s.known_from, s.end_source) for s in w.spells(41)] == [(hm(9), "event"), (hm(12, 1), None)]


def test_rejected_good_snapshot_gives_no_absence(world):
    w = world
    w.snapshot(hm(10), {42: (hm(10, 1), H1), 43: (hm(10, 2), H1)})
    s2 = w.snapshot(hm(11), {43: (hm(11, 2), H1)})  # 42 is missing from a good snapshot
    w.rebuild([42])
    assert w.spells(42)[0].suspect_since == hm(11)
    w.reject(s2)
    assert w.rebuild([42]) == history.RebuildResult(changed=1)
    assert w.spells(42)[0].suspect_since is None


def test_run_that_does_not_match_the_chat_snapshots_keeps_only_its_ends(world):
    w = world
    sids = [w.snapshot(hm(k), {} if k == 11 else {44: (hm(k, 1), H1)}) for k in (10, 11, 12, 13)]
    # a writer that broke the 003 contract: one run across a snapshot that did not list the user
    w.c.execute("DELETE FROM tg.sighting WHERE user_id = 44")
    w.c.execute("""INSERT INTO tg.sighting (chat_id, user_id, first_seen_at, first_snapshot_id, last_seen_at,
                                            last_snapshot_id, times_seen, joined_date, interior_seen_at)
                   VALUES (%s, 44, %s, %s, %s, %s, 3, %s, %s)""",
                (CHAT, hm(10, 1), sids[0], hm(13, 1), sids[3], H1, [hm(12, 1)]))
    ev = history.load_evidence(w.c, [(CHAT, 44)])[(CHAT, 44)]
    assert [p.seen_at for p in ev.sightings] == [hm(10, 1), hm(13, 1)]  # the interior point is not attributed
