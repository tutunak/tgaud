"""The reconciler job (design.md 4-6 and the reconcile task's design-2 with its review amendments): grading,
the state table through real snapshots, probes, late listings, migrations, erasure, G1, contradictions, pruning,
the fixture of verify.md, equivalence with a full rebuild, and resuming after a stop."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from reconcile_lab import CHAT, Lab, at, job
from tgaud import history
from tgaud.jobs import reconciler

UTC = timezone.utc
H = 60  # minutes


@pytest.fixture
def lab(tgaud_dsn):
    with psycopg.connect(tgaud_dsn) as c:
        yield Lab(c)
        c.rollback()


def versions(c) -> list[tuple]:
    return c.execute("SELECT chat_id, version FROM tg.rollup_dirty ORDER BY chat_id").fetchall()


def assert_equivalent(lab: Lab) -> None:
    """A full rebuild over every pair changes nothing and marks nothing dirty."""
    before = versions(lab.c)
    assert lab.rebuild_all() == history.RebuildResult()
    assert versions(lab.c) == before


# --- grading (design.md 5) ---------------------------------------------------------------------------------------

@pytest.mark.parametrize("reported, fetched, complete, expected", [
    (20000, 19980, True, "good"),       # shortfall 20 = 0.1 % of 20 000
    (20000, 19979, True, "partial"),
    (5000, 4990, True, "good"),         # the floor of 10
    (5000, 4989, True, "partial"),
    (1000000, 999000, True, "good"),
    (1000000, 998999, True, "partial"),
    (100, 120, True, "good"),           # more than reported
    (20000, 19990, False, "partial"),   # interrupted
    (None, 3, True, "partial"),         # no count to compare with
])
def test_grade_at_the_thresholds(lab, reported, fetched, complete, expected):
    sid = lab.collect(at(0), {1: at(1), 2: at(2), 3: at(3)}, fetched=fetched, reported=reported or 0,
                      complete=complete)
    if reported is None:
        lab.c.execute("UPDATE tg.snapshot SET reported_at_start = NULL, reported_at_end = NULL WHERE snapshot_id = %s",
                      (sid,))
    assert reconciler.grade(lab.c, sid) == expected
    assert lab.snapshot(sid)["quality"] == expected


def test_grade_uses_the_larger_count(lab):
    sid = lab.collect(at(0), {1: at(1)}, reported=4990, reported_end=5000, fetched=4990)
    assert reconciler.grade(lab.c, sid) == "good"
    sid = lab.collect(at(60), {1: at(61)}, reported=4990, reported_end=5001, fetched=4990)
    assert reconciler.grade(lab.c, sid) == "partial"


@pytest.mark.parametrize("reported, absent, expected", [
    (1000, 20, "good"), (1000, 21, "partial"),   # 2 % of 1 000
    (100, 10, "good"), (100, 11, "partial"),     # the floor of 10 (agreed with the conductor)
])
def test_absent_candidates_over_two_percent_downgrade(lab, reported, absent, expected):
    for u in range(1, absent + 1):
        lab.join(u, at(-60))
    lab.join(500, at(-60))
    lab.join(501, at(30))  # joined during the listing: not a candidate (A2)
    lab.rebuild()
    sid = lab.collect(at(0), {500: at(5)}, reported=reported, fetched=reported)
    assert reconciler.grade(lab.c, sid) == expected


def test_unlisted_users_confirmed_after_the_start_are_no_candidates(lab):
    for u in range(1, 15):
        lab.join(u, at(-60))
        lab.event(u, at(10), "member", "administrator")  # promoted during the listing: L >= started_at
    lab.rebuild()
    sid = lab.collect(at(0), {}, reported=100, fetched=100)
    assert reconciler.grade(lab.c, sid) == "good"


# --- the state table through real snapshots (design.md 4) --------------------------------------------------------

def test_every_s_and_a_row_through_a_snapshot(lab):
    t = at(0)
    lab.join(2, t - timedelta(days=3))                                      # S2: open, seen again
    lab.join(3, t - timedelta(days=3)); lab.leave(3, t + timedelta(minutes=5))  # S3: seen 5 min after the exit
    lab.join(4, t - timedelta(days=3)); lab.leave(4, t - timedelta(minutes=10))  # S4: seen 20 min after the exit
    lab.leave(5, t + timedelta(hours=2))                                    # S5: an exit after the sighting
    lab.join(6, t - timedelta(days=3))                                      # A1: open, not listed
    lab.join(7, t + timedelta(minutes=3))                                   # A2: joined during the listing
    lab.join(8, t - timedelta(days=3)); lab.leave(8, t - timedelta(days=1))  # A3: closed, not listed
    lab.rebuild()
    hint = datetime(2025, 1, 1, tzinfo=UTC)
    sid = lab.collect(t, {1: (t + timedelta(minutes=10), hint), 2: t + timedelta(minutes=10),
                          3: t + timedelta(minutes=10), 4: t + timedelta(minutes=10), 5: t + timedelta(minutes=10)},
                      reported=7, fetched=7)
    units = lab.run()
    assert units[:2] == ["grade", "apply"] and lab.snapshot(sid)["quality"] == "good"
    seen = t + timedelta(minutes=10)
    [s1] = lab.spells(1)
    assert (s1.start_source, s1.known_from, s1.joined_hint, s1.end_source) == ("snapshot", seen, hint, None)
    [s2] = lab.spells(2)
    assert (s2.start_source, s2.last_confirmed_at, s2.end_source) == ("event", seen, None)
    [s3] = lab.spells(3)
    assert (s3.end_source, s3.left_at) == ("event", t + timedelta(minutes=5))  # the stale listing changed nothing
    s4 = lab.spells(4)
    assert [(s.start_source, s.end_source) for s in s4] == [("event", "event"), ("snapshot", None)]
    assert s4[1].known_from == seen
    [s5] = lab.spells(5)
    assert (s5.start_source, s5.known_from, s5.end_source, s5.left_at) == (
        "snapshot", seen, "event", t + timedelta(hours=2))
    [s6] = lab.spells(6)
    assert (s6.end_source, s6.suspect_since) == (None, t)
    [s7] = lab.spells(7)
    assert s7.suspect_since is None
    assert lab.spells(8)[0].suspect_since is None and lab.spells(8)[0].end_source == "event"
    assert lab.pending() == [(3, "conflict", None, None), (6, "snapshot_absent", sid, t)]
    assert lab.totals(sid) == {"chunks": 1, "listed": 5, "seeded": 2, "missed_joins": 1, "confirmed": 1,
                               "absent_candidates": 1, "probes_queued": 2, "conflicts": 1}
    assert [r[0] for r in lab.c.execute("SELECT rule FROM tg.contradiction_case").fetchall()] == ["S3"]
    assert len(lab.sightings()) == 5 and lab.snapshot(sid)["reconciled_at"] is not None
    assert lab.snapshot(sid)["applied_through_user_id"] is None
    assert_equivalent(lab)


def test_a_partial_snapshot_gives_sightings_but_no_absences(lab):
    t = at(0)
    lab.join(2, t - timedelta(days=3))
    lab.join(6, t - timedelta(days=3))  # not listed
    lab.rebuild()
    sid = lab.collect(t, {1: t + timedelta(minutes=1), 2: t + timedelta(minutes=2)}, complete=False)
    lab.run()
    assert lab.snapshot(sid)["quality"] == "partial"
    assert lab.spells(1)[0].start_source == "snapshot"
    assert lab.spells(2)[0].last_confirmed_at == t + timedelta(minutes=2)
    assert lab.spells(6)[0].suspect_since is None
    assert lab.pending() == []
    assert lab.totals(sid)["absent_candidates"] == 0
    assert_equivalent(lab)


def test_runs_extend_over_consecutive_snapshots_and_break_at_exits_and_new_dates(lab):
    d1, d2 = datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 6, 1, tzinfo=UTC)
    s1 = lab.collect(at(0), {1: (at(1), d1), 2: (at(1), d1), 3: (at(1), d1)})
    lab.run()
    lab.leave(2, at(2 * H))  # user 2 leaves between the listings (ingest)
    lab.join(2, at(3 * H))
    lab.rebuild([2])
    s2 = lab.collect(at(24 * H), {1: (at(24 * H + 1), d1), 2: (at(24 * H + 1), d1), 3: (at(24 * H + 1), d2)})
    s3 = lab.collect(at(48 * H), {1: (at(48 * H + 1), d1)})
    lab.run()
    runs = {(r[1], r[3]): r for r in lab.sightings()}
    assert runs[(1, s1)][5:7] == (s3, 3) and runs[(1, s1)][8] == [at(24 * H + 1)]  # one run of three points
    assert (2, s2) in runs and runs[(2, s1)][6] == 1  # the exit broke user 2's run
    assert (3, s2) in runs and runs[(3, s1)][6] == 1  # a new participant date started another run
    assert_equivalent(lab)


# --- snapshot order, catch-up and the 2-day cleanup ---------------------------------------------------------------

def test_waits_for_ingest_to_catch_up_past_the_end(lab):
    lab.c.execute("UPDATE tg.bot_state SET caught_up_at = %s", (at(30),))
    sid = lab.collect(at(0), {1: at(1)}, finished=at(30))
    assert lab.run() == []  # caught_up_at = finished_at is not past it
    lab.c.execute("UPDATE tg.bot_state SET caught_up_at = %s", (at(31),))
    assert lab.run()[0] == "grade" and lab.snapshot(sid)["reconciled_at"] is not None


def test_a_later_snapshot_waits_for_an_earlier_unfinished_one(lab):
    now = lab.c.execute("SELECT now()").fetchone()[0]
    early = lab.collect(now - timedelta(days=1), {1: now - timedelta(days=1)}, done=False)
    later = lab.collect(now - timedelta(hours=1), {1: now - timedelta(minutes=50)})
    assert lab.run() == []
    assert lab.snapshot(later)["quality"] is None and lab.snapshot(early)
    lab.c.execute("UPDATE tg.snapshot SET started_at = %s WHERE snapshot_id = %s",
                  (now - timedelta(days=2, seconds=1), early))
    units = lab.run()
    assert units[0] == "expire" and lab.snapshot(early) == {}
    assert lab.snapshot(later)["reconciled_at"] is not None


def test_an_unfinished_snapshot_is_deleted_after_two_days_in_batches(lab, monkeypatch):
    monkeypatch.setattr(reconciler, "DELETE_BATCH", 2)
    now = lab.c.execute("SELECT now()").fetchone()[0]
    keep = lab.collect(now - timedelta(days=2) + timedelta(minutes=1), {1: now}, done=False, chat=None)
    lab.add_chat(CHAT - 1)
    old = lab.collect(now - timedelta(days=2, seconds=1), {u: now - timedelta(days=2) for u in range(1, 6)},
                      done=False, chat=CHAT - 1)
    assert lab.job.run_unit(lab.c) == "expire"
    s = lab.snapshot(old)
    assert s["expired_at"] is not None
    assert lab.c.execute("SELECT count(*) FROM tg.snapshot_member WHERE snapshot_id = %s", (old,)).fetchone()[0] == 3
    # the script finishes it meanwhile: it stays doomed and is never graded
    lab.c.execute("SET LOCAL ROLE collect_writer")
    lab.c.execute("UPDATE tg.snapshot SET finished_at = now(), reported_at_end = 5, fetched = 5, complete = true "
                  "WHERE snapshot_id = %s", (old,))
    lab.c.execute("RESET ROLE")
    units = lab.run()
    assert units == ["expire", "expire"] and lab.snapshot(old) == {}
    assert lab.snapshot(keep)["expired_at"] is None


# --- probes keep the highest priority ----------------------------------------------------------------------------

def _queue(lab, user, reason, sid=None, since=None):
    return lab.c.execute("SELECT tg.queue_probes(%s::bigint[], %s::bigint[], %s, %s, %s)",
                         ([CHAT], [user], reason, sid, since)).fetchone()[0]


def test_one_pending_probe_keeps_the_highest_priority(lab):
    lab.user(1)
    sid = lab.collect(at(0), {})
    assert _queue(lab, 1, "snapshot_absent", sid, at(0)) == 1
    lab.c.execute("UPDATE tg.probe SET attempts = 2 WHERE user_id = 1")
    pid = lab.c.execute("SELECT probe_id FROM tg.probe WHERE user_id = 1").fetchone()[0]
    assert _queue(lab, 1, "outage") == 1
    assert _queue(lab, 1, "conflict") == 1
    assert _queue(lab, 1, "snapshot_absent", sid, at(0)) == 0
    assert _queue(lab, 1, "outage") == 0
    assert lab.c.execute("SELECT probe_id, reason, snapshot_id, absent_since, attempts FROM tg.probe "
                         "WHERE user_id = 1").fetchall() == [(pid, "conflict", None, None, 2)]
    lab.user(2)
    lab.c.execute("INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (%s, 2, 'manual')", (CHAT,))
    assert _queue(lab, 2, "conflict") == 0
    assert lab.pending() == [(1, "conflict", None, None), (2, "manual", None, None)]


def test_a_stale_listing_raises_a_pending_absence_probe_to_conflict(lab):
    lab.join(1, at(-3 * 24 * H))
    lab.rebuild()
    s1 = lab.collect(at(0), {}, reported=1, fetched=1)
    lab.run()
    assert lab.pending() == [(1, "snapshot_absent", s1, at(0))]
    lab.leave(1, at(24 * H))
    lab.rebuild([1])
    lab.collect(at(24 * H - 30), {1: at(24 * H + 5)})  # listed 5 minutes after the exit: S3
    lab.run()
    assert lab.pending() == [(1, "conflict", None, None)]


# --- late listings (conductor ruling B) ---------------------------------------------------------------------------

def test_a_late_listing_keeps_the_runs_around_it(lab):
    t = at(0)
    s1 = lab.collect(t, {9: t + timedelta(minutes=1), 10: t + timedelta(minutes=1)})
    s2 = lab.collect(t + timedelta(hours=1), {9: t + timedelta(minutes=61), 10: t + timedelta(minutes=61)})
    s3 = lab.collect(t + timedelta(hours=2), {9: t + timedelta(minutes=121)}, reported=2, fetched=2)
    lab.run()
    lab.leave(9, t + timedelta(minutes=30))  # a late exit: the 11:01 point is now a rejoin (S4)
    lab.rebuild([9])
    lab.c.execute("UPDATE tg.snapshot SET members_pruned_at = now(), prune_started_at = now()")
    lab.c.execute("DELETE FROM tg.snapshot_member")

    def rejoin():
        return [s.known_from for s in lab.spells(9)]

    expected = [t + timedelta(minutes=1), t + timedelta(minutes=61)]
    assert rejoin() == expected
    assert lab.spells(10)[0].suspect_since == t + timedelta(hours=2)  # absent from s3 (good)
    run9 = lab.sightings(9)
    assert len(run9) == 1 and run9[0][6] == 3  # one three-point run

    for listed in (True, False):
        lab.c.execute("SAVEPOINT late")
        late = lab.collect(t + timedelta(minutes=90), {9: t + timedelta(minutes=91)} if listed else {11: t + timedelta(minutes=91)},
                           reported=1, fetched=1)
        assert lab.rebuild([9, 10]) == history.RebuildResult() and rejoin() == expected  # finished, not graded
        lab.fixed_chunk(1)
        lab.job.run_unit(lab.c)  # grade
        assert lab.snapshot(late)["late"] is True and lab.snapshot(late)["quality"] == "good"
        lab.run(stop_after=lambda u: len(u) == 1)  # one user applied
        assert rejoin() == expected
        lab.run()
        assert rejoin() == expected and lab.sightings(9)[0] == run9[0]  # the run is untouched
        if listed:
            assert [r[3] for r in lab.sightings(9)] == [s1, late]  # a one-point run of its own
            ev = history.load_evidence(lab.c, [(CHAT, 9)])[(CHAT, 9)]
            assert t + timedelta(minutes=91) in [p.seen_at for p in ev.sightings]
        assert lab.spells(10)[0].suspect_since == t + timedelta(hours=2)  # the late listing gives no absence
        assert [p for p in lab.pending() if p[2] == late] == []
        assert lab.totals(late)["absent_candidates"] == 0
        assert_equivalent(lab)
        lab.c.execute("ROLLBACK TO SAVEPOINT late")


def test_a_late_singleton_turns_an_applied_page_into_s3(lab):
    # the reviewer's reproduction: an out probe at 10:00, an applied page at 10:05, a late singleton at 09:50
    t = at(0)  # 10:00
    lab.probe(11, t, "out", sent=t - timedelta(seconds=1))
    s = lab.collect(t + timedelta(minutes=4), {11: t + timedelta(minutes=5)})
    lab.run()
    assert [(x.start_source, x.known_from) for x in lab.spells(11)] == [("snapshot", t + timedelta(minutes=5))]
    late = lab.collect(t - timedelta(minutes=11), {11: t - timedelta(minutes=10)})
    lab.run()
    assert lab.snapshot(late)["late"] is True
    [sp] = lab.spells(11)  # opened at 09:50, closed by the probe at 10:00; the 10:05 page is a stale listing
    assert (sp.known_from, sp.end_source, sp.left_before) == (t - timedelta(minutes=10), "probe", t)
    assert lab.c.execute("SELECT user_id, rule, at, source_id FROM tg.contradiction_case").fetchall() == [
        (11, "S3", t + timedelta(minutes=5), s)]
    assert lab.pending() == [(11, "conflict", None, None)]
    assert_equivalent(lab)


# --- M1: sources, no run across a migration, retired sources give no absences -----------------------------------

def test_runs_follow_sources_and_a_retired_source_gives_no_absences(lab):
    old, new = CHAT, CHAT - 5
    lab.add_chat(new)
    m = at(2 * H)  # 12:00, the migration
    o1 = lab.collect(at(0), {1: at(1), 2: at(1)})
    lab.run()
    lab.c.execute("UPDATE tg.bot_state SET caught_up_at = %s", (at(H),))  # ingest lags: o2, n1, o3 wait
    o2 = lab.collect(m, {1: m + timedelta(minutes=1)}, reported=2, fetched=2)  # old id, exactly at the cutoff
    n1 = lab.collect(m + timedelta(minutes=20), {1: m + timedelta(minutes=21)}, chat=new, reported=2, fetched=2)
    o3 = lab.collect(m + timedelta(minutes=40), {1: m + timedelta(minutes=41)}, reported=2, fetched=2)
    lab.c.execute("SELECT tg.move_chat_history(%s, %s, %s)", (old, new, m))
    lab.chat_id = new
    assert [lab.snapshot(s)["source_chat_id"] for s in (o1, o2, n1, o3)] == [old, old, None, old]
    lab.c.execute("UPDATE tg.bot_state SET caught_up_at = %s", (reconciler_far(),))
    lab.join(2, m + timedelta(minutes=5), chat=new)  # user 2 is in the supergroup, omitted by every listing after
    lab.rebuild([1, 2])
    lab.run()
    runs = sorted((r[1], r[3], r[5], r[6]) for r in lab.sightings())
    assert runs == sorted([(1, o1, o1, 1), (1, o2, o3, 2), (1, n1, n1, 1), (2, o1, o1, 1)])  # o1 | o2-o3 | n1
    for s in (o2, o3):
        assert lab.totals(s)["absent_candidates"] == 0
        assert [p for p in lab.pending(new) if p[2] == s] == []
    assert lab.totals(n1)["absent_candidates"] == 1  # the supergroup's own listing does see the omission
    lab.add_chat(CHAT - 6)  # a chained move (the base's move_chat_history refuses a second hop: an UPDATE does it)
    lab.c.execute("UPDATE tg.snapshot SET chat_id = %s WHERE chat_id = %s", (CHAT - 6, new))
    assert [lab.snapshot(s)["source_chat_id"] for s in (o1, n1)] == [old, new]  # the first source stays


def reconciler_far():
    return datetime(2100, 1, 1, tzinfo=UTC)


# --- tg_user: is_deleted follows the newest snapshot observation -------------------------------------------------

@pytest.mark.parametrize("first", ["monday", "tuesday"])
def test_is_deleted_follows_the_newest_observation_in_either_order(lab, first):
    a, b = CHAT, CHAT - 7
    lab.add_chat(b)
    monday, tuesday = at(0), at(24 * H)
    lab.join(12, monday - timedelta(days=30))
    lab.rebuild([12])
    lab.c.execute("UPDATE tg.bot_state SET caught_up_at = %s", (at(-H),))
    snaps = {
        "monday": lambda: lab.collect(monday, rows=[{"user_id": 12, "seen_at": monday, "is_deleted": False}], chat=a),
        "tuesday": lambda: lab.collect(tuesday, rows=[{"user_id": 12, "seen_at": tuesday, "is_deleted": True}],
                                       chat=b),
    }
    later = "tuesday" if first == "monday" else "monday"
    snaps[first]()
    lab.c.execute("UPDATE tg.bot_state SET caught_up_at = %s", (reconciler_far(),))
    lab.run()
    snaps[later]()
    before = versions(lab.c)
    lab.run()
    row = lab.c.execute("SELECT is_deleted, deleted_seen_at FROM tg.tg_user WHERE user_id = 12").fetchone()
    assert row == (True, tuesday)
    if first == "monday":  # the deletion came second: the chat with spells is marked for the rollups
        assert versions(lab.c) != before
    # an ingest-only update (newer last_seen_at, no deletion observation) changes nothing
    lab.c.execute("UPDATE tg.tg_user SET last_seen_at = %s WHERE user_id = 12", (at(48 * H),))
    lab.collect(at(30 * H), rows=[{"user_id": 12, "seen_at": at(30 * H), "is_deleted": False}], chat=b)
    lab.run()
    assert lab.c.execute("SELECT is_deleted FROM tg.tg_user WHERE user_id = 12").fetchone()[0] is False  # newer obs.
    lab.collect(at(29 * H), rows=[{"user_id": 12, "seen_at": at(29 * H), "is_deleted": True}], chat=a)
    lab.run()
    assert lab.c.execute("SELECT is_deleted FROM tg.tg_user WHERE user_id = 12").fetchone()[0] is False  # older


# --- contradictions ---------------------------------------------------------------------------------------------

def contradiction_gaps(lab, chat=None):
    return lab.c.execute("SELECT lower(period), upper(period) FROM tg.gap WHERE chat_id = %s AND "
                         "reason = 'contradiction' ORDER BY 1", (chat or lab.chat_id,)).fetchall()


@pytest.mark.parametrize("n, gaps", [(11, 1), (10, 0)])
def test_old_e4_cases_found_by_a_delayed_snapshot_raise_a_gap(lab, n, gaps):
    old_hour = at(0)  # long before the snapshot is applied
    for u in range(1, n + 1):
        lab.join(u, old_hour + timedelta(minutes=u))  # ingest: exact joins
    lab.rebuild()
    assert lab.c.execute("SELECT count(*) FROM tg.contradiction_case").fetchone()[0] == 0
    lab.collect(old_hour - timedelta(hours=1), {u: old_hour - timedelta(minutes=30) for u in range(1, n + 1)},
                finished=old_hour - timedelta(minutes=20))  # listed before the joins: every join is then an E4
    lab.run()
    assert lab.c.execute("SELECT count(*), min(rule), max(rule) FROM tg.contradiction_case").fetchone() == (
        n, "E4", "E4")
    assert len(contradiction_gaps(lab)) == gaps
    if gaps:
        assert contradiction_gaps(lab)[0] == (old_hour + timedelta(minutes=1), old_hour + timedelta(minutes=n))
    lab.rebuild_all()  # a retry records nothing twice
    lab.run()
    assert lab.c.execute("SELECT count(*) FROM tg.contradiction_case").fetchone()[0] == n
    assert len(contradiction_gaps(lab)) == gaps
    assert_equivalent(lab)


def test_eleven_s3_in_a_sliding_hour_raise_a_gap_across_the_clock_hour(lab):
    t = at(55)  # 10:55
    for u in range(1, 12):
        lab.join(u, at(-3 * 24 * H))
        lab.leave(u, t + timedelta(minutes=u - 1))  # exits 10:55 .. 11:05
    lab.rebuild()
    lab.collect(at(50), {u: t + timedelta(minutes=u - 1, seconds=30) for u in range(1, 12)})  # 30 s after each
    lab.run()
    assert lab.c.execute("SELECT count(*) FROM tg.contradiction_case WHERE rule = 'S3'").fetchone()[0] == 11
    assert contradiction_gaps(lab) == [(t + timedelta(seconds=30), t + timedelta(minutes=10, seconds=30))]
    assert {p[1] for p in lab.pending()} == {"conflict"} and len(lab.pending()) == 11
    # the new gap is G1 evidence: the reconciler handled it, so a full rebuild changes nothing
    assert lab.c.execute("SELECT bool_and(done) FROM tg.gap_progress").fetchone()[0] is True
    assert_equivalent(lab)


def test_two_chats_under_the_threshold_cross_it_when_a_migration_merges_them(lab):
    old, new = CHAT, CHAT - 8
    lab.add_chat(new)
    for chat, base in ((old, 0), (new, 30)):
        users = [lab.user(100 + base + i) for i in range(6)]
        lab.c.execute("SELECT tg.record_contradictions(%s::bigint[], %s::bigint[], %s::text[], %s::timestamptz[], "
                      "%s::bigint[])", ([chat] * 6, users, ["E4"] * 6,
                                        [at(base + i) for i in range(6)], list(range(base, base + 6))))
    assert contradiction_gaps(lab, old) == [] and contradiction_gaps(lab, new) == []
    lab.c.execute("SELECT tg.move_chat_history(%s, %s, %s)", (old, new, at(2 * H)))
    assert lab.c.execute("SELECT count(*) FROM tg.contradiction_case WHERE chat_id = %s", (new,)).fetchone()[0] == 12
    assert contradiction_gaps(lab, new) == [(at(0), at(35))]


def test_a_contradiction_gap_widens_and_an_earlier_start_redoes_g1(lab):
    lab.join(1, at(-24 * H))
    lab.rebuild()
    users = [lab.user(200 + i) for i in range(30)]

    def record(times, src):
        lab.c.execute("SELECT tg.record_contradictions(%s::bigint[], %s::bigint[], %s::text[], %s::timestamptz[], "
                      "%s::bigint[])", ([CHAT] * len(times), users[:len(times)], ["E10"] * len(times), times,
                                        [src + i for i in range(len(times))]))

    record([at(i) for i in range(11)], 0)
    lab.run()
    gap_id = lab.c.execute("SELECT gap_id FROM tg.gap WHERE reason = 'contradiction'").fetchone()[0]
    assert lab.c.execute("SELECT done FROM tg.gap_progress WHERE gap_id = %s", (gap_id,)).fetchone()[0] is True
    record([at(5 + i) for i in range(11)], 100)  # overlaps and ends later: widened, same start
    assert contradiction_gaps(lab) == [(at(0), at(15))]
    assert lab.c.execute("SELECT count(*) FROM tg.gap_progress WHERE gap_id = %s", (gap_id,)).fetchone()[0] == 1
    record([at(-20 + i * 2) for i in range(11)], 200)  # starts earlier: G1 again
    assert contradiction_gaps(lab) == [(at(-20), at(15))]
    assert lab.c.execute("SELECT count(*) FROM tg.gap_progress WHERE gap_id = %s", (gap_id,)).fetchone()[0] == 0
    lab.run()
    assert lab.spells(1)[0].suspect_since == at(-20)


# --- R3 in batches ------------------------------------------------------------------------------------------------

def test_prune_in_batches_sets_the_marker_with_the_last_row_and_matches_r3(lab, monkeypatch):
    from tgaud import sql
    monkeypatch.setattr(reconciler, "DELETE_BATCH", 2)
    sids = [lab.collect(at(i * 24 * H), {u: at(i * 24 * H + 1) for u in range(1, 6)}) for i in range(4)]
    sids.append(lab.collect(at(5 * 24 * H), {1: at(5 * 24 * H + 1)}, complete=False))  # partial
    lab.run(stop_after=lambda u: u.count("apply") == 5)
    lab.c.execute("SAVEPOINT r3")
    sql.run(lab.c, "R3")
    reference = lab.c.execute("SELECT snapshot_id, members_pruned_at IS NOT NULL FROM tg.snapshot ORDER BY 1").fetchall()
    reference_rows = lab.c.execute("SELECT snapshot_id, user_id FROM tg.snapshot_member ORDER BY 1, 2").fetchall()
    lab.c.execute("ROLLBACK TO SAVEPOINT r3")
    assert lab.job.run_unit(lab.c) == "prune"
    first = lab.snapshot(sids[0])
    assert first["prune_started_at"] is not None and first["members_pruned_at"] is None
    assert lab.c.execute("SELECT count(*) FROM tg.snapshot_member WHERE snapshot_id = %s",
                         (sids[0],)).fetchone()[0] == 3
    lab.run()
    assert lab.c.execute("SELECT snapshot_id, members_pruned_at IS NOT NULL FROM tg.snapshot ORDER BY 1"
                         ).fetchall() == reference
    assert lab.c.execute("SELECT snapshot_id, user_id FROM tg.snapshot_member ORDER BY 1, 2").fetchall() == reference_rows
    assert [s for s, pruned in reference if pruned] == [sids[0], sids[1], sids[4]]
    assert len(lab.sightings()) == 5  # the evidence stays


# --- erasure -------------------------------------------------------------------------------------------------------

def test_an_erased_users_old_page_stays_absent_and_an_erased_inviter_stays_erased(lab):
    erased_at = at(H)
    lab.c.execute("INSERT INTO tg.erasure (user_hash, erased_at) VALUES (tg.erasure_hash(20), %s)", (erased_at,))
    old = lab.collect(at(0), rows=[{"user_id": 20, "seen_at": at(5)},
                                   {"user_id": 21, "seen_at": at(6), "inviter_id": 20},
                                   {"user_id": 22, "seen_at": at(7), "inviter_id": 23}])
    lab.run()
    assert lab.c.execute("SELECT count(*) FROM tg.tg_user WHERE user_id = 20").fetchone()[0] == 0
    assert lab.sightings(20) == [] and lab.spells(20) == []
    assert lab.c.execute("SELECT user_id, inviter_id FROM tg.snapshot_member WHERE snapshot_id = %s ORDER BY 1",
                         (old,)).fetchall() == [(21, None), (22, 23)]
    new = lab.collect(at(2 * H), {20: at(2 * H + 1)})  # after the erasure: new evidence
    lab.run()
    assert [r[3] for r in lab.sightings(20)] == [new] and lab.spells(20)[0].start_source == "snapshot"


def test_names_are_blanked_after_reconcile_when_the_owner_keeps_none(tgaud_dsn):
    with psycopg.connect(tgaud_dsn) as c:
        lab = Lab(c, env={"TGAUD_STORE_NAMES": "false"})
        sid = lab.collect(at(0), rows=[{"user_id": 30, "seen_at": at(1), "username": "u30", "first_name": "F"}])
        lab.run()
        assert c.execute("SELECT username, first_name FROM tg.snapshot_member WHERE snapshot_id = %s",
                         (sid,)).fetchone() == (None, None)
        assert c.execute("SELECT username, first_name FROM tg.tg_user WHERE user_id = 30").fetchone() == (None, None)
        c.rollback()


def test_names_are_kept_with_store_names(lab):
    lab.collect(at(0), rows=[{"user_id": 31, "seen_at": at(1), "username": "u31", "first_name": "F",
                              "is_premium": True, "is_bot": True}])
    lab.run()
    assert lab.c.execute("SELECT username, first_name, is_premium, is_bot FROM tg.tg_user WHERE user_id = 31"
                         ).fetchone() == ("u31", "F", True, True)


# --- G1 ------------------------------------------------------------------------------------------------------------

def test_g1_suspects_and_queues_outage_probes_newest_first_within_the_budget(lab, monkeypatch):
    monkeypatch.setattr(reconciler, "OUTAGE_BUDGET", 3)
    for u in range(1, 6):
        lab.join(u, at(u))
    lab.join(6, at(4 * H))  # confirmed after the gap start: not doubted
    lab.rebuild()
    lab.gap(at(3 * H), None, reason="not_admin")  # open
    lab.fixed_chunk(2)
    units = lab.run()
    assert units.count("g1") == 3
    assert [lab.spells(u)[0].suspect_since for u in range(1, 7)] == [at(3 * H)] * 5 + [None]
    assert lab.pending() == [(3, "outage", None, None), (4, "outage", None, None), (5, "outage", None, None)]
    # ingest moves the start earlier and deletes the progress: G1 again from the lowest user
    lab.c.execute("UPDATE tg.gap SET period = tstzrange(%s, NULL) WHERE chat_id = %s", (at(2 * H), CHAT))
    lab.c.execute("DELETE FROM tg.gap_progress")
    lab.run()
    assert [lab.spells(u)[0].suspect_since for u in range(1, 6)] == [at(2 * H)] * 5
    assert_equivalent(lab)


def test_g1_for_a_gap_of_every_chat_and_no_probes_for_an_untracked_chat(lab):
    other = CHAT - 9
    lab.c.execute("INSERT INTO tg.chat (chat_id, chat_type, title, tracked) VALUES (%s, 'group', 'x', false)", (other,))
    lab.join(1, at(0))
    lab.join(2, at(0), chat=other)
    lab.rebuild()
    history.rebuild_pairs(lab.c, [(other, 2)])
    lab.gap(at(H), at(2 * H), all_chats=True)
    lab.run()
    assert lab.spells(1)[0].suspect_since == at(H)
    assert lab.spells(2, other)[0].suspect_since == at(H)
    assert lab.pending() == [(1, "outage", None, None)] and lab.pending(other) == []
    assert lab.c.execute("SELECT count(*), bool_and(done) FROM tg.gap_progress").fetchone() == (2, True)


# --- the job contract ------------------------------------------------------------------------------------------------

def test_idle_job_looks_again_after_the_poll_interval(lab):
    now = [1000.0]
    lab.job.ctx.clock = lambda: now[0]
    assert lab.job.due(now[0])
    lab.job.step(lab.c)
    assert not lab.job.due(now[0] + 1)
    assert lab.job.due(now[0] + reconciler.IDLE_POLL)
    lab.collect(at(0), {1: at(1)})
    now[0] += reconciler.IDLE_POLL
    lab.job.step(lab.c)
    assert lab.job.due(now[0])  # it found work: due again at once


def test_make_returns_the_job():
    assert reconciler.make(job().ctx).name == "reconciler"


# --- the fixture of verify.md, reconciled from its snapshots ------------------------------------------------------

FIXTURE_SQL = __import__("pathlib").Path(__file__).resolve().parents[1] / "fixtures" / "history_fixture.sql"
B = -1001000000002
SIGHTINGS = ("SELECT chat_id, user_id, first_seen_at, first_snapshot_id, last_seen_at, last_snapshot_id, times_seen, "
             "joined_date, interior_seen_at FROM tg.sighting ORDER BY 1, 2, 3")


@pytest.fixture
def fixture_db(pg):
    dsn = pg.make_db(migrated=True)
    pg.psql(psycopg.conninfo.conninfo_to_dict(dsn)["dbname"], FIXTURE_SQL)
    return dsn


def test_the_fixture_reconciled_from_its_snapshots(fixture_db):
    from reconcile_lab import COLS
    with psycopg.connect(fixture_db) as c:
        c.execute("SET TimeZone = 'UTC'")
        grades = c.execute("SELECT snapshot_id, quality FROM tg.snapshot ORDER BY 1").fetchall()
        sightings = c.execute(SIGHTINGS).fetchall()
        spells = c.execute(f"SELECT {COLS} FROM tg.membership ORDER BY {COLS}").fetchall()
        answers = c.execute("""SELECT chat_id, user_id, snapshot_id, absent_since, attempts, last_attempt_at, outcome,
                                      answered_at, status_seen, error_code
                               FROM tg.probe WHERE reason = 'snapshot_absent' ORDER BY 1, 2""").fetchall()
        assert c.execute("SELECT count(*) FROM tg.snapshot_member").fetchone()[0] == 36
        # back to the moment the collect script finished: nothing graded, folded or queued yet
        c.execute("DELETE FROM tg.sighting")
        c.execute("DELETE FROM tg.probe WHERE reason = 'snapshot_absent'")
        c.execute("DELETE FROM tg.reconcile_run")
        c.execute("UPDATE tg.snapshot SET quality = NULL, reconciled_at = NULL, members_pruned_at = NULL")
        c.execute("UPDATE tg.bot_state SET caught_up_at = '2026-11-23 12:00+00'")
        history.rebuild_pairs(c, c.execute(ALL_PAIRS).fetchall())  # the spells of that moment
        j = job()
        units = []
        while (u := j.run_unit(c)) is not None:
            units.append(u)
        assert units.count("grade") == 15
        assert c.execute("SELECT snapshot_id, quality FROM tg.snapshot ORDER BY 1").fetchall() == grades
        assert c.execute(SIGHTINGS).fetchall() == sightings
        assert [r[:4] for r in answers] == c.execute("""
            SELECT chat_id, user_id, snapshot_id, absent_since FROM tg.probe
            WHERE reason = 'snapshot_absent' AND outcome = 'pending' ORDER BY 1, 2""").fetchall()
        assert sorted((r[0], r[1], r[2]) for r in answers) == sorted(
            [(B, 1019, 14), (-1002000000011, 2011, 13), (-1002000000010, 2010, 11)])
        # R3 (verify.md 4): snapshots 2 (B, older than its last two good ones) and 15 (partial) lose their rows
        assert c.execute("SELECT count(*) FROM tg.snapshot_member").fetchone()[0] == 30
        assert [r[0] for r in c.execute("SELECT snapshot_id FROM tg.snapshot WHERE members_pruned_at IS NOT NULL "
                                        "ORDER BY 1").fetchall()] == [2, 15]
        assert c.execute("SELECT count(*) FROM tg.sighting WHERE first_snapshot_id IN (2, 15) "
                         "OR last_snapshot_id IN (2, 15)").fetchone()[0] > 0
        # the prober answers the absence probes as the fixture's did; the spells are the fixture's again
        for chat, user, _, _, attempts, last_attempt, outcome, answered, status, error in answers:
            c.execute("""UPDATE tg.probe SET attempts = %s, last_attempt_at = %s, outcome = %s, answered_at = %s,
                                status_seen = %s, error_code = %s
                         WHERE chat_id = %s AND user_id = %s AND reason = 'snapshot_absent'""",
                      (attempts, last_attempt, outcome, answered, status, error, chat, user))
        pairs = c.execute(ALL_PAIRS).fetchall()
        history.rebuild_pairs(c, pairs)
        assert c.execute(f"SELECT {COLS} FROM tg.membership ORDER BY {COLS}").fetchall() == spells
        before = versions(c)
        assert history.rebuild_pairs(c, pairs) == history.RebuildResult() and versions(c) == before
        c.rollback()


ALL_PAIRS = """SELECT chat_id, user_id FROM tg.member_event UNION SELECT chat_id, user_id FROM tg.sighting
               UNION SELECT chat_id, user_id FROM tg.probe UNION SELECT chat_id, user_id FROM tg.membership"""


# --- a stop after a committed chunk, then a new job (a restart): the same result as one run -----------------------

def _seed_snapshot_case(lab: Lab, *, commit: bool) -> int:
    """40 users: open spells (some confirmed later, some not listed), closed ones, newcomers; one earlier snapshot.
    Commits only on a database of the test's own (`commit`)."""
    t = at(0)
    for u in range(1, 41):
        if u % 5 == 0:
            lab.join(u, t - timedelta(days=5)); lab.leave(u, t - timedelta(days=1))
        elif u % 5 == 1:
            lab.join(u, t - timedelta(days=5)); lab.leave(u, t + timedelta(minutes=3))  # S3 below
        elif u % 5 != 4:
            lab.join(u, t - timedelta(days=5))
    lab.rebuild()
    lab.collect(t - timedelta(days=2), {u: t - timedelta(days=2) + timedelta(seconds=u) for u in range(1, 41, 2)})
    lab.run(commit=commit)
    listed = {u: t + timedelta(minutes=5, seconds=u) for u in range(1, 41) if u % 3 != 0}
    sid = lab.collect(t, listed, reported=40, fetched=40)
    if commit:
        lab.c.commit()
    return sid


def _state(lab: Lab, sid: int) -> tuple:
    from reconcile_lab import COLS
    c = lab.c
    return (lab.totals(sid), lab.snapshot(sid)["quality"], lab.sightings(),
            c.execute(f"SELECT {COLS} FROM tg.membership ORDER BY {COLS}").fetchall(),
            c.execute("SELECT chat_id, user_id, reason, snapshot_id, absent_since FROM tg.probe "
                      "WHERE outcome = 'pending' ORDER BY 1, 2").fetchall(),
            c.execute("SELECT chat_id, user_id, rule, at, source_id FROM tg.contradiction_case ORDER BY 1, 2, 3, 4"
                      ).fetchall())


def test_a_stop_after_a_chunk_resumes_to_the_same_result(pg):
    results = []
    for stop in (False, True):
        with psycopg.connect(pg.make_db(migrated=True)) as c:
            c.execute("SET TimeZone = 'UTC'")
            lab = Lab(c)
            c.commit()
            sid = _seed_snapshot_case(lab, commit=True)
            lab.fixed_chunk(7)
            if stop:
                lab.run(commit=True, stop_after=lambda u: u.count("apply") == 2)
                assert lab.snapshot(sid)["applied_through_user_id"] is not None
                lab.job = job()  # the restart: a new job object, nothing in memory
                lab.fixed_chunk(7)
            lab.run(commit=True)
            assert lab.snapshot(sid)["reconciled_at"] is not None
            assert lab.totals(sid)["chunks"] >= 4  # listed users and unlisted open spells, 7 a chunk
            results.append(_state(lab, sid))
            assert_equivalent(lab)
    assert results[0] == results[1]


def test_a_cursor_moved_back_folds_nothing_twice(lab):
    sid = _seed_snapshot_case(lab, commit=False)
    lab.fixed_chunk(7)
    lab.c.execute("SAVEPOINT once")
    lab.run()
    once = _state(lab, sid)[2:]
    lab.c.execute("ROLLBACK TO SAVEPOINT once")
    lab.run(stop_after=lambda u: u.count("apply") == 2)
    lab.c.execute("UPDATE tg.snapshot SET applied_through_user_id = 3 WHERE snapshot_id = %s", (sid,))  # e.g. forget-user
    lab.run()
    assert _state(lab, sid)[2:] == once  # sightings, spells, probes, cases


def test_g1_stopped_after_its_first_chunk_resumes_to_the_same_result(pg):
    results = []
    for stop in (False, True):
        with psycopg.connect(pg.make_db(migrated=True)) as c:
            c.execute("SET TimeZone = 'UTC'")
            lab = Lab(c)
            for u in range(1, 13):
                lab.join(u, at(u))
            lab.rebuild()
            lab.gap(at(H), at(30 * H), all_chats=True)
            c.commit()
            lab.fixed_chunk(5)
            if stop:
                lab.run(commit=True, stop_after=lambda u: u.count("g1") == 1)
                assert c.execute("SELECT last_user_id, done FROM tg.gap_progress").fetchone() == (5, False)
                assert lab.pending() == []
                lab.job = job()
                lab.fixed_chunk(5)
            lab.run(commit=True)
            results.append(([s.suspect_since for u in range(1, 13) for s in lab.spells(u)], lab.pending(),
                            c.execute("SELECT done FROM tg.gap_progress").fetchall()))
    assert results[0] == results[1]
    assert results[0][0] == [at(H)] * 12 and len(results[0][1]) == 12 and results[0][2] == [(True,)]


def test_the_scheduler_runs_the_job_fenced_and_committed(migrated_dsn):
    import threading

    from tgaud import config, db, metrics
    from tgaud.jobs import JobContext, Scheduler
    with psycopg.connect(migrated_dsn) as c:
        lab = Lab(c)
        sid = lab.collect(at(0), {1: at(1), 2: at(2)})
        c.commit()
        generation = db.claim_generation(c)
    ctx = JobContext(config=config.load({}), api=None, health=metrics.Health(), stop=threading.Event(),
                     generation=generation)
    s = Scheduler("worker", [reconciler.make(ctx)], lambda: psycopg.connect(migrated_dsn), ctx)
    s.conn = s.connect()
    for _ in range(5):
        s.run_once()
    s._discard()
    with psycopg.connect(migrated_dsn) as c:
        assert c.execute("SELECT quality, reconciled_at IS NOT NULL FROM tg.snapshot WHERE snapshot_id = %s",
                         (sid,)).fetchone() == ("good", True)
        assert c.execute("SELECT count(*) FROM tg.membership").fetchone()[0] == 2


# --- review round 1 -------------------------------------------------------------------------------------------------

def _wait_for_lock_waiter(dsn: str) -> None:
    """Until another session of this database waits on a lock."""
    import time
    with psycopg.connect(dsn, autocommit=True) as probe:
        for _ in range(300):
            if probe.execute("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
                             "AND wait_event_type = 'Lock'").fetchone()[0]:
                return
            time.sleep(0.05)
    raise AssertionError("no session waited on a lock")


def test_expiry_yields_to_a_collector_that_finishes_first(migrated_dsn):
    import threading
    with psycopg.connect(migrated_dsn) as c:
        lab = Lab(c)
        now = c.execute("SELECT now()").fetchone()[0]
        sid = lab.collect(now - timedelta(days=2, minutes=1), {1: now - timedelta(days=2), 2: now - timedelta(days=2)},
                          done=False)
        c.commit()
    collector = psycopg.connect(migrated_dsn)
    collector.execute("SET ROLE collect_writer")
    collector.execute("UPDATE tg.snapshot SET finished_at = now(), reported_at_end = 2, fetched = 2, complete = true "
                      "WHERE snapshot_id = %s", (sid,))  # the last transaction of the script, not committed yet
    ran: list = []

    def cleanup() -> None:
        with psycopg.connect(migrated_dsn) as r:
            ran.append(job().run_unit(r))  # reads the old (unfinished) row, then waits for the collector's lock
            r.commit()

    t = threading.Thread(target=cleanup)
    t.start()
    _wait_for_lock_waiter(migrated_dsn)
    collector.commit()
    collector.close()
    t.join(30)
    assert ran == ["expire"]
    with psycopg.connect(migrated_dsn) as c:
        lab = Lab(c)
        s = lab.snapshot(sid)
        assert s["finished_at"] is not None and s["expired_at"] is None
        assert c.execute("SELECT count(*) FROM tg.snapshot_member WHERE snapshot_id = %s", (sid,)).fetchone()[0] == 2
        lab.run()
        assert lab.snapshot(sid)["quality"] == "good" and lab.snapshot(sid)["reconciled_at"] is not None


def test_a_claimed_expiry_goes_on_after_the_collector_finishes_and_a_restart(migrated_dsn, monkeypatch):
    import threading
    monkeypatch.setattr(reconciler, "DELETE_BATCH", 1)
    with psycopg.connect(migrated_dsn) as c:
        lab = Lab(c)
        now = c.execute("SELECT now()").fetchone()[0]
        sid = lab.collect(now - timedelta(days=2, minutes=1), {u: now - timedelta(days=2) for u in (1, 2, 3)},
                          done=False)
        c.commit()
    r = psycopg.connect(migrated_dsn)
    assert job().run_unit(r) == "expire"  # the claim and one row, not committed yet
    finished: list = []

    def finish() -> None:
        with psycopg.connect(migrated_dsn) as w:
            w.execute("SET ROLE collect_writer")
            finished.append(w.execute("UPDATE tg.snapshot SET finished_at = now(), reported_at_end = 3, fetched = 3, "
                                      "complete = true WHERE snapshot_id = %s", (sid,)).rowcount)
            w.commit()

    t = threading.Thread(target=finish)
    t.start()
    _wait_for_lock_waiter(migrated_dsn)
    r.commit()  # the claim commits first; the script's last transaction then goes through
    r.close()
    t.join(30)
    assert finished == [1]
    with psycopg.connect(migrated_dsn) as c:
        lab = Lab(c)  # a new job object: nothing in memory
        s = lab.snapshot(sid)
        assert s["expired_at"] is not None and s["finished_at"] is not None and s["quality"] is None
        assert lab.run() == ["expire", "expire", "expire"]
        assert lab.snapshot(sid) == {}


def test_an_erased_page_never_reaches_the_cursor_or_the_run_rows(lab):
    lab.c.execute("INSERT INTO tg.erasure (user_hash, erased_at) VALUES (tg.erasure_hash(20), %s)", (at(H),))
    sid = lab.collect(at(0), {20: at(5), 21: at(6)})
    lab.fixed_chunk(1)
    cursors = []
    while lab.job.run_unit(lab.c) is not None:
        cursors.append(lab.snapshot(sid).get("applied_through_user_id"))
    assert 20 not in cursors and 21 in cursors
    assert lab.c.execute("SELECT chunk, from_user_id, through_user_id FROM tg.reconcile_run WHERE snapshot_id = %s "
                         "ORDER BY chunk", (sid,)).fetchall() == [(1, None, None), (2, 21, 21), (3, None, None)]
    assert lab.snapshot(sid)["reconciled_at"] is not None and lab.totals(sid)["listed"] == 1
    assert lab.c.execute("SELECT count(*) FROM tg.tg_user WHERE user_id = 20").fetchone()[0] == 0


def test_a_last_chunk_of_erased_rows_completes_the_snapshot(lab):
    for u in (20, 22):
        lab.c.execute("INSERT INTO tg.erasure (user_hash, erased_at) VALUES (tg.erasure_hash(%s), %s)", (u, at(H)))
    sid = lab.collect(at(0), {20: at(5), 22: at(6)})
    lab.run()
    s = lab.snapshot(sid)
    assert s["reconciled_at"] is not None and s["applied_through_user_id"] is None
    assert lab.c.execute("SELECT from_user_id, through_user_id, listed FROM tg.reconcile_run WHERE snapshot_id = %s",
                         (sid,)).fetchall() == [(None, None, 0)]


def test_profile_values_follow_the_latest_page_and_names_go_when_not_kept(lab):
    other = CHAT - 10
    lab.add_chat(other)

    def profile():
        return lab.c.execute("SELECT username, first_name, last_name, is_premium FROM tg.tg_user WHERE user_id = 40"
                             ).fetchone()

    lab.collect(at(0), rows=[{"user_id": 40, "seen_at": at(1), "username": "oldname", "first_name": "Old",
                              "last_name": "One", "is_premium": True}])
    lab.run()
    assert profile() == ("oldname", "Old", "One", True)
    lab.collect(at(H), rows=[{"user_id": 40, "seen_at": at(H + 1), "username": None, "first_name": "New",
                              "last_name": None, "is_premium": False}])  # username removed, premium gone
    lab.run()
    assert profile() == (None, "New", None, None)
    lab.collect(at(30), rows=[{"user_id": 40, "seen_at": at(31), "username": "older", "first_name": "Older",
                               "is_premium": True}], chat=other)  # an older page applied later
    lab.run()
    assert profile() == (None, "New", None, None)
    lab.job = job({"TGAUD_STORE_NAMES": "false"})
    lab.collect(at(2 * H), rows=[{"user_id": 40, "seen_at": at(2 * H + 1), "username": "u40", "first_name": "F",
                                  "is_premium": True}])
    lab.run()
    assert profile() == (None, None, None, True)
    lab.c.execute("UPDATE tg.tg_user SET username = 'kept', first_name = 'Kept' WHERE user_id = 40")
    lab.collect(at(H + 30), rows=[{"user_id": 40, "seen_at": at(H + 31)}], chat=other)  # older, names off
    lab.run()
    assert profile()[:2] == (None, None)  # blanking is not an update of values: any visit clears them


def _record(lab, times, src, users):
    lab.c.execute("SELECT tg.record_contradictions(%s::bigint[], %s::bigint[], %s::text[], %s::timestamptz[], "
                  "%s::bigint[])", ([CHAT] * len(times), users[:len(times)], ["E4"] * len(times), times,
                                    [src + i for i in range(len(times))]))


@pytest.mark.parametrize("g1_first", [False, True])
def test_a_bridging_batch_merges_contradiction_gaps_into_one_and_g1_follows(lab, g1_first):
    lab.join(1, at(-H))
    lab.event(1, at(80), "member", "administrator")  # confirmed at 11:20
    lab.rebuild()
    users = [lab.user(300 + i) for i in range(11)]
    _record(lab, [at(i) for i in range(11)], 0, users)              # 10:00-10:10
    _record(lab, [at(100 + i) for i in range(11)], 100, users)      # 11:40-11:50
    assert contradiction_gaps(lab) == [(at(0), at(10)), (at(100), at(110))]
    if g1_first:
        lab.run()
        assert lab.spells(1)[0].suspect_since == at(100)  # doubted by the 11:40 start
        assert (1, "outage", None, None) in lab.pending()
    _record(lab, [at(50 + i) for i in range(11)], 50, users)        # 10:50-11:00 bridges both
    assert contradiction_gaps(lab) == [(at(0), at(110))]
    lab.run()
    assert lab.spells(1)[0].suspect_since is None  # confirmed after the merged episode began
    assert [p for p in lab.pending() if p[0] == 1] == []
    assert lab.c.execute("SELECT count(*), bool_and(done) FROM tg.gap_progress").fetchone() == (1, True)
    assert_equivalent(lab)


def test_a_moved_start_lifts_doubts_that_rested_on_the_later_start(lab):
    lab.join(1, at(0))
    lab.event(1, at(2 * H), "member", "administrator")  # confirmed at 12:00
    lab.rebuild()
    lab.gap(at(3 * H), None, reason="not_admin")
    lab.run()
    assert lab.spells(1)[0].suspect_since == at(3 * H)
    lab.c.execute("UPDATE tg.gap SET period = tstzrange(%s, NULL) WHERE chat_id = %s", (at(H), CHAT))  # ingest
    lab.c.execute("DELETE FROM tg.gap_progress")
    lab.run()
    assert lab.spells(1)[0].suspect_since is None  # positive evidence after the (new) start
    assert [p for p in lab.pending() if p[0] == 1] == []
    assert_equivalent(lab)
