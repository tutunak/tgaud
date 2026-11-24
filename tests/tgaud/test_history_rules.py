"""One test per row of the state table (design.md 4), on the pure replay: build_spells / replay with a trace."""

from __future__ import annotations

import itertools
import random
from datetime import datetime, timedelta, timezone

from tgaud import history
from tgaud.history import Absence, Event, Evidence, ProbeAnswer, SightingPoint

T0 = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)
HINT = datetime(2024, 5, 5, tzinfo=timezone.utc)
_ids = itertools.count(1)


def at(minutes: float = 0, **kw) -> datetime:
    return T0 + timedelta(minutes=minutes, **kw)


def ev(t: datetime, old: str, new: str, *, old_m=None, new_m=None, actor=False, link=None, req=False,
       folder=False, update_id=None) -> Event:
    n = next(_ids)
    return Event(n, t, update_id if update_id is not None else n, old, old_m, new, new_m, actor, link, req, folder)


def join(t, **kw) -> Event:
    return ev(t, kw.pop("old", "left"), kw.pop("new", "member"), **kw)


def leave(t, **kw) -> Event:
    return ev(t, kw.pop("old", "member"), kw.pop("new", "left"), **kw)


def seen(t, hint=None, run=1, i=0) -> SightingPoint:
    return SightingPoint(t, hint, run, i)


def run(**kw) -> tuple[list[history.Spell], list[str]]:
    trace: list = []
    out = history.replay(Evidence(-100, 7, **kw), trace)
    return out, [rule for rule, _ in trace]


def open_(s: history.Spell) -> bool:
    return s.end_source is None


# --- events --------------------------------------------------------------------------------------------------

def test_E1_join_opens_an_exact_spell_with_its_route():
    for kw, route in [({}, "unattributed"), ({"link": 5}, "link"), ({"req": True}, "request"),
                      ({"folder": True}, "folder"), ({"actor": True}, "added")]:
        e = join(at(0), **kw)
        (s,), rules = run(events=[e])
        assert rules == ["E1"]
        assert (s.start_source, s.joined_at, s.join_event_id, s.known_from, s.last_confirmed_at) == (
            "event", e.event_at, e.event_id, e.event_at, e.event_at)
        assert s.join_route == route and s.invite_link_id == kw.get("link") and open_(s)


def test_E2_rejoin_after_the_end_opens_a_new_exact_spell():
    j1, l1, j2 = join(at(0)), leave(at(60)), join(at(120))
    (a, b), rules = run(events=[j1, l1, j2])
    assert rules == ["E1", "E5", "E2"]
    assert (a.left_at, a.end_reason) == (l1.event_at, "left")
    assert b.joined_at == j2.event_at and open_(b)


def test_E3_migration_echo_is_absorbed_within_the_window():
    m = at(60 * 24 * 9)
    j, echo = join(at(0)), join(m + timedelta(seconds=5))
    (s,), rules = run(events=[j, echo], echo_from=[m])
    assert rules == ["E1", "E3"]
    assert s.joined_at == j.event_at and s.last_confirmed_at == echo.event_at and open_(s)
    # after the hour, or for a stay that began after the migration, it is a missed leave (E4)
    late = join(m + history.ECHO_WINDOW + timedelta(seconds=1))
    assert run(events=[j, late], echo_from=[m])[1] == ["E1", "E4"]
    after = join(m + timedelta(seconds=1))
    assert run(events=[after, join(m + timedelta(seconds=9))], echo_from=[m])[1] == ["E1", "E4"]


def test_E4_join_while_open_closes_inferred_and_opens_exact():
    j1, j2 = join(at(0)), join(at(60 * 24 * 14))
    (a, b), rules = run(events=[j1, j2])
    assert rules == ["E1", "E4"]
    assert (a.end_source, a.end_reason, a.left_after, a.left_before, a.left_at) == (
        "inferred", "gone", j1.event_at, j2.event_at, None)
    assert (b.start_source, b.joined_at) == ("event", j2.event_at) and open_(b)


def test_E5_exit_closes_exactly_left_or_kicked():
    cases = [(leave(at(10)), "left"), (leave(at(10), new="kicked"), "kicked"), (leave(at(10), actor=True), "kicked"),
             (ev(at(10), "restricted", "restricted", old_m=True, new_m=False), "left")]
    for exit_, reason in cases:
        (s,), rules = run(events=[join(at(0)), exit_])
        assert rules == ["E1", "E5"]
        assert (s.end_source, s.end_reason, s.left_at, s.left_after, s.left_before, s.left_event_id) == (
            "event", reason, exit_.event_at, exit_.event_at, exit_.event_at, exit_.event_id)
        assert s.last_confirmed_at == at(0)


def test_E6_exit_first_is_a_left_censored_spell():
    e = leave(at(0))
    (s,), rules = run(events=[e])
    assert rules == ["E6"]
    assert (s.start_source, s.known_from, s.last_confirmed_at, s.joined_at, s.join_route) == (
        "inferred", e.event_at, e.event_at, None, None)
    assert (s.end_source, s.left_at, s.left_event_id) == ("event", e.event_at, e.event_id)


def test_E7_exit_after_the_end_is_another_left_censored_spell():
    j, l1, l2 = join(at(0)), leave(at(10)), leave(at(20))
    (a, b), rules = run(events=[j, l1, l2])
    assert rules == ["E1", "E5", "E7"]
    assert (b.start_source, b.known_from, b.left_at) == ("inferred", l2.event_at, l2.event_at)


def test_E8_in_to_in_confirms():
    promote = ev(at(30), "member", "administrator")
    (s,), rules = run(events=[join(at(0)), promote], gaps=[at(10)])
    assert rules == ["E1", "G1", "E8"]
    assert s.last_confirmed_at == promote.event_at and s.suspect_since is None and open_(s)


def test_E9_in_to_in_without_an_open_spell_is_a_missed_join():
    (s,), rules = run(events=[ev(at(0), "member", "restricted", new_m=True)])
    assert rules == ["E9"]
    assert (s.start_source, s.known_from, s.joined_at, s.join_route) == ("inferred", at(0), None, None)
    (_, b), rules = run(events=[join(at(0)), leave(at(5)), ev(at(9), "administrator", "member")])
    assert rules == ["E1", "E5", "E9"] and b.start_source == "inferred" and open_(b)


def test_E10_out_to_out_while_open_closes_inferred():
    j, ban = join(at(0)), ev(at(30), "left", "kicked", actor=True)
    (s,), rules = run(events=[j, ban])
    assert rules == ["E1", "E10"]
    assert (s.end_source, s.end_reason, s.left_after, s.left_before) == ("inferred", "gone", j.event_at, ban.event_at)


def test_E11_out_to_out_without_an_open_spell_changes_nothing():
    assert run(events=[ev(at(0), "kicked", "left")]) == ([], ["E11"])
    spells, rules = run(events=[join(at(0)), leave(at(10)), ev(at(20), "left", "kicked", actor=True)])
    assert rules == ["E1", "E5", "E11"] and len(spells) == 1 and spells[0].left_at == at(10)


def test_R1_restricted_back_to_member_is_a_join():
    events = [join(at(0)), ev(at(10), "member", "restricted", new_m=True),
              ev(at(20), "restricted", "restricted", old_m=True, new_m=False),
              ev(at(30), "restricted", "restricted", old_m=False, new_m=True)]
    (a, b), rules = run(events=events)
    assert rules == ["E1", "E8", "E5", "R1", "E2"]
    assert (a.last_confirmed_at, a.left_at, a.end_reason) == (at(10), at(20), "left")
    assert b.joined_at == at(30) and b.join_route == "unattributed" and open_(b)


# --- sightings -----------------------------------------------------------------------------------------------

def test_S1_sighting_opens_a_snapshot_spell_with_the_hint():
    (s,), rules = run(sightings=[seen(at(0), HINT)])
    assert rules == ["S1"]
    assert (s.start_source, s.known_from, s.last_confirmed_at, s.joined_hint, s.joined_at) == (
        "snapshot", at(0), at(0), HINT, None)


def test_S2_sighting_confirms_clears_suspect_and_fills_an_empty_hint():
    (s,), rules = run(events=[join(at(0))], sightings=[seen(at(60 * 24), HINT)], gaps=[at(60)])
    assert rules == ["E1", "G1", "S2"]
    assert (s.last_confirmed_at, s.joined_hint, s.suspect_since) == (at(60 * 24), HINT, None)
    other = HINT + timedelta(days=1)
    (s,), _ = run(sightings=[seen(at(0), HINT), seen(at(10), other, run=2)])
    assert s.joined_hint == HINT  # a hint is filled only when empty


def test_S3_sighting_within_G_after_the_end_is_ignored():
    exit_at = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
    page = exit_at + history.GRACE - timedelta(milliseconds=200)  # 12:09:59.800
    (s,), rules = run(events=[join(at(0)), leave(exit_at)], sightings=[seen(page)])
    assert rules == ["E1", "E5", "S3"] and s.left_at == exit_at


def test_S4_sighting_at_or_after_end_plus_G_is_a_missed_rejoin():
    exit_at = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
    (a, b), rules = run(events=[join(at(0)), leave(exit_at)], sightings=[seen(exit_at + history.GRACE, HINT)])
    assert rules == ["E1", "E5", "S4"]
    assert (b.start_source, b.known_from, b.joined_hint) == ("snapshot", exit_at + history.GRACE, HINT) and open_(b)


def test_S5_sighting_older_than_the_first_event_opens_first():
    # a later exit closes that spell (an older snapshot is the entry of a left-censored stay)
    (s,), rules = run(sightings=[seen(at(0), HINT)], events=[leave(at(60))])
    assert rules == ["S1", "S5", "E5"]
    assert (s.start_source, s.known_from, s.left_at) == ("snapshot", at(0), at(60))
    # a later join with old status out closes it by E4
    (a, b), rules = run(sightings=[seen(at(0))], events=[join(at(60))])
    assert rules == ["S1", "S5", "E4"]
    assert (a.end_source, a.left_after, a.left_before) == ("inferred", at(0), at(60)) and b.joined_at == at(60)


# --- absences ------------------------------------------------------------------------------------------------

def test_A1_absence_after_L_makes_the_open_spell_suspect():
    (s,), rules = run(sightings=[seen(at(0))], absences=[Absence(at(60 * 24 * 7), 2), Absence(at(60 * 24 * 14), 3)])
    assert rules == ["S1", "A1", "A1"]
    assert s.suspect_since == at(60 * 24 * 7) and open_(s)  # the earliest doubt stays


def test_A2_absence_at_or_before_L_changes_nothing():
    (s,), rules = run(sightings=[seen(at(10))], absences=[Absence(at(10), 2)], events=[join(at(10))])
    assert rules == ["E1", "S2", "A2"] and s.suspect_since is None


def test_A3_absence_without_an_open_spell_changes_nothing():
    assert run(absences=[Absence(at(0), 1)]) == ([], ["A3"])
    (s,), rules = run(events=[join(at(0)), leave(at(5))], absences=[Absence(at(60), 1)])
    assert rules == ["E1", "E5", "A3"] and s.left_at == at(5)


# --- probes --------------------------------------------------------------------------------------------------

def test_P1_probe_in_confirms_and_clears_suspect():
    (s,), rules = run(sightings=[seen(at(0))], absences=[Absence(at(60), 2)],
                      probes=[ProbeAnswer(at(120), "in", "member", at(119))])
    assert rules == ["S1", "A1", "P1"]
    assert (s.last_confirmed_at, s.suspect_since) == (at(120), None)


def test_P2_probe_in_without_an_open_spell_opens_a_probe_spell():
    (s,), rules = run(probes=[ProbeAnswer(at(0), "in", "member")])
    assert rules == ["P2"] and (s.start_source, s.known_from, s.last_confirmed_at) == ("probe", at(0), at(0))
    (_, b), rules = run(events=[join(at(0)), leave(at(5))], probes=[ProbeAnswer(at(60), "in", "member")])
    assert rules == ["E1", "E5", "P2"] and b.start_source == "probe" and open_(b)


def test_P3_probe_out_closes_with_bounds():
    (s,), rules = run(sightings=[seen(at(0))], absences=[Absence(at(60), 2)],
                      probes=[ProbeAnswer(at(120), "out", "left", at(119))])
    assert rules == ["S1", "A1", "P3"]
    assert (s.end_source, s.end_reason, s.left_after, s.left_before, s.suspect_since) == (
        "probe", "gone", at(0), at(120), None)
    (s,), _ = run(events=[join(at(0))], probes=[ProbeAnswer(at(120), "out", "kicked")])
    assert s.end_reason == "kicked"


def test_P3_answer_sent_after_an_event_of_the_same_second_counts():
    j = join(datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc))
    sent = datetime(2026, 9, 1, 12, 0, 0, 800000, tzinfo=timezone.utc)
    (s,), rules = run(events=[j], probes=[ProbeAnswer(sent + timedelta(milliseconds=300), "out", "left", sent)])
    assert rules == ["E1", "P3"] and s.end_source == "probe"


def test_P_late_answer_straddling_an_event_is_ignored():
    j = join(at(0))
    for sent, answered in [(at(-1), at(1)), (at(0), at(1)), (at(-1), at(0))]:  # inclusive at both ends
        (s,), rules = run(events=[j], probes=[ProbeAnswer(answered, "out", "left", sent)])
        assert rules == ["P-late", "E1"] and open_(s)
    # no send time recorded: the answer instant itself
    (s,), rules = run(events=[j], probes=[ProbeAnswer(at(0), "out", "left")])
    assert rules == ["P-late", "E1"] and open_(s)


def test_P4_failed_probe_changes_nothing():
    (s,), rules = run(sightings=[seen(at(0))], absences=[Absence(at(60), 2)],
                      probes=[ProbeAnswer(None, "error"), ProbeAnswer(None, "pending"),
                              ProbeAnswer(at(90), "cancelled")])
    assert sorted(rules) == ["A1", "P4", "P4", "P4", "S1"]
    assert s.suspect_since == at(60) and open_(s)


# --- gaps and migrations -------------------------------------------------------------------------------------

def test_G1_gap_after_L_makes_the_open_spell_suspect_until_positive_evidence():
    (s,), rules = run(events=[join(at(0))], gaps=[at(60), at(120)])
    assert rules == ["E1", "G1", "G1"] and s.suspect_since == at(60)
    (s,), rules = run(events=[join(at(60))], gaps=[at(60)])
    assert rules == ["E1", "G1-none"] and s.suspect_since is None  # the gap does not start after L
    (s,), rules = run(events=[join(at(0))], gaps=[at(60)], sightings=[seen(at(90))])
    assert rules == ["E1", "G1", "S2"] and s.suspect_since is None
    (s,), rules = run(events=[join(at(0))], gaps=[at(60)], probes=[ProbeAnswer(at(90), "out", "left")])
    assert rules == ["E1", "G1", "P3"] and (s.left_after, s.left_before, s.suspect_since) == (at(0), at(90), None)


def test_M1_moved_history_opens_the_echo_window():
    # tg.move_chat_history moved the old group's events here; its migrated_at is an E3 window (DB test too)
    m = at(60 * 24)
    (s,), rules = run(events=[join(at(0)), join(m + timedelta(seconds=5)), leave(m + timedelta(days=5))],
                      echo_from=[m])
    assert rules == ["E1", "E3", "E5"] and s.joined_at == at(0) and s.left_at == m + timedelta(days=5)


def test_M2_migration_to_an_untracked_chat_closes_by_migration():
    m = at(60 * 24)
    (s,), rules = run(events=[join(at(0))], sightings=[seen(at(60))], migrations=[m])
    assert rules == ["E1", "S2", "M2"]
    assert (s.end_source, s.end_reason, s.left_after, s.left_before, s.left_at) == (
        "migration", "migrated", at(60), m, None)
    # an old-id exit dated before the migration closes exactly; the migration then changes nothing
    (s,), rules = run(events=[join(at(0)), leave(m - timedelta(seconds=1))], migrations=[m])
    assert rules == ["E1", "E5", "M2-none"] and s.end_source == "event"
    # an exit dated the migration's second replays before it
    (s,), rules = run(events=[join(at(0)), leave(m)], migrations=[m])
    assert rules == ["E1", "E5", "M2-none"]


# --- order ---------------------------------------------------------------------------------------------------

def test_same_instant_order_is_events_by_update_id_then_sightings_then_probes():
    t = at(0)
    events = [join(t, update_id=3), leave(t, update_id=4), join(t, update_id=5)]  # join, leave, join in a second
    (a, b), rules = run(events=list(reversed(events)), sightings=[seen(t)])
    assert rules == ["E1", "E5", "E2", "S2"]
    assert (a.known_from, a.left_at, b.known_from) == (t, t, t) and open_(b)
    # a probe answered at an event's instant falls inside its own [sent_at, answered_at]: ignored (P-late); at
    # an instant without an event the sighting comes first, so the probe's bounds start at it
    t2 = at(60)
    (s,), rules = run(events=[join(t)], sightings=[seen(t2)], probes=[ProbeAnswer(t2, "out", "left")])
    assert rules == ["E1", "S2", "P3"] and (s.left_after, s.left_before) == (t2, t2)


def test_input_order_never_matters():
    m = at(60 * 24 * 3)
    kw = dict(events=[join(at(0)), ev(at(10), "member", "administrator"), leave(at(60 * 30)), join(at(60 * 31)),
                      join(m + timedelta(seconds=3))],
              sightings=[seen(at(5), HINT, 1, 0), seen(at(60 * 24), HINT, 1, 1), seen(at(60 * 30 + 5), None, 2, 0)],
              probes=[ProbeAnswer(at(60 * 40), "out", "left"), ProbeAnswer(at(60 * 41), "in", "member")],
              absences=[Absence(at(60 * 2), 9)], gaps=[at(60 * 3), at(60 * 3)], echo_from=[m])
    expected = history.build_spells(Evidence(-100, 7, **kw))
    rnd = random.Random(7)
    for _ in range(20):
        shuffled = {k: rnd.sample(v, len(v)) for k, v in kw.items()}
        assert history.build_spells(Evidence(-100, 7, **shuffled)) == expected
