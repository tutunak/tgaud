"""Property tests of the rebuild: random evidence of one (chat, user).

Pure: every output satisfies tg.membership's constraints (mirrored in Python), stays never overlap, and the input
order never matters. Database: the same evidence written the way the writers write it, in shuffled order, gives the
same spells; every insert passes the real constraints; a second call changes nothing and marks nothing; and the
result equals a brute-force replay that takes every good snapshot without the user as an absence (no run coverage,
no windowing), so the loader's run coverage, boundary and bounding are checked too.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from history_world import CHAT, World
from tgaud import history
from tgaud.history import Absence, Event, Evidence, ProbeAnswer, SightingPoint

T0 = datetime(2026, 10, 24, 23, 0, tzinfo=timezone.utc)  # the 4 h around the EU fall-back (2026-10-25 01:00Z)
INF = datetime.max.replace(tzinfo=timezone.utc)
STATUSES = ["creator", "administrator", "member", "restricted", "left", "kicked"]
H1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
HINTS = [H1, H1, H1, None, datetime(2026, 8, 1, tzinfo=timezone.utc)]  # mostly one date: runs get extended
USER = 4242
OTHER_CHAT = -1009000000099
OLD_CHAT = -4242
ZONES = ["UTC", "Europe/Belgrade", "America/New_York"]
BELGRADE = ZoneInfo("Europe/Belgrade")


def t_of(minutes: int, ms: int) -> datetime:
    return T0 + timedelta(minutes=minutes, milliseconds=ms)


times = st.builds(t_of, st.integers(0, 240), st.sampled_from([0, 0, 0, 200, 999]))
is_member = st.sampled_from([None, True, False])


@st.composite
def events(draw):
    out = []
    for i in range(draw(st.integers(0, 6))):
        old, new = draw(st.sampled_from(STATUSES)), draw(st.sampled_from(STATUSES))
        out.append(Event(1000 + i, draw(times), 5000 + i, old, draw(is_member) if old == "restricted" else None,
                         new, draw(is_member) if new == "restricted" else None, draw(st.booleans()),
                         None, draw(st.booleans()), draw(st.booleans())))
    return out


@st.composite
def probes(draw):
    out = []
    for _ in range(draw(st.integers(0, 3))):
        outcome = draw(st.sampled_from(["in", "out", "error"]))
        if outcome == "error":
            out.append(ProbeAnswer(None, "error"))
            continue
        answered = draw(times)
        status = draw(st.sampled_from(["member", "administrator"] if outcome == "in" else ["left", "kicked"]))
        sent = draw(st.sampled_from([None, 0, 500, 60_000]))
        sent_at = None if sent is None else answered - timedelta(milliseconds=sent)
        out.append(ProbeAnswer(answered, outcome, status, sent_at))
    return out


@st.composite
def snapshots(draw):
    """(started_at, good, applied to the user, listed page time or None, hint, rejected later), in started_at order."""
    starts = sorted(draw(st.lists(st.integers(0, 240), max_size=5, unique=True)))
    out = []
    for k, s in enumerate(starts):
        listed = draw(st.sampled_from([True, True, True, False]))
        page = t_of(s, 0) + timedelta(minutes=draw(st.integers(0, 15)), milliseconds=draw(st.sampled_from([0, 800])))
        # only the latest snapshot can be mid-apply in practice; earlier ones are mostly applied
        applied = draw(st.booleans() if k == len(starts) - 1 else st.sampled_from([True] * 5 + [False]))
        out.append((t_of(s, 0), draw(st.booleans()), applied, page if listed else None, draw(st.sampled_from(HINTS)),
                    draw(st.sampled_from([False] * 5 + [True]))))
    return out


world_evidence = st.fixed_dictionaries({
    "events": events(), "probes": probes(), "snapshots": snapshots(),
    "gaps": st.lists(st.tuples(times, st.booleans()), max_size=2),
    "echo": st.lists(times, max_size=1), "m2": st.lists(times, max_size=1)})


def brute_force(d: dict) -> Evidence:
    """The evidence by its definition: every applied listing a point, every applied good non-listing an absence;
    a snapshot the owner rejected is no evidence at all."""
    ev = Evidence(CHAT, USER, events=list(d["events"]), probes=[p for p in d["probes"] if p.outcome != "error"],
                  gaps=[g for g, _ in d["gaps"]], echo_from=list(d["echo"]), migrations=list(d["m2"]))
    for k, (started, good, applied, page, hint, rejected) in enumerate(d["snapshots"]):
        if not applied or rejected:
            continue
        if page is not None:
            ev.sightings.append(SightingPoint(page, hint, k, 0))
        elif good:
            ev.absences.append(Absence(started, k))
    return ev


def check_constraints(spells: list[history.Spell], ev: Evidence) -> None:
    by_id = {e.event_id: e for e in ev.events}
    for s in spells:
        assert (s.start_source == "event") == (s.joined_at is not None) == (s.join_event_id is not None) \
            == (s.join_route is not None)
        assert s.joined_at is None or (s.joined_at == s.known_from == by_id[s.join_event_id].event_at)
        ended = s.end_source is not None
        assert ended == (s.end_reason is not None) == (s.left_after is not None) == (s.left_before is not None)
        assert (s.left_at is not None) == (s.end_source == "event") == (s.left_event_id is not None)
        assert s.left_at is None or (s.left_after == s.left_at == s.left_before == by_id[s.left_event_id].event_at)
        assert (s.end_source == "migration") == (s.end_reason == "migrated")
        assert s.known_from <= s.last_confirmed_at
        if ended:
            assert s.known_from <= s.left_after <= s.left_before and s.last_confirmed_at <= s.left_before
        assert s.suspect_since is None or not ended
    assert sum(s.end_source is None for s in spells) <= 1
    stays = sorted((s.known_from, s.left_before or INF) for s in spells)
    for (a0, a1), (b0, b1) in zip(stays, stays[1:]):
        if a0 < a1 and b0 < b1:  # empty stays [t, t) overlap nothing
            assert a1 <= b0, (stays, spells)


@given(world_evidence, st.randoms(use_true_random=False))
@settings(max_examples=600, deadline=None)
def test_pure_replay_satisfies_the_constraints_and_ignores_input_order(d, rnd):
    ev = brute_force(d)
    spells = history.build_spells(ev)
    check_constraints(spells, ev)
    shuffled = Evidence(CHAT, USER, **{k: rnd.sample(v, len(v)) for k, v in vars(ev).items() if isinstance(v, list)})
    assert history.build_spells(shuffled) == spells
    assert history.build_spells(in_zone(ev, BELGRADE)) == spells  # local wall times run backwards at 01:00Z


def in_zone(ev: Evidence, zone) -> Evidence:
    z = lambda t: t if t is None else t.astimezone(zone)  # noqa: E731
    return Evidence(ev.chat_id, ev.user_id, [e._replace(event_at=z(e.event_at)) for e in ev.events],
                    [s._replace(seen_at=z(s.seen_at), joined_date=z(s.joined_date)) for s in ev.sightings],
                    [p._replace(answered_at=z(p.answered_at), sent_at=z(p.sent_at)) for p in ev.probes],
                    [a._replace(started_at=z(a.started_at)) for a in ev.absences], [z(t) for t in ev.gaps],
                    [z(t) for t in ev.migrations], [z(t) for t in ev.echo_from])


def write(w: World, d: dict, rnd: random.Random) -> None:
    """The evidence of `d` as the writers write it; everything but the snapshot order is shuffled."""
    w.user(USER)
    actions = []
    for e in d["events"]:
        actions.append(lambda e=e: w.c.execute("""
            INSERT INTO tg.member_event (event_id, chat_id, user_id, event_at, update_id, old_status, old_is_member,
                                         new_status, new_is_member, actor_id, via_join_request, via_folder_link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (e.event_id, CHAT, USER, e.event_at, e.update_id, e.old_status, e.old_is_member, e.new_status,
             e.new_is_member, 9 if e.actor_is_other else None, e.via_join_request, e.via_folder_link)))
    for p in d["probes"]:
        actions.append(lambda p=p: w.probe(USER, p.answered_at, p.outcome, p.status_seen, sent=p.sent_at))
    for g, global_ in d["gaps"]:
        actions.append(lambda g=g, global_=global_: w.gap(g, g + timedelta(hours=1), all_chats=global_))
    for m in d["echo"]:
        actions.append(lambda m=m: w.c.execute(
            "INSERT INTO tg.chat_alias (old_chat_id, chat_id, migrated_at, moved_at) VALUES (%s, %s, %s, %s)",
            (OLD_CHAT, CHAT, m, m)))
    for m in d["m2"]:
        actions.append(lambda m=m: w.c.execute(
            "INSERT INTO tg.chat_alias (old_chat_id, chat_id, migrated_at, moved_at) VALUES (%s, %s, %s, NULL)",
            (CHAT, OTHER_CHAT, m)))

    def listings():
        last = len(d["snapshots"]) - 1
        for k, (started, good, applied, page, hint, rejected) in enumerate(d["snapshots"]):
            listed = {USER: (page, hint)} if page is not None else {}
            # the latest snapshot may be mid-apply with this user's run already folded ahead of the cursor
            sid = w.snapshot(started, listed, finished=started + timedelta(minutes=30), good=good,
                             through="all" if applied else USER - 1, snapshot_id=900 + k,
                             fold_through="all" if k == last else "same")
            if rejected:  # the owner grades it rejected after it was folded; later listings extend through it
                w.reject(sid)
    actions.append(listings)
    rnd.shuffle(actions)
    for a in actions:
        a()


@pytest.fixture(scope="module")
def conn(pg):
    dsn = pg.make_db(migrated=True)
    with psycopg.connect(dsn) as c:
        World(c).add_chat(OTHER_CHAT)
        c.commit()
        yield c


@given(world_evidence, st.randoms(use_true_random=False))
@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_database_rebuild_is_valid_deterministic_idempotent_and_matches_the_definition(conn, d, rnd):
    results = []
    for _ in range(2):
        with conn.transaction(force_rollback=True):
            conn.execute("SELECT set_config('TimeZone', %s, true)", (rnd.choice(ZONES),))
            w = World(conn)
            write(w, d, rnd)
            history.rebuild_pairs(conn, [(CHAT, USER)])  # every insert passes the real constraints
            got = w.spells(USER)
            marks = conn.execute("SELECT chat_id, from_day, version FROM tg.rollup_dirty").fetchall()
            assert history.rebuild_pairs(conn, [(CHAT, USER)]) == history.RebuildResult()
            assert conn.execute("SELECT chat_id, from_day, version FROM tg.rollup_dirty").fetchall() == marks
            results.append(got)
    assert canonical(results[0]) == canonical(results[1])  # another insertion order, the same spells
    assert canonical(results[0]) == canonical(history.build_spells(brute_force(d)))


def canonical(spells: list[history.Spell]) -> list[history.Spell]:
    return sorted(spells, key=lambda s: tuple((v is None, v if v is not None else 0) for v in s))
