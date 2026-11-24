"""The prober (design.md 6, P1-P4): queue order, the token bucket and the per-chat half on a fake clock, 429 with
retry_after, the backoff schedule and the final error, and the answers end to end through rebuild_pairs,
including a probe re-keyed by M1 and answers that straddle an event of the pair (plan reviews 2 and 3)."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from history_world import CHAT, at
from jobs_world import Env, make_env, no_closed_log_streams  # noqa: F401 (autouse fixture)
from tgaud import history, metrics
from tgaud.jobs import prober
from tgaud.jobs.prober import TokenBucket

START = datetime(2026, 9, 20, 12, 0, 0, 500000, tzinfo=timezone.utc)  # half a second past: sent_at is floored
OTHER = -1009000000002


@pytest.fixture
def env(migrated_dsn):
    e = make_env(migrated_dsn, START)
    yield e
    e.conn.close()
    e.w.c.close()


def answer(e: Env, statuses: dict[int, str] | None = None, default: str = "member") -> None:
    statuses = statuses or {}

    def compute(params):
        return {"status": statuses.get(params["user_id"], default), "user": {"id": params["user_id"], "is_bot": False}}
    e.fake.respond("getChatMember", compute)


def queue(e: Env, user: int, reason: str = "conflict", chat: int = CHAT, queued: datetime | None = None,
          snapshot: int | None = None) -> int:
    e.w.user(user)
    absent = None
    if snapshot is not None:
        absent = e.w.c.execute("SELECT started_at FROM tg.snapshot WHERE snapshot_id = %s", (snapshot,)).fetchone()[0]
    return e.w.c.execute("""INSERT INTO tg.probe (chat_id, user_id, reason, snapshot_id, absent_since, queued_at)
                            VALUES (%s, %s, %s, %s, %s, %s) RETURNING probe_id""",
                         (chat, user, reason, snapshot, absent, queued or START - timedelta(hours=1))).fetchone()[0]


def asked(e: Env) -> list[tuple[int, int]]:
    return [(c.params["chat_id"], c.params["user_id"]) for c in e.fake.calls_of("getChatMember")]


def probe_row(e: Env, probe_id: int) -> dict:
    with psycopg.connect(e.dsn) as c:
        cur = c.execute("SELECT * FROM tg.probe WHERE probe_id = %s", (probe_id,))
        return dict(zip([d.name for d in cur.description], cur.fetchone()))


def spells(e: Env, user: int, chat: int = CHAT) -> list[history.Spell]:
    with psycopg.connect(e.dsn) as c:
        cols = ", ".join(history.SPELL_COLUMNS)
        rows = c.execute(f"SELECT {cols} FROM tg.membership WHERE chat_id = %s AND user_id = %s ORDER BY known_from",
                         (chat, user)).fetchall()
    return [history.Spell(*(v.astimezone(timezone.utc) if isinstance(v, datetime) else v for v in r)) for r in rows]


# --- the token bucket ---------------------------------------------------------------------------------------------

def test_token_bucket_rate_burst_and_pause():
    b = TokenBucket(5, now=100.0)
    assert [b.take(100.0) for _ in range(6)] == [True] * 5 + [False]
    assert b.wait(100.0) == pytest.approx(0.2)
    assert b.take(100.2) and not b.take(100.2)
    b.pause(100.2, 7)
    assert b.wait(100.2) == pytest.approx(7.0) and not b.take(107.0 - 1e-6)
    assert b.take(107.2 + 1e-9) and not b.take(107.2 + 1e-9)  # one call at the pause end, never a burst
    assert not b.take(107.3) and b.take(107.4 + 1e-9)
    assert b.wait(1000.0) == 0 and b.tokens == 5.0  # capped at the burst
    with pytest.raises(ValueError):
        TokenBucket(0, now=0)


# --- queue order ---------------------------------------------------------------------------------------------------

def test_queue_order_manual_conflict_outage_newest_spell_first_absent_sample(env):
    e = env
    w = e.w
    for u, t in ((31, at(0)), (32, at(60)), (33, at(30))):  # open spells for the outage probes
        w.join(u, t)
    sid = w.snapshot(at(DAY := 60 * 24 * 5), {40: (at(DAY + 1), None)})
    w.join(41, at(10))
    w.rebuild()
    queue(e, 50, "sample", queued=START - timedelta(hours=9))
    queue(e, 41, "snapshot_absent", snapshot=sid, queued=START - timedelta(hours=8))
    for u in (31, 32, 33):
        queue(e, u, "outage", queued=START - timedelta(hours=7))
    queue(e, 21, "conflict", queued=START - timedelta(hours=2))
    queue(e, 20, "conflict", queued=START - timedelta(hours=3))
    queue(e, 10, "manual", queued=START - timedelta(minutes=1))
    w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 5)
    assert [u for _, u in asked(e)] == [10, 20, 21, 32, 33, 31, 41, 50]


# --- the budget ----------------------------------------------------------------------------------------------------

def test_global_rate_and_backlog_on_a_fake_clock(env):
    e = env
    for u in range(1, 61):
        queue(e, u)
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 4.0)  # burst of 5, then 5 per second
    assert 24 <= len(asked(e)) <= 26
    e.run(p, 10.0)
    assert len(asked(e)) == 60
    assert len(set(asked(e))) == 60  # each probe once


def test_one_chat_gets_at_most_half_while_others_wait(env):
    e = env
    e.w.add_chat(OTHER)
    for u in range(1, 101):
        queue(e, u, "conflict")  # higher priority: alone it would take every token
        queue(e, 1000 + u, "sample", chat=OTHER)
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 10.0)
    calls = asked(e)
    a = sum(1 for c, _ in calls if c == CHAT)
    b = len(calls) - a
    assert 50 <= len(calls) <= 56
    assert a <= len(calls) / 2 + 2 and b >= len(calls) / 2 - 2, (a, b)
    # a manual probe of the busy chat skips its share: it is the next call
    queue(e, 500, "manual")
    e.w.c.commit()
    n = len(calls)
    e.run(p, 0.5)
    assert asked(e)[n] == (CHAT, 500)


def test_a_chat_alone_uses_the_whole_budget(env):
    e = env
    for u in range(1, 101):
        queue(e, u)
    e.w.c.commit()
    answer(e)
    e.run(prober.make(e.ctx), 10.0)
    assert 53 <= len(asked(e)) <= 56


def test_429_pauses_the_whole_bucket_for_retry_after_and_is_not_an_attempt(env):
    e = env
    ids = [queue(e, u) for u in (1, 2)]
    e.w.c.commit()
    e.fake.error("getChatMember", 429, "Too Many Requests: retry after 7", retry_after=7)
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 6.9)
    assert asked(e) == [(CHAT, 1)]
    assert probe_row(e, ids[0])["attempts"] == 0
    e.run(p, 0.15)
    assert asked(e) == [(CHAT, 1), (CHAT, 1)]  # the same probe again, first, at the end of the pause
    e.run(p, 0.3)
    assert asked(e)[:3] == [(CHAT, 1), (CHAT, 1), (CHAT, 2)]
    row = probe_row(e, ids[0])
    assert (row["outcome"], row["attempts"]) == ("in", 1)


# --- failures: backoff and P4 ---------------------------------------------------------------------------------------

def test_four_failures_end_in_error_and_the_spell_stays_open_and_suspect(env):
    e = env
    w = e.w
    w.join(1, at(0))
    w.gap(at(60 * 24 * 10), at(60 * 24 * 11))  # G1: suspect since the gap start
    w.rebuild()
    pid = queue(e, 1, "outage")
    w.c.commit()
    before = spells(e, 1)
    assert before[0].suspect_since is not None
    e.fake.raise_timeout("getChatMember", times=4)
    e.run(prober.make(e.ctx), 60 + 600 + 3600 + 60, dt=1.0)
    assert len(e.fake.calls_of("getChatMember")) == 4
    row = probe_row(e, pid)
    assert (row["outcome"], row["error_code"], row["attempts"], row["answered_at"]) == ("error", "timeout", 4, None)
    assert spells(e, 1) == before  # P4: unknown, nothing changes


def test_backoff_waits_exactly(env):
    e = env
    pid = queue(e, 1)
    e.w.c.commit()
    e.fake.raise_timeout("getChatMember", times=4)
    p = prober.make(e.ctx)
    seen: list[float] = []
    start = e.clock()
    while e.clock() < start + 60 + 600 + 3600 + 60:
        n = len(asked(e))
        e.tick(p)
        if len(asked(e)) > n:
            seen.append(e.clock() - start)
        e.clock.advance(1.0)
    gaps = [b - a for a, b in zip(seen, seen[1:])]
    assert len(seen) == 4
    for gap, backoff in zip(gaps, prober.BACKOFF):
        assert backoff <= gap <= backoff + prober.IDLE_REFRESH + 1, (gaps, prober.BACKOFF)
    assert probe_row(e, pid)["outcome"] == "error"


def test_a_failure_is_retried_and_a_later_success_counts(env):
    e = env
    pid = queue(e, 1)
    e.w.c.commit()
    e.fake.error("getChatMember", 400, "Bad Request: chat not found")
    answer(e, default="left")
    p = prober.make(e.ctx)
    e.run(p, 1)
    row = probe_row(e, pid)
    assert (row["outcome"], row["attempts"], row["error_code"]) == ("pending", 1, None)
    e.run(p, 61, dt=1.0)
    row = probe_row(e, pid)
    assert (row["outcome"], row["attempts"], row["status_seen"]) == ("out", 2, "left")


def test_probes_total_counts_outcomes(env):
    e = env
    for u in (1, 2, 3):
        queue(e, u)
    e.w.c.commit()
    before = {o: metrics.REGISTRY.get_sample_value("tgaud_probes_total", {"outcome": o}) or 0
              for o in ("in", "out", "error")}
    answer(e, {2: "left", 3: "kicked"})
    e.run(prober.make(e.ctx), 2)
    after = {o: metrics.REGISTRY.get_sample_value("tgaud_probes_total", {"outcome": o}) or 0
             for o in ("in", "out", "error")}
    assert {o: after[o] - before[o] for o in after} == {"in": 1, "out": 2, "error": 0}


# --- P1-P3 end to end ------------------------------------------------------------------------------------------------

def test_p1_in_confirms_and_clears_suspect(env):
    e = env
    e.w.join(1, at(0))
    e.w.gap(at(60 * 24 * 10), at(60 * 24 * 11))
    e.w.rebuild()
    pid = queue(e, 1, "outage")
    e.w.c.commit()
    assert spells(e, 1)[0].suspect_since is not None
    answer(e)
    e.run(prober.make(e.ctx), 1)
    row = probe_row(e, pid)
    assert row["outcome"] == "in" and row["sent_at"] == START.replace(microsecond=0)
    assert row["sent_at"] <= row["answered_at"]
    (s,) = spells(e, 1)
    assert s.end_source is None and s.suspect_since is None and s.last_confirmed_at == row["answered_at"]


def test_p2_in_without_a_spell_opens_one_from_the_probe(env):
    e = env
    e.w.join(1, at(0))
    e.w.leave(1, at(60))
    e.w.rebuild()
    pid = queue(e, 1, "manual")
    queue(e, 2, "manual")
    e.w.c.commit()
    answer(e, {1: "administrator", 2: "restricted"})
    e.fake.respond("getChatMember", lambda p: {"status": "administrator"} if p["user_id"] == 1 else
                   {"status": "restricted", "is_member": True})
    e.run(prober.make(e.ctx), 1)
    answered = probe_row(e, pid)["answered_at"]
    first, second = spells(e, 1)
    assert first.left_at == at(60)
    assert (second.start_source, second.known_from, second.end_source) == ("probe", answered, None)
    (only,) = spells(e, 2)
    assert only.start_source == "probe"
    assert probe_row(e, pid + 1)[("is_member_seen")] is True


@pytest.mark.parametrize("status, reason", [("left", "gone"), ("kicked", "kicked")])
def test_p3_out_closes_with_bounds(env, status, reason):
    e = env
    e.w.join(1, at(0))
    e.w.event(1, at(90), "member", "administrator")  # L
    e.w.rebuild()
    pid = queue(e, 1, "snapshot_absent", snapshot=e.w.snapshot(at(60 * 24 * 3), {2: (at(60 * 24 * 3 + 1), None)}))
    e.w.rebuild()
    e.w.c.commit()
    answer(e, default=status)
    e.run(prober.make(e.ctx), 1)
    row = probe_row(e, pid)
    assert (row["outcome"], row["status_seen"]) == ("out", status)
    (s,) = spells(e, 1)
    assert (s.end_source, s.end_reason, s.left_after, s.left_before) == ("probe", reason, at(90), row["answered_at"])
    dirty = psycopg.connect(e.dsn).execute("SELECT from_day FROM tg.rollup_dirty WHERE chat_id = %s",
                                           (CHAT,)).fetchone()
    assert dirty is not None


def test_restricted_not_member_is_out(env):
    e = env
    e.w.join(1, at(0))
    e.w.rebuild()
    pid = queue(e, 1)
    e.w.c.commit()
    e.fake.respond("getChatMember", lambda p: {"status": "restricted", "is_member": False})
    e.run(prober.make(e.ctx), 1)
    row = probe_row(e, pid)
    assert (row["outcome"], row["status_seen"], row["is_member_seen"]) == ("out", "restricted", False)
    assert spells(e, 1)[0].end_source == "probe"


def test_an_answer_without_a_status_is_a_failure(env):
    e = env
    pid = queue(e, 1)
    e.w.c.commit()
    e.fake.respond("getChatMember", lambda p: {"status": "owner?"})
    e.run(prober.make(e.ctx), 1)
    row = probe_row(e, pid)
    assert (row["outcome"], row["attempts"]) == ("pending", 1)


# --- the answer belongs to the probe it was asked for ----------------------------------------------------------------

def _held(e: Env, status: str = "left"):
    """getChatMember blocks until `go` is set; `sent` is set when the request arrived."""
    sent, go = threading.Event(), threading.Event()

    def compute(params):
        sent.set()
        assert go.wait(10)
        return {"status": status}
    e.fake.respond("getChatMember", compute)
    return sent, go


def _step_in_thread(e: Env, job) -> threading.Thread:
    errors: list[BaseException] = []

    def body():
        try:
            e.tick(job)
        except BaseException as x:  # noqa: BLE001
            errors.append(x)
    t = threading.Thread(target=body)
    t.errors = errors  # type: ignore[attr-defined]
    t.start()
    return t


def test_m1_during_an_in_flight_probe_drops_the_old_answer(env):
    e = env
    new = -1009000000077
    e.w.join(1, at(0))
    e.w.rebuild()
    pid = queue(e, 1, "outage")
    e.w.c.commit()
    sent, go = _held(e, "left")
    p = prober.make(e.ctx)
    t = _step_in_thread(e, p)
    assert sent.wait(10)
    with psycopg.connect(e.dsn) as c:  # ingest's M1: the history moves to the supergroup, pairs rebuilt
        c.execute("INSERT INTO tg.chat (chat_id, chat_type, title, tracked) VALUES (%s, 'supergroup', 'new', true)",
                  (new,))
        history.lock_chats(c, [CHAT, new])
        c.execute("SELECT tg.move_chat_history(%s, %s, %s)", (CHAT, new, at(60 * 24)))
        history.rebuild_pairs(c, [(new, 1)])
        c.commit()
    go.set()
    t.join(10)
    assert not t.errors
    row = probe_row(e, pid)
    assert (row["chat_id"], row["outcome"], row["answered_at"]) == (new, "pending", None)
    (s,) = spells(e, 1, new)
    assert s.end_source is None  # the old id's "out" never closed the moved stay
    assert spells(e, 1, CHAT) == []
    # the re-keyed probe is asked for the new chat next
    e.fake.respond("getChatMember", lambda params: {"status": "member"})
    e.clock.advance(1)
    e.run(p, 1)
    assert asked(e)[-1] == (new, 1) and probe_row(e, pid)["outcome"] == "in"


def test_a_probe_cancelled_by_the_cli_while_in_flight_is_dropped(env):
    e = env
    pid = queue(e, 1, "conflict")
    e.w.c.commit()
    sent, go = _held(e, "member")
    t = _step_in_thread(e, prober.make(e.ctx))
    assert sent.wait(10)
    with psycopg.connect(e.dsn) as c:  # the CLI's manual probe replaces the pending one
        history.lock_chats(c, [CHAT])
        c.execute("UPDATE tg.probe SET outcome = 'cancelled' WHERE probe_id = %s", (pid,))
        c.execute("INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (%s, 1, 'manual')", (CHAT,))
    go.set()
    t.join(10)
    assert not t.errors
    assert probe_row(e, pid)["outcome"] == "cancelled"
    assert spells(e, 1) == []


# --- an answer that straddles an exact event of the pair (plan review 3) --------------------------------------------

def _ingest_join(dsn: str, user: int, t: datetime, update_id: int) -> None:
    """What ingest does for a join: the event and the pair's rebuild in one locked transaction."""
    with psycopg.connect(dsn) as c:
        history.lock_chats(c, [CHAT])
        c.execute("""INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status)
                     VALUES (%s, %s, %s, %s, 'left', 'member')""", (CHAT, user, t, update_id))
        history.rebuild_pairs(c, [(CHAT, user)])


@pytest.mark.parametrize("reason", ["manual", "conflict"])
def test_out_held_across_a_join_committed_first_is_not_stored_and_is_asked_again(env, reason):
    e = env
    e.w.user(1)
    pid = queue(e, 1, reason)
    e.w.c.commit()
    sent, go = _held(e, "left")
    p = prober.make(e.ctx)
    t = _step_in_thread(e, p)
    assert sent.wait(10)
    event_at = START.replace(microsecond=0)  # the same second as the send: Telegram dates are whole seconds
    _ingest_join(e.dsn, 1, event_at, 910001)
    e.clock.advance(2)
    go.set()
    t.join(10)
    assert not t.errors
    row = probe_row(e, pid)
    assert (row["outcome"], row["attempts"], row["sent_at"]) == ("pending", 0, None)
    (s,) = spells(e, 1)
    assert (s.start_source, s.joined_at, s.end_source) == ("event", event_at, None)
    e.fake.respond("getChatMember", lambda params: {"status": "member"})
    e.clock.advance(1)
    e.run(p, 1)
    row = probe_row(e, pid)
    assert (row["outcome"], row["reason"]) == ("in", reason) and len(asked(e)) == 2


@pytest.mark.parametrize("reason", ["manual", "conflict"])
def test_out_committed_before_a_join_inside_its_call_is_ignored_and_asked_again(env, reason):
    e = env
    e.w.user(1)
    pid = queue(e, 1, reason)
    e.w.c.commit()
    sent, go = _held(e, "left")
    p = prober.make(e.ctx)
    t = _step_in_thread(e, p)
    assert sent.wait(10)
    e.clock.advance(3)
    go.set()
    t.join(10)
    assert not t.errors
    row = probe_row(e, pid)
    assert row["outcome"] == "out" and row["sent_at"] == START.replace(microsecond=0)
    assert spells(e, 1) == []  # an out with nothing open changes nothing
    # the join happened during the call and arrives later (a late update): the rebuild ignores the answer and the
    # trigger queues the same question again, in the join's transaction
    _ingest_join(e.dsn, 1, START.replace(microsecond=0) + timedelta(seconds=1), 910002)
    (s,) = spells(e, 1)
    assert (s.start_source, s.end_source) == ("event", None)  # the straddling "out" never closes it
    with psycopg.connect(e.dsn) as c:
        again = c.execute("SELECT probe_id, reason, queued_at FROM tg.probe WHERE chat_id = %s AND user_id = 1 "
                          "AND outcome = 'pending'", (CHAT,)).fetchall()
    assert len(again) == 1 and again[0][1:] == (reason, probe_row(e, pid)["queued_at"])
    assert probe_row(e, pid)["outcome"] == "out"  # the answered row stays as history ignores it
    # the second call
    e.fake.respond("getChatMember", lambda params: {"status": "member"})
    e.clock.advance(1)
    e.run(p, 1)
    assert len(asked(e)) == 2
    assert probe_row(e, again[0][0])["outcome"] == "in"
    (s,) = spells(e, 1)
    assert s.end_source is None and s.last_confirmed_at == probe_row(e, again[0][0])["answered_at"]


def test_the_requeue_trigger_keeps_one_pending_probe_and_rolls_back_with_the_event(env):
    e = env
    w = e.w
    w.user(1)
    w.user(2)
    sent = START - timedelta(seconds=10)
    old1 = w.probe(1, sent + timedelta(seconds=2), "out", sent=sent, reason="outage")
    w.probe(2, sent + timedelta(seconds=2), "in", sent=sent, reason="manual")
    pending2 = queue(e, 2, "sample")  # a probe of the pair is already pending: it asks the question
    w.c.commit()
    with psycopg.connect(e.dsn) as c:  # an event inside both calls whose transaction rolls back: nothing queued
        history.lock_chats(c, [CHAT])
        for u, upd in ((1, 920001), (2, 920002)):
            c.execute("""INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status)
                         VALUES (%s, %s, %s, %s, 'left', 'member')""", (CHAT, u, sent + timedelta(seconds=1), upd))
        assert c.execute("SELECT count(*) FROM tg.probe WHERE outcome = 'pending'").fetchone()[0] == 2
        c.rollback()
    with psycopg.connect(e.dsn) as c:
        assert c.execute("SELECT count(*) FROM tg.probe WHERE outcome = 'pending'").fetchone()[0] == 1
    _ingest_join(e.dsn, 1, sent + timedelta(seconds=1), 920003)
    _ingest_join(e.dsn, 2, sent + timedelta(seconds=1), 920004)
    _ingest_join(e.dsn, 1, sent + timedelta(seconds=5), 920005)  # outside the call: no new question
    with psycopg.connect(e.dsn) as c:
        rows = c.execute("SELECT user_id, reason, outcome FROM tg.probe WHERE outcome = 'pending' ORDER BY user_id"
                         ).fetchall()
    assert rows == [(1, "outage", "pending"), (2, "sample", "pending")]
    assert probe_row(e, old1)["outcome"] == "out" and probe_row(e, pending2)["outcome"] == "pending"


def test_a_conflict_of_another_chat_queued_during_a_backlog_is_asked_next(env):
    e = env
    e.w.add_chat(OTHER)
    for u in range(1, 301):
        queue(e, u, "sample")
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 3.0)  # the backlog is cached and draining with the whole budget
    n = len(asked(e))
    assert all(c == CHAT for c, _ in asked(e))
    for u in range(1001, 1051):
        queue(e, u, "conflict", chat=OTHER, queued=START)
    e.w.c.commit()
    e.run(p, 0.3)
    assert asked(e)[n] == (OTHER, 1001)  # the very next call
    e.run(p, 10.0)
    later = asked(e)[n:]
    a = sum(1 for c, _ in later if c == CHAT)
    assert a <= len(later) / 2 + 3, (a, len(later))  # the busy chat is held to half while the other waits
    assert [u for c, u in later if c == OTHER] == list(range(1001, 1001 + len(later) - a))


def test_a_conflict_queued_after_the_read_goes_before_cached_lower_priority_probes(env):
    e = env
    for u in range(1, 101):
        queue(e, u, "sample")
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 1.5)
    n = len(asked(e))
    queue(e, 500, "conflict", queued=START)
    e.w.c.commit()
    e.run(p, 0.3)
    assert asked(e)[n] == (CHAT, 500)


def test_no_transaction_is_open_during_the_call(env):
    e = env
    queue(e, 1)
    e.w.c.commit()
    seen = []

    def compute(params):
        seen.append(e.conn.info.transaction_status)
        return {"status": "member"}
    e.fake.respond("getChatMember", compute)
    e.run(prober.make(e.ctx), 1)
    assert seen == [psycopg.pq.TransactionStatus.IDLE]


def test_not_due_without_a_bot_api(env):
    e = env
    e.ctx.api = None
    assert not prober.make(e.ctx).due(e.clock())


def test_probe_rate_comes_from_the_environment():
    from tgaud import config
    assert config.load({}).probe_rate == 5.0
    assert config.load({"TGAUD_PROBE_RATE": "12.5"}).probe_rate == 12.5
    for bad in ("0", "fast", "nan", "inf", "101"):
        with pytest.raises(config.ConfigError, match="TGAUD_PROBE_RATE"):
            config.load({"TGAUD_PROBE_RATE": bad})
