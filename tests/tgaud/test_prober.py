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


def _pending(dsn: str) -> list[tuple]:
    with psycopg.connect(dsn) as c:
        return c.execute("SELECT user_id, reason, attempts FROM tg.probe WHERE outcome = 'pending' ORDER BY user_id"
                         ).fetchall()


def test_the_requeue_trigger_keeps_one_pending_probe_at_the_higher_priority_and_rolls_back(env):
    e = env
    w = e.w
    for u in (1, 2, 3):
        w.user(u)
    sent = START - timedelta(seconds=10)
    old1 = w.probe(1, sent + timedelta(seconds=2), "out", sent=sent, reason="outage")
    w.probe(2, sent + timedelta(seconds=2), "in", sent=sent, reason="manual")
    w.probe(3, sent + timedelta(seconds=2), "out", sent=sent, reason="outage")
    low2 = queue(e, 2, "sample")  # pending below the manual answer, in backoff after a failure
    w.c.execute("UPDATE tg.probe SET attempts = 1, last_attempt_at = %s, next_attempt_at = %s WHERE probe_id = %s",
                (START, START + timedelta(minutes=1), low2))
    high3 = queue(e, 3, "conflict")  # pending above the outage answer: it already asks the question
    w.c.commit()
    before = [(2, "sample", 1), (3, "conflict", 0)]
    assert _pending(e.dsn) == before
    with psycopg.connect(e.dsn) as c:  # events inside the calls whose transaction rolls back: nothing changes
        history.lock_chats(c, [CHAT])
        for u, upd in ((1, 920001), (2, 920002), (3, 920003)):
            c.execute("""INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status)
                         VALUES (%s, %s, %s, %s, 'left', 'member')""", (CHAT, u, sent + timedelta(seconds=1), upd))
        assert c.execute("SELECT count(*) FROM tg.probe WHERE outcome = 'pending'").fetchone()[0] == 3
        c.rollback()
    assert _pending(e.dsn) == before and probe_row(e, low2)["outcome"] == "pending"
    for u, upd in ((1, 920011), (2, 920012), (3, 920013)):
        _ingest_join(e.dsn, u, sent + timedelta(seconds=1), upd)
    _ingest_join(e.dsn, 1, sent + timedelta(seconds=5), 920014)  # outside the call: no new question
    # 1: requeued; 2: the manual question takes the sample's place, fresh (no attempts, no backoff); 3: unchanged
    assert _pending(e.dsn) == [(1, "outage", 0), (2, "manual", 0), (3, "conflict", 0)]
    assert probe_row(e, low2)["outcome"] == "cancelled" and probe_row(e, high3)["outcome"] == "pending"
    assert probe_row(e, old1)["outcome"] == "out"


def test_a_straddled_manual_answer_outranks_a_pending_probe_in_backoff(env):
    e = env
    e.w.user(1)
    pid = queue(e, 1, "manual")
    e.w.c.commit()
    sent, go = _held(e, "left")
    p = prober.make(e.ctx)
    t = _step_in_thread(e, p)
    assert sent.wait(10)
    e.clock.advance(3)
    go.set()
    t.join(10)
    assert not t.errors and probe_row(e, pid)["outcome"] == "out"  # the answer commits first
    low = queue(e, 1, "sample")  # later a routine probe of the pair fails once: in backoff for a minute
    e.w.c.execute("UPDATE tg.probe SET attempts = 1, last_attempt_at = %s, next_attempt_at = %s WHERE probe_id = %s",
                  (e.clock.now, e.clock.now + timedelta(minutes=1), low))
    e.w.c.commit()
    _ingest_join(e.dsn, 1, START.replace(microsecond=0) + timedelta(seconds=1), 930001)  # dated inside the call
    assert _pending(e.dsn) == [(1, "manual", 0)]
    e.fake.respond("getChatMember", lambda params: {"status": "member"})
    e.clock.advance(1)
    e.run(p, 1)  # well inside the sample's backoff
    assert len(asked(e)) == 2
    with psycopg.connect(e.dsn) as c:
        assert c.execute("SELECT reason, outcome FROM tg.probe WHERE chat_id = %s AND user_id = 1 ORDER BY probe_id",
                         (CHAT,)).fetchall() == [("manual", "out"), ("sample", "cancelled"), ("manual", "in")]


def test_a_cached_probe_raised_to_conflict_in_place_is_asked_next(env):
    e = env
    ids = [queue(e, u, "sample") for u in range(1, 201)]
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 2.0)  # cached and draining
    n = len(asked(e))
    with psycopg.connect(e.dsn) as c:  # the reconciler raises a cached routine probe in place
        c.execute("UPDATE tg.probe SET reason = 'conflict' WHERE probe_id = %s", (ids[150],))
    e.run(p, 0.3)
    assert asked(e)[n] == (CHAT, 151)


def test_a_new_outage_probe_of_a_newer_spell_goes_before_cached_older_ones(env):
    e = env
    w = e.w
    for u in range(1, 101):
        w.join(u, at(u))  # spell u starts at minute u
    w.join(500, at(1000))  # the newest spell
    w.rebuild()
    for u in range(1, 101):
        queue(e, u, "outage", queued=START - timedelta(hours=2))
    w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 2.0)
    assert [u for _, u in asked(e)][:5] == [100, 99, 98, 97, 96]  # newest spell first
    n = len(asked(e))
    queue(e, 500, "outage", queued=START)
    e.w.c.commit()
    e.run(p, 0.3)
    assert asked(e)[n] == (CHAT, 500)


def test_a_reconcile_batch_of_outage_probes_is_asked_by_spell_not_by_queue_order(env):
    """probe-sample review-3: 200 outage probes are cached; reconcile's tg.queue_probes then queues six more in one
    batch (one queued_at, ids in user order): the first queued user has the newest spell of all, the other five the
    oldest. The newest spell is the next call; the five oldest wait behind every cached probe."""
    e = env
    w = e.w
    for u in range(1, 201):
        w.join(u, at(100 + u))  # cached: spells from minute 101 to 300
    w.join(301, at(1000))  # the newest spell
    for u in range(302, 307):
        w.join(u, at(u - 301))  # the oldest: minutes 1-5
    w.rebuild()
    for u in range(1, 201):
        queue(e, u, "outage", queued=START - timedelta(hours=2))
    w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 2.0)
    assert [u for _, u in asked(e)][:3] == [200, 199, 198]
    n = len(asked(e))
    with psycopg.connect(e.dsn) as c:
        assert c.execute("SELECT tg.queue_probes(%s::bigint[], %s::bigint[], 'outage')",
                         ([CHAT] * 6, list(range(301, 307)))).fetchone()[0] == 6
    e.run(p, 20.0)
    later = [u for _, u in asked(e)[n:]]
    assert later[0] == 301 and len(later) > 90
    assert later[1:] == sorted(later[1:], reverse=True) and not set(later) & set(range(302, 307))


def _outage_backlog(e: Env, newest: int = 301) -> None:
    """200 outage probes of spells from minute 101 to 300, and user `newest` with the newest spell, no probe yet."""
    for u in range(1, 201):
        e.w.join(u, at(100 + u))
    e.w.join(newest, at(1000))
    for u in range(302, 307):
        e.w.join(u, at(u - 301))  # the oldest spells
    e.w.rebuild()
    for u in range(1, 201):
        queue(e, u, "outage", queued=START - timedelta(hours=2))


def test_a_probe_promoted_to_outage_in_place_is_asked_by_its_spell(env):
    """review round 1: the newest spell's pair has an hour-old sample probe, cached at the end of the queue; the batch
    of tg.queue_probes raises it to outage in place (its queued_at and id stay) and adds five old spells. The promoted
    probe is the next call, once; the five wait behind every cached outage probe."""
    e = env
    _outage_backlog(e)
    queue(e, 301, "sample", queued=START - timedelta(hours=1))
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 2.0)
    n = len(asked(e))
    with psycopg.connect(e.dsn) as c:
        assert c.execute("SELECT tg.queue_probes(%s::bigint[], %s::bigint[], 'outage')",
                         ([CHAT] * 6, list(range(301, 307)))).fetchone()[0] == 6  # one raised, five inserted
    e.run(p, 20.0)
    later = [u for _, u in asked(e)[n:]]
    assert later[0] == 301 and later.count(301) == 1 and len(later) > 90
    assert later[1:] == sorted(later[1:], reverse=True) and not set(later) & set(range(302, 307))


def test_an_outage_probe_out_of_its_backoff_is_asked_at_its_place(env):
    """review round 1: the newest spell's outage probe failed once 30 s before the cache was read (backoff 60 s): it is
    cached waiting, skipped until its backoff ends, and the first call after that."""
    e = env
    _outage_backlog(e)
    pid = queue(e, 301, "outage", queued=START - timedelta(hours=2))
    e.w.c.execute("UPDATE tg.probe SET attempts = 1, last_attempt_at = %s, next_attempt_at = %s WHERE probe_id = %s",
                  (START - timedelta(seconds=30), START + timedelta(seconds=30), pid))
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 29.0)
    assert 301 not in [u for _, u in asked(e)]
    n = len(asked(e))
    e.clock.set(START + timedelta(seconds=30))  # the end of its backoff
    e.run(p, 2.0)
    assert asked(e)[n] == (CHAT, 301)
    assert probe_row(e, pid)["outcome"] == "in"


def test_an_outage_set_swapped_with_the_same_count_and_id_sum_is_ranked_again(env):
    """crossfix review 2: four consecutive probes a < b < c < d, so a + d = b + c. b and c (outage, old spells) are
    raised to manual, a and d (sample probes of the two newest spells) raised to outage, in one transaction: the
    outage set keeps its count and id sum. The manual ones go first, then the two promoted newest spells."""
    e = env
    for u in range(1, 201):
        e.w.join(u, at(100 + u))
    for u, minute in ((401, 1000), (402, 5), (403, 6), (404, 999)):
        e.w.join(u, at(minute))
    e.w.rebuild()
    for u in range(1, 201):
        queue(e, u, "outage", queued=START - timedelta(hours=2))
    ids = [queue(e, 401, "sample"), queue(e, 402, "outage"), queue(e, 403, "outage"), queue(e, 404, "sample")]
    assert ids[0] + ids[3] == ids[1] + ids[2]
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 2.0)
    n = len(asked(e))
    with psycopg.connect(e.dsn) as c:
        c.execute("UPDATE tg.probe SET reason = 'manual' WHERE probe_id = ANY(%s)", ([ids[1], ids[2]],))
        c.execute("UPDATE tg.probe SET reason = 'outage' WHERE probe_id = ANY(%s)", ([ids[0], ids[3]],))
    e.run(p, 3.0)
    assert [u for _, u in asked(e)[n:n + 4]] == [402, 403, 401, 404]


def test_a_rebuild_that_makes_a_cached_pairs_spell_the_newest_moves_its_probe_up(env):
    """crossfix review 2: user 1 has the oldest spell of the 200 cached outage probes; late events (a leave and a rejoin
    after every other spell started) are ingested and rebuilt, so its newest spell is now the newest of all, with the
    same probe id. It is the next call."""
    e = env
    _outage_backlog(e)
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 2.0)
    n = len(asked(e))
    with psycopg.connect(e.dsn) as c:  # ingest: the events and the pair's rebuild under the chat's lock
        history.lock_chats(c, [CHAT])
        for update_id, (old, new, minute) in enumerate((("member", "left", 950), ("left", "member", 951)), 920001):
            c.execute("INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status) "
                      "VALUES (%s, 1, %s, %s, %s, %s)", (CHAT, at(minute), update_id, old, new))
        history.rebuild_pairs(c, [(CHAT, 1)])
    e.run(p, 1.0)
    assert asked(e)[n] == (CHAT, 1)


def test_an_outage_retry_left_out_of_the_cache_is_asked_when_it_becomes_eligible(env):
    """crossfix review 2: 600 newer spells are in a ten-minute backoff (the cache's share of probes in backoff); the
    next spell, user 601, is in a one-minute backoff that ends 2 s after the full read, so it is left out. Once
    eligible it is the newest eligible spell: the chat is read again and it is the next call."""
    e = env
    w = e.w
    for u in range(1, 101):
        w.join(u, at(100 + u))  # ready, older
    for u in range(1001, 1601):
        w.join(u, at(u))  # newer, in backoff
    w.join(601, at(1000))
    w.rebuild()
    for u in range(1, 101):
        queue(e, u, "outage", queued=START - timedelta(hours=2))
    waiting = [queue(e, u, "outage", queued=START - timedelta(hours=2)) for u in range(1001, 1601)]
    w.c.execute("UPDATE tg.probe SET attempts = 2, last_attempt_at = %s, next_attempt_at = %s WHERE probe_id = ANY(%s)",
                (START - timedelta(minutes=1), START + timedelta(minutes=9), waiting))
    pid = queue(e, 601, "outage", queued=START - timedelta(hours=2))
    w.c.execute("UPDATE tg.probe SET attempts = 1, last_attempt_at = %s, next_attempt_at = %s WHERE probe_id = %s",
                (START - timedelta(seconds=58), START + timedelta(seconds=2), pid))
    w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 1.5)
    assert 601 not in [u for _, u in asked(e)]
    n = len(asked(e))
    e.clock.set(START + timedelta(seconds=2))  # the end of its backoff
    e.run(p, 1.0)
    assert asked(e)[n] == (CHAT, 601)


def test_cancelling_a_chats_cached_probes_keeps_its_share(env):
    """crossfix review 2: chat CHAT has 900 outage probes, so the full read cuts it at 600; another chat waits too.
    Every pending row of CHAT's cached 600 is cancelled: the chat is read again with a cut of its own, and its 300
    survivors keep getting their half of the calls."""
    e = env
    w = e.w
    w.add_chat(OTHER)
    for u in range(1, 901):
        w.join(u, at(u))
    w.rebuild()
    for u in range(1, 901):
        queue(e, u, "outage", queued=START - timedelta(hours=2))
    for u in range(2001, 2301):
        queue(e, u, "conflict", chat=OTHER, queued=START - timedelta(hours=2))
    w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 2.0)
    n = len(asked(e))
    with psycopg.connect(e.dsn) as c:  # the reconciler or the CLI cancels the rest of the top 600 (users 301-900)
        c.execute("UPDATE tg.probe SET outcome = 'cancelled' WHERE chat_id = %s AND user_id > 300 "
                  "AND outcome = 'pending'", (CHAT,))
    e.run(p, 15.0)
    later = asked(e)[n:]
    mine = [u for c, u in later if c == CHAT]
    assert len(later) > 60 and 0.3 * len(later) <= len(mine) <= 0.55 * len(later), (len(mine), len(later))
    assert mine == sorted(mine, reverse=True) and max(mine) <= 300  # the survivors, newest spell first


def test_a_straddled_outage_answer_is_asked_again_from_the_next_second(env):
    """crossfix review 2: a role change of user 200 (the newest spell of the cached 200) commits during its call, dated
    the second the call returns in; the answer straddles the event and is not stored. The probe goes back into the
    cache, eligible from the next second (a new call in the event's second would straddle it again), and is asked
    then, ahead of the older spells (the event's rebuild re-keys it without making it eligible earlier); its second
    answer is stored."""
    e = env
    _outage_backlog(e)
    pid = e.w.c.execute("SELECT probe_id FROM tg.probe WHERE user_id = 200").fetchone()[0]
    e.w.c.commit()
    sent, go = _held(e, "member")
    p = prober.make(e.ctx)
    t = _step_in_thread(e, p)
    assert sent.wait(10)
    event_second = (START + timedelta(seconds=2)).replace(microsecond=0)  # the call returns at START + 2 s
    with psycopg.connect(e.dsn) as c:  # ingest: the role change and the pair's rebuild
        history.lock_chats(c, [CHAT])
        c.execute("INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status) "
                  "VALUES (%s, 200, %s, 930001, 'member', 'administrator')", (CHAT, event_second))
        history.rebuild_pairs(c, [(CHAT, 200)])
    e.clock.advance(2)
    go.set()
    t.join(10)
    assert not t.errors and probe_row(e, pid)["outcome"] == "pending"
    answer(e)
    n = len(asked(e))
    e.run(p, 3.0)
    later = [u for _, u in asked(e)[n:]]
    assert 200 in later[:4] and later.count(200) == 1  # only the calls of the event's second go before it
    assert probe_row(e, pid)["outcome"] == "in"


def test_a_rebuild_committed_across_head_reads_still_moves_its_probe_up(env):
    """The order of the spell mark: ingest's rebuild of user 1 (late leave and rejoin: its spell becomes the newest of
    all) takes its spell row ids and holds the chat's lock; another chat's rebuild takes later ids and commits; while
    the prober asks that chat's manual probes, its head reads mark past both; only then does the first commit. The
    head reads scan from a mark SPELL_MARGIN old, so they still see it: user 1 is the chat's next call."""
    e = env
    e.w.add_chat(OTHER)
    e.w.join(9999, at(1), chat=OTHER)
    _outage_backlog(e)
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 2.0)
    first = psycopg.connect(e.dsn)
    try:
        history.lock_chats(first, [CHAT])
        first.execute("INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status) "
                      "VALUES (%s, 1, %s, 940001, 'member', 'left'), (%s, 1, %s, 940002, 'left', 'member')",
                      (CHAT, at(950), CHAT, at(951)))
        history.rebuild_pairs(first, [(CHAT, 1)])  # ids taken, not committed
        with psycopg.connect(e.dsn) as later:  # another chat's rebuild: later ids, committed first
            history.lock_chats(later, [OTHER])
            later.execute("INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status) "
                          "VALUES (%s, 9999, %s, 940003, 'member', 'left')", (OTHER, at(5)))
            history.rebuild_pairs(later, [(OTHER, 9999)])
            for u in range(3001, 3016):  # work of the other chat for the prober meanwhile (manual: first, no share)
                later.execute("INSERT INTO tg.tg_user (user_id, first_seen_at, last_seen_at) VALUES (%s, now(), now()) "
                              "ON CONFLICT DO NOTHING", (u,))
                later.execute("INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (%s, %s, 'manual')", (OTHER, u))
        n = len(asked(e))
        e.run(p, 1.5)  # several steps, each with a head read that marks past the later ids
        assert all(c == OTHER for c, _ in asked(e)[n:]) and len(asked(e)) - n >= 3
        first.commit()
    finally:
        first.close()
    n = len(asked(e))
    e.run(p, 4.0)
    assert [u for c, u in asked(e)[n:] if c == CHAT][0] == 1


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


# --- prober-simplify: the four ways the old cache drifted (crossfix review 3) --------------------------------------

def test_rank_changes_committed_late_by_other_transactions_are_seen(env):
    """review 3, finding 1: (a) ingest's rebuild of user 1 (a late leave and rejoin: its spell becomes the newest of
    all) holds its spell ids uncommitted across many steps and far longer than any margin, while another chat's
    rebuild commits; once it commits, user 1 is the chat's next call. (b) a rebuild that only deletes the spell of
    user 501 (a sighting of a snapshot then rejected): 501 had the newest spell, now has none and goes after every
    probe with an open spell."""
    e = env
    e.w.add_chat(OTHER)
    e.w.join(9999, at(1), chat=OTHER)
    _outage_backlog(e)
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 2.0)
    first = psycopg.connect(e.dsn)
    try:
        history.lock_chats(first, [CHAT])
        first.execute("INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status) "
                      "VALUES (%s, 1, %s, 960001, 'member', 'left'), (%s, 1, %s, 960002, 'left', 'member')",
                      (CHAT, at(950), CHAT, at(951)))
        history.rebuild_pairs(first, [(CHAT, 1)])  # ids taken, not committed
        with psycopg.connect(e.dsn) as later:  # another chat's rebuild: later ids, committed first
            history.lock_chats(later, [OTHER])
            later.execute("INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status) "
                          "VALUES (%s, 9999, %s, 960003, 'member', 'left')", (OTHER, at(5)))
            history.rebuild_pairs(later, [(OTHER, 9999)])
        for u in range(3001, 3061):  # work of the other chat meanwhile (manual: first, no share); CHAT is locked
            queue(e, u, "manual", chat=OTHER)
        e.w.c.commit()
        n = len(asked(e))
        e.run(p, 8.0)  # 45 calls at most: the manual probes last, so the job never waits for `first`'s lock
        assert {c for c, _ in asked(e)[n:]} == {OTHER} and len(asked(e)) - n >= 40
        first.commit()
    finally:
        first.close()
    n = len(asked(e))
    e.run(p, 5.0)  # the rest of the manual probes, then CHAT
    assert [u for c, u in asked(e)[n:] if c == CHAT][0] == 1

    # (b) user 501: listed by a snapshot only, so its spell is the newest; then the owner rejects the snapshot
    sid = e.w.snapshot(at(2000), {501: (at(2000), None)}, good=False)
    e.w.rebuild([501])
    queue(e, 501, "outage", queued=START)
    e.w.c.commit()
    assert e.w.c.execute("SELECT spell_from FROM tg.probe WHERE user_id = 501 AND outcome = 'pending'").fetchone()[0]
    e.w.reject(sid)
    e.w.rebuild([501])  # deletes its only spell, inserts nothing
    e.w.c.commit()
    assert e.w.spells(501) == []
    n = len(asked(e))
    e.run(p, 60.0)
    later = [u for _, u in asked(e)[n:]]
    assert later[-1] == 501 and later[:-1] == sorted(later[:-1], reverse=True), later


def _two_chat_backlog(e: Env, per_chat: int) -> None:
    """`per_chat` outage probes in CHAT (users 1..) and in OTHER (users 10001..), spells one minute apart."""
    e.w.add_chat(OTHER)
    for u in range(1, per_chat + 1):
        e.w.join(u, at(u))
        e.w.join(10000 + u, at(u) + timedelta(seconds=30), chat=OTHER)
    e.w.rebuild()
    history.rebuild_pairs(e.w.c, [(OTHER, 10000 + u) for u in range(1, per_chat + 1)])
    for u in range(1, per_chat + 1):
        queue(e, u, "outage", queued=START - timedelta(hours=2))
        queue(e, 10000 + u, "outage", chat=OTHER, queued=START - timedelta(hours=2))


def _longest_run(chats: list[int]) -> int:
    best = run = 0
    for i, c in enumerate(chats):
        run = run + 1 if i and chats[i - 1] == c else 1
        best = max(best, run)
    return best


def test_each_chat_keeps_its_half_while_the_other_has_ready_probes(env):
    """review 3, finding 2: two chats with 900 outage probes each, at 100 calls/s (the old cache cut each chat at 600
    and gave every call to the other chat once those ran out). Late joins of CHAT (users 1-50, the oldest, rejoin
    after everyone) make their probes the chat's newest; they are the chat's next calls. Each chat keeps its half
    until well past 600 calls of its own: in every 200 consecutive calls each has 40-60 %, and no chat gets more than
    its bucket's burst in a row."""
    e = env
    _two_chat_backlog(e, 900)
    e.w.c.commit()
    answer(e)
    p = prober.Prober(e.ctx, rate=100.0)
    e.run(p, 3.0, dt=0.02)
    n = len(asked(e))
    with psycopg.connect(e.dsn) as c:  # ingest: the late leaves and rejoins of users 1-50 and their rebuild
        history.lock_chats(c, [CHAT])
        for u in range(1, 51):
            c.execute("INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status) "
                      "VALUES (%s, %s, %s, %s, 'member', 'left'), (%s, %s, %s, %s, 'left', 'member')",
                      (CHAT, u, at(5000 + u), 970000 + 2 * u, CHAT, u, at(5001 + u), 970001 + 2 * u))
        history.rebuild_pairs(c, [(CHAT, u) for u in range(1, 51)])
    e.run(p, 14.5, dt=0.02)
    calls = asked(e)
    later = calls[n:]
    assert [u for c, u in later if c == CHAT][:50] == list(range(50, 0, -1))  # the rejoined, newest first
    assert len(calls) >= 1600
    chats = [c for c, _ in calls[:1600]]
    for i in range(0, len(chats) - 200, 50):
        mine = chats[i:i + 200].count(CHAT)
        assert 80 <= mine <= 120, (i, mine)
    assert _longest_run(chats) <= 55 and chats.count(CHAT) > 700
    assert len(set(calls)) == len(calls)


def test_outage_probes_rank_by_the_open_spell_and_those_without_one_go_last(env):
    """review 3, finding 3: 200 outage probes; exact exits then close the 100 newest stays (users 101-200), whose
    probes stay pending. The 100 open, suspect stays are asked first, newest first; the closed ones after them, in
    queue order (queued_at, then probe id)."""
    e = env
    w = e.w
    for u in range(1, 201):
        w.join(u, at(u))
    w.rebuild()
    ids = {u: queue(e, u, "outage", queued=START - timedelta(hours=2)) for u in range(1, 201)}
    w.c.commit()
    for u in range(101, 201):
        w.leave(u, at(1000 + u))
    w.rebuild(list(range(101, 201)))
    w.c.commit()
    assert all(s.end_source is not None for u in range(101, 201) for s in w.spells(u))
    answer(e, default="left")
    e.run(prober.make(e.ctx), 45.0)
    calls = [u for _, u in asked(e)]
    assert calls[:100] == list(range(100, 0, -1))
    assert calls[100:200] == sorted(range(101, 201), key=lambda u: ids[u])


def test_a_retry_deadline_is_kept_while_nothing_else_is_ready(env):
    """review 3, finding 4 (and D3): 600 probes in a ten-minute backoff and one in a one-minute backoff that ends 2 s
    after the first read. The job is due at that deadline (not at the next idle read) and asks it then; a probe
    queued meanwhile while everything waits is asked within the idle interval."""
    e = env
    w = e.w
    for u in range(1001, 1601):
        w.join(u, at(u))
    w.join(601, at(1000))
    w.rebuild()
    waiting = [queue(e, u, "outage", queued=START - timedelta(hours=2)) for u in range(1001, 1601)]
    w.c.execute("UPDATE tg.probe SET attempts = 2, last_attempt_at = %s, next_attempt_at = %s WHERE probe_id = ANY(%s)",
                (START - timedelta(minutes=1), START + timedelta(minutes=9), waiting))
    pid = queue(e, 601, "outage", queued=START - timedelta(hours=2))
    w.c.execute("UPDATE tg.probe SET attempts = 1, last_attempt_at = %s, next_attempt_at = %s WHERE probe_id = %s",
                (START - timedelta(seconds=58), START + timedelta(seconds=2.3), pid))
    w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    seen = []
    while e.clock() < START.timestamp() + 4:
        n = len(asked(e))
        e.tick(p)
        if len(asked(e)) > n:
            seen.append(e.clock() - START.timestamp())
        e.clock.advance(0.1)
    assert asked(e) == [(CHAT, 601)] and 2.3 <= seen[0] < 2.45, seen
    assert probe_row(e, pid)["outcome"] == "in"
    queue(e, 77, "conflict", queued=START)
    w.c.commit()
    t = e.clock()
    while len(asked(e)) == 1 and e.clock() < t + 30:
        e.tick(p)
        e.clock.advance(0.1)
    assert asked(e)[1] == (CHAT, 77) and e.clock() - t <= prober.IDLE_REFRESH + 0.2


# --- prober-simplify review 1 -----------------------------------------------------------------------------------------

def test_a_status_that_is_not_a_string_is_a_failure_and_the_queue_moves_on(env):
    """review 1, finding 3: the chat's first probe gets a list as its status on every call. Each answer is a failure
    with its backoff (not an exception that rolls the step back), so the next probe is asked."""
    e = env
    first, second = queue(e, 1, queued=START - timedelta(hours=2)), queue(e, 2)
    e.w.c.commit()
    e.fake.respond("getChatMember", lambda params: {"status": ["member"]} if params["user_id"] == 1
                   else {"status": "member"})
    e.run(prober.make(e.ctx), 2.0)
    assert asked(e) == [(CHAT, 1), (CHAT, 2)]
    row = probe_row(e, first)
    assert (row["outcome"], row["attempts"]) == ("pending", 1)
    assert row["next_attempt_at"] == row["last_attempt_at"] + timedelta(seconds=prober.BACKOFF[0])
    assert probe_row(e, second)["outcome"] == "in"


def test_a_failure_written_without_its_deadline_gets_the_backoff_of_its_attempts(env):
    """review 1, finding 2: the previous image records a second failure with its own UPDATE (attempts and
    last_attempt_at, no next_attempt_at): the trigger gives it the ten-minute deadline, and the prober waits for it. A
    deadline set in the same UPDATE (this prober's) is kept, and one set alone (the straddle delay) is not touched."""
    e = env
    pid = queue(e, 1)
    e.w.c.commit()
    with psycopg.connect(e.dsn) as c:  # the previous image's failure write
        c.execute("UPDATE tg.probe SET attempts = %s, last_attempt_at = %s WHERE probe_id = %s", (2, START, pid))
    assert probe_row(e, pid)["next_attempt_at"] == START + timedelta(seconds=prober.BACKOFF[1])
    with psycopg.connect(e.dsn) as c:  # the SQL schedule is the job's, for every attempt count
        for n, backoff in enumerate(prober.BACKOFF + (prober.BACKOFF[-1],), 1):
            c.execute("UPDATE tg.probe SET attempts = %s, next_attempt_at = NULL WHERE probe_id = %s", (n, pid))
            c.execute("UPDATE tg.probe SET last_attempt_at = %s WHERE probe_id = %s", (START + timedelta(n), pid))
            assert c.execute("SELECT next_attempt_at FROM tg.probe WHERE probe_id = %s", (pid,)).fetchone()[0] == \
                START + timedelta(n) + timedelta(seconds=backoff)
        explicit = START + timedelta(seconds=7)
        c.execute("UPDATE tg.probe SET attempts = 2, last_attempt_at = %s, next_attempt_at = %s WHERE probe_id = %s",
                  (START - timedelta(seconds=1), explicit, pid))
        assert c.execute("SELECT next_attempt_at FROM tg.probe WHERE probe_id = %s", (pid,)).fetchone()[0] == explicit
        c.execute("UPDATE tg.probe SET next_attempt_at = %s WHERE probe_id = %s", (START, pid))  # the straddle delay
        assert c.execute("SELECT next_attempt_at FROM tg.probe WHERE probe_id = %s", (pid,)).fetchone()[0] == START
        c.execute("UPDATE tg.probe SET attempts = 2, last_attempt_at = %s WHERE probe_id = %s",
                  (START - timedelta(seconds=1), pid))  # the same values again: nothing to derive
        assert c.execute("SELECT next_attempt_at FROM tg.probe WHERE probe_id = %s", (pid,)).fetchone()[0] == START
        c.execute("UPDATE tg.probe SET attempts = 1, last_attempt_at = NULL, next_attempt_at = NULL "
                  "WHERE probe_id = %s", (pid,))
        c.execute("UPDATE tg.probe SET attempts = 2, last_attempt_at = %s WHERE probe_id = %s",
                  (START - timedelta(seconds=1), pid))  # the old image's second failure, a second before the job
    answer(e)
    p = prober.make(e.ctx)
    e.run(p, 598.0, dt=1.0)
    assert asked(e) == []
    e.run(p, 3.0, dt=0.1)
    assert asked(e) == [(CHAT, 1)] and probe_row(e, pid)["outcome"] == "in"


def test_a_locked_probe_past_its_deadline_does_not_hold_up_the_step(env):
    """Clearing passed deadlines skips rows another transaction holds (the reconciler raising one in place, the CLI
    cancelling it): the step neither waits nor fails, the other probes are asked, and the held one becomes ready by a
    later step once the lock is gone, at its place."""
    e = env
    held = queue(e, 1, queued=START - timedelta(hours=3))
    for u in (2, 3):
        queue(e, u)
    e.w.c.execute("UPDATE tg.probe SET attempts = 1, last_attempt_at = %s, next_attempt_at = %s WHERE probe_id = %s",
                  (START - timedelta(minutes=1), START, held))
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    other = psycopg.connect(e.dsn)
    try:
        other.execute("SELECT 1 FROM tg.probe WHERE probe_id = %s FOR UPDATE", (held,))
        t = _step_in_thread(e, p)
        t.join(10)
        assert not t.is_alive() and not t.errors
        assert asked(e) == [(CHAT, 2), (CHAT, 3)]
        assert probe_row(e, held)["next_attempt_at"] == START  # not cleared: someone else holds it
    finally:
        other.close()  # its transaction ends without a change
    e.clock.advance(1.0)
    e.run(p, 1.0)
    assert asked(e)[2:] == [(CHAT, 1)] and probe_row(e, held)["outcome"] == "in"


def test_a_step_interrupted_after_clearing_deadlines_leaves_them_for_the_next(env):
    """The clearing commits with the read: a step that fails before it commits (a lost connection, a stale
    generation) leaves the deadlines as they were, and the next step clears them again and asks the probe."""
    e = env
    pid = queue(e, 1)
    e.w.c.execute("UPDATE tg.probe SET attempts = 1, last_attempt_at = %s, next_attempt_at = %s WHERE probe_id = %s",
                  (START - timedelta(minutes=1), START, pid))
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.ctx.fence(e.conn)
    assert [c.probe_id for c in p.heads(e.conn, e.clock())] == [pid]
    e.conn.rollback()  # the step failed before its commit
    assert probe_row(e, pid)["next_attempt_at"] == START
    e.run(p, 1.0)
    assert asked(e) == [(CHAT, 1)] and probe_row(e, pid)["outcome"] == "in"


def test_passed_deadlines_beyond_one_steps_share_are_cleared_by_the_next_steps(env, monkeypatch):
    """After a long stop every deadline has passed: a step clears RELEASE_PER_STEP of them (the earliest first) and
    the job reads again at once, without the idle wait, until all are cleared; every probe is asked once."""
    e = env
    monkeypatch.setattr(prober, "RELEASE_PER_STEP", 3)
    ids = [queue(e, u) for u in range(1, 11)]
    e.w.c.execute("UPDATE tg.probe SET attempts = 1, last_attempt_at = %s, next_attempt_at = %s - (probe_id * "
                  "interval '1 second') WHERE probe_id = ANY(%s)", (START - timedelta(hours=1), START, ids))
    e.w.c.commit()
    answer(e)
    p = prober.make(e.ctx)
    e.tick(p)
    assert p.backlog and p.next_read <= e.clock() and len(asked(e)) == 3
    assert sorted(u for _, u in asked(e)) == [8, 9, 10]  # the earliest deadlines (the highest ids)
    e.run(p, 3.0)
    assert sorted(u for _, u in asked(e)) == list(range(1, 11)) and not p.backlog


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
