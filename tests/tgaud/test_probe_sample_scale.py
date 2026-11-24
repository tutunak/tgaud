"""Scale of the metric cache and the prober's queue read on the test server (production limits: 512 MB,
work_mem 2 MB, no parallel workers): 200 tracked chats, one of them with 1 000 000 open spells (1 % suspect),
30 days of drift samples for every chat (864 000 rows, the verify.md 5 size), 100 000 pending outage probes in the
big chat and a few in the others.

Targets (briefs): one metric cache refresh under 5 s; the prober's queue read (before every step) under 50 ms.
The measured times are printed (pytest -s) and written to the test's report section.
TGAUD_SCALE_SPELLS overrides the 1 000 000.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from tgaud import config, history, metrics
from tgaud.jobs import JobContext, prober, sampler

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
BIG = -1006000000001
SPELLS = int(os.environ.get("TGAUD_SCALE_SPELLS", "1000000"))


@pytest.fixture(scope="module")
def scale_dsn(pg):
    dsn = pg.make_db(migrated=True)
    t = time.perf_counter()
    with psycopg.connect(dsn) as c:
        c.execute("""INSERT INTO tg.chat (chat_id, chat_type, title, tracked)
                     SELECT -1006000000000 - k, 'channel', 'scale ' || k, true FROM generate_series(1, 200) k""")
        c.execute("""INSERT INTO tg.tg_user (user_id, first_seen_at, last_seen_at)
                     SELECT u, '2026-08-01', '2026-10-01' FROM generate_series(1, %s) u""", (SPELLS,))
        c.execute("""INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at,
                                                suspect_since)
                     SELECT %s, u, 'snapshot', timestamptz '2026-08-01' + (u %% 86400) * interval '1 second',
                            timestamptz '2026-09-01', CASE WHEN u %% 100 = 0 THEN timestamptz '2026-09-20' END
                     FROM generate_series(1, %s) u""", (BIG, SPELLS))
        c.execute("""INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at)
                     SELECT -1006000000000 - k, u, 'snapshot', '2026-08-01', '2026-09-01'
                     FROM generate_series(2, 200) k, generate_series(1, 500) u""")
        c.execute("""INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, suspect_open, net_events)
                     SELECT -1006000000000 - k, %s - i * interval '10 minutes', 1000 + i %% 7, 990, 0, i %% 3 - 1
                     FROM generate_series(1, 200) k, generate_series(1, 4320) i""", (NOW,))
        c.execute("""INSERT INTO tg.probe (chat_id, user_id, reason, queued_at)
                     SELECT %s, u, 'outage', timestamptz '2026-09-30' + (u %% 3600) * interval '1 second'
                     FROM generate_series(1, 1000000, 10) u""", (BIG,))
        c.execute("""INSERT INTO tg.probe (chat_id, user_id, reason, queued_at)
                     SELECT -1006000000000 - k, u, 'conflict', '2026-09-30'
                     FROM generate_series(2, 200) k, generate_series(1, 5) u""")
        c.execute("""INSERT INTO tg.snapshot (chat_id, started_at, finished_at, method, fetched, complete, quality)
                     SELECT -1006000000000 - k, '2026-09-25', '2026-09-25 03:00', 'listing', 500, true, 'good'
                     FROM generate_series(1, 200) k""")
        c.execute("""INSERT INTO tg.gap (chat_id, period, reason)
                     VALUES (NULL, tstzrange('2026-09-01', '2026-09-03'), 'service_down')""")
        c.execute("INSERT INTO tg.bot_state (bot_id, allowed_updates, caught_up_at) VALUES (5000, '{x}', %s)", (NOW,))
        c.commit()
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("VACUUM ANALYZE")
    print(f"\nscale fixture: {SPELLS} open spells in one chat, 200 chats, built in {time.perf_counter() - t:.0f} s")
    return dsn


def _ctx() -> JobContext:
    return JobContext(config=config.load({}), api=None, health=metrics.Health(), stop=threading.Event(),
                      generation=1, clock=lambda: NOW.timestamp())


def test_metric_cache_refresh_under_5_s(scale_dsn, record_property):
    job = sampler.Sampler(_ctx())
    with psycopg.connect(scale_dsn) as c:
        times = []
        for _ in range(3):
            t = time.perf_counter()
            snap = job.collect(c, NOW.timestamp())
            c.commit()
            job.publish(snap)
            times.append(time.perf_counter() - t)
    print(f"metric cache refresh: {', '.join(f'{x:.2f}' for x in times)} s (200 chats, {SPELLS} open spells)")
    record_property("metric_cache_refresh_seconds", times)
    assert len(snap.chats) == 200
    assert snap.values["known"][BIG] == SPELLS and snap.values["suspect"][BIG] == SPELLS // 100
    assert snap.values["probe_queue"][BIG] == 100000
    assert max(times) < 5.0


def test_finalize_and_sample_insert_on_the_big_chat(scale_dsn, record_property):
    with psycopg.connect(scale_dsn) as c:
        c.execute("""INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, net_events)
                     SELECT -1006000000000 - k, %s - interval '1 minute', 1000, 990, NULL FROM generate_series(1, 200) k""",
                  (NOW,))
        t = time.perf_counter()
        n = sampler.finalize(c)
        fin = time.perf_counter() - t
        t = time.perf_counter()
        c.execute(sampler._INSERT_SAMPLE, {"chat": BIG, "at": NOW, "reported": SPELLS, "ms": 1})  # noqa: SLF001
        ins = time.perf_counter() - t
        c.rollback()
    print(f"finalize 200 pending samples: {fin:.2f} s; one sample of the big chat (index-only counts): {ins:.2f} s")
    record_property("finalize_seconds", fin)
    record_property("big_sample_seconds", ins)
    assert n == 200 and fin < 5.0 and ins < 5.0


def _timed_heads(p: prober.Prober, c: psycopg.Connection, now: float, runs: int = 5) -> tuple[list, float]:
    """The queue read (the worst of `runs`, after one warm-up) and its candidates."""
    heads = p.heads(c, now)
    took = 0.0
    for _ in range(runs):
        t = time.perf_counter()
        heads = p.heads(c, now)
        took = max(took, time.perf_counter() - t)
    return heads, took


def test_prober_queue_read_with_100k_pending(scale_dsn, record_property):
    """The read before every step: the first PER_CHAT ready probes of each of the 200 chats in queue order, beside
    100 995 pending (100 000 outage probes of the big chat); target under 50 ms."""
    p = prober.Prober(_ctx(), rate=5)
    with psycopg.connect(scale_dsn) as c:
        c.execute("INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (%s, 999, 'manual')", (BIG - 1,))
        heads, took = _timed_heads(p, c, NOW.timestamp())
        plan = "\n".join(r[0] for r in c.execute("EXPLAIN (ANALYZE, BUFFERS) " + prober._HEADS,  # noqa: SLF001
                                                   {"now": NOW, "per_chat": prober.PER_CHAT, "many": prober.MANY},
                                                   prepare=False))
        c.rollback()
    print(f"prober queue read, 100 996 pending in 200 chats: {took * 1000:.1f} ms, {len(heads)} candidates (every "
          f"step)\n{plan}")
    record_property("prober_read_seconds", took)
    assert [h.reason for h in heads[:2]] == ["manual", "conflict"]
    assert len(heads) == 1 + 199 * 5 + prober.PER_CHAT
    big = [h.user_id for h in heads if h.chat_id == BIG]
    assert big[0] % 86400 == 86391 and len(big) == prober.PER_CHAT  # the newest spells with a probe
    assert took < 0.05


def test_prober_read_ranks_a_g1_batch_by_spell_with_100k_pending(scale_dsn, record_property):
    """A G1 batch of OUTAGE_BUDGET outage probes in one tg.queue_probes call beside the 100 000 pending ones; user
    86399 has the newest spell of the chat but neither the first nor the last id of the batch. The next read has it
    first, in under 50 ms."""
    from tgaud.jobs import reconciler
    p = prober.Prober(_ctx(), rate=5)
    users = list(range(9, 10 * reconciler.OUTAGE_BUDGET, 10))
    assert 86399 in users and users.index(86399) not in (0, len(users) - 1)
    with psycopg.connect(scale_dsn) as c:
        t = time.perf_counter()
        c.execute("SELECT tg.queue_probes(%s::bigint[], %s::bigint[], 'outage')", ([BIG] * len(users), users))
        queued = time.perf_counter() - t
        heads, took = _timed_heads(p, c, NOW.timestamp())
        c.rollback()
    outage = [x.user_id for x in heads if x.chat_id == BIG and x.reason == "outage"]
    print(f"prober read after a G1 batch of {len(users)} outage probes beside 100 000 pending: {took * 1000:.1f} ms "
          f"(the batch queued in {queued * 1000:.0f} ms)")
    record_property("prober_read_batch_seconds", took)
    assert outage[0] == 86399 and len(outage) == prober.PER_CHAT
    assert took < 0.05


def test_prober_sees_raised_probes_and_a_rebuilt_spell_with_100k_pending(scale_dsn, record_property):
    """crossfix review 2 at scale: (a) four new probes of the big chat; b and c are raised to manual and a and d (the
    two newest spells) to outage: the next read has them in that order; (b) late events rebuild user 1's spell (the
    oldest) into the newest of all: the next read has its probe first among the outage probes."""
    p = prober.Prober(_ctx(), rate=5)
    with psycopg.connect(scale_dsn) as c:
        ids = [c.execute("INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (%s, %s, %s) RETURNING probe_id",
                         (BIG, u, r)).fetchone()[0]
               for u, r in ((86399, "sample"), (2, "outage"), (3, "outage"), (172799, "sample"))]
        p.heads(c, NOW.timestamp())
        c.execute("UPDATE tg.probe SET reason = 'manual' WHERE probe_id = ANY(%s)", ([ids[1], ids[2]],))
        c.execute("UPDATE tg.probe SET reason = 'outage' WHERE probe_id = ANY(%s)", ([ids[0], ids[3]],))
        heads, raised = _timed_heads(p, c, NOW.timestamp() + 1)
        first = [(x.user_id, x.reason) for x in heads if x.chat_id == BIG][:4]
        c.execute("SELECT tg.ensure_partitions('member_event', 'month', '2026-08-01', '2026-09-01')")
        c.execute("INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status) VALUES "
                  "(%s, 1, '2026-08-02 00:00+00', 950001, 'member', 'left'), "
                  "(%s, 1, '2026-08-02 00:00:01+00', 950002, 'left', 'member')", (BIG, BIG))
        history.rebuild_pairs(c, [(BIG, 1)])
        heads, respelled = _timed_heads(p, c, NOW.timestamp() + 2)
        outage = [x.user_id for x in heads if x.chat_id == BIG and x.reason == "outage"]
        c.rollback()
    print(f"prober read, probes raised in place beside 100 000 pending: {raised * 1000:.1f} ms; a rebuilt spell: "
          f"{respelled * 1000:.1f} ms")
    record_property("prober_read_raised_seconds", raised)
    record_property("prober_read_respelled_seconds", respelled)
    assert first == [(2, "manual"), (3, "manual"), (86399, "outage"), (172799, "outage")]
    assert outage[0] == 1 and raised < 0.05 and respelled < 0.05


def _deadlines(dsn: str, sql: str, params: tuple) -> None:
    """Set retry deadlines on the fixture's pending probes and vacuum (what autovacuum does as failures come in)."""
    with psycopg.connect(dsn) as c:
        c.execute(sql, params)
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("VACUUM ANALYZE tg.probe")


@pytest.mark.parametrize("state", ["ahead", "passed", "mixed"])
def test_prober_read_with_retry_deadlines(scale_dsn, record_property, state):
    """prober-simplify reviews 1 and 2: the whole selection (heads()) with the 100 996 pending probes in backoff.
    ahead: every deadline but two is still ahead (the probes are skipped by index); passed: every deadline has
    passed (a restart after a long stop: all are ready, at their place, nothing is written); mixed: in the big chat
    the newest half of the spells is still in backoff and the older half has passed (the read passes over the waiting
    half in queue order). One manual retry of another chat has passed too: it is first. Under 50 ms in each."""
    p = prober.Prober(_ctx(), rate=5)
    ahead, passed = NOW + timedelta(minutes=10), NOW - timedelta(minutes=10)
    with psycopg.connect(scale_dsn) as c:
        manual = c.execute("INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (%s, 999, 'manual') "
                           "RETURNING probe_id", (BIG - 1,)).fetchone()[0]
    try:
        _deadlines(scale_dsn, """
            UPDATE tg.probe SET attempts = 2, last_attempt_at = %(at)s - interval '10 minutes',
                   next_attempt_at = CASE
                     WHEN probe_id = %(manual)s THEN %(passed)s
                     WHEN %(state)s = 'ahead' THEN CASE WHEN chat_id = %(big)s AND user_id = 1 THEN %(passed)s
                                                       ELSE %(ahead)s END
                     WHEN %(state)s = 'passed' THEN %(passed)s
                     ELSE CASE WHEN chat_id = %(big)s AND user_id %% 86400 >= 43200 THEN %(ahead)s ELSE %(passed)s END
                   END
            WHERE outcome = 'pending'""", {"at": NOW, "manual": manual, "passed": passed, "ahead": ahead,
                                           "state": state, "big": BIG})
        with psycopg.connect(scale_dsn) as c:
            heads, took = _timed_heads(p, c, NOW.timestamp())
            plan = "\n".join(r[0] for r in c.execute("EXPLAIN (ANALYZE, BUFFERS) " + prober._HEADS,  # noqa: SLF001
                                                       {"now": NOW, "per_chat": prober.PER_CHAT, "many": prober.MANY},
                                                       prepare=False))
            pending_ahead = c.execute("SELECT count(*) FROM tg.probe WHERE outcome = 'pending' "
                                      "AND next_attempt_at > %s", (NOW,)).fetchone()[0]
    finally:
        with psycopg.connect(scale_dsn) as c:  # the fixture as it was, for the tests after this one
            c.execute("DELETE FROM tg.probe WHERE probe_id = %s", (manual,))
        _deadlines(scale_dsn, "UPDATE tg.probe SET attempts = 0, last_attempt_at = NULL, next_attempt_at = NULL "
                              "WHERE outcome = 'pending'", ())
    print(f"prober read, deadlines {state} ({pending_ahead} still ahead): {took * 1000:.1f} ms, {len(heads)} heads\n"
          f"{plan}")
    record_property(f"prober_read_deadlines_{state}_seconds", took)
    assert (heads[0].chat_id, heads[0].reason) == (BIG - 1, "manual")
    big = [h.user_id for h in heads if h.chat_id == BIG]
    if state == "ahead":
        assert len(heads) == 2 and big == [1]
    else:
        assert heads[1].reason == "conflict" and len(heads) == 1 + 199 * 5 + prober.PER_CHAT
        assert big[0] % 86400 == (86391 if state == "passed" else 43191)  # the newest spells that are ready
    assert took < 0.05
