"""Scale of the metric cache and the prober's queue read on the test server (production limits: 512 MB,
work_mem 2 MB, no parallel workers): 200 tracked chats, one of them with 1 000 000 open spells (1 % suspect),
30 days of drift samples for every chat (864 000 rows, the verify.md 5 size), 100 000 pending outage probes in the
big chat and a few in the others.

Target (brief): one metric cache refresh under 5 s. The measured times are printed (pytest -s) and written to
the test's report section. TGAUD_SCALE_SPELLS overrides the 1 000 000.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone

import psycopg
import pytest

from tgaud import config, metrics
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


def test_prober_queue_read_with_100k_pending(scale_dsn, record_property):
    p = prober.Prober(_ctx(), rate=5)
    with psycopg.connect(scale_dsn) as c:
        t = time.perf_counter()
        p._read(c, NOW.timestamp())  # noqa: SLF001
        took = time.perf_counter() - t
        full = len(p.queue)
        c.execute("INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (%s, 999, 'manual')", (BIG - 1,))
        t = time.perf_counter()
        p._read_heads(c, NOW.timestamp() + 1)  # noqa: SLF001
        heads = time.perf_counter() - t
        c.rollback()
    print(f"prober full queue read, 100 995 pending: {took:.2f} s, {full} candidates "
          f"(every {prober.FULL_REFRESH:.0f} s); head read: {heads * 1000:.0f} ms (every step)")
    record_property("prober_read_seconds", took)
    record_property("prober_head_read_seconds", heads)
    assert p.queue[0].reason == "manual" and heads < 0.2
    p.queue.pop(0)
    assert p.queue[0].reason == "conflict"
    assert full == 199 * 5 + prober.PER_CHAT
    assert took < 5.0


def test_prober_head_read_ranks_a_g1_batch_by_spell_with_100k_pending(scale_dsn, record_property):
    """A G1 batch of OUTAGE_BUDGET outage probes in one tg.queue_probes call after the full read, beside the 100 000
    pending ones; user 86399 has the newest spell of the chat but neither the first nor the last id of the batch."""
    from tgaud.jobs import reconciler
    p = prober.Prober(_ctx(), rate=5)
    users = list(range(9, 10 * reconciler.OUTAGE_BUDGET, 10))
    assert 86399 in users and users.index(86399) not in (0, len(users) - 1)
    with psycopg.connect(scale_dsn) as c:
        p._read(c, NOW.timestamp())  # noqa: SLF001
        c.execute("SELECT tg.queue_probes(%s::bigint[], %s::bigint[], 'outage')", ([BIG] * len(users), users))
        t = time.perf_counter()
        p._read_heads(c, NOW.timestamp() + 1)  # noqa: SLF001
        heads = time.perf_counter() - t
        c.rollback()
    print(f"prober head read with a G1 batch of {len(users)} outage probes beside 100 000 pending: "
          f"{heads * 1000:.0f} ms")
    record_property("prober_head_read_batch_seconds", heads)
    outage = [c.user_id for c in p.queue if c.chat_id == BIG and c.reason == "outage"]
    assert outage[0] == 86399 and heads < 0.5
