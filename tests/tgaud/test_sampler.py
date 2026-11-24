"""The sampler (design.md 6, 9; D7): staggered drift samples, pending samples finalized once ingest caught up, the
residual over finalized samples (stats.v_drift, block 9b, the gauges), the metric cache with its series removal
and finite labels, scrapes that never query, join-request aging and the lost-time counter."""

from __future__ import annotations

import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from history_world import CHAT, at
from jobs_world import Env, make_env, no_closed_log_streams  # noqa: F401 (autouse fixture)
from tgaud import metrics, sql
from tgaud.jobs import sampler
from tgaud.jobs.sampler import CHAT_GAUGES, INTERVAL

START = datetime(2026, 9, 20, 12, 0, 0, tzinfo=timezone.utc)  # a round time: samples must not all come at :00
B = -1009000000002
C = -1009000000003


@pytest.fixture(autouse=True)
def clean_registry():
    for g in (*CHAT_GAUGES.values(), metrics.CHAT_INFO):
        g.clear()
    yield


@pytest.fixture
def env(migrated_dsn):
    e = make_env(migrated_dsn, START)
    yield e
    e.conn.close()
    e.w.c.close()


def value(name: str, chat: int | None = None, **labels) -> float | None:
    if chat is not None:
        labels["chat"] = str(chat)
    return metrics.REGISTRY.get_sample_value(name, labels)


def caught_up(e: Env, t: datetime) -> None:
    with psycopg.connect(e.dsn) as c:
        c.execute("UPDATE tg.bot_state SET caught_up_at = %s", (t,))


def counts(e: Env, values: dict[int, int]) -> None:
    e.fake.respond("getChatMemberCount", lambda params: values[params["chat_id"]])


def samples(e: Env, chat: int = CHAT) -> list[tuple]:
    with psycopg.connect(e.dsn) as c:
        return c.execute("""SELECT sampled_at, reported, known_open, suspect_open, net_events FROM tg.count_sample
                            WHERE chat_id = %s ORDER BY sampled_at""", (chat,)).fetchall()


def sample_at(e: Env, job: sampler.Sampler, t: datetime) -> None:
    """Run the scheduler at `t` until the chat's due sample is taken (a metric refresh may come first)."""
    e.clock.set(t)
    for _ in range(3):
        e.tick(job)
        if job.schedule[CHAT] > t.timestamp():
            return
    raise AssertionError("no sample taken")


def refresh(e: Env, job: sampler.Sampler) -> None:
    """One metric cache refresh now (what the step does every minute)."""
    snap = job.collect(e.conn, e.clock())
    e.conn.commit()
    job.publish(snap)


# --- staggering ------------------------------------------------------------------------------------------------------

def test_samples_are_staggered_over_the_interval_and_every_10_minutes(env):
    e = env
    chats = [-1007000000000 - k for k in range(200)]
    for c in chats:
        e.w.add_chat(c)
    e.w.c.execute("UPDATE tg.chat SET tracked = false WHERE chat_id = %s", (CHAT,))
    e.w.c.commit()
    times: dict[int, list[float]] = {c: [] for c in chats}

    def compute(params):
        times[params["chat_id"]].append(e.clock() - START.timestamp())
        return 100
    e.fake.respond("getChatMemberCount", compute)
    job = sampler.make(e.ctx)
    e.run(job, 2 * INTERVAL + 5, dt=1.0)
    for c in chats:
        assert len(times[c]) in (2, 3), (c, times[c])
        gaps = {round(b - a) for a, b in zip(times[c], times[c][1:])}
        assert gaps == {600}, (c, times[c])
    first = [ts[0] for ts in times.values()]
    assert all(0 < t <= INTERVAL + 1 for t in first)  # nothing at start: each chat waits for its slot
    per_10s = Counter(int(t // 10) for t in first)
    assert len(per_10s) >= 45 and max(per_10s.values()) <= 12, per_10s
    assert len(samples(e, chats[0])) == len(times[chats[0]])


def test_restart_resumes_at_the_slot_after_the_last_sample_and_never_bursts(env):
    e = env
    e.w.add_chat(B)
    for chat, last in ((CHAT, START - timedelta(minutes=3)), (B, START - timedelta(hours=1))):
        e.w.c.execute("""INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, net_events)
                         VALUES (%s, %s, 1, 0, 0)""", (chat, last))
    e.w.c.commit()
    job = sampler.make(e.ctx)
    e.tick(job)  # the first step loads the schedule (and the metric cache)
    last = (START - timedelta(minutes=3)).timestamp()
    assert job.schedule[CHAT] == sampler.next_slot(CHAT, last)
    assert last < job.schedule[CHAT] <= last + INTERVAL
    assert START.timestamp() < job.schedule[B] <= START.timestamp() + INTERVAL


def test_next_slot_and_phase():
    p = sampler.phase(CHAT)
    assert 0 <= p < INTERVAL and sampler.phase(CHAT) == p
    assert sampler.next_slot(CHAT, p) == p + INTERVAL
    assert sampler.next_slot(CHAT, p - 0.001) == p
    assert len({sampler.phase(-1007000000000 - k) for k in range(200)}) > 190


# --- samples, finalization and the residual --------------------------------------------------------------------------

def test_net_events_across_two_samples_and_a_late_join_gives_residual_zero(env):
    e = env
    w = e.w
    for u in (1, 2, 3):
        w.join(u, START - timedelta(days=1))
    w.rebuild()
    w.c.commit()
    job = sampler.make(e.ctx)
    reported = {CHAT: 10}
    counts(e, reported)
    caught_up(e, START - timedelta(hours=1))
    e.tick(job)  # metrics; schedule loaded
    first = job.schedule[CHAT]
    sample_at(e, job, datetime.fromtimestamp(first, timezone.utc))
    t1 = datetime.fromtimestamp(first, timezone.utc)
    (s1,) = samples(e)
    assert s1 == (t1, 10, 3, 0, None)  # pending until ingest caught up past it
    caught_up(e, t1 + timedelta(seconds=5))
    e.clock.advance(1)
    sampler.finalize(e.conn)
    e.conn.commit()
    assert samples(e)[0][4] == 0  # a chat's first sample

    # Telegram's count rises (one join, one leave, one join); the sample is taken before ingest commits them
    reported[CHAT] = 11
    sample_at(e, job, t1 + timedelta(seconds=INTERVAL))
    t2 = t1 + timedelta(seconds=INTERVAL)
    assert samples(e)[1][:2] == (t2, 11) and samples(e)[1][4] is None
    e.clock.advance(1)
    sampler.finalize(e.conn)
    e.conn.commit()
    assert samples(e)[1][4] is None  # caught_up_at is still before the sample

    with psycopg.connect(e.dsn) as c:  # a stop before finalization: the BI view and block 9b skip the pending row
        assert [r[1] for r in c.execute("SELECT * FROM stats.v_drift WHERE chat_id = %s", (CHAT,)).fetchall()] == [t1]
        res = sql.run(c, "9b", as_of=t2 + timedelta(minutes=1), scope=[CHAT]).dicts()
        assert [(r["sampled_at"], r["residual"]) for r in res] == [(t1, None)]
    e.clock.advance(60)
    refresh(e, job)
    assert value("tgaud_chat_members_reported", CHAT) == 10  # the latest finalized sample
    assert value("tgaud_chat_drift_residual_24h", CHAT) is None  # no comparison yet: no series, not a zero

    # ingest commits the updates, dated before the sample, then catches up past it
    w.join(4, t1 + timedelta(minutes=2))
    w.leave(1, t1 + timedelta(minutes=3))
    w.join(5, t2.replace(microsecond=0) - timedelta(seconds=1))  # the second before the call: inside (t1, t2]
    w.join(6, t2 + timedelta(seconds=1))  # after it: the next interval
    w.rebuild()
    w.c.commit()
    caught_up(e, t2 + timedelta(seconds=30))
    e.clock.advance(1)
    sampler.finalize(e.conn)
    e.conn.commit()
    assert samples(e)[1][4] == 1  # 2 joins - 1 exit
    with psycopg.connect(e.dsn) as c:
        rows = c.execute("SELECT sampled_at, mismatch, residual FROM stats.v_drift WHERE chat_id = %s "
                         "ORDER BY sampled_at", (CHAT,)).fetchall()
        assert rows == [(t1, 7, None), (t2, 8, 0)]  # residual (11 - 10) - 1; mismatch 11 - 3 known at the sample
        res = sql.run(c, "9b", as_of=t2 + timedelta(minutes=1), scope=[CHAT]).dicts()
        assert [(r["sampled_at"], r["residual"]) for r in res] == [(t1, None), (t2, 0)]
    e.clock.advance(60)
    refresh(e, job)
    assert value("tgaud_chat_members_reported", CHAT) == 11
    assert value("tgaud_chat_drift_residual_24h", CHAT) == 0
    assert value("tgaud_chat_drift_mismatch", CHAT) == 8
    assert value("tgaud_chat_members_known", CHAT) == 5  # live: 3 + 3 joins - 1 leave


def test_mismatch_and_residual_on_a_hand_fixture(env):
    e = env
    w = e.w
    w.add_chat(B)
    w.add_chat(C)
    now = START
    rows = [  # chat, hours before now, reported, known_open, net_events (None = pending)
        (CHAT, 30, 100, 90, 0),
        (CHAT, 25, 103, 92, 2),   # residual 1, before the window: the base
        (CHAT, 20, 104, 95, 2),   # residual -1
        (CHAT, 10, 110, 99, 4),   # residual 2
        (CHAT, 1, 109, 100, -3),  # residual 2
        (CHAT, 0.5, 150, 100, None),  # pending: ignored everywhere
        (B, 5, 50, 40, 0),        # no sample before the window: the first one has no residual
        (B, 4, 55, 41, 3),        # residual 2
        (B, 3, 54, 41, -2),       # residual 1
    ]
    for chat, h, rep, known, net in rows:
        w.c.execute("""INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, net_events)
                       VALUES (%s, %s, %s, %s, %s)""", (chat, now - timedelta(hours=h), rep, known, net))
    for u in (1, 2, 3):
        w.join(u, at(0))
    w.gap(at(60 * 24 * 10), at(60 * 24 * 11))  # all three suspect
    w.join(4, at(60 * 24 * 12))
    w.rebuild()
    w.c.commit()
    job = sampler.make(e.ctx)
    refresh(e, job)
    assert value("tgaud_chat_members_reported", CHAT) == 109
    assert value("tgaud_chat_drift_mismatch", CHAT) == 9
    assert value("tgaud_chat_drift_residual_24h", CHAT) == 3  # -1 + 2 + 2
    assert value("tgaud_chat_members_known", CHAT) == 4
    assert value("tgaud_chat_members_suspect", CHAT) == 3
    assert value("tgaud_chat_members_reported", B) == 54
    assert value("tgaud_chat_drift_mismatch", B) == 13
    assert value("tgaud_chat_drift_residual_24h", B) == 3  # 2 + 1
    for name in ("tgaud_chat_members_reported", "tgaud_chat_drift_mismatch", "tgaud_chat_drift_residual_24h"):
        assert value(name, C) is None  # no sample: no series
    assert value("tgaud_chat_members_known", C) == 0
    # the gauge agrees with the BI view's residuals over the same 24 h
    with psycopg.connect(e.dsn) as c:
        for chat in (CHAT, B):
            total = c.execute("""SELECT coalesce(sum(residual), 0) FROM stats.v_drift WHERE chat_id = %s
                                 AND sampled_at > %s AND sampled_at <= %s""",
                              (chat, now - timedelta(hours=24), now)).fetchone()[0]
            assert total == value("tgaud_chat_drift_residual_24h", chat)


def test_a_sample_of_a_chat_untracked_meanwhile_is_not_stored(env):
    e = env
    job = sampler.make(e.ctx)
    counts(e, {CHAT: 5})
    e.tick(job)
    with psycopg.connect(e.dsn) as c:
        c.execute("UPDATE tg.chat SET tracked = false WHERE chat_id = %s", (CHAT,))
    assert job.sample(e.conn, CHAT) is False
    assert samples(e) == []


def test_a_failed_count_skips_to_the_next_slot(env):
    e = env
    job = sampler.make(e.ctx)
    e.fake.raise_timeout("getChatMemberCount")
    e.tick(job)
    slot = job.schedule[CHAT]
    sample_at(e, job, datetime.fromtimestamp(slot, timezone.utc))
    assert samples(e) == [] and job.schedule[CHAT] == pytest.approx(slot + INTERVAL)


# --- the metric cache ----------------------------------------------------------------------------------------------

def test_untracked_chat_loses_its_series_and_a_rename_replaces_chat_info(env):
    e = env
    w = e.w
    w.add_chat(B)
    for chat in (CHAT, B):
        w.c.execute("INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, net_events) "
                    "VALUES (%s, %s, 7, 3, 0)", (chat, START - timedelta(hours=1)))
        w.c.execute("INSERT INTO tg.snapshot (chat_id, started_at, finished_at, method, fetched, complete, quality) "
                    "VALUES (%s, %s, %s, 'listing', 0, true, 'good')",
                    (chat, START - timedelta(days=3), START - timedelta(days=2)))
    w.user(1)
    w.c.execute("INSERT INTO tg.probe (chat_id, user_id, reason, queued_at) VALUES (%s, 1, 'conflict', %s)",
                (B, START - timedelta(hours=2)))
    w.c.commit()
    job = sampler.make(e.ctx)
    refresh(e, job)
    assert value("tgaud_chat_info", chat=B, type="supergroup", title="history test") == 1
    assert value("tgaud_snapshot_age_seconds", B) == 2 * 86400
    assert value("tgaud_probe_queue", B) == 1 and value("tgaud_probe_oldest_age_seconds", B) == 7200
    assert value("tgaud_probe_queue", CHAT) == 0 and value("tgaud_probe_oldest_age_seconds", CHAT) == 0
    with psycopg.connect(e.dsn) as c:
        c.execute("UPDATE tg.chat SET tracked = false WHERE chat_id = %s", (B,))
        c.execute("UPDATE tg.chat SET title = 'renamed' WHERE chat_id = %s", (CHAT,))
    refresh(e, job)
    text = scrape_text()
    assert f'chat="{B}"' not in text
    assert value("tgaud_chat_info", chat=CHAT, type="supergroup", title="renamed") == 1
    assert value("tgaud_chat_info", chat=CHAT, type="supergroup", title="history test") is None
    assert text.count(f'tgaud_chat_info{{chat="{CHAT}"') == 1
    refresh(e, job)  # a second scrape cycle adds nothing
    assert scrape_text().count(f'tgaud_chat_info{{chat="{CHAT}"') == 1


def scrape_text() -> str:
    from prometheus_client.exposition import generate_latest
    return generate_latest(metrics.REGISTRY).decode()


def test_label_sets_are_the_designs_finite_labels(env):
    e = env
    e.w.add_chat(B)
    e.w.c.commit()
    job = sampler.make(e.ctx)
    refresh(e, job)
    tracked = {str(CHAT), str(B)}
    families = {g._name for g in CHAT_GAUGES.values()}  # noqa: SLF001
    seen = 0
    for family in metrics.REGISTRY.collect():
        if family.name in families:
            for s in family.samples:
                assert set(s.labels) == {"chat"} and s.labels["chat"] in tracked, s
                seen += 1
        elif family.name == "tgaud_chat_info":
            for s in family.samples:
                assert set(s.labels) == {"chat", "type", "title"} and s.labels["chat"] in tracked
        elif family.name == "tgaud_probes":
            for s in family.samples:
                assert set(s.labels) <= {"outcome"} and s.labels.get("outcome", "in") in ("in", "out", "error")
        elif family.name == "tgaud_chat_join_requests":
            for s in family.samples:
                assert s.labels["outcome"] in ("received", "approved", "declined", "unknown")
    assert seen >= 2 * len({"known", "suspect", "gap_open", "requests_pending", "probe_queue", "probe_oldest"})


def test_a_scrape_never_queries_it_serves_the_cached_values(env):
    e = env
    job = sampler.make(e.ctx)
    e.w.join(1, at(0))
    e.w.rebuild()
    e.w.c.commit()
    refresh(e, job)
    server = metrics.MetricsServer("127.0.0.1", 0, metrics.Health()).start()
    try:
        url = f"http://127.0.0.1:{server.port}/metrics"
        line = f'tgaud_chat_members_known{{chat="{CHAT}"}} 1.0'
        assert line in urllib.request.urlopen(url, timeout=5).read().decode()
        with psycopg.connect(e.dsn) as c:  # the database changes, and then goes away for the scrape
            c.execute("DELETE FROM tg.membership")
        e.conn.close()
        assert line in urllib.request.urlopen(url, timeout=5).read().decode()
        e.conn = psycopg.connect(e.dsn)
        refresh(e, job)
        assert f'tgaud_chat_members_known{{chat="{CHAT}"}} 0.0' in urllib.request.urlopen(url, timeout=5).read().decode()
    finally:
        server.close()


def test_gap_open_flags_open_and_global_gaps(env):
    e = env
    e.w.add_chat(B)
    e.w.gap(START - timedelta(days=2), START - timedelta(days=1))  # closed: not open
    e.w.c.execute("INSERT INTO tg.gap (chat_id, period, reason) VALUES (%s, tstzrange(%s, NULL), 'not_admin')",
                  (B, START - timedelta(hours=1)))
    e.w.c.commit()
    job = sampler.make(e.ctx)
    refresh(e, job)
    assert (value("tgaud_chat_gap_open", CHAT), value("tgaud_chat_gap_open", B)) == (0, 1)
    e.w.gap(START - timedelta(hours=1), None, all_chats=True)
    e.w.c.commit()
    refresh(e, job)
    assert (value("tgaud_chat_gap_open", CHAT), value("tgaud_chat_gap_open", B)) == (1, 1)


def test_lost_seconds_counts_confirmed_service_down_gaps_once(env):
    e = env
    job = sampler.make(e.ctx)
    before = value("tgaud_lost_seconds_total") or 0
    e.w.gap(START - timedelta(days=5), START - timedelta(days=5) + timedelta(hours=30), all_chats=True)
    e.w.c.execute("INSERT INTO tg.gap (chat_id, period, reason, confirmed) VALUES (NULL, tstzrange(%s, %s), "
                  "'service_down', false)", (START - timedelta(days=3), START - timedelta(days=2)))  # suspected
    e.w.gap(START - timedelta(hours=3), None, all_chats=True)  # still open: not counted yet
    e.w.c.commit()
    refresh(e, job)
    assert value("tgaud_lost_seconds_total") - before == 30 * 3600
    refresh(e, job)
    assert value("tgaud_lost_seconds_total") - before == 30 * 3600


# --- join requests: aging to unknown -------------------------------------------------------------------------------

def test_join_requests_without_an_outcome_become_unknown_after_7_days(env):
    e = env
    w = e.w
    for u, days in ((1, 8), (2, 6.9), (3, 7)):
        w.user(u)
        w.c.execute("INSERT INTO tg.join_request (chat_id, user_id, requested_at) VALUES (%s, %s, %s)",
                    (CHAT, u, START - timedelta(days=days)))
    w.c.commit()
    job = sampler.make(e.ctx)
    unknown0 = value("tgaud_chat_join_requests_total", CHAT, outcome="unknown") or 0
    e.tick(job)  # no update came: the step alone crosses the threshold
    assert value("tgaud_chat_join_requests_pending", CHAT) == 1
    assert value("tgaud_chat_join_requests_total", CHAT, outcome="unknown") - unknown0 == 2

    def report():
        with psycopg.connect(e.dsn) as c:
            (r,) = sql.run(c, "12", as_of=START, since=START - timedelta(days=30)).dicts()
            return r["requests"], r["approved"], r["unknown"], r["pending"]
    assert report() == (3, 0, 2, 1)
    # a delayed approval (ingest supersedes the unknown outcome) and the next refresh agree with the report
    with psycopg.connect(e.dsn) as c:
        c.execute("""UPDATE tg.join_request SET outcome = 'approved', resolved_at = %s
                     WHERE chat_id = %s AND user_id = 1""", (START, CHAT))
    e.clock.advance(sampler.METRICS_EVERY)
    e.tick(job)
    assert report() == (3, 1, 1, 1)
    assert value("tgaud_chat_join_requests_pending", CHAT) == 1
    assert value("tgaud_chat_join_requests_total", CHAT, outcome="unknown") - unknown0 == 2  # not aged again
    e.clock.advance(sampler.METRICS_EVERY + 86400)
    e.tick(job)
    assert report()[2:] == (2, 0) and value("tgaud_chat_join_requests_pending", CHAT) == 0


# --- a count call that an exact transition falls into (review 1) -----------------------------------------------------

@pytest.mark.parametrize("counted", [True, False], ids=["count-includes-the-join", "count-before-the-join"])
def test_a_join_inside_the_count_call_withholds_the_residuals_on_both_sides(env, counted):
    e = env
    w = e.w
    w.join(1, START - timedelta(days=1))
    w.rebuild()
    w.c.commit()
    job = sampler.make(e.ctx)
    e.tick(job)  # the schedule
    caught_up(e, START - timedelta(days=1))
    t1 = datetime.fromtimestamp(job.schedule[CHAT], timezone.utc)
    reported = {CHAT: 10}
    counts(e, reported)
    sample_at(e, job, t1)

    # the second call: a join happens (and ingest commits it) while getChatMemberCount is in flight
    t2 = t1 + timedelta(seconds=INTERVAL)
    join_at = t2.replace(microsecond=0) + timedelta(seconds=1)  # inside [floor(sent), reply]

    def compute(params):
        w.join(2, join_at)
        w.rebuild()
        w.c.commit()
        e.clock.advance(1.5)  # the reply comes 1.5 s after the send
        return 11 if counted else 10
    e.fake.respond("getChatMemberCount", compute)
    sample_at(e, job, t2)
    # the third count after it: Telegram now has the join either way
    t3 = t2 + timedelta(seconds=INTERVAL)
    counts(e, {CHAT: 11})
    sample_at(e, job, t3)
    caught_up(e, t3 + timedelta(seconds=5))
    sampler.finalize(e.conn)
    e.conn.commit()
    rows = samples(e)
    assert [r[4] for r in rows] == [0, 0, 1]  # the join is dated after t2: it belongs to the third interval
    with psycopg.connect(e.dsn) as c:
        drift = c.execute("SELECT sampled_at, residual, straddled FROM stats.v_drift WHERE chat_id = %s "
                          "ORDER BY sampled_at", (CHAT,)).fetchall()
        assert drift == [(t1, None, False), (t2, None, True), (t3, None, False)]
        res = sql.run(c, "9b", as_of=t3 + timedelta(minutes=1), scope=[CHAT]).dicts()
        assert [r["residual"] for r in res] == [None, None, None]
    e.clock.set(t3 + timedelta(minutes=1))
    refresh(e, job)
    assert value("tgaud_chat_drift_residual_24h", CHAT) is None  # no false +1 (or -1), and no false zero
    assert value("tgaud_chat_members_reported", CHAT) == 11


def test_a_join_just_before_the_count_call_is_not_ambiguous(env):
    e = env
    w = e.w
    job = sampler.make(e.ctx)
    e.tick(job)
    t1 = datetime.fromtimestamp(job.schedule[CHAT], timezone.utc)
    counts(e, {CHAT: 10})
    sample_at(e, job, t1)
    t2 = t1 + timedelta(seconds=INTERVAL)
    w.join(2, t2.replace(microsecond=0) - timedelta(seconds=1))  # the second before the send
    w.rebuild()
    w.c.commit()
    counts(e, {CHAT: 11})
    sample_at(e, job, t2)
    caught_up(e, t2 + timedelta(seconds=5))
    sampler.finalize(e.conn)
    e.conn.commit()
    with psycopg.connect(e.dsn) as c:
        assert c.execute("SELECT residual, straddled FROM stats.v_drift WHERE chat_id = %s AND sampled_at = %s",
                         (CHAT, t2)).fetchone() == (0, False)


# --- finalizing an old sample marks its day for R1 (review 1) -------------------------------------------------------

def test_finalizing_a_sample_of_an_earlier_day_marks_the_chat_dirty_from_that_day(env):
    e = env
    w = e.w
    now = datetime.now(timezone.utc)
    old = now.replace(microsecond=0) - timedelta(days=5)
    day = old.date()
    w.c.execute("INSERT INTO tg.coverage (chat_id, period) VALUES (%s, tstzrange(%s, NULL))",
                (CHAT, now - timedelta(days=8)))
    w.c.execute("""INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, net_events)
                   VALUES (%s, %s, 40, 0, 0)""", (CHAT, old - timedelta(minutes=10)))
    w.c.commit()

    def r1():
        with psycopg.connect(e.dsn) as c:
            sql.run(c, "R1", as_of=datetime.now(timezone.utc))
            c.commit()
            return c.execute("SELECT reported_eod FROM stats.chat_day WHERE chat_id = %s AND day = %s",
                             (CHAT, day)).fetchone()
    assert r1() == (40,)  # the chat's days are built
    # a sample of that day arrives late and waits for ingest (it was stuck for days)
    with psycopg.connect(e.dsn) as c:
        c.execute("""INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, net_events)
                     VALUES (%s, %s, 42, 0, NULL)""", (CHAT, old))
        c.execute("DELETE FROM tg.rollup_dirty")
    assert r1() == (40,)  # a routine cycle looks back 2 days only
    caught_up(e, datetime.now(timezone.utc))
    assert sampler.finalize(e.conn) == 1
    e.conn.rollback()  # interrupted: the sample stays pending and nothing is marked, together
    with psycopg.connect(e.dsn) as c:
        assert c.execute("SELECT count(*) FROM tg.rollup_dirty").fetchone()[0] == 0
        assert c.execute("SELECT count(*) FROM tg.count_sample WHERE net_events IS NULL").fetchone()[0] == 1
    assert sampler.finalize(e.conn) == 1
    e.conn.commit()
    with psycopg.connect(e.dsn) as c:
        assert c.execute("SELECT from_day FROM tg.rollup_dirty WHERE chat_id = %s", (CHAT,)).fetchone() == (day,)
    assert r1() == (42,)  # the next cycle republishes the day with its finalized last sample
    with psycopg.connect(e.dsn) as c:
        assert c.execute("SELECT count(*) FROM tg.rollup_dirty").fetchone()[0] == 0  # consumed
        # a sample of today marks nothing (R1's lookback covers it)
        c.execute("""INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, net_events)
                     VALUES (%s, now() - interval '1 second', 43, 0, NULL)""", (CHAT,))
    caught_up(e, datetime.now(timezone.utc) + timedelta(seconds=1))
    assert sampler.finalize(e.conn) == 1
    e.conn.commit()
    with psycopg.connect(e.dsn) as c:
        assert c.execute("SELECT count(*) FROM tg.rollup_dirty").fetchone()[0] == 0


def test_a_sample_waits_for_ingest_to_pass_the_end_of_its_call(env):
    """The count included a join made 2 s after the send; ingest's watermark first passes only the send."""
    e = env
    w = e.w
    job = sampler.make(e.ctx)
    e.tick(job)
    t1 = datetime.fromtimestamp(job.schedule[CHAT], timezone.utc)
    counts(e, {CHAT: 10})
    sample_at(e, job, t1)
    t2 = t1 + timedelta(seconds=INTERVAL)
    join_at = t2.replace(microsecond=0) + timedelta(seconds=2)

    def compute(params):
        e.clock.advance(2.5)  # the reply, which counts the join, comes 2.5 s after the send
        return 11
    e.fake.respond("getChatMemberCount", compute)
    sample_at(e, job, t2)
    caught_up(e, t2 + timedelta(milliseconds=100))  # past the send, not past the reply
    sampler.finalize(e.conn)
    e.conn.commit()
    assert [r[4] for r in samples(e)] == [0, None]  # still pending
    w.join(2, join_at)  # ingest commits the join, then catches up past the reply
    w.rebuild()
    w.c.commit()
    caught_up(e, t2 + timedelta(seconds=3))
    sampler.finalize(e.conn)
    e.conn.commit()
    with psycopg.connect(e.dsn) as c:
        assert c.execute("SELECT residual, straddled FROM stats.v_drift WHERE chat_id = %s AND sampled_at = %s",
                         (CHAT, t2)).fetchone() == (None, True)
        res = sql.run(c, "9b", as_of=t2 + timedelta(minutes=1), scope=[CHAT]).dicts()
        assert [r["residual"] for r in res] == [None, None]
    e.clock.set(t2 + timedelta(minutes=1))
    refresh(e, job)
    assert value("tgaud_chat_drift_residual_24h", CHAT) is None


def test_the_residual_gauge_is_zero_only_for_a_measured_zero(env):
    e = env
    w = e.w
    w.add_chat(B)
    rows = [(CHAT, 3, 10, False), (CHAT, 2, 11, True),   # CHAT: the only comparison is withheld
            (B, 3, 20, False), (B, 2, 20, False)]        # B: one valid comparison, residual 0
    for chat, h, rep, straddled in rows:
        w.c.execute("""INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, net_events, straddled)
                       VALUES (%s, %s, %s, 0, 0, %s)""", (chat, START - timedelta(hours=h), rep, straddled))
    w.c.commit()
    job = sampler.make(e.ctx)
    refresh(e, job)
    assert value("tgaud_chat_drift_residual_24h", CHAT) is None
    assert value("tgaud_chat_drift_residual_24h", B) == 0
    assert value("tgaud_chat_members_reported", CHAT) == 11  # the count itself is still published
    with psycopg.connect(e.dsn) as c:  # the series goes when a later refresh has no comparison any more
        c.execute("UPDATE tg.count_sample SET straddled = true WHERE chat_id = %s", (B,))
    refresh(e, job)
    assert value("tgaud_chat_drift_residual_24h", B) is None
