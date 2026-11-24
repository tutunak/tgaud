"""Scale of the reconciler on the test server (production limits: 512 MB, work_mem 2 MB, no parallel workers).

A good snapshot of N members against N open spells (generate_series), the weekly case: every listed user only
confirms an open spell seeded by the previous snapshot (S2, run extended). The job's units run as the scheduler runs
them, one transaction each. Reported: the total time, the grade step (the absent-candidate count) and the longest
step.

The suite runs N = 20 000; TGAUD_RECONCILE_SCALE=1000000 runs the brief's size and asserts its targets (under 30
minutes in all, no step over 10 s).
"""

from __future__ import annotations

import os
import random
import time

import psycopg

from reconcile_lab import job
from tgaud import history

CHAT = -1008800000001
WEEK0, WEEK1 = "2026-09-01 00:00+00", "2026-09-08 00:00+00"


def _generate(c: psycopg.Connection, n: int) -> None:
    c.execute("INSERT INTO tg.chat (chat_id, chat_type, title, tracked) VALUES (%s, 'channel', 'scale', true)",
              (CHAT,))
    c.execute("INSERT INTO tg.bot_state (bot_id, allowed_updates, caught_up_at) "
              "VALUES (5000, '{chat_member}', '2100-01-01+00')")
    c.execute("INSERT INTO tg.tg_user (user_id, first_seen_at, last_seen_at) "
              "SELECT u, %s, %s FROM generate_series(1, %s) u", (WEEK0, WEEK0, n))
    # last week's good snapshot, applied and pruned: one run per user and the spell it seeded
    c.execute("""INSERT INTO tg.snapshot (snapshot_id, chat_id, started_at, finished_at, method, reported_at_start,
                                          reported_at_end, fetched, complete, quality, reconciled_at, members_pruned_at)
                 VALUES (1, %(c)s, %(t)s, %(t)s::timestamptz + interval '3 hours', 'listing', %(n)s, %(n)s, %(n)s,
                         true, 'good', %(t)s::timestamptz + interval '4 hours', %(t)s::timestamptz + interval '1 day')""",
              {"c": CHAT, "t": WEEK0, "n": n})
    seen0 = "%(t)s::timestamptz + u * interval '10 milliseconds'"
    c.execute(f"""INSERT INTO tg.sighting (chat_id, user_id, first_seen_at, first_snapshot_id, last_seen_at,
                                           last_snapshot_id, times_seen)
                  SELECT %(c)s, u, {seen0}, 1, {seen0}, 1, 1 FROM generate_series(1, %(n)s) u""",
              {"c": CHAT, "t": WEEK0, "n": n})
    c.execute(f"""INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at)
                  SELECT %(c)s, u, 'snapshot', {seen0}, {seen0} FROM generate_series(1, %(n)s) u""",
              {"c": CHAT, "t": WEEK0, "n": n})
    # this week's listing, as the collect script writes it
    c.execute("""INSERT INTO tg.snapshot (snapshot_id, chat_id, started_at, method, reported_at_start)
                 VALUES (2, %s, %s, 'listing', %s)""", (CHAT, WEEK1, n))
    c.execute("""INSERT INTO tg.snapshot_member (snapshot_id, user_id, seen_at)
                 SELECT 2, u, %s::timestamptz + u * interval '10 milliseconds' FROM generate_series(1, %s) u""",
              (WEEK1, n))
    c.execute("""UPDATE tg.snapshot SET finished_at = %s::timestamptz + interval '3 hours', reported_at_end = %s,
                                        fetched = %s, complete = true WHERE snapshot_id = 2""", (WEEK1, n, n))
    c.execute("ANALYZE")


def test_reconcile_a_weekly_snapshot_at_scale(pg, capsys):
    n = int(os.environ.get("TGAUD_RECONCILE_SCALE", "20000"))
    dsn = pg.make_db(migrated=True)
    with psycopg.connect(dsn) as c:
        t = time.perf_counter()
        _generate(c, n)
        c.commit()
        gen = time.perf_counter() - t
        j = job()
        steps: list[tuple[str, float]] = []
        start = time.perf_counter()
        while True:
            t = time.perf_counter()
            unit = j.run_unit(c)
            c.commit()
            if unit is None:
                break
            steps.append((unit, time.perf_counter() - t))
        total = time.perf_counter() - start
        longest = max(s for _, s in steps)
        grade = sum(s for u, s in steps if u == "grade")
        applies = [s for u, s in steps if u == "apply"]
        run = c.execute("""SELECT count(*), sum(listed), sum(confirmed), sum(seeded), sum(absent_candidates)
                           FROM tg.reconcile_run WHERE snapshot_id = 2""").fetchone()
        snap = c.execute("SELECT quality, reconciled_at IS NOT NULL FROM tg.snapshot WHERE snapshot_id = 2").fetchone()
        extended = c.execute("SELECT count(*) FROM tg.sighting WHERE chat_id = %s AND times_seen = 2 "
                             "AND last_snapshot_id = 2", (CHAT,)).fetchone()[0]
        confirmed = c.execute("""SELECT count(*) FROM tg.membership WHERE chat_id = %s AND end_source IS NULL
                                   AND last_confirmed_at >= %s""", (CHAT, WEEK1)).fetchone()[0]
        sample = random.Random(7).sample(range(1, n + 1), min(n, 2000))
        same = history.rebuild_pairs(c, [(CHAT, u) for u in sample])
        c.rollback()
    with capsys.disabled():
        print(f"\nreconcile scale: {n} members against {n} open spells, generated in {gen:.0f} s; "
              f"reconciled in {total:.1f} s ({n / total:.0f} users/s) over {len(steps)} steps: grade {grade:.2f} s, "
              f"{len(applies)} chunks (longest {max(applies):.2f} s, mean {sum(applies) / len(applies):.2f} s); "
              f"longest step {longest:.2f} s")
    assert snap == ("good", True)
    assert run[1:] == (n, n, 0, 0)
    assert extended == n and confirmed == n
    assert same == history.RebuildResult()  # the sampled pairs equal a full rebuild
    if "TGAUD_RECONCILE_SCALE" in os.environ:
        assert total < 30 * 60 and longest < 10
