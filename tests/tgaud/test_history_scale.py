"""Scale of rebuild_pairs on the test server (production limits: 512 MB, work_mem 2 MB, no parallel workers).

Pairs get 1-6 pieces of evidence each (pair i has 1 + i % 6): a join, a listing in a good snapshot, a second
listing (the run extended), a probe answer "out", a rejoin, a leave; the snapshots the user is missing from are
absences, and one global outage gap touches everyone. The first pass builds every spell (inserts), the second finds
nothing to change (the reconciler's common case). Each chunk of history.CHUNK pairs is one transaction, as the
callers do.

The suite runs 20 000 pairs; TGAUD_SCALE_PAIRS=200000 runs the brief's size and asserts the target
(2 000 pairs per second for the first pass).
"""

from __future__ import annotations

import os
import time

import psycopg

from tgaud import history

CHATS = 4


def _generate(c: psycopg.Connection, n: int) -> None:
    per_chat = n // CHATS
    c.execute("SELECT tg.ensure_partitions('member_event', 'month', '2026-08-01', '2026-12-01')")
    c.execute("""INSERT INTO tg.chat (chat_id, chat_type, title, tracked)
                 SELECT -1008000000000 - k, 'channel', 'scale ' || k, true FROM generate_series(1, %s) k""", (CHATS,))
    c.execute("""INSERT INTO tg.tg_user (user_id, first_seen_at, last_seen_at)
                 SELECT u, '2026-08-01', '2026-12-01' FROM generate_series(1, %s) u""", (per_chat,))
    c.execute("""CREATE TEMP TABLE scale_pair AS
                 SELECT -1008000000000 - k AS chat_id, u AS user_id, ((k - 1) * %(p)s + u - 1) AS i,
                        1 + ((k - 1) * %(p)s + u - 1) %% 6 AS pieces
                 FROM generate_series(1, %(c)s) k, generate_series(1, %(p)s) u""", {"p": per_chat, "c": CHATS})
    # three weekly good snapshots per chat; the members are not stored (pruned), only the runs and the grade
    c.execute("""INSERT INTO tg.snapshot (snapshot_id, chat_id, started_at, finished_at, method, reported_at_start,
                                          reported_at_end, fetched, complete, quality, reconciled_at)
                 SELECT 100 * k + w, -1008000000000 - k, timestamptz '2026-09-10' + (w - 1) * interval '7 days',
                        timestamptz '2026-09-10 02:00' + (w - 1) * interval '7 days', 'listing', %s, %s, %s, true,
                        'good', timestamptz '2026-09-10 03:00' + (w - 1) * interval '7 days'
                 FROM generate_series(1, %s) k, generate_series(1, 3) w""", (per_chat, per_chat, per_chat, CHATS))
    at = "timestamptz '2026-09-01' + (p.i % 7) * interval '1 day' + (p.i % 86400) * interval '1 second'"
    c.execute(f"""INSERT INTO tg.member_event (event_id, chat_id, user_id, event_at, update_id, old_status, new_status)
                  SELECT 3 * p.i + 1, p.chat_id, p.user_id, {at}, 3 * p.i + 1, 'left', 'member' FROM scale_pair p""")
    c.execute("""INSERT INTO tg.sighting (chat_id, user_id, first_seen_at, first_snapshot_id, last_seen_at,
                                          last_snapshot_id, times_seen)
                 SELECT p.chat_id, p.user_id, timestamptz '2026-09-10 00:30' + (p.i % 3600) * interval '1 second',
                        100 * (-1008000000000 - p.chat_id) + 1,
                        CASE WHEN p.pieces >= 3 THEN timestamptz '2026-09-17 00:30'
                             ELSE timestamptz '2026-09-10 00:30' END
                          + (p.i % 3600) * interval '1 second',
                        100 * (-1008000000000 - p.chat_id) + CASE WHEN p.pieces >= 3 THEN 2 ELSE 1 END,
                        CASE WHEN p.pieces >= 3 THEN 2 ELSE 1 END
                 FROM scale_pair p WHERE p.pieces >= 2""")
    c.execute("""INSERT INTO tg.member_event (event_id, chat_id, user_id, event_at, update_id, old_status, new_status)
                 SELECT 3 * p.i + 2, p.chat_id, p.user_id,
                        timestamptz '2026-09-30' + (p.i % 3600) * interval '1 second',
                        3 * p.i + 2, 'member', 'left' FROM scale_pair p WHERE p.pieces >= 6""")
    c.execute("""INSERT INTO tg.probe (chat_id, user_id, reason, outcome, answered_at, status_seen, sent_at)
                 SELECT p.chat_id, p.user_id, 'sample', 'out', timestamptz '2026-09-26 12:00', 'left',
                        timestamptz '2026-09-26 11:59:59' FROM scale_pair p WHERE p.pieces >= 4""")
    c.execute("""INSERT INTO tg.member_event (event_id, chat_id, user_id, event_at, update_id, old_status, new_status)
                 SELECT 3 * p.i + 3, p.chat_id, p.user_id,
                        timestamptz '2026-09-27' + (p.i % 3600) * interval '1 second',
                        3 * p.i + 3, 'left', 'member' FROM scale_pair p WHERE p.pieces >= 5""")
    c.execute("""INSERT INTO tg.gap (chat_id, period, reason)
                 VALUES (NULL, tstzrange('2026-09-12', '2026-09-12 10:00'), 'service_down')""")
    c.execute("ANALYZE")


def _pass(dsn: str, pairs: list[tuple[int, int]]) -> tuple[float, history.RebuildResult, float]:
    total = history.RebuildResult()
    longest = 0.0
    start = time.perf_counter()
    with psycopg.connect(dsn) as c:
        for i in range(0, len(pairs), history.CHUNK):
            t = time.perf_counter()
            total += history.rebuild_pairs(c, pairs[i:i + history.CHUNK])
            c.commit()
            longest = max(longest, time.perf_counter() - t)
    return time.perf_counter() - start, total, longest


def test_rebuild_rate(pg, capsys):
    n = int(os.environ.get("TGAUD_SCALE_PAIRS", "20000"))
    dsn = pg.make_db(migrated=True)
    with psycopg.connect(dsn) as c:
        t = time.perf_counter()
        _generate(c, n)
        pairs = c.execute("SELECT chat_id, user_id FROM scale_pair ORDER BY chat_id, user_id").fetchall()
        c.commit()
        gen = time.perf_counter() - t
    first, built, longest1 = _pass(dsn, pairs)
    second, again, longest2 = _pass(dsn, pairs)
    with psycopg.connect(dsn) as c:
        spells = c.execute("SELECT count(*) FROM tg.membership").fetchone()[0]
        kinds = c.execute("SELECT count(*) FILTER (WHERE suspect_since IS NOT NULL), count(*) FILTER "
                          "(WHERE end_source = 'probe') FROM tg.membership").fetchone()
    with capsys.disabled():
        print(f"\nrebuild scale: {len(pairs)} pairs, generated in {gen:.1f} s; first pass {first:.1f} s = "
              f"{len(pairs) / first:.0f} pairs/s (longest chunk {longest1:.2f} s, {built}); second pass "
              f"{second:.1f} s = {len(pairs) / second:.0f} pairs/s (longest chunk {longest2:.2f} s); "
              f"{spells} spells, {kinds[0]} suspect, {kinds[1]} closed by probe")
    assert built.opened == spells and again == history.RebuildResult()
    assert kinds[0] > 0 and kinds[1] > 0
    if "TGAUD_SCALE_PAIRS" in os.environ:
        assert len(pairs) / first >= 2000
