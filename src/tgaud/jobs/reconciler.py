"""Job `reconciler` (worker thread): applies the finished snapshots of the collect script, handles gaps (G1) and
keeps the snapshot member rows of the last 2 good snapshots per chat (design.md 4-6).

Every `step(conn)` does one unit of work inside the scheduler's transaction (opened by the generation fence,
committed by the scheduler) and never commits itself. A unit is bounded (a chunk aims at CHUNK_SECONDS), and all
of its writes, with the cursor that says how far it got, commit together, so a stop at any point resumes from the
durable state without counting anything twice. Units, in order of preference:

    expire   a snapshot left unfinished for 2 days: marked (expired_at), its member rows deleted in batches, then
             the snapshot row; once marked it is never graded, even if the script finishes it meanwhile
    G1       one chunk of one (gap, chat): the chat's open spells doubted by the gap are rebuilt (suspect since the
             gap start), the last chunk queues outage probes (newest spells first, within OUTAGE_BUDGET)
    apply    one chunk of the in-progress snapshot of a chat: users, sighting runs, cursor, spells, probes, counts
    grade    the earliest unreconciled snapshot of a chat, once ingest has caught up past its finished_at
    prune    one batch of R3 (snapshot member rows outside the last 2 good snapshots of a chat)

G1 and apply alternate when both have work. Snapshots of a chat are applied strictly in (started_at, snapshot_id)
order: a later one waits for every earlier one to finish or expire. The E4/E10/S3 cases and the contradiction
gaps they raise are recorded by tgaud.history.rebuild_pairs itself (migration 004), whoever rebuilds.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

import psycopg

from tgaud import history
from tgaud.jobs import JobContext

log = logging.getLogger(__name__)

NAME = "reconciler"
IDLE_POLL = 30.0  # seconds between looks for work while there is none
EXPIRE_AFTER = timedelta(days=2)  # design.md 5: an unfinished snapshot is deleted after this
DELETE_BATCH = 20_000  # member rows per expiry or prune step
CHUNK_FIRST, CHUNK_MIN, CHUNK_MAX = 2_000, 200, 20_000  # users per chunk, adapted to CHUNK_SECONDS
CHUNK_SECONDS = 2.0  # the scheduler's step target is under 5 s
OUTAGE_BUDGET = 10_000  # outage probes per (gap, chat): about 1.1 h of the prober's per-chat half at 5/s
BEFORE_ALL = -(2 ** 63)  # a cursor below every user id
UTC = timezone.utc
S_RULES = ("S1", "S2", "S3", "S4")


def make(ctx: JobContext) -> "Reconciler":
    return Reconciler(ctx)


class Reconciler:
    name = NAME

    def __init__(self, ctx: JobContext) -> None:
        self.ctx = ctx
        self.chunk = CHUNK_FIRST
        self._next_look = 0.0
        self._g1_first = False

    def due(self, now: float) -> bool:
        return now >= self._next_look

    def step(self, conn: psycopg.Connection) -> None:
        ran = self.run_unit(conn)
        self._next_look = 0.0 if ran else self.ctx.clock() + IDLE_POLL

    def run_unit(self, conn: psycopg.Connection) -> str | None:
        """Do one unit of work in the caller's transaction; returns its name, or None when there was none."""
        if self._expire(conn):
            return "expire"
        units = [("g1", self._g1), ("apply", self._apply)]
        if not self._g1_first:
            units.reverse()
        self._g1_first = not self._g1_first
        for name, unit in units:
            if unit(conn):
                return name
        if self._grade(conn):
            return "grade"
        if self._prune(conn):
            return "prune"
        return None

    def _adapt(self, n: int, elapsed: float) -> None:
        """The next chunk size from this one's: aim at CHUNK_SECONDS, at most double, within the bounds."""
        if n < self.chunk:
            return  # a short last chunk says nothing about the rate
        want = int(n * CHUNK_SECONDS / max(elapsed, 0.01))
        self.chunk = max(CHUNK_MIN, min(CHUNK_MAX, 2 * n, want))

    # --- expire -------------------------------------------------------------------------------------------------

    def _expire(self, conn: psycopg.Connection) -> bool:
        row = conn.execute("""
            SELECT snapshot_id, chat_id, expired_at IS NOT NULL FROM tg.snapshot
            WHERE expired_at IS NOT NULL OR (finished_at IS NULL AND started_at <= now() - %s::interval)
            ORDER BY expired_at NULLS LAST, snapshot_id LIMIT 1""", (EXPIRE_AFTER,)).fetchone()
        if row is None:
            return False
        snapshot_id, chat_id, marked = row
        if not marked:
            # the claim re-checks the row under its lock: a collector whose finishing UPDATE was in flight when the
            # row was read wins, and its listing is graded as usual (READ COMMITTED re-reads the updated row)
            claimed = conn.execute("""
                UPDATE tg.snapshot SET expired_at = now()
                WHERE snapshot_id = %s AND expired_at IS NULL AND finished_at IS NULL
                  AND started_at <= now() - %s::interval""", (snapshot_id, EXPIRE_AFTER)).rowcount
            if not claimed:
                return True
        deleted = conn.execute("""
            DELETE FROM tg.snapshot_member WHERE snapshot_id = %(s)s AND user_id <= (
              SELECT max(user_id) FROM (SELECT user_id FROM tg.snapshot_member WHERE snapshot_id = %(s)s
                                        ORDER BY user_id LIMIT %(n)s) x)""",
                               {"s": snapshot_id, "n": DELETE_BATCH}).rowcount
        if deleted < DELETE_BATCH:
            conn.execute("DELETE FROM tg.snapshot WHERE snapshot_id = %s", (snapshot_id,))
            log.info("snapshot %s of chat %s was unfinished for %s: deleted", snapshot_id, chat_id, EXPIRE_AFTER)
        return True

    # --- grade --------------------------------------------------------------------------------------------------

    def _grade(self, conn: psycopg.Connection) -> bool:
        row = conn.execute("""
            WITH firsts AS (
              SELECT DISTINCT ON (chat_id) snapshot_id, chat_id, started_at, finished_at, quality, expired_at
              FROM tg.snapshot WHERE reconciled_at IS NULL ORDER BY chat_id, started_at, snapshot_id)
            SELECT f.snapshot_id FROM firsts f
            WHERE f.finished_at IS NOT NULL AND f.quality IS NULL AND f.expired_at IS NULL
              AND f.finished_at < (SELECT max(caught_up_at) FROM tg.bot_state)
              AND NOT EXISTS (SELECT 1 FROM tg.snapshot p WHERE p.chat_id = f.chat_id AND p.quality IS NOT NULL
                                                           AND p.reconciled_at IS NULL)
            ORDER BY f.started_at, f.snapshot_id LIMIT 1""").fetchone()
        if row is None:
            return False
        grade(conn, row[0])
        return True

    # --- apply --------------------------------------------------------------------------------------------------

    def _apply(self, conn: psycopg.Connection) -> bool:
        row = conn.execute("""
            SELECT snapshot_id, chat_id FROM tg.snapshot WHERE quality IS NOT NULL AND reconciled_at IS NULL
            ORDER BY started_at, snapshot_id LIMIT 1""").fetchone()
        if row is None:
            return False
        started = time.monotonic()
        n = apply_chunk(conn, row[0], row[1], self.chunk, store_names=self.ctx.config.store_names)
        self._adapt(n, time.monotonic() - started)
        return True

    # --- G1 -----------------------------------------------------------------------------------------------------

    def _g1(self, conn: psycopg.Connection) -> bool:
        registered = register_gaps(conn)
        row = conn.execute("SELECT gap_id, chat_id FROM tg.gap_progress WHERE NOT done "
                           "ORDER BY gap_id, chat_id LIMIT 1").fetchone()
        if row is None:
            return registered > 0
        started = time.monotonic()
        n = g1_chunk(conn, row[0], row[1], self.chunk)
        self._adapt(n, time.monotonic() - started)
        return True

    # --- prune --------------------------------------------------------------------------------------------------

    def _prune(self, conn: psycopg.Connection) -> bool:
        row = conn.execute("""
            SELECT snapshot_id FROM tg.snapshot WHERE prune_started_at IS NOT NULL AND members_pruned_at IS NULL
            ORDER BY snapshot_id LIMIT 1""").fetchone()
        if row is None:
            # R3's selection (queries.sql): reconciled, not pruned, not among the last 2 good snapshots of its chat
            row = conn.execute("""
                WITH keep AS (
                  SELECT snapshot_id FROM (
                    SELECT snapshot_id, row_number() OVER (PARTITION BY chat_id ORDER BY started_at DESC) AS rn
                    FROM tg.snapshot WHERE quality = 'good') x
                  WHERE rn <= 2)
                SELECT s.snapshot_id FROM tg.snapshot s
                WHERE s.reconciled_at IS NOT NULL AND s.members_pruned_at IS NULL
                  AND s.snapshot_id NOT IN (SELECT snapshot_id FROM keep)
                ORDER BY s.snapshot_id LIMIT 1""").fetchone()
        if row is None:
            return False
        prune_batch(conn, row[0])
        return True


# ---------------------------------------------------------------------------------------------------------------
# Units (each runs in the caller's transaction and commits nothing)
# ---------------------------------------------------------------------------------------------------------------


def _retired(conn: psycopg.Connection, source: int, started_at: datetime) -> bool:
    """Retired source: the snapshot was taken under an old chat id at or after that chat's migration."""
    return conn.execute("SELECT EXISTS (SELECT 1 FROM tg.chat_alias WHERE old_chat_id = %s AND migrated_at <= %s)",
                        (source, started_at)).fetchone()[0]


def grade(conn: psycopg.Connection, snapshot_id: int) -> str | None:
    """Grade one finished snapshot (design.md 5) and store tg.snapshot.quality and .late; returns the grade.

    good = complete and R - fetched <= max(10, 0.1 % of R), R = greatest(reported_at_start, reported_at_end),
    computed in integers as 1000 * (R - fetched) <= max(10000, R); otherwise partial.

    The 2 % rule, the one place it is written (agreed with the conductor, plan of the reconcile task): a good
    snapshot whose absent candidates exceed max(10, 2 % of R) is downgraded to partial. Absent candidates are the
    chat's open spells whose user is not listed and whose last_confirmed_at < started_at (the A1 condition). The
    floor of 10 is the shortfall rule's: without it one leave in a 5-member chat would void every absence there.
    They are counted by one statement (one consistent view) that stops at the first count over the threshold.

    A late snapshot (its source already has a reconciled snapshot with a later key) and one of a retired source
    (taken under an old chat id at or after its migration) give no absences, so they skip the count.
    """
    s = conn.execute("""
        SELECT chat_id, coalesce(source_chat_id, chat_id), started_at, reported_at_start, reported_at_end, fetched,
               complete, quality, finished_at IS NOT NULL
        FROM tg.snapshot WHERE snapshot_id = %s FOR UPDATE""", (snapshot_id,)).fetchone()
    if s is None or s[7] is not None or not s[8]:
        return None
    chat_id, source, started_at, rep_start, rep_end, fetched, complete, _, _ = s
    late = conn.execute("""
        SELECT EXISTS (SELECT 1 FROM tg.snapshot o WHERE coalesce(o.source_chat_id, o.chat_id) = %(e)s
                         AND o.reconciled_at IS NOT NULL AND (o.started_at, o.snapshot_id) > (%(t)s, %(s)s))""",
                        {"e": source, "t": started_at, "s": snapshot_id}).fetchone()[0]
    retired = _retired(conn, source, started_at)
    reported = max((r for r in (rep_start, rep_end) if r is not None), default=None)
    good = bool(complete) and reported is not None and 1000 * (reported - fetched) <= max(10000, reported)
    absent = None
    if good and not late and not retired:
        limit = max(10, reported // 50) + 1
        absent = conn.execute("""
            SELECT count(*) FROM (
              SELECT 1 FROM tg.membership m
              WHERE m.chat_id = %(c)s AND m.end_source IS NULL AND m.last_confirmed_at < %(t)s
                AND NOT EXISTS (SELECT 1 FROM tg.snapshot_member sm WHERE sm.snapshot_id = %(s)s
                                                                      AND sm.user_id = m.user_id)
              LIMIT %(n)s) x""", {"c": chat_id, "t": started_at, "s": snapshot_id, "n": limit}).fetchone()[0]
        if absent > 10 and 50 * absent > reported:
            good = False
    quality = "good" if good else "partial"
    conn.execute("UPDATE tg.snapshot SET quality = %s, late = %s WHERE snapshot_id = %s", (quality, late, snapshot_id))
    log.info("snapshot %s of chat %s graded %s (reported %s, fetched %s, complete %s, absent candidates %s%s%s)",
             snapshot_id, chat_id, quality, reported, fetched, complete, "-" if absent is None else absent,
             ", late" if late else "", ", retired source" if retired else "")
    return quality


# Per listed user, its own runs and spells by the pair indexes (LATERAL): a chunk reads what it folds, never the
# chat's other runs or exits, so its cost does not grow with the chat.
_FOLD = """
WITH r AS (
  SELECT m.user_id, m.seen_at, m.joined_date, x.done, x.run_first, x.run_last, x.run_date
  FROM tg.snapshot_member m
  CROSS JOIN LATERAL (
    SELECT coalesce(bool_or(x.first_snapshot_id = %(s)s OR x.last_snapshot_id = %(s)s), false) AS done,
           max(x.first_snapshot_id) FILTER (WHERE x.last_snapshot_id = %(prev)s) AS run_first,
           max(x.last_seen_at) FILTER (WHERE x.last_snapshot_id = %(prev)s) AS run_last,
           (array_agg(x.joined_date) FILTER (WHERE x.last_snapshot_id = %(prev)s))[1] AS run_date
    FROM tg.sighting x WHERE x.chat_id = %(c)s AND x.user_id = m.user_id) x
  WHERE m.snapshot_id = %(s)s AND m.user_id > %(k)s AND m.user_id <= %(hi)s
), todo AS (
  SELECT * FROM r WHERE NOT r.done   -- a user folded already (a cursor moved back) is left alone
), ext AS (   -- 003's writer contract: the run that ended at the previous snapshot of the source goes on
  SELECT t.user_id, t.seen_at, t.run_first AS first_snapshot_id FROM todo t
  CROSS JOIN LATERAL (
    SELECT coalesce(bool_or(e.end_source IS NOT NULL AND e.left_before > t.run_last
                            AND e.left_after <= t.seen_at), false) AS exited   -- exit evidence between the points
    FROM tg.membership e WHERE e.chat_id = %(c)s AND e.user_id = t.user_id) e
  WHERE %(extend)s AND t.run_first IS NOT NULL AND t.run_date IS NOT DISTINCT FROM t.joined_date
    AND t.seen_at >= t.run_last AND NOT e.exited
), upd AS (
  UPDATE tg.sighting x
     SET interior_seen_at = CASE WHEN x.times_seen >= 2 THEN x.interior_seen_at || x.last_seen_at
                                 ELSE x.interior_seen_at END,
         last_seen_at = ext.seen_at, last_snapshot_id = %(s)s, times_seen = x.times_seen + 1
    FROM ext WHERE x.chat_id = %(c)s AND x.user_id = ext.user_id AND x.first_snapshot_id = ext.first_snapshot_id
  RETURNING 1
), ins AS (
  INSERT INTO tg.sighting (chat_id, user_id, first_seen_at, first_snapshot_id, last_seen_at, last_snapshot_id,
                           times_seen, joined_date)
  SELECT %(c)s, t.user_id, t.seen_at, %(s)s, t.seen_at, %(s)s, 1, t.joined_date FROM todo t
  WHERE t.user_id NOT IN (SELECT user_id FROM ext)
  RETURNING 1
)
SELECT (SELECT count(*) FROM upd), (SELECT count(*) FROM ins)"""

_UPSERT_USERS = """
WITH r AS (SELECT * FROM tg.snapshot_member WHERE snapshot_id = %(s)s AND user_id > %(k)s AND user_id <= %(hi)s)
INSERT INTO tg.tg_user AS u (user_id, is_bot, is_deleted, deleted_seen_at, is_premium, username, first_name,
                             last_name, first_seen_at, last_seen_at)
SELECT r.user_id, r.is_bot, r.is_deleted, r.seen_at, CASE WHEN r.is_premium THEN true END,
       CASE WHEN %(names)s THEN r.username END, CASE WHEN %(names)s THEN r.first_name END,
       CASE WHEN %(names)s THEN r.last_name END, r.seen_at, r.seen_at
FROM r ORDER BY r.user_id
ON CONFLICT (user_id) DO UPDATE SET
  is_bot = u.is_bot OR excluded.is_bot,
  -- is_deleted comes from snapshots only: the newest observation of it wins, whatever order chats are applied in
  is_deleted = CASE WHEN u.deleted_seen_at IS NULL OR excluded.deleted_seen_at >= u.deleted_seen_at
                    THEN excluded.is_deleted ELSE u.is_deleted END,
  deleted_seen_at = greatest(u.deleted_seen_at, excluded.deleted_seen_at),
  -- latest profile values: a page at least as recent as the user's last sighting sets them, removals (a NULL
  -- username, premium false = NULL) included; an older page changes none; names are blanked when not kept
  is_premium = CASE WHEN excluded.last_seen_at >= u.last_seen_at THEN excluded.is_premium ELSE u.is_premium END,
  username = CASE WHEN NOT %(names)s THEN NULL WHEN excluded.last_seen_at >= u.last_seen_at
                  THEN excluded.username ELSE u.username END,
  first_name = CASE WHEN NOT %(names)s THEN NULL WHEN excluded.last_seen_at >= u.last_seen_at
                    THEN excluded.first_name ELSE u.first_name END,
  last_name = CASE WHEN NOT %(names)s THEN NULL WHEN excluded.last_seen_at >= u.last_seen_at
                   THEN excluded.last_name ELSE u.last_name END,
  first_seen_at = least(u.first_seen_at, excluded.first_seen_at),
  last_seen_at = greatest(u.last_seen_at, excluded.last_seen_at)
RETURNING WITH (OLD AS o, NEW AS n) n.user_id,   -- reports count humans: not a bot, is_deleted not true
  o.user_id IS NOT NULL AND (o.is_bot OR o.is_deleted IS TRUE) <> (n.is_bot OR n.is_deleted IS TRUE)"""


def apply_chunk(conn: psycopg.Connection, snapshot_id: int, chat_id: int, size: int, *,
                store_names: bool = True) -> int:
    """Apply the next chunk of a graded snapshot to up to `size` users (one transaction, the caller's); returns the
    number of users in the chunk. The last chunk sets reconciled_at."""
    history.lock_chats(conn, [chat_id])
    s = conn.execute("""
        SELECT chat_id, coalesce(source_chat_id, chat_id), started_at, quality, late, applied_through_user_id,
               reconciled_at IS NOT NULL
        FROM tg.snapshot WHERE snapshot_id = %s FOR UPDATE""", (snapshot_id,)).fetchone()
    if s is None or s[0] != chat_id or s[3] is None or s[6]:
        return 0  # re-keyed by a migration, deleted or done meanwhile: the next step picks again
    _, source, started_at, quality, late, cursor, _ = s
    retired = _retired(conn, source, started_at)
    absences = quality == "good" and not late and not retired  # A1-A3 apply to the unlisted
    k = BEFORE_ALL if cursor is None else cursor
    p = {"s": snapshot_id, "c": chat_id, "k": k, "n": size, "absences": absences}
    users = [r[0] for r in conn.execute("""
        SELECT user_id FROM (
          (SELECT user_id FROM tg.snapshot_member WHERE snapshot_id = %(s)s AND user_id > %(k)s
           ORDER BY user_id LIMIT %(n)s)
          UNION
          (SELECT user_id FROM tg.membership WHERE %(absences)s AND chat_id = %(c)s AND end_source IS NULL
             AND user_id > %(k)s ORDER BY user_id LIMIT %(n)s)) u
        ORDER BY user_id LIMIT %(n)s""", p).fetchall()]
    scanned = len(users)
    last = scanned < size
    counts = dict(listed=0, seeded=0, missed_joins=0, confirmed=0, absent_candidates=0, probes_queued=0,
                  conflicts=0)
    if users:
        p["hi"] = users[-1]
        if _erase_and_blank(conn, p, store_names):
            # the cursor and the run row never hold an erased id: they come from the users left in the range (the
            # dropped rows are gone, so a later chunk does not meet them again)
            users = [r[0] for r in conn.execute("""
                SELECT user_id FROM tg.snapshot_member WHERE snapshot_id = %(s)s AND user_id > %(k)s
                  AND user_id <= %(hi)s
                UNION
                SELECT user_id FROM tg.membership WHERE %(absences)s AND chat_id = %(c)s AND end_source IS NULL
                  AND user_id > %(k)s AND user_id <= %(hi)s
                ORDER BY 1""", p).fetchall()]
            if users:
                p["hi"] = users[-1]
    if users:
        _upsert_users(conn, p, store_names)
        prev = None
        if not late:
            # the previous graded snapshot of the same chat and source that is not late (history numbers these)
            prev = conn.execute("""
                SELECT snapshot_id, started_at FROM tg.snapshot
                WHERE chat_id = %(c)s AND coalesce(source_chat_id, chat_id) = %(e)s AND finished_at IS NOT NULL
                  AND quality IS NOT NULL AND NOT late AND (started_at, snapshot_id) < (%(t)s, %(s)s)
                ORDER BY started_at DESC, snapshot_id DESC LIMIT 1""",
                                {"c": chat_id, "e": source, "t": started_at, "s": snapshot_id}).fetchone()
        extend = prev is not None and not conn.execute("""
            SELECT EXISTS (SELECT 1 FROM tg.chat_alias WHERE (old_chat_id = %(e)s OR chat_id = %(e)s)
                             AND migrated_at > %(lo)s AND migrated_at <= %(t)s)""",
                                                       {"e": source, "lo": prev[1], "t": started_at}).fetchone()[0]
        conn.execute(_FOLD, {**p, "prev": prev[0] if prev else None, "extend": extend})
    _advance(conn, snapshot_id, users[-1] if users else None, last)
    if users:
        listed = conn.execute("SELECT user_id, seen_at FROM tg.snapshot_member WHERE snapshot_id = %(s)s "
                              "AND user_id > %(k)s AND user_id <= %(hi)s ORDER BY user_id", p).fetchall()
        counts["listed"] = len(listed)
        _count_rules(conn, chat_id, listed, counts)
        pairs = {u for u, _ in listed}
        if absences:
            pairs.update(r[0] for r in conn.execute(
                "SELECT user_id FROM tg.membership WHERE chat_id = %(c)s AND end_source IS NULL "
                "AND user_id > %(k)s AND user_id <= %(hi)s", p).fetchall())
        result = history.rebuild_pairs(conn, [(chat_id, u) for u in sorted(pairs)])
        counts["probes_queued"] += result.conflict_probes
        if absences:
            absent = [r[0] for r in conn.execute("""
                SELECT m.user_id FROM tg.membership m
                WHERE m.chat_id = %(c)s AND m.end_source IS NULL AND m.user_id > %(k)s AND m.user_id <= %(hi)s
                  AND m.last_confirmed_at < %(t)s
                  AND NOT EXISTS (SELECT 1 FROM tg.snapshot_member sm WHERE sm.snapshot_id = %(s)s
                                                                        AND sm.user_id = m.user_id)""",
                                                 {**p, "t": started_at}).fetchall()]
            counts["absent_candidates"] = len(absent)
            if absent:
                counts["probes_queued"] += conn.execute(
                    "SELECT tg.queue_probes(%s::bigint[], %s::bigint[], 'snapshot_absent', %s, %s)",
                    ([chat_id] * len(absent), absent, snapshot_id, started_at)).fetchone()[0]
    conn.execute("""
        INSERT INTO tg.reconcile_run (chat_id, snapshot_id, finished_at, listed, seeded, missed_joins, confirmed,
                                      absent_candidates, probes_queued, closed_by_probe, conflicts, chunk,
                                      from_user_id, through_user_id)
        VALUES (%(c)s, %(s)s, clock_timestamp(), %(listed)s, %(seeded)s, %(missed_joins)s, %(confirmed)s,
                %(absent_candidates)s, %(probes_queued)s, 0, %(conflicts)s,
                (SELECT count(*) + 1 FROM tg.reconcile_run WHERE snapshot_id = %(s)s), %(lo)s, %(hi)s)""",
                 {**counts, "c": chat_id, "s": snapshot_id, "lo": users[0] if users else None,
                  "hi": users[-1] if users else None})  # NULL ends: no user left in the chunk
    if last:
        total = conn.execute("""
            SELECT count(*), sum(listed), sum(seeded), sum(missed_joins), sum(confirmed), sum(absent_candidates),
                   sum(probes_queued), sum(conflicts) FROM tg.reconcile_run WHERE snapshot_id = %s""",
                             (snapshot_id,)).fetchone()
        log.info("snapshot %s of chat %s reconciled in %s chunks: listed %s, seeded %s, missed joins %s, "
                 "confirmed %s, absent candidates %s, probes queued %s, conflicts %s", snapshot_id, chat_id, *total)
    return scanned


def _erase_and_blank(conn: psycopg.Connection, p: dict, store_names: bool) -> int:
    """Drop the chunk's rows about people erased at or after the page (and erased inviters); blank names when the
    owner keeps none (design.md 11: snapshot_member after reconcile). Returns the rows dropped."""
    erasures = conn.execute("""
        SELECT EXISTS (SELECT 1 FROM tg.erasure WHERE erased_at >= (
          SELECT min(seen_at) FROM tg.snapshot_member WHERE snapshot_id = %(s)s AND user_id > %(k)s
                                                        AND user_id <= %(hi)s))""", p).fetchone()[0]
    dropped = 0
    if erasures:
        dropped = conn.execute("DELETE FROM tg.snapshot_member WHERE snapshot_id = %(s)s AND user_id > %(k)s "
                               "AND user_id <= %(hi)s AND tg.is_erased(user_id, seen_at)", p).rowcount
        conn.execute("UPDATE tg.snapshot_member SET inviter_id = NULL WHERE snapshot_id = %(s)s AND user_id > %(k)s "
                     "AND user_id <= %(hi)s AND inviter_id IS NOT NULL AND tg.is_erased(inviter_id, seen_at)", p)
    if not store_names:
        conn.execute("""UPDATE tg.snapshot_member SET username = NULL, first_name = NULL, last_name = NULL
                        WHERE snapshot_id = %(s)s AND user_id > %(k)s AND user_id <= %(hi)s
                          AND (username IS NOT NULL OR first_name IS NOT NULL OR last_name IS NOT NULL)""", p)
    return dropped


def _upsert_users(conn: psycopg.Connection, p: dict, store_names: bool) -> None:
    """tg.tg_user from the chunk's rows; a user who became (or stopped being) a bot or deleted account changes
    the humans of every chat they have spells in, so those chats' rollups are marked from their earliest spell."""
    rows = conn.execute(_UPSERT_USERS, {**p, "names": store_names}).fetchall()
    flipped = [u for u, changed in rows if changed]
    if flipped:
        conn.execute("""
            SELECT tg.mark_dirty(c.chat_id, (x.k AT TIME ZONE 'UTC')::date)
            FROM tg.chat c CROSS JOIN LATERAL (
              SELECT min(m.known_from) AS k FROM tg.membership m
              WHERE m.chat_id = c.chat_id AND m.user_id = ANY(%s::bigint[])) x
            WHERE x.k IS NOT NULL""", (flipped,))


def _advance(conn: psycopg.Connection, snapshot_id: int, hi: int | None, last: bool) -> None:
    """Move the cursor past the chunk before any evidence is read for the rebuild (the rebuild counts the chunk's
    points and absences only for users within it). The last chunk marks the snapshot reconciled and clears the
    cursor, so no user id stays behind in it; a chunk with no user left (every row erased) keeps the cursor."""
    if last:
        conn.execute("UPDATE tg.snapshot SET reconciled_at = now(), applied_through_user_id = NULL "
                     "WHERE snapshot_id = %s", (snapshot_id,))
    elif hi is not None:
        conn.execute("UPDATE tg.snapshot SET applied_through_user_id = %s WHERE snapshot_id = %s", (hi, snapshot_id))


def _count_rules(conn: psycopg.Connection, chat_id: int, listed: list[tuple[int, datetime]], counts: dict) -> None:
    """S1 (seeded), S2 (confirmed), S4 (missed join), S3 (conflict) at each listed user's page time, from the
    replay's trace (tgaud.history), so the counts follow the rebuild rules without restating them."""
    evidence = history.load_evidence(conn, [(chat_id, u) for u, _ in listed])
    key = {"S1": "seeded", "S2": "confirmed", "S3": "conflicts", "S4": "missed_joins"}
    for u, seen in listed:
        trace: history.Trace = []
        history.replay(evidence[(chat_id, u)], trace)
        at = seen.astimezone(UTC)
        rule = next((r for r, t in trace if r in S_RULES and t == at), None)
        if rule is not None:
            counts[key[rule]] += 1


def prune_batch(conn: psycopg.Connection, snapshot_id: int) -> bool:
    """One batch of R3 for one snapshot; returns True when its last member row is gone (members_pruned_at set in
    that same transaction; prune_started_at, set by the first batch, keeps it selected until then)."""
    conn.execute("UPDATE tg.snapshot SET prune_started_at = coalesce(prune_started_at, now()) WHERE snapshot_id = %s",
                 (snapshot_id,))
    deleted = conn.execute("""
        DELETE FROM tg.snapshot_member WHERE snapshot_id = %(s)s AND user_id <= (
          SELECT max(user_id) FROM (SELECT user_id FROM tg.snapshot_member WHERE snapshot_id = %(s)s
                                    ORDER BY user_id LIMIT %(n)s) x)""", {"s": snapshot_id, "n": DELETE_BATCH}).rowcount
    if deleted < DELETE_BATCH:
        conn.execute("UPDATE tg.snapshot SET members_pruned_at = now() WHERE snapshot_id = %s", (snapshot_id,))
        return True
    return False


def register_gaps(conn: psycopg.Connection) -> int:
    """tg.gap_progress rows for gaps G1 has not seen: a chat's gap gets one row, a gap of every chat one row per
    chat with an open spell now (later chats were rebuilt with the gap already). A (gap, chat) whose row ingest
    deleted (its start moved earlier) is registered again and handled from the lowest user id."""
    return conn.execute("""
        INSERT INTO tg.gap_progress (gap_id, chat_id)
        SELECT g.gap_id, g.chat_id FROM tg.gap g
         WHERE g.chat_id IS NOT NULL AND NOT isempty(g.period) AND NOT lower_inf(g.period)
           AND NOT EXISTS (SELECT 1 FROM tg.gap_progress p WHERE p.gap_id = g.gap_id)
        UNION ALL
        SELECT g.gap_id, c.chat_id FROM tg.gap g CROSS JOIN tg.chat c
         WHERE g.chat_id IS NULL AND NOT isempty(g.period) AND NOT lower_inf(g.period)
           AND NOT EXISTS (SELECT 1 FROM tg.gap_progress p WHERE p.gap_id = g.gap_id)
           AND EXISTS (SELECT 1 FROM tg.membership m WHERE m.chat_id = c.chat_id AND m.end_source IS NULL)
        ON CONFLICT DO NOTHING""").rowcount


def g1_chunk(conn: psycopg.Connection, gap_id: int, chat_id: int, size: int) -> int:
    """Rule G1 for one (gap, chat), one chunk: rebuild the next open spells doubted by the gap (last positive
    evidence before its start) or doubted since its start or later (a moved or merged gap start), and cancel the
    pending outage probes of those no longer in doubt; the last chunk queues outage probes for the newest spells
    doubted by the gap, within OUTAGE_BUDGET and for tracked chats only, and marks the progress done. Returns the
    spells taken."""
    history.lock_chats(conn, [chat_id])
    row = conn.execute("""
        SELECT p.last_user_id, p.done, lower(g.period), c.tracked
        FROM tg.gap_progress p JOIN tg.gap g ON g.gap_id = p.gap_id JOIN tg.chat c ON c.chat_id = p.chat_id
        WHERE p.gap_id = %s AND p.chat_id = %s FOR UPDATE OF p""", (gap_id, chat_id)).fetchone()
    if row is None or row[1]:
        return 0
    cursor, _, start, tracked = row
    # doubted by this gap (last positive evidence before its start), or doubted since its start or later: when a
    # gap start moved earlier or vanished (gaps merged), such a doubt may have no gap left to stand on
    users = [r[0] for r in conn.execute("""
        SELECT user_id FROM tg.membership
        WHERE chat_id = %(c)s AND end_source IS NULL AND user_id > %(k)s
          AND (last_confirmed_at < %(t)s OR suspect_since >= %(t)s)
        ORDER BY user_id LIMIT %(n)s""", {"c": chat_id, "k": BEFORE_ALL if cursor is None else cursor, "t": start,
                                          "n": size}).fetchall()]
    result = history.rebuild_pairs(conn, [(chat_id, u) for u in users])
    if users:
        # an outage probe for a spell no longer in doubt has nothing to check
        conn.execute("""
            UPDATE tg.probe p SET outcome = 'cancelled'
            WHERE p.chat_id = %s AND p.user_id = ANY(%s::bigint[]) AND p.outcome = 'pending' AND p.reason = 'outage'
              AND NOT EXISTS (SELECT 1 FROM tg.membership m WHERE m.chat_id = p.chat_id AND m.user_id = p.user_id
                                AND m.end_source IS NULL AND m.suspect_since IS NOT NULL)""", (chat_id, users))
    if len(users) == size:
        conn.execute("UPDATE tg.gap_progress SET last_user_id = %s, updated_at = now() WHERE gap_id = %s "
                     "AND chat_id = %s", (users[-1], gap_id, chat_id))
        return len(users)
    probes = 0
    if tracked:
        newest = [r[0] for r in conn.execute("""
            SELECT user_id FROM tg.membership
            WHERE chat_id = %s AND end_source IS NULL AND last_confirmed_at < %s
            ORDER BY known_from DESC, user_id LIMIT %s""", (chat_id, start, OUTAGE_BUDGET)).fetchall()]
        if newest:
            probes = conn.execute("SELECT tg.queue_probes(%s::bigint[], %s::bigint[], 'outage')",
                                  ([chat_id] * len(newest), newest)).fetchone()[0]
    conn.execute("UPDATE tg.gap_progress SET done = true, last_user_id = NULL, updated_at = now() "
                 "WHERE gap_id = %s AND chat_id = %s", (gap_id, chat_id))
    conn.execute("""INSERT INTO tg.reconcile_run (chat_id, snapshot_id, finished_at, probes_queued)
                    VALUES (%s, NULL, clock_timestamp(), %s)""", (chat_id, probes + result.conflict_probes))
    log.info("gap %s, chat %s: G1 done, %s outage probes queued", gap_id, chat_id, probes)
    return len(users)
