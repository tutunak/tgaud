"""Job `prober` (worker thread): getChatMember checks from tg.probe within the budget (design.md 6, D5).

Queue order: `manual` (the CLI) first, then `conflict`, `outage` (the newest open spell first), `snapshot_absent`,
`sample`; ties by queued_at. Only probes that are ready are taken: never tried, or past the backoff of their
last failure. The candidates are cached: a full read every FULL_REFRESH seconds or when the cache runs out. Between
full reads, every step first brings the cache up to date at the places of the same order (the head read):
  - per chat and first-come reason, the oldest ready probes (by the index probe_pending_reason): a conflict or manual
    probe queued or raised in place meanwhile, or a chat that starts waiting;
  - the set of each chat's pending outage probes, as a count, an id sum and an xor of id hashes, read before the
    full read and before every head read, is compared with what the cache expects (the read's, less the outage
    probes this job has finished since). Any other change (probes queued, raised in place to outage, re-keyed by M1,
    cancelled) makes the job read and rank that chat's whole slice again, its cut included;
  - an outage probe's key is its pair's newest spell, which rebuilds change: a rebuild writes new spell rows, so the
    pairs with membership ids above the mark (the highest id) of a read SPELL_MARGIN ago are the ones whose keys may
    have moved; their pending outage probes are keyed again in place. The margin covers a rebuild that took its ids
    before a read and committed after it (ingest's short transactions; the reconciler shares this job's thread);
  - probes in backoff are cached with the time their backoff ends (per chat as many as ready ones; every failure this
    job records too) and taken at their place from then on. When an outage probe in backoff that did not fit becomes
    eligible, the chat is ranked again;
  - an answer that straddles an event of the pair goes back into the cache, eligible from the next second, when a
    new call falls outside that event's second.
A head candidate the full read would have left out (after the last cached candidate of a chat whose read was cut at
PER_CHAT) stays out.

Budget: one global token bucket at `config.probe_rate` calls/s (TGAUD_PROBE_RATE, default 5; burst one second's
worth). While other chats have ready probes, one chat gets at most half of the rate (its own bucket at rate/2);
alone, a chat may use all of it. Manual probes skip the per-chat share. A 429 pauses the whole bucket for its
`retry_after` and does not count as an attempt: the probe is asked again when the bucket resumes.

Failures (any other ApiError, or an answer without a known status): the probe is retried 1 min, 10 min and 1 h
after its first, second and third failure; the fourth failure is outcome `error` with the class of the last one
(P4: unknown, nothing changes, so no rebuild).

Per probe, without a transaction open across the HTTP call:
  1. the candidates are read (the cache above) and that transaction is committed;
  2. getChatMember; sent_at = the send time floored to the second (event dates are whole seconds, so an event of
     the same second counts as inside the call);
  3. fence, lock_chats(chat), then the answer is written only if the probe row is still pending with the chat and
     user the request was sent for (M1 re-keys rows, the CLI cancels a pending probe for its manual one);
     otherwise it is dropped;
  4. an exact event of the pair with sent_at <= event_at <= answered_at: the answer may describe either side, so
     it is not stored and the probe stays pending (requeued; not an attempt). In the other commit order (the answer
     first, the event later) the rebuild ignores the answer (history's rule) and the trigger
     tg.probe_requeue_straddled (migration 005) queues the same question again in the event's transaction;
  5. the answer (outcome in/out, answered_at, sent_at, status) and rebuild_pairs for the pair (P1-P3) commit
     together.
"""

from __future__ import annotations

import bisect
import logging
import math
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone

import psycopg

from tgaud import history, metrics
from tgaud.botapi import ApiError
from tgaud.jobs import Job, JobContext

log = logging.getLogger(__name__)

NAME = "prober"
BACKOFF = (60.0, 600.0, 3600.0)  # seconds after the 1st, 2nd, 3rd failure; the 4th failure is `error`
PRIORITY = ("manual", "conflict", "outage", "snapshot_absent", "sample")
FULL_REFRESH = 120.0  # seconds between full candidate reads while the cache has work; a cache that runs out is read
# again at once. The full read orders every pending probe (1-3 s at 100 000 outage probes), so it is not repeated often.
IDLE_REFRESH = 5.0  # seconds between reads of an empty queue
PER_CHAT = 600  # candidates read per chat: two minutes of the whole default budget
PER_HEAD = 5  # ready probes read per chat and first-come reason by each step's head read
PER_STEP = 20  # probes per step
STEP_SECONDS = 3.0  # and at most this long (wall time) per step
SPELL_MARGIN = 5.0  # seconds a rebuild may take between its spell ids and its commit and still be seen
STATUSES = frozenset({"creator", "administrator", "member", "restricted", "left", "kicked"})

_READY_AT = """CASE WHEN p.attempts > 0 AND p.last_attempt_at IS NOT NULL THEN
           p.last_attempt_at + make_interval(secs => (%(backoff)s::float8[])[least(p.attempts, %(steps)s)]) END"""

# Pending probes in queue order, per chat at most per_chat ready ones and per_chat in backoff (with the end of their
# backoff, so the cache takes them when they become ready), and per chat the earliest end of backoff among the outage
# probes in backoff left out. {where} narrows it to one chat.
_CANDIDATES = """
WITH pending AS (
  SELECT p.probe_id, p.chat_id, p.user_id, p.reason, p.queued_at,
         array_position(%(priority)s::text[], p.reason) AS prio, {ready_at} AS ready_at
  FROM tg.probe p
  WHERE p.outcome = 'pending'{where}
), keyed AS (
  SELECT q.*, coalesce(q.ready_at > %(now)s, false) AS waiting,
         -- the pair's newest spell (the open one G1 queued it for): an index-only read of membership_pair
         CASE WHEN q.reason = 'outage' THEN
           (SELECT max(m.known_from) FROM tg.membership m
             WHERE m.chat_id = q.chat_id AND m.user_id = q.user_id) END AS spell_from
  FROM pending q
), ranked AS (
  SELECT k.*, row_number() OVER (PARTITION BY k.chat_id, k.waiting
                                 ORDER BY k.prio, k.spell_from DESC NULLS LAST, k.queued_at, k.probe_id) AS rn
  FROM keyed k
), cut AS (
  SELECT r.*, min(r.ready_at) FILTER (WHERE r.waiting AND r.reason = 'outage' AND r.rn > %(per_chat)s)
                OVER (PARTITION BY r.chat_id) AS omitted_ready
  FROM ranked r
)
SELECT prio, extract(epoch FROM spell_from)::float8, extract(epoch FROM queued_at)::float8, probe_id, chat_id,
       user_id, reason, CASE WHEN waiting THEN extract(epoch FROM ready_at)::float8 END,
       extract(epoch FROM omitted_ready)::float8
FROM cut WHERE rn <= %(per_chat)s
ORDER BY prio, spell_from DESC NULLS LAST, queued_at, probe_id"""

_ALL = _CANDIDATES.format(ready_at=_READY_AT, where="")
_CHAT = _CANDIDATES.format(ready_at=_READY_AT, where=" AND p.chat_id = %(chat)s")

# The pending outage probes of each chat as a set: count, id sum and an xor of id hashes (this job takes the hash of
# what it finishes from the row it locks, so its own answers keep the expected set exact).
_OUTAGE_SETS = """
SELECT p.chat_id, count(*), sum(p.probe_id), bit_xor(hashint8extended(p.probe_id, 0)) FROM tg.probe p
WHERE p.outcome = 'pending' AND p.reason = 'outage' GROUP BY p.chat_id"""

# The newest spell row (a rebuild inserts every spell it changes, with a new id)
_SPELL_MARK = "SELECT coalesce(max(membership_id), 0) FROM tg.membership"

# The pending outage probes of the pairs with spell rows above the mark, keyed by their newest spell now
_RESPELLED = f"""
SELECT array_position(%(priority)s::text[], p.reason), extract(epoch FROM s.spell_from)::float8,
       extract(epoch FROM p.queued_at)::float8, p.probe_id, p.chat_id, p.user_id, p.reason,
       CASE WHEN r.ready_at > %(now)s THEN extract(epoch FROM r.ready_at)::float8 END
FROM (SELECT DISTINCT m.chat_id, m.user_id FROM tg.membership m WHERE m.membership_id > %(mark)s) x
JOIN tg.probe p ON p.chat_id = x.chat_id AND p.user_id = x.user_id AND p.outcome = 'pending' AND p.reason = 'outage'
CROSS JOIN LATERAL (SELECT max(m.known_from) AS spell_from FROM tg.membership m
                    WHERE m.chat_id = p.chat_id AND m.user_id = p.user_id) s
CROSS JOIN LATERAL (SELECT {_READY_AT} AS ready_at) r"""

_READY = """
    AND (p.attempts = 0 OR p.last_attempt_at IS NULL
         OR p.last_attempt_at + make_interval(secs => (%(backoff)s::float8[])[least(p.attempts, %(steps)s)])
            <= %(now)s)"""

# The heads of the first-come reasons, per chat and reason (short index ranges of probe_pending_reason): the oldest
# ready probes, so also a row whose reason was raised in place. Same columns and order key as _CANDIDATES.
_HEADS = f"""
SELECT r.prio::int, NULL::float8, extract(epoch FROM h.queued_at)::float8, h.probe_id, h.chat_id, h.user_id, r.reason
FROM tg.chat c
CROSS JOIN unnest(%(priority)s::text[]) WITH ORDINALITY AS r(reason, prio)
CROSS JOIN LATERAL (
  SELECT p.probe_id, p.chat_id, p.user_id, p.queued_at FROM tg.probe p
  WHERE p.chat_id = c.chat_id AND p.reason = r.reason AND p.outcome = 'pending' {_READY}
  ORDER BY p.queued_at, p.probe_id LIMIT %(per_head)s) h
WHERE r.reason <> 'outage'"""


class TokenBucket:
    """`rate` tokens per second up to `burst`; times are the caller's clock (seconds). No sleeping: callers ask
    how long to wait."""

    def __init__(self, rate: float, now: float, burst: float | None = None) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        self.rate = rate
        self.burst = burst if burst is not None else max(1.0, rate)
        self.tokens = self.burst
        self.at = now
        self.paused_until = now

    def _refill(self, now: float) -> None:
        if now > self.at:
            self.tokens = min(self.burst, self.tokens + (now - self.at) * self.rate)
            self.at = now

    def wait(self, now: float) -> float:
        """Seconds until a token is available (0 = now)."""
        if now < self.paused_until:
            return self.paused_until - now
        self._refill(now)
        return 0.0 if self.tokens >= 1.0 else (1.0 - self.tokens) / self.rate

    def take(self, now: float) -> bool:
        if self.wait(now) > 0:
            return False
        self.tokens -= 1.0
        return True

    def pause(self, now: float, seconds: float) -> None:
        """Nothing for `seconds` (a 429's retry_after); at its end one call may go, then the rate (never a burst)."""
        until = now + max(0.0, seconds)
        if until > self.paused_until:
            self.paused_until = until
        self.tokens = 0.0
        self.at = self.paused_until - 1.0 / self.rate


@dataclass(frozen=True)
class Candidate:
    key: tuple[int, float, float, int]  # queue order: priority, newest spell first (outage), queued_at, probe_id
    probe_id: int
    chat_id: int
    user_id: int
    reason: str
    ready_at: float = 0.0  # the end of its backoff (job clock); taken only from then on


def _utc(t: float) -> datetime:
    return datetime.fromtimestamp(t, timezone.utc)


def _candidate(prio: int, spell_from: float | None, queued_at: float, probe_id: int, chat_id: int, user_id: int,
               reason: str, ready_at: float | None = None) -> Candidate:
    """A row of _CANDIDATES or _HEADS, keyed like their ORDER BY (spell_from DESC NULLS LAST)."""
    spell = -spell_from if spell_from is not None else math.inf
    return Candidate((prio, spell, queued_at, probe_id), probe_id, chat_id, user_id, reason, ready_at or 0.0)


class Prober:
    name = NAME

    def __init__(self, ctx: JobContext, *, rate: float | None = None) -> None:
        self.ctx = ctx
        self.rate = float(rate if rate is not None else getattr(ctx.config, "probe_rate", 5.0))
        now = ctx.clock()
        self.bucket = TokenBucket(self.rate, now)
        self.chat_buckets: dict[int, TokenBucket] = {}
        self.queue: list[Candidate] = []
        self.next_read = now  # the full read
        self.horizon: dict[int, tuple[int, float, float, int]] = {}  # chat -> its last ready candidate's key, if cut
        self.outage_sets: dict[int, tuple[int, int, int]] = {}  # chat -> the pending outage set the cache expects
        self.omitted: dict[int, float] = {}  # chat -> when an outage probe in backoff left out becomes eligible
        self.spell_marks: list[tuple[float, int]] = []  # (when, highest membership_id) of recent reads
        self.not_before: dict[int, float] = {}  # probe_id -> no new call before (an answer that straddled an event)

    # --- scheduling --------------------------------------------------------------------------------------------

    def due(self, now: float) -> bool:
        if self.ctx.api is None:
            return False
        if self.bucket.wait(now) > 0:
            return False
        return now >= self.next_read or self._pick(now) is not None

    def _chat_bucket(self, chat_id: int, now: float) -> TokenBucket:
        b = self.chat_buckets.get(chat_id)
        if b is None:
            b = self.chat_buckets[chat_id] = TokenBucket(self.rate / 2, now, burst=max(1.0, self.rate / 2))
        return b

    def _pick(self, now: float) -> Candidate | None:
        """The first candidate in queue order the budget allows now, or None."""
        if not self.queue or self.bucket.wait(now) > 0:
            return None
        ready = [c for c in self.queue if c.ready_at <= now]
        alone = len({c.chat_id for c in ready}) == 1
        for cand in ready:
            if cand.reason == "manual" or alone or self._chat_bucket(cand.chat_id, now).wait(now) == 0:
                return cand
        return None

    def _take(self, cand: Candidate, now: float) -> None:
        self.queue.remove(cand)
        if not any(c.ready_at <= now for c in self.queue):
            self.next_read = min(self.next_read, now)  # a backlog longer than one read: read on at once
        self.bucket.take(now)
        self._chat_bucket(cand.chat_id, now).take(now)  # counted even when the share did not apply

    def _requeue(self, cand: Candidate) -> None:
        bisect.insort(self.queue, cand, key=lambda c: c.key)

    # --- the step -----------------------------------------------------------------------------------------------

    def step(self, conn: psycopg.Connection) -> None:
        now = self.ctx.clock()
        if now >= self.next_read:
            self._read(conn, now)
        else:
            self._read_heads(conn, now)
        conn.commit()  # never hold a transaction (or the fence) across a Bot API call
        started = time.monotonic()
        done = 0
        while done < PER_STEP and time.monotonic() - started < STEP_SECONDS and not self.ctx.stop.is_set():
            now = self.ctx.clock()
            cand = self._pick(now)
            if cand is None:
                break
            self._take(cand, now)
            self.probe(conn, cand)
            done += 1

    def _params(self, now: float, **more) -> dict:
        return {"priority": list(PRIORITY), "backoff": list(BACKOFF), "steps": len(BACKOFF), "now": _utc(now),
                "per_chat": PER_CHAT, **more}

    def _entry(self, row: tuple) -> Candidate:
        cand = _candidate(*row[:8])
        wait = self.not_before.get(cand.probe_id, 0.0)
        return replace(cand, ready_at=wait) if wait > cand.ready_at else cand

    def _slice(self, rows: list[tuple], chats: set[int]) -> None:
        """The rows of a read of `chats` (ranked, cut per chat) become their cached slices, cuts and omitted
        retries; whatever the cache held of those chats goes."""
        cands = [self._entry(r) for r in rows]
        ids = {c.probe_id for c in cands}
        self.queue = [c for c in self.queue if c.chat_id not in chats and c.probe_id not in ids]
        ready: dict[int, list[Candidate]] = {}
        for chat in chats:
            self.horizon.pop(chat, None)
            self.omitted.pop(chat, None)
        for r, c in zip(rows, cands):
            if r[7] is None:
                ready.setdefault(c.chat_id, []).append(c)
            if r[8] is not None:
                self.omitted[c.chat_id] = r[8]
        for chat, rs in ready.items():
            if len(rs) >= PER_CHAT:
                self.horizon[chat] = rs[-1].key
        for c in cands:
            if c.chat_id not in self.horizon or c.key <= self.horizon[c.chat_id]:
                self._requeue(c)

    def _outage_sets(self, conn: psycopg.Connection) -> dict[int, tuple[int, int, int]]:
        return {c: (n, int(total), h) for c, n, total, h in conn.execute(_OUTAGE_SETS).fetchall()}

    def _read(self, conn: psycopg.Connection, now: float) -> None:
        mark = conn.execute(_SPELL_MARK).fetchone()[0]  # the marks and sets before the read: a change shows later
        sets = self._outage_sets(conn)
        rows = conn.execute(_ALL, self._params(now)).fetchall()
        self.not_before = {p: t for p, t in self.not_before.items() if t > now}
        self.queue, self.horizon, self.omitted = [], {}, {}
        self._slice(rows, {r[4] for r in rows})
        self.outage_sets = sets
        self._mark(now, mark)
        self.next_read = now + (FULL_REFRESH if self.queue else IDLE_REFRESH)
        known = {c.chat_id for c in self.queue}
        for chat_id in [c for c in self.chat_buckets if c not in known]:
            del self.chat_buckets[chat_id]  # idle chats start again with a full share

    def _read_heads(self, conn: psycopg.Connection, now: float) -> None:
        """The changes since the full read into the cache at their place in the queue order: a chat whose outage set
        changed, or whose left-out outage retry became eligible, is read again whole; outage probes whose spells were
        rebuilt are keyed again; the heads of the first-come reasons (a cached probe whose reason was raised in place
        moves to its new place)."""
        mark = conn.execute(_SPELL_MARK).fetchone()[0]
        sets = self._outage_sets(conn)
        since = self._mark(now, mark)
        respelled = conn.execute(_RESPELLED, self._params(now, mark=since)).fetchall()
        again = {c for c in set(sets) | set(self.outage_sets) if sets.get(c) != self.outage_sets.get(c)}
        again |= {c for c, t in self.omitted.items() if t <= now}
        for chat in sorted(again):
            self._read_chat(conn, now, chat, sets.get(chat))
        cached = {c.probe_id: c for c in self.queue}
        for row in respelled:  # the chats read again above already have these keys
            cand = self._entry(row)
            if cand.chat_id in again:
                continue
            old = cached.pop(cand.probe_id, None)
            if old is not None:
                self.queue.remove(old)
            cut = self.horizon.get(cand.chat_id)
            if cut is None or cand.key <= cut:
                self._requeue(cand)
                cached[cand.probe_id] = cand
        for row in conn.execute(_HEADS, self._params(now, per_head=PER_HEAD)):
            cand = _candidate(*row)
            cut = self.horizon.get(cand.chat_id)
            if cut is not None and cand.key > cut:
                continue  # beyond what the full read kept of this chat: it comes with a later full read
            old = cached.get(cand.probe_id)
            if old is not None and old.key[0] <= cand.key[0]:
                continue  # cached at its place (or higher: the full read's spell order stays)
            if old is not None:
                self.queue.remove(old)
            self._requeue(cand)
            cached[cand.probe_id] = cand

    def _read_chat(self, conn: psycopg.Connection, now: float, chat: int, expect: tuple[int, int, int] | None) -> None:
        """One chat's whole slice read and ranked again; `expect` is its outage set as read before."""
        self._slice(conn.execute(_CHAT, self._params(now, chat=chat)).fetchall(), {chat})
        if expect is None:
            self.outage_sets.pop(chat, None)
        else:
            self.outage_sets[chat] = expect

    def _mark(self, now: float, mark: int) -> int:
        """Record this read's spell mark; returns the mark of the latest read at least SPELL_MARGIN ago (else the
        oldest kept), from which spell rows may still be new to the cache."""
        self.spell_marks.append((now, mark))
        old = [i for i, (t, _) in enumerate(self.spell_marks) if t <= now - SPELL_MARGIN]
        if old:
            del self.spell_marks[:old[-1]]
        return self.spell_marks[0][1]

    def _finished_outage(self, chat: int, probe_id: int, probe_hash: int) -> None:
        """This job finished a pending outage probe: the set the cache expects loses it."""
        n, total, h = self.outage_sets.get(chat, (0, 0, 0))
        if n <= 1:
            self.outage_sets.pop(chat, None)
        else:
            self.outage_sets[chat] = (n - 1, total - probe_id, h ^ probe_hash)

    def probe(self, conn: psycopg.Connection, cand: Candidate) -> str:
        """Ask once and record the result; returns what happened (in, out, retry, error, late, dropped, 429)."""
        api = self.ctx.api
        assert api is not None
        sent = self.ctx.clock()
        member = error = None
        try:
            member = api.get_chat_member(cand.chat_id, cand.user_id)
        except ApiError as e:
            error = e
        answered = self.ctx.clock()
        if error is not None and error.outcome == "429":
            self.bucket.pause(answered, error.retry_after if error.retry_after is not None else 1.0)
            self._requeue(cand)
            log.info("prober: 429 from getChatMember, pausing %.0f s", error.retry_after or 1.0)
            return "429"

        status = member.get("status") if isinstance(member, dict) else None
        if error is None and status not in STATUSES:
            error = ApiError("getChatMember", "other", "an answer without a known status")

        self.ctx.fence(conn)
        history.lock_chats(conn, [cand.chat_id])
        row = conn.execute("SELECT chat_id, user_id, attempts, reason, hashint8extended(probe_id, 0) FROM tg.probe "
                           "WHERE probe_id = %s AND outcome = 'pending' FOR UPDATE", (cand.probe_id,)).fetchone()
        if row is None or (row[0], row[1]) != (cand.chat_id, cand.user_id):
            conn.commit()
            log.info("prober: probe %d is no longer pending for (%d, %d): answer dropped",
                     cand.probe_id, cand.chat_id, cand.user_id)
            return "dropped"
        attempts = row[2] + 1
        outage = row[3] == "outage"

        if error is not None:
            if attempts > len(BACKOFF):
                conn.execute("""UPDATE tg.probe SET outcome = 'error', error_code = %s, attempts = %s,
                                       last_attempt_at = %s WHERE probe_id = %s""",
                             (error.outcome, attempts, _utc(sent), cand.probe_id))
                conn.commit()
                if outage:
                    self._finished_outage(cand.chat_id, cand.probe_id, row[4])
                metrics.PROBES.labels("error").inc()
                log.info("prober: probe %d failed %d times (%s): error", cand.probe_id, attempts, error.outcome)
                return "error"
            conn.execute("UPDATE tg.probe SET attempts = %s, last_attempt_at = %s WHERE probe_id = %s",
                         (attempts, _utc(sent), cand.probe_id))
            conn.commit()
            # the SQL's readiness (last_attempt_at + the backoff of its attempts) on the job's clock
            self._requeue(replace(cand, ready_at=sent + BACKOFF[attempts - 1]))
            return "retry"

        assert isinstance(member, dict)
        is_member = member.get("is_member") if status == "restricted" else None
        is_member = is_member if isinstance(is_member, bool) else None
        outcome = "in" if history.status_in(status, is_member) else "out"
        sent_at = _utc(math.floor(sent))
        answered_at = _utc(answered)
        late = conn.execute("""SELECT EXISTS (SELECT 1 FROM tg.member_event
                                              WHERE chat_id = %s AND user_id = %s AND event_at BETWEEN %s AND %s)""",
                            (cand.chat_id, cand.user_id, sent_at, answered_at)).fetchone()[0]
        if late:
            conn.commit()
            # asked again from the next whole second on: a send then is after the event (dates are whole seconds)
            self.not_before[cand.probe_id] = math.floor(answered) + 1.0
            self._requeue(replace(cand, ready_at=self.not_before[cand.probe_id]))
            log.info("prober: probe %d straddles an event of the pair: asked again", cand.probe_id)
            return "late"
        conn.execute("""UPDATE tg.probe SET outcome = %s, answered_at = %s, sent_at = %s, status_seen = %s,
                               is_member_seen = %s, attempts = %s, last_attempt_at = %s
                        WHERE probe_id = %s""",
                     (outcome, answered_at, sent_at, status, is_member, attempts, _utc(sent), cand.probe_id))
        history.rebuild_pairs(conn, [(cand.chat_id, cand.user_id)])
        conn.commit()
        if outage:
            self._finished_outage(cand.chat_id, cand.probe_id, row[4])
        metrics.PROBES.labels(outcome).inc()
        return outcome


def make(ctx: JobContext) -> Job:
    return Prober(ctx)
