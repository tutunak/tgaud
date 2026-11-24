"""Job `prober` (worker thread): getChatMember checks from tg.probe within the budget (design.md 6, D5).

Queue order: `manual` (the CLI) first, then `conflict`, `outage` (by the pair's open spell, the newest first; a pair
without an open spell after those with one), `snapshot_absent`, `sample`; ties by queued_at, probe_id. Only probes
that are ready are taken: never tried, or past their `next_attempt_at` (the end of the backoff of their last failure).
Nothing is cached: every step that may make a call first reads the heads of the queue from the database (_HEADS:
per chat its first PER_CHAT ready probes, from one range of the index probe_ready, migration 010) and commits. The
order key of an outage probe (tg.probe.spell_from) is kept by triggers in the transactions that queue the probe or
rebuild the pair's spells, so the read sees every committed change, and what it misses is at most one step old.

Budget: one global token bucket at `config.probe_rate` calls/s (TGAUD_PROBE_RATE, default 5; burst one second's
worth). While other chats have ready probes, one chat gets at most half of the rate (its own bucket at rate/2);
alone, a chat may use all of it. Manual probes skip the per-chat share, never the global bucket. A 429 pauses the
whole bucket for its `retry_after` and does not count as an attempt: the probe is asked again when the bucket resumes.
A step takes at most PER_STEP probes, so PER_CHAT = PER_STEP heads of each chat are always enough for it.

Failures (any other ApiError, or an answer without a known status): the probe is retried 1 min, 10 min and 1 h
after its first, second and third failure (next_attempt_at); the fourth failure is outcome `error` with the class of
the last one (P4: unknown, nothing changes, so no rebuild). While nothing is ready the queue is read again every
IDLE_REFRESH seconds, and at the earliest retry deadline when that comes sooner.

Per probe, without a transaction open across the HTTP call:
  1. the heads are read (above) and that transaction is committed;
  2. getChatMember; sent_at = the send time floored to the second (event dates are whole seconds, so an event of
     the same second counts as inside the call);
  3. fence, lock_chats(chat), then the answer is written only if the probe row is still pending with the chat and
     user the request was sent for (M1 re-keys rows, the CLI cancels a pending probe for its manual one);
     otherwise it is dropped;
  4. an exact event of the pair with sent_at <= event_at <= answered_at: the answer may describe either side, so
     it is not stored and the probe stays pending, ready from the next second (next_attempt_at; not an attempt). In
     the other commit order (the answer first, the event later) the rebuild ignores the answer (history's rule) and
     the trigger tg.probe_requeue_straddled (migration 005) queues the same question again in the event's
     transaction;
  5. the answer (outcome in/out, answered_at, sent_at, status) and rebuild_pairs for the pair (P1-P3) commit
     together.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg

from tgaud import history, metrics
from tgaud.botapi import ApiError
from tgaud.jobs import Job, JobContext

log = logging.getLogger(__name__)

NAME = "prober"
BACKOFF = (60.0, 600.0, 3600.0)  # seconds after the 1st, 2nd, 3rd failure; the 4th failure is `error`
PRIORITY = ("manual", "conflict", "outage", "snapshot_absent", "sample")  # tg.probe_rank's order
IDLE_REFRESH = 5.0  # seconds between reads while nothing is ready (sooner when a retry falls due)
PER_STEP = 20  # probes per step
PER_CHAT = PER_STEP  # heads read per chat: one chat may fill a whole step
STEP_SECONDS = 3.0  # and at most this long (wall time) per step
STATUSES = frozenset({"creator", "administrator", "member", "restricted", "left", "kicked"})

# The first ready probes of every chat in queue order (one range of probe_ready each), then all of them in that order
_HEADS = """
SELECT h.probe_id, h.chat_id, h.user_id, h.reason
FROM tg.chat c
CROSS JOIN LATERAL (
  SELECT p.probe_id, p.chat_id, p.user_id, p.reason, tg.probe_rank(p.reason) AS rank, p.spell_from, p.queued_at
  FROM tg.probe p
  WHERE p.chat_id = c.chat_id AND p.outcome = 'pending' AND (p.next_attempt_at IS NULL OR p.next_attempt_at <= %(now)s)
  ORDER BY tg.probe_rank(p.reason), p.spell_from DESC NULLS LAST, p.queued_at, p.probe_id
  LIMIT %(per_chat)s) h
ORDER BY h.rank, h.spell_from DESC NULLS LAST, h.queued_at, h.probe_id"""

# The earliest retry deadline still ahead (probe_retry_due)
_NEXT_RETRY = """
SELECT extract(epoch FROM min(p.next_attempt_at))::float8 FROM tg.probe p
WHERE p.outcome = 'pending' AND p.next_attempt_at > %(now)s"""


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
    probe_id: int
    chat_id: int
    user_id: int
    reason: str


def _utc(t: float) -> datetime:
    return datetime.fromtimestamp(t, timezone.utc)


class Prober:
    name = NAME

    def __init__(self, ctx: JobContext, *, rate: float | None = None) -> None:
        self.ctx = ctx
        self.rate = float(rate if rate is not None else getattr(ctx.config, "probe_rate", 5.0))
        now = ctx.clock()
        self.bucket = TokenBucket(self.rate, now)
        self.chat_buckets: dict[int, TokenBucket] = {}
        self.next_read = now  # the next read of the queue (sooner never makes a call)

    # --- scheduling --------------------------------------------------------------------------------------------

    def due(self, now: float) -> bool:
        if self.ctx.api is None:
            return False
        if self.bucket.wait(now) > 0:
            return False
        return now >= self.next_read

    def _chat_bucket(self, chat_id: int, now: float) -> TokenBucket:
        b = self.chat_buckets.get(chat_id)
        if b is None:
            b = self.chat_buckets[chat_id] = TokenBucket(self.rate / 2, now, burst=max(1.0, self.rate / 2))
        return b

    def _share_wait(self, cand: Candidate, alone: bool, now: float) -> float:
        """Seconds until the per-chat share lets `cand` go (0 for a manual probe or a chat alone with ready ones)."""
        return 0.0 if cand.reason == "manual" or alone else self._chat_bucket(cand.chat_id, now).wait(now)

    def _pick(self, heads: list[Candidate], now: float) -> Candidate | None:
        """The first head in queue order the budget allows now, or None."""
        if not heads or self.bucket.wait(now) > 0:
            return None
        alone = len({c.chat_id for c in heads}) == 1
        return next((c for c in heads if self._share_wait(c, alone, now) == 0), None)

    def _take(self, cand: Candidate, now: float) -> None:
        self.bucket.take(now)
        self._chat_bucket(cand.chat_id, now).take(now)  # counted even when the share did not apply

    # --- the step -----------------------------------------------------------------------------------------------

    def heads(self, conn: psycopg.Connection, now: float) -> list[Candidate]:
        """The first ready probes of every chat (PER_CHAT each), in queue order."""
        return [Candidate(*r) for r in conn.execute(_HEADS, {"now": _utc(now), "per_chat": PER_CHAT})]

    def step(self, conn: psycopg.Connection) -> None:
        read_at = self.ctx.clock()
        heads = self.heads(conn, read_at)
        retry = None if heads else conn.execute(_NEXT_RETRY, {"now": _utc(read_at)}).fetchone()[0]
        conn.commit()  # never hold a transaction (or the fence) across a Bot API call
        self._forget_idle({c.chat_id for c in heads}, read_at)
        started = time.monotonic()
        done = 0
        while done < PER_STEP and time.monotonic() - started < STEP_SECONDS and not self.ctx.stop.is_set():
            now = self.ctx.clock()
            cand = self._pick(heads, now)
            if cand is None:
                break
            heads.remove(cand)
            self._take(cand, now)
            self.probe(conn, cand)
            done += 1
        now = self.ctx.clock()
        if done == 0 and not heads:  # nothing ready: read again after a while, or when a retry falls due
            self.next_read = min(read_at + IDLE_REFRESH, retry if retry is not None else math.inf)
        elif heads:  # what is left waits for the global bucket (due() checks it) or its chat's share
            alone = len({c.chat_id for c in heads}) == 1
            self.next_read = now + min(self._share_wait(c, alone, now) for c in heads)
        else:  # every head read was asked: there may be more
            self.next_read = now

    def _forget_idle(self, chats: set[int], now: float) -> None:
        """A chat without ready probes whose share has refilled starts again with a fresh one (the same tokens)."""
        for chat_id in [c for c, b in self.chat_buckets.items() if c not in chats and b.wait(now) == 0
                        and b.tokens >= b.burst]:
            del self.chat_buckets[chat_id]

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
            log.info("prober: 429 from getChatMember, pausing %.0f s", error.retry_after or 1.0)
            return "429"  # still pending and ready: the first read after the pause takes it again

        status = member.get("status") if isinstance(member, dict) else None
        if error is None and status not in STATUSES:
            error = ApiError("getChatMember", "other", "an answer without a known status")

        self.ctx.fence(conn)
        history.lock_chats(conn, [cand.chat_id])
        row = conn.execute("SELECT chat_id, user_id, attempts FROM tg.probe "
                           "WHERE probe_id = %s AND outcome = 'pending' FOR UPDATE", (cand.probe_id,)).fetchone()
        if row is None or (row[0], row[1]) != (cand.chat_id, cand.user_id):
            conn.commit()
            log.info("prober: probe %d is no longer pending for (%d, %d): answer dropped",
                     cand.probe_id, cand.chat_id, cand.user_id)
            return "dropped"
        attempts = row[2] + 1

        if error is not None:
            if attempts > len(BACKOFF):
                conn.execute("""UPDATE tg.probe SET outcome = 'error', error_code = %s, attempts = %s,
                                       last_attempt_at = %s WHERE probe_id = %s""",
                             (error.outcome, attempts, _utc(sent), cand.probe_id))
                conn.commit()
                metrics.PROBES.labels("error").inc()
                log.info("prober: probe %d failed %d times (%s): error", cand.probe_id, attempts, error.outcome)
                return "error"
            conn.execute("UPDATE tg.probe SET attempts = %s, last_attempt_at = %s, next_attempt_at = %s "
                         "WHERE probe_id = %s",
                         (attempts, _utc(sent), _utc(sent + BACKOFF[attempts - 1]), cand.probe_id))
            conn.commit()
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
            # asked again from the next whole second on: a send then is after the event (dates are whole seconds)
            conn.execute("UPDATE tg.probe SET next_attempt_at = %s WHERE probe_id = %s",
                         (_utc(math.floor(answered) + 1.0), cand.probe_id))
            conn.commit()
            log.info("prober: probe %d straddles an event of the pair: asked again", cand.probe_id)
            return "late"
        conn.execute("""UPDATE tg.probe SET outcome = %s, answered_at = %s, sent_at = %s, status_seen = %s,
                               is_member_seen = %s, attempts = %s, last_attempt_at = %s
                        WHERE probe_id = %s""",
                     (outcome, answered_at, sent_at, status, is_member, attempts, _utc(sent), cand.probe_id))
        history.rebuild_pairs(conn, [(cand.chat_id, cand.user_id)])
        conn.commit()
        metrics.PROBES.labels(outcome).inc()
        return outcome


def make(ctx: JobContext) -> Job:
    return Prober(ctx)
