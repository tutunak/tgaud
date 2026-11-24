"""Job `sampler` (worker thread): drift samples (getChatMemberCount every 10 minutes per tracked chat, D7) and the
metric cache (design.md 6, 9).

Samples. Each tracked chat has a fixed phase in the 10-minute interval (a hash of its id), so the samples of 200
chats spread over the interval instead of all coming at :00; a chat is sampled at the first slot after its last
sample (after a longer stop, at the first slot after now, so a restart does not burst). A sample stores
`reported`, `known_open` (open spells, all accounts: an index-only count) and `suspect_open` at once, and
`net_events` NULL: the sample is pending. sampled_at is the send time of getChatMemberCount and request_ms the
call's length (job clock). Every step finalizes the pending samples that ingest has caught up past
(tg.bot_state.caught_up_at > sampled_at, so every update dated before the sample is committed): net_events =
exact joins minus exact exits of spells in (previous sample, this sample] (0 for a chat's first sample), the
definition R1's sample_net uses; `straddled` = an exact transition lies inside the call (from sampled_at floored to
the second, like event dates, to sampled_at + request_ms), so the count may or may not include it and the residuals
on both sides of the sample are withheld. The residual reads finalized samples only. A newly finalized sample of an
earlier UTC day than today marks its chat dirty from that day (tg.mark_dirty), so R1 republishes a day its 2-day
lookback no longer covers.

Metric cache (every minute): one query per gauge family over all tracked chats, then the gauges of
tgaud.metrics are set from the results; a scrape reads only those values. A chat that is no longer tracked loses
its series in these families; a renamed chat's tgaud_chat_info replaces its old series. The same step marks join
requests without an outcome after 7 days `unknown` (the funnel's rule, block 12) and counts them in
tgaud_chat_join_requests_total{outcome="unknown"}; a later approval found by ingest supersedes it.

Values set: tgaud_chat_members_reported/_known/_suspect, tgaud_chat_drift_mismatch (reported - known_open of the
latest finalized sample), tgaud_chat_drift_residual_24h (the sum of the delta residuals of the finalized samples of
the last 24 h, each against the previous finalized sample, as stats.v_drift; withheld residuals count as nothing), tgaud_chat_gap_open,
tgaud_chat_join_requests_pending, tgaud_snapshot_age_seconds (since the last good snapshot finished; no series
without one), tgaud_chat_info, tgaud_lost_seconds_total (confirmed service_down gaps that have ended; the counter
only grows), tgaud_probe_queue and tgaud_probe_oldest_age_seconds (0 for an empty queue).
"""

from __future__ import annotations

import hashlib
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import psycopg
from prometheus_client import Gauge

from tgaud import metrics
from tgaud.botapi import ApiError
from tgaud.jobs import Job, JobContext

log = logging.getLogger(__name__)

NAME = "sampler"
INTERVAL = 600.0  # seconds between two samples of a chat
METRICS_EVERY = 60.0  # seconds between two metric cache refreshes
SAMPLES_PER_STEP = 10
STEP_SECONDS = 3.0
REQUEST_UNKNOWN_AFTER = timedelta(days=7)
RESIDUAL_WINDOW = timedelta(hours=24)

_FINALIZE = """
WITH fin AS (
  UPDATE tg.count_sample s SET net_events = n.net, straddled = n.straddled
  FROM (
    SELECT p.chat_id, p.sampled_at,
           CASE WHEN p.prev IS NULL THEN 0 ELSE
             (SELECT count(*) FROM tg.membership m
               WHERE m.chat_id = p.chat_id AND m.joined_at > p.prev AND m.joined_at <= p.sampled_at)
           - (SELECT count(*) FROM tg.membership m
               WHERE m.chat_id = p.chat_id AND m.end_source = 'event'
                 AND m.left_before > p.prev AND m.left_before <= p.sampled_at) END AS net,
           EXISTS (SELECT 1 FROM tg.membership m
                    WHERE m.chat_id = p.chat_id AND m.joined_at >= p.call_from AND m.joined_at <= p.call_to)
           OR EXISTS (SELECT 1 FROM tg.membership m
                       WHERE m.chat_id = p.chat_id AND m.end_source = 'event'
                         AND m.left_before >= p.call_from AND m.left_before <= p.call_to) AS straddled
    FROM (SELECT x.chat_id, x.sampled_at,
                 (SELECT max(y.sampled_at) FROM tg.count_sample y
                   WHERE y.chat_id = x.chat_id AND y.sampled_at < x.sampled_at) AS prev,
                 date_trunc('second', x.sampled_at) AS call_from,
                 x.sampled_at + make_interval(secs => coalesce(x.request_ms, 0) / 1000.0) AS call_to
          FROM tg.count_sample x
          WHERE x.net_events IS NULL AND x.sampled_at < (SELECT max(b.caught_up_at) FROM tg.bot_state b)) p
  ) n
  WHERE s.chat_id = n.chat_id AND s.sampled_at = n.sampled_at AND s.net_events IS NULL
  RETURNING s.chat_id, s.sampled_at
), marked AS (
  SELECT tg.mark_dirty(f.chat_id, f.day)
  FROM (SELECT chat_id, min((sampled_at AT TIME ZONE 'UTC')::date) AS day FROM fin GROUP BY chat_id) f
  WHERE f.day < (now() AT TIME ZONE 'UTC')::date
)
SELECT (SELECT count(*) FROM fin), (SELECT count(*) FROM marked)"""

_INSERT_SAMPLE = """
INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, suspect_open, net_events, request_ms)
SELECT c.chat_id, %(at)s, %(reported)s,
       (SELECT count(*) FROM tg.membership m WHERE m.chat_id = c.chat_id AND m.end_source IS NULL),
       (SELECT count(*) FROM tg.membership m
         WHERE m.chat_id = c.chat_id AND m.end_source IS NULL AND m.suspect_since IS NOT NULL),
       NULL, %(ms)s
FROM tg.chat c WHERE c.chat_id = %(chat)s AND c.tracked
ON CONFLICT (chat_id, sampled_at) DO NOTHING"""

_SCHEDULE = """
SELECT c.chat_id, c.chat_type, c.title,
       (SELECT max(s.sampled_at) FROM tg.count_sample s WHERE s.chat_id = c.chat_id) AS last_sample
FROM tg.chat c WHERE c.tracked ORDER BY c.chat_id"""

_AGE_REQUESTS = """
UPDATE tg.join_request SET outcome = 'unknown'
WHERE outcome IS NULL AND requested_at <= %(cutoff)s
RETURNING chat_id"""

# One query per gauge family, each over all tracked chats (%(chats)s).
_Q_KNOWN = """
SELECT m.chat_id, count(*) FROM tg.membership m
WHERE m.chat_id = ANY(%(chats)s::bigint[]) AND m.end_source IS NULL GROUP BY m.chat_id"""

_Q_SUSPECT = """
SELECT m.chat_id, count(*) FROM tg.membership m
WHERE m.chat_id = ANY(%(chats)s::bigint[]) AND m.end_source IS NULL AND m.suspect_since IS NOT NULL
GROUP BY m.chat_id"""

# The latest finalized sample (reported, mismatch) and the 24 h residual: the sum of the delta residuals of the
# window's finalized samples, each against the previous finalized sample (the one before the window for the first),
# without the withheld ones (a straddled count on either side), as stats.v_drift computes them.
_Q_DRIFT = """
SELECT c.chat_id, l.reported, l.reported - l.known_open AS mismatch, w.residual
FROM unnest(%(chats)s::bigint[]) AS c(chat_id)
LEFT JOIN LATERAL (
  SELECT s.reported, s.known_open FROM tg.count_sample s
  WHERE s.chat_id = c.chat_id AND s.net_events IS NOT NULL AND s.sampled_at <= %(now)s
  ORDER BY s.sampled_at DESC LIMIT 1) l ON true
LEFT JOIN LATERAL (
  SELECT coalesce(sum(d.residual), 0) AS residual FROM (
    SELECT x.sampled_at,
           CASE WHEN x.straddled OR lag(x.straddled) OVER o THEN NULL
                ELSE (x.reported - lag(x.reported) OVER o) - x.net_events END AS residual
    FROM (SELECT s.sampled_at, s.reported, s.net_events, s.straddled FROM tg.count_sample s
          WHERE s.chat_id = c.chat_id AND s.net_events IS NOT NULL
            AND s.sampled_at > %(since)s AND s.sampled_at <= %(now)s
          UNION ALL
          (SELECT s.sampled_at, s.reported, s.net_events, s.straddled FROM tg.count_sample s
           WHERE s.chat_id = c.chat_id AND s.net_events IS NOT NULL AND s.sampled_at <= %(since)s
           ORDER BY s.sampled_at DESC LIMIT 1)) x
    WINDOW o AS (ORDER BY x.sampled_at)) d
  WHERE d.sampled_at > %(since)s
  HAVING count(*) > 0) w ON true"""

_Q_GAP_OPEN = """
SELECT c.chat_id FROM unnest(%(chats)s::bigint[]) AS c(chat_id)
WHERE EXISTS (SELECT 1 FROM tg.gap g WHERE (g.chat_id = c.chat_id OR g.chat_id IS NULL)
                AND NOT isempty(g.period) AND (upper_inf(g.period) OR g.period @> %(now)s::timestamptz))"""

_Q_REQUESTS_PENDING = """
SELECT r.chat_id, count(*) FROM tg.join_request r
WHERE r.chat_id = ANY(%(chats)s::bigint[]) AND r.outcome IS NULL GROUP BY r.chat_id"""

_Q_SNAPSHOT = """
SELECT s.chat_id, max(s.finished_at) FROM tg.snapshot s
WHERE s.chat_id = ANY(%(chats)s::bigint[]) AND s.quality = 'good' GROUP BY s.chat_id"""

_Q_PROBES = """
SELECT p.chat_id, count(*), min(p.queued_at) FROM tg.probe p
WHERE p.chat_id = ANY(%(chats)s::bigint[]) AND p.outcome = 'pending' GROUP BY p.chat_id"""

_Q_LOST = """
SELECT coalesce(sum(extract(epoch FROM upper(g.period) - lower(g.period))), 0)::float8 FROM tg.gap g
WHERE g.chat_id IS NULL AND g.reason = 'service_down' AND g.confirmed
  AND NOT isempty(g.period) AND NOT upper_inf(g.period) AND NOT lower_inf(g.period)"""

# the gauges the cache owns, by name in Snapshot.values
CHAT_GAUGES: dict[str, Gauge] = {
    "reported": metrics.CHAT_MEMBERS_REPORTED,
    "known": metrics.CHAT_MEMBERS_KNOWN,
    "suspect": metrics.CHAT_MEMBERS_SUSPECT,
    "mismatch": metrics.CHAT_DRIFT_MISMATCH,
    "residual": metrics.CHAT_DRIFT_RESIDUAL_24H,
    "gap_open": metrics.CHAT_GAP_OPEN,
    "requests_pending": metrics.CHAT_JOIN_REQUESTS_PENDING,
    "snapshot_age": metrics.SNAPSHOT_AGE,
    "probe_queue": metrics.PROBE_QUEUE,
    "probe_oldest": metrics.PROBE_OLDEST_AGE,
}


def _utc(t: float) -> datetime:
    return datetime.fromtimestamp(t, timezone.utc)


def phase(chat_id: int) -> float:
    """The chat's fixed offset in the interval, in seconds (stable across restarts and chat changes)."""
    h = int.from_bytes(hashlib.blake2b(str(chat_id).encode(), digest_size=8).digest(), "big")
    return (h % int(INTERVAL * 1000)) / 1000.0


def next_slot(chat_id: int, after: float) -> float:
    """The chat's first sampling time strictly after `after`."""
    p = phase(chat_id)
    return p + (math.floor((after - p) / INTERVAL) + 1) * INTERVAL


@dataclass
class Snapshot:
    """One refresh of the metric cache: per family {chat id: value}, plus the chats and the totals."""

    chats: dict[int, tuple[str, str]] = field(default_factory=dict)  # chat -> (type, title)
    values: dict[str, dict[int, float]] = field(default_factory=dict)
    lost_seconds: float = 0.0
    requests_aged: dict[int, int] = field(default_factory=dict)


class Sampler:
    name = NAME

    def __init__(self, ctx: JobContext) -> None:
        self.ctx = ctx
        self.schedule: dict[int, float] = {}  # tracked chat -> next sample time
        self.next_metrics = ctx.clock()
        self._published: dict[str, set[int]] = {k: set() for k in CHAT_GAUGES}
        self._info: dict[int, tuple[str, str, str]] = {}
        self._lost = 0.0

    def due(self, now: float) -> bool:
        if now >= self.next_metrics:
            return True
        return self.ctx.api is not None and any(t <= now for t in self.schedule.values())

    def step(self, conn: psycopg.Connection) -> None:
        now = self.ctx.clock()
        finalize(conn)
        if now >= self.next_metrics:
            self.next_metrics = now + METRICS_EVERY
            snap = self.collect(conn, now)
            conn.commit()
            self.publish(snap)
            return
        conn.commit()  # never hold a transaction (or the fence) across a Bot API call
        if self.ctx.api is None:
            return
        started = time.monotonic()
        due = sorted((t, c) for c, t in self.schedule.items() if t <= now)
        for _, chat_id in due[:SAMPLES_PER_STEP]:
            if self.ctx.stop.is_set() or time.monotonic() - started >= STEP_SECONDS:
                break
            self.sample(conn, chat_id)

    def sample(self, conn: psycopg.Connection, chat_id: int) -> bool:
        """One getChatMemberCount and its pending sample; True when a sample was stored."""
        api = self.ctx.api
        assert api is not None
        sent = self.ctx.clock()
        self.schedule[chat_id] = next_slot(chat_id, sent)
        try:
            reported = api.get_chat_member_count(chat_id)
        except ApiError as e:
            log.info("sampler: getChatMemberCount of %d failed (%s): next slot", chat_id, e.outcome)
            return False
        ms = max(0, math.ceil((self.ctx.clock() - sent) * 1000))  # the call's window, on the clock of sampled_at
        if isinstance(reported, bool) or not isinstance(reported, int) or reported < 0:
            log.warning("sampler: getChatMemberCount of %d returned no count", chat_id)
            return False
        self.ctx.fence(conn)
        n = conn.execute(_INSERT_SAMPLE, {"chat": chat_id, "at": _utc(sent), "reported": reported, "ms": ms}).rowcount
        conn.commit()
        return n == 1

    # --- the metric cache ---------------------------------------------------------------------------------------

    def collect(self, conn: psycopg.Connection, now: float) -> Snapshot:
        """Reload the tracked chats (and the sampling schedule), age join requests, read every gauge family."""
        snap = Snapshot()
        at = _utc(now)
        rows = conn.execute(_SCHEDULE).fetchall()
        tracked = []
        for chat_id, chat_type, title, last in rows:
            tracked.append(chat_id)
            snap.chats[chat_id] = (chat_type, title)
            if chat_id not in self.schedule:
                last_t = last.timestamp() if last is not None else None
                after = last_t if last_t is not None and last_t > now - INTERVAL else now
                self.schedule[chat_id] = next_slot(chat_id, after)
        for chat_id in [c for c in self.schedule if c not in snap.chats]:
            del self.schedule[chat_id]

        for (chat_id,) in conn.execute(_AGE_REQUESTS, {"cutoff": at - REQUEST_UNKNOWN_AFTER}).fetchall():
            snap.requests_aged[chat_id] = snap.requests_aged.get(chat_id, 0) + 1

        p = {"chats": tracked, "now": at, "since": at - RESIDUAL_WINDOW}
        v = snap.values
        v["known"] = {c: 0 for c in tracked} | dict(conn.execute(_Q_KNOWN, p).fetchall())
        v["suspect"] = {c: 0 for c in tracked} | dict(conn.execute(_Q_SUSPECT, p).fetchall())
        v["reported"], v["mismatch"], v["residual"] = {}, {}, {}
        for chat_id, reported, mismatch, residual in conn.execute(_Q_DRIFT, p).fetchall():
            if reported is not None:
                v["reported"][chat_id] = reported
                v["mismatch"][chat_id] = mismatch
            if residual is not None:
                v["residual"][chat_id] = residual
        v["gap_open"] = {c: 0 for c in tracked} | {r[0]: 1 for r in conn.execute(_Q_GAP_OPEN, p).fetchall()}
        v["requests_pending"] = {c: 0 for c in tracked} | dict(conn.execute(_Q_REQUESTS_PENDING, p).fetchall())
        v["snapshot_age"] = {c: max(0.0, now - t.timestamp())
                             for c, t in conn.execute(_Q_SNAPSHOT, p).fetchall()}
        v["probe_queue"] = {c: 0 for c in tracked}
        v["probe_oldest"] = {c: 0.0 for c in tracked}
        for chat_id, n, oldest in conn.execute(_Q_PROBES, p).fetchall():
            v["probe_queue"][chat_id] = n
            v["probe_oldest"][chat_id] = max(0.0, now - oldest.timestamp())
        snap.lost_seconds = conn.execute(_Q_LOST).fetchone()[0]
        return snap

    def publish(self, snap: Snapshot) -> None:
        """Set the cached gauges from one refresh; drop the series of chats that are gone."""
        for key, gauge in CHAT_GAUGES.items():
            values = snap.values.get(key, {})
            for chat_id, value in values.items():
                gauge.labels(str(chat_id)).set(float(value))
            for chat_id in self._published[key] - set(values):
                _remove(gauge, str(chat_id))
            self._published[key] = set(values)
        for chat_id, (chat_type, title) in snap.chats.items():
            labels = (str(chat_id), chat_type, title)
            old = self._info.get(chat_id)
            if old is not None and old != labels:
                _remove(metrics.CHAT_INFO, *old)
            metrics.CHAT_INFO.labels(*labels).set(1)
            self._info[chat_id] = labels
        for chat_id in [c for c in self._info if c not in snap.chats]:
            _remove(metrics.CHAT_INFO, *self._info.pop(chat_id))
        for chat_id, n in snap.requests_aged.items():
            if chat_id in snap.chats:
                metrics.CHAT_JOIN_REQUESTS.labels(str(chat_id), "unknown").inc(n)
        if snap.lost_seconds > self._lost:
            metrics.LOST_SECONDS.inc(snap.lost_seconds - self._lost)
            self._lost = snap.lost_seconds


def _remove(gauge: Gauge, *labels: str) -> None:
    try:
        gauge.remove(*labels)
    except KeyError:
        pass


def finalize(conn: psycopg.Connection) -> int:
    """Fill net_events and straddled of the pending samples ingest has caught up past, and mark the chats of
    samples from an earlier UTC day dirty from that day; returns how many samples were finalized."""
    return conn.execute(_FINALIZE).fetchone()[0]


def make(ctx: JobContext) -> Job:
    return Sampler(ctx)
