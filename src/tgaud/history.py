"""Spell history: the per-chat locks every evidence writer takes, and the rebuild of tg.membership.

`build_spells(evidence)` is the state table of design.md 4 as a pure function of one (chat, user)'s evidence;
`rebuild_pairs(conn, pairs)` loads the evidence of many pairs set-based, replays it and writes only the
differences. Every row of the state table is one branch below, named in a comment (`# E4`).

Evidence and its time order (design.md 4 and the plan reviews):
  rank 0  events            tg.member_event at event_at, ties by update_id
  rank 1  sighting points   every point of every tg.sighting run at its exact page time (first_seen_at,
                            interior_seen_at, last_seen_at), with the run's joined_date as the hint; point i was
                            listed by the run's i-th finished snapshot and counts only if that snapshot counts
  rank 2  probe answers     tg.probe in/out at answered_at; ignored when an event of the pair has
                            sent_at <= event_at <= answered_at (the answer may describe either side)
  rank 3  absences          good, applied snapshots of the chat that no run of the user covers, at started_at
  rank 4  gap starts        tg.gap of the chat or of every chat, at lower(period)
  rank 5  migration (M2)    an alias of this chat as old_chat_id with moved_at NULL, at migrated_at
Ranks 3-5 are this module's choice where design.md is silent: an absence or a gap start after positive evidence of
the same instant ends in the same state either way, and an event dated the second of a migration happened
before the service message. Aliases with this chat as chat_id (M1, history moved in) open the E3 echo windows.
Snapshot evidence comes only from tg.sighting and tg.snapshot, never tg.snapshot_member, so pruning (R3) can
never change a replay. A snapshot is evidence for a user once it is graded good or partial (never rejected) and
applied to that user (reconciled_at set, or user_id <= applied_through_user_id), for every point of every run.
Runs are numbered over the chat's graded snapshots that are not late (migration 004): an ungraded snapshot has no
points yet, and a late one (graded after a later snapshot of its source) only has one-point runs of its own, read
by snapshot id, and gives no absences, so neither shifts the points of the runs around it.

Every rebuild records the E4, E10 and S3 cases its replay produced (tg.record_contradictions, migration 004).

Every time is compared as a UTC instant. Python orders, subtracts and compares datetimes that share a tzinfo by
wall time, so a zone with daylight saving (the session's, which psycopg loads timestamptz into) would misorder the
hour of a fall-back: the replay converts its inputs to UTC, and the rebuild reads in a UTC session.
"""

from __future__ import annotations

import bisect
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

GRACE = timedelta(minutes=10)  # S3: a sighting within G after an exit is stale, never a rejoin
ECHO_WINDOW = timedelta(hours=1)  # E3: join echoes in a migrated supergroup arrive within this after migrated_at
CHUNK = 10_000  # pairs per set of load queries
SMALL_INSERT = 50  # up to this many new spells per chunk go by executemany, more by COPY

IN_STATUSES = frozenset({"creator", "administrator", "member"})
UTC = timezone.utc


def lock_chats(conn: psycopg.Connection, chat_ids: Iterable[int]) -> list[int]:
    """Take a transaction-scoped advisory lock on each distinct chat id, in ascending order, and return them.

    Every writer of evidence or spells (ingest, reconciler, prober, CLI probe and forget-user, migrations of a
    chat's history) calls this in its writing transaction before it reads the evidence it will rebuild from,
    so two writers of one chat serialize and never rebuild from each other's half-written evidence. Ascending
    order (one statement per id, so the order is the call order) means two transactions locking overlapping
    sets cannot deadlock. The locks are released at commit or rollback; keep such transactions short (a
    reconcile chunk under 5 s), because ingest of that chat waits for them.

    Key space: the single-bigint form of pg_advisory_xact_lock is reserved for chat ids; tgaud's own locks use
    the two-int4 form (tgaud.db), which PostgreSQL keeps apart.
    """
    if conn.autocommit:
        raise ValueError("lock_chats needs a transaction (autocommit is on: the locks would end with the statement)")
    ids = sorted({int(c) for c in chat_ids})
    for chat_id in ids:
        conn.execute("SELECT pg_advisory_xact_lock(%s::bigint)", (chat_id,))
    return ids


def status_in(status: str | None, is_member: bool | None) -> bool:
    """tg.status_in: creator, administrator, member, or restricted with is_member (missing counts as in)."""
    return status in IN_STATUSES or (status == "restricted" and (is_member is None or is_member))


# ---------------------------------------------------------------------------------------------------------
# Evidence and spells
# ---------------------------------------------------------------------------------------------------------


class Event(NamedTuple):
    """One tg.member_event row (the columns the rules read)."""

    event_id: int
    event_at: datetime
    update_id: int
    old_status: str
    old_is_member: bool | None
    new_status: str
    new_is_member: bool | None
    actor_is_other: bool = False
    invite_link_id: int | None = None
    via_join_request: bool = False
    via_folder_link: bool = False


class SightingPoint(NamedTuple):
    """One point of a tg.sighting run: the exact page time and the run's Telegram join date."""

    seen_at: datetime
    joined_date: datetime | None = None
    first_snapshot_id: int = 0  # the run's key; with index only a tiebreak between identical points
    index: int = 0
    snapshot_id: int = 0  # the snapshot that listed this point (the source of an S3 case)


class ProbeAnswer(NamedTuple):
    """A tg.probe row: outcome in/out with answered_at (anything else is P4 and changes nothing)."""

    answered_at: datetime | None
    outcome: str
    status_seen: str | None = None
    sent_at: datetime | None = None


class Absence(NamedTuple):
    """A good snapshot of the chat, applied to this user, that no run of the user covers (A1-A3)."""

    started_at: datetime
    snapshot_id: int = 0


@dataclass
class Evidence:
    """Everything the state table reads for one (chat, user)."""

    chat_id: int
    user_id: int
    events: list[Event] = field(default_factory=list)
    sightings: list[SightingPoint] = field(default_factory=list)
    probes: list[ProbeAnswer] = field(default_factory=list)
    absences: list[Absence] = field(default_factory=list)
    gaps: list[datetime] = field(default_factory=list)  # gap starts
    migrations: list[datetime] = field(default_factory=list)  # M2: migrated_at, history not moved (old id)
    echo_from: list[datetime] = field(default_factory=list)  # M1: migrated_at of history moved into this chat


class Spell(NamedTuple):
    """A tg.membership row without membership_id, stay and rebuilt_at; the field order is the COPY order."""

    chat_id: int
    user_id: int
    start_source: str
    joined_at: datetime | None
    join_event_id: int | None
    joined_hint: datetime | None
    known_from: datetime
    join_route: str | None
    invite_link_id: int | None
    last_confirmed_at: datetime
    suspect_since: datetime | None
    end_source: str | None
    end_reason: str | None
    left_at: datetime | None
    left_event_id: int | None
    left_after: datetime | None
    left_before: datetime | None


SPELL_COLUMNS = Spell._fields


@dataclass(slots=True)
class _Stay:
    """A spell while the replay builds it."""

    start_source: str
    known_from: datetime
    last_confirmed_at: datetime
    joined_at: datetime | None = None
    join_event_id: int | None = None
    joined_hint: datetime | None = None
    join_route: str | None = None
    invite_link_id: int | None = None
    suspect_since: datetime | None = None
    end_source: str | None = None
    end_reason: str | None = None
    left_at: datetime | None = None
    left_event_id: int | None = None
    left_after: datetime | None = None
    left_before: datetime | None = None

    def confirm(self, t: datetime) -> None:
        """Positive evidence at t (E3, E8, S2, P1): L = t and the doubt is gone (it was raised at or before t)."""
        if t > self.last_confirmed_at:
            self.last_confirmed_at = t
        self.suspect_since = None

    def suspect(self, t: datetime) -> None:
        """Doubt from t on (A1, G1); an earlier doubt stays."""
        if self.suspect_since is None:
            self.suspect_since = t

    def close(self, source: str, reason: str, before: datetime, event: Event | None = None) -> None:
        """End the stay: exact at the event's time (event), else bounds L .. before."""
        self.end_source, self.end_reason, self.suspect_since = source, reason, None
        if event is not None:
            self.left_at = self.left_after = self.left_before = event.event_at
            self.left_event_id = event.event_id
        else:
            self.left_after, self.left_before = self.last_confirmed_at, before


def _join_route(e: Event) -> str:
    if e.invite_link_id is not None:
        return "link"
    if e.via_join_request:
        return "request"
    if e.via_folder_link:
        return "folder"
    if e.actor_is_other:
        return "added"
    return "unattributed"


def _exit_reason(e: Event) -> str:
    return "kicked" if e.new_status == "kicked" or e.actor_is_other else "left"


def _open_exact(e: Event) -> _Stay:
    return _Stay("event", e.event_at, e.event_at, joined_at=e.event_at, join_event_id=e.event_id,
                 join_route=_join_route(e), invite_link_id=e.invite_link_id)


_NO_HINT = datetime.min.replace(tzinfo=UTC)


def _utc(t: datetime | None) -> datetime | None:
    """t as a UTC datetime (None stays None); a naive time has no instant and is refused."""
    if t is None or t.tzinfo is UTC:
        return t
    if t.tzinfo is None:
        raise ValueError(f"naive datetime {t!r}: the history rules need aware times")
    return t.astimezone(UTC)


def _is_utc(t: datetime | None) -> bool:
    return t is None or t.tzinfo is UTC


def _in_utc(ev: Evidence) -> Evidence:
    """`ev` with every time in UTC, so ordering, intervals and equality are on instants (see the module doc)."""
    return Evidence(
        ev.chat_id, ev.user_id,
        [e if e.event_at.tzinfo is UTC else e._replace(event_at=_utc(e.event_at)) for e in ev.events],
        [s if _is_utc(s.seen_at) and _is_utc(s.joined_date)
         else s._replace(seen_at=_utc(s.seen_at), joined_date=_utc(s.joined_date)) for s in ev.sightings],
        [p if _is_utc(p.answered_at) and _is_utc(p.sent_at)
         else p._replace(answered_at=_utc(p.answered_at), sent_at=_utc(p.sent_at)) for p in ev.probes],
        [a if a.started_at.tzinfo is UTC else a._replace(started_at=_utc(a.started_at)) for a in ev.absences],
        [_utc(t) for t in ev.gaps], [_utc(t) for t in ev.migrations], [_utc(t) for t in ev.echo_from])

Trace = list[tuple[str, datetime | None]]
Cases = list[tuple[str, datetime, int]]  # (E4 | E10 | S3, evidence time, event_id or snapshot_id)


def _probe_is_late(p: ProbeAnswer, event_times: Sequence[datetime]) -> bool:
    """An event of the pair with sent_at <= event_at <= answered_at (exact, inclusive): the answer is ignored."""
    lo = p.sent_at if p.sent_at is not None else p.answered_at
    i = bisect.bisect_left(event_times, lo)
    return i < len(event_times) and event_times[i] <= p.answered_at


def replay(ev: Evidence, trace: Trace | None = None, cases: Cases | None = None) -> list[Spell]:
    """The state table of design.md 4 for one (chat, user); appends (rule, time) of every branch to `trace`, and
    (rule, time, source id) of every E4, E10 and S3 to `cases`.

    Times may be in any zone (aware); they are compared, and the spells returned, in UTC.
    """
    ev = _in_utc(ev)
    note = trace.append if trace is not None else None
    event_times = sorted(e.event_at for e in ev.events)
    items: list[tuple] = []
    for e in ev.events:
        items.append(((e.event_at, 0, e.update_id), e))
    for s in ev.sightings:
        hint = s.joined_date
        items.append(((s.seen_at, 1, hint is None, hint or _NO_HINT, s.first_snapshot_id, s.index), s))
    for p in ev.probes:
        if p.outcome not in ("in", "out") or p.answered_at is None:
            if note:
                note(("P4", p.answered_at))  # P4: failed or unanswered: unknown, nothing changes
            continue
        if _probe_is_late(p, event_times):
            if note:
                note(("P-late", p.answered_at))  # plan review 3: an event inside [sent_at, answered_at]
            continue
        items.append(((p.answered_at, 2, p.outcome, p.status_seen or ""), p))
    for a in ev.absences:
        items.append(((a.started_at, 3, a.snapshot_id), a))
    for g in ev.gaps:
        items.append(((g, 4), None))
    for m in ev.migrations:
        items.append(((m, 5), m))
    items.sort(key=lambda x: x[0])

    stays: list[_Stay] = []
    for key, item in items:
        t, rank = key[0], key[1]
        last = stays[-1] if stays else None
        is_open = last is not None and last.end_source is None

        if rank == 0:
            e: Event = item
            old_in = status_in(e.old_status, e.old_is_member)
            new_in = status_in(e.new_status, e.new_is_member)
            if not old_in and new_in:
                if note and e.old_status == "restricted":
                    note(("R1", t))  # R1: restricted is_member false -> true is a join (E1/E2/E4 rules)
                if is_open:
                    if any(m <= t <= m + ECHO_WINDOW and last.known_from < m for m in ev.echo_from):
                        last.confirm(t)  # E3: migration echo of a stay begun before the migration (M1 moved it)
                        if note:
                            note(("E3", t))
                        continue
                    last.close("inferred", "gone", t)  # E4: missed leave, unknown end before this join
                    stays.append(_open_exact(e))
                    if note:
                        note(("E4", t))
                    if cases is not None:
                        cases.append(("E4", t, e.event_id))
                else:
                    stays.append(_open_exact(e))  # E1 (none) / E2 (closed: a rejoin)
                    if note:
                        note(("E1" if last is None else "E2", t))
            elif old_in and not new_in:
                if is_open:
                    last.close("event", _exit_reason(e), t, e)  # E5: exact exit
                    if note:
                        note(("E5", t))
                else:
                    # E6 (none) / E7 (closed): a left-censored stay, start unknown, exact exit
                    stay = _Stay("inferred", t, t)
                    stay.close("event", _exit_reason(e), t, e)
                    stays.append(stay)
                    if note:
                        note(("E6" if last is None else "E7", t))
            elif old_in:
                if is_open:
                    last.confirm(t)  # E8: role or restriction change: L = e
                    if note:
                        note(("E8", t))
                else:
                    stays.append(_Stay("inferred", t, t))  # E9: missed join
                    if note:
                        note(("E9", t))
            else:
                if is_open:
                    last.close("inferred", "gone", t)  # E10: the old status says out
                    if note:
                        note(("E10", t))
                    if cases is not None:
                        cases.append(("E10", t, e.event_id))
                elif note:
                    note(("E11", t))  # E11: ban from out, unban: moderation only

        elif rank == 1:
            s: SightingPoint = item
            if last is None:
                stays.append(_Stay("snapshot", t, t, joined_hint=s.joined_date))  # S1: seeded by a snapshot
                if note:
                    note(("S1", t))
                    if event_times and event_times[-1] > t:
                        note(("S5", t))  # S5: older than events of the pair; later exits/joins close it (E5/E4)
            elif is_open:
                last.confirm(t)  # S2: confirm; fill an empty hint
                if last.joined_hint is None:
                    last.joined_hint = s.joined_date
                if note:
                    note(("S2", t))
            elif t < last.left_before + GRACE:
                if note:
                    note(("S3", t))  # S3: stale listing within G after the end: ignored (conflict probe)
                if cases is not None:
                    cases.append(("S3", t, s.snapshot_id))
            else:
                stays.append(_Stay("snapshot", t, t, joined_hint=s.joined_date))  # S4: missed rejoin
                if note:
                    note(("S4", t))

        elif rank == 2:
            p: ProbeAnswer = item
            if p.outcome == "in":
                if is_open:
                    last.confirm(t)  # P1
                    if note:
                        note(("P1", t))
                else:
                    stays.append(_Stay("probe", t, t))  # P2: missed join
                    if note:
                        note(("P2", t))
            elif is_open:
                last.close("probe", "kicked" if p.status_seen == "kicked" else "gone", t)  # P3
                if note:
                    note(("P3", t))
            elif note:
                note(("P3-out", t))  # out while none/closed: agrees with the projection

        elif rank == 3:
            if not is_open:
                if note:
                    note(("A3", t))  # A3: absence never closes or opens anything
            elif last.last_confirmed_at < t:
                last.suspect(t)  # A1: absent from a good snapshot after the last positive evidence
                if note:
                    note(("A1", t))
            elif note:
                note(("A2", t))  # A2: seen or joined during the listing

        elif rank == 4:
            if is_open and last.last_confirmed_at < t:
                last.suspect(t)  # G1: a gap after the last positive evidence
                if note:
                    note(("G1", t))
            elif note:
                note(("G1-none", t))

        else:
            if is_open:
                last.close("migration", "migrated", t)  # M2: censoring at the migration, never an exit
                if note:
                    note(("M2", t))
            elif note:
                note(("M2-none", t))

    return [Spell(ev.chat_id, ev.user_id, s.start_source, s.joined_at, s.join_event_id, s.joined_hint,
                  s.known_from, s.join_route, s.invite_link_id, s.last_confirmed_at, s.suspect_since,
                  s.end_source, s.end_reason, s.left_at, s.left_event_id, s.left_after, s.left_before)
            for s in stays]


def build_spells(evidence: Evidence, cases: Cases | None = None) -> list[Spell]:
    """The spells of one (chat, user) from all its evidence, in time order (pure; no database); appends the E4,
    E10 and S3 cases to `cases`."""
    return replay(evidence, cases=cases)


# ---------------------------------------------------------------------------------------------------------
# Rebuild
# ---------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RebuildResult:
    """What a rebuild changed: spells opened, closed and otherwise changed, over all pairs given; besides (not
    compared) the E4/E10/S3 cases the replay produced, as (chat_id, user_id, rule, time, source id), and the conflict
    probes their recording queued or raised."""

    opened: int = 0
    closed: int = 0
    changed: int = 0
    cases: tuple[tuple[int, int, str, datetime, int], ...] = field(default=(), compare=False)
    conflict_probes: int = field(default=0, compare=False)

    def __add__(self, other: "RebuildResult") -> "RebuildResult":
        return RebuildResult(self.opened + other.opened, self.closed + other.closed, self.changed + other.changed,
                             self.cases + other.cases, self.conflict_probes + other.conflict_probes)


_PAIRS = "unnest(%(chats)s::bigint[], %(users)s::bigint[]) AS p(chat_id, user_id)"

_Q_EVENTS = f"""
SELECT e.chat_id, e.user_id, e.event_id, e.event_at, e.update_id, e.old_status, e.old_is_member, e.new_status,
       e.new_is_member, e.actor_is_other, e.invite_link_id, e.via_join_request, e.via_folder_link
FROM {_PAIRS} JOIN tg.member_event e ON e.chat_id = p.chat_id AND e.user_id = p.user_id"""

_Q_SIGHTINGS = f"""
SELECT s.chat_id, s.user_id, s.first_snapshot_id, fs.started_at, s.last_snapshot_id, ls.started_at,
       s.first_seen_at, s.interior_seen_at, s.last_seen_at, s.times_seen, s.joined_date
FROM {_PAIRS} JOIN tg.sighting s ON s.chat_id = p.chat_id AND s.user_id = p.user_id
JOIN tg.snapshot fs ON fs.snapshot_id = s.first_snapshot_id
JOIN tg.snapshot ls ON ls.snapshot_id = s.last_snapshot_id"""

_Q_PROBES = f"""
SELECT pr.chat_id, pr.user_id, pr.answered_at, pr.outcome, pr.status_seen, pr.sent_at
FROM {_PAIRS} JOIN tg.probe pr ON pr.chat_id = p.chat_id AND pr.user_id = p.user_id
WHERE pr.outcome IN ('in', 'out')"""

_Q_SPELLS = f"""
SELECT m.membership_id, {", ".join("m." + c for c in SPELL_COLUMNS)}
FROM {_PAIRS} JOIN tg.membership m ON m.chat_id = p.chat_id AND m.user_id = p.user_id"""

# finished snapshots of the chats (the sources of run points; the good ones also give absences); whether one counts
# for a user is decided per user (the reconciler's cursor)
_Q_SNAPSHOTS = """
SELECT s.chat_id, s.started_at, s.snapshot_id, s.quality, s.applied_through_user_id, s.reconciled_at IS NOT NULL,
       s.late
FROM tg.snapshot s WHERE s.chat_id = ANY(%(cs)s) AND s.finished_at IS NOT NULL"""

_Q_GAPS = """
SELECT g.chat_id, lower(g.period) FROM tg.gap g
WHERE (g.chat_id = ANY(%(cs)s) OR g.chat_id IS NULL) AND NOT isempty(g.period) AND NOT lower_inf(g.period)"""

_Q_ALIASES = """
SELECT a.old_chat_id, a.chat_id, a.migrated_at, a.moved_at IS NULL FROM tg.chat_alias a
WHERE a.chat_id = ANY(%(cs)s) OR a.old_chat_id = ANY(%(cs)s)"""

_INSERT = (f"INSERT INTO tg.membership ({', '.join(SPELL_COLUMNS)}) "
           f"VALUES ({', '.join(['%s'] * len(SPELL_COLUMNS))})")
_COPY = f"COPY tg.membership ({', '.join(SPELL_COLUMNS)}) FROM STDIN"


@dataclass
class _ChatContext:
    fin_index: dict[int, int] = field(default_factory=dict)  # snapshot_id -> position among numbered, by key
    fin_meta: list[tuple[str | None, int | None, bool]] = field(default_factory=list)  # (quality, through, reconciled)
    fin_ids: list[int] = field(default_factory=list)  # snapshot_id by position
    late: dict[int, tuple[str | None, int | None, bool]] = field(default_factory=dict)  # late snapshot_id -> meta
    snap_keys: list[tuple[datetime, int]] = field(default_factory=list)  # good snapshots, sorted
    snap_starts: list[datetime] = field(default_factory=list)  # their started_at, for bisect
    snap_meta: list[tuple[int | None, bool]] = field(default_factory=list)  # (applied_through, reconciled)
    gaps: list[datetime] = field(default_factory=list)  # sorted gap starts (this chat and every chat)
    migrations: list[datetime] = field(default_factory=list)  # M2
    echo_from: list[datetime] = field(default_factory=list)  # M1


def _window(times: list[datetime], lo: datetime, hi: datetime, keep: Callable[[int], bool]) -> Iterable[int]:
    """Indexes of sorted `times` in [lo, hi] that `keep`, then the first kept one after hi.

    Exact for absences and gap starts: before the pair's first event, point or probe nothing is open; after its
    last event, point, probe or migration nothing positive follows, so only the first doubt can change a stay.
    """
    i = bisect.bisect_left(times, lo)
    n = len(times)
    while i < n and times[i] <= hi:
        if keep(i):
            yield i
        i += 1
    while i < n:
        if keep(i):
            yield i
            return
        i += 1


def _counts_for(meta: tuple[str | None, int | None, bool], user_id: int) -> bool:
    """A snapshot is evidence for a user once graded good or partial (never rejected) and applied to that user."""
    quality, through, reconciled = meta
    return quality in ("good", "partial") and (reconciled or (through is not None and user_id <= through))


def _evidence(chat_id: int, user_id: int, ctx: _ChatContext, events: list[Event], runs: list[tuple],
              probes: list[ProbeAnswer]) -> Evidence:
    points: list[SightingPoint] = []
    covered: list[tuple[tuple[datetime, int], tuple[datetime, int]]] = []
    for (first_id, first_started, last_id, last_started, first_seen, interior, last_seen, times_seen,
         joined_date) in runs:
        if first_id in ctx.late:
            # a late listing's run is one point, read by its snapshot id (the reconciler never extends it)
            if _counts_for(ctx.late[first_id], user_id):
                points.append(SightingPoint(first_seen, joined_date, first_id, 0, first_id))
            continue
        times = [first_seen] if times_seen == 1 else [first_seen, *interior, last_seen]
        lo, hi = ctx.fin_index.get(first_id), ctx.fin_index.get(last_id)
        if lo is not None and hi is not None and hi - lo + 1 == len(times):
            sources: Sequence[int | None] = range(lo, hi + 1)  # the 003 contract: every snapshot of the range
        else:
            # the run does not match the chat's numbered snapshots (a writer broke the 003 contract): only its ends
            # can be attributed, so its interior points are not evidence
            times = times[:1] if times_seen == 1 else [first_seen, last_seen]
            sources = [lo] if times_seen == 1 else [lo, hi]
        kept = [(t, k) for t, k in zip(times, sources) if k is not None and _counts_for(ctx.fin_meta[k], user_id)]
        points.extend(SightingPoint(t, joined_date, first_id, i, ctx.fin_ids[k]) for i, (t, k) in enumerate(kept))
        covered.append(((first_started, first_id), (last_started, last_id)))
    ev = Evidence(chat_id, user_id, events, points, probes, migrations=ctx.migrations, echo_from=ctx.echo_from)
    opening = [e.event_at for e in events] + [p.seen_at for p in points] + [p.answered_at for p in probes]
    if not opening:
        return ev  # nothing can open a stay: absences and gaps change nothing
    lo = min(opening)
    hi = max(opening + ctx.migrations)

    def applied_and_absent(i: int) -> bool:
        through, reconciled = ctx.snap_meta[i]
        if not reconciled and (through is None or user_id > through):
            return False  # not applied to this user yet (mid-reconcile): never an absence
        key = ctx.snap_keys[i]
        return not any(a <= key <= b for a, b in covered)

    ev.absences = [Absence(*ctx.snap_keys[i]) for i in _window(ctx.snap_starts, lo, hi, applied_and_absent)]
    ev.gaps = [ctx.gaps[i] for i in _window(ctx.gaps, lo, hi, lambda i: True)]
    return ev


def _load(conn: psycopg.Connection, pairs: list[tuple[int, int]]) -> list[list[tuple]]:
    chats = [c for c, _ in pairs]
    users = [u for _, u in pairs]
    cs = sorted(set(chats))
    params = {"chats": chats, "users": users}
    curs = [conn.cursor(row_factory=tuple_row) for _ in range(7)]
    with conn.pipeline():
        curs[0].execute(_Q_EVENTS, params)
        curs[1].execute(_Q_SIGHTINGS, params)
        curs[2].execute(_Q_PROBES, params)
        curs[3].execute(_Q_SPELLS, params)
        curs[4].execute(_Q_SNAPSHOTS, {"cs": cs})
        curs[5].execute(_Q_GAPS, {"cs": cs})
        curs[6].execute(_Q_ALIASES, {"cs": cs})
    rows = [c.fetchall() for c in curs]
    for c in curs:
        c.close()
    return rows


def _load_chunk(conn: psycopg.Connection, pairs: list[tuple[int, int]]
                ) -> tuple[dict[tuple[int, int], Evidence], dict[tuple[int, int], list[tuple[int, Spell]]]]:
    """The evidence and the stored spells (with membership_id) of each pair of one chunk."""
    ev_rows, run_rows, probe_rows, spell_rows, snap_rows, gap_rows, alias_rows = _load(conn, pairs)

    ctxs: dict[int, _ChatContext] = defaultdict(_ChatContext)
    global_gaps: list[datetime] = []
    for chat_id, started_at, snapshot_id, quality, through, reconciled, late in sorted(
            snap_rows, key=lambda r: (r[0], r[1], r[2])):
        ctx = ctxs[chat_id]
        if quality is None:
            continue  # not graded yet: no points, no absences, and no place in the numbering
        if late:
            ctx.late[snapshot_id] = (quality, through, reconciled)
            continue
        ctx.fin_index[snapshot_id] = len(ctx.fin_meta)
        ctx.fin_meta.append((quality, through, reconciled))
        ctx.fin_ids.append(snapshot_id)
        if quality == "good":
            ctx.snap_keys.append((started_at, snapshot_id))
            ctx.snap_starts.append(started_at)
            ctx.snap_meta.append((through, reconciled))
    per_chat_gaps: dict[int, list[datetime]] = defaultdict(list)
    for chat_id, start in gap_rows:
        (global_gaps if chat_id is None else per_chat_gaps[chat_id]).append(start)
    for old_chat_id, chat_id, migrated_at, not_moved in alias_rows:
        ctxs[chat_id].echo_from.append(migrated_at)  # M1: history moved (or to be moved) into chat_id
        if not_moved:
            ctxs[old_chat_id].migrations.append(migrated_at)  # M2: the old id keeps its history, closed
    for chat_id in {c for c, _ in pairs}:
        ctx = ctxs[chat_id]
        ctx.gaps = sorted(per_chat_gaps.get(chat_id, []) + global_gaps)
        ctx.migrations.sort()
        ctx.echo_from.sort()

    events: dict[tuple[int, int], list[Event]] = defaultdict(list)
    for r in ev_rows:
        events[(r[0], r[1])].append(Event(*r[2:]))
    runs: dict[tuple[int, int], list[tuple]] = defaultdict(list)
    for r in run_rows:
        runs[(r[0], r[1])].append(r[2:])
    probes: dict[tuple[int, int], list[ProbeAnswer]] = defaultdict(list)
    for chat_id, user_id, answered_at, outcome, status_seen, sent_at in probe_rows:
        probes[(chat_id, user_id)].append(ProbeAnswer(answered_at, outcome, status_seen, sent_at))
    stored: dict[tuple[int, int], list[tuple[int, Spell]]] = defaultdict(list)
    for r in spell_rows:
        sp = Spell(*r[1:])
        stored[(sp.chat_id, sp.user_id)].append((r[0], sp))

    evidence = {pair: _evidence(pair[0], pair[1], ctxs[pair[0]], events.get(pair, []), runs.get(pair, []),
                                probes.get(pair, [])) for pair in pairs}
    return evidence, stored


@contextmanager
def _utc_session(conn: psycopg.Connection) -> Iterator[None]:
    """Read in UTC, so psycopg loads every timestamptz as a UTC datetime whatever the caller's zone.

    The zone is switched with set_config(..., is_local) and put back afterwards (the caller's SET or SET LOCAL
    value); after a database error the transaction is aborted and its rollback restores the zone.
    """
    zone = conn.info.parameter_status("TimeZone")
    if zone in (None, "UTC"):  # psycopg loads UTC already
        yield
        return
    conn.execute("SELECT set_config('TimeZone', 'UTC', true)")
    try:
        yield
    finally:
        if conn.info.transaction_status == TransactionStatus.INTRANS:
            conn.execute("SELECT set_config('TimeZone', %s, true)", (zone,))


def load_evidence(conn: psycopg.Connection, pairs: Iterable[tuple[int, int]]) -> dict[tuple[int, int], Evidence]:
    """The evidence rebuild_pairs replays for each pair, as `Evidence` in UTC. Absences and gap starts are bounded
    to the ones that can change a stay, so its absences are not a per-snapshot list. Takes no locks: inside a
    writer's transaction, call lock_chats first."""
    todo = sorted({(int(c), int(u)) for c, u in pairs})
    out: dict[tuple[int, int], Evidence] = {}
    with _utc_session(conn):
        for i in range(0, len(todo), CHUNK):
            out.update(_load_chunk(conn, todo[i:i + CHUNK])[0])
    return out


def _rebuild_chunk(conn: psycopg.Connection, pairs: list[tuple[int, int]]) -> RebuildResult:
    evidence, stored = _load_chunk(conn, pairs)
    delete_ids: list[int] = []
    inserts: list[Spell] = []
    dirty: dict[int, datetime] = {}
    opened = closed = changed = 0
    found: list[tuple[int, int, str, datetime, int]] = []
    for pair in pairs:
        chat_id = pair[0]
        pair_cases: Cases = []
        new = build_spells(evidence[pair], pair_cases)
        found.extend((chat_id, pair[1], rule, t, source) for rule, t, source in pair_cases)
        old = stored.get(pair, [])
        if len(new) == len(old) and Counter(new) == Counter(sp for _, sp in old):
            continue
        want = Counter(new)
        gone: list[Spell] = []
        for membership_id, sp in old:
            if want[sp] > 0:
                want[sp] -= 1
            else:
                delete_ids.append(membership_id)
                gone.append(sp)
        added = []
        for sp in new:
            if want[sp] > 0:
                want[sp] -= 1
                added.append(sp)
        inserts.extend(added)
        by_start: dict[tuple, list[Spell]] = defaultdict(list)
        for sp in gone:
            by_start[(sp.start_source, sp.known_from, sp.join_event_id)].append(sp)
        for sp in added:
            before = by_start.get((sp.start_source, sp.known_from, sp.join_event_id))
            if not before:
                opened += 1
            elif before.pop().end_source is None and sp.end_source is not None:
                closed += 1
            else:
                changed += 1
        changed += sum(len(v) for v in by_start.values())
        for sp in (*gone, *added):
            earliest = min(t for t in (sp.known_from, sp.left_after, sp.left_before, sp.suspect_since)
                           if t is not None)
            if chat_id not in dirty or earliest < dirty[chat_id]:
                dirty[chat_id] = earliest

    with conn.cursor(row_factory=tuple_row) as cur:
        if delete_ids:
            cur.execute("DELETE FROM tg.membership WHERE membership_id = ANY(%s)", (delete_ids,))
        if len(inserts) > SMALL_INSERT:
            with cur.copy(_COPY) as cp:
                for sp in inserts:
                    cp.write_row(sp)
        elif inserts:
            cur.executemany(_INSERT, inserts)
        if dirty:
            chats = sorted(dirty)
            days: list[date] = [dirty[c].astimezone(timezone.utc).date() for c in chats]
            cur.execute("SELECT tg.mark_dirty(c, d) FROM unnest(%s::bigint[], %s::date[]) AS x(c, d)", (chats, days))
        probes = 0
        if found:
            # E4/E10/S3 once per source evidence: new S3 queue conflict probes, the chat's windows are checked
            probes = cur.execute("SELECT tg.record_contradictions(%s::bigint[], %s::bigint[], %s::text[], "
                                 "%s::timestamptz[], %s::bigint[])", [list(col) for col in zip(*found)]).fetchone()[0]
    return RebuildResult(opened, closed, changed, tuple(found), probes)


def rebuild_pairs(conn: psycopg.Connection, pairs: Iterable[tuple[int, int]]) -> RebuildResult:
    """Recompute tg.membership for each (chat_id, user_id) from all evidence by the state table of design.md 4.

    - Runs inside the caller's transaction (READ COMMITTED, the default) and never commits or rolls back. It
      takes lock_chats() for every chat of `pairs` before it reads anything (a no-op for locks the caller holds;
      callers should take them first themselves, in one call, so the order stays ascending). It reads and
      compares in UTC: a session in another zone is switched to UTC for the call and switched back after it.
    - Loads the evidence of CHUNK pairs at a time with one pipelined set of queries, replays each pair
      (build_spells) and writes only the differences: changed spells deleted, new ones inserted (COPY for big
      chunks), then tg.mark_dirty per chat with the UTC day of the earliest known_from, left_after, left_before
      or suspect_since of every deleted or inserted spell. A pair whose spells are unchanged is not written and
      marks nothing, so a second call changes nothing.
    - Records the E4, E10 and S3 cases the replay produced, of every pair, changed or not (an S3 never changes a
      spell), with tg.record_contradictions: each case once per source evidence, a conflict probe for a new S3,
      and a contradiction gap where a chat passes 10 cases within 60 minutes. They are in the result's `cases`.
    - On any exception the caller must roll back (or let the exception reach code that does): a Python error in
      a later chunk leaves the earlier chunks written in a transaction that could still commit.
    - Returns the counts: `opened` = new spells with no stored spell of the same start (start_source,
      known_from, join_event_id); `closed` = such a stored spell was open and its new version is closed;
      `changed` = every other spell rewritten or deleted.
    """
    todo = sorted({(int(c), int(u)) for c, u in pairs})
    if not todo:
        return RebuildResult()
    lock_chats(conn, (c for c, _ in todo))
    result = RebuildResult()
    with _utc_session(conn):
        for i in range(0, len(todo), CHUNK):
            result += _rebuild_chunk(conn, todo[i:i + CHUNK])
    return result
