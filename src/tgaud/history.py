"""Spell history: the per-chat locks every evidence writer takes, and the rebuild interface.

`lock_chats` is real (core task). `rebuild_pairs` is a stub here: the history task implements the state table
of design.md 4 behind this exact signature.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import psycopg


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


@dataclass(frozen=True)
class RebuildResult:
    """What a rebuild changed: spells opened, closed and otherwise changed, over all pairs given."""

    opened: int = 0
    closed: int = 0
    changed: int = 0

    def __add__(self, other: "RebuildResult") -> "RebuildResult":
        return RebuildResult(self.opened + other.opened, self.closed + other.closed, self.changed + other.changed)


def rebuild_pairs(conn: psycopg.Connection, pairs: Iterable[tuple[int, int]]) -> RebuildResult:
    """Recompute tg.membership for each (chat_id, user_id) from all evidence by the state table of design.md 4.

    Contract (implemented by the history task):
      - runs inside the caller's transaction and never commits; the caller holds lock_chats() for every chat
        of `pairs`;
      - deterministic and idempotent: the same evidence gives the same spells, a second call changes nothing;
      - reads the evidence (tg.member_event in (event_at, update_id) order, tg.sighting, good snapshots'
        absences, tg.probe answers, tg.gap, tg.chat_alias) and uses member_event.actor_is_other, never actor_id;
      - upserts tg.rollup_dirty through tg.mark_dirty with the earliest affected day (the earliest of the old
        and new known_from, left_after and left_before of every changed spell);
      - returns how many spells were opened, closed and changed.

    This core stub does nothing and returns an empty RebuildResult.
    """
    for _pair in pairs:
        pass
    return RebuildResult()
