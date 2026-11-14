"""Set arithmetic over the latest snapshot of each chat, and chat reference resolution."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Protocol

from . import db
from .db import ChatRow, SnapshotRow, UserRow


class AnalysisError(Exception):
    """A user-facing problem with the request or the data; shown as one message."""


class ChatLike(Protocol):
    id: int
    title: str
    username: str | None


def describe(chat: ChatLike) -> str:
    handle = f" @{chat.username}" if chat.username else ""
    return f"{chat.id}{handle} {chat.title!r}"


def resolve_chat(ref: str, chats: Iterable[ChatLike]) -> ChatLike:
    """Match a numeric id, an @username or an exact title; ambiguity is an error."""
    chats = list(chats)
    ref = ref.strip()
    matches: list[ChatLike] = []

    if ref.startswith("@"):
        name = ref[1:].lower()
        matches = [c for c in chats if c.username and c.username.lower() == name]
    else:
        try:
            number = int(ref)
        except ValueError:
            number = None
        if number is not None:
            # accept the marked id (-100123...) as well as the bare channel/group id
            ids = {number, -number, -(10**12 + number)}
            matches = [c for c in chats if c.id in ids]
        if not matches:
            matches = [c for c in chats if c.title == ref]

    if len(matches) == 1:
        return matches[0]
    if not matches:
        known = "; ".join(describe(c) for c in chats) or "none"
        raise AnalysisError(f"unknown chat {ref!r}; known chats: {known}")
    listed = "; ".join(describe(c) for c in matches)
    raise AnalysisError(f"ambiguous chat {ref!r}; candidates: {listed}; use the numeric id")


@dataclass(frozen=True)
class ChatSet:
    chat: ChatRow
    snapshot: SnapshotRow
    members: dict[int, UserRow]

    @property
    def ids(self) -> frozenset[int]:
        return frozenset(self.members)


def load_sets(
    conn: sqlite3.Connection,
    among: list[str] | None = None,
    include_bots: bool = False,
) -> list[ChatSet]:
    """Latest snapshot of every collected chat, or of the --among chats."""
    chats = db.list_chats(conn)
    snapshots = db.latest_snapshots(conn)
    if among:
        picked: list[ChatRow] = []
        for ref in among:
            chat = resolve_chat(ref, chats)
            if chat.id not in snapshots:
                raise AnalysisError(f"chat {describe(chat)} has no snapshot yet: run `tgstats collect`")
            if chat not in picked:
                picked.append(chat)
    else:
        picked = [c for c in chats if c.id in snapshots]
    if not picked:
        raise AnalysisError("no collected chats: run `tgstats collect` first")
    return [
        ChatSet(c, snapshots[c.id], db.snapshot_members(conn, snapshots[c.id].id, include_bots))
        for c in picked
    ]


@dataclass(frozen=True)
class UniqueResult:
    chat_set: ChatSet
    unique: frozenset[int]

    @property
    def total(self) -> int:
        return len(self.chat_set.members)

    @property
    def percent(self) -> float:
        return pct(len(self.unique), self.total)


def pct(part: int, whole: int) -> float:
    return 100.0 * part / whole if whole else 0.0


def unique_per_chat(sets: list[ChatSet]) -> list[UniqueResult]:
    """Members of each chat that are in none of the other compared chats."""
    results = []
    for s in sets:
        others: set[int] = set()
        for o in sets:
            if o is not s:
                others |= o.ids
        results.append(UniqueResult(s, s.ids - others))
    return results


@dataclass(frozen=True)
class Intersection:
    a: ChatSet
    b: ChatSet
    common: frozenset[int]

    @property
    def percent_of_a(self) -> float:
        return pct(len(self.common), len(self.a.members))

    @property
    def percent_of_b(self) -> float:
        return pct(len(self.common), len(self.b.members))

    @property
    def jaccard(self) -> float:
        union = len(self.a.ids | self.b.ids)
        return len(self.common) / union if union else 0.0


def intersect(a: ChatSet, b: ChatSet) -> Intersection:
    return Intersection(a, b, a.ids & b.ids)


def overlap_matrix(sets: list[ChatSet]) -> list[list[int]]:
    """Pairwise common-user counts; the diagonal holds each chat's own total."""
    return [[len(a.ids & b.ids) for b in sets] for a in sets]


def incomplete_sets(sets: Iterable[ChatSet]) -> list[ChatSet]:
    return [s for s in sets if not s.snapshot.complete]


def incomplete_warning(sets: Iterable[ChatSet], kind: str) -> str | None:
    """Warning text naming every compared chat with an incomplete snapshot, or None.

    kind is "unique", "intersect" or "overlap"; the consequences differ.
    """
    bad = incomplete_sets(sets)
    if not bad:
        return None
    names = "; ".join(
        f"{s.chat.title!r} (fetched {s.snapshot.fetched} of {s.snapshot.reported} reported)" for s in bad
    )
    lines = [f"WARNING: incomplete snapshot for {names}."]
    if kind == "unique":
        lines.append(
            "Unique counts can be wrong in either direction, even for a chat whose own snapshot is "
            "complete: a user missing from another chat's snapshot looks unique."
        )
    elif kind == "overlap":
        lines.append(
            "Absolute common-user counts are a lower bound: members Telegram did not return are not counted."
        )
    else:
        lines.append(
            "The absolute common-user count is a lower bound: members Telegram did not return are not "
            "counted. The percentages and the Jaccard index can be wrong in either direction, even if "
            "one chat's own snapshot is complete: a user missing from the other chat's snapshot "
            "looks like a non-member."
        )
    return " ".join(lines)
