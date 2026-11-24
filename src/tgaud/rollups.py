"""R1 (stats.chat_day) and R2 (the audience rollups) as Python: the SQL of queries.sql, the branch chosen here.

queries.sql stays the reference (psql runs it whole, `\\gset` + `\\if` and all) and the only copy of the SQL:
the statements below are taken from its parsed R1 and R2 blocks (tgaud.sql), so a fix to the SQL is one edit.
What Python does instead of psql is the control flow:

    refresh_chat_days(conn, chats, as_of)   R1 for these chats, in the caller's transaction (never commits):
                                            the set-based statement when a target day is more than FULL_DAYS
                                            back, else the per-chat index-scan statement; both give the same rows
    plan_chat_days(conn, as_of)             which chats need how much (the job's cycle plan)
    refresh_audience(conn, as_of)           R2 in the caller's REPEATABLE READ transaction: the materialized
                                            view refresh is CONCURRENTLY once it is populated, plain before

Values are bound as parameters by tgaud.sql (`Statement.bind`); nothing is formatted into SQL text.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from dataclasses import dataclass

import psycopg

from tgaud import sql

FULL_DAYS = 7  # a target day further back than this is recomputed by the set-based statement
UTC = dt.timezone.utc
_TXN_CONTROL = {"BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT"}


class QueryShapeError(RuntimeError):
    """queries.sql R1 or R2 no longer has the statement layout this module runs."""


def _flat(block_id: str, qf: sql.QueryFile) -> list[sql.Statement | sql.If]:
    (block,) = qf.blocks_for(block_id)
    return [i for i in block.items
            if not (isinstance(i, sql.Statement) and i.head.split(" ", 1)[0] in _TXN_CONTROL)]


def _expect(item: object, kind: type, *heads: str) -> object:
    if not isinstance(item, kind) or (heads and item.head not in heads):
        raise QueryShapeError(f"queries.sql: expected {kind.__name__} {heads or ''}, found "
                              f"{getattr(item, 'head', type(item).__name__)}")
    return item


@dataclass(frozen=True)
class R1:
    """The statements of block R1, by role."""

    set_work_mem: sql.Statement
    target: sql.Statement  # CREATE TEMP TABLE chat_day_target (chat_id, from_day, from_ts, dirty_version)
    analyze: sql.Statement
    delete: sql.Statement
    insert_full: sql.Statement
    insert_chat: sql.Statement
    release: sql.Statement  # DELETE the markers whose version is unchanged


def r1(qf: sql.QueryFile | None = None) -> R1:
    qf = qf or sql.load()
    items = _flat("R1", qf)
    if len(items) != 8:
        raise QueryShapeError(f"queries.sql R1 has {len(items)} items, expected 8")
    choice = _expect(items[5], sql.If)
    if choice.var != "chat_day_full" or len(choice.then) != 1 or len(choice.otherwise) != 1:
        raise QueryShapeError("queries.sql R1: the \\if :chat_day_full branch changed")
    out = R1(_expect(items[0], sql.Statement, "SET LOCAL"), _expect(items[1], sql.Statement, "CREATE TEMP"),
             _expect(items[2], sql.Statement, "ANALYZE CHAT_DAY_TARGET"),
             _expect(items[3], sql.Statement, "DELETE FROM"),
             _expect(choice.then[0], sql.Statement, "INSERT INTO"),
             _expect(choice.otherwise[0], sql.Statement, "INSERT INTO"),
             _expect(items[6], sql.Statement, "DELETE FROM"))
    _expect(items[4], sql.Statement)  # the \gset that picks the branch in psql; Python picks it itself
    if not items[4].gset:
        raise QueryShapeError("queries.sql R1: statement 5 should be the \\gset branch choice")
    _expect(items[7], sql.Statement, "INSERT INTO")  # the rollup_run row: written per cycle by the job
    return out


def _utc_day(as_of: dt.datetime) -> dt.date:
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    return as_of.astimezone(UTC).date()


def _run(conn: psycopg.Connection, stmt: sql.Statement, vals: dict[str, str]) -> psycopg.Cursor:
    return conn.execute(stmt.query(), stmt.bind(vals))


@dataclass(frozen=True)
class ChatDayResult:
    mode: str  # full | chat
    chats: int
    rows: int  # chat_day rows written
    markers_cleared: int
    from_days: dict[int, dt.date]


def refresh_chat_days(conn: psycopg.Connection, chats: Sequence[int], as_of: dt.datetime, *, mode: str = "auto",
                      qf: sql.QueryFile | None = None) -> ChatDayResult:
    """R1 for `chats` (tracked ones; others are ignored), in the caller's transaction: recompute each chat from
    min(its dirty day, as_of's day - 2) to as_of's day and clear the dirty markers it read, unless a writer
    changed them meanwhile. `mode`: auto picks the set-based statement when any target day is more than
    FULL_DAYS back; 'full' and 'chat' force one (the two give the same rows)."""
    if mode not in ("auto", "full", "chat"):
        raise ValueError(f"mode must be auto, full or chat, not {mode!r}")
    if not chats:
        return ChatDayResult("chat", 0, 0, 0, {})
    qf = qf or sql.load()
    q = r1(qf)
    vals = sql.values(conn, qf, as_of=as_of, scope=[int(c) for c in chats])
    _run(conn, q.set_work_mem, vals)
    _run(conn, q.target, vals)
    _run(conn, q.analyze, vals)
    targets = {int(c): d for c, d in conn.execute("SELECT chat_id, from_day FROM chat_day_target")}
    _run(conn, q.delete, vals)
    today = _utc_day(as_of)
    full = mode == "full" or (mode == "auto" and any((today - d).days > FULL_DAYS for d in targets.values()))
    rows = _run(conn, q.insert_full if full else q.insert_chat, vals).rowcount
    cleared = _run(conn, q.release, vals).rowcount
    conn.execute("DROP TABLE chat_day_target")  # not only at commit: a caller may run several passes in one transaction
    return ChatDayResult("full" if full else "chat", len(targets), rows, cleared, targets)


def plan_chat_days(conn: psycopg.Connection, as_of: dt.datetime, *,
                   qf: sql.QueryFile | None = None) -> dict[int, dt.date]:
    """The from_day R1 would recompute for every tracked chat now (target statement of R1, then dropped).
    Reads only; the caller's transaction is left open."""
    qf = qf or sql.load()
    q = r1(qf)
    vals = sql.values(conn, qf, as_of=as_of)
    _run(conn, q.target, vals)
    out = {int(c): d for c, d in conn.execute("SELECT chat_id, from_day FROM chat_day_target ORDER BY chat_id")}
    conn.execute("DROP TABLE chat_day_target")
    return out


@dataclass(frozen=True)
class R2:
    set_work_mem: sql.Statement
    populated: sql.Statement  # SELECT ispopulated AS user_chats_populated (the \gset)
    refresh_concurrently: sql.Statement
    refresh_plain: sql.Statement
    publish: tuple[sql.Statement, ...]  # DELETE/INSERT of chat_set, overlap, unique, chats_per_user, the run row


def r2(qf: sql.QueryFile | None = None) -> R2:
    qf = qf or sql.load()
    items = _flat("R2", qf)
    if len(items) != 12:
        raise QueryShapeError(f"queries.sql R2 has {len(items)} items, expected 12 (the branch counts as one)")
    choice = _expect(items[2], sql.If)
    if choice.var != "user_chats_populated" or len(choice.then) != 1 or len(choice.otherwise) != 1:
        raise QueryShapeError("queries.sql R2: the \\if :user_chats_populated branch changed")
    rest = items[3:]
    for s in rest:
        _expect(s, sql.Statement)
    heads = [s.head for s in rest]
    if heads != ["DELETE FROM", "INSERT INTO"] * 4 + ["INSERT INTO"]:
        raise QueryShapeError(f"queries.sql R2: publishing statements changed: {heads}")
    return R2(_expect(items[0], sql.Statement, "SET LOCAL"), _expect(items[1], sql.Statement),
              _expect(choice.then[0], sql.Statement, "REFRESH MATERIALIZED"),
              _expect(choice.otherwise[0], sql.Statement, "REFRESH MATERIALIZED"), tuple(rest))


@dataclass(frozen=True)
class AudienceResult:
    as_of: dt.datetime
    pairs: int  # stats.overlap rows
    chat_sets: int
    refreshed: str  # concurrently | plain


def refresh_audience(conn: psycopg.Connection, as_of: dt.datetime, *,
                     qf: sql.QueryFile | None = None) -> AudienceResult:
    """R2 in the caller's transaction, which must be REPEATABLE READ (one snapshot for the view refresh and
    every projection: they describe the same user_chats, tracked chats and side of a migration)."""
    qf = qf or sql.load()
    q = r2(qf)
    level = conn.execute("SELECT current_setting('transaction_isolation')").fetchone()[0]
    if level != "repeatable read":
        raise RuntimeError(f"R2 needs a REPEATABLE READ transaction, this one is {level}")
    vals = sql.values(conn, qf, as_of=as_of)
    _run(conn, q.set_work_mem, vals)
    populated = _run(conn, q.populated, vals).fetchone()[0]
    refresh = q.refresh_concurrently if populated else q.refresh_plain
    _run(conn, refresh, vals)
    for stmt in q.publish:
        _run(conn, stmt, vals)
    pairs = conn.execute("SELECT count(*) FROM stats.overlap").fetchone()[0]
    sets = conn.execute("SELECT count(*) FROM tg.chat_set").fetchone()[0]
    return AudienceResult(as_of, pairs, sets, "concurrently" if populated else "plain")
