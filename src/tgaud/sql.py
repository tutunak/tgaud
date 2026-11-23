"""The one loader of sql/queries.sql (the analytics 1-16 and the rollup SQL R1-R4).

queries.sql is a psql script: a header that sets parameter defaults (`\\if :{?x} \\else \\set x v \\endif`) and
derives `as_of_ts`, `since_ts`, `day_ts`, `scope_ids` with one `SELECT ... \\gset`; then blocks, each
`SELECT :'only' IN ('all', '2', '2a') AS run \\gset` + `\\if :run` ... `\\endif`. A block id is any id of such a
list except `all`: `2a` is one block, `2` is 2a and 2b together.

    block(id)                 -> list[Statement]   the block's statements (BEGIN/COMMIT dropped, \\echo dropped)
    run(conn, id, **params)   -> Result            fills defaults and derived values like the header, runs the
                                                   block in the caller's transaction, returns the last
                                                   statement that produced rows (all_results=True: each one)

psql variables become psycopg named parameters, one per occurrence: `:'name'` is bound as a str with no type
(the server infers it, like psql's quoted literal), `:name` is bound with the type of that parameter
(PARAM_TYPES). `:explain` is a syntax prefix, not a value: `run(..., explain="EXPLAIN (ANALYZE, BUFFERS)")`,
checked against EXPLAIN's option words and never bound. No value is ever formatted into SQL text.

Transaction control in a block (16c, R1, R2: BEGIN ... COMMIT) is dropped: the caller owns the transaction
and `run` never commits. `SET LOCAL` stays and lasts until the caller's transaction ends.

R1 and R2 choose a branch at run time (`SELECT ... \\gset` then `\\if :var`). `block()` refuses them with
BranchingBlock; `run()` executes them the way psql does (the rollups task ports them to Python and can compare
with this). A missing scope (`scope_ids` NULL because no chat is tracked) becomes the empty array `{}`.
"""

from __future__ import annotations

import datetime as dt
import functools
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from importlib.resources.abc import Traversable
from typing import Any

import psycopg
from psycopg.types.numeric import Int4, Int8

from tgaud import sqlscript

# Header inputs bound as psql quoted literals (str), and the values the header derives with \gset.
QUOTED_INPUTS = ("as_of", "scope", "since", "day")
DERIVED = ("as_of_ts", "since_ts", "day_ts", "scope_ids")
# `:name` parameters and the type they are bound with (psql interpolates their text as a number literal).
PARAM_TYPES: dict[str, type] = {
    "hours": Decimal,
    "campaign": Int8,
    "chat": Int8,
    "user": Int8,
    "flow_days": Int4,
    "burst_minutes": Int4,
    "burst_users": Int4,
    "raid_min_joins": Int4,
    "raid_hours": Int4,
    "post_hours": Int4,
    "active_days": Int4,
    "lookback_days": Int4,
}
_EXPLAIN_OPTION = (r"(?:(?:ANALYZE|VERBOSE|COSTS|SETTINGS|GENERIC_PLAN|BUFFERS|WAL|TIMING|SUMMARY|MEMORY)"
                   r"(?:\s+(?:ON|OFF|TRUE|FALSE))?|SERIALIZE(?:\s+(?:NONE|TEXT|BINARY))?"
                   r"|FORMAT\s+(?:TEXT|XML|JSON|YAML))")
_EXPLAIN = re.compile(rf"^EXPLAIN(?:\s+ANALYZE)?(?:\s*\(\s*{_EXPLAIN_OPTION}(?:\s*,\s*{_EXPLAIN_OPTION})*\s*\))?$",
                      re.IGNORECASE)
_SCOPE = re.compile(r"^\{(-?[0-9]+(,-?[0-9]+)*)?\}$")
_BLOCK_SELECTOR = re.compile(r"^SELECT\s+:'only'\s+IN\s*\((?P<ids>[^)]*)\)\s+AS\s+run$", re.IGNORECASE | re.DOTALL)
_ECHO_TITLE = re.compile(r"^==\s+(?P<id>[0-9]+[a-z]?|R[0-9]+)\.\s+(?P<title>.*)$")
_TXN_CONTROL = {"BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT"}
_PSQL_TRUE = ("true", "on", "yes", "1")
_PSQL_FALSE = ("false", "off", "no", "0")


class QueryFileError(sqlscript.ScriptError):
    """queries.sql does not have the structure this loader serves."""


class BranchingBlock(ValueError):
    """The block picks a branch at run time with \\gset + \\if (R1, R2); block() cannot flatten it."""


class UnknownBlock(KeyError):
    pass


@dataclass(frozen=True)
class Statement:
    """One statement of a block. `query(explain)` gives the psycopg query; `bind(values)` its parameters."""

    text: str
    line: int
    vars: tuple[sqlscript.Var, ...]
    gset: bool = False  # psql \gset: its single row sets variables (R1/R2 branch choice)

    @property
    def explainable(self) -> bool:
        return any(v.name == "explain" and v.quoting == "" for v in self.vars)

    @property
    def head(self) -> str:
        return sqlscript.Statement(self.text, self.line, self.vars, ";").head.removeprefix("EXPLAIN ").strip()

    def query(self, explain: str = "") -> str:
        """The SQL with `%(vN_name)s` placeholders (one per occurrence) and `%` escaped."""
        if explain and not _EXPLAIN.match(explain.strip()):
            raise ValueError(f"explain must be EXPLAIN with known options, not {explain!r}")
        out: list[str] = []
        pos = 0
        for n, v in enumerate(self.vars):
            out.append(self.text[pos:v.start].replace("%", "%%"))
            if v.name == "explain" and v.quoting == "":
                out.append(explain.strip())
            else:
                out.append(f"%(v{n}_{v.name})s")
            pos = v.end
        out.append(self.text[pos:].replace("%", "%%"))
        return "".join(out)

    def bind(self, values: dict[str, str]) -> dict[str, Any]:
        """Parameters for query(): quoted variables as untyped str, `:name` ones with their PARAM_TYPES type."""
        params: dict[str, Any] = {}
        for n, v in enumerate(self.vars):
            if v.name == "explain" and v.quoting == "":
                continue
            if v.name not in values:
                raise KeyError(f"line {self.line}: no value for :{v.name}")
            value = values[v.name]
            if v.quoting == "'":
                params[f"v{n}_{v.name}"] = value
            else:
                params[f"v{n}_{v.name}"] = _typed(v.name, value)
        return params


@dataclass(frozen=True)
class If:
    """A run-time branch inside a block: `\\if :var` then / else."""

    var: str
    then: tuple[Statement | If, ...]
    otherwise: tuple[Statement | If, ...]
    line: int


@dataclass(frozen=True)
class Block:
    ids: tuple[str, ...]  # the selector's ids without 'all', e.g. ('2', '2a')
    title: str
    items: tuple[Statement | If, ...]
    line: int

    @property
    def branching(self) -> bool:
        return any(isinstance(i, If) or (isinstance(i, Statement) and i.gset) for i in self.items)


@dataclass(frozen=True)
class QueryFile:
    defaults: dict[str, str]
    header: Statement
    blocks: tuple[Block, ...]
    titles: dict[str, str] = field(default_factory=dict)

    def ids(self) -> list[str]:
        seen: dict[str, None] = {}
        for b in self.blocks:
            for i in b.ids:
                seen.setdefault(i, None)
        return list(seen)

    def blocks_for(self, block_id: str) -> list[Block]:
        found = [b for b in self.blocks if block_id in b.ids]
        if not found:
            raise UnknownBlock(f"queries.sql has no block {block_id!r}; known: {', '.join(self.ids())}")
        return found


@dataclass(frozen=True)
class Result:
    columns: list[str]
    rows: list[tuple]

    def dicts(self) -> list[dict[str, Any]]:
        return [dict(zip(self.columns, r)) for r in self.rows]


def _typed(name: str, value: str | int | Decimal) -> Any:
    kind = PARAM_TYPES.get(name)
    if kind is None:
        raise KeyError(f"unknown :{name} parameter")
    if isinstance(value, bool):
        raise TypeError(f"{name} must be a number, not a boolean")
    text = str(value).strip()
    if kind is Decimal:
        try:
            d = Decimal(text)
        except Exception:
            raise ValueError(f"{name} must be a number, not {text!r}") from None
        if not d.is_finite():
            raise ValueError(f"{name} must be a finite number")
        return d
    if not re.fullmatch(r"-?[0-9]+", text):
        raise ValueError(f"{name} must be an integer, not {text!r}")
    return kind(int(text))


# ------------------------------------------------------------------------------------------------------
# Parsing queries.sql
# ------------------------------------------------------------------------------------------------------


def parse(text: str, name: str = "queries.sql") -> QueryFile:
    items = sqlscript.parse(text, name)
    pos = 0
    defaults: dict[str, str] = {}
    # header defaults: \if :{?x} \else \set x v \endif
    while pos < len(items) and isinstance(items[pos], sqlscript.Meta) and items[pos].command == "if":
        m = items[pos]
        if len(m.args) != 1 or not re.fullmatch(r":\{\?[A-Za-z0-9_]+\}", m.args[0]):
            break
        var = m.args[0][3:-1]
        seq = items[pos + 1:pos + 4]
        if (len(seq) != 3 or not all(isinstance(x, sqlscript.Meta) for x in seq)
                or [x.command for x in seq] != ["else", "set", "endif"] or not seq[1].args
                or seq[1].args[0] != var or len(seq[1].args) > 2):
            raise QueryFileError(f"{name}:{m.line}: header default for {var} is not \\if :{{?x}} \\else \\set x v \\endif")
        defaults[var] = seq[1].args[1] if len(seq[1].args) == 2 else ""
        pos += 4
    if not defaults:
        raise QueryFileError(f"{name}: no parameter defaults in the header")
    head = items[pos] if pos < len(items) else None
    if not isinstance(head, sqlscript.Statement) or head.terminator != "gset":
        raise QueryFileError(f"{name}: the header must derive its values with one SELECT ... \\gset")
    header = _stmt(head, name, set(defaults), gset=True)
    pos += 1
    known_vars = set(defaults) | set(DERIVED)
    blocks: list[Block] = []
    titles: dict[str, str] = {}
    while pos < len(items):
        item = items[pos]
        if isinstance(item, sqlscript.Meta) and item.command == "echo":
            pos += 1
            continue
        if not (isinstance(item, sqlscript.Statement) and item.terminator == "gset"):
            raise QueryFileError(f"{name}:{_line(item)}: expected a block selector SELECT :'only' IN (...) AS run \\gset")
        sel = _BLOCK_SELECTOR.match(item.code)
        if not sel:
            raise QueryFileError(f"{name}:{item.line}: expected a block selector SELECT :'only' IN (...) AS run \\gset")
        ids = tuple(x.strip().strip("'") for x in sel.group("ids").split(","))
        ids = tuple(i for i in ids if i != "all")
        nxt = items[pos + 1] if pos + 1 < len(items) else None
        if not (isinstance(nxt, sqlscript.Meta) and nxt.command == "if" and nxt.args == (":run",)):
            raise QueryFileError(f"{name}:{item.line}: block selector not followed by \\if :run")
        body, pos, local = _body(items, pos + 2, name, known_vars, titles)
        title = next((titles[i] for i in ids if i in titles), "")
        blocks.append(Block(ids, title, tuple(body), item.line))
    if not blocks:
        raise QueryFileError(f"{name}: no blocks")
    return QueryFile(defaults, header, tuple(blocks), titles)


def _line(item: sqlscript.Statement | sqlscript.Meta) -> int:
    return item.line


def _stmt(s: sqlscript.Statement, name: str, known: set[str], gset: bool = False) -> Statement:
    for v in s.vars:
        if v.quoting not in ("", "'"):
            raise QueryFileError(f"{name}:{s.line}: variable form :{v.quoting}{v.name} is not served")
        if v.name == "explain":
            if v.quoting != "":
                raise QueryFileError(f"{name}:{s.line}: :explain must be unquoted")
            continue
        if v.name not in known:
            raise QueryFileError(f"{name}:{s.line}: unknown variable :{v.name}")
        if v.quoting == "" and v.name not in PARAM_TYPES:
            raise QueryFileError(f"{name}:{s.line}: :{v.name} has no declared type (sql.PARAM_TYPES)")
    return Statement(s.text, s.line, s.vars, gset)


def _body(items: list, pos: int, name: str, known: set[str], titles: dict[str, str]):
    """Parse a block body up to its \\endif; returns (items, next position, variables set by \\gset)."""
    out: list[Statement | If] = []
    local: set[str] = set()
    while pos < len(items):
        item = items[pos]
        if isinstance(item, sqlscript.Meta):
            if item.command == "endif":
                return out, pos + 1, local
            if item.command == "echo":
                m = _ECHO_TITLE.match(" ".join(item.args))
                if m:
                    titles.setdefault(m.group("id"), m.group("title"))
                pos += 1
                continue
            if item.command == "if":
                if len(item.args) != 1 or not re.fullmatch(r":[A-Za-z0-9_]+", item.args[0]):
                    raise QueryFileError(f"{name}:{item.line}: \\if needs one :variable")
                var = item.args[0][1:]
                if var not in local:
                    raise QueryFileError(f"{name}:{item.line}: \\if :{var} before a \\gset sets it")
                then, pos2, more = _branch(items, pos + 1, name, known | local, titles)
                otherwise: list = []
                if pos2 < len(items) and isinstance(items[pos2], sqlscript.Meta) and items[pos2].command == "else":
                    otherwise, pos2, more2 = _branch(items, pos2 + 1, name, known | local, titles)
                    more |= more2
                end = items[pos2] if pos2 < len(items) else None
                if not (isinstance(end, sqlscript.Meta) and end.command == "endif"):
                    raise QueryFileError(f"{name}:{item.line}: \\if :{var} without \\endif")
                out.append(If(var, tuple(then), tuple(otherwise), item.line))
                local |= more
                pos = pos2 + 1
                continue
            raise QueryFileError(f"{name}:{item.line}: meta command \\{item.command} is not served")
        if item.terminator == "g":
            raise QueryFileError(f"{name}:{item.line}: \\g is not served")
        st = _stmt(item, name, known | local, gset=item.terminator == "gset")
        if st.gset:
            local |= set(_gset_columns(item))
        out.append(st)
        pos += 1
    raise QueryFileError(f"{name}: a block is not closed by \\endif")


def _branch(items: list, pos: int, name: str, known: set[str], titles: dict[str, str]):
    out: list[Statement | If] = []
    local: set[str] = set()
    while pos < len(items):
        item = items[pos]
        if isinstance(item, sqlscript.Meta):
            if item.command in ("else", "endif"):
                return out, pos, local
            if item.command == "echo":
                pos += 1
                continue
            raise QueryFileError(f"{name}:{item.line}: \\{item.command} inside a branch is not served")
        st = _stmt(item, name, known | local, gset=item.terminator == "gset")
        if st.gset:
            local |= set(_gset_columns(item))
        out.append(st)
        pos += 1
    raise QueryFileError(f"{name}: unterminated \\if")


def _gset_columns(s: sqlscript.Statement) -> list[str]:
    """The `AS name` output names of a \\gset statement (the variables it sets)."""
    names = re.findall(r"\bAS\s+([a-z_][a-z0-9_]*)\b", s.code, re.IGNORECASE)
    if not names:
        raise QueryFileError(f"line {s.line}: \\gset statement without AS names")
    return [n.lower() for n in names]


@functools.lru_cache(maxsize=4)
def _load_cached(root: Traversable | None) -> QueryFile:
    from tgaud.db import sql_root

    path = (root or sql_root()) / "queries.sql"
    return parse(path.read_text("utf-8"), "queries.sql")


def load(root: Traversable | None = None) -> QueryFile:
    """The parsed queries.sql of the package (or of another sql root, for tests and --selfcheck)."""
    return _load_cached(root)


def ids() -> list[str]:
    return load().ids()


def title(block_id: str) -> str:
    qf = load()
    qf.blocks_for(block_id)
    return qf.titles.get(block_id, "")


def block(block_id: str, qf: QueryFile | None = None) -> list[Statement]:
    """The statements of a block (and its sub-blocks), transaction control and \\echo dropped."""
    qf = qf or load()
    out: list[Statement] = []
    for b in qf.blocks_for(block_id):
        if b.branching:
            raise BranchingBlock(f"block {block_id} picks a branch at run time (\\gset + \\if); it is run by "
                                 "tgaud.sql.run or ported to Python, not flattened")
        out.extend(s for s in b.items if isinstance(s, Statement) and not _is_txn_control(s))
    return out


def _is_txn_control(s: Statement) -> bool:
    return s.head.split(" ", 1)[0] in _TXN_CONTROL if s.head else False


# ------------------------------------------------------------------------------------------------------
# Running
# ------------------------------------------------------------------------------------------------------


def values(conn: psycopg.Connection, qf: QueryFile | None = None, **params: Any) -> dict[str, str]:
    """Defaults + params + the header's derived values, as the psql variables would hold them (text)."""
    qf = qf or load()
    vals = dict(qf.defaults)
    for k, v in params.items():
        if k not in qf.defaults:
            raise TypeError(f"unknown query parameter {k!r}; known: {', '.join(sorted(qf.defaults))}")
        if k in ("only", "explain"):
            raise TypeError(f"{k} is not a value: use the block id / the explain option")
        vals[k] = _input_text(k, v)
    hq = qf.header.query()
    derived_cols = _gset_columns(sqlscript.Statement(qf.header.text, qf.header.line, qf.header.vars, "gset"))
    wrapped = "SELECT " + ", ".join(f"h.{c}::text" for c in derived_cols) + f" FROM ({hq}) h"
    row = conn.execute(wrapped, qf.header.bind(vals)).fetchone()
    for col, value in zip(derived_cols, row):
        vals[col] = value
    if vals.get("scope_ids") is None:
        vals["scope_ids"] = "{}"  # no tracked chat: an empty scope instead of psql's unset variable
    for col in derived_cols:
        if vals.get(col) is None:
            raise ValueError(f"the header derived NULL for {col}")
    return vals


def _input_text(name: str, v: Any) -> str:
    if name in PARAM_TYPES:
        _typed(name, v)  # validate now
        return str(v)
    if v is None:
        return ""
    if name == "scope":
        if isinstance(v, str):
            text = v.replace(" ", "")
        else:
            ids_ = list(v)
            if any(isinstance(x, bool) or not isinstance(x, int) for x in ids_):
                raise TypeError("scope must be chat ids (int)")
            text = "{" + ",".join(str(x) for x in ids_) + "}"
        if not _SCOPE.match(text):
            raise ValueError(f"scope must look like {{-1001,-1002}}, not {v!r}")
        return text
    if isinstance(v, dt.datetime):
        if v.tzinfo is None:
            raise ValueError(f"{name} must be timezone-aware")
        return v.isoformat()
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, str):
        return v
    raise TypeError(f"{name} must be a str, date or datetime")


def run(conn: psycopg.Connection, block_id: str, *, explain: str = "", all_results: bool = False,
        qf: QueryFile | None = None, **params: Any) -> Result | list[Result]:
    """Run a block in the caller's transaction (never commits) and return the last statement that produced
    rows; with all_results, a Result for every statement that produced rows, in order."""
    qf = qf or load()
    if explain and not _EXPLAIN.match(explain.strip()):
        raise ValueError(f"explain must be EXPLAIN with known options, not {explain!r}")
    blocks = qf.blocks_for(block_id)
    vals = values(conn, qf, **params)
    results: list[Result] = []
    for b in blocks:
        _exec_items(conn, b.items, vals, explain, results)
    if all_results:
        return results
    if not results:
        return Result([], [])
    return results[-1]


def _exec_items(conn: psycopg.Connection, items: Sequence[Statement | If], vals: dict[str, str], explain: str,
                results: list[Result]) -> None:
    for item in items:
        if isinstance(item, If):
            _exec_items(conn, item.then if _psql_bool(vals.get(item.var), item) else item.otherwise, vals,
                        explain, results)
            continue
        if _is_txn_control(item):
            continue
        cur = conn.execute(item.query("" if item.gset else explain), item.bind(vals))
        if item.gset:
            row = cur.fetchone()
            if row is None or cur.fetchone() is not None:
                raise ValueError(f"line {item.line}: \\gset query must return exactly one row")
            for desc, value in zip(cur.description, row):
                vals[desc.name] = None if value is None else ("t" if value is True else "f" if value is False else str(value))
            continue
        if cur.description is not None:
            results.append(Result([d.name for d in cur.description], cur.fetchall()))


def _psql_bool(value: str | None, item: If) -> bool:
    if value is None:
        raise ValueError(f"line {item.line}: \\if :{item.var} but the variable is unset")
    v = value.strip().lower()
    if v and any(t.startswith(v) for t in _PSQL_TRUE) and v not in ("o",):
        return True
    if v and any(f.startswith(v) for f in _PSQL_FALSE) and v not in ("o",):
        return False
    raise ValueError(f"line {item.line}: \\if :{item.var} = {value!r} is not a boolean")
