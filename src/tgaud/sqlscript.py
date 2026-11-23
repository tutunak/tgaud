"""The one lexer of psql-flavoured SQL files (migrations, queries.sql, admin.sql).

It splits a file the way psql does into top-level SQL statements and backslash meta commands, and finds
the psql variable references (`:name`, `:'name'`, `:"name"`, `:{?name}`) outside literals and comments.
It understands what these files use: string literals ('..' with '', E'..' with backslash escapes), quoted
identifiers, `--` and nested `/* */` comments, dollar quotes ($$ .. $$, $tag$ .. $tag$), the `::` cast,
statements ended by `;` or by `\\gset`/`\\g`, and several meta commands on one line
(`\\if :{?x} \\else \\set x 1 \\endif`). SQL-standard function bodies (BEGIN ATOMIC) are refused.

Nothing here talks to a database; it is pure text processing, so `--selfcheck` can use it offline.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_IDENT_START = re.compile(r"[A-Za-z_\x80-￿]")
_IDENT_CHAR = re.compile(r"[A-Za-z0-9_$\x80-￿]")
_VAR_NAME = re.compile(r"[A-Za-z0-9_]+")
_DOLLAR_TAG = re.compile(r"\$([A-Za-z_\x80-￿][A-Za-z0-9_\x80-￿]*)?\$")
_WORD = re.compile(r"[A-Za-z_]+")


class ScriptError(ValueError):
    """A file that this lexer cannot split safely; the message names the file and line."""


@dataclass(frozen=True)
class Var:
    """A psql variable reference inside a statement: offsets are into Statement.text."""

    name: str
    quoting: str  # "" for :name, "'" for :'name', '"' for :"name", "?" for :{?name}
    start: int
    end: int


@dataclass(frozen=True)
class Statement:
    """One top-level SQL statement, without its terminator."""

    text: str
    line: int  # line of the statement's first character (1-based)
    vars: tuple[Var, ...]
    terminator: str  # ";", "gset", "g" or "eof"

    @property
    def code(self) -> str:
        """The text without comments, whitespace collapsed (for keyword checks and errors)."""
        return strip_comments(self.text)

    @property
    def head(self) -> str:
        """The first two keywords, upper case, e.g. 'BEGIN', 'SET LOCAL', 'COMMIT'."""
        code = self.code
        if self.vars and code.startswith(":"):
            code = code.split(None, 1)[1] if " " in code else ""
        words = _WORD.findall(code[:40])
        return " ".join(w.upper() for w in words[:2])

    def is_blank(self) -> bool:
        return not self.code


@dataclass(frozen=True)
class Meta:
    """A backslash command: `\\if :run` is Meta('if', [':run'], line)."""

    command: str
    args: tuple[str, ...]
    line: int


def strip_comments(text: str) -> str:
    """The code of a statement with comments removed and whitespace collapsed; literals stay."""
    out: list[str] = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c == "-" and text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
            out.append(" ")
        elif c == "/" and text.startswith("/*", i):
            i = _skip_block_comment(text, i, "<text>", 1)
            out.append(" ")
        elif c in "'\"" or c == "$" and _DOLLAR_TAG.match(text, i):
            j = _skip_quoted(text, i, "<text>", 1)
            out.append(text[i:j])
            i = j
        else:
            out.append(c)
            i += 1
    return " ".join("".join(out).split())


def _line_at(text: str, pos: int) -> int:
    return text.count("\n", 0, pos) + 1


def _skip_block_comment(text: str, i: int, name: str, base_line: int) -> int:
    depth, j, n = 0, i, len(text)
    while j < n:
        if text.startswith("/*", j):
            depth += 1
            j += 2
        elif text.startswith("*/", j):
            depth -= 1
            j += 2
            if depth == 0:
                return j
        else:
            j += 1
    raise ScriptError(f"{name}:{base_line + _line_at(text, i) - 1}: unterminated /* comment")


def _skip_quoted(text: str, i: int, name: str, base_line: int) -> int:
    """i points at ', E', " or a dollar tag; returns the index after the closing quote."""
    n = len(text)
    c = text[i]
    if c == "$":
        m = _DOLLAR_TAG.match(text, i)
        assert m
        tag = m.group(0)
        j = text.find(tag, m.end())
        if j < 0:
            raise ScriptError(f"{name}:{base_line + _line_at(text, i) - 1}: unterminated dollar quote {tag}")
        return j + len(tag)
    escapes = False
    if c in "Ee":
        escapes = True
        i += 1
        c = "'"
    j = i + 1
    while j < n:
        ch = text[j]
        if escapes and ch == "\\":
            j += 2
            continue
        if ch == c:
            if j + 1 < n and text[j + 1] == c:
                j += 2
                continue
            return j + 1
        j += 1
    what = "identifier" if c == '"' else "string literal"
    raise ScriptError(f"{name}:{base_line + _line_at(text, i) - 1}: unterminated quoted {what}")


def _split_meta(text: str, i: int, name: str, base_line: int) -> tuple[list[Meta], int]:
    """Parse backslash commands from text[i] (a backslash) to the end of the line."""
    j = text.find("\n", i)
    end = len(text) if j < 0 else j
    line = base_line + _line_at(text, i) - 1
    metas: list[Meta] = []
    pos = i
    while pos < end:
        if text[pos] != "\\":
            raise ScriptError(f"{name}:{line}: unexpected text {text[pos:end]!r} in a meta command line")
        k = pos + 1
        while k < end and not text[k].isspace() and text[k] != "\\":
            k += 1
        command = text[pos + 1:k]
        if not command:
            raise ScriptError(f"{name}:{line}: empty backslash command")
        args: list[str] = []
        k = _skip_ws(text, k, end)
        while k < end and text[k] != "\\":
            arg, k = _meta_arg(text, k, end, name, line)
            args.append(arg)
            k = _skip_ws(text, k, end)
        metas.append(Meta(command, tuple(args), line))
        pos = k
    return metas, end


def _skip_ws(text: str, k: int, end: int) -> int:
    while k < end and text[k].isspace():
        k += 1
    return k


def _meta_arg(text: str, k: int, end: int, name: str, line: int) -> tuple[str, int]:
    """One argument of a meta command; a single-quoted argument is de-quoted as psql does."""
    if text[k] == "'":
        out: list[str] = []
        j = k + 1
        while j < end:
            if text[j] == "'":
                if j + 1 < end and text[j + 1] == "'":
                    out.append("'")
                    j += 2
                    continue
                return "".join(out), j + 1
            out.append(text[j])
            j += 1
        raise ScriptError(f"{name}:{line}: unterminated quote in a meta command argument")
    j = k
    while j < end and not text[j].isspace() and text[j] != "\\":
        if text[j] in "'\"`":
            raise ScriptError(f"{name}:{line}: quote inside a meta command argument is not supported")
        j += 1
    return text[k:j], j


def parse(text: str, name: str = "<sql>") -> list[Statement | Meta]:
    """Split a psql script into statements and meta commands, in file order."""
    items: list[Statement | Meta] = []
    buf_start = 0  # start offset of the current statement
    vars_: list[tuple[str, str, int, int]] = []
    i, n = 0, len(text)
    code_seen = False  # the current statement has code beyond comments and whitespace

    def flush(end: int, terminator: str) -> None:
        nonlocal buf_start, vars_, code_seen
        raw = text[buf_start:end]
        lead = len(raw) - len(raw.lstrip())
        body = raw[lead:].rstrip()
        offset = buf_start + lead
        if code_seen:
            stmt = Statement(
                text=body,
                line=_line_at(text, offset),
                vars=tuple(Var(v, q, s - offset, e - offset) for v, q, s, e in vars_),
                terminator=terminator,
            )
            if re.search(r"\bBEGIN\s+ATOMIC\b", stmt.code, re.IGNORECASE):
                raise ScriptError(f"{name}:{stmt.line}: SQL-standard function bodies (BEGIN ATOMIC) are not supported")
            items.append(stmt)
        vars_ = []
        code_seen = False

    while i < n:
        c = text[i]
        if c == "-" and text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        if c == "/" and text.startswith("/*", i):
            i = _skip_block_comment(text, i, name, 1)
            continue
        if c in "Ee" and i + 1 < n and text[i + 1] == "'" and (i == 0 or not _IDENT_CHAR.match(text[i - 1])):
            i = _skip_quoted(text, i, name, 1)
            code_seen = True
            continue
        if c in "'\"":
            i = _skip_quoted(text, i, name, 1)
            code_seen = True
            continue
        if c == "$" and (i == 0 or not _IDENT_CHAR.match(text[i - 1])) and _DOLLAR_TAG.match(text, i):
            i = _skip_quoted(text, i, name, 1)
            code_seen = True
            continue
        if c == ":":
            if text.startswith("::", i):
                i += 2
                code_seen = True
                continue
            m = None
            if text.startswith(":'", i) or text.startswith(':"', i):
                q = text[i + 1]
                m = _VAR_NAME.match(text, i + 2)
                if m and m.end() < n and text[m.end()] == q:
                    vars_.append((m.group(0), q, i, m.end() + 1))
                    i = m.end() + 1
                    code_seen = True
                    continue
            elif text.startswith(":{?", i):
                m = _VAR_NAME.match(text, i + 3)
                if m and m.end() < n and text[m.end()] == "}":
                    vars_.append((m.group(0), "?", i, m.end() + 1))
                    i = m.end() + 1
                    code_seen = True
                    continue
            else:
                m = _VAR_NAME.match(text, i + 1)
                if m:
                    vars_.append((m.group(0), "", i, m.end()))
                    i = m.end()
                    code_seen = True
                    continue
            i += 1
            code_seen = True
            continue
        if c == ";":
            flush(i, ";")
            i += 1
            buf_start = i
            continue
        if c == "\\":
            metas, end = _split_meta(text, i, name, 1)
            first = metas[0]
            if first.command in ("gset", "g"):
                if not code_seen:
                    raise ScriptError(f"{name}:{first.line}: \\{first.command} without a query")
                flush(i, first.command)
                metas = metas[1:]
            elif code_seen:
                raise ScriptError(f"{name}:{first.line}: \\{first.command} inside an unfinished statement")
            items.extend(metas)
            vars_ = []
            code_seen = False
            i = end
            buf_start = i
            continue
        if not c.isspace():
            code_seen = True
        i += 1
    flush(n, "eof")
    return items


def statements_only(items: list[Statement | Meta], name: str) -> list[Statement]:
    """The statements of a plain SQL file; refuses meta commands and psql variables."""
    out: list[Statement] = []
    for item in items:
        if isinstance(item, Meta):
            raise ScriptError(f"{name}:{item.line}: psql meta command \\{item.command} is not allowed here")
        if item.vars:
            v = item.vars[0]
            raise ScriptError(f"{name}:{item.line}: psql variable :{v.name} is not allowed here")
        out.append(item)
    return out
