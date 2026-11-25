"""The pages of the web UI (design.md 10): overview, overlap matrix and pair drill-down, user lookup, campaigns
(create, bot links, item 4 per chat and per person, entrant CSV) and the CLI's other reports as tables.

Every page reads in one REPEATABLE READ transaction through the blocks of queries.sql (tgaud.sql) or the CLI's
own helpers (tgaud.commands), so a page and `tgaud <report>` give the same numbers for the same as-of time.
The pages of the audience rollup show its as_of and carry its publication (the rollup_run id): a drill-down is
answered only while that publication is still the current one; after a newer R2 publish the page asks to reload the matrix instead of showing a different
population.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import logging
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import jinja2
import psycopg

from tgaud import db, sql
from tgaud.botapi import ApiError
from tgaud.commands import campaigns as campaigns_cmd
from tgaud.commands import common, reports
from tgaud.commands.common import CliError, Window
from tgaud.config import Config
from tgaud.web.app import HttpError, Request, Response, WebDb, db_error_line, finish, redirect, same_origin, \
    text, token_ok

log = logging.getLogger(__name__)

UTC = dt.timezone.utc
PAGE_SIZE = 100  # people per page of a pair drill-down
ALL_ROWS = 2**31 - 1  # block 1b's own page size when sql.run_page pages it by position
REPORT_PAGE = 500  # rows per page of a report table; every page is its own bounded query (sql.run_page)
SIDES = {"shared": "in both", "only_a": "only in A", "only_b": "only in B"}
USER_COLUMNS = frozenset({"user_id"})
ASOF_MODES = {"counts": "16a", "list": "16b", "eod": "16d", "matrix": "16c"}
ASOF_INPUTS = ("chat", "date", "mode", "among")  # what an asof page link keeps (with its frozen as_of)
PAGES = (("/", "Overview"), ("/overlap", "Overlap"), ("/user", "User"), ("/campaigns", "Campaigns"),
         ("/reports", "Reports"))


def environment() -> jinja2.Environment:
    env = jinja2.Environment(loader=jinja2.PackageLoader("tgaud.web", "templates"), autoescape=True,
                             undefined=jinja2.StrictUndefined, trim_blocks=True, lstrip_blocks=True)
    env.filters["when"] = common.when
    env.filters["cell"] = common.cell
    env.filters["day"] = lambda v: "-" if v is None else v.astimezone(UTC).strftime("%Y-%m-%d")
    env.globals["qs"] = lambda **kw: urlencode([(k, v) for k, vs in kw.items()
                                                for v in (vs if isinstance(vs, (list, tuple)) else [vs])
                                                if v is not None and v != ""])
    env.globals["pages"] = PAGES
    return env


def static_file(name: str) -> bytes | None:
    from importlib.resources import files

    if not re.fullmatch(r"[a-z0-9_-]+\.(css|svg)", name):
        return None
    path = files("tgaud.web") / "static" / name
    return path.read_bytes() if path.is_file() else None


# ------------------------------------------------------------------------------------------------------
# Tables
# ------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Cell:
    text: str
    href: str | None = None
    sub: str | None = None  # small text after it (a chat's id)
    user: bool = False  # a user id: a POST form to the lookup (no user id ever goes into a URL)
    time: bool = False  # a date or a time: may break only between the date and the time


@dataclass
class Table:
    columns: list[str]
    rows: list[list[Cell]]
    numeric: list[bool]
    start: int = 0  # the offset of the first row (a page of a longer result)
    next: str | None = None  # the URL of the next page
    prev: str | None = None  # the URL of the previous page

    @property
    def paged(self) -> bool:
        return self.next is not None or self.prev is not None


def _numeric(values: Sequence[Any]) -> bool:
    present = [v for v in values if v is not None]
    return bool(present) and all(isinstance(v, (int, float, Decimal)) and not isinstance(v, bool) for v in present)


def table(result: sql.Result, titles: dict[int, str]) -> Table:
    """A result as cells, every row: a chat id column shows the chat's title and id unless the result has its own
    title column, a user id links to its lookup, a campaign id to its page."""
    own_titles = any("title" in c or c in ("channel", "campaign") for c in result.columns)
    rows = result.rows
    numeric: list[bool] = []
    cols: list[list[Cell]] = []
    for i, name in enumerate(result.columns):
        values = [r[i] for r in rows]
        decorate = not own_titles and name in common.CHAT_COLUMNS
        out: list[Cell] = []
        for v in values:
            if v is None:
                out.append(Cell(""))
            elif decorate:
                out.append(Cell(titles.get(int(v), "?"), sub=str(v)))
            elif name in USER_COLUMNS:
                out.append(Cell(str(v), user=True))
            elif name == "campaign_id":
                out.append(Cell(str(v), f"/campaigns/{int(v)}"))
            elif isinstance(v, (dt.datetime, dt.date)):
                out.append(Cell(common.cell(v) if isinstance(v, dt.datetime) else v.isoformat(), time=True))
            else:
                out.append(Cell(common.cell(v)))
        cols.append(out)
        numeric.append(not decorate and name not in common.CHAT_COLUMNS and _numeric([r[i] for r in result.rows]))
    grid = [[cols[c][r] for c in range(len(result.columns))] for r in range(len(rows))]
    return Table(list(result.columns), grid, numeric)


def transposed(result: sql.Result, titles: dict[int, str], key: str) -> Table:
    """A few wide rows (item 4: one per chat, or one per campaign) turned into one row per column, one column per
    row, headed by the `key` column's value: readable without scrolling sideways."""
    t = table(result, titles)
    k = result.columns.index(key)
    heads = [f"{titles.get(int(r[k]), '?')} ({r[k]})" if key in common.CHAT_COLUMNS else f"{key.removesuffix('_id')} {r[k]}"
             for r in result.rows[:len(t.rows)]]
    rows = [[Cell(name), *(r[i] for r in t.rows)] for i, name in enumerate(result.columns) if i != k]
    numeric = [False, *([True] * len(heads))]
    return Table(["", *heads], rows, numeric)


def paged_table(req: Request, report: reports.Report, titles: dict[int, str], offset: int,
                inputs: Sequence[str], list_inputs: Sequence[str] = ("chat",)) -> Table:
    """One page of a report read with page=(offset, REPORT_PAGE + 1): its rows, and links to the pages before and
    after. A link keeps only the report's own `inputs` (scope, options) from the request, never another field
    (a lookup's user id must not travel into a URL), and freezes the as-of time."""
    rows = report.result.rows
    more = len(rows) > REPORT_PAGE
    t = table(sql.Result(report.result.columns, rows[:REPORT_PAGE]), titles)
    t.start = offset

    def href(at: int) -> str:
        # only what the report read: every value of the page's chat lists (`list_inputs`: each value checked as a
        # tracked chat by _scope, or the request was refused), the first value of any other input; never a
        # value it ignored
        query: dict[str, list[str]] = {}
        for k in inputs:
            if k in ("offset", "as_of"):
                continue
            values = req.args(k) if k in list_inputs else [v for v in [req.arg(k)] if v is not None]
            if values:
                query[k] = values
        query["as_of"] = [report.as_of.isoformat()]
        if at:
            query["offset"] = [str(at)]
        return req.path + "?" + urlencode(query, doseq=True)

    t.next = href(offset + REPORT_PAGE) if more else None
    t.prev = href(max(0, offset - REPORT_PAGE)) if offset else None
    return t


def _offset(req: Request) -> int:
    n = _int(req.arg("offset"), "offset") or 0
    if n < 0:
        raise HttpError(400, "offset must not be negative")
    return n


@dataclass
class Header:
    """The report header of tgaud.commands.common.print_header, as data."""

    title: str
    as_of: dt.datetime
    window: Window | None
    rollup: bool
    rollup_as_of: dt.datetime | None
    notes: Sequence[str]
    coverage: list[dict[str, Any]]


def header_of(report: reports.Report) -> Header:
    return Header(report.title, report.as_of, report.window, report.rollup, report.rollup_as_of, report.notes,
                  report.coverage)


# ------------------------------------------------------------------------------------------------------
# Inputs
# ------------------------------------------------------------------------------------------------------


def _int(text_: str | None, what: str) -> int | None:
    if text_ is None:
        return None
    if not re.fullmatch(r"-?[0-9]{1,19}", text_):
        raise HttpError(400, f"{what} must be a number")
    return int(text_)


def _time(text_: str | None, what: str) -> dt.datetime | None:
    if text_ is None:
        return None
    try:
        return common.parse_time(text_, what)
    except CliError as e:
        raise HttpError(400, str(e)) from None


def _scope(req: Request, chats: Sequence[common.Chat], field: str = "chat") -> list[int] | None:
    """The `chat` fields: tracked chat ids, in the order given, no repeats; None = every tracked chat."""
    tracked = {c.id for c in chats if c.tracked}
    out: list[int] = []
    for raw in req.args(field):
        n = _int(raw, field)
        if n not in tracked:
            raise HttpError(400, f"chat {raw} is not a tracked chat")
        if n not in out:
            out.append(n)
    return out or None


def _option(spec_name: str, raw: str | None) -> Any:
    if raw is None:
        return None
    if spec_name == "since":
        return raw
    kind = reports.OPTIONS[spec_name][1].get("type")
    try:
        return kind(raw) if kind else raw
    except argparse.ArgumentTypeError as e:
        raise HttpError(400, f"{spec_name}: {e}") from None


# ------------------------------------------------------------------------------------------------------
# The app
# ------------------------------------------------------------------------------------------------------


Handler = Callable[[Request, dict[str, str]], Response]


class WebApp:
    """The WSGI application. `api_factory(cfg)` gives the Bot API client for `link create` (a context manager);
    `fatal(code, reason)` stops serve (a takeover seen by a write's generation fence)."""

    def __init__(self, cfg: Config, database: WebDb, token: bytes, *,
                 api_factory: Callable[[Config], Any] | None = None,
                 fatal: Callable[[int, str], None] = lambda code, reason: None) -> None:
        self.cfg = cfg
        self.db = database
        self.token = token
        self.api_factory = api_factory or campaigns_cmd.make_api
        self.fatal = fatal
        self.env = environment()
        self.routes: list[tuple[str, re.Pattern[str], Handler]] = [
            ("GET", re.compile(r"/healthz"), self.healthz),
            ("GET", re.compile(r"/static/(?P<name>[^/]+)"), self.static),
            ("GET", re.compile(r"/"), self.overview),
            ("GET", re.compile(r"/overlap"), self.overlap),
            ("GET", re.compile(r"/overlap/pair"), self.pair),
            ("GET", re.compile(r"/user"), self.user),
            ("POST", re.compile(r"/user"), self.user),
            ("GET", re.compile(r"/campaigns"), self.campaigns),
            ("POST", re.compile(r"/campaigns"), self.campaign_create),
            ("GET", re.compile(r"/campaigns/(?P<id>[0-9]{1,18})"), self.campaign),
            ("POST", re.compile(r"/campaigns/(?P<id>[0-9]{1,18})/entrants\.csv"), self.entrants_csv),
            ("POST", re.compile(r"/campaigns/(?P<id>[0-9]{1,18})/link"), self.link_create),
            ("GET", re.compile(r"/reports"), self.report_index),
            ("GET", re.compile(r"/reports/asof"), self.asof),
            ("GET", re.compile(r"/reports/coverage"), self.coverage),
            ("GET", re.compile(r"/reports/(?P<name>[a-z-]+)"), self.report),
        ]

    def __call__(self, environ: dict[str, Any], start_response: Callable) -> list[bytes]:
        # The proxy token before anything else: no parsing, no query, no hint which paths exist.
        if not token_ok(environ, self.token):
            return finish(start_response, text(403, "forbidden\n"))
        try:
            resp = self.dispatch(Request.from_environ(environ))
        except HttpError as e:
            resp = self.error(e.status, e.message)
        except db.StaleGeneration as e:
            log.error("web: %s; stopping", e)
            self.fatal(1, str(e))
            resp = self.error(503, "another tgaud serve took over; this one is stopping")
        except psycopg.Error as e:
            log.warning("web: database error: %s", db_error_line(e))
            resp = self.error(503, f"database error: {db_error_line(e)}")
        except Exception:
            log.exception("web: request failed")
            resp = self.error(500, "the request failed; the service log has the details")
        return finish(start_response, resp)

    def dispatch(self, req: Request) -> Response:
        allowed: set[str] = set()
        for method, pattern, handler in self.routes:
            m = pattern.fullmatch(req.path)
            if not m:
                continue
            allowed.add(method)
            if method == req.method or (method == "GET" and req.method == "HEAD"):
                if method == "POST" and not same_origin(req):
                    raise HttpError(403, "cross-site form refused: the Origin (or Referer) is not this host")
                resp = handler(req, m.groupdict())
                if req.method == "HEAD":
                    resp.body = b""
                return resp
        if allowed:
            raise HttpError(405, f"{req.method} is not allowed here")
        raise HttpError(404, "no such page")

    def render(self, template: str, status: int = 200, **ctx: Any) -> Response:
        html = self.env.get_template(template).render(**ctx)
        return Response(status, html.encode())

    def error(self, status: int, message: str) -> Response:
        return self.render("error.html", status, title=f"{status}", message=message, nav="")

    # --------------------------------------------------------------------------------------------------
    # Health and assets
    # --------------------------------------------------------------------------------------------------

    def healthz(self, req: Request, _m: dict[str, str]) -> Response:
        suffix = f"activation={self.cfg.activation_id}"
        try:
            self.db.ping()
        except HttpError as e:
            return text(503, f"unhealthy: {e.message} {suffix}\n")
        except Exception as e:
            return text(503, f"unhealthy: database: {db_error_line(e)} {suffix}\n")
        return text(200, f"ok {suffix}\n")

    def static(self, req: Request, m: dict[str, str]) -> Response:
        body = static_file(m["name"])
        if body is None:
            raise HttpError(404, "no such file")
        ctype = "text/css; charset=utf-8" if m["name"].endswith(".css") else "image/svg+xml"
        return Response(200, body, ctype)

    # --------------------------------------------------------------------------------------------------
    # Overview
    # --------------------------------------------------------------------------------------------------

    def overview(self, req: Request, _m: dict[str, str]) -> Response:
        at = _time(req.arg("as_of"), "as_of")
        with self.db.read() as conn:
            chats = common.load_chats(conn)
            vals = sql.values(conn, **({"as_of": at.isoformat()} if at else {}))
            as_of = _ts(conn, vals["as_of_ts"])
            since = _ts(conn, vals["since_ts"])
            scope = [c.id for c in chats if c.tracked]
            cov = reports.coverage_table(conn, as_of, since, scope).dicts()
            drift = sql.run(conn, "9b", as_of=vals["as_of_ts"], scope=scope).dicts()
            runs = conn.execute("SELECT rollup, max(as_of) AS as_of, max(finished_at) AS finished_at "
                                "FROM stats.rollup_run GROUP BY rollup ORDER BY rollup").fetchall()
        residual: dict[int, int] = {}
        for d in drift:
            if d["residual"] is not None:
                residual[d["chat_id"]] = residual.get(d["chat_id"], 0) + int(d["residual"])
        rows = [dict(r, residual_24h=residual.get(r["chat_id"])) for r in cov]
        rows.sort(key=lambda r: (r["reported"] is None, -(r["reported"] or 0), r["title"] or "", r["chat_id"]))
        return self.render("overview.html", nav="/", title="Overview", as_of=as_of, since=since, rows=rows,
                           rollups={r[0]: (r[1], r[2]) for r in runs})

    # --------------------------------------------------------------------------------------------------
    # Overlap matrix and drill-down
    # --------------------------------------------------------------------------------------------------

    def overlap(self, req: Request, _m: dict[str, str]) -> Response:
        with self.db.read() as conn:
            chats = common.load_chats(conn)
            scope = _scope(req, chats)
            report = reports.build_report(conn, "1", as_of=None, scope=scope, params={}, rollup=True,
                                          window=lambda as_of, since: None)
            published = publication(conn)  # the same snapshot as the matrix
        result = report.result.dicts()
        generation = published[1] if result and published else None
        order = report.scope
        cells = {(r["chat_a"], r["chat_b"]): r for r in result}
        sizes = {a: cells[(a, a)]["size_a"] for a in order if (a, a) in cells}
        titles = common.titles_of(chats)
        grid = []
        for a in order:
            line = []
            for b in order:
                r = cells.get((a, b))
                if r is None:
                    line.append(None)
                    continue
                j = float(r["jaccard"] or 0)
                line.append({"shared": r["shared"], "pct_row": r["pct_of_a"], "pct_col": r["pct_of_b"],
                             "jaccard": r["jaccard"], "shade": min(10, max(0, round(j * 10))), "diagonal": a == b,
                             "href": "/overlap/pair?" + urlencode({"a": a, "b": b, "gen": published[0]})
                             if generation else None,
                             "tip": (f"{titles.get(a, a)}: {r['shared']} members" if a == b else
                                     f"{titles.get(a, a)} and {titles.get(b, b)}: {r['shared']} shared, "
                                     f"{r['pct_of_a']} % of the row, {r['pct_of_b']} % of the column, "
                                     f"Jaccard {r['jaccard']}")})
            grid.append((a, line))
        return self.render("overlap.html", nav="/overlap", title="Overlap", header=header_of(report),
                           chats=[c for c in chats if c.tracked], chosen=set(scope or []), order=order,
                           titles=titles, grid=grid, sizes=sizes, generation=generation)

    def pair(self, req: Request, _m: dict[str, str]) -> Response:
        a = _int(req.arg("a"), "a")
        b = _int(req.arg("b"), "b")
        if a is None or b is None:
            raise HttpError(400, "a pair needs chats a and b")
        side = req.arg("side", "shared")
        if side not in SIDES:
            raise HttpError(400, f"side must be one of {', '.join(SIDES)}")
        gen = _int(req.arg("gen"), "gen")
        if gen is None:
            raise HttpError(400, "open a pair from the overlap matrix: the request carries the matrix's rollup run")
        page_no = _int(req.arg("page"), "page") or 0
        if page_no < 0:
            raise HttpError(400, "page must not be negative")
        with self.db.read() as conn:
            chats = common.load_chats(conn)
            titles = common.titles_of(chats)
            if not any(c.id == a and c.tracked for c in chats) or not any(c.id == b and c.tracked for c in chats):
                raise HttpError(400, "both chats of a pair must be tracked")
            # The published generation, in the same snapshot as the counts and the list below.
            published = publication(conn)
            if published is None or published[0] != gen:
                return self.render("stale.html", 409, nav="/overlap", title="Overlap refreshed",
                                   published=published[1] if published else None,
                                   back="/overlap?" + urlencode([("chat", a), ("chat", b)]))
            cell = {(r["chat_a"], r["chat_b"]): r for r in sql.run(conn, "1", scope=[a, b]).dicts()}[(a, b)]
            # Paged by position in the generation's list (tg.user_chats changes only with a new R2 publication,
            # which this request refused above), so a page link carries no user id.
            page = sql.run_page(conn, "1b", offset=page_no * PAGE_SIZE, limit=PAGE_SIZE + 1, chat=a, chat_b=b,
                                side=side, after_user=0, page_size=ALL_ROWS).dicts()
        more = len(page) > PAGE_SIZE
        page = page[:PAGE_SIZE]
        counts = {"shared": cell["shared"], "only_a": cell["size_a"] - cell["shared"],
                  "only_b": cell["size_b"] - cell["shared"]}
        base = {"a": a, "b": b, "gen": gen}
        nxt = "/overlap/pair?" + urlencode({**base, "side": side, "page": page_no + 1}) if more else None
        prev = "/overlap/pair?" + urlencode({**base, "side": side, "page": page_no - 1}) if page_no else None
        return self.render("pair.html", nav="/overlap", title="Pair", a=a, b=b, titles=titles, cell=cell,
                           counts=counts, side=side, sides=SIDES, people=page, base=base, next=nxt, prev=prev,
                           generation=published[1], start=page_no * PAGE_SIZE)

    # --------------------------------------------------------------------------------------------------
    # User lookup
    # --------------------------------------------------------------------------------------------------

    def user(self, req: Request, _m: dict[str, str]) -> Response:
        """GET: the empty form. POST: the lookup, rendered from the POST (no redirect): a user id or @name never
        goes into a URL, where proxy logs and Referers would keep it."""
        if req.method != "POST" and (req.query.get("q") or req.query.get("as_of")):
            raise HttpError(400, "a lookup is sent as a form (POST): user ids never go into a URL")
        q = req.arg("q")
        at = _time(req.arg("as_of"), "as_of")
        if q is None:
            return self.render("user.html", nav="/user", title="User", q="", found=None, problem=None)
        with self.db.read() as conn:
            chats = common.load_chats(conn)
            try:
                user_id = common.resolve_user(conn, q)
            except CliError as e:
                return self.render("user.html", 404, nav="/user", title="User", q=q, found=None, problem=str(e))
            as_of = sql.values(conn, **({"as_of": at.isoformat()} if at else {}))["as_of_ts"]
            as_of_time = _ts(conn, as_of)
            profile = conn.execute("SELECT user_id, username, first_name, last_name, is_bot, is_deleted, is_premium, "
                                   "language_code, first_seen_at, last_seen_at FROM tg.tg_user WHERE user_id = %s",
                                   (user_id,)).fetchone()
            stays = sql.run(conn, "2b", as_of=as_of, user=user_id)
            entered = sql.run(conn, "2c", as_of=as_of, user=user_id)
            activity = sql.run(conn, "2d", as_of=as_of, user=user_id)
        titles = common.titles_of(chats)
        now_in = [r for r in stays.dicts() if r["now"] == "in"]
        return self.render("user.html", nav="/user", title=f"User {user_id}", q=q, found=user_id, problem=None,
                           as_of=as_of_time, profile=profile, now_in=now_in, stays=table(stays, titles),
                           entered=table(entered, titles), activity=table(activity, titles))

    # --------------------------------------------------------------------------------------------------
    # Campaigns
    # --------------------------------------------------------------------------------------------------

    def campaigns(self, req: Request, _m: dict[str, str], *, problem: str | None = None,
                  form: dict[str, Any] | None = None, status: int = 200) -> Response:
        with self.db.read() as conn:
            chats = common.load_chats(conn)
            listed = conn.execute(campaigns_cmd._LIST)
            result = sql.Result([d.name for d in listed.description], listed.fetchall())
        return self.render("campaigns.html", status, nav="/campaigns", title="Campaigns", list=table(result, {}),
                           chats=[c for c in chats if c.tracked], kinds=campaigns_cmd.KINDS, problem=problem,
                           form=form or {})

    def campaign_create(self, req: Request, m: dict[str, str]) -> Response:
        form = {"name": req.arg("name", ""), "kind": req.arg("kind", "giveaway"), "starts": req.arg("starts", ""),
                "draw": req.arg("draw", ""), "note": req.arg("note", ""), "winners": req.arg("winners", ""),
                "only_new_members": req.arg("only_new_members") is not None, "chats": req.args("chat")}
        try:
            if not form["name"]:
                raise HttpError(400, "the campaign needs a name")
            if form["kind"] not in campaigns_cmd.KINDS:
                raise HttpError(400, f"kind must be one of {', '.join(campaigns_cmd.KINDS)}")
            starts = _time(form["starts"] or None, "starts")
            draw = _time(form["draw"] or None, "draw")
            if starts is None or draw is None:
                raise HttpError(400, "the campaign needs a start and a draw time")
            if starts >= draw:
                raise HttpError(400, "the start must be before the draw")
            winners = _int(form["winners"] or None, "winners")
            if winners is not None and winners < 1:
                raise HttpError(400, "winners must be at least 1")
            with self.db.write() as conn:
                chats = common.load_chats(conn)
                picked = _scope(req, chats)
                if not picked:
                    raise HttpError(400, "pick at least one chat")
                campaign_id = campaigns_cmd.insert_campaign(
                    conn, name=form["name"], kind=form["kind"], starts=starts, draw=draw,
                    only_new_members=form["only_new_members"], winners=winners, note=form["note"] or None,
                    chat_ids=picked)
        except HttpError as e:
            if e.status != 400:
                raise
            return self.campaigns(req, m, problem=e.message, form=form, status=400)
        log.info("web: campaign %d added", campaign_id)
        return redirect(f"/campaigns/{campaign_id}")

    def campaign(self, req: Request, m: dict[str, str], *, problem: str | None = None, status: int = 200) -> Response:
        campaign_id = int(m["id"])
        as_of = _time(req.arg("as_of"), "as_of") if req.method == "GET" else None
        hours = _option("hours", req.arg("hours")) if req.method == "GET" else None
        with self.db.read() as conn:
            chats = common.load_chats(conn)
            row = conn.execute("SELECT campaign_id, name, kind, origin, starts_at, draw_at, only_new_members, "
                               "winner_count, note FROM tg.campaign WHERE campaign_id = %s", (campaign_id,)).fetchone()
            if row is None:
                raise HttpError(404, f"no campaign {campaign_id}")
            try:
                per_chat = campaigns_cmd.campaign_report(conn, chats, campaign_id, "4a", as_of=as_of, hours=hours)
                per_person = campaigns_cmd.campaign_report(conn, chats, campaign_id, "4b", as_of=per_chat.as_of,
                                                           hours=hours)
            except CliError as e:
                raise HttpError(400, str(e)) from None
            links = conn.execute("SELECT invite_link_id, chat_id, name, first_seen_at FROM tg.invite_link "
                                 "WHERE campaign_id = %s AND made_by_bot ORDER BY invite_link_id",
                                 (campaign_id,)).fetchall()
        titles = common.titles_of(chats)
        csv_form = {k: v for k, v in (("as_of", per_chat.as_of.isoformat()), ("hours", hours)) if v is not None}
        return self.render("campaign.html", status, nav="/campaigns", title=f"Campaign {campaign_id}", campaign=row,
                           header=header_of(per_chat), per_chat=transposed(per_chat.result, titles, "chat_id"),
                           per_person=transposed(per_person.result, titles, "campaign_id"), links=links, titles=titles,
                           chats=[c for c in chats if c.id in per_chat.scope] or [c for c in chats if c.tracked],
                           csv_form=csv_form, problem=problem, name_max=campaigns_cmd.NAME_MAX,
                           hours=hours)

    def entrants_csv(self, req: Request, m: dict[str, str]) -> Response:
        campaign_id = int(m["id"])
        as_of = _time(req.arg("as_of"), "as_of")
        hours = _option("hours", req.arg("hours"))
        with self.db.read() as conn:
            chats = common.load_chats(conn)
            try:
                report = campaigns_cmd.campaign_report(conn, chats, campaign_id, "4c", as_of=as_of, hours=hours)
            except CliError as e:
                raise HttpError(404, str(e)) from None
        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(report.result.columns)
        for r in report.result.rows:
            writer.writerow([common.csv_cell(v) for v in r])
        stamp = report.as_of.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
        return Response(200, out.getvalue().encode(), "text/csv; charset=utf-8",
                        [("Content-Disposition", f'attachment; filename="campaign-{campaign_id}-entrants-{stamp}.csv"')])

    def link_create(self, req: Request, m: dict[str, str]) -> Response:
        """createChatInviteLink with the link's name; the link is shown once and stored only as its HMAC digest (D8).
        Never logged, never in a URL (the response to a POST, no redirect). Once Telegram has made the link, the
        page is built only from this request's values, so no later failure (storing it, a report) can hide it."""
        campaign_id = int(m["id"])
        name = (req.arg("name") or "").strip()
        chat_id = _int(req.arg("chat"), "chat")
        if not 1 <= len(name) <= campaigns_cmd.NAME_MAX:
            return self.campaign(req, m, problem=f"the link's name must be 1-{campaigns_cmd.NAME_MAX} characters "
                                                 "(Telegram's limit)", status=400)
        with self.db.read() as conn:  # the lock is released before any other page is built
            chats = common.load_chats(conn)
            chat = next((c for c in chats if c.id == chat_id and c.tracked), None)
            row = conn.execute("SELECT name FROM tg.campaign WHERE campaign_id = %s", (campaign_id,)).fetchone()
            state = conn.execute("SELECT bot_id FROM tg.bot_state").fetchone()
        if row is None:
            raise HttpError(404, f"no campaign {campaign_id}")
        if chat is None:
            return self.campaign(req, m, problem="pick a tracked chat for the link", status=400)
        # No transaction is open during the Bot API call.
        try:
            with self.api_factory(self.cfg) as api:
                made = api.create_chat_invite_link(chat.id, name=name)
        except ApiError as e:
            return self.campaign(req, m, problem=f"createChatInviteLink failed: {e}", status=502)
        link = made.get("invite_link") if isinstance(made, dict) else None
        if not isinstance(link, str) or not link:
            return self.campaign(req, m, problem="createChatInviteLink returned no invite_link", status=502)
        shown = {"link": link, "name": made.get("name") or name, "chat": chat, "campaign_id": campaign_id,
                 "campaign": row[0], "id": None, "problem": None}
        try:
            with self.db.write() as conn:
                shown["id"] = campaigns_cmd.insert_link(conn, self.cfg, chat.id, link, made, name=name,
                                                        campaign_id=campaign_id, bot_id=state[0] if state else None)
        except Exception as e:  # the link exists at Telegram and cannot be shown again: show it with the failure
            if isinstance(e, db.StaleGeneration):
                self.fatal(1, str(e))
            reason = e.message if isinstance(e, HttpError) else db_error_line(e)
            log.error("web: a bot invite link for chat %d was made but not stored: %s", chat.id, type(e).__name__)
            shown["problem"] = ("the link was created at Telegram but could not be stored, so joins through it will "
                                f"not be attributed: {reason}")
            return self.render("link_made.html", 500, nav="/campaigns", title="Invite link", made=shown)
        log.info("web: bot invite link %d made for chat %d, campaign %d", shown["id"], chat.id, campaign_id)
        return self.render("link_made.html", nav="/campaigns", title="Invite link", made=shown)

    # --------------------------------------------------------------------------------------------------
    # Reports
    # --------------------------------------------------------------------------------------------------

    def report_index(self, req: Request, _m: dict[str, str]) -> Response:
        return self.render("reports.html", nav="/reports", title="Reports", specs=reports.PLAIN)

    def report(self, req: Request, m: dict[str, str]) -> Response:
        spec = next((s for s in reports.PLAIN if s.name == m["name"]), None)
        if spec is None:
            raise HttpError(404, f"no report {m['name']!r}")
        as_of = _time(req.arg("as_of"), "as_of")
        offset = _offset(req)
        params = {n: _option(n, req.arg(n)) for n in spec.params}
        params = {k: v for k, v in params.items() if v is not None}
        if "since" in params:
            params["since"] = _time(params["since"], "since").isoformat()
        with self.db.read() as conn:
            chats = common.load_chats(conn)
            scope = _scope(req, chats)
            try:
                report = reports.build_report(conn, spec.block, as_of=as_of, scope=scope, params=params,
                                              rollup=spec.rollup, page=(offset, REPORT_PAGE + 1),
                                              **reports.header_parts(spec, conn, params))
            except (CliError, ValueError) as e:
                raise HttpError(400, str(e)) from None
        return self.render("report.html", nav="/reports", title=spec.name, spec=spec, header=header_of(report),
                           table=paged_table(req, report, common.titles_of(chats), offset, ("chat", *spec.params)),
                           chats=[c for c in chats if c.tracked], chosen=set(scope or []),
                           form={n: req.arg(n, "") for n in ("as_of", *spec.params)},
                           options={n: reports.OPTIONS[n][1]["help"] for n in spec.params}, action=f"/reports/{spec.name}")

    def asof(self, req: Request, _m: dict[str, str]) -> Response:
        """Item 16 as the CLI's `asof`: counts (16a), the members (16b), the end-of-day count (16d), or the pair
        matrix of the `among` chats at that moment (16c, a batch of up to a minute at full scale)."""
        mode = req.arg("mode", "counts")
        if mode not in ASOF_MODES:
            raise HttpError(400, f"mode must be one of {', '.join(ASOF_MODES)}")
        block = ASOF_MODES[mode]
        chat_id = _int(req.arg("chat"), "chat")
        day = _time(req.arg("date"), "date")
        as_of = _time(req.arg("as_of"), "as_of")
        offset = _offset(req)
        form = {"chat": req.arg("chat", ""), "date": req.arg("date", ""), "as_of": req.arg("as_of", ""), "mode": mode}
        with self.db.read() as conn:
            chats = common.load_chats(conn)
            tracked = [c for c in chats if c.tracked]
            among = _scope(req, chats, "among")
            page = dict(nav="/reports", title="asof", chats=tracked, form=form, chosen=set(among or []))
            if day is None or (chat_id is None and mode != "matrix"):
                return self.render("asof.html", header=None, table=None, **page)
            if chat_id is not None and chat_id not in {c.id for c in tracked}:
                raise HttpError(400, f"chat {chat_id} is not a tracked chat")
            params: dict[str, Any] = {"day": day.isoformat()}
            window: Callable[[dt.datetime, dt.datetime], Window | None] = lambda a, s: Window(day, day, point=True)  # noqa: E731
            as_of_notes = None
            scope: list[int] | None = [chat_id] if chat_id is not None else None
            if mode == "matrix":
                scope = among  # the CLI's --matrix: the --among chats (default every tracked chat)
            elif mode == "eod":
                start = dt.datetime.combine(day.astimezone(UTC).date(), dt.time(), UTC)
                end = start + dt.timedelta(days=1)
                window = lambda a, s: Window(start, end)  # noqa: E731
                as_of_notes = lambda a: () if end <= a else (  # noqa: E731
                    f"{start.date()} has not ended at as_of: no end-of-day count (the day of as_of is left out)",)
            else:
                params["chat"] = chat_id
            report = reports.build_report(conn, block, as_of=as_of, scope=scope, params=params, window=window,
                                          as_of_notes=as_of_notes, page=(offset, REPORT_PAGE + 1))
        return self.render("asof.html", header=header_of(report),
                           table=paged_table(req, report, common.titles_of(chats), offset, ASOF_INPUTS,
                                             list_inputs=("among",)), **page)

    def coverage(self, req: Request, _m: dict[str, str]) -> Response:
        gaps = req.arg("gaps") is not None
        as_of_in = _time(req.arg("as_of"), "as_of")
        since_in = _time(req.arg("since"), "since")
        with self.db.read() as conn:
            chats = common.load_chats(conn)
            scope = _scope(req, chats)
            params: dict[str, Any] = {}
            if as_of_in:
                params["as_of"] = as_of_in.isoformat()
            if since_in:
                params["since"] = since_in.isoformat()
            if scope is not None:
                params["scope"] = scope
            vals = sql.values(conn, **params)
            as_of, since = _ts(conn, vals["as_of_ts"]), _ts(conn, vals["since_ts"])
            if since > as_of:
                raise HttpError(400, "since is after as_of")
            in_scope = scope if scope is not None else reports._scope_ids(vals["scope_ids"])
            window = Window(since, as_of)
            header = Header("coverage: when the bot was admin, drift, gaps", as_of, window, False, None, (),
                            common.coverage_rows(conn, in_scope, as_of, window))
            result = reports.coverage_table(conn, as_of, since, in_scope, gaps=gaps)
        return self.render("report.html", nav="/reports", title="coverage", spec=None, header=header,
                           table=table(result, common.titles_of(chats)), chats=[c for c in chats if c.tracked],
                           chosen=set(scope or []),
                           form={"as_of": req.arg("as_of", ""), "since": req.arg("since", ""), "gaps": gaps},
                           options={"since": "start of the window for the gap flags (default: as of minus 90 days)"},
                           action="/reports/coverage")


def publication(conn: psycopg.Connection) -> tuple[int, dt.datetime] | None:
    """The published audience rollup: its stats.rollup_run row (id, as_of), written by R2 in the transaction that
    publishes the matrix and tg.user_chats. The id names one publication; two runs with the same as_of differ.
    The latest is the highest id, not the latest finished_at (a wall clock can step back): R2 takes its id with
    its last statement, and two R2 runs are ordered by the lock of the tg.user_chats refresh, their first write,
    so the run that commits later inserts later and has the higher id."""
    row = conn.execute("SELECT rollup_run_id, as_of FROM stats.rollup_run WHERE rollup = 'audience' "
                       "ORDER BY rollup_run_id DESC LIMIT 1").fetchone()
    return (int(row[0]), row[1]) if row else None


def _ts(conn: psycopg.Connection, value: str) -> dt.datetime:
    return conn.execute("SELECT %s::timestamptz", (value,)).fetchone()[0]
