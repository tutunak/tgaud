"""The web UI (tgaud.web) on the history fixture (docs/design/verify.md 4), called as a WSGI app.

"Now" is 2026-11-23 12:00 UTC: every page gets it as `as_of`, R1 and R2 ran at it (clidb.report_db). Each page is
checked by the numbers verify.md expects for its block, read back from the HTML tables. Then the guards: the proxy
token (403 before any query), the same-origin check of POST forms, escaping of Telegram's text, the bot link
stored only as its digest, the audience generation of a drill-down, and serve with the web UI on.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import html.parser
import io
import logging
import re
from urllib.parse import parse_qsl, urlencode
from wsgiref.util import setup_testing_defaults

import psycopg
import pytest
from botapi_fake import FakeBotApi
from clidb import A, AD, B, C, D, LINK_KEY, NOW, NOW_ARG, TOKEN, Builder, copy_db, ts  # noqa: F401
from clidb import fresh_db, report_db  # noqa: F401 (fixtures)
from test_serve import fake, serves  # noqa: F401 (fixtures)

from tgaud import config, sql
from tgaud.jobs import audience
from tgaud.web import WebApp, WebDb, pages

PROXY_TOKEN = "proxy-Token-0123456789-abcdef"
HOST = "tgaud.tail1234.ts.net"
SECRET_LINK = "https://t.me/+Zq9SecretWebLinkAbCd"


def make_config(pg, dsn: str) -> config.Config:
    name = psycopg.conninfo.conninfo_to_dict(dsn)["dbname"]
    return config.load({"TGAUD_DATABASE_URL": pg.tenant_url(name), "TGAUD_DATABASE_PASSWORD": pg.tenant_password,
                        "TGAUD_BOT_TOKEN": TOKEN, "TGAUD_LINK_HMAC_KEY": LINK_KEY,
                        "TGAUD_WEB_LISTEN": "127.0.0.1:8097", "TGAUD_WEB_PROXY_TOKEN": PROXY_TOKEN,
                        "TGAUD_ACTIVATION_ID": "act-web"})


class Page:
    def __init__(self, status: int, headers: dict[str, str], body: bytes) -> None:
        self.status, self.headers, self.body = status, headers, body
        self.text = body.decode("utf-8", "replace")

    def tables(self) -> list[list[list[str]]]:
        p = _Tables()
        p.feed(self.text)
        return p.tables

    def table_with(self, heading: str) -> list[dict[str, str]]:
        """The first table whose header row has this column, as dicts by the header's cells."""
        for t in self.tables():
            if t and heading in t[0]:
                return [dict(zip(t[0], r)) for r in t[1:]]
        raise AssertionError(f"no table with column {heading!r}:\n{self.text}")


class _Tables(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._stack: list[list[list[str]]] = []
        self._cell: list[str] | None = None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._stack.append([])
        elif tag == "tr" and self._stack:
            self._stack[-1].append([])
        elif tag in ("td", "th") and self._stack:
            self._cell = []
            col = dict(attrs).get("data-col")  # a result column: its name as the block returns it
            if col is not None:
                self._stack[-1][-1].append(col)
                self._cell = None

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._cell is not None and self._stack and self._stack[-1]:
            self._stack[-1][-1].append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "table" and self._stack:
            self.tables.append(self._stack.pop())

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


class Client:
    def __init__(self, app: WebApp) -> None:
        self.app = app

    def request(self, method: str, path: str, *, params=None, data=None, token: str | None = PROXY_TOKEN,
                headers: dict[str, str] | None = None) -> Page:
        environ: dict = {}
        setup_testing_defaults(environ)
        body = urlencode(data or {}, doseq=True).encode()
        environ.update({"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": urlencode(params or {}, doseq=True),
                        "HTTP_HOST": HOST, "wsgi.input": io.BytesIO(body), "CONTENT_LENGTH": str(len(body)),
                        "CONTENT_TYPE": "application/x-www-form-urlencoded"})
        if token is not None:
            environ["HTTP_X_TGAUD_PROXY_TOKEN"] = token
        environ["HTTP_X_FORWARDED_PROTO"] = "https"  # Caddy terminates TLS and says so
        for k, v in (headers or {}).items():
            environ["HTTP_" + k.upper().replace("-", "_")] = v
        got: dict = {}

        def start_response(status, hdrs):
            got["status"] = int(status.split(" ", 1)[0])
            got["headers"] = dict(hdrs)

        out = b"".join(self.app(environ, start_response))
        return Page(got["status"], got["headers"], out)

    def get(self, path: str, **params) -> Page:
        return self.request("GET", path, params=params)

    def post(self, path: str, data, *, origin: str | None = f"https://{HOST}", **kw) -> Page:
        headers = dict(kw.pop("headers", {}) or {})
        if origin is not None:
            headers["Origin"] = origin
        return self.request("POST", path, data=data, headers=headers, **kw)


class Opened:
    """Connections the web app opened (tgaud.web.WebDb's connect), so a test can count them."""

    def __init__(self, cfg: config.Config) -> None:
        self.cfg = cfg
        self.count = 0

    def __call__(self) -> psycopg.Connection:
        self.count += 1
        return psycopg.connect(self.cfg.dsn("web"), autocommit=False)


def make_client(pg, dsn: str, **kw) -> tuple[Client, WebDb, Opened]:
    cfg = make_config(pg, dsn)
    opened = Opened(cfg)
    database = WebDb(opened)
    app = WebApp(cfg, database, PROXY_TOKEN.encode(), **kw)
    return Client(app), database, opened


@pytest.fixture
def web(pg, report_db):
    client, database, _ = make_client(pg, report_db)
    yield client
    database.close()


@pytest.fixture
def fresh_web(pg, fresh_db):
    fake = FakeBotApi(token=TOKEN, bot_id=5000)
    client, database, opened = make_client(pg, fresh_db, api_factory=lambda cfg: fake.api())
    client.fake, client.db, client.opened, client.dsn = fake, database, opened, fresh_db
    yield client
    database.close()


def ok(page: Page) -> Page:
    assert page.status == 200, page.text
    return page


def column(page: Page, heading: str) -> dict[str, str]:
    """A transposed table (one row per measure): the column with this heading, by measure."""
    for t in page.tables():
        if t and heading in t[0]:
            i = t[0].index(heading)
            return {r[0]: r[i] for r in t[1:]}
    raise AssertionError(f"no column {heading!r}:\n{page.text}")


def row(rows: list[dict[str, str]], **want) -> dict[str, str]:
    found = [r for r in rows if all(str(v) in r[k] if k == "Chat" else r[k] == str(v) for k, v in want.items())]
    assert len(found) == 1, (want, rows)
    return found[0]


def q(dsn: str, query: str, *args):
    with psycopg.connect(dsn) as c:
        return c.execute(query, args).fetchall()


# ------------------------------------------------------------------------------------------------------
# Pages and the numbers of verify.md 4
# ------------------------------------------------------------------------------------------------------


def test_overview_shows_every_tracked_chat_with_counts_coverage_and_rollup_times(web):
    page = ok(web.get("/", as_of=NOW_ARG))
    rows = page.table_with("Coverage")
    tracked = q_tracked(web)
    assert len(rows) == tracked
    a = row(rows, Chat="Alpha news")
    assert (a["Reported"], a["Known"], a["Coverage"]) == ("15", "12", "80.0 %")
    assert "2026-11-23 12:00:00 UTC" in page.text  # the as-of time and both rollups' as_of
    assert "Daily rollup" in page.text and "Audience rollup" in page.text
    assert page.headers["Cache-Control"] == "no-store"
    assert "script-src" not in page.headers["Content-Security-Policy"] and "default-src 'none'" in page.headers["Content-Security-Policy"]


def q_tracked(web) -> int:
    with web.app.db.read() as conn:
        return conn.execute("SELECT count(*) FROM tg.chat WHERE tracked").fetchone()[0]


def matrix(page: Page) -> dict[tuple[str, str], str]:
    t = next(t for t in page.tables() if t and t[0] and t[0][0] == "row \\ column")
    cols = [re.sub(r"^\d+ ", "", c) for c in t[0][1:]]
    out = {}
    for r in t[1:]:
        name = re.sub(r"^\d+ ", "", r[0])
        for col, cell in zip(cols, r[1:]):
            out[(name, col)] = cell
    return out


def test_overlap_matrix_has_shared_counts_shares_and_jaccard_shades(web):
    page = ok(web.get("/overlap", chat=[str(c) for c in AD]))
    m = matrix(page)
    titles = dict(q_titles(web))
    ta, tb, tc, td = (titles[c] for c in AD)
    shared = {k: v.split(" ", 1)[0] for k, v in m.items()}
    assert [shared[(ta, tb)], shared[(ta, tc)], shared[(ta, td)], shared[(tb, tc)], shared[(tb, td)], shared[(tc, td)]] \
        == ["4", "2", "1", "3", "1", "1"]
    assert [shared[(t, t)] for t in (ta, tb, tc, td)] == ["10", "6", "5", "2"]
    assert m[(ta, tb)] == "4 40.00 % · 66.67 %"  # % of the row (A), % of the column (B)
    assert m[(tb, ta)] == "4 66.67 % · 40.00 %"
    assert 'title="Alpha news and ' in page.text and "Jaccard 0.3333" in page.text
    assert 'class="j3"' in page.text  # Jaccard 0.3333 -> shade 3 of 10
    assert "Audience rollup" in page.text and "2026-11-23 12:00:00 UTC" in page.text
    assert "coverage" in page.text.lower()


def q_titles(web):
    with web.app.db.read() as conn:
        return conn.execute("SELECT chat_id, title FROM tg.chat").fetchall()


def pair_link(page: Page, a: int, b: int) -> dict[str, str]:
    m = re.search(r'href="/overlap/pair\?a=' + re.escape(str(a)) + r"&amp;b=" + re.escape(str(b)) + r'&amp;gen=(\d+)"',
                  page.text)
    assert m, page.text
    return {"a": str(a), "b": str(b), "gen": m.group(1)}


def people(page: Page) -> list[str]:
    return [r["User id"] for r in page.table_with("User id")] if "User id" in page.text else []


def test_pair_drill_down_lists_exactly_the_people_of_its_cell(web):
    link = pair_link(ok(web.get("/overlap", chat=[str(A), str(B)])), A, B)
    page = ok(web.get("/overlap/pair", **link))
    assert re.search(r"in both <b>4</b>", page.text) and re.search(r"only in A <b>6</b>", page.text) \
        and re.search(r"only in B <b>2</b>", page.text)
    assert len(people(page)) == 4
    assert len(people(ok(web.get("/overlap/pair", **link, side="only_a")))) == 6
    assert len(people(ok(web.get("/overlap/pair", **link, side="only_b")))) == 2
    diag = ok(web.get("/overlap/pair", a=str(A), b=str(A), gen=link["gen"]))
    assert len(people(diag)) == 10
    # the list is block 1b over tg.user_chats: the same people as a direct read of the snapshot
    with web.app.db.read() as conn:
        want = sorted(str(r[0]) for r in conn.execute(
            "SELECT user_id FROM tg.user_chats WHERE chat_ids @> %s::bigint[]", ([A, B],)))
    assert sorted(people(page)) == want
    assert web.get("/overlap/pair", **link, side="nobody").status == 400
    assert web.get("/overlap/pair", a=str(A), b=str(B)).status == 400  # no gen: not from a matrix page


def follow(web, href: str) -> Page:
    path, _, query = href.partition("?")
    return web.request("GET", path, params=parse_qsl(query))


def test_a_join_after_the_refresh_changes_neither_cell_nor_list_until_the_next_one(fresh_web, monkeypatch):
    web = fresh_web
    monkeypatch.setattr(pages, "PAGE_SIZE", 3)
    link = pair_link(ok(web.get("/overlap", chat=[str(A), str(B)])), A, B)
    first = ok(web.get("/overlap/pair", **link))
    assert len(people(first)) == 3 and "Next 3" in first.text
    with psycopg.connect(web.dsn) as c:  # a new person joins A and B after the rollup
        b = Builder(c)
        b.stay(A, 8801, ts("2026-11-23T12:30:00"), update_id=880100)
        b.stay(B, 8801, ts("2026-11-23T12:31:00"), update_id=880200)
        c.commit()
    m = matrix(ok(web.get("/overlap", chat=[str(A), str(B)])))
    assert m[("Alpha news", next(k[1] for k in m if k[1] != "Alpha news"))].startswith("4 ")
    again = ok(web.get("/overlap/pair", **link))
    assert people(again) == people(first) and "in both <b>4</b>" in again.text
    nxt = re.search(r'href="(/overlap/pair\?[^"]+page=1)"', first.text).group(1).replace("&amp;", "&")
    second = ok(follow(web, nxt))
    assert len(people(second)) == 1 and "8801" not in second.text
    # an R2 publish between the matrix and the click, and between two pages: no answer from the new population
    with psycopg.connect(web.dsn) as c:
        audience.refresh(c, NOW + dt.timedelta(hours=1))
    stale = web.get("/overlap/pair", **link)
    assert stale.status == 409 and "Reload the overlap matrix" in stale.text and "8801" not in stale.text
    stale_next = follow(web, nxt)
    assert stale_next.status == 409
    fresh_link = pair_link(ok(web.get("/overlap", chat=[str(A), str(B)])), A, B)
    assert fresh_link["gen"] != link["gen"]
    now = ok(web.get("/overlap/pair", **fresh_link))
    assert "in both <b>5</b>" in now.text


def test_a_new_publication_at_the_same_as_of_is_a_new_generation(fresh_web, monkeypatch):
    """Two stays dated before the cutoff, then R2 again at the same as_of: the old links are not answered from it."""
    web = fresh_web
    monkeypatch.setattr(pages, "PAGE_SIZE", 3)
    link = pair_link(ok(web.get("/overlap", chat=[str(A), str(B)])), A, B)
    first = ok(web.get("/overlap/pair", **link))
    nxt = re.search(r'href="(/overlap/pair\?[^"]+page=1)"', first.text).group(1).replace("&amp;", "&")
    with psycopg.connect(web.dsn) as c:
        b = Builder(c)
        b.stay(A, 8802, ts("2026-11-20T10:00:00"), update_id=880300)
        b.stay(B, 8802, ts("2026-11-20T10:05:00"), update_id=880400)
        c.commit()
        audience.refresh(c, NOW)  # the same cutoff as the published run
    for href in ("/overlap/pair?" + urlencode(link), nxt):
        stale = follow(web, href)
        assert stale.status == 409 and "8802" not in stale.text, stale.text
    fresh = pair_link(ok(web.get("/overlap", chat=[str(A), str(B)])), A, B)
    assert int(fresh["gen"]) > int(link["gen"])
    assert "in both <b>5</b>" in ok(web.get("/overlap/pair", **fresh)).text


def test_user_lookup_shows_chats_stays_campaigns_and_activity(web):
    page = ok(web.post("/user", {"q": "1001"}))
    assert "In now: 4 chats" in page.text
    assert len(page.table_with("start_source")) == 4
    page = ok(web.post("/user", {"q": "1006"}))
    stays = page.table_with("start_source")
    assert len([r for r in stays if r["chat_id"] == str(A)]) == 2
    with web.app.db.read() as conn:
        entrant = sql.run(conn, "4c", as_of=NOW_ARG, campaign=1).dicts()[0]
        writer = conn.execute("SELECT user_id, sum(messages) FROM tg.activity_daily GROUP BY user_id "
                              "ORDER BY user_id LIMIT 1").fetchone()
    page = ok(web.post("/user", {"q": str(entrant["user_id"])}))
    entered = page.table_with("campaign")
    assert any(r["campaign_id"] == "1" and r["joined_at"] == entrant["joined_at"].strftime("%Y-%m-%d %H:%M:%S")
               for r in entered)
    page = ok(web.post("/user", {"q": str(writer[0])}))
    assert sum(int(r["messages"]) for r in page.table_with("days_active")) == writer[1]
    missing = web.post("/user", {"q": "@nobody_here"})
    assert missing.status == 404 and "no user with username" in missing.text


def test_campaign_page_has_item_4_per_chat_and_per_person_and_the_entrant_csv(web):
    page = ok(web.get("/campaigns/1", as_of=NOW_ARG))
    a = column(page, f"Alpha news ({A})")  # item 4a is shown with one column per chat
    assert [a[k] for k in ("entrants", "first_time", "returners", "eligible", "quick_exit", "in_at_draw",
                           "exit_after_draw", "still_in")] == ["6", "5", "1", "5", "3", "4", "2", "2"]
    assert column(page, f"Alpha chat ({B})")["entrants"] == "3"
    p = column(page, "campaign 1")  # item 4b: one column per campaign
    assert [p[k] for k in ("persons", "multi_chat", "first_time", "eligible", "quick_exit", "in_at_draw",
                           "exit_after_draw", "still_in")] == ["8", "1", "7", "7", "4", "5", "2", "3"]
    assert "Gap flags" in page.text and "2026-11-01" in page.text  # the header: coverage, flags, the window
    listed = ok(web.get("/campaigns"))
    assert len(listed.table_with("campaign_id")) == 2  # G1 (owner), G2 (Telegram)
    csv_page = ok(web.post("/campaigns/1/entrants.csv", {"as_of": NOW_ARG}))
    assert csv_page.headers["Content-Type"].startswith("text/csv")
    assert "attachment" in csv_page.headers["Content-Disposition"]
    lines = csv_page.text.strip().splitlines()
    assert lines[0].startswith("campaign_id,chat_id,user_id,username,joined_at")
    assert len(lines) - 1 == 9  # 6 entrants in A, 3 in B
    assert web.get("/campaigns/999").status == 404


SPEC_CHECKS = {
    "unique": lambda p: [row(p.table_with("exclusive"), title=t)["exclusive"] for t in ("Alpha news",)] == ["6"],
    "chats-per-user": lambda p: [r["users"] for r in p.table_with("chat_count")] == ["10", "3", "1", "1"],
}


@pytest.mark.parametrize("name", [s.name for s in pages.reports.PLAIN])
def test_every_cli_report_renders_with_its_header(web, name):
    spec = next(s for s in pages.reports.PLAIN if s.name == name)
    params = {"as_of": NOW_ARG, "chat": [str(c) for c in AD]}
    if "since" in spec.params:
        params["since"] = "2026-08-01"
    page = ok(web.get(f"/reports/{name}", **params))
    assert "As of" in page.text and "2026-11-23 12:00:00 UTC" in page.text
    assert "Chats in scope" in page.text and "Coverage and gap flags" in page.text
    if spec.rollup:
        assert "Audience rollup" in page.text
    if name in SPEC_CHECKS:
        assert SPEC_CHECKS[name](page), page.text


def test_report_numbers_match_verify(web):
    scope = {"chat": [str(c) for c in AD], "as_of": NOW_ARG}
    unique = ok(web.get("/reports/unique", **scope)).table_with("exclusive")
    assert [row(unique, title=t)["exclusive"] for t in ("Alpha news",)] == ["6"]
    assert row(unique, title="distinct total")["members"] == "15"
    flow = ok(web.get("/reports/flow", since="2026-08-01", as_of=NOW_ARG)).table_with("from_chat")
    ab = [r for r in flow if r["from_chat"].endswith(str(A)) and r["to_chat"].endswith(str(B))]
    assert [r["users"] for r in ab] == ["5"]
    counts = ok(web.get("/reports/asof", chat=str(A), date="2026-11-07", as_of=NOW_ARG)).table_with("known_in")
    assert counts[0]["known_in"] == "11"
    eod = ok(web.get("/reports/asof", chat=str(A), date="2026-11-07", mode="eod", as_of=NOW_ARG)).table_with("members_eod")
    assert eod[0]["members_eod"] == "12"
    members = ok(web.get("/reports/asof", chat=str(B), date="2026-11-07", mode="list", as_of=NOW_ARG)).table_with("state")
    assert sorted(r["state"] for r in members).count("in") == 6
    assert ok(web.get("/reports/coverage", as_of=NOW_ARG)).table_with("open_gaps")
    assert ok(web.get("/reports/coverage", as_of=NOW_ARG, since="2026-08-01", gaps="1")).table_with("reason")
    assert web.get("/reports/tenure", hours="-1").status == 400
    assert web.get("/reports/nope").status == 404
    assert ok(web.get("/reports"))


# ------------------------------------------------------------------------------------------------------
# Writes: campaigns and bot links
# ------------------------------------------------------------------------------------------------------


def campaign_form(**more):
    return {"name": "Winter giveaway", "kind": "giveaway", "starts": "2026-12-01T10:00", "draw": "2026-12-08T18:00",
            "chat": [str(A), str(B)], "note": "web", **more}


def test_campaign_create_from_the_form(fresh_web):
    web = fresh_web
    before = q(web.dsn, "SELECT count(*) FROM tg.campaign")[0][0]
    resp = web.post("/campaigns", campaign_form(only_new_members="1", winners="3"))
    assert resp.status == 303, resp.text
    cid = int(resp.headers["Location"].rsplit("/", 1)[1])
    got = q(web.dsn, "SELECT name, kind, origin, starts_at, draw_at, only_new_members, winner_count, note "
                     "FROM tg.campaign WHERE campaign_id = %s", cid)[0]
    assert got == ("Winter giveaway", "giveaway", "annotation", ts("2026-12-01T10:00:00"), ts("2026-12-08T18:00:00"),
                   True, 3, "web")
    assert sorted(q(web.dsn, "SELECT chat_id FROM tg.campaign_chat WHERE campaign_id = %s", cid)) == sorted([(A,), (B,)])
    assert ok(web.get(f"/campaigns/{cid}")).text.count("Winter giveaway") >= 1
    for bad in (campaign_form(draw="2026-11-30"), campaign_form(name=""), campaign_form(chat=[]),
                campaign_form(kind="tg_giveaway"), campaign_form(chat=["-1"]), campaign_form(starts="soon")):
        page = web.post("/campaigns", bad)
        assert page.status == 400, bad
    assert q(web.dsn, "SELECT count(*) FROM tg.campaign")[0][0] == before + 1


def test_post_from_another_origin_is_refused(fresh_web):
    web = fresh_web
    before = q(web.dsn, "SELECT count(*) FROM tg.campaign")[0][0]
    for origin, referer in (("https://evil.example", None), ("null", None), (None, None),
                            (None, "https://evil.example/campaigns"), (f"https://{HOST}.evil.example", None),
                            ("http://evil.example", f"https://{HOST}/campaigns"),
                            # the same host, another origin: the scheme or the effective port differs
                            (f"http://{HOST}", None), (f"http://{HOST}:443", None), (f"https://{HOST}:80", None),
                            (f"https://{HOST}:8443", None), (None, f"http://{HOST}/campaigns"),
                            (f"https://user@{HOST}", None), (f"ftp://{HOST}", None)):
        headers = {"Referer": referer} if referer else {}
        resp = web.post("/campaigns", campaign_form(), origin=origin, headers=headers)
        assert resp.status == 403 and "cross-site" in resp.text, (origin, referer)
    assert web.post("/campaigns/1/link", {"chat": str(A), "name": "x"}, origin="https://evil.example").status == 403
    assert q(web.dsn, "SELECT count(*) FROM tg.campaign")[0][0] == before
    assert not web.fake.calls_of("createChatInviteLink")
    # the own host: by Origin (any port spelling of the default), or by Referer when there is no Origin
    assert web.post("/campaigns", campaign_form(), origin=f"https://{HOST}:443").status == 303
    assert web.post("/campaigns", campaign_form(), origin=None,
                    headers={"Referer": f"https://{HOST}/campaigns"}).status == 303
    # a site the proxy serves over plain http (X-Forwarded-Proto: http): only http origins match
    plain = {"X-Forwarded-Proto": "http"}
    assert web.post("/campaigns", campaign_form(), origin=f"http://{HOST}", headers=plain).status == 303
    assert web.post("/campaigns", campaign_form(), origin=f"https://{HOST}", headers=plain).status == 403
    assert q(web.dsn, "SELECT count(*) FROM tg.campaign")[0][0] == before + 3


def test_link_page_shows_the_link_once_and_stores_only_its_digest(fresh_web, caplog):
    web = fresh_web
    web.fake.ok("createChatInviteLink", {"invite_link": SECRET_LINK, "name": "web-dec",
                                         "creator": {"id": 5000, "is_bot": True, "first_name": "bot"},
                                         "creates_join_request": False, "is_primary": False, "is_revoked": False})
    with caplog.at_level(logging.DEBUG):
        page = web.post("/campaigns/1/link", {"chat": str(A), "name": "web-dec"})
    assert page.status == 200, page.text
    assert page.text.count(SECRET_LINK) == 1 and "only time it is shown" in page.text
    assert page.headers["Cache-Control"] == "no-store"
    assert SECRET_LINK not in caplog.text and "Zq9Secret" not in caplog.text
    assert web.fake.calls_of("createChatInviteLink")[-1].params == {"chat_id": A, "name": "web-dec"}
    rows = q(web.dsn, "SELECT link_digest, campaign_id, made_by_bot, masked_link FROM tg.invite_link WHERE name = 'web-dec'")
    digest = hmac.new(bytes.fromhex(LINK_KEY), SECRET_LINK.encode(), hashlib.sha256).digest()
    assert rows == [(digest, 1, True, None)]
    with psycopg.connect(web.dsn) as c:  # no row of any table in tg or stats holds the link or a piece of it
        tables = c.execute("SELECT format('%I.%I', schemaname, relname) FROM pg_stat_user_tables "
                           "WHERE schemaname IN ('tg', 'stats')").fetchall()
        for (t,) in tables:
            hit = c.execute(f"SELECT count(*) FROM {t} x WHERE x::text LIKE '%%Zq9Secret%%'").fetchone()[0]
            assert hit == 0, t
    # the campaign page afterwards lists the link by name, never the link
    after = ok(web.get("/campaigns/1"))
    assert "web-dec" in after.text and SECRET_LINK not in after.text
    assert web.post("/campaigns/1/link", {"chat": str(A), "name": "x" * 33}).status == 400
    assert web.post("/campaigns/999/link", {"chat": str(A), "name": "n"}).status == 404


# ------------------------------------------------------------------------------------------------------
# Guards
# ------------------------------------------------------------------------------------------------------


def test_every_request_needs_the_proxy_token_before_any_query(pg, report_db):
    client, database, opened = make_client(pg, report_db)
    try:
        for path in ("/", "/overlap", "/user", "/healthz", "/static/style.css", "/nope", "/campaigns/1/entrants.csv"):
            for token in (None, "", "wrong", PROXY_TOKEN + "x", PROXY_TOKEN[:-1]):
                page = client.request("GET", path, params={"q": "1001"}, token=token)
                assert page.status == 403 and page.text == "forbidden\n", (path, token)
        assert client.request("POST", "/campaigns", data=campaign_form(), token=None,
                              headers={"Origin": f"https://{HOST}"}).status == 403
        assert opened.count == 0 and database.conn is None  # no connection, so no query
        assert ok(client.get("/static/style.css")).headers["Content-Type"].startswith("text/css")
        assert opened.count == 0
        health = client.get("/healthz")
        assert (health.status, health.text) == (200, "ok activation=act-web\n")
        assert opened.count == 1
        ok(client.get("/"))
        assert opened.count == 1  # one connection for every request
    finally:
        database.close()


def test_healthz_is_503_with_the_reason_when_the_database_fails(pg, report_db):
    cfg = make_config(pg, report_db)

    def refuse():
        raise psycopg.OperationalError("connection refused")

    app = WebApp(cfg, WebDb(refuse), PROXY_TOKEN.encode())
    page = Client(app).get("/healthz")
    assert page.status == 503 and page.text == "unhealthy: database: connection refused activation=act-web\n"
    assert Client(app).get("/").status == 503


def test_telegram_text_is_escaped_everywhere(fresh_web):
    web = fresh_web
    evil = '<script>alert("x")</script>&<b>'
    with psycopg.connect(web.dsn) as c:
        c.execute("UPDATE tg.chat SET title = %s WHERE chat_id = %s", (evil, A))
        c.execute("UPDATE tg.tg_user SET first_name = %s, username = 'u1001' WHERE user_id = 1001", (evil,))
        c.commit()
    escaped = "&lt;script&gt;alert(&#34;x&#34;)&lt;/script&gt;&amp;&lt;b&gt;"
    for page in (web.get("/", as_of=NOW_ARG), web.get("/overlap"), web.post("/user", {"q": "1001"}),
                 web.post("/user", {"q": "@u1001"}), web.get("/campaigns"), web.get("/campaigns/1", as_of=NOW_ARG),
                 web.get("/reports/unique", as_of=NOW_ARG), web.get("/reports/asof", chat=str(A), date="2026-11-07")):
        ok(page)
        assert "<script" not in page.text and escaped in page.text, page.text
    link = pair_link(ok(web.get("/overlap")), A, B)
    page = ok(web.get("/overlap/pair", **link))
    assert "<script" not in page.text and escaped in page.text
    bad = web.post("/user", {"q": "<script>"})
    assert "<script>" not in bad.text and "&lt;script&gt;" in bad.text


def test_methods_and_unknown_pages(web):
    assert web.request("POST", "/", headers={"Origin": f"https://{HOST}"}).status == 405
    assert web.get("/nope").status == 404
    assert web.get("/static/../app.py").status == 404
    assert web.request("HEAD", "/reports").body == b""


# ------------------------------------------------------------------------------------------------------
# Inside `tgaud serve`
# ------------------------------------------------------------------------------------------------------


def http(port: int, path: str, token: str | None = PROXY_TOKEN) -> tuple[int | None, str]:
    import urllib.error
    import urllib.request

    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", headers={"Host": HOST})
    if token is not None:
        req.add_header("X-Tgaud-Proxy-Token", token)
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except OSError:
        return None, ""


def test_serve_starts_the_web_ui_on_its_own_listener_with_one_more_connection(pg, migrated_dsn, fake, serves, tmp_path):
    from test_serve import free_port, start

    port = free_port()
    s = start(serves, pg, migrated_dsn, fake, mode="poll", tmp_path=tmp_path,
              TGAUD_WEB_LISTEN=f"127.0.0.1:{port}", TGAUD_WEB_PROXY_TOKEN=PROXY_TOKEN)
    s.wait_for(lambda: s.health()[0] == 200, what="/health 200")
    s.wait_for(lambda: http(port, "/healthz")[0] == 200, what="/healthz 200")
    assert http(port, "/healthz") == (200, "ok activation=act-test\n")
    assert http(port, "/healthz", token=None) == (403, "forbidden\n")
    assert http(port, "/", token="wrong") == (403, "forbidden\n")
    status, body = http(port, "/")
    assert status == 200 and "Overview" in body
    assert s.health()[0] == 200 and "tgaud_poll_last_success" in s.metrics()  # the metrics port needs no token
    name = psycopg.conninfo.conninfo_to_dict(migrated_dsn)["dbname"]
    with psycopg.connect(migrated_dsn, autocommit=True) as c:
        apps = sorted(r[0] for r in c.execute(
            "SELECT application_name FROM pg_stat_activity WHERE datname = %s AND backend_type = 'client backend' "
            "AND pid <> pg_backend_pid()", (name,)))
    assert apps == ["tgaud-batch", "tgaud-ingest", "tgaud-web", "tgaud-worker"]  # the contract's 4
    code, took = s.sigterm_and_time()
    assert code == 0 and took < 10, s.text()
    assert http(port, "/healthz")[0] is None


def test_serve_without_the_proxy_token_refuses_to_start_the_web_ui(pg, migrated_dsn, fake, serves, tmp_path):
    from test_serve import free_port, start

    s = start(serves, pg, migrated_dsn, fake, mode="poll", tmp_path=tmp_path, TGAUD_WEB_LISTEN=f"127.0.0.1:{free_port()}")
    assert s.proc.wait(20) == 2
    assert "TGAUD_WEB_PROXY_TOKEN is required" in s.text()


def test_every_page_renders_on_an_empty_database(pg, migrated_dsn):
    client, database, _ = make_client(pg, migrated_dsn)
    try:
        for path, params in (("/", {}), ("/overlap", {}), ("/user", {}),  ("/campaigns", {}),
                             ("/reports", {}), ("/reports/coverage", {}), ("/reports/asof", {}),
                             *((f"/reports/{s.name}", {}) for s in pages.reports.PLAIN)):
            page = client.get(path, **params)
            assert page.status == 200, (path, page.text)
        assert client.get("/overlap/pair", a="1", b="2", gen="1").status == 400
        assert client.get("/campaigns/1").status == 404
    finally:
        database.close()


# ------------------------------------------------------------------------------------------------------
# Review round 1
# ------------------------------------------------------------------------------------------------------


def urls(page: Page) -> list[str]:
    return [html.unescape(u) for u in re.findall(r'(?:href|action)="([^"]*)"', page.text)]


def test_no_user_id_ever_goes_into_a_url(web, monkeypatch):
    """Proxy logs and Referers keep URLs: a person is looked up by POST, a link to a person is a POST form, the pair
    list pages by position, the entrant CSV is a POST."""
    monkeypatch.setattr(pages, "PAGE_SIZE", 2)
    with web.app.db.read() as conn:
        users = {str(r[0]) for r in conn.execute("SELECT user_id FROM tg.tg_user")}
    assert users and min(int(u) for u in users) > 100  # no clash with campaign ids, page numbers, generations
    overlap = ok(web.get("/overlap", chat=[str(c) for c in AD]))
    link = pair_link(overlap, A, B)
    shown = [ok(web.get("/", as_of=NOW_ARG)), overlap, ok(web.get("/campaigns")), ok(web.get("/campaigns/1", as_of=NOW_ARG)),
             ok(web.post("/user", {"q": "1001"})), ok(web.post("/user", {"q": "1019", "as_of": NOW_ARG})),
             ok(web.get("/reports/asof", chat=str(B), date="2026-11-07", mode="list", as_of=NOW_ARG)),
             ok(web.get("/reports/coverage", as_of=NOW_ARG))]
    for side in pages.SIDES:
        first = ok(web.get("/overlap/pair", **link, side=side))
        shown.append(first)
        for href in urls(first):
            if href.startswith("/overlap/pair?") and "page=" in href:
                shown.append(ok(follow(web, href)))
    for spec in pages.reports.PLAIN:
        shown.append(ok(web.get(f"/reports/{spec.name}", as_of=NOW_ARG, since="2026-08-01", burst_users="2")))
    found = [(u, n) for page in shown for u in urls(page) for n in re.findall(r"\d+", u) if n in users]
    assert not found, found
    assert any('action="/user"' in p.text for p in shown)  # people are still one click away
    assert 'action="/campaigns/1/entrants.csv"' in shown[3].text
    for path, params in (("/user", {"q": "1001"}), ("/user", {"q": "@user1001"}), ("/user", {"as_of": NOW_ARG})):
        refused = web.get(path, **params)
        assert refused.status == 400 and "1001" not in refused.text, refused.text
    assert ok(web.get("/user")).text.count('method="post" action="/user"') == 1
    assert web.get("/campaigns/1/entrants.csv").status == 405


def test_report_pages_reach_every_row_and_keep_scope_and_as_of(web, monkeypatch):
    monkeypatch.setattr(pages, "REPORT_PAGE", 3)
    with web.app.db.read() as conn:
        want = [str(r["user_id"]) for r in sql.run(conn, "16b", as_of=NOW_ARG, chat=B, day="2026-11-07").dicts()]
    assert len(want) == 7
    page = ok(web.get("/reports/asof", chat=str(B), date="2026-11-07", mode="list"))  # as_of: now, then frozen
    got, seen = [], 0
    while True:
        rows = page.table_with("user_id")
        got += [r["user_id"] for r in rows]
        seen += 1
        nxt = [u for u in urls(page) if "offset=" in u and "Next page" in page.text]
        nxt = [u for u in nxt if f"offset={len(got)}" in u]
        if not nxt:
            break
        query = dict(parse_qsl(nxt[0].partition("?")[2]))
        assert query["chat"] == str(B) and query["mode"] == "list" and query["date"] == "2026-11-07"
        assert dt.datetime.fromisoformat(query["as_of"]) > NOW  # the first page's now, frozen for the next ones
        page = ok(follow(web, nxt[0]))
    assert seen == 3 and got == want
    assert "rows 7–7" in page.text and "Previous page" in page.text
    # a plain report the same way, with its scope
    page = ok(web.get("/reports/sources", chat=str(A), since="2026-08-01", as_of=NOW_ARG))
    nxt = [u for u in urls(page) if "offset=3" in u]
    assert nxt and f"chat={A}" in nxt[0].replace("%2D", "-") and "since=2026-08-01" in nxt[0]
    assert len(ok(follow(web, nxt[0])).table_with("join_route")) == 3
    assert web.get("/reports/sources", offset="-1").status == 400


def test_the_past_date_matrix_is_an_asof_mode(web):
    page = ok(web.get("/reports/asof", mode="matrix", date="2026-11-07", among=[str(c) for c in AD], as_of=NOW_ARG))
    rows = page.table_with("shared")
    cell = {frozenset((r["chat_a"].split()[-1], r["chat_b"].split()[-1])): r["shared"] for r in rows}  # one row per pair
    assert len(rows) == 10
    a, b, c, d = (str(x) for x in AD)

    def at(x, y):
        return cell[frozenset((x, y))]

    assert [at(a, a), at(b, b), at(c, c), at(d, d)] == ["11", "6", "5", "4"]
    assert [at(a, b), at(a, c), at(b, c), at(a, d), at(b, d), at(c, d)] == ["4", "2", "3", "1", "1", "1"]
    assert "Chats in scope" in page.text and "2026-11-07 00:00:00 UTC" in page.text


def test_a_link_form_for_an_untracked_chat_answers_without_waiting(fresh_web):
    web = fresh_web
    web.db._lock_wait = 2.0  # a second lock on the same thread would wait this long, then 503
    with psycopg.connect(web.dsn) as c:
        c.execute("UPDATE tg.chat SET tracked = false WHERE chat_id = %s", (C,))
        c.commit()
    import time

    started = time.monotonic()
    page = web.post("/campaigns/1/link", {"chat": str(C), "name": "late"})
    assert page.status == 400 and "pick a tracked chat" in page.text, page.text
    assert time.monotonic() - started < 1.5
    assert not web.fake.calls_of("createChatInviteLink")


def made_link(web, link: str) -> None:
    web.fake.ok("createChatInviteLink", {"invite_link": link, "name": "web-x",
                                         "creator": {"id": 5000, "is_bot": True, "first_name": "bot"},
                                         "creates_join_request": False, "is_primary": False, "is_revoked": False})


def test_the_made_link_is_shown_whatever_fails_after_telegram_made_it(fresh_web, monkeypatch):
    web = fresh_web

    def broken(*args, **kwargs):
        raise psycopg.errors.QueryCanceled("canceling statement due to statement timeout")

    # a report after the link is not needed: the page is built from the POST
    monkeypatch.setattr(pages.campaigns_cmd, "campaign_report", broken)
    made_link(web, SECRET_LINK + "1")
    page = web.post("/campaigns/1/link", {"chat": str(A), "name": "web-x"})
    assert page.status == 200 and page.text.count(SECRET_LINK + "1") == 1 and "only time it is shown" in page.text
    monkeypatch.undo()
    # storing fails: the link is still shown, with the failure
    monkeypatch.setattr(pages.campaigns_cmd, "insert_link", broken)
    made_link(web, SECRET_LINK + "2")
    page = web.post("/campaigns/1/link", {"chat": str(A), "name": "web-x"})
    assert page.status == 500 and page.text.count(SECRET_LINK + "2") == 1 and "could not be stored" in page.text
    # the connection is busy: the same
    monkeypatch.undo()
    made_link(web, SECRET_LINK + "3")
    real_write = web.db.write

    def busy():
        raise pages.HttpError(503, "the database connection is busy with another request; try again")

    monkeypatch.setattr(web.db, "write", busy)
    page = web.post("/campaigns/1/link", {"chat": str(A), "name": "web-x"})
    assert page.status == 500 and page.text.count(SECRET_LINK + "3") == 1 and "busy" in page.text
    monkeypatch.setattr(web.db, "write", real_write)
    assert q(web.dsn, "SELECT count(*) FROM tg.invite_link WHERE name = 'web-x'") == [(1,)]


# ------------------------------------------------------------------------------------------------------
# The listener
# ------------------------------------------------------------------------------------------------------


@pytest.fixture
def listener(pg, report_db):
    import dataclasses

    from tgaud import web as web_pkg

    cfg = make_config(pg, report_db)
    cfg = dataclasses.replace(cfg, web_listen=config.Listen("127.0.0.1", 0))

    def start(patch=None):
        for (obj, name), value in (patch or {}).items():
            setattr(obj, name, value)
        server = web_pkg.WebServer(cfg, cfg.web_proxy_token, lambda: psycopg.connect(cfg.dsn("web"), autocommit=False))
        servers.append(server.start())
        return server

    servers: list = []
    yield start
    for s in servers:
        s.close()


def raw(port: int, method: str, path: str, body: bytes = b"", headers: dict[str, str] | None = None) -> tuple[int, bytes]:
    import http.client

    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    hdrs = {"X-Tgaud-Proxy-Token": PROXY_TOKEN, "Host": f"127.0.0.1:{port}", **(headers or {})}
    if body:
        hdrs["Content-Type"] = "application/x-www-form-urlencoded"
    conn.request(method, path, body=body, headers=hdrs)
    r = conn.getresponse()
    out = (r.status, r.read())
    conn.close()
    return out


def test_access_logs_carry_no_query_and_no_user_value(listener, caplog):
    server = listener()
    port = server.port
    with caplog.at_level(logging.DEBUG, logger="tgaud.web"):
        status, body = raw(port, "POST", "/user", b"q=1001", {"Origin": f"http://127.0.0.1:{port}"})
        assert status == 200 and b"In now: 4 chats" in body
        assert raw(port, "GET", "/user?q=1001")[0] == 400
        assert raw(port, "GET", "/reports/asof?chat=-1001000000002&date=2026-11-07&mode=list&as_of=x1001")[0] == 400
        import socket

        with socket.create_connection(("127.0.0.1", port)) as s:  # a malformed request line with a user value
            s.sendall(b"GET /user?q=1001 BOGUS/9\r\n\r\n")
            s.recv(1000)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "web http: POST /user 200" in text and "web http: GET /user 400" in text
    assert "1001" not in text and "q=" not in text, text


def test_connections_are_bounded_and_an_idle_one_is_dropped_at_the_deadline(listener):
    import socket
    import time

    from tgaud import web as web_pkg

    server = listener({(web_pkg._Server, "max_handlers"): 2, (web_pkg._Handler, "read_deadline"): 1.0})
    port = server.port
    try:
        idle = [socket.create_connection(("127.0.0.1", port)) for _ in range(2)]  # hold both handler slots
        time.sleep(0.2)
        started = time.monotonic()
        status, body = raw(port, "GET", "/healthz")
        assert (status, body) == (503, b"busy\n") and time.monotonic() - started < 1.0
        for s in idle:  # nothing sent: the handler gives up at the deadline and closes the connection
            s.settimeout(5)
            assert s.recv(100) == b""
        time.sleep(0.2)
        assert raw(port, "GET", "/healthz")[0] == 200
        slow = socket.create_connection(("127.0.0.1", port))  # a request trickled slower than the deadline
        slow.settimeout(5)
        slow.sendall(b"GET /healthz HTTP/1.0\r\n")
        time.sleep(0.6)
        slow.sendall(b"X-A: 1\r\n")
        time.sleep(0.6)
        try:
            slow.sendall(b"\r\n")
            assert slow.recv(100) == b""
        except OSError:
            pass  # reset by the server: also a refusal
        for s in (*idle, slow):
            s.close()
    finally:
        web_pkg._Server.max_handlers = web_pkg.MAX_HANDLERS
        web_pkg._Handler.read_deadline = web_pkg.READ_DEADLINE
