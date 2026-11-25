"""Screenshots of every web page for docs/web/screenshots/, at 1366x768 (laptop) and 390x844 (phone), with
headless Chromium through Playwright, against a real tgaud.web.WebServer on the history fixture.

It writes into the repo, so it runs only when asked:

    TGAUD_WEB_SCREENSHOTS=1 uv run --with playwright pytest tests/tgaud/test_web_screenshots.py
    (once: uv run --with playwright playwright install chromium)

Each page also has to fit the phone: the test fails when the page itself scrolls sideways (a wide table scrolls
inside its own box instead).
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from pathlib import Path
from urllib.parse import urlencode

import psycopg
import pytest
from botapi_fake import FakeBotApi
from clidb import A, AD, B, NOW, NOW_ARG, TOKEN  # noqa: F401
from clidb import fresh_db, report_db  # noqa: F401 (fixtures)
from test_web import PROXY_TOKEN, make_config

from tgaud.jobs import audience
from tgaud.web import WebServer
from tgstats import db as tgdb

OUT = Path(__file__).resolve().parents[2] / "docs" / "web" / "screenshots"
SIZES = {"laptop": (1366, 768), "phone": (390, 844)}
SECRET_LINK = "https://t.me/+ScreenshotDemoLink42"

pytestmark = pytest.mark.skipif(not os.environ.get("TGAUD_WEB_SCREENSHOTS"),
                                reason="writes docs/web/screenshots: set TGAUD_WEB_SCREENSHOTS=1")


def import_file(tmp_path: Path, size: str) -> Path:
    """A tgstats.db (synthetic, written by tgstats' own writer) with one chat per outcome: Alpha chat newer than the
    fixture's snapshot (imported), Alpha news older (not newer), Gamma group collected by an old tgstats, Delta
    finishing in the future, and a chat tgaud does not track. Each size gets its own collection of Alpha chat."""
    path = tmp_path / f"tgstats-{size}.db"
    conn = tgdb.connect(path)
    hour = 9 if size == "laptop" else 10
    real_now = dt.datetime.now(dt.timezone.utc)

    def snap(chat, title, users, started, finished):
        members = [tgdb.Member(tgdb.UserRow(u, f"user{u}", f"Name{u}", None),
                               (started + dt.timedelta(seconds=i + 1)).isoformat()) for i, u in enumerate(users)]
        tgdb.add_snapshot(conn, tgdb.ChatRow(chat, "supergroup", title, None), members, len(users),
                          collection=tgdb.Collection(str(uuid.uuid4()), started.isoformat(), finished.isoformat(),
                                                     "listing", len(users), len(users)))

    t = dt.datetime(2026, 11, 22, hour, tzinfo=dt.timezone.utc)
    snap(B, "Alpha chat", range(1001, 1013), t, t + dt.timedelta(minutes=5))
    snap(A, "Alpha news", range(1001, 1009), t - dt.timedelta(days=7), t - dt.timedelta(days=7, minutes=-5))
    tgdb.add_snapshot(conn, tgdb.ChatRow(-1001000000003, "supergroup", "Gamma group", None),
                      [tgdb.UserRow(u, None, None, None) for u in range(1001, 1006)], 5,
                      taken_at="2026-11-14T10:00:00+00:00")  # as tgstats before the snapshot contract wrote it
    snap(-1001000000004, "Delta", range(1001, 1004), real_now, real_now + dt.timedelta(days=1))
    snap(-1009876543210, "Someone else's channel", range(1, 40), t, t + dt.timedelta(minutes=2))
    conn.close()
    return path


def test_screenshots_of_every_page(pg, fresh_db, tmp_path):
    from playwright.sync_api import sync_playwright

    cfg = make_config(pg, fresh_db)
    cfg = type(cfg)(**{**cfg.__dict__, "web_listen": type(cfg.web_listen)("127.0.0.1", 0)})
    fake = FakeBotApi(token=TOKEN, bot_id=5000)
    for n in (1, 2):  # Telegram makes a new link every time
        fake.ok("createChatInviteLink", {"invite_link": f"{SECRET_LINK}{n}", "name": "dec-promo",
                                         "creator": {"id": 5000, "is_bot": True, "first_name": "bot"},
                                         "creates_join_request": False, "is_primary": False, "is_revoked": False})
    server = WebServer(cfg, cfg.web_proxy_token, lambda: psycopg.connect(cfg.dsn("web"), autocommit=False),
                       api_factory=lambda c: fake.api()).start()
    base = f"http://127.0.0.1:{server.port}"
    ad = [("chat", c) for c in AD]
    shots = [
        ("overview", "/?" + urlencode({"as_of": NOW_ARG})),
        ("overlap-all", "/overlap"),
        ("overlap-scope", "/overlap?" + urlencode(ad)),
        ("pair", None),  # opened from the matrix
        ("user", None),  # a person who entered the giveaway in both of its chats
        ("user-empty", "/user"),
        ("campaigns", "/campaigns"),
        ("campaign", "/campaigns/1?" + urlencode({"as_of": NOW_ARG})),
        ("link-made", None),  # the form's response
        ("reports", "/reports"),
        ("report-unique", "/reports/unique?" + urlencode([*ad, ("as_of", NOW_ARG)])),
        ("report-growth", "/reports/growth?" + urlencode([("chat", A), ("since", "2026-11-01"), ("as_of", NOW_ARG)])),
        ("report-sources", "/reports/sources?" + urlencode([("chat", A), ("since", "2026-08-01"), ("as_of", NOW_ARG)])),
        ("report-coverage", "/reports/coverage?" + urlencode({"as_of": NOW_ARG, "since": "2026-08-01", "gaps": "1"})),
        ("asof", "/reports/asof?" + urlencode({"chat": B, "date": "2026-11-07", "mode": "list", "as_of": NOW_ARG})),
        ("asof-matrix", "/reports/asof?" + urlencode([("mode", "matrix"), ("date", "2026-11-07"), ("as_of", NOW_ARG),
                                                       *(("among", c) for c in AD)])),
        ("stale", None),  # a pair after a newer R2 publish
        ("import", "/import"),
        ("import-result", None),  # the form's response to a synthetic tgstats.db
    ]
    with psycopg.connect(fresh_db) as c:
        entrant = c.execute("SELECT x.user_id FROM tg.campaign_entrant(1, 48, %s) x GROUP BY x.user_id "
                            "ORDER BY count(*) DESC, x.user_id LIMIT 1", (NOW,)).fetchone()[0]
    OUT.mkdir(parents=True, exist_ok=True)
    overflow: list[str] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            for size, (w, h) in SIZES.items():
                ctx = browser.new_context(viewport={"width": w, "height": h},
                                          extra_http_headers={"X-Tgaud-Proxy-Token": PROXY_TOKEN})
                page = ctx.new_page()
                pair_url = None
                for name, path in shots:
                    if name == "pair":
                        page.goto(base + "/overlap?" + urlencode(ad))
                        page.locator("table.matrix tbody tr").nth(0).locator("td a").nth(1).click()
                        pair_url = page.url
                    elif name == "user":  # a lookup is a POST form: no user id in any URL
                        page.goto(base + "/user")
                        page.fill("input[name=q]", str(entrant))
                        page.click("form[action='/user'] button")
                    elif name == "link-made":
                        page.goto(base + "/campaigns/1")
                        page.fill("input[name=name]", "dec-promo")
                        page.click("form[action$='/link'] button")
                    elif name == "import-result":
                        page.goto(base + "/import")
                        page.set_input_files("input[name=file]", str(import_file(tmp_path, size)))
                        page.click("form[action='/import'] button")
                    elif name == "stale":
                        with psycopg.connect(fresh_db) as c:
                            audience.refresh(c, NOW + dt.timedelta(hours=1 if size == "laptop" else 2))
                        page.goto(pair_url)
                    else:
                        page.goto(base + path)
                    page.wait_for_load_state("load")
                    wide = page.evaluate("document.documentElement.scrollWidth > document.documentElement.clientWidth")
                    if wide:
                        overflow.append(f"{name} at {size}")
                    page.screenshot(path=str(OUT / f"{name}-{size}.png"), full_page=True)
                ctx.close()
            browser.close()
    finally:
        server.close()
    assert not overflow, f"pages wider than the viewport: {overflow}"
