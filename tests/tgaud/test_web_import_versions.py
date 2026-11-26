"""The Import page in a browser (headless Chromium through Playwright, as test_web_screenshots.py): the partial
version of a collection uploaded through the form, reconciled, then its finished version, whose result row says it
added the members the partial one lacked (chat A at the laptop size, chat B at the phone size). Pictures of that
result at both sizes go to the test's tmp_path (see them with --basetemp).

It needs Playwright, so it runs only when asked, with the screenshots:

    TGAUD_WEB_SCREENSHOTS=1 uv run --with playwright pytest tests/tgaud/test_web_import_versions.py
    (once: uv run --with playwright playwright install chromium)
"""

from __future__ import annotations

import dataclasses
import os

import pytest
from import_lab import A, B, Lab
from test_import import ALL, LATER_REASON, MISSING, SHARED, at, version
from test_web import PROXY_TOKEN
from test_web_screenshots import SIZES

from tgaud import config
from tgaud import web as web_pkg

pytestmark = pytest.mark.skipif(not os.environ.get("TGAUD_WEB_SCREENSHOTS"),
                                reason="needs Playwright: set TGAUD_WEB_SCREENSHOTS=1")


def test_a_later_version_uploaded_through_the_form(pg, migrated_dsn, tmp_path):
    from playwright.sync_api import sync_playwright

    lab = Lab(pg, migrated_dsn, tmp_path)
    cfg = dataclasses.replace(lab.cfg, web_listen=config.Listen("127.0.0.1", 0))
    server = web_pkg.WebServer(cfg, cfg.web_proxy_token, lab.connect, fence=lab.ctx.fence)
    server.app.spool_dir = str(lab.spool)
    server.start()
    base = f"http://127.0.0.1:{server.port}"
    overflow = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            for (size, (w, h)), chat in zip(SIZES.items(), (A, B)):  # each size its own chat and collection
                partial, u = version(lab, SHARED, at(2), chat=chat)
                full, _ = version(lab, ALL, at(20), uuid=u, chat=chat)
                ctx = browser.new_context(viewport={"width": w, "height": h},
                                          extra_http_headers={"X-Tgaud-Proxy-Token": PROXY_TOKEN})
                page = ctx.new_page()
                for path in (partial, full):
                    page.goto(base + "/import")
                    page.set_input_files("input[name=file]", str(path))
                    page.click("form[action='/import'] button")
                    page.wait_for_load_state("load")
                    if path == partial:
                        lab.reconcile()
                row = page.locator("table.imports tbody tr").nth(0)
                outcome = row.locator("td[data-label=Outcome]").inner_text().strip()
                reason = row.locator("td[data-label=Reason]").inner_text().strip()
                first = lab.snapshots(chat)[0]["snapshot_id"]
                assert outcome == "imported"
                assert reason == (f"{LATER_REASON} {first}: {len(MISSING)} members it lacked written "
                                  f"({len(ALL)} in the file)")
                assert row.locator("td[data-label=Written]").inner_text().strip() == str(len(MISSING))
                assert "count. A later version adds only the members tgaud lacked" in page.inner_text("body")
                if page.evaluate("document.documentElement.scrollWidth > document.documentElement.clientWidth"):
                    overflow.append(size)
                page.screenshot(path=str(tmp_path / f"import-later-version-{size}.png"), full_page=True)
                ctx.close()
            browser.close()
    finally:
        server.close()
        lab.close()
    assert not overflow, f"the result page is wider than the viewport: {overflow}"
