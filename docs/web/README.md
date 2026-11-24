# tgaud web UI

A small server-rendered UI inside `tgaud serve` (design.md 10, D16; deploy contract, HTTP). It listens on
`TGAUD_WEB_LISTEN` only when that is set (`0.0.0.0:8097` in the container, published on host loopback, behind the
shared Caddy with `basic_auth` on a tailnet-only domain) and has no login of its own.

## Guards

- **Proxy token.** Every request must carry `X-Tgaud-Proxy-Token` equal to `TGAUD_WEB_PROXY_TOKEN` (Podman secret
  `tgaud_web_proxy_token`, compared in constant time); otherwise `403 forbidden` before anything else runs, a query
  included. Caddy adds the header. `serve` exits 2 when the web UI is on and the token is missing.
- **Same origin for forms.** A POST (new campaign, bot link) is refused with 403 unless its `Origin`, or its
  `Referer` when there is no `Origin`, names the `Host` the request came to. Caddy passes the client's `Host`
  through by default; keep it that way (no `header_up Host`).
- **Escaping.** Jinja2 autoescaping everywhere; titles, names and usernames come from Telegram.
- **Headers.** Every response is `Cache-Control: no-store` (user-level data) with a Content-Security-Policy that
  allows only the app's own stylesheet: the UI has no JavaScript and loads nothing from a CDN.
- **Readiness.** `GET /healthz` with the token: `200 ok activation=<TGAUD_ACTIVATION_ID>` when the app runs
  `select 1` on its connection, `503 unhealthy: <reason> activation=...` otherwise. `/metrics` and `/health` stay on
  the metrics port without the token.

## Stack and connection

The standard library's `wsgiref` server with a thread per request (the model of the metrics listener: no server
dependency to pin or patch), a plain WSGI app (`tgaud.web.app`, `tgaud.web.pages`) and Jinja2 templates
(`src/tgaud/web/templates`), one stylesheet (`src/tgaud/web/static/style.css`).

One database connection, `tgaud-web`, opened on the first request (none while the web UI is off), so serve holds
at most 4. Requests take turns on it; each statement has a 60 s `statement_timeout`. Pages read in one REPEATABLE
READ read-only transaction, like the CLI's reports. Writes open with serve's generation fence. SIGTERM cancels the
statement in flight.

## Pages

| Path | What | Reads |
|---|---|---|
| `/` | every tracked chat: Telegram's count, known, suspect, coverage, mismatch, residual of 24 h, open gaps, pending probes, last good snapshot, gap flags; both rollups' as-of | `tgaud coverage`'s table, block 9b, `stats.rollup_run` |
| `/overlap?chat=…` | the pair matrix of a scope: shared count, % of the row and of the column in each cell (also on hover), Jaccard as the shade | block 1 |
| `/overlap/pair?a=&b=&as_of=&side=&after=` | one cell: in both, only in A, only in B, and the people, 100 per page | block 1 (counts), block **1b** (list) |
| `/user?q=ID or @name` | chats now, every stay with start/end and how sure, campaigns entered, messages per group | blocks 2b, **2c**, **2d** |
| `/campaigns` | the list; a form for a new owner campaign (window, chats, note) | `tgaud campaign list`'s query |
| `/campaigns/ID` | item 4 per chat (4a) and per person (4b), the window flags, a form for a named bot invite link, the campaign's bot links | blocks 4a, 4b |
| `/campaigns/ID/entrants.csv` | the entrants, one row per chat and person (for the owner only) | block 4c |
| `/reports`, `/reports/NAME` | every other CLI report as a table, with as-of, scope, coverage and gap flags | the report's block |
| `/reports/asof` | who was in a chat at a moment: counts, the list, the end-of-day count | 16a, 16b, 16d |
| `/reports/coverage` | `tgaud coverage` and `--gaps` | the CLI's queries |

The past-date matrix (16c, a batch of about a minute) is not on the web; it stays `tgaud asof CHAT DATE --matrix`.
A report table shows its first 500 rows; the CLI's `--csv` has all of them.

**Overlap generation.** The matrix shows its rollup `as_of`, and every link from it (a cell, the next page of a cell)
carries it. A drill-down is answered, in one snapshot, only while that `as_of` is still the published one
(`stats.overlap`). After a newer R2 publish the page asks to reload the matrix (409) instead of showing a different
population. Block 1b reads `tg.user_chats`, which R2 refreshes in the same transaction as the matrix, so a cell's
count and its list are always the same people.

**Bot links.** `createChatInviteLink` with the name (1-32 characters). The link is shown once in the POST's
response, with `no-store` and no redirect, so it is never in a URL. It is never logged and is stored only as its
HMAC-SHA256 digest (D8). If storing fails after Telegram made the link, the page still shows the link with the error.

## Screenshots

`docs/web/screenshots/<page>-laptop.png` (1366x768) and `-phone.png` (390x844), full page, rendered by headless
Chromium from a real `WebServer` on the history fixture. Regenerate:

    uv run --with playwright playwright install chromium     # once
    TGAUD_WEB_SCREENSHOTS=1 uv run --with playwright pytest tests/tgaud/test_web_screenshots.py

The test fails if a page is wider than the viewport. On a phone, data tables turn into one card per row; the overlap
matrix scrolls inside its own box with sticky headers.
