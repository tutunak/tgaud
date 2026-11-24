"""The web UI (phase 2; design.md 10, D16; deploy contract, HTTP): a small server-rendered app inside `tgaud serve`.

It listens on TGAUD_WEB_LISTEN only when that is set, behind the shared Caddy (basic auth, tailnet only), and has
no login of its own. Every request must carry `X-Tgaud-Proxy-Token` equal to TGAUD_WEB_PROXY_TOKEN (constant-time
compare; host loopback is shared by every local account), else 403 before any query. `GET /healthz` (with the
token) answers `200 ok activation=<id>` when the app runs `select 1` on its connection, 503 with the reason
otherwise. `/metrics` and `/health` stay on the metrics port without the token.

Stack: the standard library's wsgiref server with one thread per request (the model of the metrics listener, no
server dependency to pin or patch), a plain WSGI app (tgaud.web.app, tgaud.web.pages) and Jinja2 templates with
autoescaping (tgaud/web/templates). No JavaScript and no CDN: the one stylesheet is served by the app.

One database connection (the contract's budget: serve holds at most 4), opened on the first request, never when
the web UI is off. Requests take turns on it. Reads are REPEATABLE READ read-only transactions through the blocks
of queries.sql, as the CLI's; writes (a campaign, a bot invite link) open with the serve generation fence. POST
forms must come from the request's own host (Origin, or Referer without one).
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from socketserver import ThreadingMixIn
from typing import Any
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

import psycopg

from tgaud.config import Config, Secret
from tgaud.web.app import WebDb
from tgaud.web.pages import WebApp

log = logging.getLogger(__name__)

__all__ = ["WebServer", "WebApp", "WebDb"]


class _Server(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class _Handler(WSGIRequestHandler):
    server_version = "tgaud"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        log.debug("web http: " + format, *args)


class WebServer:
    """The web listener on a daemon thread. `connect()` opens the web connection (application_name tgaud-web);
    `fence(conn)` is serve's generation fence for writes; `fatal(code, reason)` stops serve."""

    def __init__(self, cfg: Config, token: Secret, connect: Callable[[], psycopg.Connection], *,
                 fence: Callable[[psycopg.Connection], None] | None = None,
                 fatal: Callable[[int, str], None] = lambda code, reason: None,
                 api_factory: Callable[[Config], Any] | None = None) -> None:
        if cfg.web_listen is None:
            raise ValueError("the web UI is off (TGAUD_WEB_LISTEN is empty)")
        raw = token.reveal()
        token_bytes = raw.encode() if isinstance(raw, str) else bytes(raw)
        if not token_bytes:
            raise ValueError("the web proxy token is empty")
        self.db = WebDb(connect, fence=fence)
        self.app = WebApp(cfg, self.db, token_bytes, api_factory=api_factory, fatal=fatal)
        self.httpd = make_server(cfg.web_listen.host, cfg.web_listen.port, self.app, server_class=_Server,
                                 handler_class=_Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, name="web", daemon=True)

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    def start(self) -> "WebServer":
        self.thread.start()
        log.info("web UI listening on %s:%d", self.httpd.server_address[0], self.port)
        return self

    def close(self) -> None:
        """Stop accepting requests, cancel the statement in flight and close the connection when it is free."""
        self.db.closed = True
        self.db.cancel()
        if self.thread.is_alive():  # shutdown() waits for serve_forever, which never started otherwise
            self.httpd.shutdown()
        self.httpd.server_close()
        self.db.close()
