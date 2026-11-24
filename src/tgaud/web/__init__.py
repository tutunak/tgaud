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

import io
import logging
import threading
import time
from collections.abc import Callable
from socketserver import ThreadingMixIn
from typing import Any
from urllib.parse import urlsplit
from wsgiref.simple_server import ServerHandler, WSGIRequestHandler, WSGIServer, make_server

import psycopg

from tgaud.config import Config, Secret
from tgaud.web.app import MAX_BODY, WebDb
from tgaud.web.pages import WebApp

log = logging.getLogger(__name__)

__all__ = ["WebServer", "WebApp", "WebDb"]

MAX_HANDLERS = 16  # requests handled at once; a connection beyond them gets 503 without a thread
READ_DEADLINE = 10.0  # seconds a client has to send its whole request (line, headers, body)
WRITE_TIMEOUT = 30.0  # seconds a send of the response may block
_BUSY = (b"HTTP/1.0 503 Service Unavailable\r\nContent-Type: text/plain; charset=utf-8\r\n"
         b"Content-Length: 5\r\nConnection: close\r\n\r\nbusy\n")


class _Server(ThreadingMixIn, WSGIServer):
    """One daemon thread per request, at most `max_handlers` at once: the slot is taken on the accepting thread
    before a handler thread exists, and a connection without a free slot is answered 503 and closed there."""

    daemon_threads = True
    max_handlers = MAX_HANDLERS

    def server_activate(self) -> None:
        super().server_activate()
        self._slots = threading.BoundedSemaphore(self.max_handlers)

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._slots.acquire(blocking=False):
            try:
                request.settimeout(1.0)
                request.sendall(_BUSY)
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._slots.release()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()

    def handle_error(self, request: Any, client_address: Any) -> None:
        log.debug("web http: a connection failed", exc_info=True)


class _DeadlineSocket(io.RawIOBase):
    """The connection's reads, each socket read armed with the time left until one deadline for the whole request,
    under the buffered reader: a line or a body trickled in many chunks still ends at the deadline."""

    def __init__(self, sock: Any, deadline: float) -> None:
        super().__init__()
        self.sock, self.deadline = sock, deadline

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        left = self.deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError("the request was not sent in time")
        self.sock.settimeout(left)
        return self.sock.recv_into(buffer)


class _Handler(WSGIRequestHandler):
    """Reads the whole request within READ_DEADLINE before the app runs (a slow or idle client cannot hold a
    handler), and logs method, path without the query, and status only: queries carry user ids and @names."""

    server_version = "tgaud"
    sys_version = ""
    read_deadline = READ_DEADLINE

    def handle(self) -> None:
        self.rfile.close()  # setup()'s reader, unused: nothing was read through it yet
        self.rfile = io.BufferedReader(_DeadlineSocket(self.connection, time.monotonic() + self.read_deadline))
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if not self.raw_requestline:
                return
            if len(self.raw_requestline) > 65536:
                self.requestline, self.request_version, self.command = "", "", ""
                self.send_error(414)
                return
            if not self.parse_request():
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                self.send_error(400, "bad Content-Length")
                return
            if length < 0 or length > MAX_BODY:
                self.send_error(413)
                return
            body = self.rfile.read(length) if length else b""
            if len(body) < length:
                return  # the client went away
        except (TimeoutError, OSError):
            self.close_connection = True
            return
        self.connection.settimeout(WRITE_TIMEOUT)
        handler = ServerHandler(io.BytesIO(body), self.wfile, self.get_stderr(), self.get_environ(),
                                multithread=True)
        handler.request_handler = self
        handler.run(self.server.get_app())

    def log_request(self, code: Any = "-", size: Any = "-") -> None:
        path = urlsplit(getattr(self, "path", "") or "").path
        log.debug("web http: %s %s %s", getattr(self, "command", "") or "-", path or "-", code)

    def log_error(self, format: str, *args: Any) -> None:  # noqa: A002
        log.debug("web http: a malformed request was refused")

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        log.debug("web http: a server note (not logged: it can hold the request line)")


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
