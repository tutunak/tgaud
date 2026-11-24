"""The web UI's plumbing: request and response, the guards every request passes, the one database connection, and
the WSGI entry that routes to tgaud.web.pages.

Order of the guards: the proxy token first (403 before anything else, a query included), then the method, then for
a POST the same-origin check, then the page. Every response is `no-store` (the pages are user-level data) and
carries a Content-Security-Policy that allows only the app's own stylesheet: there is no script at all.
"""

from __future__ import annotations

import contextlib
import hmac
import logging
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit

import psycopg
from psycopg import IsolationLevel

log = logging.getLogger(__name__)

TOKEN_HEADER = "X-Tgaud-Proxy-Token"
MAX_BODY = 64 * 1024  # the largest form: a campaign with a note and a few hundred chat checkboxes
STATEMENT_TIMEOUT = "60s"  # a web request never holds the one connection longer
LOCK_WAIT = 75.0  # seconds a request waits for the connection while another one uses it
SECURITY_HEADERS = [
    ("Cache-Control", "no-store"),
    ("X-Content-Type-Options", "nosniff"),
    ("Referrer-Policy", "same-origin"),
    ("X-Frame-Options", "DENY"),
    ("Content-Security-Policy", "default-src 'none'; style-src 'self'; img-src 'self'; form-action 'self'; "
                                "frame-ancestors 'none'; base-uri 'none'"),
]
REASONS = {200: "OK", 303: "See Other", 400: "Bad Request", 403: "Forbidden", 404: "Not Found",
           405: "Method Not Allowed", 409: "Conflict", 413: "Payload Too Large", 500: "Internal Server Error",
           502: "Bad Gateway", 503: "Service Unavailable"}


class HttpError(Exception):
    """Ends a request with this status and a short message (shown escaped on the error page)."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass
class Request:
    method: str
    path: str
    query: dict[str, list[str]]
    form: dict[str, list[str]]
    environ: dict[str, Any] = field(repr=False)

    @classmethod
    def from_environ(cls, environ: dict[str, Any]) -> "Request":
        method = environ.get("REQUEST_METHOD", "GET").upper()
        query = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
        form: dict[str, list[str]] = {}
        if method == "POST":
            try:
                length = int(environ.get("CONTENT_LENGTH") or 0)
            except ValueError:
                raise HttpError(400, "bad Content-Length") from None
            if length > MAX_BODY:
                raise HttpError(413, "the form is too large")
            body = environ["wsgi.input"].read(length) if length > 0 else b""
            ctype = environ.get("CONTENT_TYPE", "").split(";", 1)[0].strip().lower()
            if ctype not in ("application/x-www-form-urlencoded", ""):
                raise HttpError(400, "forms must be sent as application/x-www-form-urlencoded")
            form = parse_qs(body.decode("utf-8", "replace"), keep_blank_values=True)
        return cls(method, environ.get("PATH_INFO", "/") or "/", query, form, environ)

    def header(self, name: str) -> str | None:
        key = "HTTP_" + name.upper().replace("-", "_")
        return self.environ.get(key)

    def arg(self, name: str, default: str | None = None) -> str | None:
        """The first value of a query (GET) or form (POST) field, stripped; default when absent or blank."""
        values = (self.form if self.method == "POST" else self.query).get(name)
        if not values or not values[0].strip():
            return default
        return values[0].strip()

    def args(self, name: str) -> list[str]:
        values = (self.form if self.method == "POST" else self.query).get(name, [])
        return [v.strip() for v in values if v.strip()]


@dataclass
class Response:
    status: int = 200
    body: bytes = b""
    content_type: str = "text/html; charset=utf-8"
    headers: list[tuple[str, str]] = field(default_factory=list)


def text(status: int, body: str) -> Response:
    return Response(status, body.encode(), "text/plain; charset=utf-8")


def redirect(location: str) -> Response:
    return Response(303, b"", "text/plain; charset=utf-8", [("Location", location)])


def token_ok(request_environ: dict[str, Any], token: bytes) -> bool:
    """The X-Tgaud-Proxy-Token header against the configured token, compared in constant time."""
    sent = request_environ.get("HTTP_X_TGAUD_PROXY_TOKEN")
    if sent is None:
        return False
    return hmac.compare_digest(sent.encode("latin-1", "replace"), token)


def _netloc(value: str) -> str:
    host = value.strip().lower()
    for port in (":80", ":443"):
        if host.endswith(port):
            host = host[: -len(port)]
    return host


def same_origin(req: Request) -> bool:
    """A POST must come from a page of this host: its Origin (or, without one, its Referer) names the Host the
    request was sent to. Neither header, or `Origin: null`, is refused (basic auth alone does not stop a
    cross-site form)."""
    host = req.header("Host")
    if not host:
        return False
    origin = req.header("Origin")
    source = origin if origin is not None else req.header("Referer")
    if not source or source.strip() == "null":
        return False
    parts = urlsplit(source.strip())
    if parts.scheme not in ("http", "https") or not parts.netloc:
        return False
    return _netloc(parts.netloc) == _netloc(host)


class WebDb:
    """The web UI's one database connection (deploy contract: serve holds at most 4), opened on first use and
    reopened after a loss. Requests take turns: one statement at a time, guarded by a lock."""

    def __init__(self, connect: Callable[[], psycopg.Connection], *,
                 fence: Callable[[psycopg.Connection], None] | None = None,
                 statement_timeout: str = STATEMENT_TIMEOUT, lock_wait: float = LOCK_WAIT) -> None:
        self._connect = connect
        self._fence = fence
        self._statement_timeout = statement_timeout
        self._lock_wait = lock_wait
        self.lock = threading.Lock()
        self.conn: psycopg.Connection | None = None
        self.closed = False

    def _ensure(self) -> psycopg.Connection:
        if self.closed:
            raise HttpError(503, "the service is stopping")
        if self.conn is not None and (self.conn.closed or self.conn.broken):
            self._discard()
        if self.conn is None:
            conn = self._connect()
            try:
                conn.execute("SELECT set_config('statement_timeout', %s, false)", (self._statement_timeout,))
                conn.commit()
            except Exception:
                conn.close()
                raise
            self.conn = conn
        return self.conn

    def _discard(self) -> None:
        conn, self.conn = self.conn, None
        if conn is not None:
            with contextlib.suppress(Exception):
                conn.close()

    @contextlib.contextmanager
    def _turn(self) -> Iterator[None]:
        if not self.lock.acquire(timeout=self._lock_wait):
            raise HttpError(503, "the database connection is busy with another request; try again")
        try:
            yield
        finally:
            self.lock.release()

    @contextlib.contextmanager
    def read(self) -> Iterator[psycopg.Connection]:
        """A read-only REPEATABLE READ transaction (as the CLI's reports): every statement of a page sees one
        snapshot. Always rolled back."""
        with self._turn():
            conn = self._ensure()
            conn.read_only = True
            conn.isolation_level = IsolationLevel.REPEATABLE_READ
            try:
                yield conn
            finally:
                self._end(conn, commit=False)

    @contextlib.contextmanager
    def write(self) -> Iterator[psycopg.Connection]:
        """A READ COMMITTED transaction that writes, opened with the serve generation fence (as every job
        transaction); committed when the block ends without an exception."""
        with self._turn():
            conn = self._ensure()
            conn.read_only = False
            conn.isolation_level = IsolationLevel.READ_COMMITTED
            ok = False
            try:
                if self._fence is not None:
                    self._fence(conn)
                yield conn
                ok = True
            finally:
                self._end(conn, commit=ok)

    def _end(self, conn: psycopg.Connection, *, commit: bool) -> None:
        try:
            if commit:
                conn.commit()
            else:
                conn.rollback()
        except psycopg.Error:
            self._discard()
            if commit:
                raise
        if conn.broken or conn.closed:
            self._discard()

    def ping(self) -> None:
        """`SELECT 1` on the connection (readiness)."""
        with self.read() as conn:
            conn.execute("SELECT 1").fetchone()

    def cancel(self) -> None:
        """From the stop: cancel the statement in flight, if any (best effort)."""
        conn = self.conn
        if conn is None or conn.closed:
            return
        with contextlib.suppress(Exception):
            cancel = getattr(conn, "cancel_safe", None)
            if cancel is not None:
                cancel(timeout=3.0)
            else:
                conn.cancel()

    def close(self, wait: float = 3.0) -> None:
        """Stop serving; the connection is closed once no request uses it (else the process exit ends it)."""
        self.closed = True
        self.cancel()
        if self.lock.acquire(timeout=wait):
            try:
                self._discard()
            finally:
                self.lock.release()


def reason(status: int) -> str:
    return f"{status} {REASONS.get(status, 'Status')}"


def db_error_line(e: BaseException) -> str:
    first = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
    return first


def finish(start_response: Callable, resp: Response) -> list[bytes]:
    headers = [("Content-Type", resp.content_type), ("Content-Length", str(len(resp.body)))]
    headers += SECURITY_HEADERS + resp.headers
    start_response(reason(resp.status), headers)
    return [resp.body]
