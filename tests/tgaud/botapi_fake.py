"""Fake Bot API for the tgaud tests; later tasks reuse it. Never the real Telegram.

FakeBotApi     an httpx.MockTransport: in-process, no sockets. `fake.api()` is a BotApi wired to it.
FakeBotServer  a threaded local HTTP server (real sockets), for serve subprocesses and containers
               (TGAUD_BOT_API_URL=fake.url), and for timing cases: hold a request, trickle a response.

Both script responses per method: queued answers are used first (FIFO), then the method's default
(getMe -> the bot, getUpdates -> [], anything else -> 400 "method not scripted"). Both record every call as
Call(method, params, token).

    fake.ok("getChatMember", {"status": "left", "user": {...}})
    fake.error("getChatMember", 429, "Too Many Requests: retry after 7", retry_after=7)
    fake.updates([...])                       # one getUpdates answer
    fake.respond("getChat", lambda params: {"id": params["chat_id"], ...})   # computed result (ok)
    fake.raise_timeout("getUpdates")          # FakeBotApi only: httpx.ReadTimeout
    server.hold("getUpdates", seconds=8)      # FakeBotServer only: answer after a delay (None = until closed)
    server.trickle_body("getUpdates")         # FakeBotServer only: headers, then one body byte per 0.2 s forever
    server.trickle_headers("getUpdates")      # FakeBotServer only: one header byte per 0.2 s forever
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx

from tgaud.botapi import BotApi

TOKEN = "123456:TEST-token_abcdefghijklmnop"
BOT_ID = 5000


@dataclass(frozen=True)
class Call:
    method: str
    params: dict[str, Any]
    token: str


@dataclass(frozen=True)
class Answer:
    kind: str  # "json", "timeout", "hold", "trickle_body", "trickle_headers", "close"
    status: int = 200
    body: Any = None
    seconds: float | None = None
    then: "Answer | None" = None
    compute: Callable[[dict[str, Any]], Any] | None = None


def _ok(result: Any) -> Answer:
    return Answer("json", 200, {"ok": True, "result": result})


def _err(code: int, description: str, retry_after: float | None = None) -> Answer:
    body: dict[str, Any] = {"ok": False, "error_code": code, "description": description}
    if retry_after is not None:
        body["parameters"] = {"retry_after": retry_after}
    return Answer("json", code, body)


class _Script:
    def __init__(self, token: str = TOKEN, bot_id: int = BOT_ID) -> None:
        self.token = token
        self.bot_id = bot_id
        self.calls: list[Call] = []
        self._queued: dict[str, deque[Answer]] = {}
        self._defaults: dict[str, Answer] = {
            "getMe": _ok({"id": bot_id, "is_bot": True, "first_name": "tgaud test", "username": "tgaud_test_bot"}),
            "getUpdates": _ok([]),
        }
        self._lock = threading.Lock()

    # scripting
    def queue(self, method: str, answer: Answer, times: int = 1) -> None:
        with self._lock:
            q = self._queued.setdefault(method, deque())
            for _ in range(times):
                q.append(answer)

    def default(self, method: str, answer: Answer) -> None:
        with self._lock:
            self._defaults[method] = answer

    def ok(self, method: str, result: Any, times: int = 1) -> None:
        self.queue(method, _ok(result), times)

    def updates(self, updates: list[dict[str, Any]]) -> None:
        self.ok("getUpdates", updates)

    def error(self, method: str, code: int, description: str, *, retry_after: float | None = None,
              times: int = 1) -> None:
        self.queue(method, _err(code, description, retry_after), times)

    def respond(self, method: str, compute: Callable[[dict[str, Any]], Any], *, default: bool = True) -> None:
        answer = Answer("json", compute=compute)
        if default:
            self.default(method, answer)
        else:
            self.queue(method, answer)

    def raw(self, method: str, status: int, body: Any, times: int = 1) -> None:
        """A response body as given (a dict is JSON-encoded, bytes are sent as they are)."""
        self.queue(method, Answer("json", status, body), times)

    # dispatch
    def _next(self, method: str, params: dict[str, Any], token: str) -> Answer:
        with self._lock:
            self.calls.append(Call(method, params, token))
            q = self._queued.get(method)
            if q:
                return q.popleft()
            return self._defaults.get(method, _err(400, f"Bad Request: method {method} not scripted"))

    def calls_of(self, method: str) -> list[Call]:
        return [c for c in self.calls if c.method == method]

    @staticmethod
    def _parse_path(path: str) -> tuple[str, str]:
        parts = path.strip("/").split("/")
        if len(parts) != 2 or not parts[0].startswith("bot"):
            return "", ""
        return parts[0][3:], parts[1]

    def _resolve(self, answer: Answer, params: dict[str, Any]) -> tuple[int, bytes]:
        body = answer.body
        status = answer.status
        if answer.compute is not None:
            body = {"ok": True, "result": answer.compute(params)}
        if isinstance(body, bytes):
            return status, body
        return status, json.dumps(body).encode()


class FakeBotApi(_Script):
    """In-process fake (httpx.MockTransport)."""

    def timeout_answer(self) -> Answer:
        return Answer("timeout")

    def raise_timeout(self, method: str, times: int = 1) -> None:
        self.queue(method, Answer("timeout"), times)

    def raise_transport_error(self, method: str, times: int = 1) -> None:
        self.queue(method, Answer("close"), times)

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def api(self, **kwargs: Any) -> BotApi:
        return BotApi(self.token, "https://api.telegram.test", transport=self.transport, **kwargs)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        token, method = self._parse_path(request.url.path)
        params = json.loads(request.content or b"{}")
        if token != self.token:
            return httpx.Response(401, json={"ok": False, "error_code": 401, "description": "Unauthorized"})
        answer = self._next(method, params, token)
        if answer.kind == "timeout":
            raise httpx.ReadTimeout("timed out", request=request)
        if answer.kind == "close":
            raise httpx.ConnectError(f"connection refused while calling {request.url}", request=request)
        status, body = self._resolve(answer, params)
        return httpx.Response(status, content=body, headers={"Content-Type": "application/json"})


class FakeBotServer(_Script):
    """A local HTTP server on 127.0.0.1 (real sockets). Use as a context manager or call close()."""

    def __init__(self, token: str = TOKEN, bot_id: int = BOT_ID, host: str = "127.0.0.1") -> None:
        super().__init__(token, bot_id)
        self._closing = threading.Event()
        self.httpd = ThreadingHTTPServer((host, 0), self._handler())
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True, name="fake-bot-api")
        self.thread.start()

    @property
    def url(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    def api(self, **kwargs: Any) -> BotApi:
        return BotApi(self.token, self.url, **kwargs)

    def hold(self, method: str, seconds: float | None = None, then: Answer | None = None, times: int = 1) -> None:
        self.queue(method, Answer("hold", seconds=seconds, then=then or self._defaults.get(method)), times)

    def trickle_body(self, method: str, interval: float = 0.2, times: int = 1) -> None:
        self.queue(method, Answer("trickle_body", seconds=interval), times)

    def trickle_headers(self, method: str, interval: float = 0.2, times: int = 1) -> None:
        self.queue(method, Answer("trickle_headers", seconds=interval), times)

    def close(self) -> None:
        self._closing.set()
        self.httpd.shutdown()
        self.httpd.server_close()

    def __enter__(self) -> "FakeBotServer":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        fake = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                pass

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b"{}"
                params = json.loads(raw or b"{}")
                token, method = fake._parse_path(self.path)
                if token != fake.token:
                    self._send(401, json.dumps({"ok": False, "error_code": 401, "description": "Unauthorized"}).encode())
                    return
                self._answer(fake._next(method, params, token), params)

            do_GET = do_POST

            def _answer(self, answer: Answer, params: dict[str, Any]) -> None:
                if answer.kind == "hold":
                    end = None if answer.seconds is None else time.monotonic() + answer.seconds
                    while not fake._closing.is_set() and (end is None or time.monotonic() < end):
                        fake._closing.wait(0.05)
                    if fake._closing.is_set():
                        return
                    answer = answer.then or _ok([])
                if answer.kind == "close":
                    self.close_connection = True
                    return
                if answer.kind == "trickle_body":
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", "100000000")
                    self.end_headers()
                    self._trickle(b'{"ok": true, "result": [' + b" " * 10**8, answer.seconds or 0.2)
                    return
                if answer.kind == "trickle_headers":
                    self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                    self._trickle(b"X-Slow: " + b"a" * 10**8, answer.seconds or 0.2)
                    return
                status, body = fake._resolve(answer, params)
                self._send(status, body)

            def _trickle(self, data: bytes, interval: float) -> None:
                try:
                    for i in range(len(data)):
                        if fake._closing.is_set():
                            return
                        self.wfile.write(data[i:i + 1])
                        self.wfile.flush()
                        fake._closing.wait(interval)
                except OSError:
                    return

            def _send(self, status: int, body: bytes) -> None:
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except OSError:
                    pass

        return Handler
