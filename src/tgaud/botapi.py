"""A small synchronous Bot API client on httpx.

`BotApi.call(method, **params)` POSTs JSON to `{base}/bot{token}/{method}` and returns the `result`, or raises
`ApiError` with the outcome class of the call:

    ok        HTTP 2xx and `"ok": true`
    429       flood limit; `retry_after` (seconds) from `parameters.retry_after`
    403       forbidden (bot not admin, kicked, user privacy)
    400       bad request (chat or user not found, ...)
    5xx       Telegram's server errors
    timeout   connect, read, write or pool timeout
    other     everything else: 401, 404, 409 (another poller), bad JSON, transport errors

Every call is counted in `tgaud_api_requests_total{method,outcome}` and timed in
`tgaud_api_request_seconds{method}`. Timeouts are httpx's per-phase ones: connect 10 s, read = the long-poll
timeout + 10 s (10 s otherwise), write 10 s, pool 5 s. Shutdown never waits on them: `tgaud serve` exits at a
hard deadline (tgaud.jobs).

The token is part of the URL path, so it never reaches a log, an exception or a repr: ApiError is built from
redacted fields inside the `except` block and raised after leaving it, so neither `__cause__` nor `__context__`
holds the token-bearing httpx request; httpx's own request log lines are kept at WARNING (tgaud.config).
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Sequence
from typing import Any

import httpx

from tgaud import metrics

log = logging.getLogger(__name__)

DEFAULT_URL = "https://api.telegram.org"
POLL_TIMEOUT = 8  # long poll seconds: a poll returns within 10 s, so SIGTERM is honoured in time
ALLOWED_UPDATES = ("chat_member", "my_chat_member", "chat_join_request", "message", "channel_post")
_METHOD = re.compile(r"^[A-Za-z]+$")


class ApiError(Exception):
    """A failed Bot API call. Every field is redacted; `retry_after` is set for outcome 429."""

    def __init__(self, method: str, outcome: str, description: str, *, status: int | None = None,
                 error_code: int | None = None, retry_after: float | None = None) -> None:
        super().__init__(f"{method}: {outcome}: {description}")
        self.method = method
        self.outcome = outcome
        self.description = description
        self.status = status
        self.error_code = error_code
        self.retry_after = retry_after

    def __repr__(self) -> str:
        return (f"ApiError(method={self.method!r}, outcome={self.outcome!r}, error_code={self.error_code!r}, "
                f"retry_after={self.retry_after!r}, description={self.description!r})")


def outcome_of(code: int | None) -> str:
    """The outcome class of an HTTP status or Bot API error_code."""
    if code is None:
        return "other"
    if 200 <= code < 300:
        return "ok"
    if code in (429, 403, 400):
        return str(code)
    if 500 <= code < 600:
        return "5xx"
    return "other"


class BotApi:
    def __init__(self, token: str, base_url: str = DEFAULT_URL, *, transport: httpx.BaseTransport | None = None,
                 connect_timeout: float = 10.0, write_timeout: float = 10.0, pool_timeout: float = 5.0,
                 read_timeout: float = 10.0) -> None:
        if not token:
            raise ValueError("a Bot API token is required")
        self._token = token
        self._base = base_url.rstrip("/")
        self._timeouts = dict(connect=connect_timeout, write=write_timeout, pool=pool_timeout)
        self._read_timeout = read_timeout
        self._client = httpx.Client(transport=transport, timeout=httpx.Timeout(read_timeout, **self._timeouts),
                                    headers={"User-Agent": "tgaud"})

    def __repr__(self) -> str:
        return f"BotApi(base_url={self._base!r}, token='***')"

    def redact(self, text: str) -> str:
        """Text with the token (bare and as bot<token>) replaced by ***."""
        return text.replace(self._token, "***") if self._token else text

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "BotApi":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def call(self, method: str, *, read_timeout: float | None = None, **params: Any) -> Any:
        """Call one method; returns `result`. Parameters that are None are left out."""
        if not _METHOD.match(method):
            raise ValueError(f"not a Bot API method name: {method!r}")
        payload = {k: v for k, v in params.items() if v is not None}
        timeout = httpx.Timeout(read_timeout if read_timeout is not None else self._read_timeout, **self._timeouts)
        url = f"{self._base}/bot{self._token}/{method}"
        started = time.monotonic()
        error: ApiError | None = None
        result: Any = None
        try:
            response = self._client.post(url, json=payload, timeout=timeout)
        except httpx.TimeoutException as e:
            error = ApiError(method, "timeout", self.redact(f"{type(e).__name__}: {e}"))
        except httpx.HTTPError as e:
            error = ApiError(method, "other", self.redact(f"{type(e).__name__}: {e}"))
        else:
            error, result = self._parse(method, response)
        finally:
            metrics.API_REQUEST_SECONDS.labels(method).observe(time.monotonic() - started)
        # Raised outside the except blocks: no __context__ that still holds the httpx request (and the token).
        if error is not None:
            metrics.API_REQUESTS.labels(method, error.outcome).inc()
            raise error
        metrics.API_REQUESTS.labels(method, "ok").inc()
        return result

    def _parse(self, method: str, response: httpx.Response) -> tuple[ApiError | None, Any]:
        status = response.status_code
        try:
            body = response.json()
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            body = None
        if not isinstance(body, dict):
            outcome = outcome_of(status) if status >= 300 else "other"
            return ApiError(method, outcome, f"HTTP {status} without a Bot API JSON body", status=status), None
        if body.get("ok") is True and 200 <= status < 300:
            return None, body.get("result")
        code = body.get("error_code")
        code = code if isinstance(code, int) else status
        params = body.get("parameters") if isinstance(body.get("parameters"), dict) else {}
        retry_after = params.get("retry_after")
        retry_after = float(retry_after) if isinstance(retry_after, (int, float)) else None
        description = self.redact(str(body.get("description") or f"HTTP {status}"))
        outcome = outcome_of(code)
        if outcome == "ok":
            outcome = "other"  # 2xx with ok false
        return ApiError(method, outcome, description, status=status, error_code=code, retry_after=retry_after), None

    # Typed helpers for the methods the design uses ------------------------------------------------------

    def get_me(self) -> dict[str, Any]:
        return self.call("getMe")

    def get_updates(self, offset: int | None = None, *, timeout: int = POLL_TIMEOUT, limit: int = 100,
                    allowed_updates: Sequence[str] | None = ALLOWED_UPDATES) -> list[dict[str, Any]]:
        """Long poll. The read timeout is the poll timeout + 10 s."""
        return self.call("getUpdates", read_timeout=timeout + 10.0, offset=offset, timeout=timeout, limit=limit,
                         allowed_updates=list(allowed_updates) if allowed_updates is not None else None)

    def get_chat(self, chat_id: int | str) -> dict[str, Any]:
        return self.call("getChat", chat_id=chat_id)

    def get_chat_member(self, chat_id: int | str, user_id: int) -> dict[str, Any]:
        return self.call("getChatMember", chat_id=chat_id, user_id=user_id)

    def get_chat_member_count(self, chat_id: int | str) -> int:
        return self.call("getChatMemberCount", chat_id=chat_id)

    def create_chat_invite_link(self, chat_id: int | str, *, name: str | None = None,
                                expire_date: int | None = None, member_limit: int | None = None,
                                creates_join_request: bool | None = None) -> dict[str, Any]:
        return self.call("createChatInviteLink", chat_id=chat_id, name=name, expire_date=expire_date,
                         member_limit=member_limit, creates_join_request=creates_join_request)
