"""Every metric of design.md 9, defined once on REGISTRY; later tasks import and set them.

Names, types, labels and help texts follow the table of design.md 9 exactly (the `_reported/_known/_suspect`
row is three gauges, the `probe_queue/oldest_age` row two). One addition the table lacks:
`tgaud_job_failures_total{job}`, counted by the scheduler for a failing job step.

Values are cached in memory: a scrape never queries the database. The sampler refreshes the per-chat values
every minute (the metric cache job), ingest and the schedulers set the rest as they work.

The HTTP listener (TGAUD_METRICS_LISTEN) serves `GET /metrics` (Prometheus text) and `GET /health`
(deploy contract): 200 when the last successful getUpdates and the last successful ingest commit are both
younger than 180 s, else 503 with the reason; the body always carries `activation=<TGAUD_ACTIVATION_ID>`.
Only ingest marks those two timestamps (Health.mark_poll_ok, Health.mark_commit); other jobs' work never
makes the service healthy.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from prometheus_client.exposition import CONTENT_TYPE_LATEST, generate_latest

log = logging.getLogger(__name__)

REGISTRY = CollectorRegistry(auto_describe=True)

OUTCOMES = ("ok", "429", "403", "400", "5xx", "timeout", "other")
JOBS = ("ingest", "reconciler", "prober", "sampler", "chat_day", "audience", "partitions")

POLL_LAST_SUCCESS = Gauge(
    "tgaud_poll_last_success_timestamp_seconds", "last getUpdates without error", registry=REGISTRY)
UPDATES = Counter(
    "tgaud_updates_total", "committed updates", ["type"], registry=REGISTRY)
UPDATE_LAG = Histogram(
    "tgaud_update_lag_seconds", "receive time - update date", ["type"], registry=REGISTRY,
    buckets=(1, 5, 15, 60, 300, 900, 3600, 4 * 3600, 12 * 3600, 24 * 3600, float("inf")))
API_REQUESTS = Counter(
    "tgaud_api_requests_total", "Bot API calls", ["method", "outcome"], registry=REGISTRY)
API_REQUEST_SECONDS = Histogram(
    "tgaud_api_request_seconds", "Bot API latency", ["method"], registry=REGISTRY,
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 8, 10, 20, 30, float("inf")))
INGEST_FAILURES = Counter(
    "tgaud_ingest_failures_total", "updates that failed", ["stage"], registry=REGISTRY)
DB_TRANSACTION_SECONDS = Histogram(
    "tgaud_db_transaction_seconds", "DB time per transaction", ["job"], registry=REGISTRY,
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, float("inf")))
JOB_LAST_SUCCESS = Gauge(
    "tgaud_job_last_success_timestamp_seconds", "job heartbeat", ["job"], registry=REGISTRY)
ROLLUP_AS_OF = Gauge(
    "tgaud_rollup_as_of_timestamp_seconds", "freshness of published rollups", ["rollup"], registry=REGISTRY)
LOST_SECONDS = Counter(
    "tgaud_lost_seconds_total", "confirmed lost time (gaps > 24 h)", registry=REGISTRY)
PARTITIONS_AHEAD_DAYS = Gauge(
    "tgaud_partitions_ahead_days", "partitions created ahead", ["table"], registry=REGISTRY)
CHAT_INFO = Gauge(
    "tgaud_chat_info", "join key for dashboards; a rename makes one new series", ["chat", "type", "title"],
    registry=REGISTRY)
CHAT_BOT_ADMIN = Gauge(
    "tgaud_chat_bot_admin", "bot is admin", ["chat"], registry=REGISTRY)
CHAT_PERMISSION_OK = Gauge(
    "tgaud_chat_permission_ok", "rights the bot needs", ["chat", "right"], registry=REGISTRY)
CHAT_GAP_OPEN = Gauge(
    "tgaud_chat_gap_open", "an open gap touches the chat", ["chat"], registry=REGISTRY)
LAST_TRANSITION = Gauge(
    "tgaud_last_transition_timestamp_seconds", "last chat_member event", ["chat"], registry=REGISTRY)
CHAT_MEMBERS_REPORTED = Gauge(
    "tgaud_chat_members_reported", "Telegram's count", ["chat"], registry=REGISTRY)
CHAT_MEMBERS_KNOWN = Gauge(
    "tgaud_chat_members_known", "open spells", ["chat"], registry=REGISTRY)
CHAT_MEMBERS_SUSPECT = Gauge(
    "tgaud_chat_members_suspect", "suspect spells", ["chat"], registry=REGISTRY)
CHAT_DRIFT_MISMATCH = Gauge(
    "tgaud_chat_drift_mismatch", "reported - known", ["chat"], registry=REGISTRY)
CHAT_DRIFT_RESIDUAL_24H = Gauge(
    "tgaud_chat_drift_residual_24h", "sum of delta residuals, 24 h", ["chat"], registry=REGISTRY)
CHAT_EVENTS = Counter(
    "tgaud_chat_events_total", "business rates", ["chat", "kind"], registry=REGISTRY)
CHAT_SHORT_STAYS = Counter(
    "tgaud_chat_short_stays_total", "exits within 48 h of the join", ["chat"], registry=REGISTRY)
CHAT_JOIN_REQUESTS = Counter(
    "tgaud_chat_join_requests_total", "request flow", ["chat", "outcome"], registry=REGISTRY)
CHAT_JOIN_REQUESTS_PENDING = Gauge(
    "tgaud_chat_join_requests_pending", "requests without an outcome", ["chat"], registry=REGISTRY)
PROBE_QUEUE = Gauge(
    "tgaud_probe_queue", "probe backlog", ["chat"], registry=REGISTRY)
PROBE_OLDEST_AGE = Gauge(
    "tgaud_probe_oldest_age_seconds", "probe backlog", ["chat"], registry=REGISTRY)
PROBES = Counter(
    "tgaud_probes_total", "probe answers", ["outcome"], registry=REGISTRY)
SNAPSHOT_AGE = Gauge(
    "tgaud_snapshot_age_seconds", "age of the last good snapshot", ["chat"], registry=REGISTRY)
# Not in the design table: failing job steps, counted by the scheduler (tgaud.jobs).
JOB_FAILURES = Counter(
    "tgaud_job_failures_total", "job steps that failed (logged, rolled back, retried)", ["job"], registry=REGISTRY)

# (name as exposed, type) of every row of design.md 9, for the tests and the selfcheck.
DESIGN_METRICS: tuple[tuple[str, str], ...] = (
    ("tgaud_poll_last_success_timestamp_seconds", "gauge"),
    ("tgaud_updates_total", "counter"),
    ("tgaud_update_lag_seconds", "histogram"),
    ("tgaud_api_requests_total", "counter"),
    ("tgaud_api_request_seconds", "histogram"),
    ("tgaud_ingest_failures_total", "counter"),
    ("tgaud_db_transaction_seconds", "histogram"),
    ("tgaud_job_last_success_timestamp_seconds", "gauge"),
    ("tgaud_rollup_as_of_timestamp_seconds", "gauge"),
    ("tgaud_lost_seconds_total", "counter"),
    ("tgaud_partitions_ahead_days", "gauge"),
    ("tgaud_chat_info", "gauge"),
    ("tgaud_chat_bot_admin", "gauge"),
    ("tgaud_chat_permission_ok", "gauge"),
    ("tgaud_chat_gap_open", "gauge"),
    ("tgaud_last_transition_timestamp_seconds", "gauge"),
    ("tgaud_chat_members_reported", "gauge"),
    ("tgaud_chat_members_known", "gauge"),
    ("tgaud_chat_members_suspect", "gauge"),
    ("tgaud_chat_drift_mismatch", "gauge"),
    ("tgaud_chat_drift_residual_24h", "gauge"),
    ("tgaud_chat_events_total", "counter"),
    ("tgaud_chat_short_stays_total", "counter"),
    ("tgaud_chat_join_requests_total", "counter"),
    ("tgaud_chat_join_requests_pending", "gauge"),
    ("tgaud_probe_queue", "gauge"),
    ("tgaud_probe_oldest_age_seconds", "gauge"),
    ("tgaud_probes_total", "counter"),
    ("tgaud_snapshot_age_seconds", "gauge"),
)


class Health:
    """The two ingest timestamps behind /health. Ages are measured on a monotonic clock, so a wall-clock
    correction can neither hide a stall nor fake one; the Prometheus gauge keeps Unix time. Float assignments
    are atomic, so no lock."""

    def __init__(self, window: float = 180.0, activation_id: str = "",
                 clock: Callable[[], float] = time.monotonic, wall: Callable[[], float] = time.time):
        self.window = window
        self.activation_id = activation_id
        self.clock = clock
        self.wall = wall
        self.poll_ok_at: float | None = None  # clock() values
        self.ingest_commit_at: float | None = None

    def mark_poll_ok(self) -> None:
        """Ingest: a getUpdates returned without error (empty or not)."""
        self.poll_ok_at = self.clock()
        POLL_LAST_SUCCESS.set(self.wall())

    def mark_commit(self) -> None:
        """Ingest: an update's transaction committed, or an empty poll's result was recorded successfully."""
        self.ingest_commit_at = self.clock()

    def check(self) -> tuple[int, str]:
        now = self.clock()
        problems: list[str] = []
        for what, at in (("getUpdates", self.poll_ok_at), ("ingest commit", self.ingest_commit_at)):
            if at is None:
                problems.append(f"no successful {what} yet")
            elif now - at >= self.window:
                problems.append(f"last successful {what} {now - at:.0f} s ago (limit {self.window:.0f} s)")
        suffix = f"activation={self.activation_id}"
        if problems:
            return 503, "unhealthy: " + "; ".join(problems) + " " + suffix + "\n"
        return 200, "ok " + suffix + "\n"


class MetricsServer:
    """`/metrics` and `/health` on a daemon thread. No database access."""

    def __init__(self, host: str, port: int, health: Health):
        self.health = health
        handler = _make_handler(health)
        self.httpd = ThreadingHTTPServer((host, port), handler)
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(target=self.httpd.serve_forever, name="metrics", daemon=True)

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    def start(self) -> "MetricsServer":
        self.thread.start()
        return self

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def _make_handler(health: Health) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "tgaud"
        sys_version = ""

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path == "/metrics":
                body, ctype, status = generate_latest(REGISTRY), CONTENT_TYPE_LATEST, 200
            elif path == "/health":
                status, text = health.check()
                body, ctype = text.encode(), "text/plain; charset=utf-8"
            else:
                body, ctype, status = b"not found\n", "text/plain; charset=utf-8", 404
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:  # noqa: A002
            log.debug("metrics http: " + format, *args)

    return Handler
