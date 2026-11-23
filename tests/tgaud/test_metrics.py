import re
import urllib.error
import urllib.request

import pytest

from tgaud import metrics


@pytest.fixture
def server():
    health = metrics.Health(window=180, activation_id="act-42")
    srv = metrics.MetricsServer("127.0.0.1", 0, health).start()
    yield srv
    srv.close()


def get(srv, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{srv.port}{path}", timeout=5) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def test_metrics_lists_every_metric_of_design_9(server):
    status, body = get(server, "/metrics")
    assert status == 200
    types = dict(re.findall(r"^# TYPE (\S+) (\S+)$", body, re.M))
    for name, kind in metrics.DESIGN_METRICS:
        base = name.removesuffix("_total") if kind == "counter" else name
        assert kind in (types.get(name), types.get(base)), name
        assert re.search(rf"^# HELP ({name}|{base}) \S", body, re.M), name
    assert "counter" in (types.get("tgaud_job_failures_total"), types.get("tgaud_job_failures"))
    assert len(metrics.DESIGN_METRICS) == 29


def test_design_table_rows_are_all_defined():
    design = (metrics.__file__.rsplit("/src/", 1)[0] + "/docs/design/design.md")
    text = open(design).read().split("## 9. Metrics", 1)[1].split("## 10.", 1)[0]
    names = set()
    for row in re.findall(r"^\| `(tgaud_[^|]+)\|", text, re.M):
        for part in re.findall(r"`?(tgaud_\w+|_\w+)`?", row):
            names.add(part)
    expanded = {"tgaud_chat_members" + n if n.startswith("_") else n for n in names}
    expanded.discard("tgaud_chat_members_reported")  # listed as the first of three
    expanded |= {"tgaud_chat_members_reported"}
    defined = {n for n, _ in metrics.DESIGN_METRICS}
    missing = {n for n in expanded if n.startswith("tgaud_") and n not in defined and not n.endswith("_")}
    assert not missing, missing


def test_health_starts_503_with_activation(server):
    status, body = get(server, "/health")
    assert status == 503 and "activation=act-42" in body and "no successful getUpdates yet" in body
    assert get(server, "/nope")[0] == 404


def test_health_200_only_with_both_ingest_timestamps(server):
    server.health.mark_poll_ok()
    status, body = get(server, "/health")
    assert status == 503 and "ingest commit" in body
    server.health.mark_commit()
    status, body = get(server, "/health")
    assert (status, body) == (200, "ok activation=act-42\n")
    assert metrics.REGISTRY.get_sample_value("tgaud_poll_last_success_timestamp_seconds") == server.health.poll_ok_at


def test_health_turns_503_after_the_window():
    now = [1000.0]
    h = metrics.Health(window=180, activation_id="", clock=lambda: now[0])
    h.mark_poll_ok()
    h.mark_commit()
    assert h.check()[0] == 200
    now[0] += 179
    assert h.check()[0] == 200
    now[0] += 2
    status, body = h.check()
    assert status == 503 and "181 s ago" in body and body.endswith("activation=\n")
