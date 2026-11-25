"""POST /import through the real listener (tgaud.web.WebServer on a socket, case 1 and 2 of the snapshot-import
brief): the proxy token and the origin are checked before the body is read, the body is streamed under its own
deadline, every refusal is an error page the client can still read, nothing changes in tg.snapshot, and no spool
file is left. Other requests keep working during an upload, and a second upload gets 409 before its body is read.
"""

from __future__ import annotations

import dataclasses
import socket
import sqlite3
import threading
import time

import psycopg
import pytest
from import_lab import BOUNDARY, A, Lab, at, multipart
from test_web import PROXY_TOKEN

from tgaud import config
from tgaud import web as web_pkg
from tgaud.web import upload


@pytest.fixture
def served(pg, migrated_dsn, tmp_path):
    lab = Lab(pg, migrated_dsn, tmp_path)
    cfg = dataclasses.replace(lab.cfg, web_listen=config.Listen("127.0.0.1", 0))
    server = web_pkg.WebServer(cfg, cfg.web_proxy_token, lab.connect, fence=lab.ctx.fence)
    server.app.spool_dir = str(lab.spool)
    server.start()
    lab.port = server.port
    yield lab
    server.close()
    lab.close()


def head(lab, length: int, *, token: str | None = PROXY_TOKEN, origin: str | None = "same",
         content_type: str = f"multipart/form-data; boundary={BOUNDARY}", path: str = "/import") -> bytes:
    lines = [f"POST {path} HTTP/1.1", f"Host: 127.0.0.1:{lab.port}", f"Content-Type: {content_type}",
             f"Content-Length: {length}"]
    if token is not None:
        lines.append(f"X-Tgaud-Proxy-Token: {token}")
    if origin is not None:
        lines.append(f"Origin: {f'http://127.0.0.1:{lab.port}' if origin == 'same' else origin}")
    return ("\r\n".join(lines) + "\r\n\r\n").encode()


def response(s: socket.socket, timeout: float = 10.0) -> tuple[int, str]:
    """Read the answer until the server closes the connection."""
    s.settimeout(timeout)
    data = b""
    while True:
        try:
            chunk = s.recv(65536)
        except ConnectionResetError:
            break
        if not chunk:
            break
        data += chunk
    status = int(data.split(b" ", 2)[1]) if data.startswith(b"HTTP/") else 0
    return status, data.decode("utf-8", "replace")


def send(lab, payload: bytes, *, content_type: str | None = None, **kw) -> tuple[int, str]:
    ctype, body = multipart(payload) if content_type is None else (content_type, payload)
    with socket.create_connection(("127.0.0.1", lab.port)) as s:
        s.sendall(head(lab, len(body), content_type=ctype, **kw) + body)
        return response(s)


def get(lab, path: str) -> int:
    with socket.create_connection(("127.0.0.1", lab.port)) as s:
        s.sendall(f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{lab.port}\r\nX-Tgaud-Proxy-Token: {PROXY_TOKEN}\r\n"
                  "Connection: close\r\n\r\n".encode())
        return response(s)[0]


def untouched(lab) -> None:
    assert lab.snapshots() == [] and lab.q("SELECT count(*) FROM tg.snapshot_member") == [(0,)]
    assert list(lab.spool.iterdir()) == []


@pytest.mark.parametrize("token, origin, why", [
    (None, "same", "forbidden"), ("wrong-token", "same", "forbidden"),
    (PROXY_TOKEN, "https://evil.example", "cross-site form refused"), (PROXY_TOKEN, None, "cross-site form refused"),
], ids=["no-token", "wrong-token", "foreign-origin", "no-origin"])
def test_the_token_and_the_origin_are_checked_before_the_body_is_read(served, token, origin, why):  # case 1
    with socket.create_connection(("127.0.0.1", served.port)) as s:
        s.sendall(head(served, 10 * 1024 * 1024, token=token, origin=origin))  # announced, never sent
        started = time.monotonic()
        status, text = response(s, timeout=5)
        assert time.monotonic() - started < 4  # answered without waiting for the body
    assert status == 403 and why in text
    untouched(served)


@pytest.mark.parametrize("token, status", [(None, 403), (PROXY_TOKEN, 405)], ids=["no-token", "token"])
def test_another_method_on_the_import_path_meets_the_guards_before_its_body(served, token, status):  # case 1
    with socket.create_connection(("127.0.0.1", served.port)) as s:
        s.sendall(head(served, 10 * 1024 * 1024, token=token).replace(b"POST ", b"PUT ", 1))
        got, _ = response(s, timeout=5)
    assert got == status
    untouched(served)


def test_a_body_over_32_mib_is_refused_before_it_is_read_and_the_client_reads_the_page(served):  # case 1
    too_big = upload.MAX_BODY + 1
    with socket.create_connection(("127.0.0.1", served.port)) as s:
        s.sendall(head(served, too_big))
        status, text = response(s, timeout=5)
    assert status == 413 and "larger than 32 MiB" in text and "<h1>" in text
    # a client that sends the whole body before it reads (http.client does): the server drains it and the page
    # still arrives instead of a reset
    import http.client
    conn = http.client.HTTPConnection("127.0.0.1", served.port, timeout=30)
    conn.request("POST", "/import", body=b"x" * too_big, headers={
        "Content-Type": f"multipart/form-data; boundary={BOUNDARY}", "X-Tgaud-Proxy-Token": PROXY_TOKEN,
        "Origin": f"http://127.0.0.1:{served.port}"})
    r = conn.getresponse()
    assert r.status == 413 and b"larger than 32 MiB" in r.read()
    conn.close()
    untouched(served)


def test_a_file_part_over_32_mib_in_a_body_within_the_limit_is_refused(served):  # case 1
    ctype, body = multipart(b"\0" * (upload.MAX_FILE + 1))
    assert len(body) <= upload.MAX_BODY
    status, text = send(served, body, content_type=ctype)
    assert status == 413 and "larger than 32 MiB" in text
    untouched(served)


def test_a_short_body_ended_by_a_half_close_gets_its_error_page(served):  # case 1
    ctype, body = multipart(b"SQLite format 3\0" + b"\0" * 200_000)
    with socket.create_connection(("127.0.0.1", served.port)) as s:
        s.sendall(head(served, len(body), content_type=ctype) + body[:50_000])
        s.shutdown(socket.SHUT_WR)  # the client gives up sending, and still reads
        status, text = response(s)
    assert status == 400 and "ended before its declared length" in text
    untouched(served)


def test_a_paced_upload_past_the_deadline_gets_408_while_it_still_sends(served, monkeypatch):  # case 1
    monkeypatch.setattr(web_pkg._Handler, "upload_deadline", 1.0)
    ctype, body = multipart(b"SQLite format 3\0" + b"\0" * 20_000)
    with socket.create_connection(("127.0.0.1", served.port)) as s:
        s.sendall(head(served, len(body), content_type=ctype))
        started, got = time.monotonic(), b""
        s.settimeout(0.2)
        for i in range(0, len(body), 500):  # 500 bytes every 0.2 s: 8 s for the whole body
            try:
                s.sendall(body[i:i + 500])
            except OSError:
                break
            try:
                chunk = s.recv(65536)
                got += chunk
                if not chunk or b"</html>" in got:
                    break
            except TimeoutError:
                pass
        status, text = response(s, timeout=5)
        took = time.monotonic() - started
    text = got.decode("utf-8", "replace") + text
    assert text.startswith("HTTP/1.0 408") and "took longer than the server waits" in text, text[:300]
    assert took < 6, took
    untouched(served)


def _sqlite_without_tgstats(path) -> bytes:
    raw = sqlite3.connect(path)
    raw.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, text TEXT)")
    raw.execute("INSERT INTO notes (text) VALUES ('hello')")
    raw.commit()
    raw.close()
    return path.read_bytes()


def _with_a_view(lab) -> bytes:
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    f.raw.execute("CREATE VIEW everyone AS SELECT * FROM users")
    f.raw.commit()
    return f.close().read_bytes()


@pytest.mark.parametrize("kind, why", [
    ("not-sqlite", "not a SQLite file"), ("plain-sqlite", "none of the tgstats tables"), ("empty", "file is empty"),
    ("view", "has a view"), ("not-multipart", "multipart/form-data"), ("other-field", "the form&#39;s file field"),
])
def test_files_and_bodies_that_are_not_a_tgstats_db_get_their_error_page(served, tmp_path, kind, why):  # case 1, rule 5
    payload = {"not-sqlite": lambda: b"PK\x03\x04 this is a zip, not a database" * 100,
               "plain-sqlite": lambda: _sqlite_without_tgstats(tmp_path / "plain.db"),
               "empty": lambda: b"", "view": lambda: _with_a_view(served),
               "not-multipart": lambda: b"file=tgstats.db",
               "other-field": lambda: b""}[kind]()
    if kind == "not-multipart":
        status, text = send(served, payload, content_type="application/x-www-form-urlencoded")
    elif kind == "other-field":
        ctype, body = multipart(b"SQLite format 3\0", name="upload")
        status, text = send(served, body, content_type=ctype)
    else:
        status, text = send(served, payload)
    assert status == 400 and why in text, text[-2000:]
    assert '<form method="post" action="/import" enctype="multipart/form-data"' in text  # the page, to try again
    untouched(served)


def test_a_valid_file_over_64_kib_imports_through_the_listener(served):  # case 1
    f = served.file()
    f.snapshot(A, {u: at(1) for u in range(1, 3001)}, at(0))
    data = f.close().read_bytes()
    assert len(data) > 64 * 1024
    status, text = send(served, data)
    assert status == 200 and "imported: 3000 members written" in text
    assert [s["fetched"] for s in served.snapshots(A)] == [3000]
    assert list(served.spool.iterdir()) == []


def test_a_second_upload_gets_409_and_other_pages_answer_during_an_upload(served):  # case 2
    f = served.file()
    f.snapshot(A, {u: at(1) for u in range(1, 2001)}, at(0))
    ctype, body = multipart(f.close().read_bytes())
    first = socket.create_connection(("127.0.0.1", served.port))
    try:
        first.sendall(head(served, len(body), content_type=ctype) + body[:1000])  # the upload is under way
        for _ in range(100):
            if list(served.spool.iterdir()):
                break
            time.sleep(0.02)
        assert len(list(served.spool.iterdir())) == 1  # streaming into its one spool file
        with socket.create_connection(("127.0.0.1", served.port)) as second:
            second.sendall(head(served, len(body), content_type=ctype))  # its body is never needed
            status, text = response(second, timeout=5)
        assert status == 409 and "another import is running" in text
        assert get(served, "/healthz") == 200
        assert get(served, "/reports/coverage") == 200
        assert get(served, "/") == 200
        first.sendall(body[1000:])
        status, text = response(first)
        assert status == 200 and "imported: 2000 members written" in text
    finally:
        first.close()
    assert list(served.spool.iterdir()) == []
    status, text = send(served, body, content_type=ctype)  # the lock is free again: a re-upload is answered
    assert status == 200 and "already imported" in text


def test_every_other_route_keeps_its_64_kib_form_limit(served):
    big = b"q=" + b"1" * (64 * 1024)
    with socket.create_connection(("127.0.0.1", served.port)) as s:
        s.sendall(head(served, len(big), content_type="application/x-www-form-urlencoded", path="/user") + big)
        status, _ = response(s)
    assert status == 413
    with socket.create_connection(("127.0.0.1", served.port)) as s:  # also a multipart body to another route
        ctype, body = multipart(b"x" * 100)
        s.sendall(head(served, len(body), content_type=ctype, path="/user") + body)
        status, text = response(s)
    assert status == 400 and "application/x-www-form-urlencoded" in text


def test_an_upload_aborted_by_the_client_leaves_no_spool_file(served):
    ctype, body = multipart(b"SQLite format 3\0" + b"\0" * 100_000)
    s = socket.create_connection(("127.0.0.1", served.port))
    s.sendall(head(served, len(body), content_type=ctype) + body[:30_000])
    for _ in range(100):
        if list(served.spool.iterdir()):
            break
        time.sleep(0.02)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")  # close with a reset
    s.close()
    for _ in range(200):
        if not list(served.spool.iterdir()) and not served.app.importing.locked():
            break
        time.sleep(0.02)
    untouched(served)
    assert not served.app.importing.locked()
