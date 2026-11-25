"""Size of an import on the test server (production limits: 512 MB, work_mem 2 MB, no parallel workers), case 10
of the snapshot-import brief.

One chat's import is one transaction on the web connection, so every other web request waits for it (WebDb's lock,
at most 75 s). Its cost is the COPY into tg.snapshot_member, where the contract's per-row trigger (migration 008,
the erasure check) dominates, plus reading and checking the file's rows twice (before any write, then for the
COPY). Measured on the development machine (docs/tgaud.md, "The Import page"): about 4 s per 100 000 members, so
the 350 000 members of the largest chat a 32 MiB file can hold took 15-18 s. importer.MAX_MEMBERS (200 000) keeps a
chat at the ceiling near 10 s: half the 20 s aim, a third of the 30 s deadline, for a slower server.

    the suite      a chat at the ceiling through the real listener with a report request waiting on the
                   connection, and a chat one over it refused before any write
    measurement    the largest chat a file of TGAUD_IMPORT_SCALE_MIB MiB holds (default 4; 32 is the brief's
                   measurement), imported with a competing request: members, file size, transaction seconds
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import os
import random
import socket
import threading
import time

import pytest
from import_lab import A, B, Lab, multipart
from test_import_listener import head, response
from test_web import Client

from tgaud import config
from tgaud import web as web_pkg
from tgaud.web import importer, upload
from tgstats import db as tgdb

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 11, 20, 9, tzinfo=UTC)


def build(path, chats: dict[int, int]) -> int:
    """A new-format file of the given chats and member counts, the densest a collect writes (no names, a join
    date each), through tgstats' own writer; returns its size."""
    rng = random.Random(7)
    conn = tgdb.connect(path)
    for chat, n in chats.items():
        members = [tgdb.Member(tgdb.UserRow(10**9 + u, None, None, None),
                               (T0 + dt.timedelta(milliseconds=10 * u)).isoformat(timespec="microseconds"),
                               (T0 - dt.timedelta(days=rng.randint(1, 2000))).isoformat(), None, None, None, False)
                   for u in range(1, n + 1)]
        end = (T0 + dt.timedelta(milliseconds=10 * n + 1)).isoformat(timespec="microseconds")
        tgdb.add_snapshot(conn, tgdb.ChatRow(chat, "channel", "scale", None), members, n,
                          collection=tgdb.Collection(f"0e9b8a61-3d4e-4f7c-9a7b-{abs(chat) % 10**12:012d}",
                                                     T0.isoformat(timespec="microseconds"), end, "listing", n, n))
    conn.execute("VACUUM")
    conn.close()
    return path.stat().st_size


class Competitor:
    """A report request every 0.2 s on the same web app while the import runs; records how long each waited."""

    def __init__(self, get) -> None:
        self.get = get
        self.waits: list[tuple[float, int]] = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self.stop.is_set():
            started = time.monotonic()
            status = self.get()
            self.waits.append((time.monotonic() - started, status))
            self.stop.wait(0.2)

    def __enter__(self) -> "Competitor":
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop.set()
        self.thread.join(120)


@pytest.fixture
def lab(pg, migrated_dsn, tmp_path):
    lab = Lab(pg, migrated_dsn, tmp_path)
    yield lab
    lab.close()


def test_a_chat_at_the_ceiling_imports_through_the_listener_while_a_report_waits(lab, monkeypatch, capsys):
    n = importer.MAX_MEMBERS
    path = lab.tmp / "ceiling.db"
    size = build(path, {A: n})
    assert size <= upload.MAX_FILE
    cfg = dataclasses.replace(lab.cfg, web_listen=config.Listen("127.0.0.1", 0))
    server = web_pkg.WebServer(cfg, cfg.web_proxy_token, lab.connect, fence=lab.ctx.fence)
    server.app.spool_dir = str(lab.spool)
    server.start()
    lab.port = server.port
    took: dict[str, float] = {}
    real = importer._write

    def timed(*args, **kw):
        started = time.monotonic()
        try:
            return real(*args, **kw)
        finally:
            took["transaction"] = time.monotonic() - started

    monkeypatch.setattr(importer, "_write", timed)

    def report() -> int:
        with socket.create_connection(("127.0.0.1", server.port)) as s:
            s.sendall(f"GET /reports/coverage HTTP/1.1\r\nHost: 127.0.0.1:{server.port}\r\n"
                      f"X-Tgaud-Proxy-Token: {cfg.web_proxy_token.reveal()}\r\nConnection: close\r\n\r\n".encode())
            return response(s, timeout=120)[0]

    try:
        ctype, body = multipart(path.read_bytes())
        started = time.monotonic()
        with Competitor(report) as other:
            with socket.create_connection(("127.0.0.1", server.port)) as s:
                s.sendall(head(lab, len(body), content_type=ctype) + body)
                status, text = response(s, timeout=120)
        total = time.monotonic() - started
    finally:
        server.close()
    assert status == 200 and f"imported: {n} members written" in text, text[-1500:]
    assert [x["fetched"] for x in lab.snapshots(A)] == [n]
    assert other.waits and all(st == 200 for _, st in other.waits)
    longest = max(w for w, _ in other.waits)
    with capsys.disabled():
        print(f"\nimport scale: {n} members ({size / 2**20:.1f} MiB): request {total:.1f} s, chat transaction "
              f"{took['transaction']:.1f} s, a competing report waited at most {longest:.1f} s")
    assert took["transaction"] < importer.CHAT_SECONDS  # the hard deadline; the aim is 20 s
    assert longest < web_pkg.WebDb(lambda: None)._lock_wait  # no report gave up on the connection
    assert list(lab.spool.iterdir()) == []


def test_a_chat_over_the_ceiling_is_refused_with_the_reason_and_nothing_written(lab):
    path = lab.tmp / "over.db"
    build(path, {A: importer.MAX_MEMBERS + 1, B: 10})
    report = lab.run(path)
    out = {o.chat_id: o for o in report.outcomes}
    assert (out[A].status, out[B].status) == ("refused", "imported")
    assert out[A].reason.startswith(f"too large: {importer.MAX_MEMBERS + 1} members") and \
        str(importer.MAX_MEMBERS) in out[A].reason
    assert lab.snapshots(A) == [] and [x["fetched"] for x in lab.snapshots(B)] == [10]


def test_the_largest_chat_a_file_of_the_limit_holds(lab, monkeypatch, capsys):
    """The measurement behind MAX_MEMBERS: TGAUD_IMPORT_SCALE_MIB=32 builds the largest chat a 32 MiB file holds."""
    mib = float(os.environ.get("TGAUD_IMPORT_SCALE_MIB", "4"))
    limit = int(mib * 2**20)
    n = int(limit / 110)  # a first guess, then refined from the measured bytes per member
    for attempt in range(4):
        path = lab.tmp / f"largest-{attempt}.db"
        size = build(path, {A: n})
        if size <= limit and limit - size < limit * 0.03:
            break
        n = int(n * limit / size * 0.99)
    assert size <= limit
    monkeypatch.setattr(importer, "MAX_MEMBERS", n)
    took: dict[str, float] = {}
    real = importer._write

    def timed(*args, **kw):
        started = time.monotonic()
        try:
            return real(*args, **kw)
        finally:
            took["transaction"] = time.monotonic() - started

    monkeypatch.setattr(importer, "_write", timed)
    client = Client(lab.app)
    with Competitor(lambda: client.get("/reports/coverage").status) as other:
        report = lab.run(path, seconds=300)
    assert lab.outcomes(report) == {A: ("imported", n)}
    longest = max((w for w, _ in other.waits), default=0.0)
    assert all(st == 200 for _, st in other.waits)
    with capsys.disabled():
        print(f"\nimport scale: the largest chat a {mib:g} MiB file holds: {n} members ({size / 2**20:.2f} MiB, "
              f"{size / n:.0f} bytes each); chat transaction {took['transaction']:.1f} s "
              f"({took['transaction'] / n * 1e5:.1f} s per 100 000); a competing report waited at most {longest:.1f} s")
