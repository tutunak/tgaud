"""The Import page (snapshot-import task): a tgstats.db written into tg.snapshot / tg.snapshot_member as design.md 5
describes, then applied by the reconciler through its real step loop. Synthetic files written by the tgstats code
itself (tests/tgaud/import_lab.py); the brief's case numbers are in the test names' comments. The transport
refusals through the real listener are in test_import_listener.py, the member ceiling in test_import_scale.py.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
import threading
import time

import psycopg
import pytest
from import_lab import A, B, C, U, Lab, at, legacy_file, migrate_chat

from tgaud import history
from tgaud.web import importer, tgstats_file
from tgstats import db as tgdb

UTC = dt.timezone.utc


@pytest.fixture
def lab(pg, migrated_dsn, tmp_path):
    lab = Lab(pg, migrated_dsn, tmp_path)
    yield lab
    lab.close()


def now() -> dt.datetime:
    return dt.datetime.now(UTC)


# ---------------------------------------------------------------------------------------------------------------
# What is imported (cases 3, 4, 6; rule 8)
# ---------------------------------------------------------------------------------------------------------------


def test_the_latest_snapshot_of_each_tracked_chat_is_written_as_the_contract_writes_it(lab):  # case 4
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))  # an earlier collection of A: left alone
    ua = f.snapshot(A, {1: {"seen_at": at(61), "joined_date": at(-9000), "inviter_id": 7, "via_request": True,
                            "subscription_until": at(90000), "is_premium": True, "last_name": "L"},
                        2: {"seen_at": at(62), "is_bot": True}, 3: {"seen_at": at(62), "is_deleted": True}},
                    at(60), finished=at(63), reported=3, reported_end=4, method="deep")
    f.snapshot(B, {5: at(31)}, at(30), complete=False)
    f.snapshot(U, {4: at(5)}, at(4))
    report = lab.run(f.close())
    assert lab.outcomes(report) == {A: ("imported", 3), B: ("imported", 1), U: ("skipped", None)}
    assert "not tracked" in next(o for o in report.outcomes if o.chat_id == U).reason
    [a] = lab.snapshots(A)
    assert (a["started_at"], a["finished_at"], a["method"], a["reported_at_start"], a["reported_at_end"],
            a["fetched"], a["complete"], str(a["collect_uuid"]), a["quality"]) == (
        at(60), at(63), "deep", 3, 4, 3, False, ua, None)  # 3 of the larger count, 4: tgstats said incomplete
    m = lab.members(a["snapshot_id"])
    assert {k: m[1][k] for k in ("seen_at", "joined_date", "inviter_id", "via_request", "subscription_until",
                                 "is_bot", "is_deleted", "is_premium", "username", "first_name", "last_name")} == {
        "seen_at": at(61), "joined_date": at(-9000), "inviter_id": 7, "via_request": True,
        "subscription_until": at(90000), "is_bot": False, "is_deleted": False, "is_premium": True,
        "username": "user1", "first_name": "First1", "last_name": "L"}
    assert (m[2]["is_bot"], m[3]["is_deleted"]) == (True, True)
    [b] = lab.snapshots(B)
    assert b["complete"] is False
    assert lab.snapshots(U) == [] and lab.snapshots(C) == []  # tracked C is not in the file: untouched


def test_re_uploading_the_same_file_imports_nothing_new(lab):  # case 3
    f = lab.file()
    f.snapshot(A, {1: at(1), 2: at(2)}, at(0))
    f.snapshot(B, {3: at(1)}, at(0))
    path = f.close()
    first = lab.run(path)
    assert lab.outcomes(first) == {A: ("imported", 2), B: ("imported", 1)}
    again = lab.run(path)
    assert [o.status for o in again.outcomes] == ["skipped", "skipped"]
    assert all(o.reason.startswith("already imported") for o in again.outcomes)
    assert len(lab.snapshots()) == 2 and lab.q("SELECT count(*) FROM tg.snapshot_member") == [(3,)]


def test_an_old_format_snapshot_is_refused_and_writes_nothing(lab, tmp_path):  # case 4
    path = legacy_file(tmp_path / "old.db", {A: [1, 2, 3], U: [4]})
    report = lab.run(path)
    assert lab.outcomes(report) == {A: ("refused", None), U: ("skipped", None)}
    assert next(o for o in report.outcomes if o.chat_id == A).reason.startswith("old format")
    assert report.current is False and lab.snapshots() == []
    # a file migrated by this tgstats: chats last collected by the old version still refuse, new ones import
    f = lab.file()
    f.legacy(A, [1, 2], at(-60))
    f.snapshot(B, {5: at(1)}, at(0))
    report = lab.run(f.close())
    assert lab.outcomes(report) == {A: ("refused", None), B: ("imported", 1)} and report.current


def test_bad_times_are_refused_and_a_later_valid_collection_still_imports(lab):  # case 4, rule 4
    t = now()
    f = lab.file()
    f.snapshot(A, {1: t - dt.timedelta(minutes=5)}, t - dt.timedelta(minutes=10), finished=t + dt.timedelta(hours=1))
    f.snapshot(B, {2: at(-1)}, at(0), finished=at(10))  # a page before the collection started
    f.snapshot(C, {3: at(1)}, at(0))  # valid: imported beside them
    report = lab.run(f.close())
    out = {o.chat_id: o for o in report.outcomes}
    assert lab.outcomes(report) == {A: ("refused", None), B: ("refused", None), C: ("imported", 1)}
    assert out[A].reason.startswith("bad times or values: finished_at") and "future" in out[A].reason
    assert out[B].reason.startswith("bad times or values: a member's seen_at") and "outside" in out[B].reason
    assert lab.snapshots(A) == [] and lab.snapshots(B) == []
    g = lab.file()
    g.snapshot(A, {1: at(61)}, at(60))
    g.snapshot(B, {2: at(61)}, at(60))
    assert lab.outcomes(lab.run(g.close())) == {A: ("imported", 1), B: ("imported", 1)}


@pytest.mark.parametrize("change, why", [
    ("UPDATE snapshots SET uuid = 'not-a-uuid'", "not a uuid"),
    ("UPDATE snapshots SET uuid = '{' || uuid || '}'", "well-formed"),
    ("UPDATE snapshots SET uuid = '00000000-0000-0000-0000-000000000000'", "well-formed"),
    ("UPDATE snapshots SET reported_start = -1", "negative"),
    ("UPDATE snapshots SET reported_end = -1", "negative"),
    ("UPDATE snapshots SET started_at = '2026-11-20 09:00:00'", "no time zone"),
    ("UPDATE snapshots SET finished_at = '2026-11-20T08:00:00+00:00'", "before started_at"),
    ("UPDATE snapshots SET finished_at = 'soon'", "not a time"),
    ("UPDATE snapshots SET method = 'scrape'", "method"),
    ("UPDATE memberships SET user_id = -5", "positive"),
    ("UPDATE memberships SET joined_date = 'yesterday'", "not a time"),
], ids=["uuid", "uuid-braces", "uuid-nil", "count-start", "count-end", "naive-time", "order", "garbled-time",
        "method", "user-id", "joined-date"])
def test_a_value_error_refuses_that_snapshot_only(lab, change, why):  # rule 4
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    f.snapshot(B, {2: at(1)}, at(0))
    f.raw.execute("PRAGMA foreign_keys = OFF")
    f.raw.execute(f"{change} WHERE chat_id = {B}" if change.split()[1] == "snapshots" else
                  f"{change} WHERE snapshot_id IN (SELECT id FROM snapshots WHERE chat_id = {B})")
    f.raw.commit()
    report = lab.run(f.close())
    assert lab.outcomes(report) == {A: ("imported", 1), B: ("refused", None)}
    assert why in next(o for o in report.outcomes if o.chat_id == B).reason
    assert lab.snapshots(B) == []


def _structural(f, kind: str) -> None:
    raw = f.raw
    raw.execute("PRAGMA foreign_keys = OFF")
    if kind == "text-user-id":
        raw.execute(f"UPDATE memberships SET user_id = 'x' || user_id WHERE snapshot_id = "
                    f"(SELECT id FROM snapshots WHERE chat_id = {B})")
    elif kind == "float-count":
        raw.execute(f"UPDATE snapshots SET fetched = 1.5 WHERE chat_id = {B}")
    elif kind == "blob-name":
        raw.execute("UPDATE memberships SET first_name = x'00ff'")
    elif kind == "extra-column":
        raw.execute("ALTER TABLE memberships ADD COLUMN note TEXT")
    elif kind == "extra-table":
        raw.execute("CREATE TABLE notes (x)")
    elif kind == "view":
        raw.execute("CREATE VIEW v AS SELECT * FROM users")
    elif kind == "trigger":
        raw.execute("CREATE TRIGGER t AFTER INSERT ON users BEGIN DELETE FROM users; END")
    elif kind == "missing-table":
        raw.execute("DROP TABLE users")
    elif kind == "same-uuid":
        raw.execute("DROP INDEX snapshots_by_uuid")
        raw.execute(f"UPDATE snapshots SET uuid = (SELECT uuid FROM snapshots WHERE chat_id = {A}) WHERE chat_id = {B}")
    raw.commit()


@pytest.mark.parametrize("kind", ["text-user-id", "float-count", "blob-name", "extra-column", "extra-table", "view",
                                  "trigger", "missing-table", "same-uuid"])
def test_a_structurally_broken_file_imports_nothing(lab, kind):  # rule 4, rule 5
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    f.snapshot(B, {2: at(1)}, at(0))
    _structural(f, kind)
    with pytest.raises(tgstats_file.FileError):
        lab.run(f.close())
    assert lab.snapshots() == []


def test_a_virtual_table_is_refused(lab, tmp_path):  # rule 5
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    try:
        f.raw.execute("CREATE VIRTUAL TABLE fts USING fts5(x)")
    except sqlite3.OperationalError:
        pytest.skip("this SQLite has no FTS5")
    f.raw.commit()
    with pytest.raises(tgstats_file.FileError, match="virtual table"):
        lab.run(f.close())
    assert lab.snapshots() == []


def test_the_latest_is_by_started_at_not_by_row_id(lab):  # rule 8
    f = lab.file()
    later = f.snapshot(A, {1: at(61), 2: at(62)}, at(60))
    f.snapshot(A, {1: at(1)}, at(0))  # collected earlier, stored later (a higher row id)
    f.snapshot(B, {1: at(61)}, at(60))
    f.snapshot(B, {1: at(1)}, at(0))
    f.raw.execute(f"UPDATE snapshots SET started_at = 'garbled' WHERE chat_id = {B} AND id = "
                  f"(SELECT max(id) FROM snapshots WHERE chat_id = {B})")
    f.raw.commit()
    report = lab.run(f.close())
    assert lab.outcomes(report) == {A: ("imported", 2), B: ("refused", None)}
    assert next(o for o in report.outcomes if o.chat_id == A).snapshots == 2
    assert "started_at is not a time" in next(o for o in report.outcomes if o.chat_id == B).reason
    assert str(lab.snapshots(A)[0]["collect_uuid"]) == later


def test_a_snapshot_not_newer_than_one_tgaud_has_is_skipped(lab):  # case 6
    f = lab.file()
    f.snapshot(A, {1: at(61)}, at(60))
    lab.run(f.close())
    for started in (at(0), at(60)):  # older, and the same start
        g = lab.file()
        g.snapshot(A, {1: started + dt.timedelta(minutes=1), 9: started + dt.timedelta(minutes=1)}, started)
        report = lab.run(g.close())
        assert lab.outcomes(report) == {A: ("skipped", None)}
        assert report.outcomes[0].reason.startswith("not newer")
    assert len(lab.snapshots(A)) == 1
    h = lab.file()
    h.snapshot(A, {1: at(121)}, at(120))
    assert lab.outcomes(lab.run(h.close())) == {A: ("imported", 1)}


# ---------------------------------------------------------------------------------------------------------------
# Names and erasure (cases 7, 8; rule 1)
# ---------------------------------------------------------------------------------------------------------------


def test_without_store_names_no_name_or_username_is_written_anywhere(pg, migrated_dsn, tmp_path):  # case 7
    lab = Lab(pg, migrated_dsn, tmp_path, TGAUD_STORE_NAMES="false")
    try:
        f = lab.file()
        f.snapshot(A, {1: {"seen_at": at(1), "username": "secret_u", "first_name": "Secret", "last_name": "Name"}},
                   at(0))
        assert lab.outcomes(lab.run(f.close())) == {A: ("imported", 1)}
        [s] = lab.snapshots(A)
        m = lab.members(s["snapshot_id"])[1]
        assert (m["username"], m["first_name"], m["last_name"]) == (None, None, None)
        lab.reconcile()
        assert lab.q("SELECT username, first_name, last_name FROM tg.tg_user WHERE user_id = 1") == [(None, None, None)]
        dump = lab.q("SELECT count(*) FROM tg.snapshot_member WHERE username IS NOT NULL OR first_name IS NOT NULL "
                     "OR last_name IS NOT NULL")
        assert dump == [(0,)]
        assert "Names and usernames are not stored" in lab.app.import_form(None, {}).body.decode()
    finally:
        lab.close()


def _forget(lab, *users: int) -> dt.datetime:
    with psycopg.connect(lab.dsn) as c:
        for u in users:
            c.execute("SELECT tg.forget_user(%s)", (u,))
        return c.execute("SELECT max(erased_at) FROM tg.erasure").fetchone()[0]


def test_an_erased_user_stamped_after_the_erasure_stays_erased_and_their_inviter_reference_goes(lab):  # case 8, rule 1
    lab.event(A, 7, at(-600), "left", "member")
    erased = _forget(lab, 7)
    t = now()
    # observed before the erasure, but the collector's clock says after it: still never imported
    f = lab.file()
    f.snapshot(A, {7: t + dt.timedelta(minutes=1), 8: {"seen_at": t + dt.timedelta(minutes=1), "inviter_id": 7},
                   9: {"seen_at": t + dt.timedelta(minutes=1), "inviter_id": 10}},
               t - dt.timedelta(minutes=10), finished=t + dt.timedelta(minutes=2))
    assert t + dt.timedelta(minutes=1) > erased
    report = lab.run(f.close())
    assert lab.outcomes(report) == {A: ("imported", 2)}
    assert report.outcomes[0].listed == 3 and "1 left out" in report.outcomes[0].reason
    [s] = lab.snapshots(A)
    assert s["fetched"] == 2  # the rows written, not the file's
    assert {u: m["inviter_id"] for u, m in lab.members(s["snapshot_id"]).items()} == {8: None, 9: 10}
    lab.reconcile()
    assert lab.q("SELECT count(*) FROM tg.tg_user WHERE user_id = 7") == [(0,)]
    assert lab.q("SELECT count(*) FROM tg.sighting WHERE user_id = 7") == [(0,)]
    assert lab.spells(A, 7) == []


def test_suppressed_rows_that_cross_the_good_threshold_grade_partial(lab):  # case 8
    erased = list(range(100, 111))  # 11 of 100: more than the 10 the good rule allows
    _forget(lab, *erased)
    f = lab.file()
    f.snapshot(A, {u: at(1) for u in range(100, 200)}, at(0), reported=100)
    f.snapshot(B, {u: at(1) for u in range(100, 200)}, at(0), reported=111)  # the same shortfall without erasure
    lab.run(f.close())
    lab.reconcile()
    [a] = lab.snapshots(A)
    assert (a["fetched"], a["complete"], a["quality"]) == (89, True, "partial")
    [b] = lab.snapshots(B)
    assert (b["fetched"], b["quality"]) == (89, "partial")
    c = lab.file()
    c.snapshot(C, {u: at(1) for u in range(100, 200)}, at(0), reported=100)
    with psycopg.connect(lab.dsn) as conn:
        conn.execute("DELETE FROM tg.erasure WHERE user_hash IN (SELECT tg.erasure_hash(u) FROM unnest(%s::bigint[]) u)",
                     (erased[1:],))  # only one erased: 99 of 100 is still good
    lab.run(c.close())
    lab.reconcile()
    [cc] = lab.snapshots(C)
    assert (cc["fetched"], cc["quality"]) == (99, "good")


# ---------------------------------------------------------------------------------------------------------------
# The reconciler after an import (case 5; rule 3)
# ---------------------------------------------------------------------------------------------------------------


def test_the_reconciler_applies_an_imported_snapshot_by_the_state_table(lab):  # case 5
    lab.event(A, 1, at(-1000), "left", "member")
    lab.event(A, 1, at(30), "member", "left")  # recorded after the page that listed user 1
    lab.event(A, 2, at(100), "left", "member")  # joined after the snapshot
    lab.event(B, 4, at(-1000), "left", "member")
    f = lab.file()
    f.snapshot(A, {1: at(10), 3: {"seen_at": at(11), "joined_date": dt.datetime(2025, 1, 1, tzinfo=UTC)}},
               at(0), finished=at(50), reported=2)
    f.snapshot(B, {5: at(10)}, at(0), finished=at(20), reported=2, complete=False)  # interrupted: partial
    lab.run(f.close())
    lab.reconcile()
    [a] = lab.snapshots(A)
    [b] = lab.snapshots(B)
    assert (a["quality"], b["quality"]) == ("good", "partial")
    assert a["reconciled_at"] is not None and b["reconciled_at"] is not None
    [s1] = lab.spells(A, 1)  # listed at 10, left at 30: stays left
    assert (s1.end_source, s1.left_at) == ("event", at(30))
    [s2] = lab.spells(A, 2)  # joined after the snapshot: its absence closes nothing, doubts nothing
    assert (s2.end_source, s2.suspect_since) == (None, None)
    [s3] = lab.spells(A, 3)
    assert (s3.start_source, s3.known_from, s3.joined_hint) == ("snapshot", at(11), dt.datetime(2025, 1, 1, tzinfo=UTC))
    [s4] = lab.spells(B, 4)  # absent from a partial snapshot: nothing
    assert (s4.end_source, s4.suspect_since) == (None, None)
    [s5] = lab.spells(B, 5)  # a partial snapshot's sighting still counts
    assert (s5.start_source, s5.end_source) == ("snapshot", None)
    assert lab.q("SELECT count(*) FROM tg.probe WHERE chat_id = %s AND reason = 'snapshot_absent'", B) == [(0,)]


def test_both_counts_reach_tg_snapshot_and_the_end_count_changes_the_grade(lab):  # rule 3
    f = lab.file()
    f.snapshot(A, {u: at(1) for u in range(1, 21)}, at(0), reported=20, reported_end=40)
    f.snapshot(B, {u: at(1) for u in range(1, 21)}, at(0), reported=20, reported_end=None, complete=False)
    f.snapshot(C, {u: at(1) for u in range(1, 21)}, at(0), reported=20, reported_end=20)
    lab.run(f.close())
    lab.reconcile()
    got = {s["chat_id"]: (s["reported_at_start"], s["reported_at_end"], s["complete"], s["quality"])
           for s in lab.snapshots()}
    assert got == {A: (20, 40, False, "partial"), B: (20, None, False, "partial"), C: (20, 20, True, "good")}


# ---------------------------------------------------------------------------------------------------------------
# Failures per chat (case 9; rules 6, 7)
# ---------------------------------------------------------------------------------------------------------------


def test_a_database_failure_between_two_chats_keeps_the_first_and_a_re_upload_finishes(lab, monkeypatch):  # case 9
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    f.snapshot(B, {2: at(1)}, at(0))
    path = f.close()
    calls = []
    real = history.lock_chats

    def lock_then_lose_the_connection(conn, chats):
        calls.append(chats)
        if len(calls) == 2:  # the second chat's transaction: the server ends the web connection
            with psycopg.connect(lab.dsn, autocommit=True) as other:
                other.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
        return real(conn, chats)

    monkeypatch.setattr(importer.history, "lock_chats", lock_then_lose_the_connection)
    report = lab.run(path)
    first, second = report.outcomes  # in chat id order: B, then A
    assert (first.status, second.status) == ("imported", "failed")
    assert "database error" in second.reason and "upload the same file again" in second.reason
    assert len(lab.snapshots(first.chat_id)) == 1 and lab.snapshots(second.chat_id) == []
    monkeypatch.setattr(importer.history, "lock_chats", real)
    from test_web import Client
    assert Client(lab.app).get("/").status == 200  # the next request works, on a new connection
    again = lab.run(path)
    assert lab.outcomes(again) == {first.chat_id: ("skipped", None), second.chat_id: ("imported", 1)}
    assert again.outcomes[0].reason.startswith("already imported")


def test_a_held_chat_lock_times_the_chat_out_and_frees_the_connection(lab):  # rule 6
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    path = f.close()
    holder = psycopg.connect(lab.dsn)
    try:
        history.lock_chats(holder, [A])
        started = time.monotonic()
        report = lab.run(path, seconds=1.5)
        took = time.monotonic() - started
        assert lab.outcomes(report) == {A: ("failed", None)} and took < 5, took
        assert "timeout" in report.outcomes[0].reason and "nothing was written" in report.outcomes[0].reason
        assert not lab.database.lock.locked()
        with lab.database.read() as conn:  # the next report gets the connection at once
            assert conn.execute("SELECT count(*) FROM tg.snapshot").fetchone()[0] == 0
    finally:
        holder.rollback()
        holder.close()
    assert lab.outcomes(lab.run(path)) == {A: ("imported", 1)}


def test_a_waiting_fence_counts_against_the_same_deadline(lab):  # rule 6
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    path = f.close()
    takeover = psycopg.connect(lab.dsn)
    try:
        takeover.execute("SELECT serve_generation FROM tg.bot_state FOR UPDATE")  # a takeover in progress
        started = time.monotonic()
        report = lab.run(path, seconds=1.5)
        assert time.monotonic() - started < 5
        assert lab.outcomes(report) == {A: ("failed", None)} and "timeout" in report.outcomes[0].reason
    finally:
        takeover.rollback()
        takeover.close()
    assert lab.snapshots() == []


def test_a_lost_commit_acknowledgement_then_a_migration_then_a_re_upload(lab, monkeypatch):  # rule 7
    f = lab.file()
    u = f.snapshot(A, {1: at(1)}, at(0))
    path = f.close()

    def commit_then_lose_the_reply(conn):
        conn.commit()
        conn.close()
        raise psycopg.OperationalError("server closed the connection unexpectedly")

    monkeypatch.setattr(importer, "_commit", commit_then_lose_the_reply)
    report = lab.run(path)
    assert lab.outcomes(report) == {A: ("unknown", None)}
    assert report.outcomes[0].reason.startswith("unknown") and "already imported" in report.outcomes[0].reason
    monkeypatch.undo()
    new = A - 100
    migrate_chat(lab.dsn, A, new, at(500), new_tracked=True)  # M1: the snapshot is re-keyed to the supergroup
    again = lab.run(path)
    assert lab.outcomes(again) == {A: ("skipped", None)} and again.outcomes[0].reason.startswith("already imported")
    rows = lab.q("SELECT chat_id, source_chat_id, finished_at IS NOT NULL FROM tg.snapshot WHERE collect_uuid = %s", u)
    assert rows == [(new, A, True)] and len(lab.snapshots()) == 1


# ---------------------------------------------------------------------------------------------------------------
# Races and migrations (case 3; rule 2)
# ---------------------------------------------------------------------------------------------------------------


class Gate:
    """Pauses the import transactions named by their lock_chats call order right after they took the chat lock."""

    def __init__(self, monkeypatch, pause_calls: set[int]) -> None:
        self.real = history.lock_chats
        self.calls = 0
        self.pause = pause_calls
        self.locked = threading.Event()
        self.go = threading.Event()
        self.mutex = threading.Lock()
        monkeypatch.setattr(importer.history, "lock_chats", self)

    def __call__(self, conn, chats):
        with self.mutex:
            self.calls += 1
            n = self.calls
        out = self.real(conn, chats)
        if n in self.pause:
            self.locked.set()
            assert self.go.wait(30)
        return out


def _waiting_on_a_lock(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as probe:
        for _ in range(400):
            if probe.execute("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
                             "AND wait_event_type = 'Lock'").fetchone()[0]:
                return
            time.sleep(0.025)
    raise AssertionError("nobody waited on a lock")


def _in_thread(fn, *args) -> tuple[threading.Thread, dict]:
    box: dict = {}

    def target():
        try:
            box["result"] = fn(*args)
        except BaseException as e:  # noqa: BLE001 - reported by the test
            box["error"] = e

    t = threading.Thread(target=target, daemon=True)
    t.start()
    return t, box


def test_racing_identical_uploads_import_once(lab, monkeypatch):  # case 3
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    path = f.close()
    gate = Gate(monkeypatch, {1})
    other = importer.WebDb(lab.connect, fence=lab.ctx.fence)  # a second connection (the page runs one at a time)
    try:
        t1, r1 = _in_thread(lambda: importer.run(lab.database, path, store_names=True))
        assert gate.locked.wait(30)
        t2, r2 = _in_thread(lambda: importer.run(other, path, store_names=True))
        _waiting_on_a_lock(lab.dsn)
        gate.go.set()
        t1.join(30)
        t2.join(30)
    finally:
        other.close()
    assert lab.outcomes(r1["result"]) == {A: ("imported", 1)}
    assert lab.outcomes(r2["result"]) == {A: ("skipped", None)}
    assert r2["result"].outcomes[0].reason.startswith("already imported") and len(lab.snapshots()) == 1


def test_an_older_snapshot_that_resumes_after_a_newer_one_committed_is_skipped(lab, monkeypatch):  # case 3
    older, newer = lab.file(), lab.file()
    older.snapshot(A, {1: at(1)}, at(0))
    u_new = newer.snapshot(A, {1: at(61), 2: at(62)}, at(60))
    old_path, new_path = older.close(), newer.close()
    gate = Gate(monkeypatch, {1})
    other = importer.WebDb(lab.connect, fence=lab.ctx.fence)
    try:
        t_new, r_new = _in_thread(lambda: importer.run(lab.database, new_path, store_names=True))
        assert gate.locked.wait(30)  # the newer import holds the chat
        t_old, r_old = _in_thread(lambda: importer.run(other, old_path, store_names=True))
        _waiting_on_a_lock(lab.dsn)  # the older one is in its transaction, waiting for the chat
        gate.go.set()
        t_new.join(30)
        t_old.join(30)
    finally:
        other.close()
    assert lab.outcomes(r_new["result"]) == {A: ("imported", 2)}
    assert lab.outcomes(r_old["result"]) == {A: ("skipped", None)}
    assert r_old["result"].outcomes[0].reason.startswith("not newer")
    assert [str(s["collect_uuid"]) for s in lab.snapshots(A)] == [u_new]


@pytest.mark.parametrize("new_tracked", [False, True], ids=["M2", "M1"])
def test_a_migration_committed_first_refuses_the_old_id(lab, monkeypatch, new_tracked):  # rule 2
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    path = f.close()
    hold, locked = threading.Event(), threading.Event()
    t_mig, r_mig = _in_thread(lambda: migrate_chat(lab.dsn, A, A - 100, at(30), new_tracked=new_tracked,
                                                   hold=hold, locked=locked))
    assert locked.wait(30)  # the migration holds both chats
    t_imp, r_imp = _in_thread(lambda: lab.run(path))
    _waiting_on_a_lock(lab.dsn)
    hold.set()
    t_mig.join(30)
    t_imp.join(30)
    assert "error" not in r_mig, r_mig
    report = r_imp["result"]
    assert lab.outcomes(report) == {A: ("refused", None)} and report.outcomes[0].reason.startswith("migrated")
    assert lab.snapshots() == []


@pytest.mark.parametrize("new_tracked", [False, True], ids=["M2", "M1"])
@pytest.mark.parametrize("migrated", [5, 30], ids=["before-the-pages", "after-the-pages"])
def test_an_import_committed_before_the_migration_leaves_the_old_id_no_open_spell(lab, monkeypatch, new_tracked,
                                                                                   migrated):  # rule 2
    """The import commits first, then ingest records the migration (dated before or after the listing's pages).
    After reconciliation the old id has no open spell: M1 moves the snapshot into the supergroup; M2 closes the old
    id at the migration, and the M2 cutoff (tgaud.history) keeps its pages after migrated_at from reopening it."""
    lab.event(A, 1, at(-1000), "left", "member")
    f = lab.file()
    f.snapshot(A, {1: at(10), 2: at(11)}, at(0), finished=at(20))
    path = f.close()
    gate = Gate(monkeypatch, {1})
    t_imp, r_imp = _in_thread(lambda: lab.run(path))
    assert gate.locked.wait(30)  # the import holds the chat
    new = A - 100
    t_mig, r_mig = _in_thread(lambda: migrate_chat(lab.dsn, A, new, at(migrated), new_tracked=new_tracked))
    _waiting_on_a_lock(lab.dsn)
    gate.go.set()
    t_imp.join(30)
    t_mig.join(30)
    assert "error" not in r_mig, r_mig
    assert lab.outcomes(r_imp["result"]) == {A: ("imported", 2)}
    lab.reconcile()
    assert lab.q("SELECT count(*) FROM tg.membership WHERE chat_id = %s AND end_source IS NULL", A) == [(0,)]
    [s] = lab.snapshots()
    assert s["quality"] is not None and s["reconciled_at"] is not None
    if new_tracked:
        assert (s["chat_id"], s["source_chat_id"]) == (new, A)
        assert [sp.end_source for sp in lab.spells(new, 2)] == [None]  # the supergroup has the member
    else:
        assert s["chat_id"] == A
        assert [(sp.end_source, sp.end_reason) for sp in lab.spells(A, 1)] == [("migration", "migrated")]
        assert all(sp.end_source == "migration" for sp in lab.spells(A, 2))
    assert lab.q("SELECT count(*) FROM tg.probe WHERE chat_id = %s AND outcome = 'pending'", A) == [(0,)]


# ---------------------------------------------------------------------------------------------------------------
# The page (WSGI)
# ---------------------------------------------------------------------------------------------------------------


def test_the_result_page_lists_every_chat_with_its_outcome_and_links_on(lab, tmp_path):
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    f.snapshot(U, {1: at(1)}, at(0))
    f.legacy(B, [1], at(-10))
    page = lab.upload(f.close())
    assert page.status == 200, page.text
    rows = page.table_with("Outcome")
    assert [(r["Outcome"], r["Reason"].split(":")[0]) for r in rows] == [
        ("skipped", "not tracked"), ("refused", "old format"), ("imported", "imported")]
    assert 'href="/reports/coverage"' in page.text and 'href="/"' in page.text and "within minutes" in page.text
    assert list(lab.spool.iterdir()) == []


def test_the_form_says_what_to_upload_the_limit_and_what_happens_next(lab):
    from test_web import Client
    page = Client(lab.app).get("/import")
    assert page.status == 200
    for want in ('enctype="multipart/form-data"', 'name="file"', "32 MiB", "latest snapshot", "within minutes",
                 "<script" not in page.text and "tgstats collect"):
        assert want is True or want in page.text
    assert '<a href="/import" aria-current="page">Import</a>' in page.text
    assert "default-src 'none'" in page.headers["Content-Security-Policy"] and page.headers["Cache-Control"] == "no-store"


def test_the_file_layout_is_the_one_tgstats_writes():
    """tgaud keeps its own copy of the tgstats layout (it never imports tgstats at run time)."""
    assert tuple(tgdb.SNAPSHOT_COLUMNS) == tgstats_file.SNAPSHOT_COLUMNS
    assert tuple(tgdb.MEMBERSHIP_COLUMNS) == tgstats_file.MEMBERSHIP_COLUMNS
    assert tgstats_file.METHODS == tgdb.METHODS
    raw = sqlite3.connect(":memory:")
    raw.executescript(tgdb.SCHEMA)
    for table, cols in tgstats_file.BASE.items():
        assert tuple(r[1] for r in raw.execute(f"PRAGMA table_info({table})")) == cols
