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
from tgaud.jobs import reconciler, sampler
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
    ("UPDATE snapshots SET fetched = -1", "negative"),  # review 1: the file's own counts
    ("UPDATE snapshots SET reported = -1", "negative"),
    ("UPDATE snapshots SET fetched = fetched + 1", "member rows"),
    ("DELETE FROM memberships", "member rows"),
    ("UPDATE snapshots SET started_at = '0001-01-01T00:30:00+01:00'", "out of range"),  # review 1: overflow
    ("UPDATE memberships SET seen_at = '0001-01-01T00:30:00+05:00'", "out of range"),
    ("UPDATE snapshots SET reported_end = NULL, complete = 1", "complete but has no member count"),  # review 2
], ids=["uuid", "uuid-braces", "uuid-nil", "count-start", "count-end", "naive-time", "order", "garbled-time",
        "method", "user-id", "joined-date", "fetched-negative", "reported-negative", "fetched-vs-rows",
        "missing-rows", "start-overflow", "seen-overflow", "complete-without-end-count"])
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
    elif kind == "generated-column":  # review 1: table_info would not show it
        raw.execute("ALTER TABLE chats ADD COLUMN label TEXT GENERATED ALWAYS AS (title) VIRTUAL")
    elif kind in ("null-is-bot", "null-is-deleted"):  # review 1: NULL would read as false
        raw.execute(f"UPDATE memberships SET {kind[5:].replace('-', '_')} = NULL WHERE snapshot_id = "
                    f"(SELECT id FROM snapshots WHERE chat_id = {B})")
    elif kind == "same-uuid":
        raw.execute("DROP INDEX snapshots_by_uuid")
        raw.execute(f"UPDATE snapshots SET uuid = (SELECT uuid FROM snapshots WHERE chat_id = {A}) WHERE chat_id = {B}")
    raw.commit()


@pytest.mark.parametrize("kind", ["text-user-id", "float-count", "blob-name", "extra-column", "extra-table", "view",
                                  "trigger", "missing-table", "same-uuid", "generated-column", "null-is-bot",
                                  "null-is-deleted"])
def test_a_structurally_broken_file_imports_nothing(lab, kind):  # rule 4, rule 5
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    f.snapshot(B, {2: at(1)}, at(0))
    _structural(f, kind)
    with pytest.raises(tgstats_file.FileError):
        lab.run(f.close())
    assert lab.snapshots() == []


@pytest.mark.parametrize("first, second", [("seen", "blob"), ("blob", "seen")], ids=["value-first", "blob-first"])
def test_a_shape_error_after_a_value_error_in_the_same_snapshot_still_refuses_the_whole_file(lab, first, second):
    """review 1: a value error must not end the structural check of the snapshot's other rows (both row orders)."""
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    f.snapshot(B, {2: at(1), 3: at(2)}, at(0), finished=at(10))
    sid = f.raw.execute(f"SELECT id FROM snapshots WHERE chat_id = {B}").fetchone()[0]
    for user, kind in ((2, first), (3, second)):
        f.raw.execute("UPDATE memberships SET seen_at = '2026-11-20T08:00:00+00:00' WHERE snapshot_id = ? AND user_id = ?"
                      if kind == "seen" else
                      "UPDATE memberships SET first_name = x'00ff' WHERE snapshot_id = ? AND user_id = ?", (sid, user))
    f.raw.commit()
    with pytest.raises(tgstats_file.FileError, match="first_name"):
        lab.run(f.close())
    assert lab.snapshots() == []


@pytest.mark.parametrize("why", ["future", "too-large", "old-reading"])
def test_a_refused_chats_rows_are_still_checked_for_shape(lab, why, monkeypatch):  # review 1
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    t = now()
    if why == "future":
        f.snapshot(B, {2: t}, t - dt.timedelta(minutes=1), finished=t + dt.timedelta(days=1))
    else:
        f.snapshot(B, {2: at(1), 3: at(2)}, at(0))
    if why == "too-large":
        monkeypatch.setattr(importer, "MAX_MEMBERS", 1)
    if why == "old-reading":  # an unreadable start time refuses the chat before its rows are read for values
        f.raw.execute(f"UPDATE snapshots SET started_at = 'garbled' WHERE chat_id = {B}")
    f.raw.execute(f"UPDATE memberships SET username = x'01' WHERE snapshot_id = (SELECT id FROM snapshots "
                  f"WHERE chat_id = {B})")
    f.raw.commit()
    with pytest.raises(tgstats_file.FileError, match="username"):
        lab.run(f.close())
    assert lab.snapshots() == []


@pytest.mark.parametrize("where", ["chat-rows", "snapshot-chats"])
def test_the_chat_limit_holds_while_the_file_is_read(lab, monkeypatch, where):  # review 1, rule 5
    monkeypatch.setattr(tgstats_file, "MAX_CHATS", 3)
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    f.snapshot(B, {2: at(1)}, at(0))
    if where == "chat-rows":  # chats without a snapshot still count
        for n in range(2):
            f.raw.execute("INSERT INTO chats (id, type, title) VALUES (?, 'channel', 'spare')", (-1008000000000 - n,))
        f.raw.commit()
    else:
        f.snapshot(C, {3: at(1)}, at(0))
        f.snapshot(U, {4: at(1)}, at(0))
    with pytest.raises(tgstats_file.FileError, match="more than 3"):
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


def test_consecutive_lock_waits_share_one_budget(lab):  # rule 6, review 1
    """The chat lock is held for 1.2 s, then the chat's row is held FOR UPDATE (the snapshot INSERT's foreign key
    waits on it): with a 2 s budget the second wait gets only what the first left, so the chat fails near 2 s."""
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    path = f.close()
    chat_lock, row_lock = psycopg.connect(lab.dsn), psycopg.connect(lab.dsn)
    try:
        history.lock_chats(chat_lock, [A])
        row_lock.execute("SELECT 1 FROM tg.chat WHERE chat_id = %s FOR UPDATE", (A,))
        release = threading.Timer(1.2, chat_lock.rollback)
        release.start()
        started = time.monotonic()
        report = lab.run(path, seconds=2.0)
        took = time.monotonic() - started
        release.join()
        assert lab.outcomes(report) == {A: ("failed", None)} and "timeout" in report.outcomes[0].reason
        assert 1.2 < took < 2.8, took
        assert lab.snapshots() == [] and not lab.database.lock.locked()
    finally:
        for c in (chat_lock, row_lock):
            c.rollback()
            c.close()
    assert lab.outcomes(lab.run(path)) == {A: ("imported", 1)}


def test_a_busy_connection_fails_only_that_chat_and_keeps_the_others(lab, monkeypatch):  # review 1
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    f.snapshot(B, {2: at(1)}, at(0))
    path = f.close()
    real, calls = importer._write, []
    monkeypatch.setattr(lab.database, "_lock_wait", 0.3)

    def second_finds_the_connection_busy(database, *args, **kw):
        calls.append(1)
        if len(calls) != 2:
            return real(database, *args, **kw)
        database.lock.acquire()  # another request holds the connection past the lock wait
        try:
            return real(database, *args, **kw)
        finally:
            database.lock.release()

    monkeypatch.setattr(importer, "_write", second_finds_the_connection_busy)
    page = lab.upload(path)
    assert page.status == 200, page.text
    rows = {r["Outcome"]: r for r in page.table_with("Outcome")}
    assert set(rows) == {"imported", "failed"}
    assert "busy" in rows["failed"]["Reason"] and "upload the same file again" in rows["failed"]["Reason"]
    assert len(lab.snapshots()) == 1
    monkeypatch.setattr(importer, "_write", real)
    again = lab.run(path)
    assert sorted(o.status for o in again.outcomes) == ["imported", "skipped"]


def test_a_failed_chat_lookup_for_the_result_page_costs_no_chat_its_import(lab, monkeypatch):  # review 1
    import contextlib
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    f.legacy(C, [1, 2], at(-60))
    path = f.close()
    real_read = lab.database.read

    @contextlib.contextmanager
    def failing_read():
        raise psycopg.OperationalError("the connection was lost")
        yield  # pragma: no cover

    monkeypatch.setattr(lab.database, "read", failing_read)
    report = lab.run(path)
    monkeypatch.setattr(lab.database, "read", real_read)
    assert lab.outcomes(report) == {A: ("imported", 1), C: ("refused", None)}
    assert next(o for o in report.outcomes if o.chat_id == C).reason.startswith("old format")


def test_a_takeover_still_stops_serve_and_writes_nothing(lab):  # review 1: the fence is not caught per chat
    f = lab.file()
    f.snapshot(A, {1: at(1)}, at(0))
    path = f.close()
    stopped = []
    lab.app.fatal = lambda code, reason: stopped.append(code)
    with psycopg.connect(lab.dsn) as c:
        c.execute("UPDATE tg.bot_state SET serve_generation = serve_generation + 1")
    page = lab.upload(path)
    assert page.status == 503 and "took over" in page.text and stopped == [1]
    assert lab.snapshots() == [] and list(lab.spool.iterdir()) == []


def test_a_lost_commit_acknowledgement_then_a_migration_then_a_re_upload(lab, monkeypatch):  # rule 7
    f = lab.file()
    u = f.snapshot(A, {1: at(1)}, at(0))
    path = f.close()

    def commit_then_lose_the_reply(conn):
        conn.commit()
        conn.close()
        raise psycopg.OperationalError("server closed the connection unexpectedly")

    monkeypatch.setattr(importer, "_commit", commit_then_lose_the_reply)
    page = lab.upload(path)  # the result page of a commit that happened but was never acknowledged
    assert page.status == 200, page.text
    [row] = page.table_with("Outcome")
    assert row["Outcome"] == "unknown" and "already imported" in row["Reason"]
    assert "Nothing was written" not in page.text and "Whether anything was written is not known" in page.text
    assert [str(x["collect_uuid"]) for x in lab.snapshots(A)] == [u]  # it was written
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


# ---------------------------------------------------------------------------------------------------------------
# A later version of an imported collection (reimport task): a partial version (about 10 % of the members, the
# file of a collection still running) and the finished one (same uuid and started_at, later finished_at, the
# partial's members with the same observations plus the rest). The finished one grades partial on its own, as the
# owner's 2004 of 2135 did. It is written as a delta: only the members no stored version listed.
# ---------------------------------------------------------------------------------------------------------------

ALL = list(range(1, 61))  # the finished version's members
SHARED = list(range(1, 7))  # the partial version's
MISSING = [u for u in ALL if u not in SHARED]
REPORTED = 90  # Telegram's count: 60 of 90 listed grades partial
LATER_REASON = "imported: a later version of the collection in snapshot"


def seen(u: int) -> dt.datetime:
    return at(u / 4)  # every version saw user u on the same page


def version(lab, users, finished: dt.datetime, *, uuid: str | None = None, chat: int = A,
            started: dt.datetime | None = None):
    """(path, uuid) of a file with one version of a collection of `chat`."""
    f = lab.file()
    u = f.snapshot(chat, {x: {"seen_at": seen(x), "joined_date": at(-5000 - x)} for x in users}, started or at(0),
                   finished=finished, reported=REPORTED, uuid=uuid)
    return f.close(), u


def partial_then(lab, *, chat: int = A):
    """The partial version imported: (its snapshot_id, its file, the finished version's file)."""
    path, u = version(lab, SHARED, at(2), chat=chat)
    report = lab.run(path)
    assert lab.outcomes(report) == {chat: ("imported", len(SHARED))}
    full, _ = version(lab, ALL, at(20), uuid=u, chat=chat)
    return report.outcomes[0].snapshot_id, path, full


@pytest.fixture
def full_only(pg, tmp_path):
    """A fresh database that imports only the finished version."""
    (tmp_path / "full-only").mkdir()
    lab = Lab(pg, pg.make_db(), tmp_path / "full-only")
    yield lab
    lab.close()


def import_full_only(full_only, users=ALL, finished: dt.datetime | None = None) -> None:
    path, _ = version(full_only, users, finished or at(20))
    assert full_only.outcomes(full_only.run(path)) == {A: ("imported", len(users))}
    full_only.reconcile()


def history_of(lab, chat: int = A) -> dict:
    """What the replay and the reconciler made of the chat, with nothing that names a snapshot id."""
    from history_world import COLS
    return {
        "spells": lab.q(f"SELECT {COLS} FROM tg.membership WHERE chat_id = %s ORDER BY user_id, known_from", chat),
        "cases": lab.q("SELECT user_id, rule, at FROM tg.contradiction_case WHERE chat_id = %s ORDER BY 1, 2, 3", chat),
        "gaps": lab.q("SELECT chat_id, period, reason FROM tg.gap ORDER BY 1, 2, 3"),
        "probes": lab.q("SELECT chat_id, user_id, reason, outcome FROM tg.probe ORDER BY 1, 2, 3, 4"),
        "users": lab.q("SELECT user_id, is_bot, is_deleted, username, first_name, first_seen_at, last_seen_at "
                       "FROM tg.tg_user ORDER BY user_id"),
        # what reports and /metrics read of a chat's snapshots: only good ones (a delta never is) and open spells
        "status": lab.q("SELECT * FROM stats.v_chat_status WHERE chat_id = %s", chat),
        "last good": lab.q(sampler._Q_SNAPSHOT.replace("%(chats)s", "ARRAY[%s]"), chat),
        "known open": lab.q("SELECT count(*) FROM tg.membership WHERE chat_id = %s AND end_source IS NULL", chat),
    }


def one_run_each(lab, users, chat: int = A) -> None:
    runs = lab.q("SELECT user_id, count(*), sum(times_seen) FROM tg.sighting WHERE chat_id = %s "
                 "GROUP BY user_id ORDER BY user_id", chat)
    assert runs == [(u, 1, 1) for u in users]
    assert lab.q("SELECT count(*) FROM tg.tg_user WHERE user_id = ANY(%s)", list(users)) == [(len(users),)]


def grade_and_apply(lab, snapshot_id: int, size: int = 10_000) -> None:
    """The reconciler's grade, then one apply chunk of `size` users, each committed (no prune)."""
    with psycopg.connect(lab.dsn) as c:
        reconciler.grade(c, snapshot_id)
    with psycopg.connect(lab.dsn) as c:
        reconciler.apply_chunk(c, snapshot_id, A, size)


def bring(lab, snapshot_id: int, state: str, monkeypatch) -> None:
    """The stored version in one reconcile state."""
    if state == "unapplied":
        pass
    elif state == "half applied":
        grade_and_apply(lab, snapshot_id, 3)
        [s] = [x for x in lab.snapshots(A) if x["snapshot_id"] == snapshot_id]
        assert s["applied_through_user_id"] == SHARED[2] and s["reconciled_at"] is None
    elif state == "applied":
        grade_and_apply(lab, snapshot_id)
    elif state == "mid-prune":
        grade_and_apply(lab, snapshot_id)
        monkeypatch.setattr(reconciler, "DELETE_BATCH", 2)
        with psycopg.connect(lab.dsn) as c:
            assert not reconciler.prune_batch(c, snapshot_id)
        monkeypatch.setattr(reconciler, "DELETE_BATCH", 20_000)
        [s] = [x for x in lab.snapshots(A) if x["snapshot_id"] == snapshot_id]
        assert s["prune_started_at"] is not None and s["members_pruned_at"] is None
        assert sorted(lab.members(snapshot_id)) == SHARED[2:]
    else:
        assert state == "pruned"
        lab.reconcile()
        [s] = [x for x in lab.snapshots(A) if x["snapshot_id"] == snapshot_id]
        assert s["members_pruned_at"] is not None and lab.members(snapshot_id) == {}


def delta_of(lab, report, first: int) -> dict:
    """The delta row of a later version's import, checked against its result row."""
    [o] = report.outcomes
    assert (o.status, o.written) == ("imported", len(MISSING)), o.reason
    assert o.reason == f"{LATER_REASON} {first}: {len(MISSING)} members it lacked written ({len(ALL)} in the file)"
    [d] = [s for s in lab.snapshots(A) if s["snapshot_id"] == o.snapshot_id]
    return d


@pytest.mark.parametrize("state", ["unapplied", "half applied", "applied", "mid-prune", "pruned"])
def test_a_later_version_adds_only_the_members_it_lacked(lab, full_only, monkeypatch, state):
    first, _, full = partial_then(lab)
    bring(lab, first, state, monkeypatch)
    report = lab.run(full)
    d = delta_of(lab, report, first)
    assert sorted(lab.members(d["snapshot_id"])) == MISSING
    assert (d["complete"], d["fetched"], d["collect_uuid"], d["started_at"], d["finished_at"]) == (
        False, len(MISSING), None, at(0), at(20))
    assert d["version_of"] == lab.snapshots(A)[0]["collect_uuid"]
    assert (d["reported_at_start"], d["reported_at_end"]) == (REPORTED, REPORTED)
    assert report.later_versions == 1
    lab.reconcile()
    assert [s["quality"] for s in lab.snapshots(A)] == ["partial", "partial"]
    one_run_each(lab, ALL)
    import_full_only(full_only)
    assert history_of(lab) == history_of(full_only)


def test_a_third_version_after_the_second_was_pruned_adds_only_members_neither_listed(lab, full_only):
    first, _, full = partial_then(lab)
    lab.reconcile()
    second = lab.run(full).outcomes[0].snapshot_id
    lab.reconcile()
    assert lab.members(second) == {}  # pruned: only its sightings say whom it listed
    uuid = str(lab.snapshots(A)[0]["collect_uuid"])
    third, _ = version(lab, range(1, 71), at(30), uuid=uuid)
    report = lab.run(third)
    [o] = report.outcomes
    assert (o.status, o.written) == ("imported", 10)
    assert o.reason == f"{LATER_REASON} {first}: 10 members it lacked written (70 in the file)"
    assert sorted(lab.members(o.snapshot_id)) == list(range(61, 71))
    lab.reconcile()
    one_run_each(lab, range(1, 71))
    import_full_only(full_only, range(1, 71), at(30))
    assert history_of(lab) == history_of(full_only)


def test_a_prune_batch_committed_after_the_version_lookup_hides_no_one(lab, monkeypatch):
    first, _, full = partial_then(lab)
    grade_and_apply(lab, first)
    real = importer._versions

    def lookup_then_prune(q, uuid):
        found = real(q, uuid)
        with psycopg.connect(lab.dsn) as c:  # commits before the importer reads the listed members
            assert reconciler.prune_batch(c, first)
        return found

    monkeypatch.setattr(importer, "_versions", lookup_then_prune)
    report = lab.run(full)
    d = delta_of(lab, report, first)
    assert lab.members(first) == {} and sorted(lab.members(d["snapshot_id"])) == MISSING
    monkeypatch.undo()
    lab.reconcile()
    one_run_each(lab, ALL)


def test_replaying_the_shared_users_twice_would_have_raised_a_contradiction_gap(lab, full_only):
    """Each shared user left 5 minutes before the page that lists them: a stale listing (S3) per listing. Six cases
    stay under the threshold (more than 10 within 60 minutes); listing them again in the later version would have
    made twelve. An unrelated open spell would then have been doubted by the gap."""
    for w in (lab, full_only):
        w.event(A, 900, at(-100), "left", "member")
        for u in SHARED:
            w.event(A, u, seen(u) - dt.timedelta(minutes=5), "member", "left")
    first, _, full = partial_then(lab)
    lab.reconcile()
    delta_of(lab, lab.run(full), first)
    lab.reconcile()
    import_full_only(full_only)
    mine = history_of(lab)
    assert lab.q("SELECT count(*) FROM tg.contradiction_case WHERE chat_id = %s AND rule = 'S3'", A) == [(6,)]
    assert mine["gaps"] == [] and not any(p[2] == "outage" for p in mine["probes"])
    assert lab.spells(A, 900) == full_only.spells(A, 900) and lab.spells(A, 900)[0].suspect_since is None
    assert mine == history_of(full_only)
    one_run_each(lab, ALL)


def test_a_join_event_between_the_imports_keeps_one_run(lab, full_only):
    first, _, full = partial_then(lab)
    lab.reconcile()
    lab.event(A, 2, at(30), "left", "member")  # a shared user joins again: the listed stay ended before (E4)
    delta_of(lab, lab.run(full), first)
    lab.reconcile()
    import_full_only(full_only)
    full_only.event(A, 2, at(30), "left", "member")
    one_run_each(lab, ALL)
    spells = lab.spells(A, 2)
    assert [(s.start_source, s.end_source) for s in spells] == [("snapshot", "inferred"), ("event", None)]
    assert spells == full_only.spells(A, 2) and history_of(lab) == history_of(full_only)


def test_versions_that_are_not_later_or_add_nobody_are_skipped(lab):
    first, partial, full = partial_then(lab)
    again = lab.run(partial)
    assert again.outcomes[0].reason == f"already imported (snapshot {first})"
    same_members, _ = version(lab, SHARED, at(10), uuid=str(lab.snapshots(A)[0]["collect_uuid"]))
    nobody = lab.run(same_members)
    assert lab.outcomes(nobody) == {A: ("skipped", None)}
    assert nobody.outcomes[0].reason == f"already imported (snapshot {first}): the file adds no member"
    assert len(lab.snapshots(A)) == 1
    delta = lab.run(full).outcomes[0].snapshot_id
    twice = lab.run(full)
    assert lab.outcomes(twice) == {A: ("skipped", None)}
    assert twice.outcomes[0].reason == f"already imported (snapshot {delta})"
    older = lab.run(same_members)  # finished before the newest version
    assert older.outcomes[0].reason == f"older version: tgaud has a later version of this collection (snapshot {delta})"
    assert lab.run(partial).outcomes[0].reason == f"already imported (snapshot {first})"
    assert len(lab.snapshots(A)) == 2 and lab.q("SELECT count(*) FROM tg.snapshot_member") == [(len(ALL),)]


def test_the_finished_version_first_then_the_partial_one_is_older(lab):
    full, u = version(lab, ALL, at(20))
    sid = lab.run(full).outcomes[0].snapshot_id
    partial, _ = version(lab, SHARED, at(2), uuid=u)
    report = lab.run(partial)
    assert lab.outcomes(report) == {A: ("skipped", None)}
    assert report.outcomes[0].reason == f"older version: tgaud has a later version of this collection (snapshot {sid})"
    assert lab.run(full).outcomes[0].reason == f"already imported (snapshot {sid})"
    assert len(lab.snapshots()) == 1


def test_a_later_version_drops_erased_members_as_a_first_import_does(lab):
    _, _, full = partial_then(lab)
    _forget(lab, 40)
    [o] = lab.run(full).outcomes
    assert (o.status, o.written) == ("imported", len(MISSING) - 1)
    assert sorted(lab.members(o.snapshot_id)) == [u for u in MISSING if u != 40]


def test_a_later_version_behind_a_later_snapshot_of_the_chat_is_not_newer(lab):
    _, _, full = partial_then(lab)
    f = lab.file()
    f.snapshot(A, {1: at(31), 70: at(32)}, at(30))  # another collection, started later
    assert lab.outcomes(lab.run(f.close())) == {A: ("imported", 2)}
    report = lab.run(full)
    assert lab.outcomes(report) == {A: ("skipped", None)} and report.outcomes[0].reason.startswith("not newer")
    assert len(lab.snapshots(A)) == 2 and lab.q("SELECT count(*) FROM tg.snapshot_member") == [(8,)]


def test_a_different_start_with_the_same_uuid_is_already_imported(lab):
    first, _, _ = partial_then(lab)
    moved, _ = version(lab, ALL, at(20), uuid=str(lab.snapshots(A)[0]["collect_uuid"]), started=at(-1))
    report = lab.run(moved)
    assert report.outcomes[0].reason == f"already imported (snapshot {first})" and len(lab.snapshots()) == 1


def test_after_m1_the_collection_stays_claimed_by_its_chat_and_source(lab):
    first, partial, _ = partial_then(lab)
    lab.reconcile()
    new = A - 100
    migrate_chat(lab.dsn, A, new, at(500), new_tracked=True)
    uuid = str(lab.snapshots(new)[0]["collect_uuid"])
    for chat in (new, A):  # the new id's source differs; the old id is no longer the claim's chat
        path, _ = version(lab, ALL, at(20), uuid=uuid, chat=chat)
        report = lab.run(path)
        assert report.outcomes[0].reason == f"already imported (snapshot {first})", report.outcomes[0].reason
    retry = lab.run(partial)
    assert lab.outcomes(retry) == {A: ("skipped", None)}
    assert retry.outcomes[0].reason == f"already imported (snapshot {first})"
    assert len(lab.snapshots()) == 1 and lab.q("SELECT count(*) FROM tg.snapshot_member") == [(0,)]


def test_racing_uploads_of_the_finished_version_write_one_delta(lab, monkeypatch):
    _, _, full = partial_then(lab)
    gate = Gate(monkeypatch, {1})
    other = importer.WebDb(lab.connect, fence=lab.ctx.fence)
    try:
        t1, r1 = _in_thread(lambda: importer.run(lab.database, full, store_names=True))
        assert gate.locked.wait(30)
        t2, r2 = _in_thread(lambda: importer.run(other, full, store_names=True))
        _waiting_on_a_lock(lab.dsn)
        gate.go.set()
        t1.join(30)
        t2.join(30)
    finally:
        other.close()
    assert lab.outcomes(r1["result"]) == {A: ("imported", len(MISSING))}
    delta = r1["result"].outcomes[0].snapshot_id
    assert r2["result"].outcomes[0].reason == f"already imported (snapshot {delta})"
    assert len(lab.snapshots(A)) == 2


def test_racing_first_imports_of_one_uuid_for_two_chats_keep_the_first(lab, monkeypatch):
    path_a, u = version(lab, SHARED, at(2), chat=A)
    path_b, _ = version(lab, ALL, at(20), chat=B, uuid=u)  # another version, another chat
    real = importer._versions
    paused, go = threading.Event(), threading.Event()
    calls = []

    def pause_the_first(q, uuid):
        found = real(q, uuid)
        calls.append(uuid)
        if len(calls) == 1:
            paused.set()
            assert go.wait(30)
        return found

    monkeypatch.setattr(importer, "_versions", pause_the_first)
    other = importer.WebDb(lab.connect, fence=lab.ctx.fence)
    try:
        ta, ra = _in_thread(lambda: importer.run(lab.database, path_a, store_names=True))
        assert paused.wait(30)  # A holds the collection after its lookup found nothing
        tb, rb = _in_thread(lambda: importer.run(other, path_b, store_names=True))
        _waiting_on_a_lock(lab.dsn)  # B waits for the collection's lock, not for A's chat
        go.set()
        ta.join(30)
        tb.join(30)
    finally:
        other.close()
    assert lab.outcomes(ra["result"]) == {A: ("imported", len(SHARED))}
    first = ra["result"].outcomes[0].snapshot_id
    assert lab.outcomes(rb["result"]) == {B: ("skipped", None)}
    assert rb["result"].outcomes[0].reason == f"already imported (snapshot {first})"
    assert lab.snapshots(B) == [] and [s["snapshot_id"] for s in lab.snapshots()] == [first]
    assert lab.q("SELECT count(*) FROM tg.snapshot_member") == [(len(SHARED),)]


def test_a_lost_commit_acknowledgement_of_a_later_version_then_a_re_upload(lab, monkeypatch):
    _, _, full = partial_then(lab)

    def commit_then_lose_the_reply(conn):
        conn.commit()
        conn.close()
        raise psycopg.OperationalError("server closed the connection unexpectedly")

    monkeypatch.setattr(importer, "_commit", commit_then_lose_the_reply)
    assert lab.outcomes(lab.run(full)) == {A: ("unknown", None)}
    monkeypatch.undo()
    [_, d] = lab.snapshots(A)
    again = lab.run(full)
    assert lab.outcomes(again) == {A: ("skipped", None)}
    assert again.outcomes[0].reason == f"already imported (snapshot {d['snapshot_id']})"
    assert len(lab.snapshots(A)) == 2


# the first import exactly as the released image wrote it (importer.py at 91e03f7): its lookup, INSERT and finish
RELEASED_LOOKUP = "SELECT snapshot_id FROM tg.snapshot WHERE collect_uuid = %s"
RELEASED_INSERT = """INSERT INTO tg.snapshot (chat_id, started_at, method, reported_at_start, collect_uuid)
                   VALUES (%s, %s, %s, %s, %s) RETURNING snapshot_id"""
RELEASED_FINISH = """UPDATE tg.snapshot SET finished_at = %s, reported_at_end = %s, fetched = %s, complete = %s
             WHERE snapshot_id = %s"""


def released_import(dsn: str, path) -> tuple[str, int]:
    """One chat's import through the released image's statements: ("imported" | "skipped", snapshot_id)."""
    with tgstats_file.Reader(path) as reader:
        [chat] = tgstats_file.check(reader, now=now(), ceiling=importer.MAX_MEMBERS).chats
        s = chat.snapshot
        with psycopg.connect(dsn) as conn:
            history.lock_chats(conn, [chat.chat_id])
            dup = conn.execute(RELEASED_LOOKUP, (chat.uuid,)).fetchone()
            if dup is not None:
                return "skipped", dup[0]
            sid = conn.execute(RELEASED_INSERT, (chat.chat_id, chat.started_at, s.method, s.reported_start,
                                                 chat.uuid)).fetchone()[0]
            with conn.cursor().copy(importer.COPY_MEMBERS) as copy:
                for m in tgstats_file.members(reader, chat):
                    copy.write_row((sid, m.user_id, m.seen_at, m.joined_date, m.inviter_id, m.via_request,
                                    m.subscription_until, m.is_bot, m.is_deleted, m.is_premium, m.username,
                                    m.first_name, m.last_name))
            written = conn.execute("SELECT count(*) FROM tg.snapshot_member WHERE snapshot_id = %s",
                                   (sid,)).fetchone()[0]
            conn.execute(RELEASED_FINISH, (chat.finished_at, s.reported_end, written, s.complete, sid))
    return "imported", sid


def test_the_owners_path_across_the_upgrade_to_012(pg, tmp_path):
    from tgaud import db
    dsn = pg.make_db(migrated=False)
    with psycopg.connect(dsn) as c:
        db.migrate(c, [m for m in db.load_migrations() if m.version <= 11])
    lab = Lab(pg, dsn, tmp_path)
    try:
        # before the upgrade: the partial version through the released image, reconciled and pruned
        partial, u = version(lab, SHARED, at(2))
        status, first = released_import(dsn, partial)
        assert status == "imported"
        lab.reconcile()
        assert lab.snapshots(A)[0]["members_pruned_at"] is not None
        with psycopg.connect(dsn) as c:
            assert db.migrate(c).applied
        retry = lab.run(partial)
        assert retry.outcomes[0].reason == f"already imported (snapshot {first})"
        full, _ = version(lab, ALL, at(20), uuid=u)
        report = lab.run(full)
        delta_of(lab, report, first)
        assert released_import(dsn, full) == ("skipped", first)  # a rolled-back image: already imported
        # after the upgrade, the released image's first import still works and is recognised
        partial_b, ub = version(lab, SHARED, at(2), chat=B)
        status, first_b = released_import(dsn, partial_b)
        assert status == "imported"
        assert lab.run(partial_b).outcomes[0].reason == f"already imported (snapshot {first_b})"
        full_b, _ = version(lab, ALL, at(20), uuid=ub, chat=B)
        [o] = lab.run(full_b).outcomes
        assert (o.status, o.written) == ("imported", len(MISSING))
        assert sorted(lab.members(o.snapshot_id)) == MISSING
        lab.reconcile()
        one_run_each(lab, ALL)
        one_run_each(lab, ALL, chat=B)
    finally:
        lab.close()


def test_a_later_version_through_the_real_server(pg, migrated_dsn, tmp_path):
    import dataclasses

    from test_import_listener import send

    from tgaud import config
    from tgaud import web as web_pkg
    lab = Lab(pg, migrated_dsn, tmp_path)
    cfg = dataclasses.replace(lab.cfg, web_listen=config.Listen("127.0.0.1", 0))
    server = web_pkg.WebServer(cfg, cfg.web_proxy_token, lab.connect, fence=lab.ctx.fence)
    server.app.spool_dir = str(lab.spool)
    server.start()
    lab.port = server.port
    try:
        partial, u = version(lab, SHARED, at(2))
        status, text = send(lab, partial.read_bytes())
        assert status == 200 and f"imported: {len(SHARED)} members written" in text
        lab.reconcile()
        full, _ = version(lab, ALL, at(20), uuid=u)
        status, text = send(lab, full.read_bytes())
        first = lab.snapshots(A)[0]["snapshot_id"]
        assert status == 200
        assert f"{LATER_REASON} {first}: {len(MISSING)} members it lacked written ({len(ALL)} in the file)" in text
        assert "A later version adds only the members tgaud lacked" in text
        lab.reconcile()
        assert {u: [(s.start_source, s.end_source, s.known_from) for s in lab.spells(A, u)] for u in ALL} == {
            u: [("snapshot", None, seen(u))] for u in ALL}
        one_run_each(lab, ALL)
    finally:
        server.close()
        lab.close()
