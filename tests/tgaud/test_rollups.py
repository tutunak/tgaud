"""The batch rollups: R1 (chat_day), R2 (audience) and R4 (partitions) on the history fixture and on small cases.

The fixture is docs/design/verify.md 2; the expected numbers are its section 4. "Now" is 2026-11-23 12:00 UTC.
Tests that must commit (two sessions, psql) work on a clone of the fixture database.
"""

import datetime as dt
import json
import pathlib
import threading
import time

import psycopg
import psycopg.conninfo
import psycopg.sql
import pytest
from psycopg.rows import dict_row

from tgaud import config, db, metrics, rollups, sql
from tgaud.jobs import JobContext, Scheduler, audience, chat_day, partitions

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 23, 12, tzinfo=UTC)
A, B, C, D = -1001000000001, -1001000000002, -1001000000003, -1001000000004
H11 = -1002000000011  # case 11: open spell, suspect since 2026-09-20 00:00 exactly
AD = [A, B, C, D]


def ts(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s).replace(tzinfo=UTC)


def q(c, query: str, params=None) -> list[dict]:
    """Rows as dicts (the connections of the code under test keep psycopg's tuple rows)."""
    with c.cursor(row_factory=dict_row) as cur:
        return cur.execute(query, params).fetchall()


def q1(c, query: str, params=None) -> dict:
    (row,) = q(c, query, params)
    return row


def tracked(c) -> list[int]:
    return [r[0] for r in c.execute("SELECT chat_id FROM tg.chat WHERE tracked ORDER BY chat_id")]


def chat_day_rows(c, where: str = "true") -> list[dict]:
    """Every stats.chat_day row without refreshed_at, as dicts."""
    return [r[0] for r in c.execute(
        f"SELECT to_jsonb(d) - 'refreshed_at' FROM stats.chat_day d WHERE {where} ORDER BY chat_id, day")]


@pytest.fixture
def fx(history_fixture):
    """The fixture database in a transaction that is rolled back."""
    with psycopg.connect(history_fixture) as c:
        yield c
        c.rollback()


@pytest.fixture
def clone(pg, history_fixture):
    """A function that makes a fresh copy of the fixture database (template copy) and returns its tenant DSN."""
    def make() -> str:
        name = f"{pg.tenant}_c{len(pg.databases) + 1}"
        src = psycopg.conninfo.conninfo_to_dict(history_fixture)["dbname"]
        with pg.admin() as a:
            a.execute(psycopg.sql.SQL("CREATE DATABASE {} TEMPLATE {} OWNER {}").format(
                psycopg.sql.Identifier(name), psycopg.sql.Identifier(src), psycopg.sql.Identifier(pg.tenant)))
            pg.databases.append(name)
            a.execute(psycopg.sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(psycopg.sql.Identifier(name)))
        return pg.tenant_dsn(name)
    return make


class HookConnection(psycopg.Connection):
    """Calls `after(query)` once a statement has run: the test commits another session's work at that point."""

    after = None

    def execute(self, query, params=None, **kw):
        cur = super().execute(query, params, **kw)
        if self.after is not None:
            self.after(query if isinstance(query, str) else str(query))
        return cur


class FakeClock:
    def __init__(self, now: dt.datetime) -> None:
        self.t = now.timestamp()

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def job_ctx(dsn: str, clock=None) -> JobContext:
    with psycopg.connect(dsn) as c:
        row = c.execute("SELECT 1 FROM tg.bot_state").fetchone()
        if row is None:
            c.execute("INSERT INTO tg.bot_state (bot_id, allowed_updates) VALUES (5000, '{chat_member}')")
        c.commit()
        gen = db.claim_generation(c)
    return JobContext(config=config.load({}), api=None, health=metrics.Health(), stop=threading.Event(),
                      generation=gen, clock=clock or FakeClock(NOW))


def gauge(name: str, **labels):
    return metrics.REGISTRY.get_sample_value(name, labels)


# ------------------------------------------------------------------------------------------------------
# R1 on the fixture
# ------------------------------------------------------------------------------------------------------


def test_r1_cold_start_gives_verify_numbers(fx):
    res = rollups.refresh_chat_days(fx, tracked(fx), NOW)
    assert res.mode == "full" and res.chats == 18 and res.rows > 1000
    day = {(r["chat_id"], r["day"].isoformat()): r for r in q(fx, "SELECT * FROM stats.v_chat_day")}
    assert day[(A, "2026-08-01")]["corrections_in"] == 5                       # 9a: A 08-01 corrections
    assert (day[(A, "2026-11-05")]["joins"], day[(A, "2026-11-05")]["exits"]) == (1, 1)
    assert day[(A, "2026-11-03")]["rejoins"] == 1
    a23 = day[(A, "2026-11-23")]
    assert (a23["reported_eod"], a23["known_eod"], a23["residual"]) == (15, 12, 1)
    assert day[(B, "2026-11-13")]["corrections_out"] == 1
    assert day[(B, "2026-11-20")]["messages"] == 9                              # 5 user + 4 automatic forwards
    eod = {c: q1(fx, "SELECT members_eod, unsure_eod FROM stats.v_chat_day WHERE chat_id = %s AND day = %s",
                 (c, "2026-11-07")) for c in AD}                              # 16d
    assert [(eod[c]["members_eod"], eod[c]["unsure_eod"]) for c in AD] == [(12, 0), (7, 1), (5, 0), (4, 0)]


def test_both_modes_give_the_same_rows(fx):
    fx.execute("SAVEPOINT s")
    full = rollups.refresh_chat_days(fx, tracked(fx), NOW, mode="full")
    rows_full = chat_day_rows(fx)
    fx.execute("ROLLBACK TO s")
    per_chat = rollups.refresh_chat_days(fx, tracked(fx), NOW, mode="chat")
    assert (full.mode, per_chat.mode) == ("full", "chat")
    assert chat_day_rows(fx) == rows_full and len(rows_full) == full.rows == per_chat.rows


def test_modes_agree_on_a_normal_run_with_an_old_dirty_day(fx):
    rollups.refresh_chat_days(fx, tracked(fx), NOW, mode="full")
    base = chat_day_rows(fx)
    for mode in ("chat", "full"):
        fx.execute("SELECT tg.mark_dirty(%s, '2026-10-20')", (A,))
        res = rollups.refresh_chat_days(fx, [A], NOW, mode=mode)
        assert res.from_days == {A: dt.date(2026, 10, 20)} and res.markers_cleared == 1
        assert chat_day_rows(fx) == base


# ------------------------------------------------------------------------------------------------------
# R1: markers
# ------------------------------------------------------------------------------------------------------


def test_invalidation_recomputes_from_the_dirty_day_and_clears_the_marker(clone):
    """docs/design/evidence/50-invalidation.sql: a late snapshot-seeded spell for A on 11-04."""
    dsn = clone()
    with psycopg.connect(dsn) as c:
        rollups.refresh_chat_days(c, tracked(c), NOW)
        c.commit()
        before = {r["day"].isoformat(): r for r in q(
            c, "SELECT day, corrections_in, refreshed_at FROM stats.chat_day WHERE chat_id = %s "
            "AND day BETWEEN '2026-11-03' AND '2026-11-05' ORDER BY day", (A,))}
        assert before["2026-11-04"]["corrections_in"] == 0
        c.execute("INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at) "
                  "VALUES (%s, 1032, 'snapshot', '2026-11-04 12:00+00', '2026-11-04 12:00+00')", (A,))
        c.execute("SELECT tg.mark_dirty(%s, '2026-11-04')", (A,))
        c.commit()
        for chats in ([A], [B]):
            rollups.refresh_chat_days(c, chats, NOW)
            c.commit()
        after = {r["day"].isoformat(): r for r in q(
            c, "SELECT day, corrections_in, refreshed_at FROM stats.chat_day WHERE chat_id = %s "
            "AND day BETWEEN '2026-11-03' AND '2026-11-05' ORDER BY day", (A,))}
        assert after["2026-11-04"]["corrections_in"] == 1
        assert after["2026-11-03"]["refreshed_at"] == before["2026-11-03"]["refreshed_at"]   # kept
        assert after["2026-11-04"]["refreshed_at"] > before["2026-11-04"]["refreshed_at"]    # recomputed
        first = {r["chat_id"]: r["day"].isoformat() for r in q(
            c, "SELECT chat_id, min(day) AS day FROM stats.chat_day WHERE refreshed_at > %s "
            "AND chat_id IN (%s, %s) GROUP BY chat_id", (before["2026-11-03"]["refreshed_at"], A, B))}
        assert first == {A: "2026-11-04", B: "2026-11-21"}                                   # B: 2 days back
        assert c.execute("SELECT count(*) FROM tg.rollup_dirty").fetchone()[0] == 0
        # undo, the same way
        c.execute("DELETE FROM tg.membership WHERE chat_id = %s AND user_id = 1032", (A,))
        c.execute("SELECT tg.mark_dirty(%s, '2026-11-04')", (A,))
        rollups.refresh_chat_days(c, [A], NOW)
        assert c.execute("SELECT corrections_in FROM stats.chat_day WHERE chat_id = %s AND day = '2026-11-04'",
                         (A,)).fetchone()[0] == 0


def test_late_evidence_on_an_old_day_recomputes_from_that_day(clone):
    dsn = clone()
    with psycopg.connect(dsn) as c:
        rollups.refresh_chat_days(c, tracked(c), NOW)
        c.commit()
        c.execute("DELETE FROM stats.chat_day WHERE chat_id = %s AND day = '2026-09-02'", (C,))  # a hole to find
        c.execute("SELECT tg.mark_dirty(%s, '2026-09-02')", (C,))
        c.commit()
        res = rollups.refresh_chat_days(c, [C], NOW)
        assert res.from_days == {C: dt.date(2026, 9, 2)} and res.mode == "full"
        n = c.execute("SELECT count(*) FROM stats.chat_day WHERE chat_id = %s AND day >= '2026-09-02'",
                      (C,)).fetchone()[0]
        assert n == (NOW.date() - dt.date(2026, 9, 2)).days + 1                  # every day to today, hole filled


def test_a_late_sighting_before_the_first_rollup_day_is_recomputed_from_its_own_day(clone):
    dsn = clone()
    with psycopg.connect(dsn) as c:
        rollups.refresh_chat_days(c, tracked(c), NOW)
        c.commit()
        first = c.execute("SELECT min(day) FROM stats.chat_day WHERE chat_id = %s", (D,)).fetchone()[0]
        # a snapshot found someone seen a month before the chat's first rollup day and before its coverage
        seen = dt.datetime.combine(first - dt.timedelta(days=30), dt.time(9), UTC)
        c.execute("INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at, "
                  "left_after, left_before, end_source, end_reason) VALUES (%s, 1033, 'snapshot', %s, %s, %s, %s, "
                  "'probe', 'gone')", (D, seen, seen, seen + dt.timedelta(days=3), seen + dt.timedelta(days=4)))
        c.execute("SELECT tg.mark_dirty(%s, %s)", (D, seen.date()))
        c.commit()
        res = rollups.refresh_chat_days(c, [D], NOW)
        assert res.from_days == {D: seen.date()}                                  # not clamped to `first`
        rows = {r["day"]: r for r in q(c, "SELECT * FROM stats.chat_day WHERE chat_id = %s ORDER BY day", (D,))}
        assert min(rows) == seen.date()
        assert rows[seen.date()]["members_started"] == 1 and rows[seen.date()]["corrections_in"] == 1
        assert rows[(seen + dt.timedelta(days=4)).date()]["members_ended"] == 1
        assert rows[(seen + dt.timedelta(days=3)).date()]["unsure_started"] == 1
        assert c.execute("SELECT count(*) FROM tg.rollup_dirty").fetchone()[0] == 0


def test_a_marker_written_during_a_run_survives_it(clone):
    dsn = clone()
    with psycopg.connect(dsn) as c:
        rollups.refresh_chat_days(c, tracked(c), NOW)
        c.execute("SELECT tg.mark_dirty(%s, '2026-11-04')", (A,))
        c.commit()
    seen: list[int] = []
    with HookConnection.connect(dsn) as c, psycopg.connect(dsn) as writer:
        writer.execute("SELECT 1")                       # the writer's transaction starts before the run's ...
        time.sleep(0.05)

        def commit_a_writer(query: str) -> None:         # ... and commits between the aggregate read and the delete
            if query.lstrip().startswith("INSERT INTO stats.chat_day") and not seen:
                seen.append(writer.execute("SELECT tg.mark_dirty(%s, '2026-11-02')", (A,)).fetchone()[0])
                writer.commit()
        c.after = commit_a_writer
        res = rollups.refresh_chat_days(c, [A], NOW)
        c.after = None
        assert seen and res.markers_cleared == 0
        c.commit()
        marker = c.execute("SELECT from_day, version FROM tg.rollup_dirty WHERE chat_id = %s", (A,)).fetchone()
        assert marker == (dt.date(2026, 11, 2), seen[0])
        res = rollups.refresh_chat_days(c, [A], NOW)                              # the next run takes it
        assert res.from_days == {A: dt.date(2026, 11, 2)} and res.markers_cleared == 1
        assert c.execute("SELECT count(*) FROM tg.rollup_dirty").fetchone()[0] == 0


# ------------------------------------------------------------------------------------------------------
# R1: stored columns
# ------------------------------------------------------------------------------------------------------


def _tiny_chat(c, chat: int = -100777, users=(1, 2)) -> None:
    c.execute("SELECT tg.ensure_partitions('member_event', 'month', '2026-10-01', '2026-12-01')")
    c.execute("INSERT INTO tg.chat (chat_id, chat_type, title, tracked) VALUES (%s, 'supergroup', 't', true)", (chat,))
    c.execute("INSERT INTO tg.coverage (chat_id, period) VALUES (%s, tstzrange('2026-10-01 00:00+00', NULL))", (chat,))
    for u in users:
        c.execute("INSERT INTO tg.tg_user (user_id, first_seen_at, last_seen_at) VALUES (%s, now(), now())", (u,))


def _event(c, chat: int, user: int, at: str, update_id: int, old: str, new: str) -> int:
    return c.execute("INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status) "
                     "VALUES (%s, %s, %s, %s, %s, %s) RETURNING event_id", (chat, user, at, update_id, old, new)
                     ).fetchone()[0]


def _stay(c, chat: int, user: int, join: tuple[int, str] | None, leave: tuple[int, str] | None = None) -> None:
    """A stay opened by the join event (id, time) and closed by the leave event, in the order given."""
    cols = {"chat_id": chat, "user_id": user, "start_source": "event", "join_route": "link",
            "join_event_id": join[0], "joined_at": join[1], "known_from": join[1], "last_confirmed_at": join[1]}
    if leave:
        cols |= {"end_source": "event", "end_reason": "left", "left_event_id": leave[0], "left_at": leave[1],
                 "left_after": leave[1], "left_before": leave[1]}
    c.execute(f"INSERT INTO tg.membership ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
              list(cols.values()))


@pytest.mark.parametrize("rebuilt", [False, True], ids=["before-rebuild", "after-rebuild"])
@pytest.mark.parametrize("mode", ["full", "chat"])
def test_join_leave_join_in_one_second_is_one_rejoin(migrated_dsn, mode, rebuilt):
    t = "2026-11-20 10:00:00+00"
    with psycopg.connect(migrated_dsn) as c:
        _tiny_chat(c)
        j1, l1, j2 = (_event(c, -100777, 1, t, 101, "left", "member"), _event(c, -100777, 1, t, 102, "member", "left"),
                      _event(c, -100777, 1, t, 103, "left", "member"))
        # the ids of the spells do not tell their order: insert the open one first after a "rebuild"
        if rebuilt:
            _stay(c, -100777, 1, (j2, t))
            _stay(c, -100777, 1, (j1, t), (l1, t))
        else:
            _stay(c, -100777, 1, (j1, t), (l1, t))
            _stay(c, -100777, 1, (j2, t))
        c.commit()
        rollups.refresh_chat_days(c, [-100777], NOW, mode=mode)
        row = c.execute("SELECT joins, rejoins, exits FROM stats.chat_day WHERE chat_id = -100777 AND day = '2026-11-20'"
                        ).fetchone()
        assert row == (2, 1, 1)
        res = sql.run(c, "10a", as_of="2026-11-23 12:00+00", since="2026-11-01", scope=[-100777])
        assert [(r["rejoins"], r["users"]) for r in res.dicts()] == [(1, 1)]


def _censored(c, chat: int, user: int, leave: tuple[int, str]) -> None:
    """E6: the first retained event is a leave: an inferred stay that starts and ends at that exit."""
    c.execute("INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at, end_source, "
              "end_reason, left_event_id, left_at, left_after, left_before) VALUES (%s, %s, 'inferred', %s, %s, 'event', "
              "'left', %s, %s, %s, %s)", (chat, user, leave[1], leave[1], leave[0], leave[1], leave[1], leave[1]))


@pytest.mark.parametrize("censored_first", [True, False], ids=["censored-spell-first", "join-spell-first"])
@pytest.mark.parametrize("mode", ["full", "chat"])
def test_a_rejoin_in_the_second_of_a_left_censored_exit_is_counted(migrated_dsn, mode, censored_first):
    """The retained history starts with a leave (E6); a larger update id rejoins in the same second."""
    t = "2026-11-20 10:00:00+00"
    with psycopg.connect(migrated_dsn) as c:
        _tiny_chat(c)
        left = _event(c, -100777, 1, t, 101, "member", "left")
        joined = _event(c, -100777, 1, t, 102, "left", "member")
        if censored_first:
            _censored(c, -100777, 1, (left, t))
            _stay(c, -100777, 1, (joined, t))
        else:
            _stay(c, -100777, 1, (joined, t))
            _censored(c, -100777, 1, (left, t))
        c.commit()
        rollups.refresh_chat_days(c, [-100777], NOW, mode=mode)
        row = c.execute("SELECT joins, rejoins FROM stats.chat_day WHERE chat_id = -100777 AND day = '2026-11-20'"
                        ).fetchone()
        assert row == (1, 1)
        res = sql.run(c, "10a", as_of="2026-11-23 12:00+00", since="2026-11-01", scope=[-100777]).dicts()
        assert [(r["rejoins"], r["observed"], r["possible"]) for r in res] == [(1, 1, 0)]  # the observed join, not E6


def _migrate_c(dsn: str) -> int:
    """M1 in its own session: C moves to a new supergroup (the old chat and its daily rows are deleted)."""
    new_chat = -1001999999998
    with psycopg.connect(dsn) as m1:
        m1.execute("INSERT INTO tg.chat (chat_id, chat_type, title, tracked) VALUES (%s, 'supergroup', 'new', true)",
                   (new_chat,))
        m1.execute("SELECT tg.move_chat_history(%s, %s, '2026-11-23 11:00+00')", (C, new_chat))
        m1.commit()
    return new_chat


def test_a_migration_committed_between_the_target_and_the_write_leaves_no_rows_for_the_old_chat(clone):
    dsn = clone()
    with psycopg.connect(dsn) as c:
        rollups.refresh_chat_days(c, tracked(c), NOW)
        c.commit()
    new_chat: list[int] = []
    with HookConnection.connect(dsn) as c:
        c.after = lambda q: (not new_chat and q.lstrip().startswith("CREATE TEMP TABLE chat_day_target")
                             and new_chat.append(_migrate_c(dsn)))
        with pytest.raises(psycopg.errors.ForeignKeyViolation):                 # the step fails, the job retries
            rollups.refresh_chat_days(c, [C], NOW)
        c.after = None
        c.rollback()
        assert c.execute("SELECT count(*) FROM stats.chat_day WHERE chat_id = %s", (C,)).fetchone()[0] == 0
        # the retry: the old chat is gone (no target), the new one is built from the marker the move left
        res = rollups.refresh_chat_days(c, [C, new_chat[0]], NOW)
        assert list(res.from_days) == [new_chat[0]] and res.markers_cleared == 1
        c.commit()
        assert c.execute("SELECT count(*) FROM stats.chat_day WHERE chat_id = %s", (C,)).fetchone()[0] == 0
        assert c.execute("SELECT count(*) FROM stats.chat_day WHERE chat_id = %s", (new_chat[0],)).fetchone()[0] > 0
        assert c.execute("SELECT count(*) FROM tg.rollup_dirty").fetchone()[0] == 0


def test_a_migration_committed_after_the_write_takes_the_daily_rows_with_the_chat(clone):
    dsn = clone()
    with psycopg.connect(dsn) as c:
        rollups.refresh_chat_days(c, tracked(c), NOW)
        c.commit()
        assert c.execute("SELECT count(*) FROM stats.chat_day WHERE chat_id = %s", (C,)).fetchone()[0] > 0
        _migrate_c(dsn)
        assert c.execute("SELECT count(*) FROM stats.chat_day WHERE chat_id = %s", (C,)).fetchone()[0] == 0
        c.rollback()
        assert c.execute("SELECT count(*) FROM stats.chat_day WHERE chat_id NOT IN (SELECT chat_id FROM tg.chat)"
                         ).fetchone()[0] == 0


def test_the_residual_compares_the_count_with_the_events_between_the_same_samples(migrated_dsn):
    """Three day-end samples and a 23:58 join after the middle one: no false residual on either day."""
    chat = -100777
    with psycopg.connect(migrated_dsn) as c:
        _tiny_chat(c)
        _stay(c, chat, 1, (_event(c, chat, 1, "2026-11-21 23:58+00", 201, "left", "member"), "2026-11-21 23:58+00"))
        with c.cursor() as cur:
            cur.executemany("INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, net_events) "
                            "VALUES (%s, %s, %s, 0, 0)",
                            [(chat, "2026-11-20 23:50+00", 100), (chat, "2026-11-21 23:50+00", 100),
                             (chat, "2026-11-22 23:50+00", 101),
                             (chat, "2026-11-23 13:00+00", 109)])                # after as_of: not finalized
        c.commit()
        rollups.refresh_chat_days(c, [chat], NOW)
        rows = {r[0].isoformat(): r[1:] for r in c.execute(
            "SELECT day, joins, net, reported_eod, reported_change, sample_net, residual FROM stats.v_chat_day "
            "WHERE chat_id = %s AND day BETWEEN '2026-11-20' AND '2026-11-23' ORDER BY day", (chat,))}
        # day: joins, calendar net, reported, change, net between the two samples, residual
        assert rows["2026-11-21"] == (1, 1, 100, 0, 0, 0)       # the join came after this day's sample
        assert rows["2026-11-22"] == (0, 0, 101, 1, 1, 0)       # ... and belongs to the next interval
        assert rows["2026-11-23"][2:] == (None, None, None, None)  # no finalized sample today (13:00 > as_of)
        sample_at = c.execute("SELECT sample_at FROM stats.chat_day WHERE chat_id = %s AND day = '2026-11-22'",
                              (chat,)).fetchone()[0]
        assert sample_at == ts("2026-11-22 23:50")


@pytest.mark.parametrize("mode", ["full", "chat"])
def test_16d_equals_16a_at_the_next_midnight_with_unresolved_suspect_spells(fx, mode):
    # a second unresolved suspicion in A, in the middle of a day
    fx.execute("INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at, suspect_since) "
               "VALUES (%s, 1032, 'snapshot', '2026-11-02 08:00+00', '2026-11-02 08:00+00', '2026-11-06 10:00+00')", (A,))
    rollups.refresh_chat_days(fx, tracked(fx), NOW, mode=mode)
    cases = [(H11, f"2026-09-{d:02d}") for d in range(17, 24)] + [(A, f"2026-11-{d:02d}") for d in range(2, 9)] \
        + [(c, "2026-11-07") for c in (B, C, D)]
    for chat, day in cases:
        want = q1(fx, "SELECT members_eod, unsure_eod FROM stats.v_chat_day WHERE chat_id = %s AND day = %s",
                  (chat, day))
        nxt = dt.date.fromisoformat(day) + dt.timedelta(days=1)
        r = sql.run(fx, "16a", as_of="2026-11-23 12:00+00", chat=chat, day=f"{nxt} 00:00+00").dicts()[0]
        assert (want["members_eod"], want["unsure_eod"]) == (r["known_in"] + r["unsure"], r["unsure"]), (chat, day)
    # the suspicion itself: H11 is known until its suspect_since (09-20 00:00 sharp), unsure from the day before's end
    h = {d: q1(fx, "SELECT members_eod, unsure_eod FROM stats.v_chat_day WHERE chat_id = %s AND day = %s",
               (H11, d)) for d in ("2026-09-18", "2026-09-19", "2026-09-20")}
    assert [v["unsure_eod"] for v in h.values()] == [0, 1, 1]
    assert len({v["members_eod"] for v in h.values()}) == 1


def test_midnight_instants_count_into_the_day_before(migrated_dsn):
    chat = -100777
    with psycopg.connect(migrated_dsn) as c:
        _tiny_chat(c)
        j = _event(c, chat, 1, "2026-11-21 00:00:00+00", 301, "left", "member")
        _stay(c, chat, 1, (j, "2026-11-21 00:00:00+00"))
        rollups.refresh_chat_days(c, [chat], NOW)
        got = {r[0].isoformat(): r[1:] for r in c.execute(
            "SELECT day, members_started, members_midnight, members_eod FROM stats.v_chat_day "
            "WHERE chat_id = %s AND day BETWEEN '2026-11-19' AND '2026-11-22' ORDER BY day", (chat,))}
        assert got["2026-11-20"] == (0, 0, 1)      # in at the end of 11-20, i.e. at 11-21 00:00 (16a's instant)
        assert got["2026-11-21"] == (1, 1, 1)
        r = sql.run(c, "16a", as_of="2026-11-23 12:00+00", chat=chat, day="2026-11-21 00:00+00").dicts()[0]
        assert r["known_in"] == 1


# ------------------------------------------------------------------------------------------------------
# R2
# ------------------------------------------------------------------------------------------------------


def audience_tables(c) -> dict:
    out = {}
    for name, order in (("tg.chat_set", "chat_ids"), ("stats.overlap", "chat_a, chat_b"),
                        ("stats.unique_per_chat", "chat_id"), ("stats.chats_per_user", "chat_count"),
                        ("tg.user_chats", "user_id")):
        out[name] = [json.loads(json.dumps(r[0], default=str)) for r in c.execute(
            f"SELECT to_jsonb(t) - 'as_of' FROM {name} t ORDER BY {order}")]
    return out


def test_r2_gives_verify_numbers(fx):
    fx.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    res = rollups.refresh_audience(fx, NOW)
    assert (res.pairs, res.refreshed) == (171, "plain")                           # 18 * 19 / 2
    pair = {(r["chat_a"], r["chat_b"]): r["shared"] for r in q(fx, "SELECT * FROM stats.overlap")}

    def shared(x: int, y: int) -> int:
        return pair[(min(x, y), max(x, y))]                                      # one row per unordered pair
    assert [shared(A, B), shared(A, C), shared(A, D), shared(B, C), shared(B, D), shared(C, D)] == [4, 2, 1, 3, 1, 1]
    assert [shared(x, x) for x in AD] == [10, 6, 5, 2]
    uniq = {r["chat_id"]: (r["members"], r["exclusive"]) for r in q(fx, "SELECT * FROM stats.unique_per_chat")}
    assert [uniq[x][1] for x in AD] == [6, 1, 2, 1]
    cpu = {r["chat_count"]: r["users"] for r in q(fx, "SELECT * FROM stats.chats_per_user")}
    assert (cpu[1], cpu[2], cpu[3], cpu[4]) == (20, 3, 1, 1) and sum(cpu.values()) == 25
    assert {r["as_of"] for r in q(fx, "SELECT as_of FROM stats.overlap")} == {NOW}
    run = q1(fx, "SELECT * FROM stats.rollup_run WHERE rollup = 'audience'")
    assert run["as_of"] == NOW and run["row_count"] == 171


def test_r2_refuses_a_weaker_isolation_level(fx):
    with pytest.raises(RuntimeError, match="REPEATABLE READ"):
        rollups.refresh_audience(fx, NOW)


def test_audience_refresh_is_a_callable_that_commits(migrated_dsn):
    with psycopg.connect(migrated_dsn) as c:
        c.execute("SELECT 1")  # an open transaction of the caller is ended first
        res = audience.refresh(c, NOW)
        assert res.refreshed == "plain" and c.isolation_level is None
        assert c.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        assert audience.refresh(c, NOW).refreshed == "concurrently"
        assert c.execute("SELECT count(*) FROM stats.rollup_run WHERE rollup = 'audience'").fetchone()[0] == 2
    with psycopg.connect(migrated_dsn, autocommit=True) as c, pytest.raises(ValueError):
        audience.refresh(c, NOW)


@pytest.mark.parametrize("populated", [False, True], ids=["plain-refresh", "concurrent-refresh"])
@pytest.mark.parametrize("pause", ["SELECT ispopulated", "REFRESH MATERIALIZED", "INSERT INTO tg.chat_set"])
def test_r2_describes_one_side_of_a_migration(clone, populated, pause):
    """M1 commits while R2 runs (before the view refresh, right after it, between projections): every
    projection still describes the state R2 started from."""
    expected_dsn, test_dsn = clone(), clone()
    new_chat = -1001999999999
    for dsn in (expected_dsn, test_dsn):
        if populated:
            with psycopg.connect(dsn) as c:
                audience.refresh(c, NOW)
    with psycopg.connect(expected_dsn) as c:
        audience.refresh(c, NOW)
        want = audience_tables(c)
    fired: list[bool] = []
    with HookConnection.connect(test_dsn) as c, psycopg.connect(test_dsn) as m1:
        def migrate(query: str) -> None:
            if query.lstrip().startswith(pause) and not fired:
                fired.append(True)
                m1.execute("INSERT INTO tg.chat (chat_id, chat_type, title, tracked) VALUES (%s, 'supergroup', 'new', true)",
                           (new_chat,))
                m1.execute("SELECT tg.move_chat_history(%s, %s, '2026-11-23 11:00+00')", (C, new_chat))
                m1.commit()
        c.after = migrate
        audience.refresh(c, NOW)
        c.after = None
        assert fired
        assert audience_tables(c) == want
        assert c.execute("SELECT count(*) FROM tg.chat WHERE chat_id = %s", (C,)).fetchone()[0] == 0  # M1 did commit


def test_python_ports_equal_the_psql_script_on_the_fixture(pg, clone):
    py, ps = clone(), clone()
    dbname = psycopg.conninfo.conninfo_to_dict(ps)["dbname"]
    with psycopg.connect(py) as c:
        rollups.refresh_chat_days(c, tracked(c), NOW)
        c.commit()
        audience.refresh(c, NOW)
        py_days, py_tables = chat_day_rows(c), audience_tables(c)
    path = pathlib.Path(str(db.sql_root() / "queries.sql"))
    for block in ("R1", "R2"):
        r = pg.psql(dbname, path, variables={"only": block, "as_of": "2026-11-23 12:00+00"}, check=False)
        assert r.returncode == 0, r.stderr
    with psycopg.connect(ps) as c:
        assert chat_day_rows(c) == py_days
        assert audience_tables(c) == py_tables
        runs = c.execute("SELECT rollup, as_of, row_count FROM stats.rollup_run ORDER BY rollup").fetchall()
        assert [r[0] for r in runs] == ["audience", "chat_day"] and all(r[1] == NOW for r in runs)
    # a normal run: a late spell three days back, so psql takes its per-chat branch, Python one step per chat
    for dsn in (py, ps):
        with psycopg.connect(dsn) as c:
            c.execute("INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at, "
                      "suspect_since) VALUES (%s, 1032, 'snapshot', '2026-11-20 12:00+00', '2026-11-20 12:00+00', "
                      "'2026-11-21 00:00+00')", (A,))
            c.execute("SELECT tg.mark_dirty(%s, '2026-11-20')", (A,))
            c.commit()
    with psycopg.connect(py) as c:
        modes = {rollups.refresh_chat_days(c, [chat], NOW).mode for chat in tracked(c)}
        c.commit()
        assert modes == {"chat"}
        py_days = chat_day_rows(c)
    assert pg.psql(dbname, path, variables={"only": "R1", "as_of": "2026-11-23 12:00+00"}, check=False).returncode == 0
    with psycopg.connect(ps) as c:
        assert chat_day_rows(c) == py_days
        assert c.execute("SELECT count(*) FROM tg.rollup_dirty").fetchone()[0] == 0


# ------------------------------------------------------------------------------------------------------
# R4
# ------------------------------------------------------------------------------------------------------


def partition_names(c, parent: str) -> list[str]:
    return [r[0] for r in c.execute(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "WHERE i.inhparent = %s::regclass ORDER BY 1", (f"tg.{parent}",))]


def test_r4_creates_partitions_ahead_and_drops_expired_ones(migrated_dsn):
    def days(first: str, last: str, parent: str) -> list[str]:
        d, out = dt.date.fromisoformat(first), []
        while d <= dt.date.fromisoformat(last):
            out.append(f"{parent}_d{d:%Y%m%d}")
            d += dt.timedelta(days=1)
        return out

    with psycopg.connect(migrated_dsn) as c:
        partitions.maintain(c, ts("2026-02-10 12:00"))
        c.commit()
        assert partition_names(c, "raw_update") == days("2026-02-08", "2026-02-17", "raw_update")
        assert partition_names(c, "update_receipt") == days("2026-02-08", "2026-02-17", "update_receipt")
        months = [n for n in partition_names(c, "member_event") if "_y2026" in n]
        assert months == [f"member_event_y2026m{m:02d}" for m in range(1, 6)]    # a month back, three ahead
        ahead = partitions.maintain(c, ts("2026-03-15 12:00"))
        c.commit()
        # raw_update keeps 30 days: the partitions that ended before 02-13 12:00 are gone, the rest stays
        assert partition_names(c, "raw_update") == days("2026-02-13", "2026-02-17", "raw_update") \
            + days("2026-03-13", "2026-03-22", "raw_update")
        assert partition_names(c, "update_receipt") == days("2026-03-13", "2026-03-22", "update_receipt")   # 8 days
        months = [n for n in partition_names(c, "member_event") if "_y2026" in n]
        assert months == [f"member_event_y2026m{m:02d}" for m in range(1, 7)]    # history is kept forever
        assert ahead == {"member_event": 107.5, "raw_update": 7.5, "update_receipt": 7.5}


# ------------------------------------------------------------------------------------------------------
# The jobs on the scheduler
# ------------------------------------------------------------------------------------------------------


@pytest.fixture
def sched(clone):
    dsn = clone()
    clock = FakeClock(NOW)
    ctx = job_ctx(dsn, clock)
    jobs = [chat_day.make(ctx), audience.make(ctx), partitions.make(ctx)]
    s = Scheduler("batch", jobs, lambda: psycopg.connect(dsn), ctx)
    s.conn = s.connect()
    yield dsn, clock, jobs, s
    s._discard()


def drain(s: Scheduler, job, limit: int = 60) -> int:
    steps = 0
    while job.due(s.ctx.clock()) and steps < limit:
        s.run_once()
        steps += 1
    return steps


def test_chat_day_job_runs_a_cycle_per_interval_and_publishes_it(sched):
    dsn, clock, (job, *_), s = sched
    assert job.due(clock()) is True
    steps = drain(s, job)
    assert steps == 2 and job.pending == 0                                   # the plan and one set-based step
    assert gauge("tgaud_db_transaction_seconds_count", job="chat_day") >= 2
    assert gauge("tgaud_rollup_as_of_timestamp_seconds", rollup="chat_day") == NOW.timestamp()
    with psycopg.connect(dsn) as c:
        run = c.execute("SELECT as_of, row_count FROM stats.rollup_run WHERE rollup = 'chat_day'").fetchall()
        assert len(run) == 1 and run[0][0] == NOW and run[0][1] == c.execute("SELECT count(*) FROM stats.chat_day").fetchone()[0]
    assert job.due(clock()) is False
    clock.advance(chat_day.INTERVAL - 1)
    assert job.due(clock()) is False
    clock.advance(1)
    assert job.due(clock()) is True
    # the second cycle: every chat is near, so one step per chat after the plan
    drain(s, job)
    with psycopg.connect(dsn) as c:
        assert c.execute("SELECT count(*) FROM stats.rollup_run WHERE rollup = 'chat_day'").fetchone()[0] == 2
    assert gauge("tgaud_rollup_as_of_timestamp_seconds", rollup="chat_day") == clock()


def test_chat_day_job_steps_are_one_chat_each(sched):
    dsn, clock, (job, *_), s = sched
    drain(s, job)
    clock.advance(chat_day.INTERVAL)
    s.run_once()                                                              # the plan
    assert job.pending == 18
    with psycopg.connect(dsn) as spy:
        before = spy.execute("SELECT max(refreshed_at) FROM stats.chat_day").fetchone()[0]
    s.run_once()
    assert job.pending == 17
    with psycopg.connect(dsn) as spy:
        n = spy.execute("SELECT count(DISTINCT chat_id) FROM stats.chat_day WHERE refreshed_at > %s", (before,)).fetchone()[0]
    assert n == 1


def test_a_failing_chat_day_step_leaves_the_previous_rows_and_markers_and_no_new_rollup_run(sched):
    dsn, clock, (job, *_), s = sched
    drain(s, job)
    with psycopg.connect(dsn) as c:
        c.execute("SELECT tg.mark_dirty(%s, '2026-11-20')", (A,))
        c.execute("CREATE FUNCTION public.boom() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                  "IF NEW.chat_id = %s THEN RAISE EXCEPTION 'boom'; END IF; RETURN NEW; END $$" % A)
        c.execute("CREATE TRIGGER boom BEFORE INSERT ON stats.chat_day FOR EACH ROW EXECUTE FUNCTION public.boom()")
        c.commit()
        old = chat_day_rows(c, f"chat_id = {A}")
        old_stamp = c.execute("SELECT max(refreshed_at) FROM stats.chat_day WHERE chat_id = %s", (A,)).fetchone()[0]
    clock.advance(chat_day.INTERVAL)
    failures = gauge("tgaud_job_failures_total", job="chat_day") or 0
    while job.due(clock()):
        s.run_once()
        if (gauge("tgaud_job_failures_total", job="chat_day") or 0) > failures:
            break
    assert (gauge("tgaud_job_failures_total", job="chat_day") or 0) == failures + 1
    assert job.due(clock()) is False                                          # backs off after a failure
    clock.advance(chat_day.RETRY)
    assert job.due(clock()) is True
    with psycopg.connect(dsn) as c:
        assert chat_day_rows(c, f"chat_id = {A}") == old
        assert c.execute("SELECT max(refreshed_at) FROM stats.chat_day WHERE chat_id = %s", (A,)).fetchone()[0] == old_stamp
        assert c.execute("SELECT from_day FROM tg.rollup_dirty WHERE chat_id = %s", (A,)).fetchone()[0] == dt.date(2026, 11, 20)
        assert c.execute("SELECT count(*) FROM stats.rollup_run WHERE rollup = 'chat_day'").fetchone()[0] == 1
    assert gauge("tgaud_rollup_as_of_timestamp_seconds", rollup="chat_day") == NOW.timestamp()


def test_audience_job_publishes_hourly_and_a_failing_run_leaves_the_previous_rollup(sched):
    dsn, clock, (_, job, _), s = sched
    assert job.due(clock()) is True
    s.run_once()
    assert gauge("tgaud_rollup_as_of_timestamp_seconds", rollup="audience") == NOW.timestamp()
    assert job.due(clock()) is False
    clock.advance(audience.INTERVAL)
    assert job.due(clock()) is True
    with psycopg.connect(dsn) as c:
        before = audience_tables(c)
        c.execute("CREATE FUNCTION public.boom() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                  "RAISE EXCEPTION 'boom'; END $$")
        c.execute("CREATE TRIGGER boom BEFORE INSERT ON stats.chats_per_user FOR EACH ROW EXECUTE FUNCTION public.boom()")
        c.commit()
    failures = gauge("tgaud_job_failures_total", job="audience") or 0
    s.run_once()
    assert (gauge("tgaud_job_failures_total", job="audience") or 0) == failures + 1
    assert gauge("tgaud_rollup_as_of_timestamp_seconds", rollup="audience") == NOW.timestamp()   # not advanced
    with psycopg.connect(dsn) as c:
        assert audience_tables(c) == before                                  # all four rollups as they were
        assert c.execute("SELECT count(*), max(as_of) FROM stats.rollup_run WHERE rollup = 'audience'").fetchone() == (1, NOW)
        assert c.execute("SELECT DISTINCT as_of FROM stats.overlap").fetchall() == [(NOW,)]
    assert job.due(clock()) is False
    clock.advance(audience.RETRY)
    assert job.due(clock()) is True


def test_partitions_job_runs_at_start_then_daily_and_sets_the_gauge(sched):
    dsn, clock, (_, _, job), s = sched
    assert job.due(clock()) is True
    s.run_once()
    assert gauge("tgaud_partitions_ahead_days", table="raw_update") == 7.5
    assert gauge("tgaud_partitions_ahead_days", table="member_event") >= 90
    assert job.due(clock()) is False
    clock.advance(partitions.INTERVAL - 1)
    assert job.due(clock()) is False
    clock.advance(1)
    assert job.due(clock()) is True
