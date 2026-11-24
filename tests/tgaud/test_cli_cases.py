"""Small hand-built cases for the decisions of the CLI task, run through the commands on a copy of the fixture:
giveaway eligibility (only_new_members), future and past campaigns, same-second ties in 10a and the entrant
choice, and the as-of cutoff of the windowed blocks (5, 10b, 13, 15, 12).
"""

import datetime as dt

import psycopg
import pytest
from clidb import A, NOW_ARG, UTC, Builder, cli_for, fresh_db, report_db, ts  # noqa: F401
from clidb import copy_db

from tgaud import sql

T = ts("2026-11-10 09:00:00")  # the instant of the same-second cases


def pick(rows, **want):
    found = [r for r in rows if all(r[k] == str(v) for k, v in want.items())]
    assert len(found) == 1, (want, found)
    return found[0]


@pytest.fixture(scope="module")
def cases_db(pg, report_db):
    """A copy of the fixture with the small cases written into their own chats."""
    dsn = copy_db(pg, report_db)
    with psycopg.connect(dsn) as c:
        b = Builder(c)
        # --- 10a: the same instant. X1: unban, join, leave, promote (the inferred stay comes after the exact join);
        #     X2 the same with the spells stored in the other order; X3: promote, leave, join; X4/X4r: join, leave, join
        for chat, title in [(-1003000000001, "tie X1"), (-1003000000002, "tie X2"), (-1003000000003, "tie X3"),
                            (-1003000000004, "tie X4"), (-1003000000005, "tie X4r")]:
            b.chat(chat, title)
        for chat, reverse in [(-1003000000001, False), (-1003000000002, True)]:
            u = 4001
            b.event(chat, u, T, "kicked", "left", 100)  # unban: opens nothing
            j = b.event(chat, u, T, "left", "member", 101)
            lv = b.event(chat, u, T, "member", "left", 102)
            b.event(chat, u, T, "member", "administrator", 103)  # an in -> in event with no stay: E9, inferred start
            if reverse:
                b.spell(chat, u, T)
                b.spell(chat, u, T, join_ev=j, left_at=T, left_ev=lv)
            else:
                b.spell(chat, u, T, join_ev=j, left_at=T, left_ev=lv)
                b.spell(chat, u, T)
        u, chat = 4002, -1003000000003
        e100 = b.event(chat, u, T, "member", "administrator", 100)  # E9: an inferred stay ...
        e101 = b.event(chat, u, T, "administrator", "left", 101)  # ... closed at once
        e102 = b.event(chat, u, T, "left", "member", 102)  # ... then an exact join: the rejoin
        b.spell(chat, u, T, left_at=T, left_ev=e101)
        b.spell(chat, u, T, join_ev=e102)
        assert e100
        for chat, reverse in [(-1003000000004, False), (-1003000000005, True)]:
            u = 4003
            j1 = b.event(chat, u, T, "left", "member", 101)
            l1 = b.event(chat, u, T, "member", "left", 102)
            j2 = b.event(chat, u, T, "left", "member", 103)
            first, second = (lambda: b.spell(chat, u, T, join_ev=j1, left_at=T, left_ev=l1)), (lambda: b.spell(chat, u, T, join_ev=j2))
            (second(), first()) if reverse else (first(), second())

        # --- campaigns: chat Y, window 11-10 .. 11-12; two campaigns (with and without only_new_members)
        y = b.chat(-1003000000010, "campaign Y")
        b.stay(y, 4101, ts("2026-11-10 10:00"), update_id=1000)                       # first-time joiner
        b.stay(y, 4102, ts("2026-09-01 10:00"), ts("2026-09-05 10:00"), update_id=1010)  # a returner:
        b.stay(y, 4102, ts("2026-11-10 11:00"), update_id=1020)                       # an earlier stay, back in the window
        # the same-second case: join, leave, join at T; spells stored latest first
        j1 = b.event(y, 4103, T, "left", "member", 1031)
        l1 = b.event(y, 4103, T, "member", "left", 1032)
        j2 = b.event(y, 4103, T, "left", "member", 1033)
        b.spell(y, 4103, T, join_ev=j2)
        b.spell(y, 4103, T, join_ev=j1, left_at=T, left_ev=l1)
        for name, flag in [("only new", True), ("everyone", False)]:
            cid = c.execute("INSERT INTO tg.campaign (name, kind, origin, starts_at, draw_at, only_new_members) "
                            "VALUES (%s, 'giveaway', 'annotation', '2026-11-10 00:00+00', '2026-11-12 00:00+00', %s) "
                            "RETURNING campaign_id", (name, flag)).fetchone()[0]
            c.execute("INSERT INTO tg.campaign_chat VALUES (%s, %s)", (cid, y))

        # --- block 5 and 15 and 10b and 12: one chat each
        z = b.chat(-1003000000020, "short stay Z")
        b.stay(z, 4201, ts("2026-11-12 10:00"), ts("2026-11-12 20:00"), update_id=2000)
        w = b.chat(-1003000000030, "posts W", "channel")
        c.execute("INSERT INTO tg.post (chat_id, message_id, posted_at, kind) VALUES (%s, 1, '2026-11-12 10:00+00', 'text')", (w,))
        b.stay(w, 4301, ts("2026-11-11 08:00"), ts("2026-11-12 12:00"), update_id=3000)
        b.stay(w, 4302, ts("2026-11-11 08:00"), ts("2026-11-12 14:00"), update_id=3010)
        p, q = b.chat(-1003000000040, "flow P"), b.chat(-1003000000041, "flow Q")
        b.stay(p, 4401, ts("2026-11-12 08:00"), update_id=4000)
        b.stay(q, 4401, ts("2026-11-12 10:00"), update_id=4010)
        c.commit()
    return dsn


@pytest.fixture
def cases(cli_for, cases_db):
    return cli_for(cases_db)


def campaign_ids(dsn):
    with psycopg.connect(dsn) as c:
        return dict(c.execute("SELECT name, campaign_id FROM tg.campaign WHERE name IN ('only new', 'everyone')").fetchall())


# ------------------------------------------------------------------------------------------------------
# Giveaway eligibility
# ------------------------------------------------------------------------------------------------------


def test_only_new_members_reports_every_entrant_and_counts_the_eligible(cases, cases_db):
    ids = campaign_ids(cases_db)
    strict = pick(cases.rows("campaign", "report", str(ids["only new"])), chat_id=-1003000000010)
    assert [strict[k] for k in ("entrants", "first_time", "returners", "eligible")] == ["3", "2", "1", "2"]
    open_ = pick(cases.rows("campaign", "report", str(ids["everyone"])), chat_id=-1003000000010)
    assert [open_[k] for k in ("entrants", "first_time", "returners", "eligible")] == ["3", "2", "1", ""]  # no flag: NULL
    rows = cases.rows("campaign", "report", str(ids["only new"]), "--entrants")
    assert {r["user_id"]: r["eligible"] for r in rows} == {"4101": "true", "4102": "false", "4103": "true"}
    assert pick(rows, user_id=4102)["first_time"] == "false"
    rows = cases.rows("campaign", "report", str(ids["everyone"]), "--entrants")
    assert {r["eligible"] for r in rows} == {"true"}  # without the flag nobody is excluded
    (people,) = cases.rows("campaign", "report", str(ids["only new"]), "--people")
    assert [people[k] for k in ("persons", "first_time", "eligible")] == ["3", "2", "2"]
    (people,) = cases.rows("campaign", "report", str(ids["everyone"]), "--people")
    assert people["eligible"] == ""


def test_same_second_rejoin_leaves_the_first_stay_as_the_entrants_spell(cases, cases_db):
    rows = cases.rows("campaign", "report", str(campaign_ids(cases_db)["only new"]), "--entrants")
    same_second = pick(rows, user_id=4103)  # join, leave, join in one second; the spells were stored latest first
    assert same_second["first_time"] == "true" and same_second["joined_at"] == "2026-11-10T09:00:00+00:00"
    assert same_second["quick_exit"] == "yes"  # the entrant is the first stay, which ended in that second
    assert same_second["still_in"] == "in"  # the second stay is open


# ------------------------------------------------------------------------------------------------------
# Future and historical campaigns
# ------------------------------------------------------------------------------------------------------


def test_a_campaign_that_starts_after_as_of_has_no_entrants_and_no_error(cases, cases_db):
    ids = campaign_ids(cases_db)
    out = cases.run("campaign", "report", str(ids["only new"]), "--as-of", "2026-10-01T00:00:00+00:00", "--csv")
    assert out.code == 0, out.err
    (row,) = [r for r in __import__("csv").DictReader(__import__("io").StringIO(out.out))]
    assert row["entrants"] == "0" and row["window_flags"] == ""
    assert "as of 2026-10-01" in out.err
    table = cases.run("campaign", "report", str(ids["only new"]), "--as-of", "2026-10-01T00:00:00+00:00")
    assert table.code == 0 and "entrants" in table.out


def test_a_scheduled_campaign_entered_with_the_cli_reports_zero_entrants(cases, cases_db):
    out = cases.run("campaign", "add", "Scheduled", "--chat", "campaign Y", "--starts", "2026-12-24", "--draw",
                    "2026-12-31", "--kind", "ad")
    assert out.code == 0, out.err
    new_id = out.out.split()[1]
    (row,) = cases.rows("campaign", "report", new_id)
    assert row["entrants"] == "0" and row["origin"] == "annotation"


def test_outcomes_are_read_at_as_of_not_from_todays_open_row(cases):
    """G1 entrant 1018 joined A 11-06 and left 11-09 12:00: after the draw (11-08), before the exit."""
    now = [r for r in cases.rows("campaign", "report", "1", "--entrants") if r["chat_id"] == str(A)]
    then = cases.rows("campaign", "report", "1", "--entrants", as_of="2026-11-08T12:00:00+00:00")
    then = [r for r in then if r["chat_id"] == str(A)]
    assert pick(now, user_id=1018)["still_in"] == "out" and pick(now, user_id=1018)["exit_after_draw"] == "yes"
    gone_later = pick(then, user_id=1018)
    assert gone_later["still_in"] == "in"  # the exit came after as_of
    assert gone_later["exit_after_draw"] == "immature"  # draw + 48 h is after as_of: never counted
    assert pick(then, user_id=1016)["quick_exit"] == "yes"  # joined 11-04, left 11-05: an exit before as_of counts
    # the summary agrees: of the two who left after the draw only 1017 (11-08 06:00) had left by 11-08 12:00
    (summary,) = [r for r in cases.rows("campaign", "report", "1", as_of="2026-11-08T12:00:00+00:00")
                  if r["chat_id"] == str(A)]
    assert summary["exit_after_draw"] == "1" and summary["exit_after_draw_immature"] == "3" and summary["still_in"] == "3"
    assert pick(then, user_id=1017)["exit_after_draw"] == "yes"


# ------------------------------------------------------------------------------------------------------
# Ties in 10a
# ------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("chat, observed, possible", [
    (-1003000000001, 0, 1),  # unban, join, leave, promote: the exact join came first, the inferred stay is the rejoin
    (-1003000000002, 0, 1),  # the same events, the spells stored in the other order
    (-1003000000003, 1, 0),  # promote, leave, join: the inferred stay came first, the exact join is the rejoin
    (-1003000000004, 1, 0),  # join, leave, join
    (-1003000000005, 1, 0),  # the same, the spells stored in the other order
])
def test_rejoiners_order_same_second_stays_by_their_opening_event(cases, chat, observed, possible):
    rows = cases.rows("rejoiners", "--among", str(chat), "--since", "2026-11-01")
    (row,) = rows
    assert [row[k] for k in ("rejoins", "observed", "possible", "found_later")] == [str(observed + possible), str(observed),
                                                                                  str(possible), "0"]


# ------------------------------------------------------------------------------------------------------
# The as-of cutoff of the windowed blocks
# ------------------------------------------------------------------------------------------------------


def test_block_5_an_exit_after_as_of_is_not_a_short_stay(cases):
    z = str(-1003000000020)
    early = cases.rows("short-stays", "--among", z, "--since", "2026-11-01", as_of="2026-11-12T12:00:00+00:00")
    (row,) = early
    assert [row["joins"], row["short_stays"], row["immature"]] == ["1", "0", "1"]  # joined 10:00, left 20:00, as of noon
    late = cases.rows("short-stays", "--among", z, "--since", "2026-11-01", as_of="2026-11-13T12:00:00+00:00")
    (row,) = late
    assert [row["joins"], row["short_stays"], row["immature"]] == ["1", "1", "0"]  # the exit is before as_of: counted


def test_block_6a_serial_counts_only_exits_before_as_of(cases_db, cases):
    with psycopg.connect(cases_db) as c:
        quick = sql.run(c, "6a", as_of="2026-11-12T12:00:00+00:00", scope=[-1003000000020], since="2026-11-01")
    assert quick.rows == []


def test_block_15_leaves_after_as_of_are_not_counted_and_the_post_is_immature(cases):
    w = str(-1003000000030)
    early = cases.rows("post-leaves", "--among", w, "--since", "2026-11-01", as_of="2026-11-12T13:00:00+00:00")
    (row,) = early
    assert [row["leaves_after"], row["leaves_before"], row["immature"]] == ["1", "0", "true"]  # 12:00 yes, 14:00 not yet
    late = cases.rows("post-leaves", "--among", w, "--since", "2026-11-01", as_of="2026-11-13T00:00:00+00:00")
    (row,) = late
    assert [row["leaves_after"], row["immature"]] == ["2", "false"]


def test_block_10b_flow_applies_as_of_to_both_joins(cases):
    p, q = str(-1003000000040), str(-1003000000041)
    between = cases.run("flow", "--among", p, q, "--since", "2026-11-01", "--as-of", "2026-11-12T09:00:00+00:00", "--csv")
    assert between.code == 0 and between.out.strip() == "from_chat,to_chat,users,median_hours"  # B not joined yet
    after = cases.rows("flow", "--among", p, q, "--since", "2026-11-01", as_of="2026-11-12T12:00:00+00:00")
    assert [(r["from_chat"], r["to_chat"], r["users"], r["median_hours"]) for r in after] == [(p, q, "1", "2.00")]


def test_block_13_a_kick_after_as_of_is_not_counted(cases):
    now = cases.rows("raids", "--raid-min-joins", "5")
    assert pick(now, chat_id=-1001000000003)["kicked_1h"] == "5"
    then = cases.rows("raids", "--raid-min-joins", "5", as_of="2026-11-23T03:10:00+00:00")
    assert pick(then, chat_id=-1001000000003)["kicked_1h"] == "0"  # the kicks came at 03:20


def test_block_12_an_answer_after_as_of_leaves_the_request_pending(cases, cases_db):
    with psycopg.connect(cases_db) as c:
        resolved = c.execute("SELECT chat_id, resolved_at, requested_at FROM tg.join_request WHERE outcome = 'approved' "
                             "ORDER BY resolved_at LIMIT 1").fetchone()
    chat, resolved_at, requested_at = resolved
    week = (requested_at.astimezone(UTC).date() - dt.timedelta(days=requested_at.astimezone(UTC).weekday())).isoformat()
    after = pick(cases.rows("requests", "--among", str(chat), "--since", "2026-08-01"), week=week)
    before = pick(cases.rows("requests", "--among", str(chat), "--since", "2026-08-01",
                             as_of=(resolved_at - dt.timedelta(minutes=1)).isoformat()), week=week)
    assert int(after["approved"]) == int(before["approved"]) + 1 and int(before["pending"]) == int(after["pending"]) + 1
