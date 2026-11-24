"""The report commands of the `tgaud` CLI on the history fixture (docs/design/verify.md 4).

"Now" is 2026-11-23 12:00 UTC (--as-of), R1 and R2 have run (clidb.report_db). Every command is checked by the
numbers verify.md expects for its block, read back from --csv; the formatting is not asserted, except that the
header names the as-of time, the chats, the coverage and the gap flags, and goes to stderr in CSV mode.
"""

import csv
import datetime as dt
import io

import psycopg
import pytest
from clidb import A, AD, B, C, D, NOW, NOW_ARG, UTC  # noqa: F401 (fixtures: report_db, cli_for)
from clidb import cli_for, report_db  # noqa: F401

from tgaud import sql
from tgaud.commands import common
from tgaud.commands.reports import PLAIN

AB = [str(A), str(B)]
ADS = [str(c) for c in AD]


@pytest.fixture
def cli(cli_for, report_db):
    return cli_for(report_db)


def pick(rows, **want):
    found = [r for r in rows if all(r[k] == str(v) for k, v in want.items())]
    assert len(found) == 1, (want, found)
    return found[0]


def nums(row, *cols):
    return [row[c] for c in cols]


# ------------------------------------------------------------------------------------------------------
# Rollup reports (R2): overlap, unique, chats-per-user, discussion
# ------------------------------------------------------------------------------------------------------


def test_overlap_pairs_sizes_and_percentages(cli):
    rows = cli.rows("overlap", "--among", *ADS)
    shared = {(r["chat_a"], r["chat_b"]): r["shared"] for r in rows}
    for a, b, n in [(A, B, 4), (A, C, 2), (A, D, 1), (B, C, 3), (B, D, 1), (C, D, 1)]:
        assert shared[(str(a), str(b))] == shared[(str(b), str(a))] == str(n)
    assert [shared[(str(c), str(c))] for c in AD] == ["10", "6", "5", "2"]
    ab = pick(rows, chat_a=A, chat_b=B)
    assert nums(ab, "pct_of_a", "pct_of_b", "jaccard") == ["40.00", "66.67", "0.3333"]
    assert ab["coverage_a"] == "80.00"  # 12 of 15 reported


def test_unique_exclusive_per_chat_and_distinct_total(cli):
    rows = cli.rows("unique", "--among", *ADS)
    assert {r["chat_id"]: r["exclusive"] for r in rows if r["chat_id"]} == {str(A): "6", str(B): "1", str(C): "2", str(D): "1"}
    assert pick(rows, chat_id="")["members"] == "15"
    rows = cli.rows("unique", "--among", *AB)
    assert {r["chat_id"]: r["exclusive"] for r in rows if r["chat_id"]} == {str(A): "6", str(B): "2"}
    assert pick(rows, chat_id="")["members"] == "12"


def test_chats_per_user_distribution(cli):
    everyone = cli.rows("chats-per-user")
    assert {r["chat_count"]: r["users"] for r in everyone} == {"1": "20", "2": "3", "3": "1", "4": "1"}
    assert sum(int(r["users"]) for r in everyone) == 25
    scoped = cli.rows("chats-per-user", "--among", *ADS)
    assert {r["chat_count"]: r["users"] for r in scoped} == {"1": "10", "2": "3", "3": "1", "4": "1"}


def test_discussion_channel_against_its_group(cli):
    row = pick(cli.rows("discussion"), channel_id=A)
    assert nums(row, "subscribers", "group_members", "in_both", "pct_subscribers_in_group", "group_only",
                "active_in_group", "active_subscribers") == ["10", "6", "4", "40.00", "2", "3", "3"]


# ------------------------------------------------------------------------------------------------------
# One user, the as-of membership
# ------------------------------------------------------------------------------------------------------


def test_user_by_id_and_by_username(cli):
    rows = cli.rows("user", "1001")
    assert {r["chat_id"] for r in rows if r["now"] == "in"} == set(ADS)
    assert rows[0]["chats_now"] == "4"
    assert sum(r["start_source"] == "snapshot" for r in rows) == 2 and sum(r["start_source"] == "event" for r in rows) == 2
    assert sum(r["chat_id"] == str(A) for r in cli.rows("user", "1006")) == 2
    assert cli.rows("user", "@user1006") == cli.rows("user", "1006")
    assert cli.run("user", "@nobody").code == 1


def test_asof_counts_list_day_end_and_matrix(cli):
    a = cli.rows("asof", "Alpha news", "2026-11-07")
    assert nums(a[0], "known_in", "unsure", "by_telegram_date") == ["11", "0", "0"]
    before_bot = cli.rows("asof", str(A), "2026-07-15")
    assert nums(before_bot[0], "known_in", "by_telegram_date") == ["0", "3"] and before_bot[0]["flags"] == "not covered"
    b = cli.rows("asof", "@alphanews", "2026-11-07", "--among", str(B))  # --among is not used by the counts
    assert b[0]["known_in"] == "11"
    b = cli.rows("asof", "Alpha chat", "2026-11-07")
    assert nums(b[0], "known_in", "unsure") == ["6", "1"]
    people = cli.rows("asof", "Alpha chat", "2026-11-07", "--list")
    assert sorted((r["state"], r["user_id"]) for r in people if r["state"] == "unknown") == [("unknown", "1019")]
    assert sum(r["state"] == "in" for r in people) == 6
    matrix = cli.rows("asof", "Alpha news", "2026-11-07", "--matrix", "--among", *ADS)
    got = {(r["chat_a"], r["chat_b"]): r["shared"] for r in matrix}
    assert [got[(str(c), str(c))] for c in AD] == ["11", "6", "5", "4"]
    assert [got[(str(a_), str(b_))] for a_, b_ in [(B, A), (C, A), (C, B), (D, A), (D, B), (D, C)]
            if (str(a_), str(b_)) in got] == ["4", "2", "3", "1", "1", "1"]
    eod = cli.rows("asof", "Alpha chat", "2026-11-07", "--eod")
    assert nums(eod[0], "members_eod", "unsure_eod") == ["7", "1"]
    assert cli.rows("asof", "Alpha news", "2026-11-07", "--eod")[0]["members_eod"] == "12"


# ------------------------------------------------------------------------------------------------------
# Windowed reports
# ------------------------------------------------------------------------------------------------------


def test_growth_corrections_joins_rejoins_and_the_residual(cli):
    rows = cli.rows("growth", "--among", *AB, "--since", "2026-08-01")
    assert pick(rows, chat_id=A, day="2026-08-01")["corrections_in"] == "5"
    assert nums(pick(rows, chat_id=A, day="2026-11-05"), "joins", "exits") == ["1", "1"]
    assert pick(rows, chat_id=A, day="2026-11-03")["rejoins"] == "1"
    assert not [r for r in rows if r["day"] == "2026-11-23"]  # the day of as_of (12:00) is not over: left out
    after = cli.rows("growth", "--among", *AB, "--since", "2026-08-01", as_of="2026-11-24T00:00:00+00:00")
    assert nums(pick(after, chat_id=A, day="2026-11-23"), "reported_eod", "known_eod", "residual") == ["15", "12", "1"]
    assert pick(rows, chat_id=B, day="2026-11-13")["corrections_out"] == "1"
    assert pick(rows, chat_id=B, day="2026-11-20")["messages"] == "9"
    out = cli.run("growth", "--among", *AB, "--since", "2026-11-01", "--as-of", NOW_ARG)
    assert out.code == 0, out.err
    head = out.out.partition("\n\n")[0]
    assert "window 2026-11-01 00:00:00 UTC .. 2026-11-23 00:00:00 UTC" in head
    assert "days that ended by as_of only: through 2026-11-22; 2026-11-23, the day of as_of, is not over" in head
    assert "short stays from 2026-11-21 on are withheld" in head


def test_asof_eod_reports_a_day_only_once_it_has_ended(cli):
    assert cli.rows("asof", "Alpha news", "2026-11-23", "--eod") == []  # as of 11-23 12:00
    out = cli.run("asof", "Alpha news", "2026-11-23", "--eod", "--as-of", NOW_ARG)
    assert out.code == 0 and "2026-11-23 has not ended at as_of: no end-of-day count" in out.out
    ended = cli.rows("asof", "Alpha news", "2026-11-22", "--eod")
    assert len(ended) == 1 and ended[0]["day"] == "2026-11-22"
    assert "has not ended" not in cli.run("asof", "Alpha news", "2026-11-22", "--eod", "--as-of", NOW_ARG).out


def test_short_stays_outside_campaigns(cli):
    rows = cli.rows("short-stays", "--among", *ADS, "--since", "2026-08-01")
    assert nums(pick(rows, chat_id=A, week="2026-10-19"), "joins", "short_stays") == ["2", "1"]
    assert nums(pick(rows, chat_id=A, week="2026-11-02"), "campaign_joins", "free_joins") == ["6", "0"]
    assert nums(pick(rows, chat_id=B, week="2026-10-19"), "joins", "short_stays") == ["2", "2"]
    assert nums(pick(rows, chat_id=C, week="2026-11-23"), "joins", "short_stays", "kicked") == ["6", "6", "5"]
    assert pick(rows, chat_id=A, week="2026-08-10")["flags"] == "gap:service_down"


def test_serial_short_stayers_and_bursts(cli):
    rows = cli.rows("serial", "--among", *ADS, "--since", "2026-08-01")
    assert [(r["user_id"], r["short_stays"], r["chats"]) for r in rows] == [("1022", "2", "2")]
    bursts = cli.rows("bursts", "--among", *ADS, "--since", "2026-08-01", "--burst-users", "2")
    assert [({r["chat_a"], r["chat_b"]}, r["users"], r["bucket"]) for r in bursts] == [
        ({str(A), str(B)}, "2", "2026-10-20T10:00:00+00:00")]


def test_tenure_median_p90_and_share_within_h(cli):
    rows = cli.rows("tenure", "--among", *ADS)
    assert nums(pick(rows, chat_id=A), "exits", "median_hours", "p90_hours", "share_within_h") == [
        "7", "24.00", "201.60", "0.5714"]
    assert pick(rows, chat_id=B)["median_hours"] == "22.97"
    assert pick(rows, chat_id=C)["median_hours"] == "0.33"
    assert nums(pick(rows, chat_id=D), "median_hours", "p90_hours") == ["1573.00", "1585.00"]


def test_cohorts_retention_at_7_30_and_90_days(cli):
    rows = cli.rows("cohorts", "--among", *AB, "--since", "2026-08-01")
    assert nums(pick(rows, chat_id=A, cohort_week="2026-11-02", days=7), "retained", "exited") == ["2", "4"]
    assert nums(pick(rows, chat_id=A, cohort_week="2026-10-19", days=7), "retained", "exited") == ["1", "1"]
    assert nums(pick(rows, chat_id=A, cohort_week="2026-10-19", days=30), "retained", "exited") == ["0", "2"]
    assert nums(pick(rows, chat_id=A, cohort_week="2026-08-31", days=30), "retained", "exited") == ["1", "1"]
    assert nums(pick(rows, chat_id=A, cohort_week="2026-08-31", days=90), "retained", "immature") == ["0", "2"]
    assert nums(pick(rows, chat_id=B, cohort_week="2026-11-02", days=7), "retained", "exited", "unknown", "retention_pct") == [
        "1", "1", "1", "50.0"]


def test_sources_per_link_and_unattributed(cli):
    rows = cli.rows("sources", "--among", str(A), "--since", "2026-08-01")
    bot_link = [r for r in rows if r["made_by_bot"] == "true"]
    assert [nums(r, "joins", "retained_7d", "exited_7d") for r in bot_link] == [["6", "2", "4"]]
    plain = pick(rows, join_route="unattributed", invite_link_id="")
    assert nums(plain, "joins", "retained_7d", "exited_7d", "retained_30d", "exited_30d") == ["4", "3", "1", "1", "3"]
    assert {r["join_route"] for r in rows} >= {"added", "folder", "link", "request", "unattributed"}


def test_rejoiners_flow_requests_raids_activity_and_post_leaves(cli):
    rows = cli.rows("rejoiners", "--since", "2026-08-01")
    by_chat = {r["chat_id"]: r["rejoins"] for r in rows}
    assert by_chat[str(A)] == "1" and {by_chat[str(-1002000000000 - n)] for n in (3, 7, 12)} == {"1"}
    assert str(-1002000000013) not in by_chat
    flow = cli.rows("flow", "--since", "2026-08-01")
    assert [(r["from_chat"], r["to_chat"], r["users"]) for r in flow] == [(str(A), str(B), "5"), (str(C), str(D), "1")]
    assert [r["median_hours"] for r in flow] == ["0.08", "122.00"]
    req = cli.rows("requests", "--among", str(A), "--since", "2026-08-01")
    assert nums(pick(req, week="2026-10-05"), "approved", "median_minutes_to_approve", "approved_retained_30d") == [
        "1", "60.0", "1"]
    assert pick(req, week="2026-10-26")["unknown"] == "1" and pick(req, week="2026-11-16")["pending"] == "1"
    raids = cli.rows("raids", "--raid-min-joins", "5")
    assert [nums(r, "chat_id", "joins", "kicked_1h", "pct_kicked_1h") for r in raids] == [[str(C), "6", "5", "83.3"]]
    assert raids[0]["minute"] == "2026-11-23T03:00:00+00:00"
    act = cli.rows("activity", "--among", *ADS)
    assert nums(pick(act, chat_id=B), "members", "active", "short_stays", "short_stays_who_wrote") == ["6", "3", "3", "1"]
    assert nums(pick(act, chat_id=C), "members", "active", "short_stays", "short_stays_who_wrote") == ["5", "1", "7", "1"]
    posts = cli.rows("post-leaves", "--among", *ADS)
    assert [nums(r, "message_id", "leaves_after", "leaves_before", "immature") for r in posts] == [
        ["10", "1", "1", "false"], ["11", "0", "0", "false"]]


def test_drift_samples_of_the_last_24_hours(cli):
    rows = [r for r in cli.rows("drift", "--among", str(A))]
    assert [r["mismatch"] for r in rows] == ["2", "2", "2", "3"]
    assert [r["residual"] for r in rows] == ["0", "0", "0", "1"]


def test_coverage_known_against_reported_and_gaps(cli):
    row = pick(cli.rows("coverage", "--among", str(A)), chat_id=A)
    assert nums(row, "reported", "known_open", "suspect_open", "coverage_pct", "bot_status") == [
        "15", "12", "0", "80.0", "administrator"]
    assert row["admin_since"] == "2026-08-01T00:00:00+00:00"
    gaps = cli.rows("coverage", "--gaps", "--among", str(A), "--since", "2026-08-01")
    assert [(r["chat_id"], r["reason"]) for r in gaps] == [("", "service_down")]  # one gap for every chat
    case11 = pick(cli.rows("coverage", "--among", "case 11 failed probe"), chat_id=-1002000000011)
    assert case11["probes_pending"] == "0"


# ------------------------------------------------------------------------------------------------------
# Campaign reports (item 4)
# ------------------------------------------------------------------------------------------------------


def test_campaign_report_per_chat_per_person_and_entrants(cli):
    rows = cli.rows("campaign", "report", "1")
    a = pick(rows, chat_id=A)
    assert nums(a, "entrants", "first_time", "returners", "eligible", "quick_exit", "in_at_draw", "exit_after_draw",
                "still_in") == ["6", "5", "1", "5", "3", "4", "2", "2"]
    b = pick(rows, chat_id=B)
    assert nums(b, "entrants", "quick_exit", "quick_exit_kicked", "quick_exit_unknown", "in_at_draw", "in_at_draw_unknown",
                "exit_after_draw_unknown", "still_in") == ["3", "1", "1", "1", "1", "1", "1", "1"]
    c = pick(cli.rows("campaign", "report", "2"), chat_id=C)
    assert nums(c, "entrants", "quick_exit", "in_at_draw", "still_in", "eligible") == ["2", "1", "1", "1", "2"]
    (people,) = cli.rows("campaign", "report", "1", "--people")
    assert nums(people, "persons", "multi_chat", "first_time", "eligible", "quick_exit", "in_at_draw", "exit_after_draw",
                "still_in") == ["8", "1", "7", "7", "4", "5", "2", "3"]
    entrants = cli.rows("campaign", "report", "1", "--entrants")
    assert len(entrants) == 9 and sum(r["first_time"] == "false" for r in entrants) == 1
    assert pick(entrants, chat_id=A, user_id=1016)["quick_exit"] == "yes"


def test_campaign_list_shows_both_campaigns_with_their_chats(cli):
    rows = cli.rows("campaign", "list")
    assert [(r["campaign_id"], r["origin"], r["only_new_members"]) for r in rows] == [
        ("1", "annotation", "true"), ("2", "observation", "true")]
    assert "Alpha news" in rows[0]["chats"] and "Alpha chat" in rows[0]["chats"] and "Gamma group" in rows[1]["chats"]
    assert cli.run("campaign", "report", "99").code == 1


# ------------------------------------------------------------------------------------------------------
# Output: header, tables, CSV
# ------------------------------------------------------------------------------------------------------


def test_header_names_the_as_of_time_the_chats_the_coverage_and_the_gap_flags(cli):
    out = cli.run("short-stays", "--among", "Alpha news", "--since", "2026-08-01", "--as-of", NOW_ARG)
    assert out.code == 0, out.err
    head, _, table = out.out.partition("\n\n")
    assert "as of 2026-11-23 12:00:00 UTC" in head
    assert "chats in scope: 1" in head and f"Alpha news ({A})" in head
    assert "known 12 of 15 reported (80.0 %)" in head and "suspect 0" in head
    assert "window flags: gap:service_down" in head  # the outage of 08-10 lies in the window
    assert table.startswith("chat_id") or "Alpha news" in table.splitlines()[0] or "chat_id" in table.splitlines()[0]
    assert f"Alpha news ({A})" in table  # the chat title beside its id


def test_table_aligns_numbers_to_the_right():
    text = common.render_table(["name", "n"], [("a", 5), ("bb", 120)])
    lines = text.splitlines()
    assert lines[2].endswith("  5") and lines[3].endswith("120") and lines[2].startswith("a ")


def test_csv_goes_to_stdout_alone_and_the_header_to_stderr(cli):
    out = cli.run("unique", "--among", *AB, "--as-of", NOW_ARG, "--csv")
    assert out.code == 0
    assert out.out.splitlines()[0] == "chat_id,title,members,exclusive,exclusive_pct,as_of"
    assert "as of 2026-11-23 12:00:00 UTC" in out.err and "chats in scope: 2" in out.err
    assert "chats in scope" not in out.out


def _canon(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dt.datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


@pytest.mark.parametrize("spec", PLAIN, ids=lambda s: s.name)
def test_csv_parses_back_to_the_rows_of_the_block(cli, report_db, spec):
    params = {"as_of": NOW_ARG}
    argv = [spec.name, "--as-of", NOW_ARG]
    if "since" in spec.params:
        params["since"] = "2026-08-01"
        argv += ["--since", "2026-08-01"]
    out = cli.run(*argv, "--csv")
    assert out.code == 0, out.err
    parsed = list(csv.reader(io.StringIO(out.out)))
    with psycopg.connect(report_db) as c:
        result = sql.run(c, spec.block, **params)
    assert parsed[0] == result.columns
    assert parsed[1:] == [[_canon(v) for v in row] for row in result.rows]
    assert len(parsed) > 1 or spec.name in ("raids", "bursts", "serial", "drift")  # an empty block still has its header


# ------------------------------------------------------------------------------------------------------
# Options and errors
# ------------------------------------------------------------------------------------------------------


def test_options_reach_the_block_and_bad_ones_are_refused(cli):
    few = cli.rows("bursts", "--since", "2026-08-01", "--burst-users", "2", "--burst-minutes", "10")
    many = cli.rows("bursts", "--since", "2026-08-01", "--burst-users", "3")
    assert len(few) == 1 and many == []
    with pytest.raises(SystemExit) as usage:  # argparse: usage error, status 2
        cli.run("tenure", "--hours", "x")
    assert usage.value.code == 2
    bad_time = cli.run("growth", "--since", "last tuesday")
    assert bad_time.code == 1 and "not a date or a time" in bad_time.err
    inverted = cli.run("growth", "--since", "2027-01-01", "--as-of", NOW_ARG)
    assert inverted.code == 1 and "after it ends" in inverted.err


def test_every_report_command_is_registered_under_its_design_name():
    from tgaud import cli as cli_mod

    cli_mod.build_parser()
    expected = {"overlap", "unique", "chats-per-user", "user", "growth", "campaign", "link", "short-stays", "serial",
                "bursts", "sources", "cohorts", "tenure", "rejoiners", "flow", "discussion", "requests", "raids",
                "activity", "post-leaves", "asof", "drift", "coverage", "probe", "forget-user"}
    assert expected <= set(cli_mod.COMMANDS)
