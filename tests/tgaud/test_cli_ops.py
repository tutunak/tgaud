"""The commands that write: campaign add, link create (fake Bot API), probe and forget-user.

Each test works on its own copy of the fixture (clidb.fresh_db): the commands commit.
"""

import hashlib
import hmac
import io
import json
import logging

import psycopg
import pytest
from botapi_fake import FakeBotApi
from clidb import A, B, C, LINK_KEY, TOKEN, Builder, cli_for, fresh_db, report_db, run_rollups, ts  # noqa: F401

from tgaud import db, history
from tgaud.commands import campaigns, common

SECRET = "https://t.me/+Zq9SecretInviteToken"


def q(dsn, query, params=None):
    with psycopg.connect(dsn) as c:
        return c.execute(query, params).fetchall()


# ------------------------------------------------------------------------------------------------------
# Chat resolution
# ------------------------------------------------------------------------------------------------------


def test_chat_resolution_by_id_username_and_title(cli_for, report_db):
    cli = cli_for(report_db)
    for ref in (str(A), "@alphanews", "@AlphaNews", "Alpha news", "1000000001"):  # the bare id works too
        assert [r["chat_id"] for r in cli.rows("unique", "--among", ref) if r["chat_id"]] == [str(A)], ref
    unknown = cli.run("unique", "--among", "No such chat")
    assert unknown.code == 1 and "unknown chat 'No such chat'" in unknown.err and "Alpha news" in unknown.err


def test_resolve_chat_lists_the_candidates_of_an_ambiguous_title():
    chats = [common.Chat(-1001, "News", "a", True), common.Chat(-1002, "News", None, True), common.Chat(-1003, "Other", None, True)]
    with pytest.raises(common.CliError) as e:
        common.resolve_chat("News", chats)
    assert "ambiguous" in str(e.value) and "-1001 @a 'News'" in str(e.value) and "-1002 'News'" in str(e.value)
    assert common.resolve_chat("-1002", chats).id == -1002 and common.resolve_chat("@A", chats).id == -1001


def test_an_ambiguous_title_is_an_error_on_the_command_line(cli_for, fresh_db):
    with psycopg.connect(fresh_db) as c:
        Builder(c).chat(-1004000000001, "Alpha news")  # a second chat with the same title
        c.commit()
    cli = cli_for(fresh_db)
    out = cli.run("unique", "--among", "Alpha news")
    assert out.code == 1 and "ambiguous chat 'Alpha news'" in out.err
    assert str(A) in out.err and "-1004000000001" in out.err
    assert cli.run("unique", "--among", "@alphanews").code == 0  # by username it is one chat
    with psycopg.connect(fresh_db) as c:
        Builder(c).chat(-1004000000002, "Quiet", tracked=False)
        c.commit()
    assert "is not tracked" in cli.run("unique", "--among", "Quiet").err


# ------------------------------------------------------------------------------------------------------
# campaign add
# ------------------------------------------------------------------------------------------------------


def test_campaign_add_stores_an_owner_annotation_with_its_chats(cli_for, fresh_db):
    cli = cli_for(fresh_db)
    out = cli.run("campaign", "add", "Spring ad", "--chat", "Alpha news", "@alphanews", str(B), "--starts", "2026-12-01",
                  "--draw", "2026-12-08T12:00:00+00:00", "--kind", "ad", "--only-new-members", "--winners", "2", "--note", "n")
    assert out.code == 0, out.err
    (row,) = q(fresh_db, "SELECT name, kind, origin, starts_at, draw_at, only_new_members, winner_count, note "
                         "FROM tg.campaign WHERE name = 'Spring ad'")
    assert row == ("Spring ad", "ad", "annotation", ts("2026-12-01 00:00"), ts("2026-12-08 12:00"), True, 2, "n")
    assert [r[0] for r in q(fresh_db, "SELECT chat_id FROM tg.campaign_chat cc JOIN tg.campaign c USING (campaign_id) "
                                      "WHERE c.name = 'Spring ad' ORDER BY chat_id")] == [B, A]  # A once, though named twice


def test_campaign_add_refuses_a_bad_window_or_chat_and_stores_nothing(cli_for, fresh_db):
    cli = cli_for(fresh_db)
    before = q(fresh_db, "SELECT count(*) FROM tg.campaign")
    assert cli.run("campaign", "add", "X", "--chat", "Alpha news", "--starts", "2026-12-08", "--draw", "2026-12-01").code == 1
    assert cli.run("campaign", "add", "X", "--chat", "Alpha news", "Nowhere", "--starts", "2026-12-01", "--draw", "2026-12-08").code == 1
    with pytest.raises(SystemExit):  # an observation kind cannot be entered by hand: argparse refuses it
        cli.run("campaign", "add", "X", "--chat", "Alpha news", "--starts", "2026-12-01", "--draw", "2026-12-08",
                "--kind", "tg_giveaway")
    assert q(fresh_db, "SELECT count(*) FROM tg.campaign") == before


# ------------------------------------------------------------------------------------------------------
# link create
# ------------------------------------------------------------------------------------------------------


@pytest.fixture
def fake(monkeypatch):
    f = FakeBotApi(token=TOKEN, bot_id=5000)
    monkeypatch.setattr(campaigns, "make_api", lambda cfg: f.api())
    return f


def link_result(**more):
    return {"invite_link": SECRET, "name": "G1 spring", "creator": {"id": 5000, "is_bot": True, "first_name": "bot"},
            "creates_join_request": False, "is_primary": False, "is_revoked": False, **more}


def test_link_create_prints_the_link_once_and_stores_only_its_digest(cli_for, fresh_db, fake, caplog):
    fake.ok("createChatInviteLink", link_result())
    cli = cli_for(fresh_db)
    with caplog.at_level(logging.DEBUG):
        out = cli.run("link", "create", "--chat", "Alpha news", "--name", "G1 spring", "--campaign", "1")
    assert out.code == 0, out.err
    assert out.out.splitlines() == [SECRET]  # printed once, alone on stdout
    assert SECRET not in out.err and SECRET not in caplog.text
    assert fake.calls_of("createChatInviteLink")[-1].params == {"chat_id": A, "name": "G1 spring"}
    rows = q(fresh_db, "SELECT to_jsonb(l) FROM tg.invite_link l WHERE name = 'G1 spring'")
    assert len(rows) == 1
    row = rows[0][0]
    digest = hmac.new(bytes.fromhex(LINK_KEY), SECRET.encode(), hashlib.sha256).digest().hex()
    assert row["link_digest"] == "\\x" + digest and row["masked_link"] is None and row["made_by_bot"] is True
    assert (row["chat_id"], row["campaign_id"], row["creator_id"]) == (A, 1, 5000)
    with psycopg.connect(fresh_db) as c:  # no text column of the table holds the link or any piece of it
        dump = c.execute("SELECT string_agg(l::text, ' ') FROM tg.invite_link l").fetchone()[0]
    assert SECRET not in dump and "Zq9Secret" not in dump and SECRET.removeprefix("https://") not in dump
    assert not any(isinstance(v, str) and "Zq9" in v for v in row.values())


def test_link_create_refuses_what_it_cannot_attribute_and_stores_nothing(cli_for, fresh_db, fake, monkeypatch):
    cli = cli_for(fresh_db)
    before = q(fresh_db, "SELECT count(*) FROM tg.invite_link")
    fake.error("createChatInviteLink", 400, "Bad Request: not enough rights")
    failed = cli.run("link", "create", "--chat", "Alpha news", "--name", "N")
    assert failed.code == 1 and "createChatInviteLink failed" in failed.err and failed.out == ""
    assert cli.run("link", "create", "--chat", "Alpha news", "--name", "x" * 33).code == 1
    assert cli.run("link", "create", "--chat", "Alpha news", "--name", "N", "--campaign", "99").code == 1
    with psycopg.connect(fresh_db) as c:
        c.execute("UPDATE tg.bot_state SET link_key_fingerprint = 'another key'")
        c.commit()
    wrong_key = cli.run("link", "create", "--chat", "Alpha news", "--name", "N")
    assert wrong_key.code == 1 and "LINK_HMAC_KEY" in wrong_key.err
    assert q(fresh_db, "SELECT count(*) FROM tg.invite_link") == before
    assert len(fake.calls_of("createChatInviteLink")) == 1  # only the first attempt reached Telegram


def test_link_create_prints_the_link_even_when_it_cannot_be_stored(cli_for, fresh_db, fake):
    fake.ok("createChatInviteLink", link_result(name="G"))
    out = cli_for(fresh_db).run("link", "create", "--chat", "Alpha news", "--name", "G", "--campaign", "1")
    assert out.code == 0
    fake.ok("createChatInviteLink", link_result(invite_link=SECRET))  # the same link twice: the digest is unique
    again = cli_for(fresh_db).run("link", "create", "--chat", "Alpha news", "--name", "G")
    assert again.code == 1 and again.out.strip() == SECRET and "could not be stored" in again.err


# ------------------------------------------------------------------------------------------------------
# probe
# ------------------------------------------------------------------------------------------------------


def test_probe_queues_a_manual_probe_and_replaces_a_pending_one(cli_for, fresh_db):
    cli = cli_for(fresh_db)
    out = cli.run("probe", "Alpha news", "@user1004")
    assert out.code == 0, out.err
    (probe,) = q(fresh_db, "SELECT chat_id, user_id, reason, outcome, snapshot_id FROM tg.probe "
                           "WHERE user_id = 1004 AND outcome = 'pending'")
    assert probe == (A, 1004, "manual", "pending", None)
    assert "already queued" in cli.run("probe", str(A), "1004").out  # the second call changes nothing
    assert q(fresh_db, "SELECT count(*) FROM tg.probe WHERE user_id = 1004") == [(1,)]
    with psycopg.connect(fresh_db) as c:
        c.execute("INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (%s, 1005, 'outage')", (A,))
        c.commit()
    replaced = cli.run("probe", str(A), "1005")
    assert replaced.code == 0 and "replacing outage" in replaced.out
    assert q(fresh_db, "SELECT reason, outcome FROM tg.probe WHERE user_id = 1005 ORDER BY probe_id") == [
        ("outage", "cancelled"), ("manual", "pending")]
    unseen = cli.run("probe", str(A), "424242")  # someone the service has not seen: a profile row is made
    assert unseen.code == 0 and q(fresh_db, "SELECT count(*) FROM tg.tg_user WHERE user_id = 424242") == [(1,)]
    assert cli.run("probe", "Nowhere", "1004").code == 1 and cli.run("probe", str(A), "@nobody").code == 1


# ------------------------------------------------------------------------------------------------------
# forget-user
# ------------------------------------------------------------------------------------------------------

U = 7777000123  # a person who is both a subject (joins, stays, requests) and an actor (kicks, approves, links)
V = 8888000321  # another member: kicked by U


def build_person(dsn):
    """U in A (open) and B (closed); V in B kicked by U; every secondary reference to U; raw payloads; a rollup run."""
    with psycopg.connect(dsn) as c:
        b = Builder(c)
        b.stay(A, U, ts("2026-11-15 10:00"), update_id=50001)
        b.stay(B, U, ts("2026-11-16 10:00"), ts("2026-11-18 10:00"), update_id=50011)
        b.stay(B, V, ts("2026-11-17 09:00"), ts("2026-11-19 10:00"), update_id=50021, left_update_id=50022, reason="kicked",
               actor=U)
        # U acted: kicked V (above), approved a join request, created two links (one merges into a twin on erasure)
        c.execute("INSERT INTO tg.join_request (chat_id, user_id, requested_at, outcome, resolved_at, resolved_by) "
                  "VALUES (%s, %s, '2026-11-17 08:00+00', 'approved', '2026-11-17 09:00+00', %s)", (B, V, U))
        c.execute("INSERT INTO tg.invite_link (chat_id, masked_link, creator_id, name) VALUES (%s, 'https://t.me/+Mm…', %s, 'mine')", (B, U))
        c.execute("INSERT INTO tg.invite_link (chat_id, masked_link, name) VALUES (%s, 'https://t.me/+Mm…', 'twin')", (B,))
        c.execute("INSERT INTO tg.invite_link (chat_id, masked_link, creator_id, name) VALUES (%s, 'https://t.me/+Oo…', %s, 'only mine')", (A, U))
        mine = c.execute("SELECT invite_link_id FROM tg.invite_link WHERE name = 'mine'").fetchone()[0]
        c.execute("UPDATE tg.member_event SET invite_link_id = %s WHERE user_id = %s", (mine, V))
        c.execute("UPDATE tg.membership SET invite_link_id = %s WHERE user_id = %s", (mine, V))
        # evidence of U
        snap = c.execute("INSERT INTO tg.snapshot (chat_id, started_at, method) VALUES (%s, '2026-11-20 00:00+00', 'listing') "
                         "RETURNING snapshot_id", (B,)).fetchone()[0]
        c.execute("INSERT INTO tg.snapshot_member (snapshot_id, user_id, seen_at, inviter_id) VALUES "
                  "(%s, %s, '2026-11-20 00:01+00', NULL), (%s, 1003, '2026-11-20 00:01+00', %s)", (snap, U, snap, U))
        c.execute("UPDATE tg.snapshot SET finished_at = '2026-11-20 00:30+00', fetched = 2, complete = true WHERE snapshot_id = %s", (snap,))
        c.execute("INSERT INTO tg.sighting (chat_id, user_id, first_seen_at, first_snapshot_id, last_seen_at, last_snapshot_id) "
                  "VALUES (%s, %s, '2026-11-20 00:01+00', %s, '2026-11-20 00:01+00', %s)", (B, U, snap, snap))
        c.execute("INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (%s, %s, 'manual')", (B, U))
        c.execute("INSERT INTO tg.activity_daily (chat_id, user_id, day, messages) VALUES (%s, %s, '2026-11-16', 3)", (B, U))
        for update_id, subject, payload in [
                (60001, U, {"from": {"id": U}, "chat": {"id": B}, "new_chat_member": {"user": {"id": U}}}),
                (60002, V, {"from": {"id": U, "first_name": "U"}, "chat": {"id": B}, "new_chat_member": {"user": {"id": V}}}),
                (60003, 1003, {"from": {"id": 1003}, "chat": {"id": B}, "new_chat_member": {"user": {"id": 1003}}})]:
            c.execute("INSERT INTO tg.raw_update (update_id, update_date, update_type, chat_id, user_id, payload) "
                      "VALUES (%s, '2026-11-22 10:00+00', 'chat_member', %s, %s, %s::jsonb)",
                      (update_id, B, subject, json.dumps(payload)))
        for chat in (A, B):
            c.execute("SELECT tg.mark_dirty(%s, '2026-11-15')", (chat,))
        c.commit()
    run_rollups(dsn)


def ids_found(dsn, needle: int) -> list[str]:
    """Every column of the tg and stats tables (and the materialized view) that holds the number: bigint equal,
    arrays containing it, text and jsonb containing its digits."""
    hits = []
    with psycopg.connect(dsn) as c:
        cols = c.execute(
            "SELECT n.nspname, cl.relname, a.attname, t.typname FROM pg_attribute a "
            "JOIN pg_class cl ON cl.oid = a.attrelid JOIN pg_namespace n ON n.oid = cl.relnamespace "
            "JOIN pg_type t ON t.oid = a.atttypid "
            "WHERE n.nspname IN ('tg', 'stats') AND cl.relkind IN ('r', 'p', 'm') AND NOT cl.relispartition "
            "AND a.attnum > 0 AND NOT a.attisdropped").fetchall()
        for schema, table, col, typ in cols:
            ref = f'"{schema}"."{table}"'
            if typ in ("int8", "int4", "numeric"):
                where = f'"{col}" = %s'
            elif typ in ("_int8", "_int4"):
                where = f'%s = ANY("{col}")'
            elif typ in ("text", "varchar", "jsonb"):
                where = f'"{col}"::text LIKE %s'
            else:
                continue
            arg = f"%{needle}%" if typ in ("text", "varchar", "jsonb") else needle
            if c.execute(f"SELECT 1 FROM {ref} WHERE {where} LIMIT 1", (arg,)).fetchone():
                hits.append(f"{schema}.{table}.{col}")
    return hits


def v_state(dsn):
    return (q(dsn, "SELECT start_source, join_route, known_from, end_source, end_reason, left_at FROM tg.membership "
                   "WHERE user_id = %s ORDER BY known_from", (V,)),
            q(dsn, "SELECT kind, event_at, actor_is_other, old_status, new_status FROM tg.member_event WHERE user_id = %s "
                   "ORDER BY event_at", (V,)))


def snapshot_of_rows(dsn):
    tables = ["tg.tg_user", "tg.membership", "tg.member_event", "tg.sighting", "tg.probe", "tg.snapshot_member",
              "tg.join_request", "tg.activity_daily", "tg.raw_update", "tg.invite_link", "tg.erasure"]
    with psycopg.connect(dsn) as c:
        return [c.execute(f"SELECT count(*), md5(string_agg(t::text, '|' ORDER BY t::text)) FROM {t} t").fetchone() for t in tables]


def test_forget_user_erases_the_person_everywhere_and_keeps_other_peoples_history(cli_for, fresh_db):
    build_person(fresh_db)
    assert len(ids_found(fresh_db, U)) > 8  # U is everywhere: subject rows, actor columns, payloads, tg.user_chats
    kicked_before = v_state(fresh_db)
    with psycopg.connect(fresh_db) as c:
        day_joins = c.execute("SELECT joins FROM stats.chat_day WHERE chat_id = %s AND day = '2026-11-15'", (A,)).fetchone()[0]
    assert day_joins >= 1
    out = cli_for(fresh_db).run("forget-user", str(U), "--yes")
    assert out.code == 0, out.err
    assert f"erased user {U}" in out.out and "audience rollup refreshed" in out.out
    assert ids_found(fresh_db, U) == []  # no table, no payload, no rollup holds the id any more
    # the other member: his kicked exit, who did it (actor_is_other) and his join route survive, and so does a rebuild
    assert v_state(fresh_db) == kicked_before
    (kick,) = [e for e in v_state(fresh_db)[1] if e[0] == "kick"]
    assert kick[2] is True
    assert q(fresh_db, "SELECT actor_id FROM tg.member_event WHERE user_id = %s AND kind = 'kick'", (V,)) == [(None,)]
    with psycopg.connect(fresh_db) as c:
        history.lock_chats(c, [B])
        history.rebuild_pairs(c, [(B, V)])
        c.commit()
    assert v_state(fresh_db) == kicked_before
    # the twin link took over the references of the link that lost its creator; the other link lost only its creator
    assert q(fresh_db, "SELECT count(*) FROM tg.invite_link WHERE masked_link = 'https://t.me/+Mm…'") == [(1,)]
    assert {n for (n,) in q(fresh_db, "SELECT i.name FROM tg.member_event e JOIN tg.invite_link i USING (invite_link_id) "
                                     "WHERE e.user_id = %s", (V,))} == {"twin"}
    assert q(fresh_db, "SELECT i.name FROM tg.membership m JOIN tg.invite_link i USING (invite_link_id) WHERE m.user_id = %s", (V,)) == [("twin",)]
    assert q(fresh_db, "SELECT creator_id FROM tg.invite_link WHERE name = 'only mine'") == [(None,)]
    assert q(fresh_db, "SELECT resolved_by, outcome FROM tg.join_request WHERE user_id = %s", (V,)) == [(None, "approved")]
    assert q(fresh_db, "SELECT update_id FROM tg.raw_update WHERE update_id IN (60001, 60002, 60003)") == [(60003,)]  # only the one without U stays
    # the person is recorded as erased (a hash, a time), so old evidence is dropped
    assert q(fresh_db, "SELECT count(*) FROM tg.erasure WHERE tg.is_erased(%s, now() - interval '1 minute')", (U,)) == [(1,)]
    assert q(fresh_db, "SELECT count(*) FROM tg.erasure WHERE tg.is_erased(%s, now() + interval '1 minute')", (U,)) == [(0,)]
    # no query and no later R2 brings the id back; the day U joined is recomputed
    assert cli_for(fresh_db).rows("user", str(U)) == []
    assert all(str(U) not in r["user_id"] for r in cli_for(fresh_db).rows("asof", "Alpha chat", "2026-11-17", "--list"))
    run_rollups(fresh_db)
    assert ids_found(fresh_db, U) == []
    assert q(fresh_db, "SELECT joins FROM stats.chat_day WHERE chat_id = %s AND day = '2026-11-15'", (A,)) == [(day_joins - 1,)]
    assert cli_for(fresh_db).run("forget-user", str(U), "--yes").code == 0  # repeating it is safe


def test_forget_user_without_yes_changes_nothing(cli_for, fresh_db, monkeypatch):
    build_person(fresh_db)
    before = snapshot_of_rows(fresh_db)
    cli = cli_for(fresh_db)
    for answer in ("n\n", "\n", "", "maybe\n"):
        monkeypatch.setattr("sys.stdin", io.StringIO(answer))
        out = cli.run("forget-user", str(U))
        assert out.code == 1 and "not confirmed" in out.err and f"Erase user {U}" in out.err
    assert snapshot_of_rows(fresh_db) == before and ids_found(fresh_db, U)
    monkeypatch.setattr("sys.stdin", io.StringIO("y\n"))
    assert cli.run("forget-user", str(U)).code == 0 and ids_found(fresh_db, U) == []


def test_forget_user_needs_the_service_stopped_and_no_unfinished_snapshot(cli_for, fresh_db):
    build_person(fresh_db)
    before = snapshot_of_rows(fresh_db)
    cli = cli_for(fresh_db)
    with psycopg.connect(fresh_db) as service:  # `tgaud serve` holds the service lock
        assert db.try_serve_lock(service)
        blocked = cli.run("forget-user", str(U), "--yes")
    assert blocked.code == 1 and "stop the service first" in blocked.err
    assert snapshot_of_rows(fresh_db) == before
    with psycopg.connect(fresh_db) as c:
        c.execute("INSERT INTO tg.snapshot (chat_id, started_at, method) VALUES (%s, '2026-11-23 00:00+00', 'listing')", (C,))
        c.commit()
    unfinished = cli.run("forget-user", str(U), "--yes")
    assert unfinished.code == 1 and "snapshot is still being written" in unfinished.err
    assert snapshot_of_rows(fresh_db) == before  # the function raised: the erasure record rolled back with the rest

