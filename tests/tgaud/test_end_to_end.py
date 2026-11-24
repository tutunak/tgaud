"""End to end: `tgaud serve` in process (its three threads: ingest, worker, batch) against the test database and
the fake Bot API (a real local HTTP server), driven through a scripted day; then the spells (design.md 4, the state
table), stats.chat_day, stats.overlap, the item-4 campaign report through the CLI, the drift numbers, /metrics,
/health and the connection budget (serve holds at most 3 connections in phase 1, read from pg_stat_activity).

The day lies in the last three hours (B = now - 3 h, scripted times are B + minutes), so update lags stay far
below the 20 h service_down suspicion and the 48 h of a short stay; the drift part (a join between two count
samples) is dated at the real time. What is not the real code: job intervals are shortened (chat_day and audience
every 2 s, the reconciler's idle look 0.5 s, the prober's empty-queue read 0.5 s, the metric cache 0.5 s), and
count samples are taken when the test asks for them (the sampler's slots are pushed to infinity; the test sets a
chat's slot to 0).

The script (chats: channel CH, its discussion group GR, basic group OG that becomes supergroup NG):
    B+0..2   the bot is made admin in CH, GR and OG (my_chat_member)
    B+10     snapshot 1 of CH (collect script, role collect_writer): U1, U2, U3 were there before the bot (S1)
             campaign "Nov giveaway" on CH, window B+30 .. B+150, and its named bot link (CLI)
    B+15     U9 joins OG                          B+20  U1 joins GR; U6 joins CH
    B+21     U8 joins GR                          B+25  U6 leaves CH (short stay)
    B+30..40 U1 writes twice in GR, U8 once       B+40  U4 joins CH through the bot link
    B+45     U5 joins CH through the bot link     B+65  a channel post in CH
    B+70     U4 leaves CH (quick exit)            B+80  U6 rejoins CH through the bot link (E2, a returner)
    B+90     U7 joins CH through the bot link     B+100 an admin bans U7 (E5, kicked)
    B+120    snapshot 2 of CH lists U1, U2, U5, U6: U3 is missing (A1); the probe gets a 429, then "left" (P3)
    B+130    OG migrates to NG (both service messages); B+131 U9's join echo in NG (E3); NG's rights check -> M1
    live     count sample 1 (Telegram says 5), U10 joins CH, count sample 2 (Telegram says 7: one missed join)
"""

from __future__ import annotations

import datetime as dt
import socket
import threading
import time
import urllib.error
import urllib.request

import psycopg
import psycopg.conninfo
import pytest
from botapi_fake import BOT_ID, Call, FakeBotServer, _err
from clidb import LINK_KEY, TOKEN, Cli
from prometheus_client.parser import text_string_to_metric_families

from tgaud import config, jobs, metrics
from tgaud.jobs import audience, chat_day, prober, reconciler, sampler

UTC = dt.timezone.utc
CH = {"id": -1007700000001, "title": "E2E channel", "type": "channel", "username": "e2e_channel"}
GR = {"id": -1007700000002, "title": "E2E discussion", "type": "supergroup"}
OG = {"id": -770000003, "title": "E2E old group", "type": "group"}
NG = {"id": -1007700000003, "title": "E2E old group", "type": "supergroup"}
ADMIN = 7700
U1, U2, U3, U4, U5, U6, U7, U8, U9, U10 = range(7701, 7711)
HINT = dt.datetime(2025, 3, 1, 9, 30, tzinfo=UTC)  # Telegram's participant date of the members from before the bot
BOT = {"id": BOT_ID, "is_bot": True, "first_name": "tgaud test", "username": "tgaud_test_bot"}
BOT_LINK = "https://t.me/+E2EcampaignLINKtoken01"
LINK_NAME = "nov-giveaway"
ACTIVATION = "e2e-activation-1"
SERVE_APPS = ("tgaud-ingest", "tgaud-worker", "tgaud-batch")


def ts(at: dt.datetime) -> int:
    return int(at.timestamp())


def person(uid: int) -> dict:
    return {"id": uid, "is_bot": False, "first_name": f"P{uid}", "username": f"p{uid}", "language_code": "en"}


def admin_rights() -> dict:
    return dict(can_be_edited=False, is_anonymous=False, can_manage_chat=True, can_delete_messages=True,
                can_manage_video_chats=True, can_restrict_members=True, can_promote_members=False,
                can_change_info=True, can_invite_users=True, can_post_stories=False, can_edit_stories=False,
                can_delete_stories=False, can_post_messages=True, can_edit_messages=True)


def bot_admin() -> dict:
    return {"user": BOT, "status": "administrator", **admin_rights()}


def bot_link() -> dict:
    return {"invite_link": BOT_LINK, "creator": BOT, "creates_join_request": False, "is_primary": False,
            "is_revoked": False, "name": LINK_NAME}


class Day:
    """Bot API updates of the scripted day, numbered in the order they are made."""

    def __init__(self, base: dt.datetime) -> None:
        self.base = base
        self.update_id = 500_000

    def at(self, minutes: float, seconds: float = 0) -> dt.datetime:
        return self.base + dt.timedelta(minutes=minutes, seconds=seconds)

    def _update(self, key: str, body: dict) -> dict:
        self.update_id += 1
        return {"update_id": self.update_id, key: body}

    def bot_added(self, chat: dict, minutes: float) -> dict:
        return self._update("my_chat_member", {
            "chat": chat, "from": person(ADMIN), "date": ts(self.at(minutes)),
            "old_chat_member": {"user": BOT, "status": "left"}, "new_chat_member": bot_admin()})

    def member(self, chat: dict, uid: int, when: float | dt.datetime, old: str, new: str, *, actor: int | None = None,
               link: dict | None = None) -> dict:
        at = when if isinstance(when, dt.datetime) else self.at(when)
        new_member = {"user": person(uid), "status": new}
        if new == "kicked":
            new_member["until_date"] = 0
        body = {"chat": chat, "from": person(actor or uid), "date": ts(at),
                "old_chat_member": {"user": person(uid), "status": old}, "new_chat_member": new_member}
        if link is not None:
            body["invite_link"] = link
        return self._update("chat_member", body)

    def message(self, chat: dict, minutes: float, mid: int, uid: int | None = None, **kw) -> dict:
        body = {"message_id": mid, "date": ts(self.at(minutes)), "chat": chat, **kw}
        if uid is not None:
            body["from"] = person(uid)
        return self._update("message", body)

    def post(self, chat: dict, minutes: float, mid: int) -> dict:
        return self._update("channel_post", {"message_id": mid, "date": ts(self.at(minutes)), "chat": chat,
                                             "sender_chat": chat, "text": "a post (never stored)"})


class Telegram(FakeBotServer):
    """The fake Bot API with the chats' answers: the bot is admin everywhere; getChatMember of a user answers
    from `status` (a 429 first for the pairs in `throttle_once`); getChatMemberCount from `counts`."""

    CHATS = {c["id"]: c for c in (CH, GR, OG, NG)}
    LINKED = {CH["id"]: GR["id"], GR["id"]: CH["id"]}

    def __init__(self) -> None:
        super().__init__(token=TOKEN, bot_id=BOT_ID)
        self.status: dict[tuple[int, int], str] = {}
        self.counts: dict[int, int] = {}
        self.throttle_once: set[tuple[int, int]] = set()
        self.respond("getChatMember", self._member)
        self.respond("getChat", self._chat)
        self.respond("getChatMemberCount", lambda p: self.counts[p["chat_id"]])
        self.respond("createChatInviteLink", lambda p: {**bot_link(), "name": p["name"]})

    def _next(self, method: str, params: dict, token: str):
        pair = (params.get("chat_id"), params.get("user_id"))
        if method == "getChatMember" and pair in self.throttle_once:
            with self._lock:
                self.throttle_once.discard(pair)
                self.calls.append(Call(method, params, token))
            return _err(429, "Too Many Requests: retry after 1", retry_after=1)
        return super()._next(method, params, token)

    def _member(self, p: dict) -> dict:
        if p["user_id"] == BOT_ID:
            return bot_admin()
        return {"user": person(p["user_id"]), "status": self.status.get((p["chat_id"], p["user_id"]), "left")}

    def _chat(self, p: dict) -> dict:
        chat = dict(self.CHATS[p["chat_id"]])
        if p["chat_id"] in self.LINKED:
            chat["linked_chat_id"] = self.LINKED[p["chat_id"]]
        return chat

    def member_calls(self, chat: int, user: int) -> int:
        return sum(1 for c in self.calls if c.method == "getChatMember"
                   and (c.params.get("chat_id"), c.params.get("user_id")) == (chat, user))


class Service:
    """`tgaud serve` (tgaud.jobs.serve) on a thread of this process, with a supervisor that is not bound to
    signals (pytest owns them) and never hard-exits; stop() asks for the stop the way SIGTERM does."""

    def __init__(self, cfg: config.Config) -> None:
        self.supervisor = jobs.Supervisor(deadline=10 ** 9)
        self.supervisor.thread.start()
        self.code: int | None = None
        self.thread = threading.Thread(target=self._run, args=(cfg,), name="e2e-serve", daemon=True)
        self.thread.start()

    def _run(self, cfg: config.Config) -> None:
        self.code = jobs.serve(cfg, self.supervisor)

    def alive(self) -> None:
        assert self.thread.is_alive(), f"tgaud serve ended early with exit code {self.code}"

    def stop(self, timeout: float = 10.0) -> float:
        started = time.monotonic()
        self.supervisor.request(0, "end of the test")
        self.thread.join(timeout)
        return time.monotonic() - started


class Connections(threading.Thread):
    """Samples pg_stat_activity every 50 ms: the most connections the service (every tgaud-* login except the
    CLI's) held at once, and which ones."""

    def __init__(self, dsn: str, dbname: str) -> None:
        super().__init__(name="e2e-connections", daemon=True)
        self.dsn, self.dbname = dsn, dbname
        self.most = 0
        self.apps: set[str] = set()
        self.done = threading.Event()

    def run(self) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as c:
            while not self.done.wait(0.05):
                rows = c.execute("""SELECT application_name FROM pg_stat_activity
                                    WHERE datname = %s AND application_name LIKE 'tgaud-%%'
                                      AND application_name <> 'tgaud-cli'""", (self.dbname,)).fetchall()
                self.most = max(self.most, len(rows))
                self.apps |= {r[0] for r in rows}

    def now(self) -> list[str]:
        with psycopg.connect(self.dsn, autocommit=True) as c:
            return [r[0] for r in c.execute("""SELECT application_name FROM pg_stat_activity
                                               WHERE datname = %s AND application_name LIKE 'tgaud-%%'
                                               ORDER BY 1""", (self.dbname,))]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http_get(url: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except urllib.error.URLError as e:  # not listening (yet)
        return 0, str(e.reason)


def scrape(url: str) -> dict[tuple[str, frozenset], float]:
    status, text = http_get(url)
    assert status == 200
    return {(s.name, frozenset(s.labels.items())): s.value
            for family in text_string_to_metric_families(text) for s in family.samples}


def utc(v):
    return v.astimezone(UTC) if isinstance(v, dt.datetime) else v


@pytest.fixture
def made_jobs(monkeypatch) -> dict:
    """The scheduled jobs serve makes, by name, with the shortened intervals of the module docstring."""
    made: dict = {}

    def capture(module, factory) -> None:
        def make(ctx):
            job = factory(ctx)
            made[job.name] = job
            return job

        monkeypatch.setattr(module, "make", make)

    capture(chat_day, lambda ctx: chat_day.ChatDayJob(ctx, interval=2.0))
    capture(audience, lambda ctx: audience.AudienceJob(ctx, interval=2.0))
    capture(sampler, sampler.Sampler)
    capture(reconciler, reconciler.Reconciler)
    capture(prober, prober.Prober)
    monkeypatch.setattr(reconciler, "IDLE_POLL", 0.5)
    monkeypatch.setattr(prober, "IDLE_REFRESH", 0.5)
    monkeypatch.setattr(sampler, "METRICS_EVERY", 0.5)
    monkeypatch.setattr(sampler, "next_slot", lambda chat_id, after: float("inf"))
    return made


def test_scripted_day(pg, migrated_dsn, made_jobs, monkeypatch, capsys):
    dbname = psycopg.conninfo.conninfo_to_dict(migrated_dsn)["dbname"]
    now0 = dt.datetime.now(UTC).replace(second=0, microsecond=0)
    day = Day(now0 - dt.timedelta(hours=3))
    at = day.at
    tg = Telegram()
    port = free_port()
    env = {"TGAUD_DATABASE_URL": pg.tenant_url(dbname), "TGAUD_DATABASE_PASSWORD": pg.tenant_password,
           "TGAUD_BOT_TOKEN": TOKEN, "TGAUD_LINK_HMAC_KEY": LINK_KEY, "TGAUD_BOT_API_URL": tg.url,
           "TGAUD_METRICS_LISTEN": f"127.0.0.1:{port}", "TGAUD_ACTIVATION_ID": ACTIVATION}
    cfg = config.load(env, need={"database", "bot", "link_key"})
    cli = Cli(pg, migrated_dsn, monkeypatch, capsys)
    monkeypatch.setenv("TGAUD_BOT_API_URL", tg.url)
    base = f"http://127.0.0.1:{port}"
    before = {(name, frozenset(labels.items())): metrics.REGISTRY.get_sample_value(name, labels) or 0.0
              for name, labels in [
        ("tgaud_probes_total", {"outcome": "out"}),
        ("tgaud_api_requests_total", {"method": "getChatMember", "outcome": "429"}),
        ("tgaud_ingest_failures_total", {"stage": "parse"}),
        ("tgaud_ingest_failures_total", {"stage": "db"}),
        *[("tgaud_updates_total", {"type": t}) for t in ("chat_member", "my_chat_member", "message", "channel_post")],
        *[("tgaud_job_failures_total", {"job": j}) for j in metrics.JOBS],
    ]}

    def q(query: str, *args) -> list[tuple]:
        with psycopg.connect(migrated_dsn, autocommit=True) as c:
            return [tuple(utc(v) for v in r) for r in c.execute(query, args or None).fetchall()]

    def one(query: str, *args):
        rows = q(query, *args)
        assert len(rows) == 1, rows
        return rows[0][0] if len(rows[0]) == 1 else rows[0]

    def wait_for(what: str, check, timeout: float = 60.0):
        deadline = time.monotonic() + timeout
        while True:
            service.alive()
            value = check()
            if value:
                return value
            if time.monotonic() > deadline:
                raise AssertionError(f"timed out waiting for {what}")
            time.sleep(0.1)

    def delivered(*updates: dict) -> None:
        """One getUpdates answer, and wait until ingest committed its last update."""
        tg.updates(list(updates))
        last = updates[-1]["update_id"]
        wait_for(f"update {last}", lambda: one("SELECT next_offset FROM tg.bot_state") > last)

    def collect(chat: dict, started: dt.datetime, listed: dict[int, dt.datetime], reported: int) -> int:
        """A snapshot written the way the collect script writes it (design.md 5), as role collect_writer; `listed`
        maps each member to Telegram's participant date."""
        with psycopg.connect(migrated_dsn) as c:
            c.execute("SET ROLE collect_writer")
            c.execute("INSERT INTO tg.chat (chat_id, chat_type, title, username) VALUES (%s, %s, %s, %s) "
                      "ON CONFLICT (chat_id) DO NOTHING",
                      (chat["id"], chat["type"], chat["title"], chat.get("username")))
            sid = c.execute("INSERT INTO tg.snapshot (chat_id, started_at, method, reported_at_start) "
                            "VALUES (%s, %s, 'listing', %s) RETURNING snapshot_id",
                            (chat["id"], started, reported)).fetchone()[0]
            c.commit()
            with c.cursor() as cur:  # one page
                cur.executemany(
                    "INSERT INTO tg.snapshot_member (snapshot_id, user_id, seen_at, joined_date, inviter_id, "
                    "via_request, subscription_until, is_bot, is_deleted, is_premium, username, first_name, last_name) "
                    "VALUES (%s, %s, %s, %s, NULL, false, NULL, false, false, false, %s, %s, NULL) "
                    "ON CONFLICT DO NOTHING",
                    [(sid, u, started + dt.timedelta(seconds=i), joined, f"p{u}", f"P{u}")
                     for i, (u, joined) in enumerate(listed.items())])
            c.commit()
            c.execute("UPDATE tg.snapshot SET finished_at = now(), reported_at_end = %s, fetched = %s, complete = true "
                      "WHERE snapshot_id = %s", (reported, len(listed), sid))
            c.commit()
        wait_for(f"snapshot {sid} reconciled",
                 lambda: one("SELECT reconciled_at IS NOT NULL FROM tg.snapshot WHERE snapshot_id = %s", sid))
        return sid

    watch = Connections(migrated_dsn, dbname)
    watch.start()
    service = Service(cfg)
    try:
        wait_for("/health 200", lambda: http_get(f"{base}/health")[0] == 200)

        # --- B+0..2: the bot becomes admin in the channel, its discussion group and a basic group ---------------
        delivered(day.bot_added(CH, 0), day.bot_added(GR, 1), day.bot_added(OG, 2))
        ids = [CH["id"], GR["id"], OG["id"]]
        wait_for("the chats tracked, with getChat's metadata", lambda: one(
            "SELECT count(*) FROM tg.chat WHERE chat_id = ANY(%s) AND tracked AND metadata_due_at IS NULL", ids) == 3)
        assert one("SELECT linked_chat_id FROM tg.chat WHERE chat_id = %s", CH["id"]) == GR["id"]

        # --- B+10: snapshot 1, the members from before the bot; the campaign and its bot link ------------------
        snap1 = collect(CH, at(10), {U1: HINT, U2: HINT, U3: HINT}, reported=3)
        out = cli.run("campaign", "add", "Nov giveaway", "--chat", str(CH["id"]),
                      "--starts", at(30).isoformat(), "--draw", at(150).isoformat())
        assert out.code == 0, out.err
        campaign = one("SELECT campaign_id FROM tg.campaign WHERE name = 'Nov giveaway'")
        out = cli.run("link", "create", "--chat", str(CH["id"]), "--name", LINK_NAME, "--campaign", str(campaign))
        assert out.code == 0, out.err
        assert out.out.strip() == BOT_LINK

        # --- the day's updates, in one getUpdates answer ---------------------------------------------------------
        link = bot_link()
        delivered(
            day.member(OG, U9, 15, "left", "member"),
            day.member(GR, U1, 20, "left", "member"),
            day.member(CH, U6, 20, "left", "member"),
            day.member(GR, U8, 21, "left", "member"),
            day.member(CH, U6, 25, "member", "left"),
            day.message(GR, 30, 1, U1, text="hello (never stored)"),
            day.message(GR, 35, 2, U1, text="again"),
            day.member(CH, U4, 40, "left", "member", link=link),
            day.message(GR, 40, 3, U8, text="hi"),
            day.member(CH, U5, 45, "left", "member", link=link),
            day.post(CH, 65, 10),
            day.member(CH, U4, 70, "member", "left"),
            day.member(CH, U6, 80, "left", "member", link=link),
            day.member(CH, U7, 90, "left", "member", link=link),
            day.member(CH, U7, 100, "member", "kicked", actor=ADMIN),
            day.message(OG, 130, 50, ADMIN, migrate_to_chat_id=NG["id"]),
            day.message(NG, 130, 1, ADMIN, migrate_from_chat_id=OG["id"]),
            day.member(NG, U9, 131, "left", "member"),
        )
        wait_for("M1: OG's history moved to NG", lambda: one(
            "SELECT moved_at IS NOT NULL FROM tg.chat_alias WHERE old_chat_id = %s", OG["id"]))

        # --- B+120: snapshot 2 misses U3; the probe gets a 429, then "left" ------------------------------------
        tg.throttle_once.add((CH["id"], U3))
        tg.status[(CH["id"], U3)] = "left"
        snap2 = collect(CH, at(120), {U1: HINT, U2: HINT, U5: at(45), U6: at(80)}, reported=4)
        wait_for("U3's spell closed by the probe", lambda: q(
            "SELECT 1 FROM tg.membership WHERE chat_id = %s AND user_id = %s AND end_source = 'probe'", CH["id"], U3))

        # --- live: two count samples around a join; Telegram's second count has one join more than the events ---
        def sample(reported: int) -> dt.datetime:
            tg.counts[CH["id"]] = reported
            n = one("SELECT count(*) FROM tg.count_sample WHERE chat_id = %s", CH["id"])
            made_jobs["sampler"].schedule[CH["id"]] = 0.0
            wait_for("a finalized count sample", lambda: one(
                "SELECT count(*) FROM tg.count_sample WHERE chat_id = %s AND net_events IS NOT NULL",
                CH["id"]) == n + 1)
            return one("SELECT max(sampled_at) FROM tg.count_sample WHERE chat_id = %s", CH["id"])

        s1 = sample(5)
        time.sleep(1.2)  # the join falls into a later second than sample 1's call
        live = dt.datetime.now(UTC).replace(microsecond=0)
        delivered(day.member(CH, U10, live, "left", "member"))
        time.sleep(1.2)  # and sample 2's call starts in a later second than the join
        s2 = sample(7)
        assert s1 < live < s2
        settled = dt.datetime.now(UTC)
        wait_for("chat_day and audience published after the last change", lambda: one(
            "SELECT count(DISTINCT rollup) FROM stats.rollup_run "
            "WHERE as_of > %s AND rollup IN ('chat_day', 'audience')",
            settled) == 2)

        # --- the spells, by the state table -----------------------------------------------------------------------
        spells = q("""SELECT chat_id, user_id, start_source, joined_at, joined_hint, known_from, join_route,
                             invite_link_id IS NOT NULL, last_confirmed_at, suspect_since, end_source, end_reason,
                             left_at, left_after, left_before
                      FROM tg.membership WHERE chat_id = ANY(%s) ORDER BY chat_id, user_id, known_from""",
                   [CH["id"], GR["id"], OG["id"], NG["id"]])
        answered = one("SELECT answered_at FROM tg.probe WHERE chat_id = %s AND user_id = %s", CH["id"], U3)
        c, g, n = CH["id"], GR["id"], NG["id"]
        snap2_seen = {u: at(120) + dt.timedelta(seconds=i) for i, u in enumerate([U1, U2, U5, U6])}
        assert spells == sorted([
            # S1 (snapshot 1), S2 (confirmed by snapshot 2)
            (c, U1, "snapshot", None, HINT, at(10, 0), None, False, snap2_seen[U1], None, None, None, None, None, None),
            (c, U2, "snapshot", None, HINT, at(10, 1), None, False, snap2_seen[U2], None, None, None, None, None, None),
            # S1, A1 (absent from the good snapshot 2: suspect), P4 (the 429 changes nothing), P3 (gone: bounds
            # L .. the answer; a closed spell is no longer suspect)
            (c, U3, "snapshot", None, HINT, at(10, 2), None, False, at(10, 2), None, "probe", "gone", None,
             at(10, 2), answered),
            # E1 through the bot link, E5 (left)
            (c, U4, "event", at(40), None, at(40), "link", True, at(40), None, "event", "left", at(70), at(70), at(70)),
            # S2 fills the empty hint of an exact stay
            (c, U5, "event", at(45), at(45), at(45), "link", True, snap2_seen[U5], None, None, None, None, None, None),
            # E1, E5; then E2 (a rejoin through the link), confirmed by snapshot 2
            (c, U6, "event", at(20), None, at(20), "unattributed", False, at(20), None, "event", "left", at(25),
             at(25), at(25)),
            (c, U6, "event", at(80), at(80), at(80), "link", True, snap2_seen[U6], None, None, None, None, None, None),
            # E1, E5 by an admin: kicked
            (c, U7, "event", at(90), None, at(90), "link", True, at(90), None, "event", "kicked", at(100), at(100),
             at(100)),
            (c, U10, "event", live, None, live, "unattributed", False, live, None, None, None, None, None, None),
            (g, U1, "event", at(20), None, at(20), "unattributed", False, at(20), None, None, None, None, None, None),
            (g, U8, "event", at(21), None, at(21), "unattributed", False, at(21), None, None, None, None, None, None),
            # M1: the stay moved from OG to NG; E3: the echo is absorbed (L = its time, no new spell)
            (n, U9, "event", at(15), None, at(15), "unattributed", False, at(131), None, None, None, None, None, None),
        ], key=lambda r: (r[0], r[1], r[5]))
        probe = one("SELECT reason, outcome, attempts, status_seen, snapshot_id FROM tg.probe "
                    "WHERE chat_id = %s AND user_id = %s", CH["id"], U3)
        assert probe == ("snapshot_absent", "out", 1, "left", snap2)  # the 429 is not an attempt
        assert tg.member_calls(CH["id"], U3) == 2
        assert one("SELECT count(*) FROM tg.probe WHERE outcome = 'pending'") == 0
        assert q("SELECT snapshot_id, quality FROM tg.snapshot ORDER BY snapshot_id") == [
            (snap1, "good"), (snap2, "good")]
        assert one("SELECT count(*) FROM tg.member_event WHERE chat_id = %s", NG["id"]) == 2  # moved join + echo
        assert one("SELECT count(*) FROM tg.chat WHERE chat_id = %s", OG["id"]) == 0  # M1 drops the old chat row
        assert one("SELECT count(*) FROM tg.gap") == 0

        # --- stats.chat_day --------------------------------------------------------------------------------------
        def by_day(*times: dt.datetime) -> dict[dt.date, int]:
            counts: dict[dt.date, int] = {}
            for t in times:
                counts[t.date()] = counts.get(t.date(), 0) + 1
            return counts

        def expect_days(**cols: dict[dt.date, int]) -> dict[dt.date, dict[str, int]]:
            days = {d for v in cols.values() for d in v}
            return {d: {k: v.get(d, 0) for k, v in cols.items()} for d in days}

        cols = ("joins", "rejoins", "exits", "kicks", "bans_from_out", "net", "short_stays", "corrections_in",
                "corrections_out", "messages", "active_users")

        def chat_days(chat: dict) -> dict[dt.date, dict[str, int]]:
            rows = q(f"SELECT day, {', '.join(cols)} FROM stats.chat_day WHERE chat_id = %s", chat["id"])
            return {r[0]: dict(zip(cols, r[1:])) for r in rows if any(r[1:])}

        joins = [at(20), at(40), at(45), at(80), at(90), live]
        exits = [at(25), at(70), at(100)]
        net = by_day(*joins)
        for d, k in by_day(*exits).items():
            net[d] = net.get(d, 0) - k
        assert chat_days(CH) == expect_days(
            joins=by_day(*joins), rejoins=by_day(at(80)), exits=by_day(*exits), kicks=by_day(at(100)),
            bans_from_out=by_day(), net=net, short_stays=by_day(at(20), at(40), at(90)),
            corrections_in=by_day(at(10), at(10, 1), at(10, 2)), corrections_out=by_day(answered), messages=by_day(),
            active_users=by_day())
        writers: dict[dt.date, set[int]] = {}
        for t, uid in ((at(30), U1), (at(35), U1), (at(40), U8)):
            writers.setdefault(t.date(), set()).add(uid)
        gr_active = {d: len(users) for d, users in writers.items()}
        assert chat_days(GR) == expect_days(
            joins=by_day(at(20), at(21)), rejoins={}, exits={}, kicks={}, bans_from_out={},
            net=by_day(at(20), at(21)), short_stays={}, corrections_in={}, corrections_out={},
            messages=by_day(at(30), at(35), at(40)), active_users=gr_active)
        assert chat_days(NG) == expect_days(
            joins=by_day(at(15)), rejoins={}, exits={}, kicks={}, bans_from_out={}, net=by_day(at(15)),
            short_stays={}, corrections_in={}, corrections_out={}, messages={}, active_users={})
        today = s2.date()
        assert one("SELECT members_eod, reported_eod, known_eod FROM stats.v_chat_day WHERE chat_id = %s AND day = %s",
                   CH["id"], today) == (5, 7, 5)

        # --- stats.overlap (R2): humans with open spells, the diagonal is the chat's size ------------------------
        overlap = {(a, b): n for a, b, n in q("SELECT chat_a, chat_b, shared FROM stats.overlap")}
        pairs = sorted([CH["id"], GR["id"], NG["id"]])
        size = {CH["id"]: 5, GR["id"]: 2, NG["id"]: 1}
        assert overlap == {(a, b): size[a] if a == b else int({a, b} == {CH["id"], GR["id"]})
                           for i, a in enumerate(pairs) for b in pairs[i:]}

        # --- item 4: the campaign report through the CLI ----------------------------------------------------------
        rows = cli.rows("campaign", "report", str(campaign), as_of=None)
        assert len(rows) == 1
        report = rows[0]
        assert {k: report[k] for k in (
            "chat_id", "entrants", "first_time", "returners", "eligible", "quick_exit", "quick_exit_kicked",
            "quick_exit_unknown", "quick_exit_immature", "in_at_draw", "in_at_draw_unknown", "exit_after_draw",
            "exit_after_draw_unknown", "exit_after_draw_immature", "still_in", "still_in_suspect", "window_flags")} == {
            # entrants U4, U5, U6 (a returner: his first stay ended before the window), U7; U4 and U7 left within
            # 48 h (U7 kicked); U5 and U6 were in at the draw and still are; their 48 h are not over yet
            "chat_id": str(CH["id"]), "entrants": "4", "first_time": "3", "returners": "1", "eligible": "",
            "quick_exit": "2", "quick_exit_kicked": "1", "quick_exit_unknown": "0", "quick_exit_immature": "2",
            "in_at_draw": "2", "in_at_draw_unknown": "0", "exit_after_draw": "0", "exit_after_draw_unknown": "0",
            "exit_after_draw_immature": "2", "still_in": "2", "still_in_suspect": "0", "window_flags": ""}
        entrants = cli.rows("campaign", "report", str(campaign), "--entrants", as_of=None)
        assert [(r["user_id"], r["first_time"], r["quick_exit"], r["exit_reason"]) for r in entrants] == [
            (str(U4), "true", "yes", "left"), (str(U5), "true", "immature", ""), (str(U6), "false", "immature", ""),
            (str(U7), "true", "yes", "kicked")]

        # --- the drift numbers --------------------------------------------------------------------------------------
        assert q("""SELECT sampled_at, reported, known_open, suspect_open, net_events, mismatch, residual
                    FROM stats.v_drift WHERE chat_id = %s ORDER BY sampled_at""", CH["id"]) == [
            (s1, 5, 4, 0, 0, 1, None),  # the first sample: nothing to compare with
            (s2, 7, 5, 0, 1, 2, 1),  # +2 reported, one join seen: one missed join
        ]
        drift = cli.rows("drift", "--among", str(CH["id"]), as_of=None)
        assert [(r["reported"], r["known_open"], r["net_events"], r["mismatch"], r["residual"]) for r in drift] == [
            ("5", "4", "0", "1", ""), ("7", "5", "1", "2", "1")]

        # --- /metrics and /health -------------------------------------------------------------------------------------
        settled = time.time()
        wait_for("the metric cache refreshed", lambda: made_jobs["sampler"].next_metrics > settled + 0.1)
        m = scrape(f"{base}/metrics")

        def value(name: str, **labels) -> float | None:
            return m.get((name, frozenset({k: str(v) for k, v in labels.items()}.items())))

        def delta(name: str, **labels) -> float:
            return (value(name, **labels) or 0.0) - before[(name, frozenset(labels.items()))]

        ch, gr, ng, og = (str(x["id"]) for x in (CH, GR, NG, OG))
        assert {k: value("tgaud_chat_events_total", chat=ch, kind=k) for k in ("join", "leave", "kick")} == {
            "join": 6, "leave": 2, "kick": 1}
        assert value("tgaud_chat_events_total", chat=gr, kind="join") == 2
        assert value("tgaud_chat_events_total", chat=og, kind="join") == 1
        assert value("tgaud_chat_events_total", chat=ng, kind="join") == 1  # the echo is an event (not a spell)
        assert value("tgaud_chat_short_stays_total", chat=ch) == 3
        assert value("tgaud_last_transition_timestamp_seconds", chat=ch) == live.timestamp()
        for chat, known in ((ch, 5), (gr, 2), (ng, 1)):
            assert value("tgaud_chat_members_known", chat=chat) == known
            assert value("tgaud_chat_members_suspect", chat=chat) == 0
            assert value("tgaud_chat_gap_open", chat=chat) == 0
            assert value("tgaud_probe_queue", chat=chat) == 0
            assert value("tgaud_chat_bot_admin", chat=chat) == 1
            assert value("tgaud_chat_permission_ok", chat=chat, right="invite_users") == 1
            assert value("tgaud_chat_join_requests_pending", chat=chat) == 0
        assert value("tgaud_chat_info", chat=ch, type="channel", title=CH["title"]) == 1
        assert value("tgaud_chat_info", chat=ng, type="supergroup", title=NG["title"]) == 1
        assert value("tgaud_chat_members_known", chat=og) is None  # M1 removed the old chat
        assert value("tgaud_chat_members_reported", chat=ch) == 7
        assert value("tgaud_chat_drift_mismatch", chat=ch) == 2
        assert value("tgaud_chat_drift_residual_24h", chat=ch) == 1
        assert value("tgaud_chat_members_reported", chat=gr) is None  # never sampled
        assert 0 < value("tgaud_snapshot_age_seconds", chat=ch) < 600
        assert delta("tgaud_probes_total", outcome="out") == 1
        assert delta("tgaud_api_requests_total", method="getChatMember", outcome="429") == 1
        assert {t: delta("tgaud_updates_total", type=t) for t in ("chat_member", "my_chat_member", "message",
                                                                    "channel_post")} == {
            "chat_member": 13, "my_chat_member": 3, "message": 5, "channel_post": 1}
        assert [delta("tgaud_ingest_failures_total", stage=s) for s in ("parse", "db")] == [0, 0]
        assert {j: delta("tgaud_job_failures_total", job=j) for j in metrics.JOBS} == {j: 0 for j in metrics.JOBS}
        for job in ("reconciler", "prober", "sampler", "chat_day", "audience", "partitions"):
            assert value("tgaud_job_last_success_timestamp_seconds", job=job) > now0.timestamp(), job
        for rollup in ("chat_day", "audience"):
            assert value("tgaud_rollup_as_of_timestamp_seconds", rollup=rollup) > s2.timestamp(), rollup
        assert value("tgaud_partitions_ahead_days", table="member_event") >= 7
        assert value("tgaud_poll_last_success_timestamp_seconds") > time.time() - 30
        status, body = http_get(f"{base}/health")
        assert status == 200, body
        assert body.startswith("ok") and body.rstrip().endswith(f"activation={ACTIVATION}")

        # --- connections: ingest, worker, batch; never more than 3 (phase 1) --------------------------------------
        assert sorted(a for a in watch.now() if a != "tgaud-cli") == sorted(SERVE_APPS)
    finally:
        took = service.stop()
        watch.done.set()
        watch.join(5)
        tg.close()
    assert service.code == 0
    assert took < 10, took
    assert watch.most <= 3, watch.most
    assert watch.apps == set(SERVE_APPS)
    with psycopg.connect(migrated_dsn, autocommit=True) as c:
        left = c.execute("SELECT count(*) FROM pg_stat_activity "
                         "WHERE datname = %s AND application_name LIKE 'tgaud-%%'", (dbname,)).fetchone()[0]
    assert left == 0
