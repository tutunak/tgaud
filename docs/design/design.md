# Bot audience statistics: final design

FRAME v1, decisions.md D1-D17 (they win), base proposal-claude.md with proposal-codex.md's evidence model and both
critiques' fixes. Bot API facts are cited as F§n (RUN1/facts.md, Bot API 10.5). Target: the owner's shared
PostgreSQL **18.6** server (512 MB, `work_mem` 2 MB, no parallel workers, about 10 connections for this tenant).

Files: `schema.sql` (DDL, loads as the tenant), `admin.sql` (optional roles, run by an admin), `queries.sql`
(analytics 1-16 and refresh R1-R4), `verify.md` + `evidence/` (fixture, constraint tests, scale test).

## 1. Overview

One Python process next to `src/tgstats/` (`tgaud`, psycopg 3, httpx, prometheus_client) does five jobs:

- **ingest**: long-polls `getUpdates` (one stream in `update_id` order, no public endpoint, D1), one DB transaction
  per update with the next offset in `tg.bot_state`;
- **reconciler**: applies finished snapshots of the owner's adapted `tgstats collect`, answers of probes, gaps and
  migrations, and rebuilds spells per (chat, user);
- **prober**: `getChatMember` checks from `tg.probe`, within a budget;
- **sampler**: `getChatMemberCount` every 10 minutes per chat, staggered (D7), and the cached metric values;
- **batch**: the rollups `stats.chat_day` (every 5 min) and the audience rollups (hourly).

History has two layers (D3): `tg.member_event`, every `chat_member` update, append-only; and `tg.membership`, one
row per continuous stay ("spell"), derived from all evidence (events, `tg.sighting` from snapshots, `tg.probe`,
`tg.gap`, `tg.chat_alias`) by one deterministic rebuild function. Every number can say how complete it is:
`tg.coverage` (bot was admin), `tg.gap` (lost or doubtful periods), `tg.count_sample` (drift), suspect spells.

## 2. Data collected, ranked

Value for the owner against effort and privacy cost. Ch/Gr = channels / groups (incl. discussion supergroups).

| # | Item | Bot API source | Ch/Gr | Answers | Table | Phase |
|---|---|---|---|---|---|---|
| 1 | Membership transitions | `chat_member`: old/new status, `is_member`, `date`, `from`, `until_date` (F§2) | both | joins, leaves, rejoins, bans, tenure | `member_event`, `membership` | 1 |
| 2 | Bot coverage | `my_chat_member` + startup `getChatMember(bot)` rights check | both | for which period numbers are complete | `coverage`, `gap` | 1 |
| 3 | Reported size | `getChatMemberCount` (F§10) | both | drift, size of unseeded chats | `count_sample` | 1 |
| 4 | Snapshot members | MTProto collect script: participant `date`, `inviter_id`, `via_request` | both | members from before the bot; drift repair | `snapshot*`, `sighting` | 1 |
| 5 | Join path | `invite_link` (`name`, `creator`, `is_primary`), `via_join_request`, `via_chat_folder_invite_link`, `from` != member (F§2, F§4) | both | which link, campaign or admin brings people who stay | `invite_link`, `membership.join_route` | 1 |
| 6 | Owner campaigns + bot links | CLI entry; `createChatInviteLink(name)` (F§4) | both | prize joiners, campaign yield (the API has no per-link counter) | `campaign`, `invite_link` | 1 |
| 7 | Spot checks | `getChatMember(chat, user)` (F§10) | both | confirm absences, repair after outages | `probe` | 1 |
| 8 | Telegram giveaways | `message.giveaway` / `channel_post.giveaway`: `chats[]`, `winners_selection_date`, `only_new_members` (F§8) | both | exact window and chats of native giveaways | `campaign` (observation) | 1 |
| 9 | Moderation | `chat_member` to `kicked`/`restricted`, `from` = admin, `until_date` | both | spam waves, joined-then-banned | `member_event` | 1 |
| 10 | Join requests | `chat_join_request`: `from`, `date`, `invite_link`; outcome from the later join (F§3) | ch, supergroups | request funnel | `join_request` | 1 |
| 11 | Chat metadata | `getChat`: type, title, username, `linked_chat_id`, `join_by_request`, `has_hidden_members`; migration fields (F§1, F§10) | both | channel <-> discussion pairs, identity across migration | `chat`, `chat_alias` | 1 |
| 12 | Group activity counts | `message.from` per day; `sender_chat`, `is_automatic_forward` apart (F§5) | groups | active vs silent; did prize joiners ever write | `activity_daily`, `unattributed_daily` | 1 |
| 13 | Channel post times | `channel_post`: `message_id`, `date`, media kind | ch | leaves after a post | `post` | 1 |
| 14 | User profile | `is_bot`, `is_premium`, `language_code`, `username`, names (F§9) | both | exclude bots; recognise people in drill-downs | `tg_user` | 1 (`store_names`) |
| 15 | Raw membership updates | the three membership update types, stripped | both | re-parse after a parser fix | `raw_update`, 30 days | 1 |
| 16 | Boosts | `chat_boost` / `removed_chat_boost` (F§7) | ch, supergroups | do boosters and prize claimers stay | - | 2 |
| 17 | Reactions | `message_reaction_count` (counts; per-user `message_reaction` not planned) (F§6) | both | engagement per post | - | 2 |
| 18 | Paid subscriptions | `member.until_date` (stored in `member_event.new_until_date`) | ch | paid churn (renewal updates unverified, F§14.4) | - | later |
| 19 | Communities | `ChatFullInfo.community`, `community_chat_joined` (F§12) | both | a new acquisition route | - | later |

Not obtainable through the Bot API (F§11): member lists, views, forwards, per-link join counts, history from before
the bot, the giveaway's participants. Never stored: request `bio`, `user_chat_id`, message text.

## 3. Schema in short

`schema.sql` has every table with a comment (why, who writes it). It loads with `psql -v ON_ERROR_STOP=1 -f` as the
**tenant login** (NOSUPERUSER NOCREATEDB NOCREATEROLE, owner of the database and of `public`): it creates the
trusted extension `btree_gist` (trusted on 18.6: `pg_available_extension_versions.trusted = t`) and no role.
`admin.sql` (optional, run by an admin) creates `bi_reader` and `collect_writer`; their grants live in
`tg.apply_grants()`, which `schema.sql` runs when the roles exist and `admin.sql` runs after creating them.

- `tg` (user-level): `bot_state`, `update_receipt` (daily partitions, 8 days), `raw_update` (daily, 30 days), `chat`,
  `chat_alias`, `coverage`, `gap`, `tg_user`, `campaign`, `campaign_chat`, `invite_link`, `member_event` (monthly,
  kept forever), `membership`, `snapshot`, `snapshot_member`, `sighting`, `probe`, `reconcile_run`, `count_sample`,
  `join_request`, `post`, `activity_daily`, `unattributed_daily`, `rollup_dirty`, materialized view `user_chats`, and
  `chat_set` (`user_chats` grouped by identical chat sets).
- `stats` (aggregates): `chat_day`, `overlap`, `unique_per_chat`, `chats_per_user`, `rollup_run`, and the BI views
  `v_chat_status`, `v_overlap`, `v_chats_per_user`, `v_unique_per_chat`, `v_chat_day`, `v_drift`, `v_gaps`,
  `v_join_requests_weekly`, `v_campaign_summary`, `v_cohort_retention`, `v_join_sources`.
- Functions: `tg.maintain_partitions` (creates ahead, drops expired raw data), `tg.move_chat_history` (D2),
  `tg.campaign_entrant` and `stats.campaign_summary` (item 4), `tg.state_at` (where a spell stands at time t),
  `tg.window_flags` (gaps and coverage of a window), `tg.forget_user` (D17).

Connections (tenant limit about 10): ingest 1, reconciler + prober + sampler 1 (one worker loop), batch 1, CLI 1-2,
web 2 (phase 2), BI 1-2 through `bi_reader`. Batch jobs raise `work_mem` with `SET LOCAL`: chat_day 16 MB, audience
rollup and 16c 64 MB; everything else runs with the server's 2 MB.

## 4. History model

"In" = creator, administrator, member, or restricted with `is_member` (a restricted member without the field counts
as in, F§2). `member_event.kind` is derived from old/new status: `join`, `leave`, `kick`, `restrict_out`, `ban_out`,
`unban`, `restrict`, `unrestrict`, `promote`, `demote`, `in_change`, `out_change`.

A spell records start and end with provenance and precision:
- start: `joined_at` (exact, from an event; enforced by a foreign key to the event's id **and** time), `joined_hint`
  (Telegram's participant `date` from a snapshot; latest join for rejoiners; unchecked), `known_from` (earliest
  moment known to be in), `start_source` event | snapshot | probe | inferred, `join_route`, `invite_link_id`;
- `last_confirmed_at` (latest positive evidence) and `suspect_since` (open but doubted);
- end: `left_at` (exact, event) or bounds `left_after` .. `left_before` (last known in .. first known out; equal to
  `left_at` when exact), `end_source` event | probe | inferred | migration, `end_reason` left | kicked | gone | migrated.
- `stay` = `[known_from, left_before)`; the exclusion constraint "one stay at a time" forbids overlapping spells and
  is the GiST index for as-of queries (D13).

**Rebuild.** For one (chat, user) the reconciler deletes the spells and replays all evidence in time order: events
by (`event_at`, `update_id`); sightings at `seen_at` (both ends of a `tg.sighting` run); absences at the good
snapshot's `started_at`; probe answers at `answered_at`; gaps at their start; the migration at `migrated_at`. At the
same instant: events, then sightings, then probes. The bot applies the same rules incrementally for a new update; any
evidence older than the pair's last applied evidence triggers a rebuild of that pair, and every change upserts
`tg.rollup_dirty` with the earliest of the old and new `known_from`, `left_after` and `left_before` of the spell. Late or older evidence therefore lands where it belongs in time; the one rule that
protects a newer explicit event from stale data is the grace period G = 10 minutes (S3).

**The state table.** Projection = the pair's latest spell: *none*, *open*, *closed*. e = evidence time,
L = `last_confirmed_at`, G = 10 min. Rows with a chat in brackets were exercised by the fixture (verify.md 2); the others are specified here and not yet tested.

| # | Projection | Evidence | Time order | Action |
|---|---|---|---|---|
| E1 | none | join event (out -> in) | - | open: start `event`, `joined_at` = `known_from` = e, route from the event (H01) |
| E2 | closed | join event | after the end | open a new exact spell: a rejoin; earlier spells stay (H03, A/1006) |
| E3 | open | join event, old status out | within 1 h after the chat's `migrated_at`, spell began before it | migration echo: absorbed, L = e, no new spell (H13) |
| E4 | open | join event, old status out | otherwise | missed leave: close `inferred`/`gone`, bounds L .. e; open a new exact spell (H12) |
| E5 | open | exit event: leave, kick, restricted `is_member` true -> false | - | close exact: `left_at` = e; `kicked` if new status kicked or `from` != member, else `left` (H02, H04, H07) |
| E6 | none | exit event | - | left-censored: start `inferred`, `known_from` = e, exact exit at e (H08) |
| E7 | closed | exit event (old status in) | after the end | missed rejoin, then exit: a left-censored spell like E6 |
| E8 | open | in -> in event (promote, restrict, ...) | - | L = e; no spell change (H07 restrict) |
| E9 | none / closed | in -> in event | - | missed join: open, start `inferred`, `known_from` = e |
| E10 | open | out -> out event (ban from out, unban) | - | the old status says out: close `inferred`/`gone`, bounds L .. e; the moderation stays an event |
| E11 | none / closed | out -> out event | - | no spell change: a ban from out is moderation, an unban adds no join (H05, H06) |
| R1 | open | restricted `is_member` false -> true | - | it is a join (E1/E2 rules): a new spell (H07) |
| S1 | none | sighting (any snapshot quality) | - | open: start `snapshot`, `known_from` = `seen_at`, `joined_hint` = participant date (H09, A/1001) |
| S2 | open | sighting | after L | confirm: L = `seen_at`; clear `suspect_since` <= `seen_at`; fill an empty hint (A/1005 from a partial snapshot) |
| S3 | closed | sighting | `seen_at` < end + G | conflict: ignored (a stale listing must not undo an explicit exit); probe queued, reason `conflict` |
| S4 | closed | sighting | `seen_at` >= end + G | missed rejoin: open, start `snapshot` |
| S5 | (replay) | sighting older than the first event | before | it opens first; a later exit closes that spell (an older snapshot becomes the entry of a left-censored stay); a later join with old status out closes it by E4 |
| A1 | open | absent from a **good** snapshot | L < `started_at` | suspect since `started_at`; probe queued, reason `snapshot_absent` (H10, H11, B/1019) |
| A2 | open | absent from a good snapshot | L >= `started_at` | nothing: seen or joined during the listing |
| A3 | any | absent from a partial snapshot, or none / closed | - | nothing: absence never closes a spell by itself (D5) |
| P1 | open | probe: in | - | confirm: L = answer time, clear suspect |
| P2 | none / closed | probe: in | after the end | missed join: open, start `probe`, `known_from` = answer time |
| P3 | open | probe: out (left, kicked, restricted not member) | - | close `probe`, reason `gone` (or `kicked`), bounds L .. answer time (H10, H14, B/1019) |
| P4 | any | probe failed (429, timeout, error) | - | unknown: nothing changes; stays open and suspect; retried with backoff (H11) |
| G1 | open | gap (outage, rights lost) | gap starts after L | suspect since the gap start; probes queued (reason `outage`) within the budget; a later sighting, probe or event clears it (H14, A-D confirmed by the Aug 12 snapshots) |
| M1 | any | migration, new chat tracked | - | `tg.move_chat_history`: all rows re-keyed, then rebuild (E3 absorbs the echoes) (H13) |
| M2 | open | migration, new chat not tracked | - | close `migration`/`migrated`, bounds L .. `migrated_at`: censoring, never an exit; reopened by M1 if the new chat is tracked later |

A member who was there before the bot comes from the collect script (S1) with Telegram's join date as a hint; a
missed leave is found later by a probe (P3, bounded exit) or by the next observed join (E4, unknown end before it).

## 5. Snapshot contract (the owner's adapted `tgstats collect`)

The script writes rows and marks the snapshot finished; the service reconciles. Role `collect_writer` allows
exactly this. Ids: chat id from `utils.get_peer_id` (Bot API marked form), user id from the participant.

1. *(optional)* `INSERT INTO tg.chat (chat_id, chat_type, title, username) VALUES (...) ON CONFLICT (chat_id) DO NOTHING`.
2. `INSERT INTO tg.snapshot (chat_id, started_at, method, reported_at_start) VALUES (...) RETURNING snapshot_id`;
   commit. `method` = `basic_group` | `listing` | `deep`; `reported_at_start` = the count read before listing.
3. For every page: `INSERT INTO tg.snapshot_member (snapshot_id, user_id, seen_at, joined_date, inviter_id,
   via_request, subscription_until, is_bot, is_deleted, is_premium, username, first_name, last_name) ... ON CONFLICT
   (snapshot_id, user_id) DO NOTHING`; commit per page or per 10 000 rows. `seen_at` = when the page arrived;
   `joined_date` = `participant.date` (`ChannelParticipant`, `ChannelParticipantSelf`, `ChatParticipant`;
   NULL for creators); `inviter_id`, `via_request`, `subscription_until` where Telethon has them; names only when
   the owner keeps names.
4. Last transaction: `UPDATE tg.snapshot SET finished_at = now(), reported_at_end = <count after listing>,
   fetched = <distinct users written>, complete = <false if interrupted or the script saw an error> WHERE
   snapshot_id = $1`. **This marks it finished.** An interrupted run may still finish with `complete = false`:
   its sightings count, its absences do not. A snapshot left unfinished for 2 days is deleted by the reconciler.

The reconciler takes finished, unreconciled snapshots per chat in `started_at` order once ingest has caught up past
`finished_at` (a `getUpdates` that returned fewer than 100 updates after `finished_at`; never "a later event in that
chat", which blocks on quiet chats). It grades `quality`: **good** = complete and
`max(reported_at_start, reported_at_end) - fetched <= max(10, 0.1 % of it)`; otherwise **partial** (the legacy 2 %
tolerance is 20 000 people at 1M). A good snapshot whose absent candidates exceed 2 % of the chat is downgraded to
partial (a broken listing, not 20 000 leaves). Then: upsert `tg_user` (is_deleted only from here), fold rows into
`tg.sighting` (runs of consecutive sightings, broken by exit evidence), apply S1-S5 and A1-A3, queue probes, mark
`rollup_dirty`, write `reconcile_run`, and prune `snapshot_member` to the last 2 good snapshots per chat (R3).

## 6. Reconciliation, probes and drift

- **Probes (D5).** Global token bucket for `getChatMember`, starting at 5 calls/s (the limit is undocumented,
  F§10, F§14.7; measure it), at most half of it for one chat while others wait, `retry_after` honoured on 429, three
  attempts with backoff (1 min, 10 min, 1 h), then `error` (unknown). Priority: conflicts, then post-outage spells
  of the affected chats (newest first), then snapshot absences. Unprobed candidates stay open and suspect; reports
  and metrics show the suspect count. A 1M chat after a long outage is repaired by a new snapshot, not by probes.
- **Drift (D7).** Every 10 minutes per chat, staggered: `reported` (`getChatMemberCount`), `known_open` (open spells,
  all accounts: an index-only count), `suspect_open`, `net_events` (exact joins minus exact exits by event time since
  the previous sample). Two numbers: absolute mismatch `reported - known_open` (before seeding it mostly measures
  unseeded members) and delta residual `(reported - previous) - net_events` (missed events right now). Matching
  counts can still hide one missed join and one missed leave; snapshots catch those.
- **Gaps (D6).** `service_down` when the poller was down longer than the 24 h retention (F§1) or lag passed 20 h;
  `not_admin` / `rights_lost` from `my_chat_member` and the startup rights check; `updates_not_allowed` if the
  startup check finds `chat_member` missing; `contradiction` when a chat has more than 10 E4/E10/S3 cases in an hour.
  Every windowed report prints `tg.window_flags(chat, window)`.

## 7. `allowed_updates` and ingest (D1, D14)

`allowed_updates` = `chat_member, my_chat_member, chat_join_request, message, channel_post` from day one: a type not
allowed when the event happens is lost for good (F§1), and v1 stores something from each. `message` brings group
activity counts, `giveaway` messages and the migration service messages; text is never stored. Not allowed in v1:
edited messages, reactions, boosts (phase 2 enables them; their earlier history is accepted as lost).

Loop: `getUpdates(offset = bot_state.next_offset, timeout = 50, limit = 100, allowed_updates)`; per update one
transaction: `INSERT INTO tg.update_receipt ... ON CONFLICT DO NOTHING` (seen = skip the body), the body by type,
`UPDATE tg.bot_state SET next_offset = update_id + 1`, commit. A crash re-delivers only uncommitted updates.
Idempotency is the update identity (`update_id`, update date): `member_event` is unique on (`update_id`,
`event_at`), so join, leave, join in one second stay three events. After a week without updates the ids restart at
random (F§1): at start, if `last_update_at` is older than 6 days, the first call omits the offset and the receipt
key drops repeats. `raw_update` keeps only `chat_member`, `my_chat_member`, `chat_join_request`, with `bio`,
`user_chat_id` (a CHECK enforces it) and, when `store_names` is off, names stripped; 30 days, daily partitions
dropped by `tg.maintain_partitions` (also creates `member_event` months 3 ahead), called at start and daily.

## 8. Analytics

`queries.sql` has one block per item (question, then SQL; parameters are psql variables, `$n` in the service). All
report the as-of time, the chats in scope, coverage and gap flags; retention reports count only mature cohorts and
show unknown outcomes apart.

| # | Item | Reads | Runs as |
|---|---|---|---|
| 1 | overlap of every pair | `stats.v_overlap` (R2) | CLI/BI, instant |
| 2 | chats per user: distribution (scope), one user's chats and stays | `tg.chat_set`; `membership` by user | CLI |
| 3 | unique per chat in a scope, distinct total | `tg.chat_set` | CLI; `stats.unique_per_chat` for all chats |
| 4 | giveaway joiners per chat and per person, entrant list | `tg.campaign_entrant` over `membership` | CLI/BI/web |
| 5 | campaign-free short stays per chat and week | `membership`, `campaign` | CLI |
| 6 | serial short-stayers; join bursts across chats | `membership` (user, join) | CLI |
| 7 | tenure; cohort retention 7/30/90 | `membership` | CLI/BI |
| 8 | join sources with retention | `membership`, `invite_link`, `campaign` | CLI/BI |
| 9 | daily growth with residual; drift samples | `stats.chat_day` (R1), `count_sample` | BI/CLI |
| 10 | rejoiners; flow between own chats | `membership` | CLI |
| 11 | channel vs its discussion group | `stats.overlap`, `activity_daily` | CLI |
| 12 | join-request funnel | `join_request`, `membership` | CLI/BI |
| 13 | raids | `membership` by join minute | CLI |
| 14 | active vs silent; did short-stayers write | `membership`, `activity_daily` | CLI |
| 15 | leaves after each post | `post`, `membership` (exits) | CLI |
| 16 | as-of membership: exact counts and list (16a/b), the pair matrix at a past date (16c, batch), members at the end of any day (16d) | GiST on `stay`; 16d `stats.chat_day` | CLI/BI |

Rollups (D13): `stats.chat_day` recomputed by R1 from the earliest dirty day (or 2 days back: short stays mature
after 48 h): the normal run reads each chat by index range scans from that day, a large recompute (the first
build, far-back dirty days) is one set-based pass, same result; besides the movement it stores
daily deltas of stays held (`members_started/ended`, `unsure_started/ended`), so `stats.v_chat_day.members_eod` is
"members at the end of day D" as a running sum (16d, instant; 16a is the exact-instant scan). R2 refreshes
`tg.user_chats`, groups it into `tg.chat_set` (338k distinct sets for 3M people at scale) and computes from the
sets, in one transaction: `stats.overlap` (pairs expanded once per set, work = sum of k^2 over distinct sets, zero
pairs and the diagonal included), `unique_per_chat`, `chats_per_user`, published with `as_of`. Items 2, 3, 11
for any scope read `tg.chat_set`; item 11 reads three cells of `stats.overlap`. Human filters over big scans use `NOT IN` against the partial index
`tg_user_not_human` (bots and deleted accounts only) instead of joining 3M users.

## 9. Metrics (D15)

`/metrics` on 127.0.0.1:9108, scraped by Prometheus and forwarded to New Relic. Labels are finite: `chat` = chat id
of a tracked chat (<= 200), `type`, `kind`, `method`, `outcome` = `ok|429|403|400|5xx|timeout|other`, `job`, `rollup`.
No user, link, post or campaign label. Scrapes read values the sampler caches every minute; they never query.
About 5 500 series at 200 chats. Alert thresholds are starting values to tune on real chats.

| Name | Type | Labels | Meaning | Starting alert |
|---|---|---|---|---|
| `tgaud_poll_last_success_timestamp_seconds` | gauge | - | last getUpdates without error | older than 2 min |
| `tgaud_updates_total` | counter | `type` | committed updates | - (dashboards) |
| `tgaud_update_lag_seconds` | histogram | `type` | receive time - update date | p99 > 1 h for 15 min; > 12 h page |
| `tgaud_api_requests_total` | counter | `method`, `outcome` | Bot API calls | 429 rate > 0.1/s for 10 min; any 401/409 |
| `tgaud_api_request_seconds` | histogram | `method` | Bot API latency | p99 > 5 s |
| `tgaud_ingest_failures_total` | counter | `stage` (parse, db) | updates that failed | any increase |
| `tgaud_db_transaction_seconds` | histogram | `job` | DB time per transaction | p99 > 1 s for 10 min |
| `tgaud_job_last_success_timestamp_seconds` | gauge | `job` (sampler, reconciler, prober, chat_day, audience, partitions) | job heartbeat | older than 2x its interval |
| `tgaud_rollup_as_of_timestamp_seconds` | gauge | `rollup` | freshness of published rollups | audience > 3 h, chat_day > 20 min |
| `tgaud_lost_seconds_total` | counter | - | confirmed lost time (gaps > 24 h) | any increase |
| `tgaud_partitions_ahead_days` | gauge | `table` | partitions created ahead | < 7 |
| `tgaud_chat_info` | gauge = 1 | `chat`, `type`, `title` | join key for dashboards; a rename makes one new series | - |
| `tgaud_chat_bot_admin` | gauge 0/1 | `chat` | bot is admin | 0 for 5 min |
| `tgaud_chat_permission_ok` | gauge 0/1 | `chat`, `right` (invite_users, restrict_members) | rights the bot needs | 0 for 5 min |
| `tgaud_chat_gap_open` | gauge 0/1 | `chat` | an open gap touches the chat | 1 |
| `tgaud_last_transition_timestamp_seconds` | gauge | `chat` | last chat_member event | none for 6 h in a chat > 10k members |
| `tgaud_chat_members_reported` / `_known` / `_suspect` | gauge | `chat` | Telegram's count, open spells, suspect spells | suspect > 1 % of known |
| `tgaud_chat_drift_mismatch` | gauge | `chat` | reported - known | growing for 24 h after seeding |
| `tgaud_chat_drift_residual_24h` | gauge | `chat` | sum of delta residuals, 24 h | abs > max(20, 0.2 % of reported) |
| `tgaud_chat_events_total` | counter | `chat`, `kind` (join, leave, kick, restrict_out, ban_out, unban, role) | business rates | joins > 10x 7-day rate: raid or giveaway |
| `tgaud_chat_short_stays_total` | counter | `chat` | exits within 48 h of the join | spike after a campaign's draw |
| `tgaud_chat_join_requests_total` | counter | `chat`, `outcome` (received, approved, declined, unknown) | request flow | - |
| `tgaud_chat_join_requests_pending` | gauge | `chat` | requests without an outcome | > 100 for 1 day |
| `tgaud_probe_queue` / `tgaud_probe_oldest_age_seconds` | gauge | `chat` | probe backlog | oldest > 24 h |
| `tgaud_probes_total` | counter | `outcome` (in, out, error) | probe answers | error share > 20 % |
| `tgaud_snapshot_age_seconds` | gauge | `chat` | age of the last good snapshot | > 14 days where suspect > 1 % |

## 10. Delivery (D16)

- **CLI** `tgaud` next to `tgstats` (chat resolution by id, @username or title as `analysis.resolve_chat`, `--among`
  for the scope, `--csv`, `--as-of`): `overlap`, `unique`, `chats-per-user`, `user ID|@name`, `growth`, `campaign
  add|list|report ID`, `link create --name --campaign`, `short-stays`, `serial`, `bursts`, `sources`, `cohorts`,
  `tenure`, `rejoiners`, `flow`, `discussion`, `requests`, `raids`, `activity`, `post-leaves`, `asof CHAT DATE`,
  `drift`, `coverage`, `probe CHAT USER`, `forget-user ID`. Each runs one block of `queries.sql`.
- **SQL views for BI** (Grafana for time series, Metabase for matrix and cohorts): the `stats.v_*` views through
  `bi_reader`, aggregates only, each with its as-of time and coverage.
- **Metrics**: section 9.
- **Web UI (phase 2)**: a small server-rendered app in the same process behind the reverse proxy's authentication,
  reading the same queries: overview (members, coverage, drift, gaps), overlap matrix with drill-down to a pair,
  user lookup, campaigns (create, make bot links, prize-joiner report and entrant download). BI covers dashboards;
  the web UI covers what BI can't: campaign entry and user-level drill-down.

## 11. Privacy (D17)

Personal data: user ids, membership history, short-stay and prize-hunter flags, activity counts, join requests,
names, usernames, language, premium. All of it stays in schema `tg`; BI sees only `stats` aggregates (verified:
`bi_reader` is denied `tg`). Never stored: request `bio`, `user_chat_id`, message text, raw messages; raw membership
updates expire after 30 days. `store_names` (default on, as `tgstats`) blanks names and usernames in `tg_user`,
`snapshot_member` (after reconcile) and raw payloads. `tgaud forget-user` calls `tg.forget_user` (erases one person
everywhere and marks the affected chat_day rows dirty). CSV exports carry ids and are for the owner only. Whether
GDPR applies (EU members) is a legal question the owner must settle; it is not checked here.

## 12. Phases

- **Phase 1**: bot ingest with the v1 `allowed_updates`; `schema.sql`; reconciler with the state table; prober and
  sampler; drift and coverage; rollups R1-R4; metrics; CLI; BI views; the snapshot contract for the owner.
- **Phase 2**: web UI; reactions (counts) and boosts (tables then designed and allowed in `allowed_updates`).
- **Later**: paid subscriptions, communities; incremental maintenance of `user_chats` and the pair matrix if the
  hourly R2 grows past its budget (24-34 s at scale, verify.md 5).

## 13. Risks and what to measure on a real chat

1. **`chat_member` completeness in a ~1M channel** (F§14.1): everything rests on it. Watch `drift_residual_24h` on the
   largest channel for 7 days, and compare one day of events with a snapshot diff.
2. **`getChatMemberCount` precision** (F§14.8): if rounded or cached, the drift alert must exceed the noise. Poll a test
   chat every 10 s around controlled joins; on the big channel compare 1 h deltas with net events.
3. **`getChatMember` rate limit** (F§14.7): sets the probe budget. Measure the sustained rate until 429 on a spare bot.
4. **Snapshot completeness of big channels**: listings come back short even with `--deep`; measure fetched/reported
   and how many absent candidates a good snapshot produces at 1M (the 0.1 % / 2 % thresholds).
5. **Migration** (F§14.13): do members get join echoes in the new supergroup, with which old status and how late?
   E3 assumes within 1 h; test once in a throwaway group.
6. **First join old status, `from` on approvals and admin adds** (F§14.3, F§14.5): affects `join_route`, not the
   join. **Masked links** (F§14.2): is the masked text stable per link? Decides grouping in item 8.
7. **Update id restart and offset** after a week of silence (F§1): test that the no-offset first call loses nothing.
8. **Server limits** (512 MB, 2 MB `work_mem`): measured in verify.md 5; re-measure on production with real data
   size, and watch `tgaud_db_transaction_seconds` and the rollup durations. The exact-instant as-of count of a 1M
   chat (16a) reads the whole chat (seconds); day-level counts (16d) are instant.
9. **The GiST "one stay at a time" index** took 572 s to build for 8.5M spells (verify.md 5); inserts keep it up to
   date cheaply, but a restore or `REINDEX` pays that again. Restore with a larger `maintenance_work_mem`. If that
   is not acceptable, drop the exclusion: `membership_one_open` still forbids two open spells, the rebuild function
   guarantees the rest, and 16a/16b fall back to `membership_pair` scans of the chat.
10. **Privacy**: membership history with prize-hunter flags profiles identifiable people; the owner decides
   `store_names` and who may read `tg`.
