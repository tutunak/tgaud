# Verification on PostgreSQL 18.6

Everything below ran on 2026-11-23 in podman, container `audience-pg-test`, image
`docker.io/library/postgres:18.6-alpine3.24` (local, no network), port bound to 127.0.0.1:55432 only, with the
production server's limits: `--memory 512m`, `shared_buffers=64MB work_mem=2MB maintenance_work_mem=32MB
autovacuum_work_mem=16MB effective_cache_size=256MB max_parallel_workers=0 max_parallel_workers_per_gather=0
max_parallel_maintenance_workers=0 max_connections=30`. A PostgreSQL 17 container was started first, before the
conductor's update; nothing was checked on it (only `select version()`), it was removed and every check ran on 18.6.

Scripts and raw outputs are in `evidence/`; numbers below are copied from those outputs.

| Step | Script | Output |
|---|---|---|
| tenant login and database | `evidence/00-setup-tenant.sql` (superuser) | - |
| schema as the tenant, then `admin.sql` | `schema.sql`, `admin.sql` via `evidence/run-fixture.sh` | grants applied for bi_reader, collect_writer |
| fixture | `evidence/10-fixture.sql` (tenant; snapshots written as `collect_writer`) | loads without error |
| history cases | `evidence/20-history-cases.sql` | `evidence/out-history-cases.txt` |
| constraints and permissions | `evidence/30-constraints.sql` | `evidence/out-constraints.txt` |
| every block of `queries.sql` | `evidence/40-run-queries.sh` | `evidence/out-fixture-queries.txt` |
| chat_day invalidation | `evidence/50-invalidation.sql` | `evidence/out-invalidation.txt` |
| scale data | `evidence/60-gen-scale.sql` (superuser loader) | `evidence/out-scale-gen.txt` |
| scale measurements | `evidence/70-scale-measure.sh` (tenant) | `evidence/out-scale-measure.txt` |
| scale database to the final schema | `evidence/65-scale-migrate.sql`, `evidence/66-scale-migrate.sql` (tenant) | in `out-scale-remeasure.txt` |
| scale re-measurements | `evidence/75-scale-remeasure.sh`, `76-scale-r1.sh`, `77-scale-r1-modes.sh` | `evidence/out-scale-remeasure.txt`, `out-scale-r1.txt`, `out-scale-r1-modes.txt` |

Rerun: `sh evidence/run-fixture.sh && psql ... -f evidence/30-constraints.sql && sh evidence/40-run-queries.sh`
(the container must mount `final/` at `/work`).

## 1. Loading as the tenant

- Role `audience`: `rolsuper = f, rolcreatedb = f, rolcreaterole = f`, owner of its database and of `public`.
- `btree_gist` is trusted on 18.6 (`pg_available_extension_versions.trusted = t` for 1.2-1.8), so the tenant
  creates it. `schema.sql` loaded with `ON_ERROR_STOP=1` and no error; with no optional role present it printed
  `grants applied for: no role yet`. `admin.sql` (superuser) created `bi_reader` and `collect_writer` and printed
  `grants applied for: bi_reader, collect_writer`. On a second database where the roles existed, `schema.sql` applied
  the grants itself.

## 2. Fixture: one chat per history case

"Now" is 2026-11-23 12:00 UTC. Bot admin everywhere from 2026-08-01; a global outage gap 2026-08-10 00:00 to
2026-08-11 12:00. Spells were written by hand from the state table (design.md 4); they took their times from the
events they name. Output: `evidence/out-history-cases.txt`. All rows were accepted by the constraints.

| Case (chat) | Evidence | Rule | Spells (expected = stored) |
|---|---|---|---|
| H01 join | join 09-01 10:00 | E1 | open, event, joined 09-01 10:00 |
| H02 leave | join 09-01, leave 09-20 | E1, E5 | event 09-01 .. exact 09-20, left |
| H03 rejoin | join 09-01, leave 09-05, join 09-10 | E1, E5, E2 | 09-01 .. 09-05 left; open from 09-10 |
| H04 ban from in | join 09-01, member -> kicked 09-08 by admin | E5 | 09-01 .. exact 09-08, kicked |
| H05 ban from out | join 09-01, leave 09-06, left -> kicked 09-07 (`ban_out`) | E5, E11 | one spell 09-01 .. 09-06 left; the ban changes nothing |
| H06 unban | join 09-01, kicked 09-04, kicked -> left 09-09 (`unban`) | E5, E11 | one spell 09-01 .. 09-04 kicked; no new join |
| H07 restricted | join 09-01; member -> restricted(true) 09-03 (`restrict`); restricted(true -> false) 09-05 (`restrict_out`); restricted(false -> true) 09-12 (`join`) | E1, E8, E5, R1 | 09-01 .. exact 09-05 left (last confirmed 09-03); open from 09-12 |
| H08 left-censored | first event member -> left 09-15 | E6 | start inferred, known_from = left_at = 09-15 10:00 |
| H09 seeded | good snapshot 09-02, seen 00:10, Telegram date 2024-05-05 | S1 | open, snapshot, known_from 09-02 00:10, hint 2024-05-05 |
| H10 probe out | seen 09-02 00:10; absent from good snapshot 09-20; probe 09-21 says left | S1, A1, P3 | closed by probe, gone, bounds 09-02 00:10 .. 09-21 00:00 |
| H11 failed probe | seen 09-02; absent from good snapshot 09-20; probe timed out 3 times | S1, A1, P4 | open, suspect since 09-20 00:00 |
| H12 missed leave + join | join 09-01; join (old status left) 09-15 | E1, E4 | 09-01 .. inferred/gone, bounds 09-01 10:00 .. 09-15 10:00; open from 09-15 |
| H13 migration | joins in old group -400013 on 09-01 (2013) and 09-02 (2014); migration 09-10 12:00; echo joins in the new id at 12:00:05; 2014 leaves 09-15 | M1, E3, E5 | after `tg.move_chat_history`: all 5 events under the new id (`source_chat_id` = -400013 on the moved ones), old chat row gone, alias row, coverage periods adjacent; 2013 open from 09-01 (one spell, not two); 2014 09-02 .. exact 09-15 |
| H14 outage | join 08-05; leave fell in the outage; post-outage probe 08-11 13:00 says left; another join 08-12 | E1, G1, P3 | 08-05 .. probe/gone, bounds 08-05 10:00 .. 08-11 13:00; open from 08-12 |

The analytics chats: A (channel "Alpha news", linked to B), B (its discussion supergroup), C (supergroup), D
(channel). 33 people, a bot (3001) and a deleted account (3002) in A, two campaigns (owner giveaway G1 on A+B,
2026-11-01 .. 11-08; Telegram giveaway G2 on C, 10-01 .. 10-05), four invite links (two masked, two bot-made),
15 snapshots (one partial), 4 probes, join requests, posts, activity, count samples.

## 3. Constraints and permissions

`evidence/out-constraints.txt`: 26 rejections, 17 acceptances, nothing accepted that should not be.

Accepted: join, leave, join of one user in one second = 3 events; a re-delivered update adds 0 rows
(`ON CONFLICT DO NOTHING` on the update identity); every fixture row; `bi_reader` reads all 16 `stats` relations
(v_overlap 324 rows, v_campaign_summary 3, v_chat_status 18, ...).

Rejected, with the constraint that did it:

| Invalid row | Rejected by |
|---|---|
| **two open spells for one (chat, user)** | `one_stay_at_a_time` (exclusion; `membership_one_open` backs it up) |
| **exact join time with start_source = snapshot** | `exact_join_iff_event_start` |
| **exact join time without its event id** | `exact_join_has_event_id` |
| **join event id that does not exist** | `join_event_exists` (FK to `member_event (event_id, event_at)`) |
| **join time different from its event's time** | `join_event_exists` |
| **an end without a reason** | `end_has_reason` |
| **a membership row for a chat that does not exist** | `membership_chat_id_fkey` |
| two overlapping closed stays | `one_stay_at_a_time` |
| exact exit time without its event | `exact_exit_iff_event_end` |
| exit bounds in the wrong order | `exit_bounds_order` |
| suspect flag on a closed spell | `suspect_only_open` |
| migration end with reason `left` | `migration_end_reason` |
| the same update stored twice | unique (`update_id`, `event_at`) |
| unknown status `banned` | `member_event_new_status_check` |
| event outside every partition (2030) | "no partition of relation found" (the service then calls `ensure_partitions`) |
| overlapping coverage periods | coverage exclusion |
| raw join request that still has `bio` | `raw_update_payload_check` |
| quality on an unfinished snapshot | `snapshot_check2` |
| probe outcome without an answer time | `probe_check1` |
| two pending probes for one (chat, user) | `probe_one_pending` |
| a link stored both masked and as a digest | `invite_link_check` |
| an owner giveaway marked as a Telegram observation | `campaign_check1` |
| `bi_reader` reading `tg.membership` | permission denied for schema tg |
| `bi_reader` calling `tg.campaign_entrant` | permission denied for schema tg |
| `collect_writer` reading `tg.membership` | permission denied for table membership |
| `collect_writer` setting `snapshot.quality` | permission denied (the service grades) |

The snapshot contract itself ran as `collect_writer`: insert chat (ON CONFLICT), insert snapshot RETURNING id,
insert members, update the finish columns; the identity column needed no extra grant.

## 4. Every block of queries.sql on the fixture

Run with `as_of = 2026-11-23 12:00+00`; scope A-D unless noted. Expected values were worked out by hand from the
fixture before the run; "got" is `evidence/out-fixture-queries.txt`. All 31 checks matched. The fixture was
rerun from an empty database after every change of section 6; the final run is the one in `evidence/`.

| Block | Check | Expected | Got |
|---|---|---|---|
| R2 | pair rows for 18 tracked chats | 18*19/2 = 171 | 171; second run with `REFRESH ... CONCURRENTLY` inside the transaction: ok |
| 1 | A∩B, A∩C, A∩D, B∩C, B∩D, C∩D; sizes A, B, C, D | 4, 2, 1, 3, 1, 1; 10, 6, 5, 2 | same; A∩B 40.00 % of A, 66.67 % of B, Jaccard 0.3333; coverage A 80 % (12 of 15) |
| 2a | distribution, all tracked (25 people) | k=1: 20, k=2: 3, k=3: 1, k=4: 1 | same |
| 2a | distribution, scope A-D | 10, 3, 1, 1 | same |
| 2b | user 1001; user 1006 | 4 chats (2 seeded with hints, 2 exact joins); 2 stays in A | same |
| 3 | exclusive A, B, C, D; distinct total | 6, 1, 2, 1; 15 | same |
| 3 | scope A, B: exclusive; total | A 6, B 2; 12 | same |
| 4a | G1 chat A (only_new_members): entrants, first-time, returners, eligible (first-time only), quick exits, in at draw, exit after draw, still in | 6, 5, 1, 5, 3, 4, 2, 2 | same |
| 4a | G1 chat B: entrants, quick exits (kicked), quick unknown, in at draw, draw unknown, after-draw unknown, still in | 3, 1 (1), 1, 1, 1, 1, 1 | same |
| 4a | G2 chat C: entrants, quick exits, in at draw, still in | 2, 1, 1, 1 | same |
| 4b | G1 per person: persons, multi-chat, first-time, eligible, quick exit, in at draw, exit after draw, still in | 8, 1, 7, 7, 4, 5, 2, 3 | same |
| 5 | A week 10-19: joins / short; A week 11-02 | 2 / 1; 6 campaign joins, 0 free | same |
| 5 | B week 10-19; C week 11-23 (raid); A week 08-10 flags | 2 / 2; 6 / 6 (5 kicked); gap | same, `gap:service_down` |
| 6a | serial short-stayers | only 1022 (2 stays, 2 chats) | same |
| 6b | bursts with burst_users = 2 | (A, B) at 10-20 10:00, 2 people | same |
| 7a | A: exits, median, p90, share <= 48 h | 7, 24 h, 201.6 h, 4/7 | 7, 24.00, 201.60, 0.5714 |
| 7a | B median; C median; D median, p90 | 22.97 h; 19 min 50 s; 1573 h, 1585 h | 22.97; 0.33; 1573.00, 1585.00 |
| 7b | A week 11-02 at 7 d; A week 10-19 at 7/30 d; A week 08-31 at 30/90 d | 2 kept, 4 out; 1/1, 0/2; 1/1, immature | same |
| 7b | B week 11-02 at 7 d | 1 kept, 1 out, 1 unknown (bounded exit), 50 % | same |
| 8 | A sources: bot link G1 (joins, 7 d kept/out); unattributed (7 d; 30 d) | 6 (2/4); 4 (3/1; 1/3) | same; admin add, folder, masked link, request 1 each, all kept |
| 9a | A: 08-01 corrections; 11-05 joins/exits; 11-03 rejoins; 11-23 reported/known/residual | 5; 1/1; 1; 15/12/+1 | same |
| 9a | B: 11-13 corrections_out (probe); 11-20 messages (5 user + 4 automatic forwards) | 1; 9 | same |
| 9b | A samples in 24 h: mismatch; residual | 2, 2, 2, 3; 0, 0, 0, 1 | same |
| 10a | rejoins per chat | H03, H07, H12, A: 1 each; H13 none | same |
| 10b | flows within 7 days | A -> B 5, C -> D 1 | same (median 0.08 h; 122 h) |
| 11 | A/B: subscribers, group, both, %, group only, active, active subscribers | 10, 6, 4, 40 %, 2, 3, 3 | same |
| 12 | A weeks 10-05, 10-26, 11-16 | 1 approved in 60 min, kept 30 d; 1 unknown; 1 pending | same |
| 13 | raids with raid_min_joins = 5 | C 11-23 03:00: 6 joins, 5 kicked in 1 h, 83.3 % | same |
| 14 | B: members, active, short stays, wrote; C | 6, 3, 3, 1; 5, 1, 7, 1 | same |
| 15 | D post 10: after / before; post 11 | 1 / 1; 0 / 0 | same |
| 16a | A at 11-07; A at 07-15 (before the bot); B at 11-07 | 11 known; 3 by Telegram date + "not covered"; 6 known + 1 unsure | same |
| 16b | B at 11-07 list | 6 `in`, 1019 `unknown` | same |
| 16c | pair matrix at 11-07 | A 11, B 6, C 5, D 4; A∩B 4, A∩C 2, B∩C 3, others 1 | same |
| 16d | members at the end of 11-07 from chat_day: A, B (unsure), C, D | 12, 7 (1), 5, 4 | same; equal to 16a at 11-08 00:00 (A 12; B 6 known + 1 unsure) |

**chat_day invalidation** (`evidence/out-invalidation.txt`): after a full R1, a late snapshot-seeded spell for A on
11-04 and a dirty marker from 11-04. R1 then recomputed A from 11-04 (11-03 kept its old `refreshed_at`, 11-04 got
`corrections_in = 1`), B only from 11-21 (2 days back), and cleared the marker. Undoing it the same way restored 0.

**Retention R3**: snapshots 2 (B, older than the last two good ones) and 15 (partial) lost their member rows
(36 -> 30 rows); their `tg.sighting` evidence stayed.

## 5. Scale

**Data** (`evidence/60-gen-scale.sql`, database `audscale`, schema loaded by the tenant, data by the superuser
loader; full size, not scaled down): 200 chats (67 channels, 133 supergroups, 66 channel/group pairs); chat 1 a
channel with **1,000,000 open spells**; **3,000,000 users** (6,000 bots), 2,994,000 humans with an open spell in
**338,478 distinct chat sets**; chats per user 1: 1,301,169, 2: 1,145,386, 3-9: 451,246, 10-19: 81,120,
20-49: 15,079. `membership` 8,515,048 rows (7,193,768 open; 30 % of the open ones started by an event, 70 % seeded
by a snapshot; 0.2 % suspect), **`member_event` 5,180,776 rows over a year** in monthly partitions, a giveaway in
chat 1 with 75,379 entrants (30k stayed, 30k quick exits, 10k later exits), `count_sample` 864,200 (30 days of
10-minute samples), `activity_daily` 1,073,199, 73k posts, 50k join requests. On disk: `membership` 3.1 GB with
its indexes, `user_chats` 336 MB, `tg_user` 283 MB, database 5.4 GB, against 64 MB of shared buffers in a 512 MB
container: every big query reads from disk or the page cache.

Loader only (not the service): FK triggers off (`session_replication_role = replica`; the fixture proved the
FKs), `work_mem` 128 MB and `maintenance_work_mem` 192 MB for the bulk sorts and index builds; CHECK constraints
and generated columns stayed on. Load 14:41 wall, of it 11:09 rebuilding the indexes after the load: the GiST
exclusion `one_stay_at_a_time` **572 s**, every btree 2-27 s. VACUUM ANALYZE 21 s.

**Measured** as the tenant with the server's settings (`work_mem` 2 MB; the batch blocks raise it with
`SET LOCAL`: R1 16 MB, R2 and 16c 64 MB). "First" is the first design, "final" the SQL in `queries.sql` now.
Times are the statement's wall time from `\timing` / `EXPLAIN (ANALYZE, BUFFERS)`; plans in
`evidence/out-scale-measure.txt` (first), `out-scale-remeasure.txt`, `out-scale-r1.txt`, `out-scale-r1-modes.txt`.

| Query | Use (budget) | First | Final | What changed |
|---|---|---|---|---|
| R2 audience rollup: `user_chats`, `chat_set`, pair matrix, unique, chats per user | hourly batch (minutes) | 45 s; concurrent 67 s | **24 s** (MV 8.9 + sets 7.1 + pairs 7.8); concurrent **34 s** | `tg.chat_set`; human filter by `NOT IN` over `tg_user_not_human` instead of a 3M-row join |
| R1 chat_day, first build (365 days x 200 chats) | once, or a far-back dirty range (minutes) | 45 s | **37 s** | full mode: one set-based pass (a per-chat pass took 12:52: 200 heap passes) |
| R1 chat_day, normal run (2 days back) | every 5 min | 12.3 s for 1 day | **9.1 s** (one day alone 8.4 s) | per-chat index range scans (LATERAL), one pass over exits, `ANALYZE` of the target list |
| 1 overlap matrix, all 40,000 ordered pairs | CLI/BI (seconds) | - | **0.11 s** | reads the rollup |
| 2a chats per user, all chats / scope of 10 | CLI | 22.2 s / - | **1.5 s / 0.7 s** | from `tg.chat_set` (338k rows) instead of `user_chats` (3M) |
| 2b one user's chats and stays | CLI | 0.11 s | 0.11 s | - |
| 3 unique per chat, all chats / scope of 10 | CLI | 12.2 s / 18.2 s | **2.7 s / 1.1 s** | from `tg.chat_set` |
| 4a giveaway report, 1M chat, 75k entrants | CLI/BI | 4.6 s | 4.6 s | - (index range on the window, three index probes per entrant) |
| 4b the same per person | CLI | 4.7 s | 4.7 s | - |
| 11 channel vs discussion group, 66 pairs | CLI | - | **5.7 s** (9.0 s from `chat_set`) | counts from three cells of `stats.overlap`; the rest is the live check of 357k activity rows |
| 16a as-of counts, 1M chat, D = 30 days ago | CLI | 6.7 s | **2.2 s** | `NOT IN` over `tg_user_not_human`; still a seq scan of the 1M chat (the right plan) |
| 16a as-of counts, 1M chat, D before the bot | CLI | 12.8 s | **1.9 s** | same |
| 16b as-of list, 1M chat (974k rows) | CLI export | 3.9 s | 3.9 s | - |
| 16c pair matrix at a past date, 200 chats | on-demand batch (minutes) | 68.7 s | **29.7 s** | `NOT IN` filter |
| 16d members at the end of a day, 200 chats | CLI/BI | - | **0.17 s** | new: running sum of daily deltas in `stats.chat_day` |

Notes:
- The generator clamped exits that would fall after the end of the data to its last minute, so the last day holds
  about 154k exits (a realistic day: about 3.4k). That inflates the "normal run" of R1 by roughly that factor; the
  plan shows the time in heap reads of those rows.
- R1 both modes give the same rows: the checksum of every chat_day row from 11-21 (without `refreshed_at`) after
  the full build equals the one after the per-chat run (600 rows, md5 `3dcf7f99779ec0a7c60d76d6b86fd616` both times, `evidence/out-scale-r1-modes.txt`).
- `REFRESH MATERIALIZED VIEW CONCURRENTLY` runs inside R2's transaction on 18.6 (fixture and scale).
- Wall time of the scale step: generation 14:41, first measurement 5:35, then about 35 minutes of redesign and
  re-measurement (the brief's ~30 minutes was exceeded by the redesign rounds, not by the data size).

## 6. Changes made during verification

Schema and SQL, in order:
1. Role creation moved out of `schema.sql` into `admin.sql`; grants in `tg.apply_grants()`, applied by whichever
   runs second (conductor's update: the tenant cannot create roles). Every check moved from 17 to 18.6 under the
   production limits.
2. Spell constraints named, so a violation says which rule broke (`end_has_reason`, not `membership_check4`).
3. `membership_found (chat_id, known_from) WHERE start_source <> 'event'`: chat_day corrections and starts, and
   the "Telegram's join date" branch of item 16.
4. Scale round 1 (2a 22 s, 3 12-18 s, 16a 6.7-12.8 s, R1 one day 12.3 s):
   - `tg.chat_set`, built by R2 from `user_chats`. The pair matrix, 2a and 3 for any scope read it.
   - `stats.chat_day` gained daily deltas (`members_started/ended`, `unsure_started/ended`) and `v_chat_day`
     running sums. New block 16d answers "members at the end of day D" from them.
   - Partial index `tg_user_not_human`. Big scans filter humans with `NOT IN` over it instead of joining 3M
     users. This covers `user_chats`, R1, 16a and 16c.
   - R1 got `ANALYZE` of its target list and per-chat index range scans.
5. Scale round 2 (R1 full build 12:45, 11 9 s):
   - R1 merged its scans (joins together with human starts; exits together with unsure starts).
   - R1 got a set-based full mode, chosen when more than 30 chat-days per chat are to be recomputed.
   - 11 reads its counts from `stats.overlap`.
6. `queries.sql`: `\echo` lines may not contain an apostrophe (psql reads it as a quote even in a skipped branch).
7. Fixture only: casts on a `UNION ALL` of literals. A giveaway message in a supergroup does not belong in
   `tg.post` (channel posts only).

The scale database got changes 3-5 through `evidence/65-scale-migrate.sql` and `66-scale-migrate.sql`, not a
reload. A stopped run applied step 65 twice; 66 recreates the view that the second run dropped. The fixture was
rebuilt from the final `schema.sql` after each change, and its results stayed identical to the hand values.

## 7. Cleanup

The container `audience-pg-test` was removed at the end (`podman rm -f audience-pg-test`).
