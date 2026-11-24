# Audience statistics bot: summary for the owner

A Bot API bot, admin in every tracked chat, records every join and leave into PostgreSQL 18. Your `tgstats collect`
script adds the members who were there before the bot. Nothing is ever deleted. Every number says how complete it
is. Details: `design.md`. SQL: `schema.sql`, `queries.sql`. Proof on a real PostgreSQL 18.6: `verify.md`.
Tested at full size (one chat with 1M members, 3M people, 5M events) under your server's limits (512 MB, small
work memory): reports answer in 0.1-6 seconds (the overlap at a past date is a 30-second batch), the hourly and
5-minute batch jobs take 10-40 seconds.

## The tables

Schema `tg` holds data about people. Schema `stats` holds only counts, for your BI tool.

| Table | What it holds | Key columns | Written by |
|---|---|---|---|
| `tg.chat` | every chat, its type, title, linked discussion group, whether it is tracked | `chat_id` | bot; collect script adds missing chats |
| `tg.chat_alias` | old group id -> new supergroup id after a migration | `old_chat_id`, `chat_id` | bot |
| `tg.coverage` | when the bot was admin in a chat | `chat_id`, `period` | bot |
| `tg.gap` | periods with lost or doubtful events, with a reason | `chat_id` (empty = all), `period`, `reason` | bot, reconciler |
| `tg.tg_user` | one row per person: bot or not, deleted, latest username and names | `user_id` | bot, reconciler |
| `tg.member_event` | every join, leave, ban, restriction, promotion, as Telegram sent it | `update_id`, `event_at`, `chat_id`, `user_id` | bot |
| `tg.membership` | one row per stay of a person in a chat, with start, end and how sure we are | `chat_id`, `user_id`, `known_from`, `left_before` | bot, reconciler |
| `tg.snapshot` | one member listing by the collect script | `snapshot_id`, `chat_id`, `started_at` | collect script; service grades it |
| `tg.snapshot_member` | the people in one listing, with Telegram's join date | `snapshot_id`, `user_id` | collect script (kept for the last 2 good listings) |
| `tg.sighting` | when the listings saw a person, kept forever in compact form | `chat_id`, `user_id`, `first_seen_at` | reconciler |
| `tg.probe` | one-person checks with `getChatMember`, queued and answered | `chat_id`, `user_id`, `outcome` | reconciler, prober |
| `tg.reconcile_run` | what each reconciliation changed, as counts | `snapshot_id` | reconciler |
| `tg.count_sample` | Telegram's member count every 10 minutes next to ours | `chat_id`, `sampled_at` | sampler |
| `tg.campaign`, `tg.campaign_chat` | giveaways and ads: your own (annotations) and Telegram's (observations) | `campaign_id` | CLI, bot |
| `tg.invite_link` | links people joined by, with name, creator, campaign | `invite_link_id` | bot, CLI (bot links) |
| `tg.join_request` | requests to join, and their outcome | `chat_id`, `user_id`, `requested_at` | bot |
| `tg.post` | channel post times, no text | `chat_id`, `message_id` | bot |
| `tg.activity_daily` | messages per person per day in groups, no text | `chat_id`, `user_id`, `day` | bot |
| `tg.unattributed_daily` | group messages sent as a channel or anonymous admin | `chat_id`, `day` | bot |
| `tg.raw_update` | the raw membership updates, 30 days, without bio | `update_id` | bot |
| `tg.update_receipt`, `tg.bot_state` | which updates were processed, the polling offset | `update_id` | bot |
| `tg.user_chats` | for each person, the set of tracked chats they are in now | `user_id` | batch job, hourly |
| `tg.chat_set` | the distinct chat sets with how many people have each (feeds overlap, chats per user, unique) | `chat_ids` | batch job, hourly |
| `tg.rollup_dirty` | which days of `stats.chat_day` must be recomputed | `chat_id` | bot, reconciler |
| `stats.chat_day` | per chat and day: joins, exits, bans, short stays, corrections, Telegram's count | `chat_id`, `day` | batch job, every 5 min |
| `stats.overlap` | members shared by every pair of chats | `chat_a`, `chat_b` | batch job, hourly |
| `stats.unique_per_chat`, `stats.chats_per_user` | exclusive members per chat; how many people are in k chats | `chat_id`; `chat_count` | batch job, hourly |
| `stats.v_*` (11 views) | the BI views: status, overlap, growth, drift, campaigns, cohorts, sources, requests, gaps | - | (views) |

## How history is recorded

A stay starts when a person comes in and ends when they go out. We record how we know each end.

| Case | What is stored | The stay |
|---|---|---|
| Join | a `join` event | a new stay with the exact join time |
| Leave | a `leave` event | the stay ends at the exact time, reason `left` |
| Rejoin | a second `join` event | a second stay; the first one stays as it was |
| Ban of a member | a `kick` event | the stay ends at the exact time, reason `kicked` |
| Ban of a non-member, unban | `ban_out`, `unban` events | no change: no exit, no new join |
| Member from before the bot | a row in the collect script's listing, with Telegram's join date | a stay "first seen at the listing"; Telegram's join date kept as a hint |
| First thing we see is a leave | a `leave` event | a stay with an unknown start, ending at the leave |
| Missed leave, found by a check | absent from a good listing, then `getChatMember` says out | the stay ends "somewhere between the last sighting and the check" |
| Missed leave, found by a new join | a `join` event while the stay is still open | the old stay ends "somewhere before this join"; a new stay starts |
| Check failed | the probe errors out | the stay stays open, marked suspect, counted apart in reports |
| Drift | our count and Telegram's count every 10 minutes | a mismatch or residual alert; probes and a new listing repair it |
| Outage longer than 24 h | a `gap` row | open stays become suspect until a listing or a check confirms them |
| Group became a supergroup | an alias row | all history moves to the new id; no double counting |

## The analytics

Every report shows its time, the chats it covers, how complete the data is, and any gap in its window.

1. **Overlap of every pair of chats.** How many members of A are also in B, for every pair. You learn which chats
   share an audience: for ad swaps, merges, cross-promotion. Each person's set of chats is grouped, then pairs are
   counted once per distinct set. Shows % of A, % of B and Jaccard. Reads `stats.overlap`, `tg.chat_set`.
2. **Chats per user.** How many of your chats each person is in, and the spread (1 chat, 2 chats, ...). You learn how
   concentrated your audience is; a lookup shows one person's chats and stays. Counted from the distinct chat sets.
   Reads `tg.chat_set`, `tg.membership`.
3. **Unique users per chat.** How many members of a chat are in none of the other chats you pick, plus the distinct
   total. You learn the reach each chat adds. Counted inside the chosen chats only. Reads `tg.chat_set`.
4. **Giveaway joiners.** Who joined during a giveaway, who left within 48 hours of joining, who left within 48 hours
   of the draw, who is still in; first-time joiners apart from returners (when the giveaway is for new members only,
   the eligible count is the first-time joiners). You learn the real yield of a giveaway.
   Only confirmed exits count; unsure ones are shown apart; multi-chat giveaways are counted per chat and per
   person. Reads `tg.campaign`, `tg.campaign_chat`, `tg.membership`.
5. **Short stays without a campaign.** Joined and left within 48 hours, per chat and week, outside campaign windows.
   Shows giveaway-like churn where you entered no campaign. Reads `tg.membership`, `tg.campaign`.
6. **Serial short-stayers and join bursts.** People with several short stays; many people joining two of your chats
   within 10 minutes. Finds prize hunters and giveaways you did not know about. Reads `tg.membership`.
7. **Tenure and retention.** How long people stay; how many of a week's joiners are still in after 7, 30, 90 days.
   Only finished horizons count; unknown outcomes apart. Reads `tg.membership`.
8. **Join sources.** Joins and 7/30-day retention per link, campaign, request, folder link, admin add. Shows which
   source brings people who stay. Reads `tg.membership`, `tg.invite_link`, `tg.campaign`.
9. **Daily growth and drift.** Joins, exits, net, and how far Telegram's count moved beyond our events. The trust
   check. Reads `stats.chat_day`, `tg.count_sample`.
10. **Rejoiners and flow.** Who came back; who joined chat A and then chat B within 7 days. Shows habits and which
    chat feeds which. Reads `tg.membership`.
11. **Channel vs its discussion group.** Share of subscribers in the group, group members who don't subscribe, and
    who writes. Reads `stats.overlap`, `tg.activity_daily`.
12. **Join requests.** Requests, approvals, time to approve, who stayed 30 days. Reads `tg.join_request`.
13. **Raids.** Minutes with joins far above normal, and the share banned within an hour. Reads `tg.membership`.
14. **Active vs silent.** Who wrote in the last 30 days; did short-stayers ever write. Reads `tg.activity_daily`.
15. **Leaves after posts.** Leaves within 6 hours after each channel post, against the 6 hours before.
    Reads `tg.post`, `tg.membership`.
16. **Who was in a chat on a date.** Members at the end of any day, instantly (`stats.chat_day`); the exact count
    and the list at any moment, with sure, unsure and "Telegram says earlier" (`tg.membership`, an index on the
    stay's time range); the overlap matrix at a past date (a batch of about a minute).

## What your collect script must write

Use the database role `collect_writer`. Same chat ids as today (`utils.get_peer_id`).

1. If the chat is new: insert it into `tg.chat` (id, type, title, username), skipping if it exists.
2. Insert a `tg.snapshot` row: chat, start time, method, Telegram's count before listing. Commit.
3. For every page: insert `tg.snapshot_member` rows: user id, the time the page arrived, Telegram's join date
   (`participant.date`), inviter and request flag when Telethon has them, bot and deleted flags, names if you keep
   them. Duplicates are skipped.
4. At the end: set `finished_at`, the count after listing, the number of people written, and `complete`
   (false if interrupted). That marks it finished. The service does the rest.

## Metrics (Prometheus, then New Relic)

| Metric | Tells you | Alert to start with |
|---|---|---|
| `tgaud_poll_last_success_timestamp_seconds` | the bot is receiving updates | older than 2 minutes |
| `tgaud_update_lag_seconds` | how late updates arrive | over 1 hour; over 12 hours is urgent (Telegram keeps 24 h) |
| `tgaud_chat_bot_admin`, `tgaud_chat_permission_ok` | the bot still has its rights in each chat | 0 |
| `tgaud_last_transition_timestamp_seconds` | when each chat last had a join or leave | nothing for 6 h in a big chat |
| `tgaud_chat_drift_residual_24h`, `_mismatch` | our count against Telegram's | above max(20, 0.2 %) |
| `tgaud_chat_members_reported`, `_known`, `_suspect` | sizes and doubtful members | suspect above 1 % |
| `tgaud_chat_events_total`, `tgaud_chat_short_stays_total` | joins, leaves, bans, short stays per chat | joins 10x normal |
| `tgaud_probe_queue`, `tgaud_probe_oldest_age_seconds` | the check backlog | oldest over 24 h |
| `tgaud_job_last_success_timestamp_seconds`, `tgaud_rollup_as_of_timestamp_seconds` | jobs and reports are fresh | twice the interval |
| `tgaud_api_requests_total`, `tgaud_ingest_failures_total`, `tgaud_lost_seconds_total` | API errors, broken updates, lost time | any failure or loss |
