-- queries.sql: the analytics of decisions.md (items 1-16) and the rollup refresh SQL (R1-R4).
--
-- Run all:      psql -d audience -f queries.sql
-- Run one item: psql -d audience -v only=4 -v campaign=1 -f queries.sql       (only = 1..16, 4a, 16c, R1, ...)
-- Plans:        psql -d audience -v only=16a -v explain='EXPLAIN (ANALYZE, BUFFERS)' -f queries.sql
--
-- Parameters are psql variables (-v name=value); the service and the CLI send the same statements with
-- $1-style placeholders in the same places. Defaults:
--   as_of          now          the report time; frozen once at the start so every block agrees
--   scope          {}           chat ids to report on, e.g. '{-1001000000001,-1001000000002}'; {} = all tracked
--   since          as_of - 90 d start of the history window (joins, exits, posts, requests)
--   hours          48           H: a short stay / quick exit is an exit within H hours
--   campaign       0            campaign id for item 4; 0 = every campaign
--   chat           0            one chat for item 16 (as-of membership); chat A of item 1b
--   chat_b         0            item 1b: chat B of the pair
--   side           shared       item 1b: shared (in both), only_a, only_b
--   after_user     0            item 1b: the page starts after this user id
--   page_size      100          item 1b: rows per page
--   user           0            one user id for item 2b (lookup)
--   day            as_of        the date D of item 16
--   flow_days      7            item 10b: joined A, then B within this many days
--   burst_minutes  10           item 6b: joins of one user to several chats within this many minutes
--   burst_users    20           item 6b: report a burst from this many users
--   raid_min_joins 20           item 13: a minute is a raid from this many joins (and 10x the baseline)
--   raid_hours     24           item 13: how far back to look
--   post_hours     6            item 15: leaves within this many hours after a post
--   active_days    30           items 11, 14: active = wrote in the last N days
--   lookback_days  2            R1: always recompute this many days back (short stays mature after 48 h)
-- Conventions: "humans" = not a bot and not a deleted account (big scans test NOT IN against the tiny
-- partial index tg_user_not_human instead of joining every user); only tracked chats. Every report shows its
-- as-of time, and the ones over a time window show tg.window_flags (gaps, missing coverage) per chat.
-- Work memory: CLI and web queries run with the server's 2 MB; the batch blocks (R1, R2, 16c) raise it
-- with SET LOCAL as stated there.

\if :{?only}           \else \set only all           \endif
\if :{?explain}        \else \set explain ''         \endif
\if :{?as_of}          \else \set as_of now          \endif
\if :{?scope}          \else \set scope '{}'         \endif
\if :{?since}          \else \set since ''           \endif
\if :{?hours}          \else \set hours 48           \endif
\if :{?campaign}       \else \set campaign 0         \endif
\if :{?chat}           \else \set chat 0             \endif
\if :{?chat_b}         \else \set chat_b 0           \endif
\if :{?side}           \else \set side shared        \endif
\if :{?after_user}     \else \set after_user 0       \endif
\if :{?page_size}      \else \set page_size 100      \endif
\if :{?user}           \else \set user 0             \endif
\if :{?day}            \else \set day ''             \endif
\if :{?flow_days}      \else \set flow_days 7        \endif
\if :{?burst_minutes}  \else \set burst_minutes 10   \endif
\if :{?burst_users}    \else \set burst_users 20     \endif
\if :{?raid_min_joins} \else \set raid_min_joins 20  \endif
\if :{?raid_hours}     \else \set raid_hours 24      \endif
\if :{?post_hours}     \else \set post_hours 6       \endif
\if :{?active_days}    \else \set active_days 30     \endif
\if :{?lookback_days}  \else \set lookback_days 2    \endif

SELECT :'as_of'::timestamptz AS as_of_ts,
       coalesce(nullif(:'since', '')::timestamptz, :'as_of'::timestamptz - interval '90 days') AS since_ts,
       coalesce(nullif(:'day', '')::timestamptz, :'as_of'::timestamptz) AS day_ts,
       CASE WHEN :'scope' = '{}' THEN (SELECT array_agg(chat_id ORDER BY chat_id) FROM tg.chat WHERE tracked)::text
            ELSE :'scope' END AS scope_ids
\gset
\echo as_of :as_of_ts   since :since_ts   scope :scope_ids

------------------------------------------------------------------------------------------------------------
-- 1. Overlap of every pair
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '1') AS run \gset
\if :run
\echo == 1. overlap of every pair
-- Q: how many members of chat A are also members of chat B, for every pair of chats in scope (diagonal = the
-- chat's own size, zero pairs included); % of A, % of B, Jaccard. Reads the audience rollup (R2), so the
-- answer is as of its as_of; coverage (known / Telegram's count) and suspect spells per chat beside it.
:explain
SELECT o.chat_a, o.title_a, o.chat_b, o.title_b, o.shared, o.size_a, o.size_b, o.pct_of_a, o.pct_of_b, o.jaccard,
       sa.coverage_pct AS coverage_a, sb.coverage_pct AS coverage_b, sa.suspect_open AS suspect_a, o.as_of
FROM stats.v_overlap o
LEFT JOIN stats.v_chat_status sa ON sa.chat_id = o.chat_a
LEFT JOIN stats.v_chat_status sb ON sb.chat_id = o.chat_b
WHERE o.chat_a = ANY (:'scope_ids'::bigint[]) AND o.chat_b = ANY (:'scope_ids'::bigint[])
ORDER BY o.chat_a, o.chat_b;
\endif

SELECT :'only' IN ('all', '1b') AS run \gset
\if :run
\echo == 1b. overlap drill-down: the people behind one pair, one page
-- Q: who are the people of one cell of item 1: in both :chat and :chat_b (side shared), only in :chat
-- (only_a) or only in :chat_b (only_b)? Reads tg.user_chats, which R2 refreshes in the transaction that
-- publishes the matrix, so a cell and its list describe the same people (both as of the rollup's as_of; a
-- join after the refresh changes neither until the next one). Paged by user id: :page_size rows after
-- :after_user. as_of is the published generation of the pair's cell; the web UI answers only while it is
-- the one its matrix page showed.
:explain
SELECT u.user_id, t.username, t.first_name, t.last_name, u.chat_count,
       (SELECT o.as_of FROM stats.overlap o
         WHERE o.chat_a = least(:chat, :chat_b) AND o.chat_b = greatest(:chat, :chat_b)) AS as_of
FROM tg.user_chats u
LEFT JOIN tg.tg_user t ON t.user_id = u.user_id
WHERE u.user_id > :after_user
  AND CASE :'side' WHEN 'shared' THEN u.chat_ids @> ARRAY[:chat, :chat_b]::bigint[]
                   WHEN 'only_a' THEN :chat = ANY (u.chat_ids) AND NOT :chat_b = ANY (u.chat_ids)
                   WHEN 'only_b' THEN :chat_b = ANY (u.chat_ids) AND NOT :chat = ANY (u.chat_ids)
                   ELSE false END
ORDER BY u.user_id
LIMIT :page_size;
\endif

------------------------------------------------------------------------------------------------------------
-- 2. Chats per user
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '2', '2a') AS run \gset
\if :run
\echo == 2a. chats per user: distribution within scope
-- Q: how many people are in 1, 2, 3 ... of the chats in scope? The total is the scope's reach.
-- Reads tg.chat_set (R2): the distinct sorted chat sets of all humans with their head counts, so the work is
-- per distinct set, not per person. stats.v_chats_per_user has the answer for all tracked chats.
:explain
WITH s AS (SELECT :'scope_ids'::bigint[] AS chats),
k AS (
  SELECT (SELECT count(*) FROM unnest(cs.chat_ids) c WHERE c = ANY (s.chats)) AS chat_count, cs.users
  FROM tg.chat_set cs, s
  WHERE cs.chat_ids && s.chats
)
SELECT chat_count, sum(users) AS users, round(100.0 * sum(users) / sum(sum(users)) OVER (), 2) AS pct_of_users,
       (SELECT max(as_of) FROM stats.rollup_run WHERE rollup = 'audience') AS as_of
FROM k GROUP BY chat_count ORDER BY chat_count;
\endif

SELECT :'only' IN ('all', '2', '2b') AS run \gset
\if :run
\echo == 2b. one user: every chat and every stay
-- Q: which tracked chats is this person in now, and what is their history in each (the owner's lookup)?
:explain
SELECT m.chat_id, c.title, CASE WHEN m.end_source IS NULL THEN 'in' ELSE 'out' END AS now,
       m.start_source, m.joined_at, m.joined_hint, m.known_from, m.join_route, coalesce(l.name, l.masked_link) AS link,
       m.end_source, m.end_reason, m.left_at, m.left_after, m.left_before, m.suspect_since,
       count(*) FILTER (WHERE m.end_source IS NULL) OVER () AS chats_now
FROM tg.membership m
JOIN tg.chat c ON c.chat_id = m.chat_id AND c.tracked
LEFT JOIN tg.invite_link l ON l.invite_link_id = m.invite_link_id
WHERE m.user_id = :user
ORDER BY m.chat_id, m.known_from;
\endif

SELECT :'only' IN ('all', '2c') AS run \gset
\if :run
\echo == 2c. one user: the campaigns they entered (web user lookup)
-- Q: in which campaigns did this person join a campaign chat inside the entry window, and what happened next
-- (item 4 per entrant: first-time, quick exit within H, in at the draw, left after it, still in at as_of)?
-- The rows of tg.campaign_entrant for :user; only campaigns where the person has a join in the window are
-- evaluated (an index probe on membership_user_join), so the lookup does not compute every entrant list.
:explain
SELECT x.campaign_id, c.name AS campaign, c.kind, x.chat_id, ch.title, x.joined_at, x.first_time, x.eligible,
       x.quick_exit, x.exit_reason, x.at_draw, x.exit_after_draw, x.still_in
FROM tg.campaign c
CROSS JOIN LATERAL tg.campaign_entrant(c.campaign_id, :hours, :'as_of_ts') x
LEFT JOIN tg.chat ch ON ch.chat_id = x.chat_id
WHERE x.user_id = :user
  AND EXISTS (SELECT 1 FROM tg.membership m JOIN tg.campaign_chat cc ON cc.chat_id = m.chat_id AND cc.campaign_id = c.campaign_id
              WHERE m.user_id = :user AND m.joined_at >= c.starts_at
                AND m.joined_at <= least(c.draw_at, :'as_of_ts'::timestamptz))
ORDER BY c.starts_at, x.campaign_id, x.chat_id;
\endif

SELECT :'only' IN ('all', '2d') AS run \gset
\if :run
\echo == 2d. one user: messages per group (web user lookup)
-- Q: how much did this person write in each group: days with a message and messages, in all and in the last
-- active_days, first and last day? Daily counts only, no text. As in 14: when as_of is earlier than the last
-- update ingest committed, only complete UTC days up to activity_through count.
:explain
WITH lim AS (
  SELECT CASE WHEN :'as_of_ts'::timestamptz >= coalesce((SELECT max(b.last_poll_ok_at) FROM tg.bot_state b), '-infinity'::timestamptz)
              THEN (:'as_of_ts'::timestamptz AT TIME ZONE 'UTC')::date
              ELSE (:'as_of_ts'::timestamptz AT TIME ZONE 'UTC')::date - 1 END AS last_day
)
SELECT a.chat_id, ch.title, count(*) AS days_active, sum(a.messages) AS messages,
       count(*) FILTER (WHERE a.day > lim.last_day - :active_days) AS days_active_recent,
       coalesce(sum(a.messages) FILTER (WHERE a.day > lim.last_day - :active_days), 0) AS messages_recent,
       min(a.day) AS first_day, max(a.day) AS last_day, lim.last_day AS activity_through
FROM tg.activity_daily a
CROSS JOIN lim
JOIN tg.chat ch ON ch.chat_id = a.chat_id AND ch.tracked
WHERE a.user_id = :user AND a.day <= lim.last_day
GROUP BY a.chat_id, ch.title, lim.last_day
ORDER BY a.chat_id;
\endif

------------------------------------------------------------------------------------------------------------
-- 3. Unique users per chat
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '3') AS run \gset
\if :run
\echo == 3. unique (exclusive) users per chat within scope, and the distinct total
-- Q: how many members of each chat are in no other chat of the scope (the reach a chat adds), and how many
-- distinct people does each chat and the whole scope have? Exclusivity is computed inside the scope.
-- The row with chat_id NULL is the scope's distinct total. Reads tg.chat_set (R2), weighted by head count.
:explain
WITH s AS (SELECT :'scope_ids'::bigint[] AS chats),
uc AS (
  SELECT cs.users, x.in_scope
  FROM tg.chat_set cs CROSS JOIN s
  CROSS JOIN LATERAL (SELECT array_agg(c) AS in_scope FROM unnest(cs.chat_ids) c WHERE c = ANY (s.chats)) x
  WHERE cs.chat_ids && s.chats
)
SELECT c.chat_id, ch.title, sum(uc.users) AS members,
       coalesce(sum(uc.users) FILTER (WHERE cardinality(uc.in_scope) = 1), 0) AS exclusive,
       round(100.0 * coalesce(sum(uc.users) FILTER (WHERE cardinality(uc.in_scope) = 1), 0) / sum(uc.users), 2) AS exclusive_pct,
       (SELECT max(as_of) FROM stats.rollup_run WHERE rollup = 'audience') AS as_of
FROM uc CROSS JOIN LATERAL unnest(uc.in_scope) c(chat_id)
LEFT JOIN tg.chat ch ON ch.chat_id = c.chat_id
GROUP BY c.chat_id, ch.title
UNION ALL
SELECT NULL, 'distinct total', sum(users), NULL, NULL, (SELECT max(as_of) FROM stats.rollup_run WHERE rollup = 'audience')
FROM uc
ORDER BY 1 NULLS LAST;
\endif

------------------------------------------------------------------------------------------------------------
-- 4. Prize / giveaway joiners
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '4', '4a') AS run \gset
\if :run
\echo == 4a. giveaway report per campaign and chat
-- Q: for each campaign chat: who joined inside [starts_at, draw_at] (first qualifying exact join per person
-- and chat; every entrant is reported); how many were first-time joiners (no earlier stay) and how many
-- returners; when the campaign has only_new_members, `eligible` is the first-time count and returners are not
-- eligible (NULL otherwise); how many left within H hours of their join (confirmed exits; kicked apart;
-- uncertain bounds apart); how many were in at the draw and left within H after it; how many are in at as_of.
-- Immature = H not over yet at as_of; an outcome after as_of is never counted, and a campaign that starts
-- after as_of has no entrants.
-- window_flags: gaps or missing coverage over [starts_at, min(draw_at + H, as_of)].
-- Body: tg.campaign_entrant (one row per chat and person) aggregated by stats.campaign_summary (schema.sql).
:explain
SELECT * FROM stats.campaign_summary(:hours, :'as_of_ts', nullif(:campaign, 0)) ORDER BY campaign_id, chat_id;
\endif

SELECT :'only' IN ('all', '4', '4b') AS run \gset
\if :run
\echo == 4b. giveaway report per person (multi-chat giveaways)
-- Q: counted once per person across all chats of the campaign: entrants, how many joined several of its
-- chats, first-time in every chat they entered, eligible (only_new_members campaigns: eligible in every chat
-- they entered, i.e. first-time in all of them; NULL otherwise), left at least one within H (confirmed; unknown only if no confirmed
-- one), were in at least one at the draw, left one within H after it, are still in at least one.
:explain
WITH e AS (
  SELECT x.*, c.only_new_members FROM tg.campaign c
  CROSS JOIN LATERAL tg.campaign_entrant(c.campaign_id, :hours, :'as_of_ts') x
  WHERE :campaign = 0 OR c.campaign_id = :campaign
), p AS (
  SELECT campaign_id, user_id, count(*) AS chats_joined, bool_and(first_time) AS first_time,
         bool_and(eligible) AS eligible, bool_or(only_new_members) AS only_new_members,
         bool_or(quick_exit = 'yes') AS quick_yes, bool_or(quick_exit = 'unknown') AS quick_unknown,
         bool_or(at_draw = 'in') AS in_at_draw, bool_or(exit_after_draw = 'yes') AS left_after_draw,
         bool_or(still_in = 'in') AS still_in
  FROM e GROUP BY campaign_id, user_id
)
SELECT campaign_id, count(*) AS persons, count(*) FILTER (WHERE chats_joined > 1) AS multi_chat,
       count(*) FILTER (WHERE first_time) AS first_time,
       CASE WHEN bool_or(only_new_members) THEN count(*) FILTER (WHERE eligible) END AS eligible,
       count(*) FILTER (WHERE quick_yes) AS quick_exit,
       count(*) FILTER (WHERE quick_unknown AND NOT quick_yes) AS quick_exit_unknown,
       count(*) FILTER (WHERE in_at_draw) AS in_at_draw,
       count(*) FILTER (WHERE left_after_draw) AS exit_after_draw,
       count(*) FILTER (WHERE still_in) AS still_in, :'as_of_ts'::timestamptz AS as_of
FROM p GROUP BY campaign_id ORDER BY campaign_id;
\endif

SELECT :'only' IN ('all', '4', '4c') AS run \gset
\if :run
\echo == 4c. giveaway entrants, one row per chat and person (CLI --csv, web drill-down)
:explain
SELECT x.campaign_id, x.chat_id, x.user_id, u.username, x.joined_at, x.first_time, x.eligible, x.quick_exit,
       x.exit_reason, x.at_draw, x.exit_after_draw, x.still_in
FROM tg.campaign c
CROSS JOIN LATERAL tg.campaign_entrant(c.campaign_id, :hours, :'as_of_ts') x
JOIN tg.tg_user u ON u.user_id = x.user_id
WHERE :campaign = 0 OR c.campaign_id = :campaign
ORDER BY x.campaign_id, x.chat_id, x.joined_at, x.user_id;
\endif

------------------------------------------------------------------------------------------------------------
-- 5. Campaign-free short stays
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '5') AS run \gset
\if :run
\echo == 5. short stays per chat and week, outside campaign windows
-- Q: per chat and join week: how many joiners left within H hours, counting only joins outside every
-- campaign window of that chat (so it works where the owner never entered a campaign). Exits are confirmed
-- ones; kicked apart; unknown = exit bounds straddle H; immature = joined less than H ago and still in. The
-- state is read at min(join + H, as_of): an exit after as_of is never counted (immature, or unknown).
:explain
WITH j AS (
  SELECT m.chat_id, date_trunc('week', m.joined_at AT TIME ZONE 'UTC')::date AS week, m.end_reason,
         tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since,
                     least(m.joined_at + :hours * interval '1 hour', :'as_of_ts'::timestamptz)) AS state,
         m.joined_at + :hours * interval '1 hour' > :'as_of_ts'::timestamptz AS young,
         EXISTS (SELECT 1 FROM tg.campaign_chat cc JOIN tg.campaign g ON g.campaign_id = cc.campaign_id
                 WHERE cc.chat_id = m.chat_id AND m.joined_at BETWEEN g.starts_at AND g.draw_at) AS in_campaign
  FROM tg.membership m
  JOIN tg.chat c ON c.chat_id = m.chat_id AND c.tracked
  JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
  WHERE m.joined_at >= :'since_ts' AND m.joined_at < :'as_of_ts' AND m.chat_id = ANY (:'scope_ids'::bigint[])
)
SELECT chat_id, week, count(*) AS joins, count(*) FILTER (WHERE in_campaign) AS campaign_joins,
       count(*) FILTER (WHERE NOT in_campaign) AS free_joins,
       count(*) FILTER (WHERE NOT in_campaign AND state = 'out') AS short_stays,
       count(*) FILTER (WHERE NOT in_campaign AND state = 'out' AND end_reason = 'kicked') AS kicked,
       count(*) FILTER (WHERE NOT in_campaign AND state = 'unknown') AS unknown,
       count(*) FILTER (WHERE NOT in_campaign AND state = 'in' AND young) AS immature,
       round(100.0 * count(*) FILTER (WHERE NOT in_campaign AND state = 'out')
             / nullif(count(*) FILTER (WHERE NOT in_campaign), 0), 1) AS short_pct,
       tg.window_flags(chat_id, tstzrange(week::timestamp AT TIME ZONE 'UTC',
                                          least((week + 7)::timestamp AT TIME ZONE 'UTC', :'as_of_ts'))) AS flags
FROM j GROUP BY chat_id, week ORDER BY chat_id, week;
\endif

------------------------------------------------------------------------------------------------------------
-- 6. Serial short-stayers; join bursts across chats
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '6', '6a') AS run \gset
\if :run
\echo == 6a. serial short-stayers: people with 2+ short stays in the window
-- Q: who keeps joining and leaving within H hours, in one chat or across chats (prize hunters)? An exit after
-- as_of is not counted.
:explain
SELECT m.user_id, u.username, count(*) AS short_stays, count(DISTINCT m.chat_id) AS chats,
       min(m.joined_at) AS first_join, max(m.joined_at) AS last_join
FROM tg.membership m
JOIN tg.chat c ON c.chat_id = m.chat_id AND c.tracked
JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
WHERE m.joined_at >= :'since_ts' AND m.joined_at < :'as_of_ts' AND m.chat_id = ANY (:'scope_ids'::bigint[])
  AND tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since,
                  least(m.joined_at + :hours * interval '1 hour', :'as_of_ts'::timestamptz)) = 'out'
GROUP BY m.user_id, u.username
HAVING count(*) >= 2
ORDER BY short_stays DESC, chats DESC, m.user_id;
\endif

SELECT :'only' IN ('all', '6', '6b') AS run \gset
\if :run
\echo == 6b. join bursts: many people joining the same two chats within minutes (unknown giveaways)
-- Q: when did at least burst_users people join both chat A and chat B within burst_minutes of each other?
-- That is the trace of a "subscribe to these channels" giveaway the owner never entered. Buckets are
-- burst_minutes wide, by the first of the two joins.
:explain
WITH j AS (
  SELECT m.user_id, m.chat_id, m.joined_at
  FROM tg.membership m JOIN tg.chat c ON c.chat_id = m.chat_id AND c.tracked
  WHERE m.joined_at >= :'since_ts' AND m.joined_at < :'as_of_ts' AND m.chat_id = ANY (:'scope_ids'::bigint[])
)
SELECT least(a.chat_id, b.chat_id) AS chat_a, greatest(a.chat_id, b.chat_id) AS chat_b,
       date_bin(:burst_minutes * interval '1 minute', a.joined_at, timestamptz '2000-01-01 00:00+00') AS bucket,
       count(DISTINCT a.user_id) AS users
FROM j a
JOIN j b ON b.user_id = a.user_id AND b.chat_id <> a.chat_id
        AND b.joined_at >= a.joined_at AND b.joined_at < a.joined_at + :burst_minutes * interval '1 minute'
GROUP BY 1, 2, 3
HAVING count(DISTINCT a.user_id) >= :burst_users
ORDER BY bucket, chat_a, chat_b;
\endif

------------------------------------------------------------------------------------------------------------
-- 7. Tenure and cohort retention
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '7', '7a') AS run \gset
\if :run
\echo == 7a. tenure of the stays that ended in the window (exact join and exact exit only)
-- Q: how long do people stay before they leave: median and 90th percentile, share gone within H.
:explain
SELECT m.chat_id, count(*) AS exits, count(*) FILTER (WHERE m.end_reason = 'kicked') AS kicked,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM m.left_at - m.joined_at) / 3600))::numeric, 2) AS median_hours,
       round((percentile_cont(0.9) WITHIN GROUP (ORDER BY extract(epoch FROM m.left_at - m.joined_at) / 3600))::numeric, 2) AS p90_hours,
       round(avg((m.left_at - m.joined_at <= :hours * interval '1 hour')::int), 4) AS share_within_h,
       tg.window_flags(m.chat_id, tstzrange(:'since_ts', :'as_of_ts')) AS flags
FROM tg.membership m
JOIN tg.chat c ON c.chat_id = m.chat_id AND c.tracked
JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
WHERE m.end_source = 'event' AND m.joined_at IS NOT NULL
  AND m.left_before >= :'since_ts' AND m.left_before < :'as_of_ts' AND m.chat_id = ANY (:'scope_ids'::bigint[])
GROUP BY m.chat_id ORDER BY m.chat_id;
\endif

SELECT :'only' IN ('all', '7', '7b') AS run \gset
\if :run
\echo == 7b. cohort retention at 7, 30 and 90 days
-- Q: of the people who joined in a week (exact joins only), how many were still in 7, 30 and 90 days later?
-- Only mature cohort members count (join + horizon <= as_of); unknown outcomes (exit bounds straddle the
-- horizon, suspect spells) are shown apart and left out of retention_pct.
:explain
SELECT m.chat_id, date_trunc('week', m.joined_at AT TIME ZONE 'UTC')::date AS cohort_week, h.days,
       count(*) AS cohort,
       count(*) FILTER (WHERE NOT x.mature) AS immature,
       count(*) FILTER (WHERE x.mature AND x.state = 'in') AS retained,
       count(*) FILTER (WHERE x.mature AND x.state = 'out') AS exited,
       count(*) FILTER (WHERE x.mature AND x.state = 'unknown') AS unknown,
       round(100.0 * count(*) FILTER (WHERE x.mature AND x.state = 'in')
             / nullif(count(*) FILTER (WHERE x.mature AND x.state IN ('in', 'out')), 0), 1) AS retention_pct
FROM tg.membership m
JOIN tg.chat c ON c.chat_id = m.chat_id AND c.tracked
JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
CROSS JOIN (VALUES (7), (30), (90)) h(days)
CROSS JOIN LATERAL (SELECT m.joined_at + h.days * interval '1 day' <= :'as_of_ts'::timestamptz AS mature,
                           tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since,
                                       m.joined_at + h.days * interval '1 day') AS state) x
WHERE m.joined_at >= :'since_ts' AND m.joined_at < :'as_of_ts' AND m.chat_id = ANY (:'scope_ids'::bigint[])
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;
\endif

------------------------------------------------------------------------------------------------------------
-- 8. Join sources
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '8') AS run \gset
\if :run
\echo == 8. join sources: joins and 7/30-day retention per link, campaign, request, folder, admin add
-- Q: which link, campaign, request flow, folder link or admin brings people who stay? Grouped by stable
-- ids (link id, campaign id), labelled with the latest name. unattributed = a join with no link.
:explain
SELECT m.chat_id, m.join_route, m.invite_link_id, coalesce(l.name, l.masked_link) AS link, l.made_by_bot,
       g.campaign_id, g.name AS campaign, count(*) AS joins,
       count(*) FILTER (WHERE x.m7) AS mature_7d,
       count(*) FILTER (WHERE x.m7 AND x.s7 = 'in') AS retained_7d,
       count(*) FILTER (WHERE x.m7 AND x.s7 = 'out') AS exited_7d,
       count(*) FILTER (WHERE x.m7 AND x.s7 = 'unknown') AS unknown_7d,
       count(*) FILTER (WHERE x.m30) AS mature_30d,
       count(*) FILTER (WHERE x.m30 AND x.s30 = 'in') AS retained_30d,
       count(*) FILTER (WHERE x.m30 AND x.s30 = 'out') AS exited_30d,
       count(*) FILTER (WHERE x.m30 AND x.s30 = 'unknown') AS unknown_30d
FROM tg.membership m
JOIN tg.chat c ON c.chat_id = m.chat_id AND c.tracked
JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
LEFT JOIN tg.invite_link l ON l.invite_link_id = m.invite_link_id
LEFT JOIN tg.campaign g ON g.campaign_id = l.campaign_id
CROSS JOIN LATERAL (SELECT
    m.joined_at + interval '7 days' <= :'as_of_ts'::timestamptz AS m7,
    m.joined_at + interval '30 days' <= :'as_of_ts'::timestamptz AS m30,
    tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since, m.joined_at + interval '7 days') AS s7,
    tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since, m.joined_at + interval '30 days') AS s30) x
WHERE m.joined_at >= :'since_ts' AND m.joined_at < :'as_of_ts' AND m.chat_id = ANY (:'scope_ids'::bigint[])
GROUP BY m.chat_id, m.join_route, m.invite_link_id, l.name, l.masked_link, l.made_by_bot, g.campaign_id, g.name
ORDER BY m.chat_id, joins DESC, m.join_route, m.invite_link_id;
\endif

------------------------------------------------------------------------------------------------------------
-- 9. Daily growth and drift
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '9', '9a') AS run \gset
\if :run
\echo == 9a. daily growth (days with any movement) from stats.chat_day
-- Q: per chat and day: joins, rejoins, exits (kicked apart), bans of non-members, net, short stays,
-- corrections by reconciliation, Telegram's count at the end of the day (the day's last count sample), and the
-- residual (change of Telegram's count - net of the events between the same two day-end samples, not the
-- calendar day's net): a non-zero residual means events we did not see.
-- Only days that ended at or before as_of (UTC): the rows are kept up to date chat by chat, so a row of the day
-- of as_of may already count what happened after as_of, and nothing in it says where its data stops. Of those
-- days, the short stays of a join day whose 48 h are not over at as_of (day + 3 > as_of) are withheld (NULL,
-- withheld = 'short_stays'): an exit after as_of must not be counted. Every other value of a finished day
-- describes that day only.
:explain
SELECT d.chat_id, d.day, d.joins, d.rejoins, d.exits, d.kicks, d.bans_from_out, d.net,
       CASE WHEN x.short_ok THEN d.short_stays END AS short_stays, d.corrections_in, d.corrections_out,
       d.reported_eod, d.known_eod, d.reported_change, d.residual, d.messages, d.active_users,
       CASE WHEN NOT x.short_ok THEN 'short_stays' END AS withheld,
       tg.window_flags(d.chat_id, tstzrange(d.day::timestamp AT TIME ZONE 'UTC', (d.day + 1)::timestamp AT TIME ZONE 'UTC')) AS flags
FROM stats.v_chat_day d
CROSS JOIN LATERAL (SELECT (d.day + 3)::timestamp AT TIME ZONE 'UTC' <= :'as_of_ts'::timestamptz AS short_ok) x
WHERE d.chat_id = ANY (:'scope_ids'::bigint[])
  AND d.day >= (:'since_ts'::timestamptz AT TIME ZONE 'UTC')::date
  AND (d.day + 1)::timestamp AT TIME ZONE 'UTC' <= :'as_of_ts'::timestamptz
  AND (d.joins + d.exits + d.bans_from_out + d.corrections_in + d.corrections_out + d.messages > 0
       OR d.reported_eod IS NOT NULL)
ORDER BY d.chat_id, d.day;
\endif

SELECT :'only' IN ('all', '9', '9b') AS run \gset
\if :run
\echo == 9b. drift samples of the last 24 hours
-- Q: right now, how far is our known member count from Telegram's (absolute mismatch), and did Telegram's
-- count move by more than our events explain since the previous sample (delta residual)?
:explain
SELECT d.chat_id, d.sampled_at, d.reported, d.known_open, d.suspect_open, d.net_events, d.mismatch, d.residual
FROM stats.v_drift d
WHERE d.chat_id = ANY (:'scope_ids'::bigint[])
  AND d.sampled_at > :'as_of_ts'::timestamptz - interval '24 hours' AND d.sampled_at <= :'as_of_ts'::timestamptz
ORDER BY d.chat_id, d.sampled_at;
\endif

------------------------------------------------------------------------------------------------------------
-- 10. Rejoiners; flow between own chats
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '10', '10a') AS run \gset
\if :run
\echo == 10a. rejoiners per chat
-- Q: how many people came back to a chat they had left? A rejoin is a stay that starts after an earlier
-- stay of the same person in the chat: observed (a join event) or found later (snapshot or probe).
-- Migration echoes are not stays, so they never count. Stays that start at the same instant (join, leave, join
-- in one second) are ordered by tg.spell_order: the replay order of the events that opened them (an inferred start
-- by the event that opened it), events before sightings and probes.
:explain
SELECT m.chat_id, count(*) AS rejoins, count(DISTINCT m.user_id) AS users,
       count(*) FILTER (WHERE m.start_source = 'event') AS observed,
       count(*) FILTER (WHERE m.start_source IN ('snapshot', 'probe')) AS found_later,
       count(*) FILTER (WHERE m.start_source = 'inferred') AS possible
FROM tg.membership m
JOIN tg.chat c ON c.chat_id = m.chat_id AND c.tracked
JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
WHERE m.known_from >= :'since_ts' AND m.known_from < :'as_of_ts' AND m.chat_id = ANY (:'scope_ids'::bigint[])
  AND EXISTS (SELECT 1 FROM tg.membership p
              WHERE p.chat_id = m.chat_id AND p.user_id = m.user_id AND p.membership_id <> m.membership_id
                AND (p.known_from < m.known_from
                     OR (p.known_from = m.known_from AND tg.spell_order(p.membership_id) < tg.spell_order(m.membership_id))))
GROUP BY m.chat_id ORDER BY m.chat_id;
\endif

SELECT :'only' IN ('all', '10', '10b') AS run \gset
\if :run
\echo == 10b. flow between own chats: joined A, then B within flow_days
-- Q: does chat A feed chat B? People with an exact join to A and a later exact join to B within flow_days;
-- both joins must be before as_of (a join to B after as_of is not counted).
:explain
SELECT a.chat_id AS from_chat, b.chat_id AS to_chat, count(DISTINCT a.user_id) AS users,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM b.joined_at - a.joined_at) / 3600))::numeric, 2) AS median_hours
FROM tg.membership a
JOIN tg.chat ca ON ca.chat_id = a.chat_id AND ca.tracked
JOIN tg.tg_user u ON u.user_id = a.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
JOIN tg.membership b ON b.user_id = a.user_id AND b.chat_id <> a.chat_id
                    AND b.joined_at > a.joined_at AND b.joined_at <= a.joined_at + :flow_days * interval '1 day'
                    AND b.joined_at < :'as_of_ts'::timestamptz
JOIN tg.chat cb ON cb.chat_id = b.chat_id AND cb.tracked
WHERE a.joined_at >= :'since_ts' AND a.joined_at < :'as_of_ts'
  AND a.chat_id = ANY (:'scope_ids'::bigint[]) AND b.chat_id = ANY (:'scope_ids'::bigint[])
GROUP BY a.chat_id, b.chat_id ORDER BY users DESC, from_chat, to_chat;
\endif

------------------------------------------------------------------------------------------------------------
-- 11. Channel vs its discussion group
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '11') AS run \gset
\if :run
\echo == 11. channel and its discussion group
-- Q: what share of a channel's subscribers is in its discussion group, how many group members do not
-- subscribe, and who of the group is active (wrote in the last active_days)? Counts of members come from
-- the pair matrix stats.overlap (R2, as of its own refresh time, the as_of column); the active ones are checked
-- live, and who is a member of the group or a subscriber is read at as_of, not today. The daily activity counts have no
-- time of day: when as_of is earlier than the last update ingest committed, only complete UTC days up to
-- activity_through count (the day of as_of is partly over and its messages may come after as_of).
:explain
WITH pair AS (
  SELECT ch.chat_id AS channel_id, ch.title AS channel, g.chat_id AS group_id, g.title AS group_title
  FROM tg.chat ch JOIN tg.chat g ON g.chat_id = ch.linked_chat_id AND g.tracked
  WHERE ch.chat_type = 'channel' AND ch.tracked AND ch.chat_id = ANY (:'scope_ids'::bigint[])
), counts AS (       -- three cells of the pair matrix (R2): channel and group diagonals, and the pair
  SELECT p.channel_id,
         (SELECT o.shared FROM stats.overlap o WHERE o.chat_a = p.channel_id AND o.chat_b = p.channel_id) AS subscribers,
         (SELECT o.shared FROM stats.overlap o WHERE o.chat_a = p.group_id AND o.chat_b = p.group_id) AS group_members,
         (SELECT o.shared FROM stats.overlap o WHERE o.chat_a = least(p.channel_id, p.group_id)
                                                 AND o.chat_b = greatest(p.channel_id, p.group_id)) AS in_both
  FROM pair p
), lim AS (
  SELECT CASE WHEN :'as_of_ts'::timestamptz >= coalesce((SELECT max(b.last_poll_ok_at) FROM tg.bot_state b), '-infinity'::timestamptz)
              THEN (:'as_of_ts'::timestamptz AT TIME ZONE 'UTC')::date
              ELSE (:'as_of_ts'::timestamptz AT TIME ZONE 'UTC')::date - 1 END AS last_day
), active AS (
  SELECT p.channel_id, a.user_id,
         EXISTS (SELECT 1 FROM tg.membership m WHERE m.chat_id = p.channel_id AND m.user_id = a.user_id
                 AND m.stay @> :'as_of_ts'::timestamptz) AS subscribed
  FROM pair p
  JOIN tg.activity_daily a ON a.chat_id = p.group_id
   AND a.day > (SELECT last_day FROM lim) - :active_days AND a.day <= (SELECT last_day FROM lim)
  JOIN tg.tg_user u ON u.user_id = a.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
  WHERE EXISTS (SELECT 1 FROM tg.membership m WHERE m.chat_id = p.group_id AND m.user_id = a.user_id
                AND m.stay @> :'as_of_ts'::timestamptz)
  GROUP BY p.channel_id, a.user_id
)
SELECT p.channel_id, p.channel, p.group_id, p.group_title, c.subscribers, c.group_members, c.in_both,
       round(100.0 * c.in_both / nullif(c.subscribers, 0), 2) AS pct_subscribers_in_group,
       c.group_members - c.in_both AS group_only,
       (SELECT count(*) FROM active x WHERE x.channel_id = p.channel_id) AS active_in_group,
       (SELECT count(*) FROM active x WHERE x.channel_id = p.channel_id AND x.subscribed) AS active_subscribers,
       (SELECT count(*) FROM active x WHERE x.channel_id = p.channel_id AND NOT x.subscribed) AS active_non_subscribers,
       (SELECT last_day FROM lim) AS activity_through,
       (SELECT max(as_of) FROM stats.rollup_run WHERE rollup = 'audience') AS as_of
FROM pair p LEFT JOIN counts c ON c.channel_id = p.channel_id
ORDER BY p.channel_id;
\endif

------------------------------------------------------------------------------------------------------------
-- 12. Join-request funnel
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '12') AS run \gset
\if :run
\echo == 12. join-request funnel per chat and week
-- Q: how many asked to join, how many were approved, how fast, and did the approved stay 30 days?
-- unknown = no answer seen within 7 days (declines by other admins send no update, F§3); pending = younger.
-- Outcomes are read as of as_of: an answer after as_of leaves the request pending until its 7 days have passed
-- and unknown after that (an answer the sampler had not seen within 7 days), 'unknown' only once 7 days have
-- passed, and the approved join must be before as_of. 7 days = 168 hours, as the sampler ages them: a calendar
-- interval would follow the session's time zone and gain or lose an hour across a clock change.
:explain
SELECT r.chat_id, date_trunc('week', r.requested_at AT TIME ZONE 'UTC')::date AS week, count(*) AS requests,
       count(*) FILTER (WHERE o.outcome = 'approved') AS approved,
       count(*) FILTER (WHERE o.outcome = 'declined') AS declined,
       count(*) FILTER (WHERE o.outcome = 'unknown') AS unknown,
       count(*) FILTER (WHERE o.outcome IS NULL) AS pending,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM r.resolved_at - r.requested_at) / 60)
         FILTER (WHERE o.outcome = 'approved') AS median_minutes_to_approve,
       count(*) FILTER (WHERE j.joined_at + interval '30 days' <= :'as_of_ts'::timestamptz) AS approved_mature_30d,
       count(*) FILTER (WHERE j.joined_at + interval '30 days' <= :'as_of_ts'::timestamptz
                          AND tg.state_at(j.known_from, j.left_after, j.left_before, j.end_source, j.suspect_since,
                                          j.joined_at + interval '30 days') = 'in') AS approved_retained_30d
FROM tg.join_request r
JOIN tg.chat c ON c.chat_id = r.chat_id AND c.tracked
CROSS JOIN LATERAL (SELECT CASE WHEN r.outcome = 'unknown' OR r.resolved_at > :'as_of_ts'::timestamptz
                                THEN CASE WHEN r.requested_at + interval '168 hours' <= :'as_of_ts'::timestamptz
                                          THEN 'unknown' END
                                ELSE r.outcome END AS outcome) o
LEFT JOIN LATERAL (
  SELECT m.joined_at, m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since
  FROM tg.membership m
  WHERE m.chat_id = r.chat_id AND m.user_id = r.user_id AND m.joined_at >= r.requested_at
    AND m.joined_at <= :'as_of_ts'::timestamptz
  ORDER BY m.joined_at LIMIT 1) j ON o.outcome = 'approved'
WHERE r.requested_at >= :'since_ts' AND r.requested_at < :'as_of_ts' AND r.chat_id = ANY (:'scope_ids'::bigint[])
GROUP BY 1, 2 ORDER BY 1, 2;
\endif

------------------------------------------------------------------------------------------------------------
-- 13. Raids and spam waves
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '13') AS run \gset
\if :run
\echo == 13. raids: minutes with joins far above the baseline of the chat, share kicked within an hour
-- Q: in the last raid_hours, which minutes had at least raid_min_joins joins and 10x the chat's average
-- per-minute joins of the 7 days before, and what share of those joiners was kicked within an hour?
-- All accounts count (spam accounts are often bots). A kick after as_of is not counted.
:explain
WITH j AS (
  SELECT m.chat_id, date_trunc('minute', m.joined_at) AS minute, count(*) AS joins,
         count(*) FILTER (WHERE m.end_reason = 'kicked' AND m.left_before <= m.joined_at + interval '1 hour'
                            AND m.left_before <= :'as_of_ts'::timestamptz) AS kicked_1h
  FROM tg.membership m JOIN tg.chat c ON c.chat_id = m.chat_id AND c.tracked
  WHERE m.joined_at > :'as_of_ts'::timestamptz - :raid_hours * interval '1 hour' AND m.joined_at <= :'as_of_ts'::timestamptz
    AND m.chat_id = ANY (:'scope_ids'::bigint[])
  GROUP BY 1, 2
), base AS (
  SELECT m.chat_id, count(*) / (7 * 1440.0) AS per_minute
  FROM tg.membership m
  WHERE m.chat_id IN (SELECT chat_id FROM j)
    AND m.joined_at > :'as_of_ts'::timestamptz - :raid_hours * interval '1 hour' - interval '7 days'
    AND m.joined_at <= :'as_of_ts'::timestamptz - :raid_hours * interval '1 hour'
  GROUP BY 1
)
SELECT j.chat_id, j.minute, j.joins, round(coalesce(b.per_minute, 0), 4) AS baseline_per_minute, j.kicked_1h,
       round(100.0 * j.kicked_1h / j.joins, 1) AS pct_kicked_1h
FROM j LEFT JOIN base b ON b.chat_id = j.chat_id
WHERE j.joins >= greatest(:raid_min_joins, 10 * coalesce(b.per_minute, 0))
ORDER BY j.chat_id, j.minute;
\endif

------------------------------------------------------------------------------------------------------------
-- 14. Active vs silent members in groups
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '14') AS run \gset
\if :run
\echo == 14. groups: active vs silent members, and did short-stayers ever write
-- Q: per group: how many current members wrote in the last active_days, how many are silent; of the
-- short stays in the window (exit within H, an exit after as_of not counted), how many wrote at least once
-- during their stay? Members are the stays that hold as_of. Activity counts are daily: when as_of is earlier than
-- the last update ingest committed, only complete UTC days up to activity_through count (as in 11).
:explain
WITH lim AS (
  SELECT CASE WHEN :'as_of_ts'::timestamptz >= coalesce((SELECT max(b.last_poll_ok_at) FROM tg.bot_state b), '-infinity'::timestamptz)
              THEN (:'as_of_ts'::timestamptz AT TIME ZONE 'UTC')::date
              ELSE (:'as_of_ts'::timestamptz AT TIME ZONE 'UTC')::date - 1 END AS last_day
), g AS (
  SELECT chat_id, title FROM tg.chat
  WHERE tracked AND chat_type IN ('group', 'supergroup') AND chat_id = ANY (:'scope_ids'::bigint[])
), mem AS (
  SELECT g.chat_id, count(*) AS members,
         count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM tg.activity_daily a WHERE a.chat_id = m.chat_id AND a.user_id = m.user_id
             AND a.day > (SELECT last_day FROM lim) - :active_days AND a.day <= (SELECT last_day FROM lim))) AS active
  FROM g JOIN tg.membership m ON m.chat_id = g.chat_id AND m.stay @> :'as_of_ts'::timestamptz
  JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
  GROUP BY g.chat_id
), ss AS (
  SELECT g.chat_id, count(*) AS short_stays,
         count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM tg.activity_daily a WHERE a.chat_id = m.chat_id AND a.user_id = m.user_id
             AND a.day BETWEEN (m.joined_at AT TIME ZONE 'UTC')::date
                           AND least((m.left_before AT TIME ZONE 'UTC')::date, (SELECT last_day FROM lim)))) AS wrote
  FROM g JOIN tg.membership m ON m.chat_id = g.chat_id
  JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
  WHERE m.joined_at >= :'since_ts' AND m.joined_at < :'as_of_ts'
    AND tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since,
                    least(m.joined_at + :hours * interval '1 hour', :'as_of_ts'::timestamptz)) = 'out'
  GROUP BY g.chat_id
)
SELECT g.chat_id, g.title, coalesce(mem.members, 0) AS members, coalesce(mem.active, 0) AS active,
       coalesce(mem.members - mem.active, 0) AS silent,
       round(100.0 * mem.active / nullif(mem.members, 0), 1) AS active_pct,
       coalesce(ss.short_stays, 0) AS short_stays, coalesce(ss.wrote, 0) AS short_stays_who_wrote,
       (SELECT last_day FROM lim) AS activity_through
FROM g LEFT JOIN mem ON mem.chat_id = g.chat_id LEFT JOIN ss ON ss.chat_id = g.chat_id
ORDER BY g.chat_id;
\endif

------------------------------------------------------------------------------------------------------------
-- 15. Leaves after channel posts
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '15') AS run \gset
\if :run
\echo == 15. leaves within post_hours after each channel post, against the same span before it
-- Q: which posts make people leave? Exact self-leaves in (posted_at, posted_at + post_hours], with the
-- span before the post as a baseline. Posts close together share leaves. Leaves after as_of are not counted:
-- a post whose window after it reaches past as_of is marked immature.
:explain
SELECT p.chat_id, p.message_id, p.posted_at, p.kind,
       (SELECT count(*) FROM tg.membership m
         WHERE m.chat_id = p.chat_id AND m.end_source = 'event' AND m.end_reason = 'left'
           AND m.left_before > p.posted_at
           AND m.left_before <= least(p.posted_at + :post_hours * interval '1 hour', :'as_of_ts'::timestamptz)) AS leaves_after,
       (SELECT count(*) FROM tg.membership m
         WHERE m.chat_id = p.chat_id AND m.end_source = 'event' AND m.end_reason = 'left'
           AND m.left_before > p.posted_at - :post_hours * interval '1 hour' AND m.left_before <= p.posted_at) AS leaves_before,
       p.posted_at + :post_hours * interval '1 hour' > :'as_of_ts'::timestamptz AS immature,
       tg.window_flags(p.chat_id, tstzrange(p.posted_at - :post_hours * interval '1 hour',
                                            least(p.posted_at + :post_hours * interval '1 hour',
                                                  :'as_of_ts'::timestamptz))) AS flags
FROM tg.post p JOIN tg.chat c ON c.chat_id = p.chat_id AND c.tracked AND c.chat_type = 'channel'
WHERE p.posted_at >= :'since_ts' AND p.posted_at < :'as_of_ts' AND p.chat_id = ANY (:'scope_ids'::bigint[])
ORDER BY p.chat_id, p.posted_at;
\endif

------------------------------------------------------------------------------------------------------------
-- 16. As-of membership
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('all', '16', '16a') AS run \gset
\if :run
\echo == 16a. who was a member of chat X at date D: counts
-- Q: how many people were in chat :chat at :day? known_in = a stay holds D for sure; unsure = D falls
-- between the last sighting and the confirmed exit, or the stay is suspect; by_telegram_date = first seen
-- after D, but Telegram's join date (from a snapshot, unchecked) says before D. The stay range is served
-- by the GiST index of the one-stay-at-a-time constraint; the hint branch by membership_found. Exact to the
-- instant; on a 1M chat it reads the whole chat (seconds). Day-level counts come instantly from 16d.
:explain
WITH hit AS (
  SELECT m.user_id, m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since
  FROM tg.membership m WHERE m.chat_id = :chat AND m.stay @> :'day_ts'::timestamptz
  UNION ALL
  SELECT m.user_id, m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since
  FROM tg.membership m WHERE m.chat_id = :chat AND m.start_source <> 'event' AND m.start_source = 'snapshot'
                         AND m.known_from > :'day_ts'::timestamptz AND m.joined_hint <= :'day_ts'::timestamptz
)
SELECT :chat::bigint AS chat_id, :'day_ts'::timestamptz AS at,
       count(*) FILTER (WHERE s.state = 'in') AS known_in,
       count(*) FILTER (WHERE s.state = 'unknown') AS unsure,
       count(*) FILTER (WHERE s.state = 'before') AS by_telegram_date,
       tg.window_flags(:chat, tstzrange(:'day_ts'::timestamptz, :'day_ts'::timestamptz, '[]')) AS flags
FROM hit h
CROSS JOIN LATERAL (SELECT tg.state_at(h.known_from, h.left_after, h.left_before, h.end_source, h.suspect_since,
                                       :'day_ts'::timestamptz) AS state) s
WHERE h.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted);
\endif

SELECT :'only' IN ('all', '16', '16b') AS run \gset
\if :run
\echo == 16b. who was a member of chat X at date D: the list (CLI --csv)
:explain
SELECT m.user_id, u.username,
       tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since, :'day_ts'::timestamptz) AS state,
       m.start_source, m.joined_at, m.joined_hint, m.known_from, m.left_at, m.left_after, m.left_before
FROM tg.membership m
JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
WHERE m.chat_id = :chat
  AND (m.stay @> :'day_ts'::timestamptz
       OR (m.start_source = 'snapshot' AND m.known_from > :'day_ts'::timestamptz AND m.joined_hint <= :'day_ts'::timestamptz))
ORDER BY state, m.user_id;
\endif

SELECT :'only' IN ('all', '16', '16c') AS run \gset
\if :run
\echo == 16c. overlap of every pair in scope at a past date D (known members only); batch, work_mem 64 MB
-- Q: the pair matrix of item 1 as it was at :day, e.g. at a giveaway's draw. Counts only stays that hold D
-- for sure. A heavy, on-demand batch: it groups every member at D by person, like R2.
BEGIN;
SET LOCAL work_mem = '64MB';
:explain
WITH s AS (SELECT :'scope_ids'::bigint[] AS chats),
m AS (
  SELECT m.chat_id, m.user_id
  FROM tg.membership m CROSS JOIN s
  WHERE m.chat_id = ANY (s.chats) AND m.stay @> :'day_ts'::timestamptz AND m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)
    AND tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since, :'day_ts'::timestamptz) = 'in'
), k AS (
  SELECT array_agg(chat_id ORDER BY chat_id) AS chat_ids FROM m GROUP BY user_id
), sets AS (
  SELECT chat_ids, count(*) AS users FROM k GROUP BY chat_ids
), pairs AS (
  SELECT a.chat_id AS chat_a, b.chat_id AS chat_b, sum(sets.users) AS shared
  FROM sets CROSS JOIN LATERAL unnest(sets.chat_ids) a(chat_id) CROSS JOIN LATERAL unnest(sets.chat_ids) b(chat_id)
  WHERE a.chat_id <= b.chat_id GROUP BY 1, 2
)
SELECT ca.chat_id AS chat_a, cb.chat_id AS chat_b, coalesce(p.shared, 0) AS shared, :'day_ts'::timestamptz AS at
FROM s CROSS JOIN LATERAL unnest(s.chats) ca(chat_id) CROSS JOIN LATERAL unnest(s.chats) cb(chat_id)
LEFT JOIN pairs p ON p.chat_a = ca.chat_id AND p.chat_b = cb.chat_id
WHERE ca.chat_id <= cb.chat_id
ORDER BY 1, 2;
COMMIT;
\endif

SELECT :'only' IN ('all', '16', '16d') AS run \gset
\if :run
\echo == 16d. members of each chat at the end of a day, from the chat_day rollup
-- Q: how many people were in chat X at the end of day D (UTC)? members_eod = stays holding the end of the day
-- (the running sum of the daily deltas in stats.chat_day), unsure_eod = of those, between the last sighting
-- and a confirmed exit; known = the difference. Same numbers as 16a at D + 1 day 00:00, without the scan.
-- Only once D has ended at as_of (as block 9a): before that the end of D is not known, and no row is returned.
:explain
SELECT d.chat_id, d.day, d.members_eod, d.unsure_eod, d.members_eod - d.unsure_eod AS known_eod, d.reported_eod,
       tg.window_flags(d.chat_id, tstzrange(d.day::timestamp AT TIME ZONE 'UTC', (d.day + 1)::timestamp AT TIME ZONE 'UTC')) AS flags
FROM stats.v_chat_day d
WHERE d.chat_id = ANY (:'scope_ids'::bigint[]) AND d.day = (:'day_ts'::timestamptz AT TIME ZONE 'UTC')::date
  AND (d.day + 1)::timestamp AT TIME ZONE 'UTC' <= :'as_of_ts'::timestamptz
ORDER BY d.chat_id;
\endif

------------------------------------------------------------------------------------------------------------
-- R1. Refresh stats.chat_day (batch job, every 5 minutes; work_mem 16 MB)
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('R1') AS run \gset
\if :run
\echo == R1. refresh stats.chat_day from the earliest affected day
-- Per tracked chat in scope: recompute from min(dirty day, today - lookback_days) to today, even when that day
-- is before the chat's first rollup row or its first coverage (the grid then starts there); a chat without rows
-- is built from its first coverage or evidence day (or an earlier dirty day). Late events, reconciliation fixes,
-- rebuilds and migrations call tg.mark_dirty (least from_day, a new version on every write). The markers read
-- here are cleared at the end only where their version is unchanged: a marker written while this ran stays,
-- and a marker another transaction holds is skipped, never waited for (a migration that deleted it waits for
-- the chat row this run's foreign key holds: waiting here would be a deadlock).
-- Readers see the old rows until the commit. The service runs this per chat (scope = one chat; far-back
-- chats together in one set-based pass) in tgaud.jobs.chat_day and gives the same rows.
--
-- Rules of the stored columns:
--  * joins, exits, net, short_stays, rejoins, corrections: calendar (UTC) day of the event. A rejoin is a stay
--    with an earlier stay of the same person in the chat; at the same instant the opening event's order
--    (event_at, update_id) decides, so join, leave, join in one second counts one rejoin, whatever the
--    spells' ids. A spell with an inferred start (E6, E9) has no opening event of its own: tg.spell_order
--    replays the events of that instant and orders it by the event that opened it (the same order as 10a).
--  * reported_eod, known_eod, sample_at: the day's last finalized count sample (net_events set: ingest had caught
--    up past it) at or before as_of. sample_net: exact joins minus exact exits in (previous day's sample, this
--    day's sample], so the residual (v_chat_day) compares Telegram's count with the events between the same two
--    samples, not with the calendar day; NULL (and so the residual) when either sample's count call straddled an
--    exact transition (count_sample.straddled), as stats.v_drift withholds it.
--  * members_started/ended, unsure_started/ended: daily deltas of human stays (known_from, left_before; bounded
--    exits at left_after and left_before; open stays at suspect_since, which are unsure from then on).
--    members_midnight and unsure_midnight hold the part at exactly 00:00 UTC, which v_chat_day counts into the
--    end of the day before: members_eod = 16a at the next midnight.
BEGIN;
SET LOCAL work_mem = '16MB';
CREATE TEMP TABLE chat_day_target ON COMMIT DROP AS
SELECT c.chat_id, t.from_day, t.from_day::timestamp AT TIME ZONE 'UTC' AS from_ts, r.version AS dirty_version
FROM tg.chat c
LEFT JOIN tg.rollup_dirty r ON r.chat_id = c.chat_id
LEFT JOIN LATERAL (SELECT min(d.day) AS first_row_day FROM stats.chat_day d WHERE d.chat_id = c.chat_id) e ON true
CROSS JOIN LATERAL (SELECT CASE
    WHEN e.first_row_day IS NULL THEN
      least((least((SELECT min(lower(v.period)) FROM tg.coverage v WHERE v.chat_id = c.chat_id),
                   (SELECT min(m.known_from) FROM tg.membership m WHERE m.chat_id = c.chat_id),
                   :'as_of_ts'::timestamptz) AT TIME ZONE 'UTC')::date,
            r.from_day)
    ELSE least(coalesce(r.from_day, 'infinity'::date),
               (:'as_of_ts'::timestamptz AT TIME ZONE 'UTC')::date - :lookback_days)
  END AS from_day) t
WHERE c.tracked AND c.chat_id = ANY (:'scope_ids'::bigint[]);
ANALYZE chat_day_target;

DELETE FROM stats.chat_day d USING chat_day_target t WHERE d.chat_id = t.chat_id AND d.day >= t.from_day;

-- Two plan shapes, one result. A large recompute (the first build, or dirty days going far back) is one
-- set-based pass over membership; the normal run (a few days per chat) reads each chat by index range scans
-- from its from_day (LATERAL), so it touches only those days' rows. The switch is how far back the oldest
-- target day is (7 days; tgaud.jobs.chat_day.FULL_DAYS).
SELECT coalesce(bool_or((:'as_of_ts'::timestamptz AT TIME ZONE 'UTC')::date - from_day > 7), false) AS chat_day_full
FROM chat_day_target \gset
\if :chat_day_full
:explain
INSERT INTO stats.chat_day (chat_id, day, joins, rejoins, exits, kicks, bans_from_out, net, short_stays,
                            corrections_in, corrections_out, reported_eod, known_eod, messages, active_users,
                            members_started, members_ended, unsure_started, unsure_ended,
                            members_midnight, unsure_midnight, sample_at, sample_net, refreshed_at)
WITH grid AS (
  SELECT t.chat_id, g::date AS day
  FROM chat_day_target t, generate_series(t.from_day, (:'as_of_ts'::timestamptz AT TIME ZONE 'UTC')::date, interval '1 day') g
), j AS (
  SELECT m.chat_id, (m.joined_at AT TIME ZONE 'UTC')::date AS day, count(*) AS joins,
         count(*) FILTER (WHERE m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)) AS members_started,
         count(*) FILTER (WHERE m.joined_at AT TIME ZONE 'UTC' = (m.joined_at AT TIME ZONE 'UTC')::date
                            AND m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)) AS mid_started,
         count(*) FILTER (WHERE EXISTS (SELECT 1 FROM tg.membership p WHERE p.chat_id = m.chat_id
                                          AND p.user_id = m.user_id AND p.known_from < m.known_from)
                             OR EXISTS (SELECT 1 FROM tg.membership p WHERE p.chat_id = m.chat_id
                                          AND p.user_id = m.user_id AND p.known_from = m.known_from
                                          AND p.membership_id <> m.membership_id
                                          AND tg.spell_order(p.membership_id) < tg.spell_order(m.membership_id))) AS rejoins,
         count(*) FILTER (WHERE tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since,
                                            m.joined_at + interval '48 hours') = 'out') AS short_stays
  FROM chat_day_target t JOIN tg.membership m ON m.chat_id = t.chat_id AND m.joined_at >= t.from_ts
  GROUP BY 1, 2
), f AS (
  SELECT m.chat_id, (m.known_from AT TIME ZONE 'UTC')::date AS day,
         count(*) FILTER (WHERE m.start_source IN ('snapshot', 'probe')) AS corrections_in,
         count(*) FILTER (WHERE m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)) AS members_started,
         count(*) FILTER (WHERE m.known_from AT TIME ZONE 'UTC' = (m.known_from AT TIME ZONE 'UTC')::date
                            AND m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)) AS mid_started
  FROM chat_day_target t JOIN tg.membership m ON m.chat_id = t.chat_id AND m.start_source <> 'event'
                                             AND m.known_from >= t.from_ts
  GROUP BY 1, 2
), sp AS (           -- open stays under suspicion: unsure from suspect_since on
  SELECT m.chat_id, (m.suspect_since AT TIME ZONE 'UTC')::date AS day, count(*) AS unsure_started,
         count(*) FILTER (WHERE m.suspect_since AT TIME ZONE 'UTC' = (m.suspect_since AT TIME ZONE 'UTC')::date) AS mid_started
  FROM chat_day_target t JOIN tg.membership m ON m.chat_id = t.chat_id AND m.end_source IS NULL
                                             AND m.suspect_since >= t.from_ts
  WHERE m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)
  GROUP BY 1, 2
), e AS MATERIALIZED (
  SELECT m.chat_id, m.left_after, m.left_before, m.end_source, m.end_reason, t.from_ts, m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted) AS human
  FROM chat_day_target t JOIN tg.membership m ON m.chat_id = t.chat_id AND m.end_source IS NOT NULL
                                             AND m.left_before >= t.from_ts
), x AS (
  SELECT u.chat_id, u.day, sum(u.exits) AS exits, sum(u.kicks) AS kicks, sum(u.corrections_out) AS corrections_out,
         sum(u.members_ended) AS members_ended, sum(u.unsure_ended) AS unsure_ended, sum(u.unsure_started) AS unsure_started,
         sum(u.mid_ended) AS mid_ended, sum(u.mid_unsure_ended) AS mid_unsure_ended, sum(u.mid_unsure_started) AS mid_unsure_started
  FROM (SELECT e.chat_id, (e.left_before AT TIME ZONE 'UTC')::date AS day, (e.end_source = 'event')::int AS exits,
               (e.end_source = 'event' AND e.end_reason = 'kicked')::int AS kicks,
               (e.end_source IN ('probe', 'inferred'))::int AS corrections_out, e.human::int AS members_ended,
               (e.human AND e.end_source <> 'event')::int AS unsure_ended, 0 AS unsure_started,
               (e.human AND e.left_before AT TIME ZONE 'UTC' = (e.left_before AT TIME ZONE 'UTC')::date)::int AS mid_ended,
               (e.human AND e.end_source <> 'event'
                  AND e.left_before AT TIME ZONE 'UTC' = (e.left_before AT TIME ZONE 'UTC')::date)::int AS mid_unsure_ended,
               0 AS mid_unsure_started
        FROM e
        UNION ALL
        SELECT e.chat_id, (e.left_after AT TIME ZONE 'UTC')::date, 0, 0, 0, 0, 0, 1, 0, 0,
               (e.left_after AT TIME ZONE 'UTC' = (e.left_after AT TIME ZONE 'UTC')::date)::int
        FROM e WHERE e.human AND e.end_source <> 'event' AND e.left_after >= e.from_ts) u
  GROUP BY 1, 2
), b AS (
  SELECT ev.chat_id, (ev.event_at AT TIME ZONE 'UTC')::date AS day, count(*) AS bans_from_out
  FROM chat_day_target t JOIN tg.member_event ev ON ev.chat_id = t.chat_id AND ev.event_at >= t.from_ts AND ev.kind = 'ban_out'
  GROUP BY 1, 2
), s0 AS (           -- the last finalized count sample of each day, from the day before the range (for its interval)
  SELECT DISTINCT ON (cs.chat_id, (cs.sampled_at AT TIME ZONE 'UTC')::date)
         cs.chat_id, (cs.sampled_at AT TIME ZONE 'UTC')::date AS day, cs.sampled_at AS sample_at, cs.reported,
         cs.known_open, cs.straddled
  FROM chat_day_target t JOIN tg.count_sample cs ON cs.chat_id = t.chat_id
                                                AND cs.sampled_at >= t.from_ts - interval '1 day'
                                                AND cs.sampled_at <= :'as_of_ts'::timestamptz
                                                AND cs.net_events IS NOT NULL
  ORDER BY cs.chat_id, (cs.sampled_at AT TIME ZONE 'UTC')::date, cs.sampled_at DESC
), s AS (            -- plus the events after that sample until the end of its day: the part of the day's net that
                     -- belongs to the interval of the next sample
  SELECT s0.*,
         (SELECT count(*) FROM tg.membership m WHERE m.chat_id = s0.chat_id AND m.joined_at > s0.sample_at
                                                 AND m.joined_at < (s0.day + 1)::timestamp AT TIME ZONE 'UTC')
         - (SELECT count(*) FROM tg.membership m WHERE m.chat_id = s0.chat_id AND m.end_source = 'event'
                                                  AND m.left_before > s0.sample_at
                                                  AND m.left_before < (s0.day + 1)::timestamp AT TIME ZONE 'UTC') AS tail_net
  FROM s0
), a AS (
  SELECT ad.chat_id, ad.day, sum(ad.messages) AS messages, count(*) AS active_users
  FROM chat_day_target t JOIN tg.activity_daily ad ON ad.chat_id = t.chat_id AND ad.day >= t.from_day
  GROUP BY 1, 2
), ua AS (
  SELECT ud.chat_id, ud.day, sum(ud.messages) AS messages
  FROM chat_day_target t JOIN tg.unattributed_daily ud ON ud.chat_id = t.chat_id AND ud.day >= t.from_day
  GROUP BY 1, 2
)
SELECT g.chat_id, g.day, coalesce(j.joins, 0), coalesce(j.rejoins, 0), coalesce(x.exits, 0), coalesce(x.kicks, 0),
       coalesce(b.bans_from_out, 0), coalesce(j.joins, 0) - coalesce(x.exits, 0), coalesce(j.short_stays, 0),
       coalesce(f.corrections_in, 0), coalesce(x.corrections_out, 0), s.reported, s.known_open,
       coalesce(a.messages, 0) + coalesce(ua.messages, 0), coalesce(a.active_users, 0),
       coalesce(j.members_started, 0) + coalesce(f.members_started, 0), coalesce(x.members_ended, 0),
       coalesce(x.unsure_started, 0) + coalesce(sp.unsure_started, 0), coalesce(x.unsure_ended, 0),
       coalesce(j.mid_started, 0) + coalesce(f.mid_started, 0) - coalesce(x.mid_ended, 0),
       coalesce(x.mid_unsure_started, 0) + coalesce(sp.mid_started, 0) - coalesce(x.mid_unsure_ended, 0),
       s.sample_at,
       CASE WHEN s.sample_at IS NOT NULL AND p.sample_at IS NOT NULL AND NOT s.straddled AND NOT p.straddled
            THEN coalesce(j.joins, 0) - coalesce(x.exits, 0) - s.tail_net + p.tail_net END,
       now()
FROM grid g
LEFT JOIN j USING (chat_id, day) LEFT JOIN f USING (chat_id, day) LEFT JOIN sp USING (chat_id, day)
LEFT JOIN x USING (chat_id, day) LEFT JOIN b USING (chat_id, day)
LEFT JOIN s USING (chat_id, day) LEFT JOIN a USING (chat_id, day) LEFT JOIN ua USING (chat_id, day)
LEFT JOIN s p ON p.chat_id = g.chat_id AND p.day = g.day - 1;

\else
:explain
INSERT INTO stats.chat_day (chat_id, day, joins, rejoins, exits, kicks, bans_from_out, net, short_stays,
                            corrections_in, corrections_out, reported_eod, known_eod, messages, active_users,
                            members_started, members_ended, unsure_started, unsure_ended,
                            members_midnight, unsure_midnight, sample_at, sample_net, refreshed_at)
WITH grid AS (
  SELECT t.chat_id, g::date AS day
  FROM chat_day_target t, generate_series(t.from_day, (:'as_of_ts'::timestamptz AT TIME ZONE 'UTC')::date, interval '1 day') g
), j AS (            -- exact joins by join day (all accounts), rejoins, short stays, human starts
  SELECT t.chat_id, z.* FROM chat_day_target t CROSS JOIN LATERAL (
    SELECT (m.joined_at AT TIME ZONE 'UTC')::date AS day, count(*) AS joins,
           count(*) FILTER (WHERE m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)) AS members_started,
           count(*) FILTER (WHERE m.joined_at AT TIME ZONE 'UTC' = (m.joined_at AT TIME ZONE 'UTC')::date
                              AND m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)) AS mid_started,
           count(*) FILTER (WHERE EXISTS (SELECT 1 FROM tg.membership p WHERE p.chat_id = m.chat_id
                                            AND p.user_id = m.user_id AND p.known_from < m.known_from)
                               OR EXISTS (SELECT 1 FROM tg.membership p WHERE p.chat_id = m.chat_id
                                            AND p.user_id = m.user_id AND p.known_from = m.known_from
                                            AND p.membership_id <> m.membership_id
                                            AND tg.spell_order(p.membership_id) < tg.spell_order(m.membership_id))) AS rejoins,
           count(*) FILTER (WHERE tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since,
                                              m.joined_at + interval '48 hours') = 'out') AS short_stays
    FROM tg.membership m WHERE m.chat_id = t.chat_id AND m.joined_at >= t.from_ts GROUP BY 1) z
), f AS (            -- stays not started by an event: corrections, and human starts
  SELECT t.chat_id, z.* FROM chat_day_target t CROSS JOIN LATERAL (
    SELECT (m.known_from AT TIME ZONE 'UTC')::date AS day,
           count(*) FILTER (WHERE m.start_source IN ('snapshot', 'probe')) AS corrections_in,
           count(*) FILTER (WHERE m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)) AS members_started,
           count(*) FILTER (WHERE m.known_from AT TIME ZONE 'UTC' = (m.known_from AT TIME ZONE 'UTC')::date
                              AND m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)) AS mid_started
    FROM tg.membership m
    WHERE m.chat_id = t.chat_id AND m.start_source <> 'event' AND m.known_from >= t.from_ts GROUP BY 1) z
), sp AS (           -- open stays under suspicion: unsure from suspect_since on
  SELECT t.chat_id, z.* FROM chat_day_target t CROSS JOIN LATERAL (
    SELECT (m.suspect_since AT TIME ZONE 'UTC')::date AS day, count(*) AS unsure_started,
           count(*) FILTER (WHERE m.suspect_since AT TIME ZONE 'UTC' = (m.suspect_since AT TIME ZONE 'UTC')::date) AS mid_started
    FROM tg.membership m
    WHERE m.chat_id = t.chat_id AND m.end_source IS NULL AND m.suspect_since >= t.from_ts
      AND m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted) GROUP BY 1) z
), x AS (            -- one pass over the ends: by left_before day, and bounded exits by left_after day
  SELECT t.chat_id, z.* FROM chat_day_target t CROSS JOIN LATERAL (
    WITH e AS MATERIALIZED (
      SELECT m.left_after, m.left_before, m.end_source, m.end_reason, m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted) AS human
      FROM tg.membership m
      WHERE m.chat_id = t.chat_id AND m.end_source IS NOT NULL AND m.left_before >= t.from_ts)
    SELECT u.day, sum(u.exits) AS exits, sum(u.kicks) AS kicks, sum(u.corrections_out) AS corrections_out,
           sum(u.members_ended) AS members_ended, sum(u.unsure_ended) AS unsure_ended,
           sum(u.unsure_started) AS unsure_started, sum(u.mid_ended) AS mid_ended,
           sum(u.mid_unsure_ended) AS mid_unsure_ended, sum(u.mid_unsure_started) AS mid_unsure_started
    FROM (SELECT (e.left_before AT TIME ZONE 'UTC')::date AS day, (e.end_source = 'event')::int AS exits,
                 (e.end_source = 'event' AND e.end_reason = 'kicked')::int AS kicks,
                 (e.end_source IN ('probe', 'inferred'))::int AS corrections_out, e.human::int AS members_ended,
                 (e.human AND e.end_source <> 'event')::int AS unsure_ended, 0 AS unsure_started,
                 (e.human AND e.left_before AT TIME ZONE 'UTC' = (e.left_before AT TIME ZONE 'UTC')::date)::int AS mid_ended,
                 (e.human AND e.end_source <> 'event'
                    AND e.left_before AT TIME ZONE 'UTC' = (e.left_before AT TIME ZONE 'UTC')::date)::int AS mid_unsure_ended,
                 0 AS mid_unsure_started
          FROM e
          UNION ALL
          SELECT (e.left_after AT TIME ZONE 'UTC')::date, 0, 0, 0, 0, 0, 1, 0, 0,
                 (e.left_after AT TIME ZONE 'UTC' = (e.left_after AT TIME ZONE 'UTC')::date)::int
          FROM e WHERE e.human AND e.end_source <> 'event' AND e.left_after >= t.from_ts) u
    GROUP BY u.day) z
), b AS (
  SELECT t.chat_id, z.* FROM chat_day_target t CROSS JOIN LATERAL (
    SELECT (e.event_at AT TIME ZONE 'UTC')::date AS day, count(*) AS bans_from_out
    FROM tg.member_event e WHERE e.chat_id = t.chat_id AND e.event_at >= t.from_ts AND e.kind = 'ban_out' GROUP BY 1) z
), s0 AS (           -- the last finalized count sample of each day, from the day before the range (for its interval)
  SELECT t.chat_id, z.* FROM chat_day_target t CROSS JOIN LATERAL (
    SELECT DISTINCT ON ((cs.sampled_at AT TIME ZONE 'UTC')::date)
           (cs.sampled_at AT TIME ZONE 'UTC')::date AS day, cs.sampled_at AS sample_at, cs.reported, cs.known_open,
           cs.straddled
    FROM tg.count_sample cs WHERE cs.chat_id = t.chat_id AND cs.sampled_at >= t.from_ts - interval '1 day'
                              AND cs.sampled_at <= :'as_of_ts'::timestamptz AND cs.net_events IS NOT NULL
    ORDER BY (cs.sampled_at AT TIME ZONE 'UTC')::date, cs.sampled_at DESC) z
), s AS (            -- plus the events after that sample until the end of its day: the part of the day's net that
                     -- belongs to the interval of the next sample
  SELECT s0.*,
         (SELECT count(*) FROM tg.membership m WHERE m.chat_id = s0.chat_id AND m.joined_at > s0.sample_at
                                                 AND m.joined_at < (s0.day + 1)::timestamp AT TIME ZONE 'UTC')
         - (SELECT count(*) FROM tg.membership m WHERE m.chat_id = s0.chat_id AND m.end_source = 'event'
                                                  AND m.left_before > s0.sample_at
                                                  AND m.left_before < (s0.day + 1)::timestamp AT TIME ZONE 'UTC') AS tail_net
  FROM s0
), a AS (
  SELECT t.chat_id, z.* FROM chat_day_target t CROSS JOIN LATERAL (
    SELECT ad.day, sum(ad.messages) AS messages, count(*) AS active_users
    FROM tg.activity_daily ad WHERE ad.chat_id = t.chat_id AND ad.day >= t.from_day GROUP BY 1) z
), ua AS (
  SELECT t.chat_id, z.* FROM chat_day_target t CROSS JOIN LATERAL (
    SELECT ud.day, sum(ud.messages) AS messages
    FROM tg.unattributed_daily ud WHERE ud.chat_id = t.chat_id AND ud.day >= t.from_day GROUP BY 1) z
)
SELECT g.chat_id, g.day, coalesce(j.joins, 0), coalesce(j.rejoins, 0), coalesce(x.exits, 0), coalesce(x.kicks, 0),
       coalesce(b.bans_from_out, 0), coalesce(j.joins, 0) - coalesce(x.exits, 0), coalesce(j.short_stays, 0),
       coalesce(f.corrections_in, 0), coalesce(x.corrections_out, 0), s.reported, s.known_open,
       coalesce(a.messages, 0) + coalesce(ua.messages, 0), coalesce(a.active_users, 0),
       coalesce(j.members_started, 0) + coalesce(f.members_started, 0), coalesce(x.members_ended, 0),
       coalesce(x.unsure_started, 0) + coalesce(sp.unsure_started, 0), coalesce(x.unsure_ended, 0),
       coalesce(j.mid_started, 0) + coalesce(f.mid_started, 0) - coalesce(x.mid_ended, 0),
       coalesce(x.mid_unsure_started, 0) + coalesce(sp.mid_started, 0) - coalesce(x.mid_unsure_ended, 0),
       s.sample_at,
       CASE WHEN s.sample_at IS NOT NULL AND p.sample_at IS NOT NULL AND NOT s.straddled AND NOT p.straddled
            THEN coalesce(j.joins, 0) - coalesce(x.exits, 0) - s.tail_net + p.tail_net END,
       now()
FROM grid g
LEFT JOIN j USING (chat_id, day) LEFT JOIN f USING (chat_id, day) LEFT JOIN sp USING (chat_id, day)
LEFT JOIN x USING (chat_id, day) LEFT JOIN b USING (chat_id, day)
LEFT JOIN s USING (chat_id, day) LEFT JOIN a USING (chat_id, day) LEFT JOIN ua USING (chat_id, day)
LEFT JOIN s p ON p.chat_id = g.chat_id AND p.day = g.day - 1;

\endif
DELETE FROM tg.rollup_dirty r USING (
  SELECT d.chat_id FROM tg.rollup_dirty d JOIN chat_day_target t ON t.chat_id = d.chat_id AND d.version = t.dirty_version
  FOR UPDATE OF d SKIP LOCKED) x
WHERE r.chat_id = x.chat_id;
INSERT INTO stats.rollup_run (rollup, started_at, finished_at, as_of, row_count)
SELECT 'chat_day', now(), clock_timestamp(), :'as_of_ts', count(*)
FROM stats.chat_day d JOIN chat_day_target t ON t.chat_id = d.chat_id AND d.day >= t.from_day;
COMMIT;
\endif

------------------------------------------------------------------------------------------------------------
-- R2. Refresh the audience rollups: tg.user_chats, stats.overlap, unique_per_chat, chats_per_user
--     (batch job, hourly; work_mem 64 MB)
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('R2') AS run \gset
\if :run
\echo == R2. refresh user_chats and chat_set, then the pair matrix from the distinct chat sets
-- The pair work is sum over distinct chat sets of k^2, not over people: most people share a few sets.
-- One REPEATABLE READ transaction: every statement reads the snapshot of the first one (the matview refresh
-- included), so overlap, unique and chats_per_user describe the same user_chats, the same tracked chats and the
-- same side of a migration (tg.move_chat_history); readers see the old rows until the commit. Every row is
-- published with :as_of (the time the run was asked to describe). The service runs this in tgaud.jobs.audience
-- (the CLI's forget-user calls audience.refresh) and picks the refresh branch in Python.
BEGIN ISOLATION LEVEL REPEATABLE READ;
SET LOCAL work_mem = '64MB';
SELECT ispopulated AS user_chats_populated FROM pg_matviews WHERE schemaname = 'tg' AND matviewname = 'user_chats' \gset
\if :user_chats_populated
REFRESH MATERIALIZED VIEW CONCURRENTLY tg.user_chats;
\else
REFRESH MATERIALIZED VIEW tg.user_chats;
\endif
DELETE FROM tg.chat_set;
INSERT INTO tg.chat_set (chat_ids, chat_count, users, as_of)
SELECT chat_ids, chat_count, count(*), :'as_of_ts'::timestamptz FROM tg.user_chats GROUP BY chat_ids, chat_count;
DELETE FROM stats.overlap;
:explain
INSERT INTO stats.overlap (chat_a, chat_b, shared, as_of)
WITH tracked AS (SELECT chat_id FROM tg.chat WHERE tracked),
pairs AS (
  SELECT a.chat_id AS chat_a, b.chat_id AS chat_b, sum(s.users) AS shared
  FROM tg.chat_set s CROSS JOIN LATERAL unnest(s.chat_ids) a(chat_id) CROSS JOIN LATERAL unnest(s.chat_ids) b(chat_id)
  WHERE a.chat_id <= b.chat_id
  GROUP BY 1, 2
)
SELECT ta.chat_id, tb.chat_id, coalesce(p.shared, 0), :'as_of_ts'::timestamptz
FROM tracked ta JOIN tracked tb ON ta.chat_id <= tb.chat_id
LEFT JOIN pairs p ON p.chat_a = ta.chat_id AND p.chat_b = tb.chat_id;
DELETE FROM stats.unique_per_chat;
INSERT INTO stats.unique_per_chat (chat_id, members, exclusive, as_of)
SELECT o.chat_a, o.shared, coalesce(x.exclusive, 0), :'as_of_ts'::timestamptz
FROM stats.overlap o
LEFT JOIN (SELECT chat_ids[1] AS chat_id, users AS exclusive FROM tg.chat_set WHERE chat_count = 1) x ON x.chat_id = o.chat_a
WHERE o.chat_a = o.chat_b;
DELETE FROM stats.chats_per_user;
INSERT INTO stats.chats_per_user (chat_count, users, as_of)
SELECT chat_count, sum(users), :'as_of_ts'::timestamptz FROM tg.chat_set GROUP BY chat_count;
INSERT INTO stats.rollup_run (rollup, started_at, finished_at, as_of, row_count)
SELECT 'audience', now(), clock_timestamp(), :'as_of_ts', count(*) FROM stats.overlap;
COMMIT;
\endif

------------------------------------------------------------------------------------------------------------
-- R3. snapshot_member retention (reconciler, after each reconciliation)
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('R3') AS run \gset
\if :run
\echo == R3. keep the member rows of the last 2 good snapshots per chat; tg.sighting keeps the evidence
WITH keep AS (
  SELECT snapshot_id FROM (
    SELECT snapshot_id, row_number() OVER (PARTITION BY chat_id ORDER BY started_at DESC) AS rn
    FROM tg.snapshot WHERE quality = 'good') x
  WHERE rn <= 2
), prune AS (
  UPDATE tg.snapshot s SET members_pruned_at = now()
  WHERE s.reconciled_at IS NOT NULL AND s.members_pruned_at IS NULL
    AND s.snapshot_id NOT IN (SELECT snapshot_id FROM keep)
  RETURNING s.snapshot_id
)
DELETE FROM tg.snapshot_member m USING prune p WHERE m.snapshot_id = p.snapshot_id;
\endif

------------------------------------------------------------------------------------------------------------
-- R4. Partitions (service, at start and daily)
------------------------------------------------------------------------------------------------------------
SELECT :'only' IN ('R4') AS run \gset
\if :run
\echo == R4. create partitions ahead, drop expired raw data
SELECT * FROM tg.maintain_partitions(:'as_of_ts');
\endif
