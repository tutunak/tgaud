-- Scale data for schema.sql, with generate_series. Run as the container superuser on a database where
-- schema.sql was loaded by the tenant:  psql -U postgres -d audscale -f 60-gen-scale.sql
-- Loader-only settings (not the service's): FK triggers off (session_replication_role = replica; the
-- fixture proved the FKs), work_mem 128 MB and maintenance_work_mem 192 MB for the bulk sort and index
-- builds. CHECK constraints and generated columns stay on. The heavy indexes of membership and member_event
-- are dropped before the load and rebuilt after it, one at a time, with their build times printed.
\set ON_ERROR_STOP 1
\timing on
SET session_replication_role = replica;
SET work_mem = '128MB';
SET maintenance_work_mem = '192MB';
SELECT setseed(0.42);

CREATE TEMP TABLE t AS SELECT timestamptz '2026-11-23 18:00+00' AS t_end,
                              timestamptz '2026-11-23 18:00+00' - interval '365 days' AS t0,
                              timestamptz '2026-11-23 18:00+00' - interval '60 days' AS w0;

-- save and drop the heavy indexes and constraints (rebuilt at the end)
CREATE TABLE public.gen_saved_ddl AS
SELECT row_number() OVER () AS n, name, ddl FROM (
  SELECT conname::text AS name, format('ALTER TABLE %s ADD CONSTRAINT %I %s', conrelid::regclass, conname, pg_get_constraintdef(oid)) AS ddl
  FROM pg_constraint WHERE conrelid IN ('tg.membership'::regclass, 'tg.member_event'::regclass) AND contype IN ('x', 'u')
  UNION ALL
  SELECT c.relname::text, replace(pg_get_indexdef(i.indexrelid), ' ON ONLY ', ' ON ')
  FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
  WHERE i.indrelid IN ('tg.membership'::regclass, 'tg.member_event'::regclass) AND NOT i.indisprimary
    AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid)) x;
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT conname, conrelid::regclass AS rel FROM pg_constraint
           WHERE conrelid IN ('tg.membership'::regclass, 'tg.member_event'::regclass) AND contype IN ('x', 'u') LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.rel, r.conname);
  END LOOP;
  FOR r IN SELECT name FROM public.gen_saved_ddl WHERE ddl LIKE 'CREATE%' LOOP
    EXECUTE format('DROP INDEX tg.%I', r.name);
  END LOOP;
END $$;

-- chats: 200, chat 1 is the big channel; every third chat is a channel linked to the next (its group)
INSERT INTO tg.chat (chat_id, chat_type, title, linked_chat_id, tracked, bot_status, first_seen_at)
SELECT -1003000000000 - n, CASE WHEN n = 1 OR n % 3 = 0 THEN 'channel' ELSE 'supergroup' END, 'scale chat ' || n,
       CASE WHEN n % 3 = 0 THEN -1003000000000 - (n + 1) END, true, 'administrator', t.t0
FROM generate_series(1, 200) n, t;
INSERT INTO tg.coverage (chat_id, period) SELECT chat_id, tstzrange(t.t0 - interval '1 day', NULL) FROM tg.chat, t;
SELECT tg.ensure_partitions('member_event', 'month', t.t0 - interval '40 days', t.t_end + interval '90 days') FROM t;

-- 3M users
INSERT INTO tg.tg_user (user_id, is_bot, username, first_name, first_seen_at, last_seen_at)
SELECT 10000000 + u, u % 500 = 0, 'u' || u, 'User', t.t0, t.t_end FROM generate_series(1, 3000000) u, t;

-- how many chats each user is in now: most 1-2, a tail of 10-50
CREATE UNLOGGED TABLE gen_k AS
SELECT 10000000 + u AS user_id,
       CASE WHEN r < 0.65 THEN 1 WHEN r < 0.90 THEN 2 WHEN r < 0.97 THEN 3 + u % 7
            WHEN r < 0.995 THEN 10 + u % 10 ELSE 20 + u % 31 END AS k
FROM (SELECT u, random() AS r FROM generate_series(1, 3000000) u) s;

-- open (user, chat) pairs: k skewed picks among chats 2..200, plus chat 1 for the first 1M users
CREATE UNLOGGED TABLE gen_open AS
SELECT DISTINCT g.user_id, c.chat_no
FROM gen_k g CROSS JOIN LATERAL (SELECT 2 + floor(199 * power(random(), 1.5))::int AS chat_no
                                 FROM generate_series(1, g.k)) c
UNION
SELECT user_id, 1 FROM gen_k WHERE user_id <= 11000000;

-- spells to write: open ones (30% with a join event in the year, 70% seeded by the first snapshot) and
-- closed ones (exact join and exit; 25% short stays; 8% kicked), plus a giveaway in chat 1 60 days ago
CREATE UNLOGGED TABLE gen_spell (n bigserial, user_id bigint, chat_no int, start_kind text, joined_at timestamptz,
  known_from timestamptz, hint timestamptz, left_at timestamptz, end_reason text, suspect boolean);
INSERT INTO gen_spell (user_id, chat_no, start_kind, joined_at, known_from, hint, suspect)
SELECT j.user_id, j.chat_no, CASE WHEN j.joined_at IS NULL THEN 'snapshot' ELSE 'event' END, j.joined_at,
       coalesce(j.joined_at, j.seen_at), CASE WHEN j.joined_at IS NULL THEN j.hint END, j.suspect
FROM (
  SELECT o.user_id, o.chat_no,
         CASE WHEN o.giveaway THEN t.w0 + random() * interval '7 days'
              WHEN o.r < 0.3 THEN t.t0 + interval '1 day' + random() * interval '363 days' END AS joined_at,
         t.t0 + interval '10 minutes' + random() * interval '30 minutes' AS seen_at,
         t.t0 - random() * interval '1500 days' AS hint,
         random() < 0.002 AS suspect
  FROM (SELECT g.*, random() AS r, (g.chat_no = 1 AND g.user_id <= 10030000) AS giveaway FROM gen_open g) o, t
) j;

CREATE UNLOGGED TABLE gen_closed AS
SELECT DISTINCT ON (user_id, chat_no) user_id, chat_no
FROM (SELECT 10000001 + floor(random() * 3000000)::bigint AS user_id, 2 + floor(199 * power(random(), 1.5))::int AS chat_no
      FROM generate_series(1, 1300000)) c
WHERE NOT EXISTS (SELECT 1 FROM gen_open o WHERE o.user_id = c.user_id AND o.chat_no = c.chat_no);
INSERT INTO gen_spell (user_id, chat_no, start_kind, joined_at, known_from, left_at, end_reason, suspect)
SELECT c.user_id, c.chat_no, 'event', j, j,
       least(t.t_end - interval '1 minute',
             j + CASE WHEN random() < 0.25 THEN interval '10 minutes' + random() * interval '47 hours'
                      ELSE interval '2 days' + random() * interval '118 days' END),
       CASE WHEN random() < 0.08 THEN 'kicked' ELSE 'left' END, false
FROM gen_closed c, t, LATERAL (SELECT t.t0 + interval '1 day' + random() * interval '360 days' + (c.user_id * 0) * interval '1 s' AS j) x;
-- the chat 1 giveaway: 30k quick exits, 10k later exits, joined in the 7-day window (30k more stayed, above)
INSERT INTO gen_spell (user_id, chat_no, start_kind, joined_at, known_from, left_at, end_reason, suspect)
SELECT 11000000 + u, 1, 'event', j, j,
       j + CASE WHEN u <= 30000 THEN interval '1 hour' + random() * interval '46 hours'
                ELSE interval '3 days' + random() * interval '7 days' END,
       'left', false
FROM generate_series(1, 40000) u, t, LATERAL (SELECT t.w0 + random() * interval '7 days' + (u * 0) * interval '1 s' AS j) x;

-- events: a join for every exact start, an exit for every exact end, and a ban from out after 400k exits
INSERT INTO tg.member_event (event_id, chat_id, user_id, event_at, update_id, received_at, old_status, new_status, actor_id)
SELECT 2 * s.n, -1003000000000 - s.chat_no, s.user_id, s.joined_at, 2 * s.n, s.joined_at + interval '1 second', 'left', 'member', NULL
FROM gen_spell s WHERE s.joined_at IS NOT NULL;
INSERT INTO tg.member_event (event_id, chat_id, user_id, event_at, update_id, received_at, old_status, new_status, actor_id)
SELECT 2 * s.n + 1, -1003000000000 - s.chat_no, s.user_id, s.left_at, 2 * s.n + 1, s.left_at + interval '1 second', 'member',
       CASE s.end_reason WHEN 'kicked' THEN 'kicked' ELSE 'left' END, CASE s.end_reason WHEN 'kicked' THEN 9001 END
FROM gen_spell s WHERE s.left_at IS NOT NULL;
INSERT INTO tg.member_event (event_id, chat_id, user_id, event_at, update_id, received_at, old_status, new_status, actor_id)
SELECT 100000000 + s.n, -1003000000000 - s.chat_no, s.user_id, s.left_at + interval '1 day', 100000000 + s.n,
       s.left_at + interval '1 day 1 second', 'left', 'kicked', 9001
FROM gen_spell s, t WHERE s.left_at IS NOT NULL AND s.end_reason = 'left' AND s.left_at < t.t_end - interval '1 day' AND s.n % 3 = 0
LIMIT 400000;

-- spells
INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, join_event_id, joined_hint, known_from, join_route,
                           last_confirmed_at, suspect_since, end_source, end_reason, left_at, left_event_id, left_after, left_before)
SELECT -1003000000000 - s.chat_no, s.user_id, s.start_kind, s.joined_at, CASE WHEN s.joined_at IS NOT NULL THEN 2 * s.n END,
       s.hint, s.known_from, CASE WHEN s.joined_at IS NOT NULL THEN 'unattributed' END, s.known_from,
       CASE WHEN s.suspect AND s.left_at IS NULL THEN t.t_end - interval '3 days' END,
       CASE WHEN s.left_at IS NOT NULL THEN 'event' END, s.end_reason, s.left_at,
       CASE WHEN s.left_at IS NOT NULL THEN 2 * s.n + 1 END, s.left_at, s.left_at
FROM gen_spell s, t;

-- the campaign, a few links, snapshots, count samples, activity, posts, join requests
INSERT INTO tg.campaign (name, kind, origin, starts_at, draw_at, only_new_members)
SELECT 'chat 1 giveaway', 'giveaway', 'annotation', t.w0, t.w0 + interval '7 days', true FROM t;
INSERT INTO tg.campaign_chat SELECT campaign_id, -1003000000001 FROM tg.campaign;
INSERT INTO tg.snapshot (chat_id, started_at, finished_at, method, reported_at_start, reported_at_end, fetched, complete, quality, reconciled_at)
SELECT c.chat_id, t.t0, t.t0 + interval '1 hour', 'listing', 1, 1, 1, true, 'good', t.t0 + interval '2 hours' FROM tg.chat c, t;
INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, suspect_open, net_events)
SELECT c.chat_id, ts, 1000 + (random() * 10)::int, 1000, 0, 0
FROM tg.chat c, t, generate_series(t.t_end - interval '30 days', t.t_end, interval '10 minutes') ts;
INSERT INTO tg.activity_daily (chat_id, user_id, day, messages)
SELECT DISTINCT ON (chat_id, user_id, day) chat_id, user_id, day, 1 + (random() * 20)::int
FROM (SELECT -1003000000000 - (2 + floor(199 * power(random(), 1.5))::int) AS chat_id,
             10000001 + floor(random() * 3000000)::bigint AS user_id,
             ((SELECT t_end FROM t) - random() * interval '90 days')::date AS day
      FROM generate_series(1, 1600000)) a
WHERE chat_id IN (SELECT chat_id FROM tg.chat WHERE chat_type = 'supergroup');
INSERT INTO tg.post (chat_id, message_id, posted_at, kind)
SELECT c.chat_id, row_number() OVER (PARTITION BY c.chat_id ORDER BY ts), ts, 'text'
FROM tg.chat c, t, generate_series(t.t0, t.t_end, interval '8 hours') ts WHERE c.chat_type = 'channel';
INSERT INTO tg.join_request (chat_id, user_id, requested_at, outcome, resolved_at)
SELECT DISTINCT ON (chat_id, user_id, requested_at) chat_id, user_id, requested_at, 'approved', requested_at + interval '1 hour'
FROM (SELECT -1003000000000 - (2 + floor(199 * power(random(), 1.5))::int) AS chat_id,
             10000001 + floor(random() * 3000000)::bigint AS user_id,
             (SELECT t0 FROM t) + random() * interval '360 days' AS requested_at
      FROM generate_series(1, 50000)) r;

SELECT setval(pg_get_serial_sequence('tg.member_event', 'event_id'), 200000000);
SELECT setval(pg_get_serial_sequence('tg.membership', 'membership_id'), (SELECT max(membership_id) FROM tg.membership));

-- rebuild the dropped indexes and constraints, timing each
DO $$ DECLARE r record; t0 timestamptz; BEGIN
  FOR r IN SELECT * FROM public.gen_saved_ddl ORDER BY n LOOP
    t0 := clock_timestamp();
    EXECUTE r.ddl;
    RAISE NOTICE 'built % in % s', r.name, round(extract(epoch FROM clock_timestamp() - t0)::numeric, 1);
  END LOOP;
END $$;
DROP TABLE public.gen_saved_ddl, gen_k, gen_open, gen_closed, gen_spell;

SELECT 'membership' AS rel, count(*) FROM tg.membership
UNION ALL SELECT 'open spells', count(*) FROM tg.membership WHERE end_source IS NULL
UNION ALL SELECT 'open in chat 1', count(*) FROM tg.membership WHERE end_source IS NULL AND chat_id = -1003000000001
UNION ALL SELECT 'distinct users with an open spell', count(DISTINCT user_id) FROM tg.membership WHERE end_source IS NULL
UNION ALL SELECT 'member_event', count(*) FROM tg.member_event
UNION ALL SELECT 'tg_user', count(*) FROM tg.tg_user
UNION ALL SELECT 'count_sample', count(*) FROM tg.count_sample
UNION ALL SELECT 'activity_daily', count(*) FROM tg.activity_daily;
