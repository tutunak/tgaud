-- Fixture for schema.sql: every history case of D3-D5 in its own chat (H01..H14), plus four analytics chats
-- (A channel, B its discussion group, C group, D channel). Run as the tenant login, after admin.sql.
-- Spells are written by hand from the rebuild rules (design.md 4); their event times are read from the events
-- they name, so a typo in a time cannot hide. "Now" for every report is 2026-11-23 12:00 UTC.
\set ON_ERROR_STOP 1
\set A  -1001000000001
\set B  -1001000000002
\set C  -1001000000003
\set D  -1001000000004
\set OLD13 -400013
\set H13 -1002000000013

BEGIN;
SELECT tg.ensure_partitions('member_event', 'month', '2026-07-01', '2027-01-01') AS member_event_partitions;
SELECT * FROM tg.maintain_partitions('2026-11-23 12:00+00');

INSERT INTO tg.bot_state (bot_id, next_offset, last_update_id, last_update_at, last_poll_ok_at, allowed_updates)
VALUES (5000, 502406, 502405, '2026-11-23 03:40+00', '2026-11-23 11:59+00',
        '{chat_member,my_chat_member,chat_join_request,message,channel_post}');

-- chats ------------------------------------------------------------------------------------------------
INSERT INTO tg.chat (chat_id, chat_type, title, username, linked_chat_id, tracked, bot_status, can_invite_users, first_seen_at) VALUES
  (:A, 'channel',    'Alpha news', 'alphanews', :B, true, 'administrator', true, '2026-08-01+00'),
  (:B, 'supergroup', 'Alpha chat', NULL,        :A, true, 'administrator', true, '2026-08-01+00'),
  (:C, 'supergroup', 'Gamma group', 'gamma',  NULL, true, 'administrator', true, '2026-08-01+00'),
  (:D, 'channel',    'Delta',       NULL,     NULL, true, 'administrator', true, '2026-08-01+00'),
  (:OLD13, 'group',  'case 13 migration (old basic group)', NULL, NULL, true, 'administrator', true, '2026-08-01+00');
INSERT INTO tg.chat (chat_id, chat_type, title, tracked, bot_status, first_seen_at)
SELECT -1002000000000 - n, 'supergroup', format('case %s %s', to_char(n, 'FM00'), label), true, 'administrator', '2026-08-01+00'
FROM (VALUES (1, 'join'), (2, 'leave'), (3, 'rejoin'), (4, 'ban from in'), (5, 'ban from out'), (6, 'unban'),
             (7, 'restricted out and back'), (8, 'left-censored leave'), (9, 'snapshot-seeded'),
             (10, 'absent then probe out'), (11, 'failed probe'), (12, 'missed leave then join'),
             (13, 'migration (new supergroup)'), (14, 'outage gap')) v(n, label);

INSERT INTO tg.coverage (chat_id, period)
SELECT chat_id, tstzrange('2026-08-01 00:00+00', NULL) FROM tg.chat WHERE chat_id NOT IN (:OLD13, :H13);
INSERT INTO tg.coverage (chat_id, period) VALUES
  (:OLD13, tstzrange('2026-08-01 00:00+00', '2026-09-10 12:00+00')),
  (:H13,   tstzrange('2026-09-10 12:00+00', NULL));

-- the outage of case 14: the service was down 36 h, longer than Telegram keeps updates; every chat is hit
INSERT INTO tg.gap (chat_id, period, reason, detected_at, note) VALUES
  (NULL, tstzrange('2026-08-10 00:00+00', '2026-08-11 12:00+00'), 'service_down', '2026-08-11 12:01+00',
   'poller down; updates older than 24 h were dropped by Telegram');

-- users ------------------------------------------------------------------------------------------------
INSERT INTO tg.tg_user (user_id, username, first_name, first_seen_at, last_seen_at)
SELECT u, 'user' || u, 'User ' || u, '2026-08-01+00'::timestamptz, '2026-11-23+00'::timestamptz
FROM generate_series(1001, 1033) u UNION ALL
SELECT u, NULL, 'User ' || u, '2026-08-01+00', '2026-11-23+00' FROM generate_series(2001, 2018) u;
INSERT INTO tg.tg_user (user_id, is_bot, is_deleted, username, first_name, first_seen_at, last_seen_at) VALUES
  (3001, true,  false, 'somehelperbot', 'Helper bot', '2026-08-01+00', '2026-11-23+00'),
  (3002, false, true,  NULL, NULL, '2026-08-01+00', '2026-11-23+00');

-- links and campaigns ----------------------------------------------------------------------------------
INSERT INTO tg.campaign (campaign_id, name, kind, origin, starts_at, draw_at, only_new_members, winner_count,
                         post_chat_id, post_message_id, note) VALUES
  (1, 'Alpha giveaway', 'giveaway', 'annotation', '2026-11-01 00:00+00', '2026-11-08 00:00+00', true, 3,
   NULL, NULL, 'owner entered: subscribe to Alpha news and Alpha chat'),
  (2, 'Gamma Telegram giveaway', 'tg_giveaway', 'observation', '2026-10-01 00:00+00', '2026-10-05 00:00+00', true, 1,
   :C, 500, 'parsed from message.giveaway');
SELECT setval(pg_get_serial_sequence('tg.campaign', 'campaign_id'), 2);
INSERT INTO tg.campaign_chat VALUES (1, :A), (1, :B), (2, :C);

INSERT INTO tg.invite_link (invite_link_id, chat_id, masked_link, link_digest, creator_id, name, is_primary,
                            creates_join_request, campaign_id, first_seen_at) VALUES
  (1, :A, 'https://t.me/+AbCd…', NULL, 9001, 'Promo summer', false, false, NULL, '2026-08-20 10:00+00'),
  (2, :A, NULL, '\x0a0b0c'::bytea, 5000, 'G1 giveaway A', false, false, 1, '2026-10-31 12:00+00'),
  (3, :B, 'https://t.me/+Zz…', NULL, 9002, NULL, true, false, NULL, '2026-08-01 00:00+00'),
  (4, :B, NULL, '\x0d0e0f'::bytea, 5000, 'G1 giveaway B', false, false, 1, '2026-10-31 12:00+00');
SELECT setval(pg_get_serial_sequence('tg.invite_link', 'invite_link_id'), 4);

-- events: (event_id, chat, user, time, old status, old is_member, new status, new is_member, actor, link, request, folder)
INSERT INTO tg.member_event (event_id, chat_id, user_id, event_at, update_id, received_at, old_status, old_is_member,
                             new_status, new_is_member, actor_id, invite_link_id, via_join_request, via_folder_link)
SELECT e, c, u, t, e + 500000, t + interval '1 second', os, om, ns, nm, actor, link, req, folder
FROM (VALUES
  -- A
  (101, :A, 1003, '2026-08-20 10:00+00'::timestamptz, 'left', NULL::boolean, 'member', NULL::boolean, NULL::bigint, 1::bigint, false, false),
  (102, :A, 1004, '2026-09-02 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (103, :A, 1005, '2026-11-02 10:00+00', 'left', NULL, 'member', NULL, NULL, 2, false, false),
  (104, :A, 1006, '2026-09-01 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (105, :A, 1006, '2026-09-10 10:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (106, :A, 1006, '2026-11-03 10:00+00', 'left', NULL, 'member', NULL, NULL, 2, false, false),
  (107, :A, 1007, '2026-08-15 10:00+00', 'left', NULL, 'member', NULL, 9001, NULL, false, false),
  (108, :A, 1009, '2026-08-16 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, true),
  (109, :A, 1010, '2026-10-10 10:00+00', 'left', NULL, 'member', NULL, 9002, NULL, true, false),
  (110, :A, 1016, '2026-11-04 10:00+00', 'left', NULL, 'member', NULL, NULL, 2, false, false),
  (111, :A, 1016, '2026-11-05 09:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (112, :A, 1017, '2026-11-07 12:00+00', 'left', NULL, 'member', NULL, NULL, 2, false, false),
  (113, :A, 1017, '2026-11-08 06:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (114, :A, 1018, '2026-11-06 00:00+00', 'left', NULL, 'member', NULL, NULL, 2, false, false),
  (115, :A, 1018, '2026-11-09 12:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (116, :A, 1019, '2026-11-05 00:00+00', 'left', NULL, 'member', NULL, NULL, 2, false, false),
  (117, :A, 1019, '2026-11-06 00:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (118, :A, 1022, '2026-10-20 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (119, :A, 1022, '2026-10-21 08:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (120, :A, 1023, '2026-10-20 10:05+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (121, :A, 1023, '2026-10-28 10:05+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  -- B
  (201, :B, 1003, '2026-08-20 10:30+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (202, :B, 1004, '2026-09-03 09:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (203, :B, 1012, '2026-11-06 12:00+00', 'left', NULL, 'member', NULL, NULL, 4, false, false),
  (204, :B, 1019, '2026-11-05 00:05+00', 'left', NULL, 'member', NULL, NULL, 4, false, false),
  (205, :B, 1020, '2026-11-02 08:00+00', 'left', NULL, 'member', NULL, NULL, 4, false, false),
  (206, :B, 1020, '2026-11-02 09:00+00', 'member', NULL, 'kicked', NULL, 9002, NULL, false, false),
  (207, :B, 1022, '2026-10-20 10:02+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (208, :B, 1022, '2026-10-21 09:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (209, :B, 1023, '2026-10-20 10:07+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (210, :B, 1023, '2026-10-21 10:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  -- C (307-318: a raid: six joins in one minute, five kicked within the hour)
  (301, :C, 1001, '2026-08-25 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (302, :C, 1002, '2026-08-26 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (303, :C, 1011, '2026-09-05 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (304, :C, 1013, '2026-10-02 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (305, :C, 1021, '2026-10-03 00:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (306, :C, 1021, '2026-10-03 20:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (307, :C, 1024, '2026-11-23 03:00:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (308, :C, 1025, '2026-11-23 03:00:10+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (309, :C, 1026, '2026-11-23 03:00:20+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (310, :C, 1027, '2026-11-23 03:00:30+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (311, :C, 1028, '2026-11-23 03:00:40+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (312, :C, 1029, '2026-11-23 03:00:50+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (313, :C, 1024, '2026-11-23 03:20+00', 'member', NULL, 'kicked', NULL, 9002, NULL, false, false),
  (314, :C, 1025, '2026-11-23 03:20+00', 'member', NULL, 'kicked', NULL, 9002, NULL, false, false),
  (315, :C, 1026, '2026-11-23 03:20+00', 'member', NULL, 'kicked', NULL, 9002, NULL, false, false),
  (316, :C, 1027, '2026-11-23 03:20+00', 'member', NULL, 'kicked', NULL, 9002, NULL, false, false),
  (317, :C, 1028, '2026-11-23 03:20+00', 'member', NULL, 'kicked', NULL, 9002, NULL, false, false),
  (318, :C, 1029, '2026-11-23 03:40+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  -- D
  (401, :D, 1001, '2026-08-30 12:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (402, :D, 1030, '2026-09-05 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (403, :D, 1030, '2026-11-10 14:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (404, :D, 1031, '2026-09-06 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (405, :D, 1031, '2026-11-10 08:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  -- history cases
  (1101, -1002000000001, 2001, '2026-09-01 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (1201, -1002000000002, 2002, '2026-09-01 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (1202, -1002000000002, 2002, '2026-09-20 10:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (1301, -1002000000003, 2003, '2026-09-01 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (1302, -1002000000003, 2003, '2026-09-05 10:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (1303, -1002000000003, 2003, '2026-09-10 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (1401, -1002000000004, 2004, '2026-09-01 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (1402, -1002000000004, 2004, '2026-09-08 10:00+00', 'member', NULL, 'kicked', NULL, 9001, NULL, false, false),
  (1501, -1002000000005, 2005, '2026-09-01 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (1502, -1002000000005, 2005, '2026-09-06 10:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (1503, -1002000000005, 2005, '2026-09-07 10:00+00', 'left', NULL, 'kicked', NULL, 9001, NULL, false, false),
  (1601, -1002000000006, 2006, '2026-09-01 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (1602, -1002000000006, 2006, '2026-09-04 10:00+00', 'member', NULL, 'kicked', NULL, 9001, NULL, false, false),
  (1603, -1002000000006, 2006, '2026-09-09 10:00+00', 'kicked', NULL, 'left', NULL, 9001, NULL, false, false),
  (1701, -1002000000007, 2007, '2026-09-01 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (1702, -1002000000007, 2007, '2026-09-03 10:00+00', 'member', NULL, 'restricted', true, 9001, NULL, false, false),
  (1703, -1002000000007, 2007, '2026-09-05 10:00+00', 'restricted', true, 'restricted', false, NULL, NULL, false, false),
  (1704, -1002000000007, 2007, '2026-09-12 10:00+00', 'restricted', false, 'restricted', true, NULL, NULL, false, false),
  (1801, -1002000000008, 2008, '2026-09-15 10:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  (2201, -1002000000012, 2012, '2026-09-01 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (2202, -1002000000012, 2012, '2026-09-15 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  -- case 13: two joins in the old basic group, then join echoes in the new supergroup at migration
  (2301, :OLD13, 2013, '2026-09-01 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (2302, :OLD13, 2014, '2026-09-02 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (2303, :H13,   2013, '2026-09-10 12:00:05+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (2304, :H13,   2014, '2026-09-10 12:00:05+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (2305, :H13,   2014, '2026-09-15 10:00+00', 'member', NULL, 'left', NULL, NULL, NULL, false, false),
  -- case 14: a join before the outage (its leave fell into the outage), a join after it
  (2401, -1002000000014, 2015, '2026-08-05 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false),
  (2402, -1002000000014, 2016, '2026-08-12 10:00+00', 'left', NULL, 'member', NULL, NULL, NULL, false, false)
) v(e, c, u, t, os, om, ns, nm, actor, link, req, folder);
SELECT setval(pg_get_serial_sequence('tg.member_event', 'event_id'), 10000);

-- one raw update, stripped of bio and user_chat_id, in today's daily partition
INSERT INTO tg.update_receipt (update_id, update_date, update_type, chat_id, received_at)
VALUES (500318, '2026-11-23 03:40+00', 'chat_member', :C, '2026-11-23 03:40:01+00');
INSERT INTO tg.raw_update (update_id, update_date, update_type, chat_id, user_id, received_at, payload)
VALUES (500318, '2026-11-23 03:40+00', 'chat_member', :C, 1029, '2026-11-23 03:40:01+00',
        '{"date": 1795412400, "old_chat_member": {"status": "member"}, "new_chat_member": {"status": "left"}}');

COMMIT;

-- snapshots, written as the collect script would (role collect_writer, snapshot contract) ------------------
CREATE TEMP TABLE fx_snap (n int, chat_id bigint, started timestamptz, finished timestamptz,
  rep_start int, rep_end int, complete boolean, users bigint[], hints timestamptz[]);
INSERT INTO fx_snap VALUES
 (1,  :A, '2026-08-01 01:00+00', '2026-08-01 01:30+00', 5, 5, true, '{1001,1002,1008,3001,3002}',
      '{2025-03-01,2025-06-01,2026-01-10,NULL,NULL}'),
 (2,  :B, '2026-08-01 01:00+00', '2026-08-01 01:20+00', 3, 3, true, '{1001,1002,1011}', '{2025-04-01,2025-06-02,2025-07-07}'),
 (3,  :C, '2026-08-01 01:00+00', '2026-08-01 01:05+00', 1, 1, true, '{1014}', '{2026-07-01}'),
 (4,  :D, '2026-08-01 01:00+00', '2026-08-01 01:05+00', 1, 1, true, '{1015}', '{2026-02-02}'),
 (5,  :A, '2026-08-12 00:00+00', '2026-08-12 00:30+00', 5, 5, true, '{1001,1002,1008,3001,3002}',
      '{2025-03-01,2025-06-01,2026-01-10,NULL,NULL}'),
 (6,  :B, '2026-08-12 00:00+00', '2026-08-12 00:20+00', 3, 3, true, '{1001,1002,1011}', '{2025-04-01,2025-06-02,2025-07-07}'),
 (7,  :C, '2026-08-12 00:00+00', '2026-08-12 00:05+00', 1, 1, true, '{1014}', '{2026-07-01}'),
 (8,  :D, '2026-08-12 00:00+00', '2026-08-12 00:05+00', 1, 1, true, '{1015}', '{2026-02-02}'),
 (9,  -1002000000009, '2026-09-02 00:00+00', '2026-09-02 00:30+00', 1, 1, true, '{2009}', '{2024-05-05}'),
 (10, -1002000000010, '2026-09-02 00:00+00', '2026-09-02 00:30+00', 2, 2, true, '{2010,2017}', '{2024-01-01,2024-02-02}'),
 (11, -1002000000010, '2026-09-20 00:00+00', '2026-09-20 00:30+00', 1, 1, true, '{2017}', '{2024-02-02}'),
 (12, -1002000000011, '2026-09-02 00:00+00', '2026-09-02 00:30+00', 2, 2, true, '{2011,2018}', '{2024-03-03,2024-04-04}'),
 (13, -1002000000011, '2026-09-20 00:00+00', '2026-09-20 00:30+00', 1, 1, true, '{2018}', '{2024-04-04}'),
 (14, :B, '2026-11-11 00:00+00', '2026-11-11 00:20+00', 6, 6, true, '{1001,1002,1003,1004,1011,1012}',
      '{2025-04-01,2025-06-02,"2026-08-20 10:30+00","2026-09-03 09:00+00",2025-07-07,"2026-11-06 12:00+00"}'),
 -- a partial listing (the script was interrupted): positive sightings count, absences do not
 (15, :A, '2026-11-20 00:00+00', '2026-11-20 00:20+00', 15, 15, false, '{1001,1005,1007}',
      '{2025-03-01,"2026-11-02 10:00+00","2026-08-15 10:00+00"}');
GRANT SELECT ON fx_snap TO collect_writer;

SET ROLE collect_writer;
DO $$
DECLARE s record; v_id bigint;
BEGIN
  FOR s IN SELECT * FROM fx_snap ORDER BY n LOOP
    -- step 0: the chat row, if the service does not know the chat yet
    INSERT INTO tg.chat (chat_id, chat_type, title) VALUES (s.chat_id, 'supergroup', 'from collect') ON CONFLICT (chat_id) DO NOTHING;
    -- step 1: the snapshot, unfinished
    INSERT INTO tg.snapshot (chat_id, started_at, method, reported_at_start)
    VALUES (s.chat_id, s.started, 'listing', s.rep_start) RETURNING snapshot_id INTO v_id;
    IF v_id <> s.n THEN RAISE EXCEPTION 'snapshot id % expected %', v_id, s.n; END IF;
    -- step 2: its members, one row per user, in pages
    INSERT INTO tg.snapshot_member (snapshot_id, user_id, seen_at, joined_date, is_bot, is_deleted)
    SELECT v_id, u, s.started + interval '10 minutes', h, u = 3001, u = 3002
    FROM unnest(s.users, s.hints) x(u, h);
    -- step 3: mark it finished, in a last transaction
    UPDATE tg.snapshot SET finished_at = s.finished, reported_at_end = s.rep_end,
           fetched = cardinality(s.users), complete = s.complete
     WHERE snapshot_id = v_id;
  END LOOP;
END $$;
RESET ROLE;
DROP TABLE fx_snap;

BEGIN;
-- the reconciler grades each finished snapshot (design.md 5: good = complete and shortfall <= max(10, 0.1%))
UPDATE tg.snapshot SET quality = CASE WHEN complete AND greatest(reported_at_start, reported_at_end) - fetched
                                                     <= greatest(10, 0.001 * greatest(reported_at_start, reported_at_end))
                                      THEN 'good' ELSE 'partial' END,
                       reconciled_at = finished_at + interval '5 minutes';

-- durable sightings (runs of consecutive sightings), folded from snapshot_member by the reconciler
INSERT INTO tg.sighting (chat_id, user_id, first_seen_at, first_snapshot_id, last_seen_at, last_snapshot_id, times_seen, joined_date)
SELECT s.chat_id, m.user_id, min(m.seen_at), (array_agg(s.snapshot_id ORDER BY s.started_at))[1],
       max(m.seen_at), (array_agg(s.snapshot_id ORDER BY s.started_at DESC))[1], count(*),
       (array_agg(m.joined_date ORDER BY s.started_at DESC))[1]
FROM tg.snapshot s JOIN tg.snapshot_member m USING (snapshot_id)
GROUP BY s.chat_id, m.user_id;

-- probes: absence from a good snapshot (cases 10, 11 and B/1019) and the post-outage check (case 14)
INSERT INTO tg.probe (chat_id, user_id, reason, snapshot_id, absent_since, queued_at, attempts, last_attempt_at,
                      outcome, answered_at, status_seen, error_code) VALUES
  (-1002000000010, 2010, 'snapshot_absent', 11, '2026-09-20 00:00+00', '2026-09-20 00:35+00', 1, '2026-09-21 00:00+00',
   'out', '2026-09-21 00:00+00', 'left', NULL),
  (-1002000000011, 2011, 'snapshot_absent', 13, '2026-09-20 00:00+00', '2026-09-20 00:35+00', 3, '2026-09-21 00:00+00',
   'error', NULL, NULL, 'timeout'),
  (:B, 1019, 'snapshot_absent', 14, '2026-11-11 00:00+00', '2026-11-11 00:25+00', 1, '2026-11-13 00:00+00',
   'out', '2026-11-13 00:00+00', 'left', NULL),
  (-1002000000014, 2015, 'outage', NULL, NULL, '2026-08-11 12:30+00', 1, '2026-08-11 13:00+00',
   'out', '2026-08-11 13:00+00', 'left', NULL);

INSERT INTO tg.reconcile_run (chat_id, snapshot_id, started_at, finished_at, listed, seeded, missed_joins, confirmed,
                              absent_candidates, probes_queued, closed_by_probe, conflicts)
SELECT s.chat_id, s.snapshot_id, s.reconciled_at, s.reconciled_at + interval '1 second', s.fetched,
       CASE s.snapshot_id WHEN 1 THEN 5 WHEN 2 THEN 3 WHEN 3 THEN 1 WHEN 4 THEN 1 WHEN 9 THEN 1 WHEN 10 THEN 2 WHEN 12 THEN 2 ELSE 0 END,
       0,
       CASE s.snapshot_id WHEN 5 THEN 5 WHEN 6 THEN 3 WHEN 7 THEN 1 WHEN 8 THEN 1 WHEN 11 THEN 1 WHEN 13 THEN 1
                          WHEN 14 THEN 6 WHEN 15 THEN 3 ELSE 0 END,
       CASE s.snapshot_id WHEN 11 THEN 1 WHEN 13 THEN 1 WHEN 14 THEN 1 ELSE 0 END,
       CASE s.snapshot_id WHEN 11 THEN 1 WHEN 13 THEN 1 WHEN 14 THEN 1 ELSE 0 END, 0, 0
FROM tg.snapshot s;

-- case 13 before the migration message was processed: the incremental apply had opened spells in both ids
INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, join_event_id, known_from, join_route, last_confirmed_at)
SELECT e.chat_id, e.user_id, 'event', e.event_at, e.event_id, e.event_at, 'unattributed', e.event_at
FROM tg.member_event e WHERE e.event_id IN (2301, 2302, 2303);
INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, join_event_id, known_from, join_route, last_confirmed_at,
                           end_source, end_reason, left_at, left_event_id, left_after, left_before)
SELECT j.chat_id, j.user_id, 'event', j.event_at, j.event_id, j.event_at, 'unattributed', j.event_at,
       'event', 'left', l.event_at, l.event_id, l.event_at, l.event_at
FROM tg.member_event j JOIN tg.member_event l ON l.event_id = 2305 WHERE j.event_id = 2304;

-- the migration: history moves to the supergroup id in one transaction (D2) ...
SELECT tg.move_chat_history(:OLD13, :H13, '2026-09-10 12:00+00') AS moved_events;
-- ... and the rebuild of the affected pairs absorbs the join echoes (rule M1 in design.md 4)
DELETE FROM tg.membership WHERE chat_id = :H13;
COMMIT;

-- spells, by hand -------------------------------------------------------------------------------------
BEGIN;
-- exact join, still open: (chat, user, join event, last positive evidence, Telegram's join date from a snapshot)
INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, join_event_id, known_from, join_route,
                           invite_link_id, last_confirmed_at, joined_hint)
SELECT j.chat_id, j.user_id, 'event', j.event_at, j.event_id, j.event_at,
       CASE WHEN j.invite_link_id IS NOT NULL THEN 'link' WHEN j.via_join_request THEN 'request'
            WHEN j.via_folder_link THEN 'folder' WHEN j.actor_id IS NOT NULL THEN 'added' ELSE 'unattributed' END,
       j.invite_link_id, coalesce(v.last_conf, j.event_at), v.hint
FROM (VALUES
  (101::bigint, NULL::timestamptz, NULL::timestamptz),          -- A 1003 via link "Promo summer"
  (102, NULL, NULL), (103, '2026-11-20 00:10+00', '2026-11-02 10:00+00'), (106, NULL, NULL),
  (107, '2026-11-20 00:10+00', '2026-08-15 10:00+00'), (108, NULL, NULL), (109, NULL, NULL),
  (201, '2026-11-11 00:10+00', '2026-08-20 10:30+00'), (202, '2026-11-11 00:10+00', '2026-09-03 09:00+00'),
  (203, '2026-11-11 00:10+00', '2026-11-06 12:00+00'),
  (301, NULL, NULL), (302, NULL, NULL), (303, NULL, NULL), (304, NULL, NULL), (401, NULL, NULL),
  (1101, NULL, NULL),                                            -- case 1: join
  (1303, NULL, NULL),                                            -- case 3: the rejoin
  (1704, NULL, NULL),                                            -- case 7: restricted is_member false -> true = join
  (2202, NULL, NULL),                                            -- case 12: the observed join after a missed leave
  (2301, '2026-09-10 12:00:05+00', NULL),                        -- case 13: the old-group join; the echo confirms it
  (2402, NULL, NULL)                                             -- case 14: joined after the outage
) v(join_event, last_conf, hint)
JOIN tg.member_event j ON j.event_id = v.join_event;

-- exact join, exact exit: (join event, exit event, last positive evidence before the exit)
INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, join_event_id, known_from, join_route,
                           invite_link_id, last_confirmed_at, end_source, end_reason, left_at, left_event_id,
                           left_after, left_before)
SELECT j.chat_id, j.user_id, 'event', j.event_at, j.event_id, j.event_at,
       CASE WHEN j.invite_link_id IS NOT NULL THEN 'link' WHEN j.via_join_request THEN 'request'
            WHEN j.via_folder_link THEN 'folder' WHEN j.actor_id IS NOT NULL THEN 'added' ELSE 'unattributed' END,
       j.invite_link_id, coalesce(v.last_conf, j.event_at), 'event',
       CASE WHEN l.new_status = 'kicked' OR l.actor_id IS NOT NULL THEN 'kicked' ELSE 'left' END,
       l.event_at, l.event_id, l.event_at, l.event_at
FROM (VALUES
  (104::bigint, 105::bigint, NULL::timestamptz),                 -- A 1006 first stay
  (110, 111, NULL), (112, 113, NULL), (114, 115, NULL), (116, 117, NULL), (118, 119, NULL), (120, 121, NULL),
  (205, 206, NULL), (207, 208, NULL), (209, 210, NULL),
  (305, 306, NULL), (307, 313, NULL), (308, 314, NULL), (309, 315, NULL), (310, 316, NULL), (311, 317, NULL), (312, 318, NULL),
  (402, 403, NULL), (404, 405, NULL),
  (1201, 1202, NULL),                                            -- case 2: leave
  (1301, 1302, NULL),                                            -- case 3: first stay
  (1401, 1402, NULL),                                            -- case 4: ban from in = exit, kicked
  (1501, 1502, NULL),                                            -- case 5: the leave; the later ban from out changes nothing
  (1601, 1602, NULL),                                            -- case 6: the ban; the unban adds no join
  (1701, 1703, '2026-09-03 10:00+00'),                           -- case 7: restricted is_member true -> false = exit
  (2302, 2305, '2026-09-10 12:00:05+00')                         -- case 13: joined in the old group, left the new one
) v(join_event, exit_event, last_conf)
JOIN tg.member_event j ON j.event_id = v.join_event
JOIN tg.member_event l ON l.event_id = v.exit_event;

-- case 8: the first event is a leave: a left-censored spell, start unknown
INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at, end_source, end_reason,
                           left_at, left_event_id, left_after, left_before)
SELECT chat_id, user_id, 'inferred', event_at, event_at, 'event', 'left', event_at, event_id, event_at, event_at
FROM tg.member_event WHERE event_id = 1801;

-- case 12: the open spell of the missed leave closes with an unknown end before the observed join
INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, join_event_id, known_from, join_route,
                           last_confirmed_at, end_source, end_reason, left_after, left_before)
SELECT j.chat_id, j.user_id, 'event', j.event_at, j.event_id, j.event_at, 'unattributed', j.event_at,
       'inferred', 'gone', j.event_at, n.event_at
FROM tg.member_event j JOIN tg.member_event n ON n.event_id = 2202 WHERE j.event_id = 2201;

-- B 1019 and case 14: exact join, closed by a probe with bounds (last positive evidence .. probe answer)
INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, join_event_id, known_from, join_route,
                           invite_link_id, last_confirmed_at, end_source, end_reason, left_after, left_before)
SELECT j.chat_id, j.user_id, 'event', j.event_at, j.event_id, j.event_at,
       CASE WHEN j.invite_link_id IS NOT NULL THEN 'link' ELSE 'unattributed' END, j.invite_link_id, j.event_at,
       'probe', 'gone', j.event_at, p.answered_at
FROM tg.member_event j JOIN tg.probe p ON p.chat_id = j.chat_id AND p.user_id = j.user_id AND p.outcome = 'out'
WHERE j.event_id IN (204, 2401);

-- snapshot-seeded spells (start_source snapshot, no exact join time, Telegram's join date as a hint)
INSERT INTO tg.membership (chat_id, user_id, start_source, joined_hint, known_from, last_confirmed_at, suspect_since)
SELECT s.chat_id, s.user_id, 'snapshot', s.joined_date, s.first_seen_at, s.last_seen_at,
       CASE WHEN (s.chat_id, s.user_id) = (-1002000000011, 2011) THEN '2026-09-20 00:00+00'::timestamptz END
FROM tg.sighting s
WHERE NOT EXISTS (SELECT 1 FROM tg.membership m WHERE m.chat_id = s.chat_id AND m.user_id = s.user_id)
  AND (s.chat_id, s.user_id) <> (-1002000000010, 2010);
-- case 10: seeded, then missing from a good snapshot, then confirmed out by a probe: closed with bounds
INSERT INTO tg.membership (chat_id, user_id, start_source, joined_hint, known_from, last_confirmed_at,
                           end_source, end_reason, left_after, left_before)
SELECT s.chat_id, s.user_id, 'snapshot', s.joined_date, s.first_seen_at, s.last_seen_at,
       'probe', 'gone', s.last_seen_at, p.answered_at
FROM tg.sighting s JOIN tg.probe p USING (chat_id, user_id)
WHERE (s.chat_id, s.user_id) = (-1002000000010, 2010);

-- join requests, posts, activity, count samples ----------------------------------------------------------
INSERT INTO tg.join_request (chat_id, user_id, requested_at, update_id, outcome, resolved_at, resolved_by) VALUES
  (:A, 1010, '2026-10-10 09:00+00', 600001, 'approved', '2026-10-10 10:00+00', 9002),
  (:A, 1033, '2026-11-01 08:00+00', 600002, 'unknown', NULL, NULL),
  (:A, 1032, '2026-11-20 08:00+00', 600003, NULL, NULL, NULL);
INSERT INTO tg.post (chat_id, message_id, posted_at, kind) VALUES
  (:D, 10, '2026-11-10 12:00+00', 'text'), (:D, 11, '2026-11-12 12:00+00', 'photo');
INSERT INTO tg.activity_daily (chat_id, user_id, day, messages) VALUES
  (:B, 1001, '2026-11-20', 5), (:B, 1002, '2026-11-01', 2), (:B, 1003, '2026-11-22', 1), (:B, 1011, '2026-09-01', 3),
  (:B, 1022, '2026-10-20', 1), (:C, 1013, '2026-11-15', 2), (:C, 1024, '2026-11-23', 3);
INSERT INTO tg.unattributed_daily (chat_id, day, sender_kind, messages) VALUES (:B, '2026-11-20', 'automatic_forward', 4);
INSERT INTO tg.count_sample (chat_id, sampled_at, reported, known_open, suspect_open, net_events) VALUES
  (:A, '2026-11-21 23:50+00', 14, 12, 0, 0), (:A, '2026-11-22 23:50+00', 14, 12, 0, 0),
  (:A, '2026-11-23 10:00+00', 14, 12, 0, 0), (:A, '2026-11-23 10:10+00', 14, 12, 0, 0),
  (:A, '2026-11-23 10:20+00', 15, 12, 0, 0), (:B, '2026-11-23 10:00+00', 6, 6, 0, 0);
COMMIT;

VACUUM ANALYZE;
