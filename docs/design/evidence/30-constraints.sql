-- Constraint and permission checks on the fixture database. Run as the tenant. Every rejection test raises
-- 'ACCEPTED' (and stops the script) if the database takes the invalid row; every acceptance test rolls back.
\set ON_ERROR_STOP 1
\set H01 -1002000000001

-- accepted: join, leave, join of one user in one second are three events (update identity, not status pair)
DO $$ DECLARE n int; BEGIN
  INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status) VALUES
    (-1002000000001, 2002, '2026-11-01 10:00:00+00', 900001, 'left', 'member'),
    (-1002000000001, 2002, '2026-11-01 10:00:00+00', 900002, 'member', 'left'),
    (-1002000000001, 2002, '2026-11-01 10:00:00+00', 900003, 'left', 'member');
  SELECT count(*) INTO n FROM tg.member_event WHERE update_id BETWEEN 900001 AND 900003;
  -- a re-delivered update is a no-op
  INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status)
  VALUES (-1002000000001, 2002, '2026-11-01 10:00:00+00', 900002, 'member', 'left') ON CONFLICT DO NOTHING;
  RAISE NOTICE 'accepted: % events for join/leave/join in one second; re-delivery added % rows', n,
    (SELECT count(*) FROM tg.member_event WHERE update_id BETWEEN 900001 AND 900003) - n;
  RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'rollback';
EXCEPTION WHEN no_data_found THEN NULL;
END $$;

-- rejected: two open spells for one (chat, user)
DO $$ BEGIN
  INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at)
  VALUES (-1002000000001, 2001, 'snapshot', '2026-11-01+00', '2026-11-01+00');
  RAISE EXCEPTION 'ACCEPTED: two open spells';
EXCEPTION WHEN unique_violation OR exclusion_violation THEN RAISE NOTICE 'rejected: two open spells for one (chat, user): %', SQLERRM;
END $$;

-- rejected: an exact join time without an event source, four ways
DO $$ BEGIN
  INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, known_from, last_confirmed_at)
  VALUES (-1002000000002, 2002, 'snapshot', '2026-10-01+00', '2026-10-01+00', '2026-10-01+00');
  RAISE EXCEPTION 'ACCEPTED: exact join with start_source snapshot';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: exact join time with start_source=snapshot: %', SQLERRM;
END $$;
DO $$ BEGIN
  INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, known_from, join_route, last_confirmed_at)
  VALUES (-1002000000002, 2002, 'event', '2026-10-01+00', '2026-10-01+00', 'unattributed', '2026-10-01+00');
  RAISE EXCEPTION 'ACCEPTED: exact join without join_event_id';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: exact join time without its event id: %', SQLERRM;
END $$;
DO $$ BEGIN
  INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, join_event_id, known_from, join_route, last_confirmed_at)
  VALUES (-1002000000002, 2002, 'event', '2026-10-01+00', 999999, '2026-10-01+00', 'unattributed', '2026-10-01+00');
  RAISE EXCEPTION 'ACCEPTED: join_event_id of a missing event';
EXCEPTION WHEN foreign_key_violation THEN RAISE NOTICE 'rejected: join event that does not exist: %', SQLERRM;
END $$;
DO $$ BEGIN   -- event 1201 exists but happened at 2026-09-01 10:00, not at the claimed time
  INSERT INTO tg.membership (chat_id, user_id, start_source, joined_at, join_event_id, known_from, join_route, last_confirmed_at)
  VALUES (-1002000000002, 2002, 'event', '2026-10-01+00', 1201, '2026-10-01+00', 'unattributed', '2026-10-01+00');
  RAISE EXCEPTION 'ACCEPTED: join time different from its event';
EXCEPTION WHEN foreign_key_violation THEN RAISE NOTICE 'rejected: join time that differs from its event''s time: %', SQLERRM;
END $$;

-- rejected: an end without a reason
DO $$ BEGIN
  INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at, end_source, left_after, left_before)
  VALUES (-1002000000002, 2002, 'snapshot', '2026-10-01+00', '2026-10-01+00', 'probe', '2026-10-01+00', '2026-10-02+00');
  RAISE EXCEPTION 'ACCEPTED: end without reason';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: an end without a reason: %', SQLERRM;
END $$;

-- rejected: a membership row for a chat that does not exist
DO $$ BEGIN
  INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at)
  VALUES (-1009999999999, 2001, 'snapshot', '2026-10-01+00', '2026-10-01+00');
  RAISE EXCEPTION 'ACCEPTED: unknown chat';
EXCEPTION WHEN foreign_key_violation THEN RAISE NOTICE 'rejected: membership for a chat that does not exist: %', SQLERRM;
END $$;

-- more rejections ------------------------------------------------------------------------------------------
DO $$ BEGIN   -- a closed spell overlapping an existing one (case 2: 2002 was in from 09-01 to 09-20)
  INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at, end_source, end_reason, left_after, left_before)
  VALUES (-1002000000002, 2002, 'snapshot', '2026-09-10+00', '2026-09-10+00', 'probe', 'gone', '2026-09-10+00', '2026-09-25+00');
  RAISE EXCEPTION 'ACCEPTED: overlapping spells';
EXCEPTION WHEN exclusion_violation THEN RAISE NOTICE 'rejected: two overlapping stays: %', SQLERRM;
END $$;
DO $$ BEGIN   -- an exact exit time with no exit event
  INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at, end_source, end_reason, left_at, left_after, left_before)
  VALUES (-1002000000002, 2002, 'snapshot', '2026-10-01+00', '2026-10-01+00', 'event', 'left', '2026-10-02+00', '2026-10-02+00', '2026-10-02+00');
  RAISE EXCEPTION 'ACCEPTED: exact exit without event';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: exact exit time without its event: %', SQLERRM;
END $$;
DO $$ BEGIN   -- bounds in the wrong order
  INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at, end_source, end_reason, left_after, left_before)
  VALUES (-1002000000002, 2002, 'snapshot', '2026-10-01+00', '2026-10-01+00', 'probe', 'gone', '2026-10-05+00', '2026-10-02+00');
  RAISE EXCEPTION 'ACCEPTED: left_after > left_before';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: exit bounds in the wrong order: %', SQLERRM;
END $$;
DO $$ BEGIN   -- a closed spell flagged suspect
  INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at, suspect_since, end_source, end_reason, left_after, left_before)
  VALUES (-1002000000002, 2002, 'snapshot', '2026-10-01+00', '2026-10-01+00', '2026-10-01+00', 'probe', 'gone', '2026-10-01+00', '2026-10-02+00');
  RAISE EXCEPTION 'ACCEPTED: suspect closed spell';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: suspect flag on a closed spell: %', SQLERRM;
END $$;
DO $$ BEGIN   -- migration end with a non-migration reason
  INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at, end_source, end_reason, left_after, left_before)
  VALUES (-1002000000002, 2002, 'snapshot', '2026-10-01+00', '2026-10-01+00', 'migration', 'left', '2026-10-01+00', '2026-10-02+00');
  RAISE EXCEPTION 'ACCEPTED: migration end with reason left';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: migration end without reason migrated: %', SQLERRM;
END $$;
DO $$ BEGIN   -- the same update stored twice
  INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status)
  VALUES (-1002000000001, 2001, '2026-09-01 10:00+00', 501101, 'left', 'member');
  RAISE EXCEPTION 'ACCEPTED: duplicate update identity';
EXCEPTION WHEN unique_violation THEN RAISE NOTICE 'rejected: the same update twice: %', SQLERRM;
END $$;
DO $$ BEGIN
  INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status)
  VALUES (-1002000000001, 2001, '2026-10-01+00', 900010, 'left', 'banned');
  RAISE EXCEPTION 'ACCEPTED: unknown status';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: unknown status: %', SQLERRM;
END $$;
DO $$ BEGIN
  INSERT INTO tg.member_event (chat_id, user_id, event_at, update_id, old_status, new_status)
  VALUES (-1002000000001, 2001, '2030-01-01+00', 900011, 'left', 'member');
  RAISE EXCEPTION 'ACCEPTED: event outside the partitions';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: event with no partition (service then calls ensure_partitions): %', SQLERRM;
END $$;
DO $$ BEGIN
  INSERT INTO tg.coverage (chat_id, period) VALUES (-1002000000001, tstzrange('2026-09-01+00', '2026-09-02+00'));
  RAISE EXCEPTION 'ACCEPTED: overlapping coverage';
EXCEPTION WHEN exclusion_violation THEN RAISE NOTICE 'rejected: overlapping coverage periods: %', SQLERRM;
END $$;
DO $$ BEGIN
  INSERT INTO tg.raw_update (update_id, update_date, update_type, chat_id, payload)
  VALUES (777, '2026-11-23 01:00+00', 'chat_join_request', -1001000000001, '{"bio": "free text", "date": 1}');
  RAISE EXCEPTION 'ACCEPTED: raw update with bio';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: raw join request that still has bio: %', SQLERRM;
END $$;
DO $$ BEGIN
  UPDATE tg.snapshot SET finished_at = NULL, fetched = NULL, complete = NULL WHERE snapshot_id = 1;
  RAISE EXCEPTION 'ACCEPTED: graded snapshot without finish';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: quality on an unfinished snapshot: %', SQLERRM;
END $$;
DO $$ BEGIN
  INSERT INTO tg.probe (chat_id, user_id, reason, outcome) VALUES (-1002000000001, 2001, 'manual', 'out');
  RAISE EXCEPTION 'ACCEPTED: probe out without answer';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: probe outcome without an answer time: %', SQLERRM;
END $$;
DO $$ BEGIN
  INSERT INTO tg.probe (chat_id, user_id, reason) VALUES (-1002000000001, 2001, 'manual'), (-1002000000001, 2001, 'manual');
  RAISE EXCEPTION 'ACCEPTED: two pending probes';
EXCEPTION WHEN unique_violation THEN RAISE NOTICE 'rejected: two pending probes for one (chat, user): %', SQLERRM;
END $$;
DO $$ BEGIN
  INSERT INTO tg.invite_link (chat_id, masked_link, link_digest) VALUES (-1001000000001, 'https://t.me/+x…', '\x01');
  RAISE EXCEPTION 'ACCEPTED: link with text and digest';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: a link stored both masked and as a digest: %', SQLERRM;
END $$;
DO $$ BEGIN
  INSERT INTO tg.campaign (name, kind, origin, starts_at, draw_at) VALUES ('x', 'giveaway', 'observation', '2026-01-01+00', '2026-01-02+00');
  RAISE EXCEPTION 'ACCEPTED: observed owner giveaway';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'rejected: an owner giveaway marked as a Telegram observation: %', SQLERRM;
END $$;

-- permissions --------------------------------------------------------------------------------------------
DO $$ BEGIN
  SET LOCAL ROLE bi_reader;
  PERFORM count(*) FROM tg.membership;
  RAISE EXCEPTION 'ACCEPTED: bi_reader read tg.membership';
EXCEPTION WHEN insufficient_privilege THEN RAISE NOTICE 'rejected: bi_reader reading tg.membership: %', SQLERRM;
END $$;
DO $$ BEGIN
  SET LOCAL ROLE bi_reader;
  PERFORM * FROM tg.campaign_entrant(1, 48, now());
  RAISE EXCEPTION 'ACCEPTED: bi_reader called tg.campaign_entrant';
EXCEPTION WHEN insufficient_privilege THEN RAISE NOTICE 'rejected: bi_reader calling the user-level campaign function: %', SQLERRM;
END $$;
DO $$ BEGIN
  SET LOCAL ROLE collect_writer;
  PERFORM count(*) FROM tg.membership;
  RAISE EXCEPTION 'ACCEPTED: collect_writer read tg.membership';
EXCEPTION WHEN insufficient_privilege THEN RAISE NOTICE 'rejected: collect_writer reading tg.membership: %', SQLERRM;
END $$;
DO $$ BEGIN
  SET LOCAL ROLE collect_writer;
  UPDATE tg.snapshot SET quality = 'good' WHERE snapshot_id = 15;
  RAISE EXCEPTION 'ACCEPTED: collect_writer graded a snapshot';
EXCEPTION WHEN insufficient_privilege THEN RAISE NOTICE 'rejected: collect_writer setting quality (the service grades): %', SQLERRM;
END $$;
-- accepted: bi_reader reads every stats view
DO $$ DECLARE r record; n bigint; BEGIN
  SET LOCAL ROLE bi_reader;
  FOR r IN SELECT table_name FROM information_schema.tables WHERE table_schema = 'stats' ORDER BY 1 LOOP
    EXECUTE format('SELECT count(*) FROM stats.%I', r.table_name) INTO n;
    RAISE NOTICE 'accepted: bi_reader reads stats.% (% rows)', r.table_name, n;
  END LOOP;
END $$;
