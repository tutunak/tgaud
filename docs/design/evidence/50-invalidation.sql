-- chat_day invalidation from the earliest affected day: a late correction for chat A dated 2026-11-04.
\set ON_ERROR_STOP 1
\set as_of '2026-11-23 12:00+00'
\set A -1001000000001
\pset footer off
SELECT day, corrections_in, refreshed_at FROM stats.chat_day WHERE chat_id = :A AND day BETWEEN '2026-11-03' AND '2026-11-05' ORDER BY day;
SELECT refreshed_at AS before_refresh_nov03 FROM stats.chat_day WHERE chat_id = :A AND day = '2026-11-03' \gset
-- the reconciler finds user 1032 in a late-imported snapshot seen 2026-11-04 12:00 and marks the chat dirty
INSERT INTO tg.membership (chat_id, user_id, start_source, known_from, last_confirmed_at)
VALUES (:A, 1032, 'snapshot', '2026-11-04 12:00+00', '2026-11-04 12:00+00');
INSERT INTO tg.rollup_dirty (chat_id, from_day) VALUES (:A, '2026-11-04')
ON CONFLICT (chat_id) DO UPDATE SET from_day = least(tg.rollup_dirty.from_day, excluded.from_day), marked_at = now();
SELECT pg_sleep(0.1);
\set only R1
\i /work/queries.sql
SELECT day, corrections_in, refreshed_at, refreshed_at > :'before_refresh_nov03'::timestamptz AS recomputed
FROM stats.chat_day WHERE chat_id = :A AND day BETWEEN '2026-11-03' AND '2026-11-05' ORDER BY day;
SELECT count(*) AS dirty_markers_left FROM tg.rollup_dirty;
SELECT chat_id, min(day) AS first_recomputed_day FROM stats.chat_day
WHERE refreshed_at > :'before_refresh_nov03'::timestamptz AND chat_id IN (:A, -1001000000002) GROUP BY chat_id ORDER BY chat_id;
-- undo, the same way
DELETE FROM tg.membership WHERE chat_id = :A AND user_id = 1032;
INSERT INTO tg.rollup_dirty (chat_id, from_day) VALUES (:A, '2026-11-04');
\i /work/queries.sql
SELECT day, corrections_in FROM stats.chat_day WHERE chat_id = :A AND day = '2026-11-04';
