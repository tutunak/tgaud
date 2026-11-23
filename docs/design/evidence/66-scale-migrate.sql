-- Second step to the final schema.sql on the scale database (after 65): the non-human partial index and
-- user_chats defined with it. Run as the tenant.
\set ON_ERROR_STOP 1
\timing on
CREATE INDEX tg_user_not_human ON tg.tg_user (user_id) WHERE is_bot OR is_deleted;
DROP MATERIALIZED VIEW tg.user_chats;
CREATE MATERIALIZED VIEW tg.user_chats AS
SELECT m.user_id, array_agg(m.chat_id ORDER BY m.chat_id) AS chat_ids, count(*)::integer AS chat_count
FROM tg.membership m
JOIN tg.chat c ON c.chat_id = m.chat_id AND c.tracked
WHERE m.end_source IS NULL
  AND m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)
GROUP BY m.user_id
WITH NO DATA;
CREATE UNIQUE INDEX user_chats_user ON tg.user_chats (user_id);
CREATE INDEX user_chats_count ON tg.user_chats (chat_count);
-- 65 ran a second time by mistake and stopped after dropping v_chat_day: recreate it
CREATE OR REPLACE VIEW stats.v_chat_day AS
SELECT d.*, d.reported_eod - lag(d.reported_eod) OVER w AS reported_change,
       (d.reported_eod - lag(d.reported_eod) OVER w) - d.net AS residual,
       sum(d.members_started - d.members_ended) OVER w AS members_eod,
       sum(d.unsure_started - d.unsure_ended) OVER w AS unsure_eod
FROM stats.chat_day d
WINDOW w AS (PARTITION BY d.chat_id ORDER BY d.day ROWS UNBOUNDED PRECEDING);
SELECT tg.apply_grants();
ANALYZE tg.tg_user;
