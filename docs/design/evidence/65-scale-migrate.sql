-- Bring the scale database (loaded with the first schema.sql) to the final schema.sql: wider membership_found,
-- chat_day member deltas, tg.chat_set, v_chat_day with running sums. Run as the tenant.
\set ON_ERROR_STOP 1
\timing on
DROP INDEX tg.membership_found;
CREATE INDEX membership_found ON tg.membership (chat_id, known_from) WHERE start_source <> 'event';
DROP VIEW stats.v_chat_day;
TRUNCATE stats.chat_day;
ALTER TABLE stats.chat_day ADD COLUMN members_started integer NOT NULL, ADD COLUMN members_ended integer NOT NULL,
  ADD COLUMN unsure_started integer NOT NULL, ADD COLUMN unsure_ended integer NOT NULL;
CREATE VIEW stats.v_chat_day AS
SELECT d.*, d.reported_eod - lag(d.reported_eod) OVER w AS reported_change,
       (d.reported_eod - lag(d.reported_eod) OVER w) - d.net AS residual,
       sum(d.members_started - d.members_ended) OVER w AS members_eod,
       sum(d.unsure_started - d.unsure_ended) OVER w AS unsure_eod
FROM stats.chat_day d
WINDOW w AS (PARTITION BY d.chat_id ORDER BY d.day ROWS UNBOUNDED PRECEDING);
CREATE TABLE tg.chat_set (chat_ids bigint[] PRIMARY KEY, chat_count integer NOT NULL, users bigint NOT NULL, as_of timestamptz NOT NULL);
CREATE INDEX chat_set_count ON tg.chat_set (chat_count);
SELECT tg.apply_grants();
