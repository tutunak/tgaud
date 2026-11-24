-- 006_rollups.sql (rollups task): what the chat_day rollup (queries.sql R1) stores beyond 001.
-- Additive for the table (new columns); stats.v_chat_day is recreated because its column list grows
-- (d.* is expanded when a view is created). Runs as the tenant login; the runner owns the transaction.

------------------------------------------------------------------------------------------------------------
-- Day-boundary residual: the count sample and the events must describe the same interval
------------------------------------------------------------------------------------------------------------
-- 001's residual was (change of the day-end count) - (calendar-day net). A join at 23:58 after the day's last
-- sample (23:50) is in that day's net but not in its count, so it made a false residual on that day and the
-- opposite one on the next. R1 now stores the net over the interval between the two day-end samples.

ALTER TABLE stats.chat_day ADD COLUMN sample_at timestamptz;
COMMENT ON COLUMN stats.chat_day.sample_at IS
  'time of the last count sample of the day taken at or before the run''s as_of (reported_eod and known_eod are '
  'its values); NULL when the day has no sample';

ALTER TABLE stats.chat_day ADD COLUMN sample_net integer;
COMMENT ON COLUMN stats.chat_day.sample_net IS
  'exact joins minus exact exits by event time in (previous day''s sample_at, sample_at]: the events the two '
  'samples can tell apart. NULL unless both days have a finalized sample, and when either sample''s count call '
  'straddled an exact transition (count_sample.straddled, as stats.v_drift). The residual is reported_change - '
  'sample_net; net stays the calendar-day movement.';

------------------------------------------------------------------------------------------------------------
-- Members at the end of a day, exact to 16a at the next midnight
------------------------------------------------------------------------------------------------------------
-- The members_* and unsure_* deltas are keyed by the calendar day of their instant (so a dirty marker on
-- day D recomputes exactly the rows it can change). 16a at D+1 00:00 also counts what starts, ends or turns
-- unsure exactly at that midnight, which belongs to day D+1's deltas: these two columns hold that part
-- (net of started and ended) and the view adds the next day's to the running sum.

ALTER TABLE stats.chat_day ADD COLUMN members_midnight integer NOT NULL DEFAULT 0;
COMMENT ON COLUMN stats.chat_day.members_midnight IS
  'members_started - members_ended of the instants exactly at 00:00 UTC of this day; v_chat_day counts them '
  'into the end of the day before';

ALTER TABLE stats.chat_day ADD COLUMN unsure_midnight integer NOT NULL DEFAULT 0;
COMMENT ON COLUMN stats.chat_day.unsure_midnight IS
  'unsure_started - unsure_ended of the instants exactly at 00:00 UTC of this day (see members_midnight)';

ALTER TABLE stats.chat_day ALTER COLUMN members_midnight DROP DEFAULT;
ALTER TABLE stats.chat_day ALTER COLUMN unsure_midnight DROP DEFAULT;

COMMENT ON COLUMN stats.chat_day.unsure_started IS
  'unsure human stays that begin on this day: bounded exits whose left_after falls on it, and open stays whose '
  'suspect_since falls on it';

------------------------------------------------------------------------------------------------------------
-- A daily row belongs to an existing chat
------------------------------------------------------------------------------------------------------------
-- tg.move_chat_history deletes the old chat and its daily rows. R1 captures its targets first and writes later,
-- so without this it could write rows under the deleted id after the move committed, and nothing would clear
-- them. With the key, the write fails (the job retries) or, when the move comes second, the delete cascades.

DELETE FROM stats.chat_day d WHERE NOT EXISTS (SELECT 1 FROM tg.chat c WHERE c.chat_id = d.chat_id);
ALTER TABLE stats.chat_day ADD CONSTRAINT chat_day_chat_fkey
  FOREIGN KEY (chat_id) REFERENCES tg.chat ON UPDATE CASCADE ON DELETE CASCADE;

DROP VIEW stats.v_chat_day;
CREATE VIEW stats.v_chat_day AS
SELECT d.*, d.reported_eod - lag(d.reported_eod) OVER w AS reported_change,
       (d.reported_eod - lag(d.reported_eod) OVER w) - d.sample_net AS residual,
       sum(d.members_started - d.members_ended) OVER w + coalesce(lead(d.members_midnight) OVER w, 0) AS members_eod,
       sum(d.unsure_started - d.unsure_ended) OVER w + coalesce(lead(d.unsure_midnight) OVER w, 0) AS unsure_eod
FROM stats.chat_day d
WINDOW w AS (PARTITION BY d.chat_id ORDER BY d.day ROWS UNBOUNDED PRECEDING);
COMMENT ON VIEW stats.v_chat_day IS
  'Daily growth with the residual against Telegram''s count (item 9: the change of the day-end count minus the '
  'net of the events between the two day-end samples), and human members at the end of each day: members_eod '
  'stays held, of which unsure_eod between the last sighting and a confirmed exit or an unresolved suspicion '
  '(item 16d, the same numbers as 16a at the next midnight).';

SELECT tg.apply_grants();
