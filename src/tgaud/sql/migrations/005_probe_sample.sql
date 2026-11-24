-- 005_probe_sample.sql (probe-sample task): drift samples are finalized only once ingest has caught up past them
-- (plan review: a sample taken before its updates commit) and withhold the residual around a count call that an exact
-- transition fell into; probe answers that a later event shows to straddle it are requeued; indexes for the prober's
-- queue and the pending join requests. Additive: it relaxes a NOT NULL and adds a column, a view column, indexes, a
-- function and a trigger. Runs as the tenant login; the runner owns the transaction.

------------------------------------------------------------------------------------------------------------
-- Pending drift samples
------------------------------------------------------------------------------------------------------------

ALTER TABLE tg.count_sample ALTER COLUMN net_events DROP NOT NULL;
COMMENT ON COLUMN tg.count_sample.net_events IS
  'Exact joins minus exact exits of spells (joined_at, and left_at of an event end, all accounts) in (previous '
  'sample, this sample]; 0 for a chat''s first sample. NULL = pending: the sampler fills it only once '
  'tg.bot_state.caught_up_at is later than sampled_at, so every update dated before the sample is committed; '
  'the residual (stats.v_drift, block 9b, the metrics) reads finalized samples only. Written by the sampler.';

CREATE INDEX count_sample_pending ON tg.count_sample (sampled_at) WHERE net_events IS NULL;
COMMENT ON INDEX tg.count_sample_pending IS 'the few samples still waiting for ingest to catch up (the sampler)';

ALTER TABLE tg.count_sample ADD COLUMN straddled boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN tg.count_sample.straddled IS
  'An exact transition of the chat (a spell''s joined_at, or the left_at of an event end) lies inside the '
  'getChatMemberCount call: from sampled_at (the send time, floored to the second like event dates) to sampled_at '
  '+ request_ms. The count may or may not include it, so the residuals on both sides of this sample are withheld '
  '(stats.v_drift.residual NULL, left out of tgaud_chat_drift_residual_24h). Set with net_events at finalization. '
  'Written by the sampler; tg.move_chat_history must copy it.';

CREATE OR REPLACE VIEW stats.v_drift AS
SELECT s.chat_id, s.sampled_at, s.reported, s.known_open, s.suspect_open, s.net_events,
       s.reported - s.known_open AS mismatch,
       CASE WHEN s.straddled OR lag(s.straddled) OVER w THEN NULL
            ELSE (s.reported - lag(s.reported) OVER w) - s.net_events END AS residual,
       s.straddled
FROM tg.count_sample s
WHERE s.net_events IS NOT NULL
WINDOW w AS (PARTITION BY s.chat_id ORDER BY s.sampled_at);
COMMENT ON VIEW stats.v_drift IS
  'Finalized count samples (net_events set once ingest caught up past them): absolute mismatch, and the delta '
  'residual against the previous finalized sample, NULL when either sample''s count call straddled an exact '
  'transition (D7, item 9).';

------------------------------------------------------------------------------------------------------------
-- Join requests without an outcome
------------------------------------------------------------------------------------------------------------

CREATE INDEX join_request_pending ON tg.join_request (requested_at) WHERE outcome IS NULL;
COMMENT ON INDEX tg.join_request_pending IS
  'requests without an outcome: the sampler marks them unknown after 7 days and counts the pending ones per chat';

------------------------------------------------------------------------------------------------------------
-- The prober's queue
------------------------------------------------------------------------------------------------------------

CREATE INDEX probe_pending_reason ON tg.probe (chat_id, reason, queued_at, probe_id) WHERE outcome = 'pending';
COMMENT ON INDEX tg.probe_pending_reason IS
  'the oldest ready probes of each chat and reason: the prober reads these heads before every batch of calls, so a '
  'new conflict or a new waiting chat is seen between its full (ordered) queue reads';

-- An answer committed before an event dated inside its call ([sent_at, answered_at], the rebuild ignores it): the
-- question is asked again. Ingest inserts the event under lock_chats(chat), so this runs under the same lock.
CREATE FUNCTION tg.probe_requeue_straddled() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO tg.probe (chat_id, user_id, reason, snapshot_id, absent_since, queued_at)
  SELECT p.chat_id, p.user_id, p.reason, p.snapshot_id, p.absent_since, p.queued_at
  FROM tg.probe p
  WHERE p.chat_id = NEW.chat_id AND p.user_id = NEW.user_id AND p.outcome IN ('in', 'out')
    AND coalesce(p.sent_at, p.answered_at) <= NEW.event_at AND NEW.event_at <= p.answered_at
  ORDER BY p.answered_at DESC, p.probe_id DESC
  LIMIT 1
  ON CONFLICT DO NOTHING;  -- probe_one_pending: a pending probe of the pair already asks the question
  RETURN NULL;
END $$;
COMMENT ON FUNCTION tg.probe_requeue_straddled() IS
  'AFTER INSERT on tg.member_event: when an in/out answer of the pair straddles the new event, queue the same '
  'question again (same reason, snapshot and queued_at, so a manual probe keeps the top priority) unless a probe of '
  'the pair is pending. The answered row stays as it was: history''s rule ignores it.';
CREATE TRIGGER probe_requeue_straddled AFTER INSERT ON tg.member_event
  FOR EACH ROW EXECUTE FUNCTION tg.probe_requeue_straddled();
