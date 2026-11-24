-- 005_probe_sample.sql (probe-sample task): drift samples are finalized only once ingest has caught up past them
-- (plan review: a sample taken before its updates commit), and the index of pending join requests. Additive: it only
-- relaxes a NOT NULL and adds indexes (also one for the prober's manual probes).
-- Runs as the tenant login; the runner owns the transaction.

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

------------------------------------------------------------------------------------------------------------
-- Join requests without an outcome
------------------------------------------------------------------------------------------------------------

CREATE INDEX join_request_pending ON tg.join_request (requested_at) WHERE outcome IS NULL;
COMMENT ON INDEX tg.join_request_pending IS
  'requests without an outcome: the sampler marks them unknown after 7 days and counts the pending ones per chat';

------------------------------------------------------------------------------------------------------------
-- Manual probes, read by the prober every 10 s between its full queue reads
------------------------------------------------------------------------------------------------------------

CREATE INDEX probe_pending_manual ON tg.probe (queued_at) WHERE outcome = 'pending' AND reason = 'manual';
COMMENT ON INDEX tg.probe_pending_manual IS
  'pending manual probes (tgaud probe CHAT USER): the prober looks for new ones every 10 s without ordering the queue';
