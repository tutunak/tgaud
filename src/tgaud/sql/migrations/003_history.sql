-- 003_history.sql (history task): what tgaud.history.rebuild_pairs reads beyond 001/002: the exact page time of
-- every sighting point of a run, and the send time of a probe answer (design.md 4, plan reviews 2 and 3).
-- Runs as the tenant login; the runner owns the transaction.

------------------------------------------------------------------------------------------------------------
-- Sighting runs keep every point exactly
------------------------------------------------------------------------------------------------------------

ALTER TABLE tg.sighting ADD COLUMN interior_seen_at timestamptz[] NOT NULL DEFAULT '{}';
ALTER TABLE tg.sighting ADD CONSTRAINT sighting_points CHECK (coalesce(
  CASE WHEN cardinality(interior_seen_at) = 0 THEN true
       WHEN array_ndims(interior_seen_at) <> 1 OR array_lower(interior_seen_at, 1) <> 1 THEN false
       ELSE array_position(interior_seen_at, NULL) IS NULL END
  AND ((times_seen = 1 AND first_seen_at = last_seen_at AND cardinality(interior_seen_at) = 0)
       OR (times_seen >= 2 AND cardinality(interior_seen_at) = times_seen - 2)), false));
COMMENT ON COLUMN tg.sighting.interior_seen_at IS
  'The page times (snapshot_member.seen_at, exact) of the run''s points between the first and the last, in run '
  'order. The points of a run are first_seen_at, these, then last_seen_at; a one-point run has first_seen_at = '
  'last_seen_at and is one point. Extending a run by a point t: with times_seen = 1 only last_seen_at := t; '
  'otherwise interior_seen_at := interior_seen_at || last_seen_at, then last_seen_at := t; always times_seen + 1. '
  'No stored time is ever rewritten, so extension and pruning never change a replay.';
COMMENT ON CONSTRAINT sighting_points ON tg.sighting IS
  'interior_seen_at is empty for one- and two-point runs and holds times_seen - 2 times otherwise; a nonempty '
  'array is one-dimensional, starts at 1 and has no NULL element; a one-point run has one time.';
COMMENT ON COLUMN tg.sighting.joined_date IS
  'The Telegram join date (participant date) of every point of the run: the joined_hint of a spell that a point '
  'opens or fills. A listing with another date (IS DISTINCT FROM, also to or from NULL) starts a new run.';
COMMENT ON TABLE tg.sighting IS
  'Durable snapshot evidence, the only snapshot input of the spell rebuild (with tg.snapshot): one row per run '
  'of a user in a chat, identified by its first snapshot. A run is consecutive applied snapshots of the chat '
  '(in started_at, snapshot_id order) that each listed the user with the same joined_date; the reconciler starts '
  'a new run when a snapshot did not list the user, when the joined_date changes, or when the page time is '
  'earlier than the run''s last_seen_at; it may also break a run at exit evidence. Every good snapshot whose key '
  'lies between a run''s first and last snapshot counts as listing the user (no absence). A point is written in '
  'the transaction that applies its snapshot to the user (with tg.snapshot.applied_through_user_id). Survives '
  'the pruning of snapshot_member. Written by the reconciler.';

------------------------------------------------------------------------------------------------------------
-- Probe answers that straddle a transition
------------------------------------------------------------------------------------------------------------

ALTER TABLE tg.probe ADD COLUMN sent_at timestamptz;
ALTER TABLE tg.probe ADD CONSTRAINT probe_sent_before_answer CHECK (sent_at <= answered_at);
COMMENT ON COLUMN tg.probe.sent_at IS
  'When the getChatMember call that gave the answer was sent; written by the prober with answered_at. The '
  'rebuild ignores an in/out answer when an event of the pair has sent_at <= event_at <= answered_at (exact '
  'times, inclusive): the answer may describe either side of that transition. NULL = answered_at.';
