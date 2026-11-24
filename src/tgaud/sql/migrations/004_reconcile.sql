-- 004_reconcile.sql (reconcile task): what the reconciler (tgaud.jobs.reconciler) and the contradiction record of
-- tgaud.history.rebuild_pairs need beyond 001-003: the source chat and the late/expiry/prune markers of a snapshot,
-- the time of the snapshot observation behind tg_user.is_deleted, one pending probe per pair at its highest
-- priority, and the E4/E10/S3 cases with the contradiction gaps they raise (design.md 4-6, design review 2).
-- Runs as the tenant login; the runner owns the transaction.

------------------------------------------------------------------------------------------------------------
-- Snapshots: source chat, late listings, expiry and pruning in steps
------------------------------------------------------------------------------------------------------------

ALTER TABLE tg.snapshot ADD COLUMN source_chat_id bigint;
COMMENT ON COLUMN tg.snapshot.source_chat_id IS
  'The chat id the snapshot was taken under, when a migration (M1, tg.move_chat_history) re-keyed it to another '
  'chat; NULL = taken under its current chat_id. Set by trigger snapshot_source_chat, never by a writer. Sighting '
  'runs are kept per source, coalesce(source_chat_id, chat_id), and never span a migration.';

CREATE FUNCTION tg.snapshot_source_chat() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.chat_id IS DISTINCT FROM OLD.chat_id AND NEW.source_chat_id IS NULL THEN
    NEW.source_chat_id := OLD.chat_id;
  END IF;
  RETURN NEW;
END $$;
COMMENT ON FUNCTION tg.snapshot_source_chat() IS
  'BEFORE UPDATE OF chat_id on tg.snapshot: keep the first chat id a moved snapshot was taken under (chained '
  'moves keep the first source).';
CREATE TRIGGER snapshot_source_chat BEFORE UPDATE OF chat_id ON tg.snapshot
  FOR EACH ROW EXECUTE FUNCTION tg.snapshot_source_chat();

ALTER TABLE tg.snapshot ADD COLUMN late boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN tg.snapshot.late IS
  'Set by the reconciler when it grades the snapshot: its source already had a reconciled snapshot with a later '
  '(started_at, snapshot_id). A late listing is positive evidence only: every listed user gets a one-point run of '
  'its own, it gives no absences, and the rebuild leaves it out of the numbering of other runs, so the points of '
  'runs written before it keep their snapshots.';

ALTER TABLE tg.snapshot ADD COLUMN expired_at timestamptz,
  ADD CONSTRAINT snapshot_expired_unreconciled CHECK (expired_at IS NULL OR reconciled_at IS NULL);
COMMENT ON COLUMN tg.snapshot.expired_at IS
  'When the reconciler began deleting a snapshot left unfinished for 2 days (design.md 5). From then on it is '
  'never graded, even if the script finishes it: its member rows go in batches, then the snapshot row.';

ALTER TABLE tg.snapshot ADD COLUMN prune_started_at timestamptz,
  ADD CONSTRAINT snapshot_prune_after_reconcile CHECK (prune_started_at IS NULL OR reconciled_at IS NOT NULL);
COMMENT ON COLUMN tg.snapshot.prune_started_at IS
  'When the reconciler began deleting the member rows of a snapshot outside the last 2 good ones of its chat '
  '(R3) in batches; members_pruned_at is set by the batch that deletes the last row.';

CREATE INDEX snapshot_source ON tg.snapshot ((coalesce(source_chat_id, chat_id)), started_at, snapshot_id);

ALTER TABLE tg.tg_user ADD COLUMN deleted_seen_at timestamptz;
COMMENT ON COLUMN tg.tg_user.deleted_seen_at IS
  'seen_at of the snapshot row that is_deleted comes from: the reconciler changes is_deleted only from a snapshot '
  'observation at least this recent, whatever order chats are applied in. Ingest never writes either column.';

CREATE INDEX reconcile_run_snapshot ON tg.reconcile_run (snapshot_id);

------------------------------------------------------------------------------------------------------------
-- One pending probe per pair, at its highest priority
------------------------------------------------------------------------------------------------------------

CREATE FUNCTION tg.probe_rank(p_reason text) RETURNS integer LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN CASE p_reason WHEN 'manual' THEN 0 WHEN 'conflict' THEN 1 WHEN 'outage' THEN 2
                     WHEN 'snapshot_absent' THEN 3 ELSE 4 END;
COMMENT ON FUNCTION tg.probe_rank(text) IS
  'Probe priority (design.md 6), lower first: manual, conflict, outage, snapshot_absent, then sample.';

CREATE FUNCTION tg.queue_probes(p_chat_ids bigint[], p_user_ids bigint[], p_reason text,
                                p_snapshot_id bigint DEFAULT NULL, p_absent_since timestamptz DEFAULT NULL)
RETURNS integer LANGUAGE sql AS $$
  WITH pairs AS (SELECT DISTINCT c, u FROM unnest(p_chat_ids, p_user_ids) AS x(c, u)),
  queued AS (
    INSERT INTO tg.probe AS p (chat_id, user_id, reason, snapshot_id, absent_since)
    SELECT c, u, p_reason, p_snapshot_id, p_absent_since FROM pairs ORDER BY c, u
    ON CONFLICT (chat_id, user_id) WHERE outcome = 'pending' DO UPDATE
      SET reason = excluded.reason, snapshot_id = excluded.snapshot_id, absent_since = excluded.absent_since
      WHERE tg.probe_rank(p.reason) > tg.probe_rank(excluded.reason)
    RETURNING 1)
  SELECT count(*)::integer FROM queued
$$;
COMMENT ON FUNCTION tg.queue_probes(bigint[], bigint[], text, bigint, timestamptz) IS
  'Queue a probe for each (chat, user) with this reason; a pair that already has a pending probe of a lower '
  'priority gets this reason and its fields (the same row: attempts and queued_at stay), one of a higher or equal '
  'priority is left as it is. Returns the rows inserted or raised. snapshot_absent needs p_snapshot_id and '
  'p_absent_since (tg.probe''s CHECK), the other reasons neither.';

------------------------------------------------------------------------------------------------------------
-- Contradictions: E4/E10/S3 cases recorded by every rebuild, and the gap they raise
------------------------------------------------------------------------------------------------------------

CREATE TABLE tg.contradiction_case (
  chat_id              bigint NOT NULL,                    -- no FK: a migration moves the rows (trigger below)
  user_id              bigint NOT NULL REFERENCES tg.tg_user ON DELETE CASCADE,
  rule                 text NOT NULL CHECK (rule IN ('E4', 'E10', 'S3')),
  at                   timestamptz NOT NULL,               -- evidence time: the event, or the sighting's page time
  source_id            bigint NOT NULL,                    -- event_id (E4, E10) or snapshot_id (S3)
  recorded_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (chat_id, user_id, rule, at, source_id)
);
CREATE INDEX contradiction_case_time ON tg.contradiction_case (chat_id, at);
COMMENT ON TABLE tg.contradiction_case IS
  'Every E4 (missed leave), E10 (out-to-out while in) and S3 (stale listing) a spell rebuild produced, once per '
  'source evidence: tgaud.history.rebuild_pairs records them through tg.record_contradictions in the rebuild''s '
  'transaction, whoever calls it, so a re-run never counts one twice. More than 10 cases of a chat within any 60 '
  'minutes of evidence time raise a contradiction gap (design.md 6). Kept with the history; a migration re-keys '
  'the rows to the destination chat. Personal data (user ids): forget-user deletes them with the user.';

CREATE FUNCTION tg.contradiction_check(p_chat_id bigint, p_times timestamptz[]) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  v_lo timestamptz;
  v_hi timestamptz;
  r record;
  v_span tstzrange;
  v_keep bigint;
  v_old tstzrange;
  v_union tstzrange;
  v_other bigint;
  v_absorbed boolean;
  v_changed integer := 0;
BEGIN
  SELECT min(t) - interval '60 minutes', max(t) + interval '60 minutes' INTO v_lo, v_hi FROM unnest(p_times) t;
  IF v_lo IS NULL OR NOT EXISTS (SELECT 1 FROM tg.chat WHERE chat_id = p_chat_id) THEN
    RETURN 0;
  END IF;
  FOR r IN
    -- more than 10 cases within 60 minutes <=> some case and the 10th case after it (in time order) are at most
    -- 60 minutes apart: one ordered pass over the index, no aggregate over each window
    WITH c AS (
      SELECT k.at FROM tg.contradiction_case k WHERE k.chat_id = p_chat_id AND k.at BETWEEN v_lo AND v_hi
    ), q AS (
      SELECT at AS lo, lead(at, 10) OVER (ORDER BY at) AS hi FROM c
    ), w AS (
      SELECT lo, hi FROM q WHERE hi - lo <= interval '60 minutes'
    ), islands AS (
      SELECT lo, hi, sum(brk) OVER (ORDER BY lo, hi) AS grp FROM (
        SELECT lo, hi, CASE WHEN lo <= max(hi) OVER (ORDER BY lo, hi ROWS BETWEEN UNBOUNDED PRECEDING
                                                                     AND 1 PRECEDING) THEN 0 ELSE 1 END AS brk
        FROM w) y
    )
    SELECT min(lo) AS lo, max(hi) AS hi FROM islands GROUP BY grp ORDER BY 1
  LOOP
    v_span := tstzrange(r.lo, r.hi, '[]');
    SELECT min(gap_id) INTO v_keep FROM tg.gap
     WHERE chat_id = p_chat_id AND reason = 'contradiction' AND period && v_span;
    IF v_keep IS NULL THEN
      INSERT INTO tg.gap (chat_id, period, reason, confirmed, note)
      VALUES (p_chat_id, v_span, 'contradiction', true,
              format('%s E4/E10/S3 cases, more than 10 within 60 minutes',
                     (SELECT count(*) FROM tg.contradiction_case
                       WHERE chat_id = p_chat_id AND at BETWEEN r.lo AND r.hi)));
      v_changed := v_changed + 1;
      CONTINUE;
    END IF;
    -- one gap per episode: the oldest overlapping gap takes the union of the span and of every contradiction gap
    -- that overlaps it, directly or through another; the others go (with their tg.gap_progress)
    SELECT period INTO v_old FROM tg.gap WHERE gap_id = v_keep FOR UPDATE;
    v_union := v_old + v_span;
    v_absorbed := false;
    LOOP
      SELECT gap_id INTO v_other FROM tg.gap
       WHERE chat_id = p_chat_id AND reason = 'contradiction' AND gap_id <> v_keep AND period && v_union
       ORDER BY gap_id LIMIT 1 FOR UPDATE;
      EXIT WHEN NOT FOUND;
      v_union := v_union + (SELECT period FROM tg.gap WHERE gap_id = v_other);
      DELETE FROM tg.gap WHERE gap_id = v_other;
      v_absorbed := true;
    END LOOP;
    IF v_union <> v_old THEN
      UPDATE tg.gap SET period = v_union WHERE gap_id = v_keep;
      v_changed := v_changed + 1;
    END IF;
    IF v_absorbed OR lower(v_union) < lower(v_old) THEN
      -- a start moved earlier or a start vanished: G1 again for the whole gap, as ingest does for a moved start;
      -- its chunks also rebuild the spells doubted since a later start (tgaud.jobs.reconciler.g1_chunk)
      DELETE FROM tg.gap_progress WHERE gap_id = v_keep;
    END IF;
  END LOOP;
  RETURN v_changed;
END $$;
COMMENT ON FUNCTION tg.contradiction_check(bigint, timestamptz[]) IS
  'For a chat whose cases changed near these evidence times: every span in which more than 10 cases lie within 60 '
  'minutes (inclusive), merged where they overlap, becomes a contradiction gap from its first to its last case. A '
  'span that overlaps contradiction gaps merges them all into the oldest one; when that removes a gap or moves the '
  'start earlier, the kept gap''s tg.gap_progress is deleted so G1 handles it again. Returns the gaps changed.';

CREATE FUNCTION tg.record_contradictions(p_chat_ids bigint[], p_user_ids bigint[], p_rules text[],
                                         p_ats timestamptz[], p_source_ids bigint[]) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  v_chats bigint[];
  v_users bigint[];
  v_rules text[];
  v_ats timestamptz[];
  v_chat bigint;
  v_probes integer;
BEGIN
  WITH added AS (
    INSERT INTO tg.contradiction_case (chat_id, user_id, rule, at, source_id)
    SELECT DISTINCT * FROM unnest(p_chat_ids, p_user_ids, p_rules, p_ats, p_source_ids)
    ON CONFLICT DO NOTHING
    RETURNING chat_id, user_id, rule, at)
  SELECT array_agg(chat_id), array_agg(user_id), array_agg(rule), array_agg(at)
    INTO v_chats, v_users, v_rules, v_ats FROM added;
  IF v_chats IS NULL THEN
    RETURN 0;
  END IF;
  -- design.md 4 S3: a stale listing within the grace period after an exit queues a conflict probe
  v_probes := tg.queue_probes(ARRAY(SELECT c FROM unnest(v_chats, v_rules) x(c, r) WHERE r = 'S3'),
                              ARRAY(SELECT u FROM unnest(v_users, v_rules) x(u, r) WHERE r = 'S3'), 'conflict');
  FOR v_chat IN SELECT DISTINCT c FROM unnest(v_chats) c ORDER BY 1 LOOP
    PERFORM tg.contradiction_check(v_chat, ARRAY(SELECT t FROM unnest(v_chats, v_ats) x(c, t) WHERE c = v_chat));
  END LOOP;
  RETURN v_probes;
END $$;
COMMENT ON FUNCTION tg.record_contradictions(bigint[], bigint[], text[], timestamptz[], bigint[]) IS
  'Called by tgaud.history.rebuild_pairs with the E4/E10/S3 cases its replay produced: records the new ones, '
  'queues (or raises to) a conflict probe for each new S3, and checks the contradiction windows of their chats. '
  'Returns the conflict probes queued or raised.';

CREATE FUNCTION tg.contradiction_case_follow_alias() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  v_ats timestamptz[];
BEGIN
  -- M2 (the new chat is not tracked) keeps the history under the old id: its old chat row stays and moved_at is
  -- NULL. tg.move_chat_history (M1) deletes the old chat row; ingest's version also sets moved_at.
  IF NEW.moved_at IS NULL AND EXISTS (SELECT 1 FROM tg.chat WHERE chat_id = NEW.old_chat_id) THEN
    RETURN NULL;
  END IF;
  WITH moved AS (
    DELETE FROM tg.contradiction_case WHERE chat_id = NEW.old_chat_id
    RETURNING user_id, rule, at, source_id, recorded_at
  ), copied AS (
    INSERT INTO tg.contradiction_case (chat_id, user_id, rule, at, source_id, recorded_at)
    SELECT NEW.chat_id, user_id, rule, at, source_id, recorded_at FROM moved
    ON CONFLICT DO NOTHING
  )
  SELECT array_agg(at) INTO v_ats FROM moved;
  IF v_ats IS NOT NULL THEN
    PERFORM tg.contradiction_check(NEW.chat_id, v_ats);
  END IF;
  RETURN NULL;
END $$;
COMMENT ON FUNCTION tg.contradiction_case_follow_alias() IS
  'AFTER INSERT OR UPDATE on tg.chat_alias: once the history of old_chat_id has moved (M1), move its contradiction '
  'cases to chat_id and check the merged windows there.';
CREATE TRIGGER chat_alias_moves_contradictions AFTER INSERT OR UPDATE ON tg.chat_alias
  FOR EACH ROW EXECUTE FUNCTION tg.contradiction_case_follow_alias();
