-- 010_prober.sql (prober-simplify task): what the prober's queue read needs beyond 001-008 (009 is the web's).
-- Additive only; runs as the tenant login, the runner owns the transaction.
--
-- The prober reads the next candidates of every chat from the database before each batch of calls (no cache), in
-- the queue order of design.md 6: manual, conflict, outage by the pair's open spell (newest first; a pair without
-- an open spell after those with one), snapshot_absent, sample; ties by queued_at, probe_id. Only probes whose
-- retry deadline has passed are taken. The two columns below make that order and that deadline part of the row, so
-- one index range per chat returns its heads in order.

------------------------------------------------------------------------------------------------------------
-- Retry deadline
------------------------------------------------------------------------------------------------------------

ALTER TABLE tg.probe ADD COLUMN next_attempt_at timestamptz;
COMMENT ON COLUMN tg.probe.next_attempt_at IS
  'A pending probe is not asked before this time; NULL = now. The prober sets it with every failure (1 min, 10 min '
  'and 1 h after the 1st, 2nd and 3rd: tgaud.jobs.prober.BACKOFF) and to the next second after an answer that '
  'straddled an event of the pair (not an attempt). A probe queued again or raised in place keeps it.';

-- the deadlines of probes that failed before this migration (the backoff of their attempts, as the prober had it)
UPDATE tg.probe
SET next_attempt_at = last_attempt_at + make_interval(secs => (ARRAY[60, 600, 3600])[least(attempts, 3)])
WHERE outcome = 'pending' AND attempts > 0 AND last_attempt_at IS NOT NULL;

CREATE INDEX probe_retry_due ON tg.probe (next_attempt_at) WHERE outcome = 'pending' AND next_attempt_at IS NOT NULL;
COMMENT ON INDEX tg.probe_retry_due IS
  'the earliest retry deadline: when the prober has nothing ready, it reads the queue again then';

------------------------------------------------------------------------------------------------------------
-- An outage probe's place: its pair's open spell
------------------------------------------------------------------------------------------------------------
-- Kept in the same transaction as the change: a probe takes it when it is queued, raised to outage or re-keyed (a
-- row trigger on tg.probe), and every statement that writes spells (rebuilds delete and insert them, M1 re-keys
-- them) sets it again for the pending outage probes of the pairs it touched. Both writers hold lock_chats(chat)
-- (queue_probes runs in the reconciler's and ingest's locked transactions), so neither misses the other's commit.

ALTER TABLE tg.probe ADD COLUMN spell_from timestamptz;
COMMENT ON COLUMN tg.probe.spell_from IS
  'A pending outage probe: known_from of its pair''s open spell (NULL when the pair has none); NULL for every other '
  'probe. The prober''s order among outage probes (newest first, NULL last). Kept by triggers on tg.probe and '
  'tg.membership.';

CREATE FUNCTION tg.open_spell_from(p_chat_id bigint, p_user_id bigint) RETURNS timestamptz LANGUAGE sql STABLE
RETURN (SELECT m.known_from FROM tg.membership m
        WHERE m.chat_id = p_chat_id AND m.user_id = p_user_id AND m.end_source IS NULL);
COMMENT ON FUNCTION tg.open_spell_from(bigint, bigint) IS
  'known_from of the open spell of (chat, user), NULL when there is none (membership_one_open).';

CREATE FUNCTION tg.probe_spell_from() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.spell_from := CASE WHEN NEW.outcome = 'pending' AND NEW.reason = 'outage'
                         THEN tg.open_spell_from(NEW.chat_id, NEW.user_id) END;
  RETURN NEW;
END $$;
COMMENT ON FUNCTION tg.probe_spell_from() IS
  'BEFORE INSERT or UPDATE of the pair, reason or outcome on tg.probe: spell_from for the row as it will be.';
CREATE TRIGGER probe_spell_from BEFORE INSERT OR UPDATE OF chat_id, user_id, reason, outcome ON tg.probe
  FOR EACH ROW EXECUTE FUNCTION tg.probe_spell_from();

UPDATE tg.probe SET spell_from = tg.open_spell_from(chat_id, user_id) WHERE outcome = 'pending' AND reason = 'outage';

CREATE FUNCTION tg.membership_probe_spell_from() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  -- from the pairs touched (probe_one_pending per pair), never a scan of every pending outage probe
  UPDATE tg.probe p SET spell_from = x.known_from
  FROM (SELECT q.probe_id, m.known_from
        FROM (SELECT DISTINCT s.chat_id, s.user_id FROM changed_spells s) k
        JOIN tg.probe q ON q.chat_id = k.chat_id AND q.user_id = k.user_id AND q.outcome = 'pending'
        LEFT JOIN tg.membership m ON m.chat_id = q.chat_id AND m.user_id = q.user_id AND m.end_source IS NULL
        WHERE q.reason = 'outage') x
  WHERE p.probe_id = x.probe_id AND p.spell_from IS DISTINCT FROM x.known_from;
  RETURN NULL;
END $$;
COMMENT ON FUNCTION tg.membership_probe_spell_from() IS
  'After each statement that inserts, updates or deletes spells: the pending outage probes of the pairs it touched '
  '(transition table changed_spells) take their pair''s open spell again.';
CREATE TRIGGER membership_probe_spell_from_ins AFTER INSERT ON tg.membership
  REFERENCING NEW TABLE AS changed_spells FOR EACH STATEMENT EXECUTE FUNCTION tg.membership_probe_spell_from();
CREATE TRIGGER membership_probe_spell_from_upd AFTER UPDATE ON tg.membership
  REFERENCING NEW TABLE AS changed_spells FOR EACH STATEMENT EXECUTE FUNCTION tg.membership_probe_spell_from();
CREATE TRIGGER membership_probe_spell_from_del AFTER DELETE ON tg.membership
  REFERENCING OLD TABLE AS changed_spells FOR EACH STATEMENT EXECUTE FUNCTION tg.membership_probe_spell_from();

------------------------------------------------------------------------------------------------------------
-- The queue read
------------------------------------------------------------------------------------------------------------

CREATE INDEX probe_ready ON tg.probe (chat_id, tg.probe_rank(reason), spell_from DESC NULLS LAST, queued_at, probe_id)
  INCLUDE (user_id, reason, next_attempt_at) WHERE outcome = 'pending';
COMMENT ON INDEX tg.probe_ready IS
  'each chat''s pending probes in the prober''s queue order: its read takes the first ready ones of every chat from '
  'one range each (tgaud.jobs.prober._HEADS)';
