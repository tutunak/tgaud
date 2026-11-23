-- 002_shared.sql (core task): every schema addition that more than one task uses, so each task's base has it.
-- Additive only (deploy contract): new columns, tables, functions and triggers; the one replaced CHECK and the
-- dropped NOT NULL only relax rules. Runs as the tenant login; the runner owns the transaction.

------------------------------------------------------------------------------------------------------------
-- Ingest state and the service fence
------------------------------------------------------------------------------------------------------------

ALTER TABLE tg.bot_state ADD COLUMN caught_up_at timestamptz;
COMMENT ON COLUMN tg.bot_state.caught_up_at IS
  'when a getUpdates last returned fewer updates than its limit (the queue was drained); written by ingest, '
  'read by the reconciler (design.md 5)';

ALTER TABLE tg.bot_state ADD COLUMN link_key_fingerprint text;
COMMENT ON COLUMN tg.bot_state.link_key_fingerprint IS
  'HMAC-SHA256 of a fixed label keyed with TGAUD_LINK_HMAC_KEY (tgaud.config.Config.link_key_fingerprint), '
  'never the key. Recorded by serve on its first start with the bot id (the row key); afterwards serve exits 2 '
  'when the configured key gives another fingerprint (deploy contract: identity guards). Written by ingest.';

ALTER TABLE tg.bot_state ADD COLUMN serve_generation bigint NOT NULL DEFAULT 0;
COMMENT ON COLUMN tg.bot_state.serve_generation IS
  'Fencing token of the running tgaud serve. Incremented (SELECT ... FOR UPDATE) by every serve start under the '
  'exclusive service advisory lock, and by maintenance commands. Every job transaction begins with '
  'SELECT serve_generation FROM tg.bot_state FOR KEY SHARE (it conflicts only with that FOR UPDATE, not with '
  'ingest''s offset updates) and makes the process exit when the value is not its own, so after a takeover an '
  'old process can commit nothing (tgaud.db).';

------------------------------------------------------------------------------------------------------------
-- Actor kind that survives erasure
------------------------------------------------------------------------------------------------------------

ALTER TABLE tg.member_event ADD COLUMN actor_is_other boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN tg.member_event.actor_is_other IS
  'true when `from` was someone other than the member (an admin, an inviter): kicked vs left (E5) and the '
  '"added" join route. Set from actor_id at insert by trigger member_event_actor_kind and never changed, so '
  'erasing an actor''s id (actor_id := NULL) cannot change anyone''s history. The rebuild and the join route '
  'read this column, never actor_id.';

CREATE FUNCTION tg.member_event_actor_kind() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT NEW.actor_is_other THEN
    NEW.actor_is_other := NEW.actor_id IS NOT NULL;
  END IF;
  RETURN NEW;
END $$;
COMMENT ON FUNCTION tg.member_event_actor_kind() IS
  'BEFORE INSERT on tg.member_event: actor_is_other := actor_id IS NOT NULL when the insert left it false.';
CREATE TRIGGER member_event_actor_kind BEFORE INSERT ON tg.member_event
  FOR EACH ROW EXECUTE FUNCTION tg.member_event_actor_kind();
UPDATE tg.member_event SET actor_is_other = true WHERE actor_id IS NOT NULL AND NOT actor_is_other;

------------------------------------------------------------------------------------------------------------
-- Migration aliases: known before the history is moved
------------------------------------------------------------------------------------------------------------

ALTER TABLE tg.chat_alias ALTER COLUMN moved_at DROP NOT NULL, ALTER COLUMN moved_at DROP DEFAULT;
COMMENT ON COLUMN tg.chat_alias.moved_at IS
  'when tg.move_chat_history moved the history to chat_id (M1). NULL = the migration is known but the history '
  'is not moved (M2: the new chat is not tracked yet); ingest routes the old id through the alias only once '
  'moved_at is set. No default: every writer sets it explicitly (the ingest task owns tg.move_chat_history, '
  'whose alias insert sets moved_at = now()).';

------------------------------------------------------------------------------------------------------------
-- Reconciliation in chunks
------------------------------------------------------------------------------------------------------------

ALTER TABLE tg.snapshot ADD COLUMN applied_through_user_id bigint;
COMMENT ON COLUMN tg.snapshot.applied_through_user_id IS
  'The reconciler''s cursor in a graded snapshot: its sightings and absences are applied in user_id order, '
  'one chunk per transaction; every user_id <= this value is applied (NULL = none yet). The history rebuild '
  'counts the snapshot''s absences only for users within it. Written by the reconciler.';

DO $$
DECLARE
  v_names text[];
BEGIN
  SELECT array_agg(conname) INTO v_names FROM pg_constraint
   WHERE conrelid = 'tg.snapshot'::regclass AND contype = 'c'
     AND pg_get_constraintdef(oid) = 'CHECK (((quality IS NULL) = (reconciled_at IS NULL)))';
  IF cardinality(v_names) IS DISTINCT FROM 1 THEN
    RAISE EXCEPTION 'expected one CHECK ((quality IS NULL) = (reconciled_at IS NULL)) on tg.snapshot, found %',
      coalesce(v_names, '{}');
  END IF;
  EXECUTE format('ALTER TABLE tg.snapshot DROP CONSTRAINT %I', v_names[1]);
END $$;
ALTER TABLE tg.snapshot ADD CONSTRAINT reconciled_needs_grade CHECK (reconciled_at IS NULL OR quality IS NOT NULL);
COMMENT ON CONSTRAINT reconciled_needs_grade ON tg.snapshot IS
  'A grade may exist without reconciled_at (the reconciler grades first, in its own transaction, then applies '
  'the snapshot in chunks); reconciled_at requires a grade. Replaces 001''s (quality IS NULL) = '
  '(reconciled_at IS NULL).';

ALTER TABLE tg.reconcile_run
  ADD COLUMN chunk integer CHECK (chunk >= 1),
  ADD COLUMN from_user_id bigint,
  ADD COLUMN through_user_id bigint,
  ADD CONSTRAINT reconcile_run_chunk_range CHECK (from_user_id <= through_user_id);
COMMENT ON COLUMN tg.reconcile_run.chunk IS
  'Chunk number (1..n) of a snapshot applied in several transactions; NULL = a one-shot run. The counts of '
  'the row are that chunk''s; a snapshot''s totals are the sum over its rows.';
COMMENT ON COLUMN tg.reconcile_run.from_user_id IS 'first user_id applied by this chunk (inclusive)';
COMMENT ON COLUMN tg.reconcile_run.through_user_id IS
  'last user_id applied by this chunk (inclusive); the snapshot''s applied_through_user_id after it';

CREATE TABLE tg.gap_progress (
  gap_id               bigint NOT NULL REFERENCES tg.gap ON DELETE CASCADE,
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE ON DELETE CASCADE,
  last_user_id         bigint,                             -- open spells with user_id <= this are done
  done                 boolean NOT NULL DEFAULT false,
  updated_at           timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (gap_id, chat_id)
);
COMMENT ON TABLE tg.gap_progress IS
  'Progress of rule G1 (a gap makes the open spells of its chats suspect and queues outage probes) per gap and '
  'chat, in user_id order, so a large chat is handled in short transactions and resumes after a restart. '
  'Written by the reconciler. A chat deleted by a migration loses its rows; the moved history is rebuilt.';

------------------------------------------------------------------------------------------------------------
-- Versioned dirty markers for the chat_day rollup
------------------------------------------------------------------------------------------------------------

CREATE SEQUENCE tg.rollup_dirty_version;
COMMENT ON SEQUENCE tg.rollup_dirty_version IS 'versions of tg.rollup_dirty markers';
ALTER TABLE tg.rollup_dirty ADD COLUMN version bigint NOT NULL DEFAULT nextval('tg.rollup_dirty_version');
COMMENT ON COLUMN tg.rollup_dirty.version IS
  'Bumped by every insert or update of the marker (trigger rollup_dirty_version). A rollup clears only markers '
  'whose version it read (DELETE ... WHERE version <= the version read), so a marker written while it ran '
  'survives for the next run.';

CREATE FUNCTION tg.rollup_dirty_bump() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.version := nextval('tg.rollup_dirty_version');
  RETURN NEW;
END $$;
COMMENT ON FUNCTION tg.rollup_dirty_bump() IS
  'BEFORE INSERT OR UPDATE on tg.rollup_dirty: a new version for every write, also for the direct upserts of '
  'tg.move_chat_history and tg.forget_user.';
CREATE TRIGGER rollup_dirty_version BEFORE INSERT OR UPDATE ON tg.rollup_dirty
  FOR EACH ROW EXECUTE FUNCTION tg.rollup_dirty_bump();

CREATE FUNCTION tg.mark_dirty(p_chat_id bigint, p_from_day date) RETURNS bigint LANGUAGE sql AS $$
  INSERT INTO tg.rollup_dirty AS r (chat_id, from_day) VALUES (p_chat_id, p_from_day)
  ON CONFLICT (chat_id) DO UPDATE SET from_day = least(r.from_day, excluded.from_day), marked_at = now()
  RETURNING r.version
$$;
COMMENT ON FUNCTION tg.mark_dirty(bigint, date) IS
  'The one way to invalidate stats.chat_day: upsert the chat''s marker with the earliest affected UTC day '
  '(least of old and new) and return its new version. Every writer of spells or evidence calls it.';

------------------------------------------------------------------------------------------------------------
-- Erasure record (forget-user)
------------------------------------------------------------------------------------------------------------

CREATE TABLE tg.erasure (
  user_hash            bytea PRIMARY KEY,                  -- tg.erasure_hash(user_id)
  erased_at            timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE tg.erasure IS
  'People erased by tgaud forget-user (D17), so evidence dated before the erasure (a late update, a snapshot '
  'listed before it, a probe answer) does not bring them back. Keyed by an unkeyed SHA-256 of the user id: a '
  'pseudonym, not anonymity (a Telegram id can be found by enumeration). Written by the CLI; read through '
  'tg.is_erased by ingest and the reconciler.';

CREATE FUNCTION tg.erasure_hash(p_user_id bigint) RETURNS bytea
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN sha256(convert_to('tgaud-erasure:' || p_user_id::text, 'UTF8'));
COMMENT ON FUNCTION tg.erasure_hash(bigint) IS 'The key of tg.erasure for one user id.';

CREATE FUNCTION tg.is_erased(p_user_id bigint, p_at timestamptz) RETURNS boolean
LANGUAGE sql STABLE PARALLEL SAFE
RETURN EXISTS (SELECT 1 FROM tg.erasure e WHERE e.user_hash = tg.erasure_hash(p_user_id) AND e.erased_at >= p_at);
COMMENT ON FUNCTION tg.is_erased(bigint, timestamptz) IS
  'True when the user was erased at or after p_at: evidence dated p_at predates the erasure and must not be '
  'stored. Evidence after the erasure (the person joins again) is new and is stored.';

------------------------------------------------------------------------------------------------------------
-- Finished snapshots are closed for new member rows
------------------------------------------------------------------------------------------------------------

CREATE FUNCTION tg.snapshot_member_open_only() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, tg AS $$
DECLARE
  r record;
BEGIN
  -- FOR SHARE conflicts with the finalizer's UPDATE (FOR NO KEY UPDATE), the FK's FOR KEY SHARE does not:
  -- a page inserted first makes the finalizer wait until it commits; a finalizer first makes the page wait
  -- and then see finished_at (READ COMMITTED re-reads the locked row) and fail.
  FOR r IN SELECT s.snapshot_id, s.finished_at FROM tg.snapshot s
            WHERE s.snapshot_id IN (SELECT DISTINCT n.snapshot_id FROM new_rows n)
            ORDER BY s.snapshot_id FOR SHARE LOOP
    IF r.finished_at IS NOT NULL THEN
      RAISE EXCEPTION 'snapshot % is finished: no member rows can be added to it', r.snapshot_id
        USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
  END LOOP;
  RETURN NULL;
END $$;
COMMENT ON FUNCTION tg.snapshot_member_open_only() IS
  'AFTER INSERT ... FOR EACH STATEMENT on tg.snapshot_member: rejects rows for a snapshot whose finished_at is '
  'set, so the reconciler never grades a snapshot that is still changing. Security definer: it locks snapshot '
  'rows for collect_writer, which may not.';
CREATE TRIGGER snapshot_member_open_only AFTER INSERT ON tg.snapshot_member
  REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION tg.snapshot_member_open_only();
