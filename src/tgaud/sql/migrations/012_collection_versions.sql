-- 012_collection_versions.sql (reimport task): a later version of an imported collection. Additive: 011's unique
-- index and everything the released image uses stay as they are. Runs as the tenant login; the runner owns the
-- transaction.

------------------------------------------------------------------------------------------------------------
-- Later versions of one collection
------------------------------------------------------------------------------------------------------------
-- A tgstats.db uploaded while `tgstats collect` was still collecting holds a partial version of the collection;
-- the finished file holds a later version of the same uuid. The Import page writes that later version as a new
-- snapshot of the chat holding only the members no stored version of the collection listed (the delta), so every
-- observation is stored once. The first import of a uuid keeps it in collect_uuid (the claim: its chat_id and
-- coalesce(source_chat_id, chat_id), which M1 keeps current); a delta row keeps collect_uuid NULL, so the unique
-- index of 011 still holds one row per uuid and the released image's lookup still finds every collection it
-- imported, and records its collection in version_of. The versions of a uuid are the rows with either column set
-- to it; each one's finished_at is the file's.

ALTER TABLE tg.snapshot ADD COLUMN version_of uuid;
COMMENT ON COLUMN tg.snapshot.version_of IS
  'The collection (tgstats collect uuid) this snapshot is a later version of: set by the web Import page on a '
  'snapshot that lists only the members no earlier version of that collection listed (always complete = false). '
  'NULL otherwise; its first import has the uuid in collect_uuid instead.';
CREATE INDEX snapshot_version_of ON tg.snapshot (version_of) WHERE version_of IS NOT NULL;
ALTER TABLE tg.snapshot ADD CONSTRAINT snapshot_one_collection_column
  CHECK (collect_uuid IS NULL OR version_of IS NULL);

------------------------------------------------------------------------------------------------------------
-- Imports of one collection serialize
------------------------------------------------------------------------------------------------------------
-- Every import of a collection takes this transaction-level lock after its chat's lock and before it reads the
-- collection's versions, so a lookup of a uuid from any chat's transaction waits for a claim in flight and then
-- sees it committed (READ COMMITTED: the read starts after the lock is granted). Two-int4 advisory keys are
-- tgaud's own (tgaud.db); this class is "tgac", the second key the uuid's first 32 bits. Two uuids sharing them
-- only serialize two unrelated imports.

CREATE FUNCTION tg.lock_collection(p_uuid uuid) RETURNS void
LANGUAGE sql VOLATILE AS $$
  SELECT pg_advisory_xact_lock(1952932195, ('x' || left(replace(p_uuid::text, '-', ''), 8))::bit(32)::int)
$$;
COMMENT ON FUNCTION tg.lock_collection(uuid) IS
  'Transaction-level advisory lock on one tgstats collection uuid (class 0x74676163, "tgac"); the Import page takes '
  'it after the chat lock, before it reads the collection''s versions.';
