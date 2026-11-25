-- 011_snapshot_import.sql (snapshot-import task): what the web UI's Import page needs beyond 001-010. Additive.
-- Runs as the tenant login; the runner owns the transaction.

------------------------------------------------------------------------------------------------------------
-- The collection's own id: duplicate detection for uploaded tgstats files
------------------------------------------------------------------------------------------------------------
-- `tgstats collect` gives every collection a random uuid when it starts. The Import page stores it with the
-- snapshot it writes, and looks it up before anything else, so re-uploading a file, two racing uploads of it, or
-- an upload whose commit acknowledgement was lost imports each collection at most once, whatever happened to the
-- chat since (a migration re-keys chat_id; the uuid stays). Snapshots written by the collect script through the
-- contract of design.md 5 have none (NULL; unique indexes let NULLs repeat).

ALTER TABLE tg.snapshot ADD COLUMN collect_uuid uuid;
COMMENT ON COLUMN tg.snapshot.collect_uuid IS
  'The random id tgstats collect gave the collection (its snapshots.uuid); set by the web Import page, NULL for '
  'snapshots the collect script wrote itself. Unique: one collection is imported at most once.';
CREATE UNIQUE INDEX snapshot_collect_uuid ON tg.snapshot (collect_uuid);
