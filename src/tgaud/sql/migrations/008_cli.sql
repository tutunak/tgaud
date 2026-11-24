-- 008_cli.sql (cli task): what only the CLI's forget-user needs beyond 001 and 002. Additive. Runs as the tenant
-- login; the runner owns the transaction.

------------------------------------------------------------------------------------------------------------
-- The collect script cannot put an erased person back
------------------------------------------------------------------------------------------------------------
-- tg.forget_user locks tg.snapshot_member against new pages, deletes the person's rows and commits the erasure
-- record. A page that was collected before the erasure but is inserted after it (the script keeps pages in
-- memory, and its snapshot may have been opened between forget-user's checks and its commit) would bring the
-- id back, so every page is checked against tg.erasure as it is inserted: a row about an erased person
-- dated at or before the erasure is skipped, and an erased inviter is cleared. A page dated after the erasure
-- (the person is listed again) is new evidence and is stored.

CREATE FUNCTION tg.snapshot_member_skip_erased() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, tg AS $$
BEGIN
  IF tg.is_erased(NEW.user_id, NEW.seen_at) THEN
    RETURN NULL;
  END IF;
  IF NEW.inviter_id IS NOT NULL AND tg.is_erased(NEW.inviter_id, NEW.seen_at) THEN
    NEW.inviter_id := NULL;
  END IF;
  RETURN NEW;
END $$;
COMMENT ON FUNCTION tg.snapshot_member_skip_erased() IS
  'BEFORE INSERT on tg.snapshot_member: skips a row about a person erased at or after its seen_at (tg.is_erased) '
  'and clears an erased inviter. Security definer: the collect script may not read tg.erasure.';
CREATE TRIGGER snapshot_member_skip_erased BEFORE INSERT ON tg.snapshot_member
  FOR EACH ROW EXECUTE FUNCTION tg.snapshot_member_skip_erased();
