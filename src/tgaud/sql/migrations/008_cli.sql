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
-- The check must see the erasure whenever it committed, which a REPEATABLE READ or SERIALIZABLE transaction that
-- started earlier cannot (it keeps its older view of tg.erasure), so pages are only accepted in READ COMMITTED
-- transactions, where the check below runs on a fresh snapshot; the collect script uses the default level.

CREATE FUNCTION tg.snapshot_member_skip_erased() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, tg AS $$
DECLARE
  v_erased boolean;
  v_inviter_erased boolean := false;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'snapshot pages must be written in a READ COMMITTED transaction (this one is %): an older '
                    'transaction snapshot cannot see a later erasure', current_setting('transaction_isolation')
      USING ERRCODE = 'invalid_transaction_state';
  END IF;
  -- plain statements of a volatile function: each takes a fresh snapshot at READ COMMITTED
  SELECT EXISTS (SELECT 1 FROM tg.erasure r WHERE r.user_hash = tg.erasure_hash(NEW.user_id)
                                              AND r.erased_at >= NEW.seen_at) INTO v_erased;
  IF v_erased THEN
    RETURN NULL;
  END IF;
  IF NEW.inviter_id IS NOT NULL THEN
    SELECT EXISTS (SELECT 1 FROM tg.erasure r WHERE r.user_hash = tg.erasure_hash(NEW.inviter_id)
                                                AND r.erased_at >= NEW.seen_at) INTO v_inviter_erased;
    IF v_inviter_erased THEN NEW.inviter_id := NULL; END IF;
  END IF;
  RETURN NEW;
END $$;
COMMENT ON FUNCTION tg.snapshot_member_skip_erased() IS
  'BEFORE INSERT on tg.snapshot_member: skips a row about a person erased at or after its seen_at (as tg.is_erased) '
  'and clears an erased inviter; refuses transactions above READ COMMITTED. Security definer: the collect script may '
  'not read tg.erasure.';
CREATE TRIGGER snapshot_member_skip_erased BEFORE INSERT ON tg.snapshot_member
  FOR EACH ROW EXECUTE FUNCTION tg.snapshot_member_skip_erased();
