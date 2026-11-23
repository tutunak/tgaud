-- admin.sql: optional, run once by a server admin (a role with CREATEROLE) in the service's database,
-- before or after schema.sql:
--   psql -v ON_ERROR_STOP=1 -v tenant=audience -d audience -f admin.sql
-- schema.sql itself runs as the tenant login (NOSUPERUSER NOCREATEDB NOCREATEROLE) and creates no role.
--
-- bi_reader       read-only BI access: schema stats only (aggregates). Grant it to the BI tool's login.
-- collect_writer  the owner's collect script: may insert tg.chat rows, tg.snapshot and tg.snapshot_member,
--                 and mark a snapshot finished. Grant it to the script's login.

\if :{?tenant}
\else
  \set tenant audience
\endif

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bi_reader') THEN
    CREATE ROLE bi_reader NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'collect_writer') THEN
    CREATE ROLE collect_writer NOLOGIN;
  END IF;
END $$;

-- The tenant may hand these roles to its own logins.
GRANT bi_reader TO :"tenant" WITH ADMIN OPTION;
GRANT collect_writer TO :"tenant" WITH ADMIN OPTION;

-- When schema.sql already ran, apply the grants now (a superuser may; an admin without superuser asks the
-- tenant to run `SELECT tg.apply_grants();`). Otherwise schema.sql applies them itself.
SELECT CASE WHEN to_regprocedure('tg.apply_grants()') IS NULL THEN 'schema not loaded yet: schema.sql will grant'
       END AS note
\gset
\if :{?note}
  \echo :note
\else
  SELECT tg.apply_grants();
\endif
