-- Test setup, run as the container superuser: a tenant login like production's
-- (NOSUPERUSER NOCREATEDB NOCREATEROLE, owner of its database and of public).
\set ON_ERROR_STOP 1
DROP DATABASE IF EXISTS :db;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'audience') THEN
    CREATE ROLE audience LOGIN PASSWORD 'tenant-pw-91x' NOSUPERUSER NOCREATEDB NOCREATEROLE;
  END IF;
END $$;
CREATE DATABASE :db OWNER audience;
\c :db
ALTER SCHEMA public OWNER TO audience;
SELECT rolname, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname = 'audience';
