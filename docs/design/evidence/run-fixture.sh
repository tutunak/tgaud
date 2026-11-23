#!/bin/sh
# Rebuild the fixture database from scratch: tenant setup, schema (as the tenant), admin.sql, fixture.
# Usage: sh run-fixture.sh   (container audience-pg-test, files mounted at /work)
set -e
DB=${DB:-audfix}
SU="podman exec -i audience-pg-test psql -U postgres -v ON_ERROR_STOP=1 -q"
TENANT="podman exec -i -e PGPASSWORD=tenant-pw-91x audience-pg-test psql -h 127.0.0.1 -U audience -v ON_ERROR_STOP=1 -q"
$SU -d postgres -v db=$DB -f /work/evidence/00-setup-tenant.sql >/dev/null
$TENANT -d $DB -f /work/schema.sql
$SU -d $DB -v tenant=audience -f /work/admin.sql
$TENANT -d $DB -f /work/evidence/10-fixture.sql 2>&1 | grep -v 'permission denied to vacuum\|^$' || true
