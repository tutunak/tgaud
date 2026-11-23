#!/bin/sh
# psql in the test container as the tenant login; DB defaults to audfix. Extra args go to psql.
exec podman exec -i -e PGPASSWORD=tenant-pw-91x audience-pg-test psql -h 127.0.0.1 -U audience -d "${DB:-audfix}" -X -v ON_ERROR_STOP=1 "$@"
