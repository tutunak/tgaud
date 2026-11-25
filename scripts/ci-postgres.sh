#!/usr/bin/env bash
# CI's PostgreSQL for the tests and the container end-to-end: the server tests/tgaud/conftest.py starts with
# podman, started here with Docker (the runner's cgroups let Docker apply the memory cap): the same image, the
# same --memory 512m and the same SERVER_SETTINGS, read from conftest.py so they cannot drift. The timing tests
# (the million-spell and 100,000-pending-probe cases) are calibrated to those limits, so the script checks that
# the cap and every setting are in effect before it reports the server ready. The bootstrap superuser, not a
# CREATEROLE+CREATEDB login, is what TGAUD_TEST_ADMIN_URL needs: the tenant database is created OWNER tenant.
# Prints TGAUD_TEST_ADMIN_URL=...; with GITHUB_ENV set (a CI step) it also appends that line there, masked.
# Usage: ci-postgres.sh
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
name=tgaud-ci-postgres-$$
say() { echo "ci-postgres: $*" >&2; }
fail() { echo "ci-postgres: FAIL: $*" >&2; docker logs "$name" >&2 2>&1 || true; exit 1; }

# IMAGE and SERVER_SETTINGS are literals in conftest.py: read them without importing it (no dependencies).
mapfile -t spec < <(python3 -I - "$repo/tests/tgaud/conftest.py" <<'PY'
import ast, sys
values = {}
for node in ast.parse(open(sys.argv[1]).read()).body:
    if isinstance(node, ast.Assign) and len(node.targets) == 1 and getattr(node.targets[0], "id", "") in ("IMAGE", "SERVER_SETTINGS"):
        values[node.targets[0].id] = ast.literal_eval(node.value)
print(values["IMAGE"])
print("\n".join(values["SERVER_SETTINGS"]))
PY
)
image=${spec[0]}
settings=("${spec[@]:1}")
[[ $image == docker.io/library/postgres:18.6-* ]] && ((${#settings[@]} > 0 && ${#settings[@]} % 2 == 0)) \
    || fail "unexpected IMAGE or SERVER_SETTINGS in conftest.py: $image ${settings[*]}"

password=$(head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n')
[[ -z ${GITHUB_ENV:-} ]] || echo "::add-mask::$password"
docker run -d --name "$name" -e "POSTGRES_PASSWORD=$password" -p 127.0.0.1::5432 --memory 512m "$image" "${settings[@]}" >/dev/null
port=$(docker port "$name" 5432/tcp | head -n 1 | sed 's/.*://')

ready=false
for _ in $(seq 120); do
    if docker exec "$name" pg_isready -q -U postgres -h 127.0.0.1; then ready=true; break; fi
    sleep 0.5
done
[[ $ready == true ]] || fail "PostgreSQL did not become ready"

# The cap: Docker's setting and the container's own cgroup.
limit=$(docker inspect -f '{{.HostConfig.Memory}}' "$name")
[[ $limit == 536870912 ]] || fail "memory cap is $limit, want 536870912"
cgroup=$(docker exec "$name" sh -c 'cat /sys/fs/cgroup/memory.max 2>/dev/null || cat /sys/fs/cgroup/memory/memory.limit_in_bytes')
[[ $cgroup == 536870912 ]] || fail "the container's cgroup limit is $cgroup, want 536870912"

# Every `-c name=value` pair of SERVER_SETTINGS is what the server reports.
for ((i = 0; i < ${#settings[@]}; i += 2)); do
    [[ ${settings[i]} == -c && ${settings[i + 1]} == *=* ]] || fail "unexpected SERVER_SETTINGS entry: ${settings[i]} ${settings[i + 1]}"
    key=${settings[i + 1]%%=*}
    want=${settings[i + 1]#*=}
    got=$(docker exec -e "PGPASSWORD=$password" "$name" psql -X -At -h 127.0.0.1 -U postgres -c "SHOW $key")
    [[ $got == "$want" ]] || fail "$key is $got, want $want"
done
version=$(docker exec -e "PGPASSWORD=$password" "$name" psql -X -At -h 127.0.0.1 -U postgres -c "SHOW server_version")
[[ $version == 18.6* ]] || fail "server_version is $version, want 18.6"
say "PostgreSQL $version ready on 127.0.0.1:$port, memory cap 512m, $((${#settings[@]} / 2)) settings in effect"

url="postgresql://postgres:$password@127.0.0.1:$port/postgres"
echo "TGAUD_TEST_ADMIN_URL=$url"
[[ -z ${GITHUB_ENV:-} ]] || echo "TGAUD_TEST_ADMIN_URL=$url" >>"$GITHUB_ENV"
