#!/usr/bin/env bash
# The image run the way the deploy role runs it (docs/deploy-contract.md, "Deploy order"), against a test database
# and the fake Bot API (tests/tgaud/container_env.py, never the real Telegram):
#
#   build -> --selfcheck with --network=none -> migrate (network on; twice: the second is a no-op)
#   -> serve with /metrics published on 127.0.0.1 -> /health 200 within 60 s through the published port
#   -> SIGTERM -> the container exits 0 within 10 s
#
# Every container runs with the unit's flags: read-only root, --tmpfs /tmp, all capabilities dropped,
# no-new-privileges. The fake and a relay to the database listen on the host's 127.0.0.1; the containers reach them
# at HOST_ALIAS, which pasta maps to the host's loopback (--map-host-loopback), and the relay reaches the database
# wherever TGAUD_TEST_ADMIN_URL points. Called by `mise run ci`; set TGAUD_TEST_ADMIN_URL to use a running
# PostgreSQL 18.6, else the helper starts one with podman.
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
HOST_ALIAS=169.254.1.3
NETWORK="pasta:--map-host-loopback,$HOST_ALIAS"
FLAGS=(--read-only --tmpfs /tmp --cap-drop=all --security-opt=no-new-privileges)
name="tgaud-e2e-$$-$RANDOM"

say() { echo "container-e2e: $*"; }
fail() { echo "container-e2e: FAIL: $*" >&2; exit 1; }

# The image is named by the ID this build printed, so a concurrent build elsewhere cannot change what this run checks.
image=$(podman build -q .)
say "image $image"

out=$(podman run --rm --network=none "${FLAGS[@]}" "$image" --selfcheck)
echo "$out"
[[ $out == "selfcheck ok: "* && $out != *$'\n'* ]] || fail "--selfcheck did not print one 'selfcheck ok:' line"

# The test database and the fake Bot API: the helper prints the environment, then "ready", and cleans up (and
# reports what serve did) once its stdin closes.
coproc HELPER { uv run --quiet python tests/tgaud/container_env.py "$HOST_ALIAS"; }
helper_pid=$HELPER_PID
helper_in=${HELPER[1]}
declare -A cfg=()
while IFS= read -r line <&"${HELPER[0]}"; do
    [[ $line == ready ]] && break
    cfg[${line%%=*}]=${line#*=}
done
helper_out=${HELPER[0]}

finish() {
    local rc=$?
    podman rm -f "$name" >/dev/null 2>&1 || true
    exec {helper_in}>&- 2>/dev/null || true
    cat <&"$helper_out" 2>/dev/null || true
    local hrc=0
    wait "$helper_pid" || hrc=$?
    if [[ $rc -eq 0 && $hrc -ne 0 ]]; then
        echo "container-e2e: FAIL: the helper's checks failed (exit $hrc)" >&2
        rc=$hrc
    fi
    [[ $rc -eq 0 ]] && say "ok"
    exit "$rc"
}
trap finish EXIT
[[ -n ${cfg[TGAUD_DATABASE_URL]:-} && -n ${cfg[HEALTH_PORT]:-} ]] || fail "the helper did not provide a database"

envs=()
for key in TGAUD_DATABASE_URL TGAUD_DATABASE_PASSWORD TGAUD_BOT_TOKEN TGAUD_LINK_HMAC_KEY TGAUD_BOT_API_URL \
           TGAUD_ACTIVATION_ID; do
    envs+=(-e "$key=${cfg[$key]}")
done
port=${cfg[HEALTH_PORT]}

out=$(podman run --rm --network="$NETWORK" "${FLAGS[@]}" "${envs[@]}" "$image" migrate) || fail "migrate failed: $out"
echo "$out"
[[ $out == "schema at "*"(applied now: 001, "* ]] || fail "migrate did not apply the migrations"
out=$(podman run --rm --network="$NETWORK" "${FLAGS[@]}" "${envs[@]}" "$image" migrate) || fail "migrate failed: $out"
echo "$out"
[[ $out == *"(applied now: none)" ]] || fail "a second migrate was not a no-op"

podman run -d --name "$name" --network="$NETWORK" "${FLAGS[@]}" -p "127.0.0.1:$port:9108" "${envs[@]}" \
    "$image" serve >/dev/null
started=$SECONDS
health=""
while (( SECONDS - started < 60 )); do
    health=$(curl -s --max-time 2 -w ' %{http_code}' "http://127.0.0.1:$port/health" || true)
    [[ $health == *" 200" ]] && break
    [[ $(podman inspect -f '{{.State.Running}}' "$name") == true ]] || { podman logs "$name" >&2; fail "serve exited"; }
    sleep 0.5
done
[[ $health == *" 200" ]] || { podman logs "$name" >&2; fail "/health not 200 within 60 s: $health"; }
[[ $health == *"activation=${cfg[TGAUD_ACTIVATION_ID]}"* ]] || fail "/health does not name the activation: $health"
say "/health 200 after $(( SECONDS - started )) s: ${health% *}"
metrics=$(curl -s --max-time 5 "http://127.0.0.1:$port/metrics")
[[ $metrics == *"tgaud_poll_last_success_timestamp_seconds "* ]] || fail "/metrics lacks the poll gauge"

t0=$(date +%s%N)
podman kill --signal TERM "$name" >/dev/null
code=$(timeout 15 podman wait "$name") || { podman logs "$name" >&2; fail "serve still running 15 s after SIGTERM"; }
ms=$(( ($(date +%s%N) - t0) / 1000000 ))
podman logs "$name" 2>&1 | tail -n 5
[[ $code == 0 ]] || fail "serve exited $code after SIGTERM, want 0"
(( ms < 10000 )) || fail "serve took ${ms} ms to stop after SIGTERM, want under 10 s"
say "serve stopped ${ms} ms after SIGTERM, exit 0"
