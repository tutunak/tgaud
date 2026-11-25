#!/usr/bin/env bash
# The gate an image passes before its tree tag is pushed: run it the way the deploy role runs it
# (docs/deploy-contract.md), offline with a read-only root, and require exactly one `selfcheck ok: ` line.
# The same flags as `mise run image-test` (scripts/image-test.sh). Exit 0 when it passes.
# Usage: gate-image.sh IMAGE    (an image in the local Docker store)
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

(($# == 1)) && [[ -n $1 ]] || { log "usage: gate-image.sh IMAGE"; exit 2; }
out=$(docker run --rm --network=none --read-only --tmpfs /tmp --cap-drop=all --security-opt=no-new-privileges "$1" --selfcheck) \
  || { printf '%s\n' "$out"; die "$1: --selfcheck failed"; }
printf '%s\n' "$out"
[[ $out == "selfcheck ok: "* && $out != *$'\n'* ]] || die "$1: --selfcheck did not print exactly one 'selfcheck ok:' line"
log "$1 passed the gate"
