#!/usr/bin/env bash
# Whether the registry has an image: exit 0 if it does, 1 if the registry says it doesn't, 2 on any
# other error (auth, network), printed on stderr. Reads the manifest only.
# Usage: image-exists.sh REF
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

(($# == 1)) || { log "usage: image-exists.sh REF"; exit 2; }
status=0
image_exists "$1" || status=$?
exit "$status"
