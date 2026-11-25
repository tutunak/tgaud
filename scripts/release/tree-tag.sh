#!/usr/bin/env bash
# Prints the image tag of REV (default HEAD): its full git tree hash. On pull_request, CI's checkout
# is GitHub's merge ref, so this is the tree main gets if the PR is merged as it is.
# Usage: tree-tag.sh [REV]
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

tree_tag "${1:-HEAD}"
