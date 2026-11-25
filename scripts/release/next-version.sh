#!/usr/bin/env bash
# Prints the release version of HEAD, from the X.Y.Z tags on the remote at this moment:
# - the X.Y.Z tag already on HEAD (the highest, if several): a re-run bumps nothing;
# - otherwise the highest X.Y.Z, in numeric order, plus one patch; 1.0.0 when there is none.
# An untagged commit gets the next patch even if a newer commit was released first, so a retried
# release is never lost; version order is release order. Plain X.Y.Z only: v1.2.3 or 1.2 don't count.
# Usage: next-version.sh [REMOTE]    (default origin)
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

remote=${1:-origin}
head=$(git rev-parse --verify HEAD)
refs=$(git ls-remote --tags "$remote") || die "can't list the tags of $remote"

# "commit version" per X.Y.Z tag. An annotated tag's peeled line (TAG^{}) names its commit and comes
# after the tag's own line, so it replaces it.
versions=$(awk '
  {
    name = $2; sub(/^refs\/tags\//, "", name)
    peeled = sub(/\^\{\}$/, "", name)
    if (name !~ /^[0-9]+\.[0-9]+\.[0-9]+$/) next
    if (!(name in commit) || peeled) commit[name] = $1
  }
  END { for (name in commit) print commit[name], name }
' <<<"$refs")

sorted=$(awk '{ print $2 }' <<<"$versions" | sort -t. -k1,1n -k2,2n -k3,3n)
on_head=$(awk -v head="$head" '$1 == head { print $2 }' <<<"$versions" | sort -t. -k1,1n -k2,2n -k3,3n | tail -n 1)
if [[ -n $on_head ]]; then
  log "HEAD already has $on_head: reusing it"
  printf '%s\n' "$on_head"
  exit 0
fi

highest=$(tail -n 1 <<<"$sorted")
if [[ -z $highest ]]; then
  log "no X.Y.Z tag on $remote: starting at 1.0.0"
  printf '1.0.0\n'
  exit 0
fi
IFS=. read -r major minor patch <<<"$highest"
next="$((10#$major)).$((10#$minor)).$((10#$patch + 1))"
log "highest tag on $remote is $highest: next is $next"
printf '%s\n' "$next"
