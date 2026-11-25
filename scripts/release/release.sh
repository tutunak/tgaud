#!/usr/bin/env bash
# Releases HEAD (a main commit) as X.Y.Z without building anything: checks that the images of its
# tree exist, picks the version (next-version.sh), adds the version tag to each image at the manifest
# level (no pull, same digest), and only then pushes the git tag X.Y.Z on HEAD. So a git tag means
# every image carries it. A stop before the git tag leaves no release; running again finishes it.
# CI's release job runs it on every push to main, one run at a time.
# Usage: release.sh REPO [REMOTE]    e.g. ghcr.io/tutunak/tgaud origin
# The image list is scripts/release/images.txt, or RELEASE_IMAGES_FILE.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

(($# == 1 || $# == 2)) && [[ -n $1 ]] || { log "usage: release.sh REPO [REMOTE]"; exit 2; }
repo=$1
remote=${2:-origin}
cd "$(git rev-parse --show-toplevel)"
load_images
head=$(git rev-parse --verify HEAD)
tag=$(tree_tag HEAD)
log "commit $head, tree $tag"

missing=()
for suffix in "${IMAGE_SUFFIX[@]}"; do
  status=0
  image_exists "$repo$suffix:$tag" || status=$?
  case $status in
    0) ;;
    1) missing+=("$repo$suffix:$tag") ;;
    *) die "can't read $repo$suffix:$tag from the registry (above); nothing was tagged" ;;
  esac
done
if ((${#missing[@]})); then
  log "missing images for tree $tag:"
  printf '  %s\n' "${missing[@]}" >&2
  log "These images are built by the pull request run on the same tree (the same file contents)."
  log "Most likely the PR was merged while it wasn't up to date with main, or without a"
  log "successful run on its final state (e.g. right after a rebase), so no run built this tree."
  log "Fix: require branches to be up to date and the checks to pass before merging. Nothing was tagged."
  exit 1
fi

version=$("$RELEASE_DIR/next-version.sh" "$remote")
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "bad version: $version"

for suffix in "${IMAGE_SUFFIX[@]}"; do
  log "tag $repo$suffix:$tag as $version"
  # --prefer-index=false: the default wraps a single manifest in a new index, which changes the digest.
  docker buildx imagetools create --prefer-index=false --tag "$repo$suffix:$version" "$repo$suffix:$tag"
done

# The ^{} pattern adds the peeled line of an annotated tag, the commit it points to, last.
if [[ $(git ls-remote --tags "$remote" "refs/tags/$version" "refs/tags/$version^{}" | awk 'END { print $1 }') == "$head" ]]; then
  log "git tag $version is already on $head"
else
  git push "$remote" "$head:refs/tags/$version"
fi
log "released $version: commit $head, tree $tag"
if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then
  printf 'Released **%s**: commit `%s`, images tagged `%s` (tree `%s`).\n' \
    "$version" "$head" "$version" "$tag" >>"$GITHUB_STEP_SUMMARY"
fi
