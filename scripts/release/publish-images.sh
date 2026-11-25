#!/usr/bin/env bash
# Builds the images of the current tree, gates each build and pushes it tagged REPO<suffix>:<tree hash>,
# skipping every image whose tag the registry already has, and stopping when the registry can't say.
# CI's publishing job runs it on pull requests, after the tests passed, so a tree tag means a tested tree.
# The build is loaded into the local Docker store, run through gate-image.sh (--selfcheck, offline,
# read-only root) and only then pushed: the image that passed is the image that is pushed, and an image
# that fails the gate never gets its tag.
# Usage: publish-images.sh REPO    e.g. ghcr.io/tutunak/tgaud
# The image list is scripts/release/images.txt, or RELEASE_IMAGES_FILE.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

(($# == 1)) && [[ -n $1 ]] || { log "usage: publish-images.sh REPO"; exit 2; }
repo=$1
cd "$(git rev-parse --show-toplevel)"
load_images
tag=$(tree_tag HEAD)
log "tree $tag"

for i in "${!IMAGE_SUFFIX[@]}"; do
  image=$repo${IMAGE_SUFFIX[i]}
  status=0
  image_exists "$image:$tag" || status=$?
  case $status in
    0)
      log "skip $image:$tag: the registry has it"
      continue
      ;;
    1) log "build $image:$tag" ;;
    # Never build over a tag that may exist: the same tree needn't build the same image (base tags and
    # package versions move), and a release may already point at the one there.
    *) die "can't tell whether the registry has $image:$tag (above); nothing more was built" ;;
  esac
  docker buildx build --load -t "$image:$tag" -f "${IMAGE_DOCKERFILE[i]}" "${IMAGE_CONTEXT[i]}"
  "$RELEASE_DIR/gate-image.sh" "$image:$tag"
  docker push "$image:$tag"
done
log "all images of tree $tag are in the registry"
