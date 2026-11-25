# Shared by the scripts in scripts/release: logging, the image list and the existence check.
# Sourced, not run. The scripts work on the git repository of the current directory.

RELEASE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

log() { printf '%s: %s\n' "${0##*/}" "$*" >&2; }
die() { log "$*"; exit 1; }

# Fills IMAGE_SUFFIX, IMAGE_CONTEXT and IMAGE_DOCKERFILE, one entry per image.
load_images() {
  local file=${RELEASE_IMAGES_FILE:-$RELEASE_DIR/images.txt} suffix context dockerfile rest
  [[ -f $file ]] || die "image list not found: $file"
  IMAGE_SUFFIX=() IMAGE_CONTEXT=() IMAGE_DOCKERFILE=()
  while IFS='|' read -r suffix context dockerfile rest; do
    [[ $suffix == '#'* || -z $suffix$context ]] && continue
    [[ -n $context && -n $dockerfile && -z $rest ]] || die "bad line in $file: $suffix|$context|$dockerfile|$rest"
    IMAGE_SUFFIX+=("$suffix") IMAGE_CONTEXT+=("$context") IMAGE_DOCKERFILE+=("$dockerfile")
  done <"$file"
  ((${#IMAGE_SUFFIX[@]})) || die "no images in $file"
}

# The tag of a tree's images: the full 40-hex git tree hash of REV (default HEAD). The full hash, since
# an image that has the tag is reused as it is: a short-hash collision would ship other code.
tree_tag() {
  local tag
  tag=$(git rev-parse --verify --quiet "${1:-HEAD}^{tree}") || die "no tree for ${1:-HEAD}"
  [[ $tag =~ ^[0-9a-f]{40}$ ]] || die "unexpected tree hash: $tag"
  printf '%s\n' "$tag"
}

# 0: the registry has REF; 1: the registry answered that it doesn't; 2: anything else (auth, network),
# with the error on stderr. Reads the manifest only, never pulls.
image_exists() {
  local out
  out=$(docker buildx imagetools inspect "$1" 2>&1 >/dev/null) && return 0
  if [[ $(tail -n 1 <<<"$out") == *": not found" ]]; then
    return 1
  fi
  printf '%s\n' "$out" >&2
  return 2
}

# The index or manifest digest REF points at.
image_digest() {
  docker buildx imagetools inspect "$1" --format '{{json .Manifest}}' | jq -er .digest
}
