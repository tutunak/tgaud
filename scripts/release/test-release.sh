#!/usr/bin/env bash
# Tests for the release scripts against a throwaway registry (registry:2 on a loopback port), a
# docker-container buildx builder like CI's, and a throwaway git repository with a bare origin, small
# busybox images (two, so the list-driven loops run for more than one image) and a commit per tree.
# Touches no real registry or remote. Everything it starts is removed on any exit. CI's release-scripts
# job and `mise run release-scripts` run it. Needs Docker with buildx, curl, git and jq.
set -Euo pipefail

RELEASE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REAL_REPO=$(cd "$RELEASE_DIR/../.." && pwd)
# shellcheck source=scripts/release/lib.sh
source "$RELEASE_DIR/lib.sh"
REGISTRY_IMAGE=registry:2.8.3
BASE_IMAGE=docker.io/library/busybox:1.37.0
WORK=$(mktemp -d)
TEST_ID=$(date +%s)-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
BUILDER=tgaud-release-test-$TEST_ID
registry_cid=""
builder_made=false
port=""

cleanup() {
  local status=$?
  trap - EXIT
  [[ -n $registry_cid ]] && { docker rm -f "$registry_cid" >/dev/null || status=1; }
  # The images the builds loaded into the local store, removed by repository:tag, never by image ID: Docker
  # would delete every tag of an ID, including one a developer or another task gave the same build.
  [[ -n $port ]] && docker image ls --format '{{.Repository}}:{{.Tag}}' --filter "reference=127.0.0.1:$port/*" |
    sort -u | xargs -r docker rmi >/dev/null 2>&1
  if [[ $builder_made == true ]] && docker buildx inspect "$BUILDER" >/dev/null 2>&1; then
    docker buildx rm "$BUILDER" >/dev/null || status=1
  fi
  rm -rf "$WORK"
  exit "$status"
}
trap cleanup EXIT

failures=0
ok() { printf 'ok    %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; failures=$((failures + 1)); }
check() { # NAME COMMAND...
  local name=$1 out
  shift
  if out=$("$@" 2>&1); then ok "$name"; else fail "$name"; printf '%s\n' "$out" | sed 's/^/      /'; fi
}

registry_cid=$(docker run -d -p 127.0.0.1::5000 "$REGISTRY_IMAGE") || exit 1
port=$(docker port "$registry_cid" 5000/tcp | head -n 1 | sed 's/.*://') || exit 1
# 127.0.0.1, not localhost: the Docker daemon pushes to it as an insecure loopback registry, and name
# resolution of localhost may prefer ::1.
REPO=127.0.0.1:$port/tgaud
for _ in $(seq 50); do
  curl -fsS "http://127.0.0.1:$port/v2/" >/dev/null 2>&1 && break
  sleep 0.2
done
printf '[registry."127.0.0.1:%s"]\n  http = true\n  insecure = true\n' "$port" >"$WORK/buildkitd.toml"
# Host network, so the builder reaches the registry on the host's loopback.
builder_made=true
docker buildx create --name "$BUILDER" --driver docker-container --driver-opt network=host \
  --buildkitd-config "$WORK/buildkitd.toml" --bootstrap >"$WORK/builder.log" 2>&1 || { cat "$WORK/builder.log"; exit 1; }
export BUILDX_BUILDER=$BUILDER

SUFFIXES=("" -second)
export RELEASE_IMAGES_FILE=$WORK/images.txt
cat >"$RELEASE_IMAGES_FILE" <<'EOF'
# suffix|context|dockerfile|, as scripts/release/images.txt
|.|img/first.Dockerfile|
-second|.|img/second.Dockerfile|
EOF
grep -v -- '-second' "$RELEASE_IMAGES_FILE" >"$WORK/one-image.txt"

ORIGIN=$WORK/origin.git
REPO_DIR=$WORK/repo
git init -q --bare "$ORIGIN"
# While $WORK/reject-pushes exists the origin refuses every push: a git tag push that fails after the retag.
cat >"$ORIGIN/hooks/pre-receive" <<EOF
#!/bin/sh
if [ -e '$WORK/reject-pushes' ]; then echo "origin: pushes are refused by the test" >&2; exit 1; fi
EOF
chmod +x "$ORIGIN/hooks/pre-receive"
git init -q -b main "$REPO_DIR"
cd "$REPO_DIR" || exit 1
git config user.name test
git config user.email test@example.invalid
git config commit.gpgsign false
git config tag.gpgsign false
git remote add origin "$ORIGIN"
mkdir img
# The fixtures' --selfcheck passes only when the container runs with the gate's flags: no network, no
# capabilities, no-new-privileges, a read-only root with a writable /tmp.
cat >img/selfcheck.sh <<'EOF'
#!/bin/sh
[ "$1" = --selfcheck ] || { echo "not run with --selfcheck" >&2; exit 2; }
[ "$(ls /sys/class/net)" = lo ] || { echo "has a network" >&2; exit 1; }
grep -q '^CapEff:[[:space:]]*0000000000000000$' /proc/self/status || { echo "has capabilities" >&2; exit 1; }
grep -q '^NoNewPrivs:[[:space:]]*1$' /proc/self/status || { echo "can gain privileges" >&2; exit 1; }
# In a subshell: a failed redirection of the special builtin `:` would end this script.
if (: >/probe) 2>/dev/null; then echo "the root file system is writable" >&2; exit 1; fi
(: >/tmp/probe) || { echo "/tmp is not writable" >&2; exit 1; }
echo "selfcheck ok: fixture"
EOF
chmod +x img/selfcheck.sh
for name in first second; do
  printf 'FROM %s\nCOPY state.txt /state.txt\nCOPY img/selfcheck.sh /selfcheck.sh\nLABEL image=%s\nENTRYPOINT ["/selfcheck.sh"]\n' \
    "$BASE_IMAGE" "$name" >"img/$name.Dockerfile"
done
# Images the gate must refuse: two lines, a failure, no `selfcheck ok: ` prefix, a write to the read-only root.
printf 'FROM %s\nENTRYPOINT ["sh","-c","echo selfcheck ok: a; echo b","--"]\n' "$BASE_IMAGE" >img/two-lines.Dockerfile
printf 'FROM %s\nENTRYPOINT ["sh","-c","echo boom >&2; exit 3","--"]\n' "$BASE_IMAGE" >img/fails.Dockerfile
printf 'FROM %s\nENTRYPOINT ["sh","-c","echo ok","--"]\n' "$BASE_IMAGE" >img/no-prefix.Dockerfile
printf 'FROM %s\nENTRYPOINT ["sh","-c","echo x >/probe && echo selfcheck ok: wrote","--"]\n' "$BASE_IMAGE" >img/writes-root.Dockerfile

# A new main commit, with a tree no other commit has. Prints its SHA.
commit() { # NAME
  printf '%s\n' "$1" >state.txt
  git add -A
  git commit -q -m "$1"
  git rev-parse HEAD
}
publish() { bash "$RELEASE_DIR/publish-images.sh" "$REPO" >"$WORK/out" 2>&1; }
release() { bash "$RELEASE_DIR/release.sh" "$REPO" >"$WORK/out" 2>&1; }
show_out() { sed 's/^/> /' "$WORK/out"; }
# The commit a remote tag names, peeled.
remote_tag() { git ls-remote --tags origin "refs/tags/$1" | awk 'END { print $1 }'; }
# The digest of REF, or an error: two unreadable digests are two empty strings, which compare equal.
digest_of() { # REF
  local digest
  digest=$(image_digest "$1") && [[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "can't read the digest of $1" >&2; return 1; }
  printf '%s\n' "$digest"
}
# The release VERSION is commit SHA: the git tag names it, and each image's VERSION tag has the digest
# of its tree tag (the top-level digest: the retag must not wrap the image in a new index).
is_release() { # VERSION SHA
  local tree suffix version_digest tree_digest
  [[ $(remote_tag "$1") == "$2" ]] || { echo "git tag $1 is on '$(remote_tag "$1")', want $2"; return 1; }
  tree=$(tree_tag "$2")
  for suffix in "${SUFFIXES[@]}"; do
    version_digest=$(digest_of "$REPO$suffix:$1") || return 1
    tree_digest=$(digest_of "$REPO$suffix:$tree") || return 1
    [[ $version_digest == "$tree_digest" ]] || { echo "$REPO$suffix:$1 isn't the image of tree $tree"; return 1; }
  done
}
no_version() { # VERSION
  local suffix status
  [[ -z $(remote_tag "$1") ]] || { echo "git tag $1 exists"; return 1; }
  for suffix in "${SUFFIXES[@]}"; do
    status=0
    image_exists "$REPO$suffix:$1" || status=$?
    ((status == 1)) || { echo "$REPO$suffix:$1: exists or unreadable ($status)"; return 1; }
  done
}

# A docker on PATH that fails `imagetools create` for one tag (FAIL_RETAG), fails every `imagetools
# inspect` as a network error would (FAIL_INSPECT=1), logs every call to SHIM_LOG, and passes the rest on.
mkdir "$WORK/shim"
real_docker=$(command -v docker)
cat >"$WORK/shim/docker" <<EOF
#!/usr/bin/env bash
printf 'docker %s\\n' "\$*" >>"\${SHIM_LOG:-/dev/null}"
if [[ \$1 == buildx && \$2 == imagetools && \$3 == create && " \$* " == *" --tag \${FAIL_RETAG:-} "* ]]; then
  echo "shim: refusing to tag \$FAIL_RETAG" >&2
  exit 1
fi
if [[ \$1 == buildx && \$2 == imagetools && \$3 == inspect && \${FAIL_INSPECT:-} == 1 ]]; then
  echo "ERROR: failed to do request: Head \"https://registry.invalid/v2/\": dial tcp: connection refused" >&2
  exit 1
fi
exec "$real_docker" "\$@"
EOF
chmod +x "$WORK/shim/docker"

test_publish_skips_existing() {
  local sha tree before after
  sha=$(commit publish-1)
  tree=$(tree_tag "$sha")
  # An image under the first image's tree tag that publish would not build: it must survive as it is.
  printf 'FROM scratch\nCOPY state.txt /state.txt\nLABEL image=pushed-earlier\n' >"$WORK/early.Dockerfile"
  docker buildx build -q --push -t "$REPO:$tree" -f "$WORK/early.Dockerfile" . >/dev/null || return 1
  before=$(digest_of "$REPO:$tree") || return 1
  : >"$WORK/shim.log"
  PATH=$WORK/shim:$PATH SHIM_LOG=$WORK/shim.log publish || { show_out; return 1; }
  grep -q "skip $REPO:$tree" "$WORK/out" || { show_out; echo "the first image wasn't skipped"; return 1; }
  [[ $(grep -c '^docker buildx build ' "$WORK/shim.log") == 1 ]] || { cat "$WORK/shim.log"; echo "want one build, the second image's"; return 1; }
  after=$(digest_of "$REPO:$tree") || return 1
  [[ $after == "$before" ]] || { echo "the existing image was replaced"; return 1; }
}

test_publish_builds_gates_and_pushes_missing() {
  local tree
  tree=$(tree_tag HEAD)
  grep -q "build $REPO-second:$tree" "$WORK/out" || { show_out; echo "-second wasn't built"; return 1; }
  image_exists "$REPO-second:$tree" || { echo "$REPO-second:$tree missing"; return 1; }
  # The gate ran on the loaded build, with the deploy role's flags, before the push.
  local run push
  run=$(grep -n "^docker run --rm --network=none --read-only --tmpfs /tmp --cap-drop=all --security-opt=no-new-privileges $REPO-second:$tree --selfcheck\$" "$WORK/shim.log" | cut -d: -f1)
  push=$(grep -n "^docker push $REPO-second:$tree\$" "$WORK/shim.log" | cut -d: -f1)
  [[ -n $run && -n $push ]] && ((run < push)) || { cat "$WORK/shim.log"; echo "the gate didn't run before the push"; return 1; }
  grep -q "selfcheck ok: fixture" "$WORK/out" || { show_out; echo "the gate's output isn't shown"; return 1; }
  # A second run builds and pushes nothing.
  : >"$WORK/shim.log"
  PATH=$WORK/shim:$PATH SHIM_LOG=$WORK/shim.log publish || { show_out; return 1; }
  [[ $(grep -c "skip $REPO" "$WORK/out") == 2 ]] || { show_out; echo "a second run built again"; return 1; }
  ! grep -qE '^docker (buildx build|run|push) ' "$WORK/shim.log" || { cat "$WORK/shim.log"; echo "a second run built, ran or pushed"; return 1; }
}

# An existence check that fails for another reason than "not found" must stop publish, never build
# over a tag that may be there.
test_publish_stops_when_registry_unreadable() {
  local tree suffix before=() i digest
  tree=$(tree_tag HEAD)
  for suffix in "${SUFFIXES[@]}"; do
    digest=$(digest_of "$REPO$suffix:$tree") || return 1
    before+=("$digest")
  done
  : >"$WORK/shim.log"
  PATH=$WORK/shim:$PATH FAIL_INSPECT=1 SHIM_LOG=$WORK/shim.log publish && { show_out; echo "publish succeeded blind"; return 1; }
  grep -q "can't tell whether the registry has" "$WORK/out" || { show_out; echo "no reason given"; return 1; }
  ! grep -qE '^docker (buildx build|run|push) ' "$WORK/shim.log" || { cat "$WORK/shim.log"; echo "built while the registry was unreadable"; return 1; }
  for i in "${!SUFFIXES[@]}"; do
    digest=$(digest_of "$REPO${SUFFIXES[i]}:$tree") || return 1
    [[ $digest == "${before[i]}" ]] || { echo "${SUFFIXES[i]} changed"; return 1; }
  done
  # Once the registry answers again, every image is skipped.
  : >"$WORK/shim.log"
  PATH=$WORK/shim:$PATH SHIM_LOG=$WORK/shim.log publish || { show_out; return 1; }
  [[ $(grep -c "skip $REPO" "$WORK/out") == 2 ]] || { show_out; echo "not all skipped"; return 1; }
  ! grep -qE '^docker (buildx build|run|push) ' "$WORK/shim.log" || { cat "$WORK/shim.log"; echo "built after the registry came back"; return 1; }
}

# The gate decides before the push: an image that fails it gets no tag, whatever the reason, and the
# next run (with the image fixed) builds and pushes it.
test_gate_refuses_before_the_push() {
  local sha tree name want
  sha=$(commit gate)
  tree=$(tree_tag "$sha")
  for name in two-lines:"did not print exactly one" fails:"--selfcheck failed" no-prefix:"did not print exactly one" \
    writes-root:"--selfcheck failed"; do
    want=${name#*:}
    name=${name%%:*}
    printf '|.|img/%s.Dockerfile|\n' "$name" >"$WORK/gate-images.txt"
    RELEASE_IMAGES_FILE=$WORK/gate-images.txt publish && { show_out; echo "$name: published an image the gate refuses"; return 1; }
    grep -q -- "$want" "$WORK/out" || { show_out; echo "$name: no reason from the gate"; return 1; }
    local status=0
    image_exists "$REPO:$tree" || status=$?
    ((status == 1)) || { echo "$name: $REPO:$tree exists or is unreadable ($status) after a refused gate"; return 1; }
  done
  publish || { show_out; return 1; }
  image_exists "$REPO:$tree" && image_exists "$REPO-second:$tree" || { show_out; echo "the fixed images weren't pushed"; return 1; }
}

test_release_refuses_missing_image() {
  local tree
  commit missing-image >/dev/null
  tree=$(tree_tag HEAD)
  RELEASE_IMAGES_FILE=$WORK/one-image.txt publish || { show_out; return 1; }
  release && { show_out; echo "released without the second image"; return 1; }
  grep -q "$REPO-second:$tree" "$WORK/out" || { show_out; echo "missing image not named"; return 1; }
  grep -q "require branches to be up to date" "$WORK/out" || { show_out; echo "no fix in the message"; return 1; }
  grep -q "$REPO:$tree" "$WORK/out" && { show_out; echo "named an image that exists"; return 1; }
  no_version 1.0.0
}

test_release_refuses_when_nothing_was_published() {
  local tree
  commit never-published >/dev/null
  tree=$(tree_tag HEAD)
  RELEASE_IMAGES_FILE=$WORK/one-image.txt release && { show_out; echo "released an unpublished tree"; return 1; }
  grep -q "$REPO:$tree" "$WORK/out" || { show_out; echo "missing image not named"; return 1; }
  no_version 1.0.0
}

test_release_refuses_unreadable_registry() {
  local status=0
  REPO=127.0.0.1:1/tgaud release && { show_out; echo "released against a closed port"; return 1; }
  grep -q "can't read" "$WORK/out" || { show_out; echo "an unreadable registry read as missing"; return 1; }
  bash "$RELEASE_DIR/image-exists.sh" 127.0.0.1:1/tgaud:x 2>/dev/null || status=$?
  ((status == 2)) || { echo "image-exists.sh: exit $status on a closed port, want 2"; return 1; }
}

# Three main commits, released one after the other, as the queue runs them. The trees were pushed by
# publish (the gated build), and is_release asserts that each version tag has the top-level digest of
# its tree tag: without --prefer-index=false, `imagetools create` wraps a single manifest in a new index
# and the digest changes. Release only reads and retags by manifest: no build, no pull, no push, no run,
# and nothing lands in the local image store.
test_three_releases() {
  local name sha shas=() versions=(1.0.0 1.0.1 1.0.2) i
  # The tree from the missing-image test gets its second image now, and is the first release.
  publish || { show_out; return 1; }
  : >"$WORK/shim.log"
  for name in second third; do
    PATH=$WORK/shim:$PATH SHIM_LOG=$WORK/shim.log release || { show_out; return 1; }
    shas+=("$(git rev-parse HEAD)")
    commit "$name" >/dev/null
    publish || { show_out; return 1; }
  done
  PATH=$WORK/shim:$PATH SHIM_LOG=$WORK/shim.log release || { show_out; return 1; }
  shas+=("$(git rev-parse HEAD)")
  for i in 0 1 2; do
    is_release "${versions[i]}" "${shas[i]}" || return 1
  done
  grep -q '^docker buildx imagetools create --prefer-index=false --tag ' "$WORK/shim.log" || { cat "$WORK/shim.log"; echo "no manifest-level retag"; return 1; }
  ! grep -vE '^docker buildx imagetools (inspect|create) ' "$WORK/shim.log" || { echo "release called docker for more than manifests (above)"; return 1; }
  for i in 0 1 2; do
    docker image inspect "$REPO:${versions[i]}" >/dev/null 2>&1 && { echo "$REPO:${versions[i]} was pulled"; return 1; }
  done
  return 0
}

test_rerun_reuses_version() {
  local before
  before=$(git ls-remote --tags origin)
  release || { show_out; return 1; }
  grep -q "already has 1.0.2" "$WORK/out" || { show_out; echo "didn't reuse 1.0.2"; return 1; }
  [[ $(git ls-remote --tags origin) == "$before" ]] || { echo "the remote tags changed"; return 1; }
  is_release 1.0.2 "$(git rev-parse HEAD)"
}

# The git tag push fails after the retag (origin refuses it): the version exists as an image tag only,
# which is a stopped run's leftover, and the retry at once takes the same number.
test_interrupted_release_retried_at_once() {
  local c
  c=$(commit interrupted)
  publish || { show_out; return 1; }
  touch "$WORK/reject-pushes"
  release && { rm -f "$WORK/reject-pushes"; show_out; echo "the release didn't stop on the refused tag push"; return 1; }
  rm -f "$WORK/reject-pushes"
  image_exists "$REPO:1.0.3" && image_exists "$REPO-second:1.0.3" || { echo "the images weren't tagged 1.0.3 before the stop"; return 1; }
  [[ -z $(remote_tag 1.0.3) ]] || { echo "a git tag exists after the refused push"; return 1; }
  release || { show_out; return 1; }
  grep -q "next is 1.0.3" "$WORK/out" || { show_out; echo "the retry didn't take 1.0.3 again"; return 1; }
  is_release 1.0.3 "$c"
}

# A (older) stops after its retag (the tag push is refused), B (newer) releases and takes A's number, then
# A is retried twice: it gets the next one and B's release stays intact.
test_interrupted_release_retried_after_newer() {
  local a b
  a=$(commit older-a)
  publish || { show_out; return 1; }
  b=$(commit newer-b)
  publish || { show_out; return 1; }

  git checkout -q --detach "$a"
  touch "$WORK/reject-pushes"
  release && { rm -f "$WORK/reject-pushes"; show_out; echo "A didn't stop"; return 1; }
  rm -f "$WORK/reject-pushes"
  image_exists "$REPO:1.0.4" || { echo "A's image wasn't tagged 1.0.4 before the stop"; return 1; }
  [[ -z $(remote_tag 1.0.4) ]] || { echo "git tag pushed after a refused push"; return 1; }

  git checkout -q --detach "$b"
  release || { show_out; return 1; }
  is_release 1.0.4 "$b" || return 1

  git checkout -q --detach "$a"
  release || { show_out; return 1; }
  is_release 1.0.5 "$a" || return 1
  is_release 1.0.4 "$b" || return 1

  release || { show_out; return 1; }
  grep -q "already has 1.0.5" "$WORK/out" || { show_out; echo "the second retry didn't reuse 1.0.5"; return 1; }
  is_release 1.0.5 "$a" || return 1
  is_release 1.0.4 "$b" || return 1
  no_version 1.0.6
  git checkout -q main
}

# The retag itself stops between two images: the first has the version, the git tag is not pushed, and the
# retry finishes the release under the same number.
test_release_stopped_between_two_retags() {
  local d
  d=$(commit half-tagged)
  publish || { show_out; return 1; }
  PATH=$WORK/shim:$PATH FAIL_RETAG=$REPO-second:1.0.6 release && { show_out; echo "the release didn't stop"; return 1; }
  image_exists "$REPO:1.0.6" || { echo "the first image wasn't tagged before the stop"; return 1; }
  [[ -z $(remote_tag 1.0.6) ]] || { echo "git tag pushed after a failed retag"; return 1; }
  release || { show_out; return 1; }
  is_release 1.0.6 "$d"
}

# A hand-made annotated X.Y.Z on HEAD: the re-run reuses it, tags the images, leaves the git tag alone.
test_rerun_with_annotated_tag() {
  local head before
  head=$(commit annotated)
  publish || { show_out; return 1; }
  git -c user.name=t -c user.email=t@example.invalid -c tag.gpgsign=false tag -a 7.0.0 -m "made by hand" "$head"
  git push -q origin refs/tags/7.0.0
  before=$(git ls-remote --tags origin)
  release || { show_out; return 1; }
  grep -q "already has 7.0.0" "$WORK/out" || { show_out; echo "didn't reuse 7.0.0"; return 1; }
  [[ $(git ls-remote --tags origin) == "$before" ]] || { echo "the remote tags changed"; return 1; }
  image_exists "$REPO:7.0.0" || { echo "the images weren't tagged 7.0.0"; return 1; }
}

# next-version.sh on its own repo: the order of the tags is numeric, other names don't count.
test_version_order() {
  local dir=$WORK/versions sha out
  git init -q --bare "$dir/origin.git"
  git init -q -b main "$dir/repo"
  (
    cd "$dir/repo" || exit 1
    git -c user.name=t -c user.email=t@example.invalid -c commit.gpgsign=false commit -q --allow-empty -m one
    git remote add origin "$dir/origin.git"
    out=$(bash "$RELEASE_DIR/next-version.sh" 2>/dev/null) && [[ $out == 1.0.0 ]] || { echo "no tags: '$out'"; exit 1; }
    for tag in 1.9.0 1.10.0 1.2.99 v2.0.0 2.0 foo 3.0.0-rc1; do
      git push -q origin "HEAD:refs/tags/$tag"
    done
    git -c user.name=t -c user.email=t@example.invalid -c commit.gpgsign=false commit -q --allow-empty -m two
    out=$(bash "$RELEASE_DIR/next-version.sh" 2>/dev/null) && [[ $out == 1.10.1 ]] || { echo "1.9.0 and 1.10.0: '$out'"; exit 1; }
    # An annotated tag on HEAD is found through its peeled line.
    git -c user.name=t -c user.email=t@example.invalid -c tag.gpgsign=false tag -a -m rel 1.10.1
    git push -q origin refs/tags/1.10.1
    out=$(bash "$RELEASE_DIR/next-version.sh" 2>/dev/null) && [[ $out == 1.10.1 ]] || { echo "annotated on HEAD: '$out'"; exit 1; }
    sha=$(git rev-parse HEAD)
    git -c user.name=t -c user.email=t@example.invalid -c commit.gpgsign=false commit -q --allow-empty -m three
    out=$(bash "$RELEASE_DIR/next-version.sh" 2>/dev/null) && [[ $out == 1.10.2 ]] || { echo "after annotated $sha: '$out'"; exit 1; }
  )
}

# The tree a PR run tags (GitHub's merge ref) is the tree main gets from a rebase merge, once the
# branch is up to date with main (here: GitHub's "Update branch", a merge of main into it).
test_merge_ref_tree_is_rebase_tree() {
  local dir=$WORK/merge-ref merge_ref rebased
  git init -q -b main "$dir"
  (
    cd "$dir" || exit 1
    git config user.name t; git config user.email t@example.invalid; git config commit.gpgsign false
    printf 'a\nb\nc\n' >f; git add f; git commit -q -m base
    git checkout -q -b pr
    printf 'a\nB\nc\n' >f; git commit -q -am "pr change"
    printf 'x\n' >g; git add g; git commit -q -m "pr file"
    git checkout -q main
    printf 'm\n' >h; git add h; git commit -q -m "main moves"
    git checkout -q pr
    git merge -q --no-edit main
    git checkout -q --detach main
    git merge -q --no-ff --no-edit pr
    merge_ref=$(git rev-parse 'HEAD^{tree}')
    git checkout -q pr
    git rebase -q main
    rebased=$(git rev-parse 'HEAD^{tree}')
    [[ $merge_ref == "$rebased" ]] || { echo "merge ref tree $merge_ref, rebase merge tree $rebased"; exit 1; }
  )
}

# BuildKit reads .dockerignore, podman .containerignore. The repo's buildx build context must lack what
# .containerignore keeps out: the same list (.dockerignore is a link to it), and the context BuildKit
# actually sends holds the sources and none of the secrets, state, tests or docs.
test_buildx_context_is_the_podman_context() {
  local out=$WORK/context found
  cmp -s "$REAL_REPO/.dockerignore" "$REAL_REPO/.containerignore" || { echo ".dockerignore differs from .containerignore"; return 1; }
  printf 'FROM scratch\nCOPY . /ctx\n' | docker buildx build -q -f - --output "type=local,dest=$out" "$REAL_REPO" >/dev/null || return 1
  [[ -f $out/ctx/Containerfile && -f $out/ctx/pyproject.toml && -f $out/ctx/uv.lock && -f $out/ctx/src/tgaud/cli.py ]] ||
    { echo "the build context lacks the sources"; return 1; }
  found=$(cd "$out/ctx" && find . \( -path ./tests -o -path ./docs -o -path ./.git -o -name .venv -o -name .env -o -name '.env.*' \
    -o -name '*.env' -o -name '*.session' -o -name '*.db' -o -name '*.csv' -o -name __pycache__ \))
  [[ -z $found ]] || { echo "the build context holds what .containerignore excludes:"; echo "$found"; return 1; }
}

check "publish skips an image whose tree tag exists" test_publish_skips_existing
check "publish builds the missing image, gates the build with the deploy flags, then pushes it; nothing on a second run" test_publish_builds_gates_and_pushes_missing
check "publish stops, building nothing, when the registry can't say whether an image exists" test_publish_stops_when_registry_unreadable
check "an image that fails the gate (two lines, failure, wrong prefix, write to the root) gets no tag" test_gate_refuses_before_the_push
check "release stops on a missing image, names it, and tags nothing" test_release_refuses_missing_image
check "release stops, naming the image, when no run published the tree" test_release_refuses_when_nothing_was_published
check "release stops when the registry can't be read, rather than calling it missing" test_release_refuses_unreadable_registry
check "three main commits released in turn get 1.0.0, 1.0.1 and 1.0.2, by manifest, with the digest of the tree tag" test_three_releases
check "a re-run on a released commit reuses its version and pushes nothing" test_rerun_reuses_version
check "a release whose git tag push fails after the retag, retried at once, takes the same number" test_interrupted_release_retried_at_once
check "an interrupted release retried after a newer commit released its number gets the next patch and leaves that release intact" test_interrupted_release_retried_after_newer
check "a release stopped between two retags has no git tag, and its retry finishes it under the same number" test_release_stopped_between_two_retags
check "a re-run on a commit with a hand-made annotated tag reuses it and pushes nothing" test_rerun_with_annotated_tag
check "versions: 1.0.0 first, numeric order, only plain X.Y.Z, annotated tags" test_version_order
check "a PR's merge ref has the tree of its rebase merge once it is up to date" test_merge_ref_tree_is_rebase_tree
check "the buildx build context is the podman build context (.dockerignore is .containerignore)" test_buildx_context_is_the_podman_context

if ((failures > 0)); then
  printf '%d check(s) failed\n' "$failures"
  exit 1
fi
echo "all checks passed"
