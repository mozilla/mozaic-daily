#!/usr/bin/env zsh
# build_and_push.sh
# Build a mozaic-daily Docker image locally or via remote buildx and (for remote) push it.
# Usage: ./build_and_push.sh <local|remote> <version>
#   - <local|remote>: build mode; must be exactly "local" or "remote"
#   - <version>: must start with 'v' (e.g., v0.3.1)

set -e
set -u
set -o pipefail

### --- Config ---------------------------------------------------------------

DOCKERFILE="Dockerfile-mozaic-daily"
IMAGE_BASE="mozaic-daily"
REMOTE_USERNAME="brwells78094"

### --- Helpers --------------------------------------------------------------

usage() {
  cat <<'EOF'
Usage:
  ./build_and_push.sh <local|remote> <version>

Arguments:
  local|remote   Build mode. "local" builds for arm64 on your machine.
                 "remote" builds for amd64 with buildx and pushes.
  version        Tag starting with 'v' (e.g., v1.2.3)

Examples:
  ./build_and_push.sh local v1.2.3
  ./build_and_push.sh remote v1.2.3
EOF
}

die() {
  print -u2 -- "Error: $*"
  exit 1
}

info() {
  print -- "[info] $*"
}

### --- Argument validation --------------------------------------------------

if [[ $# -ne 2 ]]; then
  usage
  exit 1
fi

MODE="$1"       # "local" or "remote"
VERSION="$2"    # must start with 'v'

if [[ "$MODE" != "local" && "$MODE" != "remote" ]]; then
  die "First argument must be 'local' or 'remote' (got: '$MODE')."
fi

if [[ ! "$VERSION" == v* ]]; then
  die "Version must start with 'v' (got: '$VERSION')."
fi

[[ -f "$DOCKERFILE" ]] || die "Dockerfile '$DOCKERFILE' not found in current directory."

command -v docker >/dev/null 2>&1 || die "'docker' is not installed or not on PATH."

### --- Tag & image naming ---------------------------------------------------

case "$MODE" in
  remote) SUFFIX="_amd64" ;;
  local)  SUFFIX="_arm64" ;;
esac

TAG="${VERSION}${SUFFIX}"
IMAGE_NAME="${IMAGE_BASE}:${TAG}"

info "Build mode:      $MODE"
info "Base image name: $IMAGE_BASE"
info "Version:         $VERSION"
info "Computed tag:    $TAG"
info "Dockerfile:      $DOCKERFILE"

### --- Build & (for remote) push -------------------------------------------

if [[ "$MODE" == "local" ]]; then
  info "Performing local build (arm64)."
  info "Running: docker build -t \"$IMAGE_NAME\" -f \"$DOCKERFILE\" ."
  docker build -t "$IMAGE_NAME" -f "$DOCKERFILE" .
  info "Local build complete: $IMAGE_NAME"
else
  # Remote build/push using buildx (amd64)
  command -v docker >/dev/null 2>&1 || die "'docker' is not installed or not on PATH."
  if ! docker buildx version >/dev/null 2>&1; then
    die "'docker buildx' is not available. Install/enable buildx to proceed."
  fi

  FULL_REMOTE_REF="${REMOTE_USERNAME}/${IMAGE_NAME}"
  info "Performing remote buildx build (amd64) and pushing."
  info "Running: docker buildx build --platform=linux/amd64 -t \"$FULL_REMOTE_REF\" -f \"$DOCKERFILE\" --push ."
  docker buildx build --platform=linux/amd64 -t "$FULL_REMOTE_REF" -f "$DOCKERFILE" --push .
  info "Remote build and push complete: $FULL_REMOTE_REF"
fi
