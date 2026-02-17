#!/usr/bin/env bash
# build_and_push.sh
# Build a mozaic-daily Docker image locally or via remote buildx and (for remote) push it.
# Usage: ./build_and_push.sh --local|--remote -v <version> [--no-cache]

set -e
set -u
set -o pipefail

### --- Config ---------------------------------------------------------------

DOCKERFILE="Dockerfile"
IMAGE_BASE="mozaic-daily"
REMOTE_USERNAME="brwells78094"
BUILD_CONTEXT="../"  # Build context is parent directory (project root)

### --- Helpers --------------------------------------------------------------

usage() {
  cat <<'EOF'
Usage:
  ./build_and_push.sh --local|--remote -v <version> [--no-cache]

Options:
  --local             Build for arm64 on your machine
  --remote            Build for amd64 with buildx and push to registry
  -v, --version <ver> Version number without 'v' prefix (e.g., 1.2.3) [required]
  --no-cache          Pass --no-cache to docker build / buildx build
  -h, --help          Show this help

Examples:
  ./build_and_push.sh --local -v 1.2.3
  ./build_and_push.sh --remote -v 1.2.3 --no-cache
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

info() {
  echo "[info] $*"
}

### --- Argument parsing -----------------------------------------------------

MODE=""
VERSION=""
NO_CACHE_FLAG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --local)
      MODE="local"
      shift
      ;;
    --remote)
      MODE="remote"
      shift
      ;;
    -v|--version)
      if [[ $# -lt 2 ]]; then
        die "Option -v/--version requires an argument"
      fi
      VERSION="$2"
      shift 2
      ;;
    --no-cache)
      NO_CACHE_FLAG="--no-cache"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

### --- Argument validation --------------------------------------------------

if [[ -z "$MODE" ]]; then
  echo "Error: Must specify --local or --remote" >&2
  echo "" >&2
  usage
  exit 1
fi

if [[ -z "$VERSION" ]]; then
  echo "Error: Version is required (use -v or --version)" >&2
  echo "" >&2
  usage
  exit 1
fi

if [[ "$VERSION" == v* ]]; then
  die "Version should not include 'v' prefix (got: '$VERSION'). Use '${VERSION#v}' instead."
fi

[[ -f "$DOCKERFILE" ]] || die "Dockerfile '$DOCKERFILE' not found in current directory."

command -v docker >/dev/null 2>&1 || die "'docker' is not installed or not on PATH."

### --- Tag & image naming ---------------------------------------------------

case "$MODE" in
  remote) SUFFIX="_amd64" ;;
  local)  SUFFIX="_arm64" ;;
esac

TAG="v${VERSION}${SUFFIX}"
IMAGE_NAME="${IMAGE_BASE}:${TAG}"

info "Build mode:      $MODE"
info "Base image name: $IMAGE_BASE"
info "Version:         $VERSION"
info "Computed tag:    $TAG"
info "Dockerfile:      $DOCKERFILE"
[[ -n "$NO_CACHE_FLAG" ]] && info "No-cache:        enabled"

### --- Build & (for remote) push -------------------------------------------

if [[ "$MODE" == "local" ]]; then
  info "Performing local build (arm64)."
  info "Running: docker build $NO_CACHE_FLAG -t \"$IMAGE_NAME\" -f \"$DOCKERFILE\" \"$BUILD_CONTEXT\""
  docker build $NO_CACHE_FLAG -t "$IMAGE_NAME" -f "$DOCKERFILE" "$BUILD_CONTEXT"
  info "Local build complete: $IMAGE_NAME"
else
  # Remote build/push using buildx (amd64)
  if ! docker buildx version >/dev/null 2>&1; then
    die "'docker buildx' is not available. Install/enable buildx to proceed."
  fi

  FULL_REMOTE_REF="${REMOTE_USERNAME}/${IMAGE_NAME}"
  info "Performing remote buildx build (amd64) and pushing."
  info "Running: docker buildx build $NO_CACHE_FLAG --platform=linux/amd64 -t \"$FULL_REMOTE_REF\" -f \"$DOCKERFILE\" --push \"$BUILD_CONTEXT\""
  docker buildx build $NO_CACHE_FLAG \
    --platform=linux/amd64 \
    -t "$FULL_REMOTE_REF" \
    -f "$DOCKERFILE" \
    --push "$BUILD_CONTEXT"
  info "Remote build and push complete: $FULL_REMOTE_REF"
fi
