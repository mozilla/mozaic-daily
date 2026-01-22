#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------
# Defaults (remote / amd64)
# ---------------------------------
MODE="remote"          # remote | local
VERSION="0.0.7"

REMOTE_PLATFORM="linux/amd64"
REMOTE_IMAGE="brwells78094/mozaic-daily"

LOCAL_PLATFORM="linux/arm64"
LOCAL_IMAGE="mozaic-daily"

CMD=("/bin/bash")

# ---------------------------------
# Helpers
# ---------------------------------
usage() {
  cat <<EOF
Usage: $0 [options] [-- command]

Common options:
  --local                 Use local (arm64) image
  --remote                Use remote (amd64) image (default)
  -v, --version <ver>     Version number (e.g. 0.0.6)

Advanced / escape hatches:
  -p, --platform <plat>   Override platform
  -i, --image <image>     Override image
  -t, --tag <tag>         Override full tag

Other:
  -h, --help              Show this help

Examples:
  $0                      # remote, v0.0.7_amd64
  $0 --local              # local,  v0.0.7_arm64
  $0 --local -v 0.0.8     # local,  v0.0.8_arm64
  $0 --remote -v 0.0.8    # remote, v0.0.8_amd64
  $0 --local -- /run_forecast.sh   # run forecast in local container
EOF
}

# ---------------------------------
# Parse args
# ---------------------------------
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
      VERSION="$2"
      shift 2
      ;;
    -p|--platform)
      PLATFORM_OVERRIDE="$2"
      shift 2
      ;;
    -i|--image)
      IMAGE_OVERRIDE="$2"
      shift 2
      ;;
    -t|--tag)
      TAG_OVERRIDE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      CMD=("$@")
      break
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

# ---------------------------------
# Resolve platform / image / tag
# ---------------------------------
if [[ "$MODE" == "local" ]]; then
  PLATFORM="$LOCAL_PLATFORM"
  IMAGE="$LOCAL_IMAGE"
  ARCH="arm64"
else
  PLATFORM="$REMOTE_PLATFORM"
  IMAGE="$REMOTE_IMAGE"
  ARCH="amd64"
fi

TAG="v${VERSION}_${ARCH}"

# Apply overrides last
PLATFORM="${PLATFORM_OVERRIDE:-$PLATFORM}"
IMAGE="${IMAGE_OVERRIDE:-$IMAGE}"
TAG="${TAG_OVERRIDE:-$TAG}"

# ---------------------------------
# Run container
# ---------------------------------
exec docker run --rm -it \
  --platform="$PLATFORM" \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  -e CLOUDSDK_CONFIG=/root/.config/gcloud \
  "${IMAGE}:${TAG}" \
  "${CMD[@]}"
