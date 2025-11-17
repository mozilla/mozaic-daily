#!/usr/bin/env zsh
# build_and_push.sh
# Usage: ./build_and_push.sh v0.0.1

set -euo pipefail

# --- config ---
IMAGE_NAME="mozaic-daily"
DOCKERFILE="Dockerfile-mozaic-daily"
DOCKER_USER="brwells78094"

# --- input check ---
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <tag>"
  exit 1
fi
TAG="$1"

# --- build image ---
echo "🛠  Building image..."
docker build -t "${IMAGE_NAME}:${TAG}" -f "${DOCKERFILE}" .

# # --- get latest image ID ---
# IMAGE_ID=$(docker images --format '{{.ID}} {{.Repository}} {{.Tag}}' \
#   | grep "${IMAGE_NAME}" \
#   | sort -u \
#   | head -n 1 \
#   | awk '{print $1}')

# if [[ -z "$IMAGE_ID" ]]; then
#   echo "❌ Could not find image ID for ${IMAGE_NAME}"
#   exit 1
# fi

# --- tag and push ---
# REMOTE_TAG="${DOCKER_USER}/${IMAGE_NAME}:${TAG}"
REMOTE_TAG="${DOCKER_USER}/${IMAGE_NAME}:${TAG}" 
echo "🏷  Tagging ${IMAGE_NAME}:${TAG} -> ${REMOTE_TAG}"
docker tag "${IMAGE_NAME}:${TAG}" "${REMOTE_TAG}"

echo "🚀 Pushing ${REMOTE_TAG} to Docker Hub..."
docker push "$REMOTE_TAG"

echo "Done!"

