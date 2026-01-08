#!/usr/bin/env bash
# Test local Docker image build and functionality
set -e

VERSION=${1:-"v0.0.1"}
IMAGE_NAME="mozaic-daily:${VERSION}_refactor_arm64"

echo "=============================================="
echo "Testing Local Docker Image (Refactor)"
echo "=============================================="
echo ""

# Build
echo "[1/3] Building image..."
./build_and_push.sh local "${VERSION}_refactor"
echo "✓ Build complete"
echo ""

# Test imports
echo "[2/3] Testing imports..."
docker run --rm "$IMAGE_NAME" python -c "from mozaic_daily import main; print('✓ Imports work')"
echo ""

# Run full test (requires BigQuery credentials or will fail gracefully)
echo "[3/3] Running full test..."
echo "Note: This will fail if BigQuery credentials are not available"
docker run --rm \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  "$IMAGE_NAME" \
  python /test_docker.py || echo "⚠ Test failed (expected if no BigQuery access)"

echo ""
echo "=============================================="
echo "Local Docker test complete"
echo "=============================================="
