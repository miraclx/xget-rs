#!/usr/bin/env bash
# Tear the harness down.
set -euo pipefail
cd "$(dirname "$0")"
docker compose down
rm -f /tmp/xget-harness-obj.bin /tmp/xget-harness-obj.bin.sha256sum
echo "harness down."
