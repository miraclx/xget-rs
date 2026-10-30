#!/usr/bin/env bash
# Bring the harness up and seed MinIO. Replaces any ad-hoc `xget-minio` container (it wants port 9000).
set -euo pipefail
cd "$(dirname "$0")"

docker rm -f xget-minio >/dev/null 2>&1 || true
docker compose up -d

echo "waiting for services..."
until curl -sf http://localhost:8080/status/200 >/dev/null 2>&1; do sleep 1; done
echo "  httpbin ready"
until curl -sf http://localhost:9000/minio/health/ready >/dev/null 2>&1; do sleep 1; done
echo "  minio ready"
until curl -sf http://localhost:8474/version >/dev/null 2>&1; do sleep 1; done
echo "  toxiproxy ready"

export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 AWS_S3_ADDRESSING_STYLE=path
aws --endpoint-url http://localhost:9000 s3 mb s3://demo >/dev/null 2>&1 || true

# A 64 MiB object, stored WITH a SHA-256 (for stored-checksum auto-verify), plus a sidecar and a public
# read policy (for anonymous and sidecar tests).
obj=/tmp/xget-harness-obj.bin
head -c 67108864 /dev/urandom >"$obj"
aws --endpoint-url http://localhost:9000 s3api put-object \
  --bucket demo --key obj.bin --body "$obj" --checksum-algorithm SHA256 >/dev/null
printf '%s  obj.bin\n' "$(shasum -a 256 "$obj" | cut -d' ' -f1)" >"$obj.sha256sum"
aws --endpoint-url http://localhost:9000 s3 cp "$obj.sha256sum" s3://demo/obj.bin.sha256sum --quiet >/dev/null
printf '%s' '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::demo/*"]}]}' \
  | aws --endpoint-url http://localhost:9000 s3api put-bucket-policy --bucket demo --policy file:///dev/stdin >/dev/null

cat <<'EOF'

harness up. endpoints:
  httpbin (direct)      http://localhost:8080        e.g. /range/104857600  /drip  /delay/5
  httpbin (via toxi)    http://localhost:8666        same paths, faults injectable
  minio  (direct)       http://localhost:9000        bucket: demo, object: obj.bin (+ .sha256sum)
  minio  (via toxi)     http://localhost:8667
  toxiproxy control     http://localhost:8474

next: ./scenarios.sh        (runs the suite)     ./down.sh   (tear everything down)
EOF
