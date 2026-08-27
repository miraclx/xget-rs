#!/usr/bin/env bash
# Exercise xget against the harness: parallel range download, throttling, connection resets, inactivity
# timeout, resume, mirror failover, and every S3 path. Assumes ./up.sh has run.
set -uo pipefail
cd "$(dirname "$0")"

echo "building xget (release, s3)..."
(cd .. && cargo build --release --features s3 --quiet)
XGET=../target/release/xget

HTTP=http://localhost:8080        # httpbin, direct
TOXI=http://localhost:8666        # httpbin, via toxiproxy
S3=http://localhost:9000          # minio, direct
SIZE=33554432                     # 32 MiB test body
RANGE="range/$SIZE"
fails=0

toxi_reset()  { curl -sf -XPOST localhost:8474/reset >/dev/null; }
toxi_add()    { curl -sf -XPOST "localhost:8474/proxies/$1/toxics" -d "$2" >/dev/null; }
proxy_set()   { curl -sf -XPOST "localhost:8474/proxies/$1" -d "{\"enabled\":$2}" >/dev/null; }
sha()         { shasum -a 256 "$1" | cut -d' ' -f1; }
check()       { if [ "$2" = "$3" ]; then echo "  PASS  $1"; else echo "  FAIL  $1 (want ${2:0:12} got ${3:0:12})"; fails=$((fails+1)); fi; }
ok()          { if [ "$1" -eq 0 ]; then echo "  PASS  $2"; else echo "  FAIL  $2 (exit $1)"; fails=$((fails+1)); fi; }

toxi_reset

echo "== reference: a clean parallel download =="
$XGET "$HTTP/$RANGE" /tmp/h-ref.bin -f -n 8 --progress none >/dev/null 2>&1
REF=$(sha /tmp/h-ref.bin)
echo "  reference sha256 ${REF:0:16}...  ($SIZE bytes, 8 chunks)"

echo "== throttled to 4 MB/s via toxiproxy (bandwidth) =="
toxi_add httpbin '{"type":"bandwidth","attributes":{"rate":4096},"stream":"downstream"}'
$XGET "$TOXI/$RANGE" /tmp/h-slow.bin -f -n 8 --progress none >/dev/null 2>&1
check "throttled download matches reference" "$REF" "$(sha /tmp/h-slow.bin)"
toxi_reset

echo "== connections dropped every 4MB (limit_data), should resume and complete, not storm =="
toxi_add httpbin '{"type":"limit_data","attributes":{"bytes":4000000},"stream":"downstream"}'
$XGET "$TOXI/$RANGE" /tmp/h-reset.bin -f -n 5 -t 40 --progress none >/dev/null 2>&1
rc=$?
toxi_reset
if [ $rc -eq 0 ]; then check "survived drops, correct bytes" "$REF" "$(sha /tmp/h-reset.bin)"; else ok $rc "survived drops"; fi

echo "== inactivity timeout (data stalls) should fail fast with -t 0 =="
toxi_add httpbin '{"type":"timeout","attributes":{"timeout":0},"stream":"downstream"}'
timeout 30 $XGET "$TOXI/$RANGE" /tmp/h-to.bin -f --timeout 2 -t 0 --progress none >/dev/null 2>&1
rc=$?
toxi_reset
if [ $rc -ne 0 ]; then echo "  PASS  timed out as expected (exit $rc)"; else echo "  FAIL  expected a timeout failure"; fails=$((fails+1)); fi

echo "== resume across runs (-c): kill a live download, then continue =="
rm -f /tmp/h-cont.bin*
toxi_add httpbin '{"type":"bandwidth","attributes":{"rate":300},"stream":"downstream"}'
$XGET "$TOXI/$RANGE" /tmp/h-cont.bin -f -n 8 --progress none >/dev/null 2>&1 &
pid=$!
sleep 3
kill -9 "$pid" 2>/dev/null
wait 2>/dev/null
toxi_reset
if [ -f /tmp/h-cont.bin.part ] && [ -f /tmp/h-cont.bin.part.st ]; then
  echo "  PASS  interrupt left a .part and control file"
else
  echo "  FAIL  interrupt left no resumable state"; fails=$((fails+1))
fi
$XGET "$HTTP/$RANGE" /tmp/h-cont.bin -c -n 8 --progress none >/dev/null 2>&1
ok $? "resume completed"
check "resumed bytes match reference" "$REF" "$(sha /tmp/h-cont.bin)"

echo "== mirror failover: dead primary -> live mirror =="
proxy_set httpbin false                                     # primary (via toxi) is down
$XGET "$TOXI/$RANGE" /tmp/h-mir.bin -f -n 5 --mirror "$HTTP/$RANGE" --progress none >/dev/null 2>&1
ok $? "failed over to mirror"
proxy_set httpbin true
check "mirror bytes match reference" "$REF" "$(sha /tmp/h-mir.bin)"

echo "== S3: signed, anonymous, stored-checksum auto-verify, s3:// sidecar =="
S3SUM=$(AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \
  aws --endpoint-url $S3 s3 cp s3://demo/obj.bin - 2>/dev/null | shasum -a 256 | cut -d' ' -f1)
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \
  $XGET s3://demo/obj.bin /tmp/s-signed.bin -f --endpoint-url $S3 --progress none >/dev/null 2>&1
ok $? "s3 signed"
check "s3 signed bytes" "$S3SUM" "$(sha /tmp/s-signed.bin)"
AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true \
  $XGET s3://demo/obj.bin /tmp/s-anon.bin -f --endpoint-url $S3 --progress none >/dev/null 2>&1
ok $? "s3 anonymous (public bucket)"
# Capture rather than pipe: `grep -q` would close the pipe early and, under pipefail, mask the result.
stored=$(AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true \
  $XGET s3://demo/obj.bin /tmp/s-stored.bin -f --endpoint-url $S3 --progress none 2>&1)
case "$stored" in
  *stored*) echo "  PASS  s3 stored-checksum auto-verify (note printed)" ;;
  *) echo "  FAIL  stored-checksum note"; fails=$((fails+1)) ;;
esac
AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true \
  $XGET s3://demo/obj.bin /tmp/s-side.bin -f --endpoint-url $S3 --expect s3://demo/obj.bin.sha256sum --progress none >/dev/null 2>&1
ok $? "s3:// sidecar --expect"

echo
if [ $fails -eq 0 ]; then echo "ALL SCENARIOS PASSED"; else echo "$fails SCENARIO(S) FAILED"; fi
rm -f /tmp/h-*.bin /tmp/h-*.part /tmp/s-*.bin
exit $fails
