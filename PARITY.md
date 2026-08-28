# Feature parity with libxget-js

Honest tracking of libxget-js against xget-rs, so "done" means done. ✅ done · 🟡 partial ·
⬜ todo · ➖ intentionally different.

## Engine

| feature | status | notes |
| ------- | ------ | ----- |
| chunked parallel range GET | ✅ | `plan` + structured concurrency |
| resume a dropped chunk from offset | ✅ | validated `Content-Range`, backoff |
| scatter to a sparse file + in-order verify | ✅ | positioned writes; verifier hashes the hole-free prefix |
| range-not-honored / length gates | ✅ | typed `RangeNotHonored` / `LengthMismatch` |
| no memory buffering (no StreamCache) | ✅ | positioned writes, so parallelism is unbounded |
| single-stream fetch for non-range sources | ✅ | one stream, hashed inline |
| configurable checksum algorithm | ✅ | none/md5/sha1/sha256/sha512/blake3 |
| resume a partial FILE across runs | ✅ | auto-resumes a partial; `-c` forces, `--restart` fresh; prefix folded into the verify pass |
| start from offset (`-i, --start-pos`) | ⬜ | |
| inactivity timeout (`--timeout`) | ✅ | per-read; a ranged chunk's retry resumes it |
| pluggable sources | ✅ | `Source` seam with HTTP, S3, and IPFS impls; a bifrost peer planned |
| S3 / S3-compatible source (`s3` feature) | ✅ | signs or anonymous, workdir `.env`, `--endpoint-url`, adopts a stored checksum |
| IPFS source (`ipfs` feature) | ✅ | `ipfs://<cid>` via an HTTP gateway; a raw-bytes CID self-verifies |
| mirror / failover (`--mirror`) | ✅ | `Mirrors` source, tried in order per operation |

## CLI (`xget`)

| flag | status | notes |
| ---- | ------ | ----- |
| `-n, --chunks <N>` | ✅ | concurrent chunks |
| `-t, --tries <N>` | ✅ | numeric or `inf` |
| `-H, --header <H>` (repeatable) | ✅ | custom request headers |
| output filename inference | ✅ | Content-Disposition (RFC 5987), else URL basename |
| `-D, --directory-prefix`, `--no-directories` | ✅ | |
| `-f, --overwrite` | ✅ | refuses to clobber without `-f`; `--force-append` TODO |
| `--progress <bar\|plain\|json\|none>`, `--no-bar` | ✅ | auto-detects a TTY |
| `--raw-sizes` | ✅ | raw byte counts |
| `-s, --checksum <algo>` reporting | ✅ | none/md5/sha1/sha256/sha512/blake3 |
| `--expect <[algo:]hex\|url>` | ✅ | gate on a known checksum; inline, bare hex, or a checksum-file URL |
| `--timeout <secs>` | ✅ | inactivity timeout |
| `-c, --continue` | ✅ | force resuming a partial (a partial resumes automatically without it) |
| `--restart` | ✅ | ignore a resumable partial and download from scratch |
| `--mirror <url>` (repeatable) | ✅ | failover for the same resource |
| `--endpoint-url <url>` | ✅ | S3-compatible endpoint (with the `s3` feature) |
| `--ipfs-gateway <url>` | ✅ | HTTP gateway for `ipfs://` (with the `ipfs` feature) |
| `--start-pos` | ⬜ | `-c` covers the real resume case |

## Progress

| feature | status | notes |
| ------- | ------ | ----- |
| live segmented bar, one segment per chunk | ✅ | xprogress |
| human size readout (done / total) | ✅ | xbytes |
| speed and ETA | ✅ | rolling readout |
| per-chunk detail / labels | ⬜ | |
| plain / json output modes | ✅ | `--progress plain\|json`, throttled |

## Deliberately different (➖)

- No `StreamCache` / `--cache-size` / `--show-cache`: chunks are written straight to their offsets in a
  sparse file, so nothing is buffered in memory and there is no cache to size. Verification is a single
  in-order read-back of the file (giving up the JS "never touch the disk twice" for unbounded
  parallelism and connections that never idle).
