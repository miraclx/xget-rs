# Showcase checklist

Features worth showing off in the README / crate docs, so nothing we built goes unsold. Keep this
list current as features land; fold the ✅ ones into the README before release.

`demo` = has a runnable example or GIF · `readme` = surfaced in the README yet.

## The pitch (why use xget over curl/wget)

- **Verified by construction.** Chunks scatter into a sparse file in parallel; a single verifier reads
  it back in order and hashes the contiguous prefix, gated on exact length, so a reported digest
  certifies the bytes on disk. demo: ✅ (harness) · readme: partial
- **`--expect` gate.** Assert a known checksum (inline `algo:hex`), point at a published checksum file
  (`--expect https://.../file.sha256sum`, or an `s3://.../file.sha256sum` sidecar), and for S3 objects
  that carry a stored checksum it verifies automatically with no `--expect` at all. Exits non-zero on
  mismatch. demo: ✅ (Helm sidecar; MinIO s3 sidecar + stored checksum) · readme: ⬜
- **Atomic output.** Writes to `<file>.part` and renames on success, so a killed download never leaves
  a truncated file that looks whole. demo: ⬜ · readme: ⬜
- **Resume across runs (`-c`).** Keeps the `.part`, fetches only what remains, and folds the existing
  prefix into the same in-order verify pass so the resumed file is still fully verified. demo: ✅
  (harness) · readme: ⬜

## Download engine

- Parallel chunked range GET, scatter-written to a sparse file, with per-chunk retry/backoff. demo: ✅
  (media/download.gif, harness) · readme: ✅
- Single-stream fetch for non-range sources, still hashed. demo: ⬜ · readme: ⬜
- Configurable checksum: none / md5 / sha1 / sha256 / sha512 / blake3 (the hash theia/iris speak).
  demo: ⬜ · readme: ⬜
- Inactivity `--timeout`; a stalled chunk's retry resumes it. demo: ✅ (harness) · readme: ⬜
- No memory buffering: positioned writes mean parallelism is unbounded and connections never idle on
  backpressure (a dropped connection just resumes). demo: ✅ (harness) · readme: ⬜
- Pluggable `Source` (HTTP, plus S3 behind the `s3` feature; a bifrost peer planned). S3 signs when
  credentials resolve and goes anonymous when they do not (no flag), reads a workdir `.env`, and works
  against any S3-compatible store (R2, MinIO, Backblaze) via `--endpoint-url`. Verified end-to-end
  against MinIO: signed, `.env`, and anonymous, all range-chunked. demo: ⬜ · readme: partial
- Mirror / failover across sources (`--mirror`), via a `Mirrors` Source. demo: ⬜ · readme: ⬜

## Presentation

- Aggregate-over-per-chunk bar with lead/done two-tone, libxget-js look, compact width. demo: ✅
  (media/download.gif) · readme: ✅
- Preamble (URL / chunks / length+type / saving) and closing summary + hash. demo: ✅ · readme: ⬜
- Progress modes: bar / plain / json / none, `--no-bar`, `--raw-sizes`. demo: ⬜ · readme: ⬜
- Output naming from Content-Disposition (RFC 5987) or the URL. demo: ⬜ · readme: ⬜
