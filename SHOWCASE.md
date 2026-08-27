# Showcase checklist

Features worth showing off in the README / crate docs, so nothing we built goes unsold. Keep this
list current as features land; fold the ✅ ones into the README before release.

`demo` = has a runnable example or GIF · `readme` = surfaced in the README yet.

## The pitch (why use xget over curl/wget)

- **Verified by construction.** One-shot streaming hash: bytes are hashed as they are written, never
  read back, and a length gate means a reported digest certifies the file. demo: n/a · readme: partial
- **`--expect` gate.** Assert a known checksum (inline `algo:hex`) or point at a published checksum
  file (`--expect https://.../file.sha256sum`); exits non-zero on mismatch. demo: ✅ (Helm sidecar) ·
  readme: ⬜
- **Atomic output.** Writes to `<file>.part` and renames on success, so a killed download never leaves
  a truncated file that looks whole. demo: ⬜ · readme: ⬜
- **Resume across runs (`-c`).** Keeps the `.part`, fetches only what remains, and re-hashes the
  existing prefix concurrently so the resumed file is still fully verified. demo: ⬜ · readme: ⬜

## Download engine

- Parallel chunked range GET with per-chunk retry/backoff. demo: ✅ (media/download.gif) · readme: ✅
- Single-stream fetch for non-range sources, still hashed. demo: ⬜ · readme: ⬜
- Configurable checksum: none / md5 / sha1 / sha256 / sha512 / blake3 (the hash theia/iris speak).
  demo: ⬜ · readme: ⬜
- Inactivity `--timeout`; a stalled chunk's retry resumes it. demo: ⬜ · readme: ⬜
- Bounded memory; `--cache-size` is the read-ahead knob. demo: ⬜ · readme: ⬜
- Pluggable `Source` (HTTP today; S3 and a bifrost peer planned). demo: ⬜ · readme: partial
- Mirror / failover across sources (`--mirror`), via a `Mirrors` Source. demo: ⬜ · readme: ⬜

## Presentation

- Aggregate-over-per-chunk bar with lead/done two-tone, libxget-js look, compact width. demo: ✅
  (media/download.gif) · readme: ✅
- Preamble (URL / chunks / length+type / saving) and closing summary + hash. demo: ✅ · readme: ⬜
- Progress modes: bar / plain / json / none, `--no-bar`, `--raw-sizes`. demo: ⬜ · readme: ⬜
- Output naming from Content-Disposition (RFC 5987) or the URL. demo: ⬜ · readme: ⬜
