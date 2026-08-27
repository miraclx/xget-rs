# Feature parity with libxget-js

Honest tracking of libxget-js against libxget-rs, so "done" means done. ✅ done · 🟡 partial ·
⬜ todo · ➖ intentionally different.

## Engine

| feature | status | notes |
| ------- | ------ | ----- |
| chunked parallel range GET | ✅ | `plan` + structured concurrency |
| resume a dropped chunk from offset | ✅ | validated `Content-Range`, backoff |
| one-shot streaming (hash while writing) | ✅ | ordered reassembly over bounded channels |
| range-not-honored / length gates | ✅ | typed `RangeNotHonored` / `LengthMismatch` |
| bounded memory (no StreamCache) | ✅ | `parts * CHUNK_BUFFER` buffers |
| single-stream fetch for non-range sources | ✅ | fetch_all, one stream, still hashed |
| configurable checksum algorithm | ✅ | none/md5/sha1/sha256/sha512 |
| resume a partial FILE across runs (`-c`) | ⬜ | different from per-chunk retry |
| start from offset (`-i, --start-pos`) | ⬜ | |
| inactivity timeout (`--timeout`) | ✅ | per-read; a ranged chunk's retry resumes it |
| pluggable sources (S3, bifrost peer) | 🟡 | `Source` seam exists; only HTTP impl |

## CLI (`xget`)

| flag | status | notes |
| ---- | ------ | ----- |
| `-n, --chunks <N>` | ✅ | concurrent chunks |
| `-t, --tries <N>` | ✅ | numeric or `inf` |
| `-H, --header <H>` (repeatable) | ✅ | custom request headers |
| output filename inference | 🟡 | URL basename; Content-Disposition TODO |
| `-D, --directory-prefix`, `--no-directories` | ✅ | |
| `-f, --overwrite` | ✅ | refuses to clobber without `-f`; `--force-append` TODO |
| `--progress <bar\|plain\|json\|none>`, `--no-bar` | ✅ | auto-detects a TTY |
| `--raw-sizes` | ✅ | raw byte counts |
| `-s, --checksum <algo>` reporting | ✅ | none/md5/sha1/sha256/sha512 |
| `--timeout <secs>` | ✅ | inactivity timeout |
| `--continue`, `--start-pos`, `--cache-size` | ⬜ | see engine |

## Progress

| feature | status | notes |
| ------- | ------ | ----- |
| live segmented bar, one segment per chunk | ✅ | xprogress |
| human size readout (done / total) | ✅ | xbytes |
| speed and ETA | ✅ | rolling readout |
| per-chunk detail / labels | ⬜ | |
| plain / json output modes | ✅ | `--progress plain\|json`, throttled |

## Deliberately different (➖)

- No `StreamCache` / `--cache-size` / `--show-cache`: replaced by bounded per-chunk channels, so memory
  is `parts * CHUNK_BUFFER` by construction. A `--cache-size` knob could map to that bound later.
