# libxget-rs decisions

A Rust rewrite of the JS libxget: a chunked, parallel, resumable, verified fetch engine. Built from the
JS autopsy in `rust-rewrites/research/libxget.md`.

## North star: never certify bytes we did not verify

The JS could hand back a corrupt file with a valid-looking sha256, because it hashed whatever bytes it
glued together without checking the server cooperated. Every footgun below is designed out so a bad
download is a typed error, not a silent bad file.

## Footguns designed out (from the autopsy)

- **Range not honored -> corruption.** Every ranged fetch requires `206` + a `Content-Range` whose
  start equals the requested offset, else `Error::RangeNotHonored`. The JS only checked for `2xx`.
- **No length contract.** Per-chunk and total length are gated before the digest is trusted
  (`Error::LengthMismatch`); the hash certifies the resource, not "the bytes we glued".
- **Blind resume.** A resumed chunk validates its `Content-Range` start, so a retry cannot duplicate or
  gap bytes.
- **Fake parallelism + hand-rolled cache.** No `StreamCache`; explicit bounded concurrency
  (`buffer_unordered`) + bounded channels, so memory is concurrency x capacity and a stalled chunk
  cannot deadlock silently.
- **Retry hammering.** Exponential backoff + jitter; an honest zero-retry.
- **Masked errors, duck-typed streams, N connection pools.** `thiserror` with `#[source]`; one
  homogeneous byte-stream type; one shared client.

## Pluggable source (the bifrost pattern)

The engine is source-agnostic behind the `Source` trait: it needs only a length probe and a
range-serving fetch. HTTP (Range GET) is the first adapter; S3 and a theia/bifrost peer (parallel
verified download by public key) are future adapters. A source that cannot serve ranges is fetched as a
single stream. Same ports-and-adapters cut as bifrost's `Transport`.

## xresilient: folded in

The one real idea in xresilient (thread cumulative bytes-read into each retry to resume-from-offset)
lives inside the `chunk` layer, not a separate crate. It is only meaningful paired with `Range`, has no
second consumer, and folding it lets the resume path validate the resumed range. Extract later only if
a real second consumer appears.

## Verification: a plain streaming digest, no merkle

A single `sha2` (optionally `blake3`) digest over the ordered, length-gated output stream. No merkle
tree: the JS did not use one, and libxget hashes the whole resource, not per-chunk against a root.

## Progress and sizes

Progress is a `ProgressSink` port (not stream injection), rendered by the binary as an xprogress
segmented bar, one segment per chunk. Human sizes via xbytes. The library core stays dependency-light;
the `xget` binary wires the ports to xprogress/xbytes.

## Housekeeping

Held to the house standard: edition 2024, wired lint gate, thiserror, no unwrap in non-test, no em
dashes, `///` on every public item, tests as `<module>_tests.rs`. Dual MIT/Apache, holder Miraculous
Owonubi.
