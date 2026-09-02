# xget

A fast, resumable, **verified** file downloader. It splits a download into parallel chunks, writes each
straight to its place on disk, and hashes the result as it goes, so the digest it prints certifies the
bytes you actually got. Ships as a command-line tool (`xget`) and a Rust library.

![xget downloading a file in parallel chunks](media/download.gif)

## Why xget

- **Verified by construction.** A source that ignores a range, or serves the wrong length, is a hard
  error, never a silent bad file. The reported hash is of the bytes on disk.
- **Parallel and resumable.** Chunks download at once; an interrupted download resumes from exactly where
  it stopped, and the resumed file is still fully verified.
- **More than HTTP.** The same engine reads from HTTP, S3 (and S3-compatible stores), and IPFS.

This is a Rust rewrite of the Node.js [`libxget`](https://github.com/miraclx/libxget-js); see
[what is intentionally different](#differences-from-libxget-js).

## Install

**Prebuilt binary** (Linux and macOS, x86-64 and arm64) from the
[latest release](https://github.com/miraclx/xget-rs/releases/latest). For example, on Apple silicon:

```console
curl -L https://github.com/miraclx/xget-rs/releases/latest/download/xget-aarch64-apple-darwin.tar.gz | tar xz
./xget --version
```

**With cargo**, from the repository:

```console
cargo install --git https://github.com/miraclx/xget-rs --features "s3 ipfs"
```

**From source:**

```console
cargo build --release --bin xget --features "s3 ipfs"
```

The `s3` and `ipfs` sources are optional features, off by default; drop them for an HTTP-only build.

## Quick start

Download a file. It is fetched in parallel chunks and the SHA-256 is printed at the end:

```console
xget https://example.com/big.iso
```

Verify against a known checksum. `--expect` takes an inline `algo:hex`, a bare hex digest, or a URL to a
checksum file, and exits non-zero on mismatch:

```console
xget https://example.com/big.iso --expect sha256:9f86d0818...
xget https://example.com/big.iso --expect https://example.com/big.iso.sha256sum
```

![a checksum mismatch fails loudly and exits non-zero](media/expect.gif)

Bad networks are the normal case, and xget is built for them. Within a run, a dropped chunk retries from
the byte it reached, and the retry is reported above the bar so a stall explains itself instead of a
silently frozen bar:

![chunks dropping and retrying, reported above the bar](media/retry.gif)

Across runs it is just as forgiving. Interrupt it and run it again: it resumes from where it stopped,
re-verifying the bytes already on disk from the start as it goes (a checksum cannot be resumed
mid-stream, so the guarantee is worth the reread), no flag needed. You can also point xget straight at
the leftover control file and it finishes the job, no URL to retype:

```console
xget big.iso.xget
```

To see what a leftover `.xget` is without touching the network, ask:

```console
xget --info big.iso.xget
```

![xget interrupted and auto-resuming](media/resume.gif)

Stream to stdout with `-`, or to a pipe or process substitution, and it streams instead of saving:

```console
xget https://example.com/big.iso - | sha256sum
xget https://example.com/big.iso >(tar xz)
```

A piped download that would not fit in the temp dir, or is simply huge, streams straight through in one
ordered pass rather than buffering to a scratch file first, so you can pipe an arbitrarily large file to a
consumer. Pass `--sequential` to force that single-stream mode for any download.

Download from S3 (with the `s3` feature). It signs when credentials resolve and goes anonymous when they
do not, and verifies against the object's stored checksum if it has one:

```console
xget s3://my-bucket/big.iso
xget s3://my-bucket/big.iso --endpoint-url https://s3.example.com   # R2, MinIO, Backblaze, ...
```

![an S3 object verified automatically against its stored checksum](media/s3.gif)

Download from IPFS through a gateway (with the `ipfs` feature). A CID that addresses raw bytes verifies
against its own hash for free:

```console
xget ipfs://bafybeih... --ipfs-gateway http://127.0.0.1:8080
```

## Common options

```console
xget [OPTIONS] <URL> [OUTPUT]
```

`<URL>` is `http(s)://`, `s3://`, or `ipfs://`. `OUTPUT` is where to write; if omitted or a directory, the
name comes from the server (`Content-Disposition`) or the URL. The full list is `xget --help`.

| flag | what it does |
| ---- | ------------ |
| `-n, --chunks <N>` | number of parallel chunks (default 5) |
| `-t, --tries <N>` | retries per chunk, each resuming from its offset; `inf` for unlimited |
| `-s, --checksum <ALGO>` | hash to report: `none`, `md5`, `sha1`, `sha256`, `sha512`, `blake3` |
| `--expect <[ALGO:]HEX>` | require a known checksum: inline, bare hex, or a URL to a checksum file |
| `--restart` | ignore a resumable partial and download from scratch (partials resume automatically) |
| `--sequential` | fetch as one ordered stream, no scratch (auto-selected for a huge or won't-fit pipe) |
| `-f, --overwrite` | permit replacing an existing complete file |
| `--mirror <URL>` | another source for the same file, tried when a chunk fails (repeatable) |
| `--endpoint-url <URL>` | endpoint for an `s3://` URL |
| `--ipfs-gateway <URL>` | gateway for an `ipfs://` URL |
| `-H, --header <H>` | set a request header (repeatable) |
| `--timeout <SECS>` | fail a chunk if no data arrives for this long, so its retry can resume it |
| `--progress <MODE>` | `auto`, `bar`, `plain`, `json`, or `none` |
| `-v, -vv` | verbose diagnostics on stderr |

## Library usage

Start with `get(url)` and finish with `.write(output)`:

```rust
use std::path::Path;
use xget::{Checksum, Output};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = xget::get("https://example.com/big.iso")?
        .chunks(8)
        .checksum(Checksum::Sha256)
        .write(Output::file(Path::new("big.iso")))
        .await?;

    println!("{} bytes, hash {:?}", report.length, report.hash);
    Ok(())
}
```

`get(url)` is the HTTP entry; `from(source)` starts from any `Source` (S3, IPFS, a `Mirrors` set, or your
own). Both chain `.chunks`, `.tries`, `.checksum`, `.timeout`, `.resume`, and `.progress`, then finish
with `.write(output)`. `Output` is `file(path)` (the only resumable sink), `writer(w)` to stream,
`Discard`, or `tee(file, w)` to keep a file and stream at once. To reuse a configuration across
downloads, wrap the chain in a small function: `let dl = |u| xget::get(u)?.chunks(8); dl(a)?.write(...)`.

## How it works

Chunks are written straight to their offsets in a sparse `<file>.xget`; one verifier reads it back in
order, hashing the contiguous prefix and gating on the exact length, then it is renamed into place. That
same `.xget` is the resume control: it records the ranges already written, so a re-run picks up each
chunk where it stopped and folds the existing bytes into the same verify pass. Before resuming, xget
checks the resource has not changed (via its `ETag`/`Last-Modified`, or an IPFS CID); if it has, the
partial is discarded rather than stitched.

## Differences from libxget-js

xget keeps the shape of the original but changes some behavior on purpose. Verification is structural,
not a cache you size: where libxget-js buffered chunks in memory (`--cache-size`, `--show-cache`), xget
writes each chunk straight to its offset and reads the file back once to hash it, trading that for
unbounded parallelism. Every download is hashed and length-gated, and a source that ignores a range is a
hard error. Resume is range-based and validated against the resource, rather than blindly appending. And
there is no `--start-pos` or `--force-append`, because both would produce bytes that cannot be verified
against the whole-resource checksum. Everything else from libxget-js is here: parallel chunks, retries,
mirrors, custom headers, and pluggable sources (with S3 and IPFS added).

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option.
