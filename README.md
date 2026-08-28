# xget

A chunked, parallel, resumable, verified fetch engine over a pluggable byte source, with a command-line
downloader (`xget`) built on top of it.

![xget downloading a file in parallel chunks](media/download.gif)

## What it is

xget plans a resource into chunks and fetches them in parallel, each written straight to its own
offset in a sparse file. A single verifier reads that file back in order, hashing the contiguous prefix
as it grows, gated on the exact length. The digest it reports certifies the bytes on disk, not "the
bytes we glued together": a source that ignores a range, or serves a length that does not match, is a
typed error, never a silent bad file.

The byte source is pluggable behind the `Source` trait. The engine is written once and works over any
source that can report a length and serve a byte range; a source that cannot serve ranges is fetched as
a single stream and still hashed. Sources today are HTTP range GET (default), S3 and S3-compatible
stores (behind the `s3` feature), and IPFS through an HTTP gateway (behind the `ipfs` feature).

This is a Rust rewrite of the Node.js [`libxget`](https://github.com/miraclx/libxget-js). It works. The
feature-by-feature comparison against the original lives in [PARITY.md](PARITY.md), and the design and
the failure modes it is built to prevent are in [DECISIONS.md](DECISIONS.md).

## Install

Build the `xget` binary from source:

```console
cargo build --release --bin xget
```

The S3 and IPFS sources are optional features, off by default:

```console
cargo build --release --bin xget --features "s3 ipfs"
```

As a library dependency:

```toml
[dependencies]
xget = { git = "https://github.com/miraclx/xget-rs" }
# or with optional sources:
# xget = { git = "https://github.com/miraclx/xget-rs", features = ["s3", "ipfs"] }
```

## Quick start

Download over HTTP in parallel chunks:

```console
xget https://example.com/big.iso
```

Verify against a known checksum. `--expect` takes an inline `algo:hex`, a bare hex digest (using
`--checksum`'s algorithm), or a URL to a checksum file. It exits non-zero on mismatch:

```console
xget https://example.com/big.iso --expect sha256:9f86d0818...
xget https://example.com/big.iso --expect https://example.com/big.iso.sha256sum
```

![a checksum mismatch fails loudly and exits non-zero](media/expect.gif)

Resume an interrupted download. A partial resumes automatically on a re-run with no flag; `-c` forces
resuming and `--restart` forces a fresh start:

```console
xget https://example.com/big.iso        # resumes a partial if one is present
xget https://example.com/big.iso -c     # force resume
xget https://example.com/big.iso --restart
```

Download from S3 or an S3-compatible store (needs the `s3` feature). It signs when credentials resolve
and goes anonymous when they do not, reads a workdir `.env`, and takes `--endpoint-url` for a
non-AWS store. If the object carries a stored checksum, it is verified automatically:

```console
xget s3://my-bucket/big.iso
xget s3://my-bucket/big.iso --endpoint-url https://s3.example.com
```

![an S3 object verified automatically against its stored checksum](media/s3.gif)

Download from IPFS through an HTTP gateway (needs the `ipfs` feature). A CID that addresses raw bytes is
verified against its own hash for free:

```console
xget ipfs://bafybeih... --ipfs-gateway http://127.0.0.1:8080
```

## CLI overview

```console
xget [OPTIONS] <URL> [OUTPUT]
```

`<URL>` is an `http(s)://`, `s3://`, or `ipfs://` URL. `OUTPUT` is where to write the file; if omitted
or a directory, the name is taken from the HTTP `Content-Disposition` or the URL, and the file is
renamed into place atomically on success.

The flags that matter most:

| flag | what it does |
| ---- | ------------ |
| `-n, --chunks <N>` | maximum concurrent chunk connections (default 5) |
| `-t, --tries <N>` | retries per chunk, each resuming from its offset; `inf` for unlimited |
| `-s, --checksum <ALGO>` | hash to verify with: `none`, `md5`, `sha1`, `sha256`, `sha512`, `blake3` |
| `--expect <[ALGO:]HEX>` | require a known checksum: inline, bare hex, or a URL to a checksum file |
| `-c, --continue` | force resuming a partial (a partial resumes automatically without it) |
| `--restart` | ignore a resumable partial and download from scratch |
| `-f, --overwrite` | permit replacing an existing complete file |
| `--mirror <URL>` | a mirror for the same resource, tried when the primary fails a chunk (repeatable) |
| `--endpoint-url <URL>` | endpoint for an `s3://` URL (R2, MinIO, Backblaze, ...) |
| `--ipfs-gateway <URL>` | gateway for an `ipfs://` URL |
| `-H, --header <H>` | set a request header (repeatable) |
| `--timeout <SECS>` | fail a chunk if no data arrives for this long, so its retry can resume it |
| `--progress <MODE>` | `auto`, `bar`, `plain`, `json`, or `none`; `--no-bar` and `--raw-sizes` also apply |
| `-v, -vv` | verbose diagnostics on stderr: chunk ranges, retries, and errors |

Run `xget --help` for the full list.

## Resume

![xget interrupted and auto-resuming](media/resume.gif)

An interrupted download leaves a single `<output>.xget` file: the data, a control trailer recording the
byte ranges written and flushed, and a fixed footer. A re-run finds it and resumes automatically with no
flag, keeping the same chunk count and picking each partial chunk up from the prefix already on disk.
In-flight progress is checkpointed (every few MiB and on a short time interval), so a resume starts near
where it stopped rather than from zero. The bytes already on disk are folded into the same in-order
verify pass, so a resumed file is still fully verified. The GIF above shows a download interrupted
partway and re-run with no flag: it picks up from where it stopped and verifies to the end.

## Progress

`xget` shows a live two-tone bar over per-chunk segments, distinguishing bytes confirmed by the verifier
from bytes buffered ahead, with a windowed speed and ETA readout. `--progress` selects `bar`, `plain`
(a single updating line), `json` (one event per update on stderr), or `none`; `auto` (the default) uses
the bar on a terminal and plain lines otherwise.

## Library usage

The engine is `download`, driven over anything that implements the `Source` trait. A built-in source is
used here; you can supply your own for any protocol that can probe a length and serve a byte range.

```rust
use xget::{download, HttpSource, Options};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = HttpSource::new("https://example.com/big.iso", Default::default())?;
    let report = download(
        &source,
        std::path::Path::new("big.iso"),
        Options::default(),
        &(), // no progress reporting
    )
    .await?;

    println!("{} bytes, {:?}", report.length, report.hash);
    Ok(())
}
```

`Options` tunes parallelism (`parts`), retries, the `checksum` algorithm, an inactivity `timeout`, and
whether to `resume`. `Progress` is an opt-in trait with a no-op `()` implementation; implement it to
observe bytes as they are received and verified. Whether a resumable partial exists beside an output can
be checked with `resumable`.

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option.
