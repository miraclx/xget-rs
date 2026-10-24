# xget

A resumable file downloader with parallel fetching and concurrent checksum measurement. Chunks go
straight to their offsets on disk; when hashing is enabled, the reported digest covers the bytes
written, not just the network response.

The workspace contains two packages:

- **`xget`**, the reusable Rust library at the repository root.
- **`xget-cli`**, the command-line package in [`cli/`](cli/), which installs the **`xget`** executable.

Library-only consumers do not need the CLI's argument parser, terminal renderer or dotenv setup.

![xget downloading a file in parallel chunks](media/download.gif)

## Why xget

- **Checks during transfer.** Length and transport range checks reject inconsistent responses. Enabled
  checksums measure the file bytes; matching a trusted expected digest is a separate integrity check.
- **Parallel and resumable.** Chunks download at once. An interrupted file download resumes from saved
  ranges and re-verifies the retained prefix.
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

**With Cargo**, from a checkout of this workspace:

```console
cargo install --path cli --features "s3 ipfs"
```

**Build without installing:**

```console
cargo build --release --package xget-cli --bin xget --features "s3 ipfs"
```

Source builds use Rust 1.97.1, matching the pinned and exercised toolchain. The former Rust 1.85
metadata was not a verified compatibility promise.

The `s3` and `ipfs` features are optional and off by default. The CLI forwards them to the library;
omit them for an HTTP-only build. The executable name remains `xget`, but its Cargo package is now
`xget-cli`. Existing library imports remain `xget`.

## Quick start

Download a file. It is fetched in parallel chunks and the SHA-256 is printed at the end:

```console
xget https://example.com/big.iso
```

Verify against a known checksum. `--expect` takes an inline `algo:hex`, a bare hex digest, or a URL to a
checksum file, and exits non-zero on mismatch:

```console
xget https://example.com/big.iso --expect sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
xget https://example.com/big.iso --expect https://example.com/big.iso.sha256sum
```

`--expect` checks a download, not a file already on disk. To check an existing file, use a checksum
utility such as `shasum -a 256 big.iso`.

The current CLI compares the expected digest after the legacy engine finishes. A mismatch returns a
non-zero status but does not roll back an already published output file. The library's separate
checked native-file entrance, described below, performs its required check before publication.

![a checksum mismatch fails loudly and exits non-zero](media/expect.gif)

Bad networks are the normal case, and xget is built for them. Within a run, a dropped chunk retries from
the byte it reached, and the retry is reported above the bar so a stall explains itself instead of a
silently frozen bar:

![chunks dropping and retrying, reported above the bar](media/retry.gif)

Across runs it is just as forgiving. Interrupt it and run it again: it resumes from where it stopped,
re-verifying the bytes already on disk from the start as it goes (a checksum cannot be resumed
mid-stream, so the guarantee is worth the reread), no flag needed. You can also point xget straight at
the leftover control file and it finishes the job, no URL to retype. Ctrl-C exits with status 130 and
prints the resume command; stdout, pipe, and discard outputs keep no resumable partial:

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

The inactivity timeout is opt-in. Without `--timeout`, a stalled chunk may wait indefinitely.

`--progress json` writes newline-delimited events to stderr: `progress`, `retry`, `end`, and, when a
checksum was computed, `verified`. A quick download may finish before the first throttled `progress`
event. Byte counts and `speed` are always bytes and bytes per second; `eta` is seconds and `end.elapsed`
is milliseconds. `--bits` and `--raw-sizes` affect human output only and do not change JSON units.

`--progress plain` updates in place on a terminal. When stderr is captured, it emits complete text
lines without cursor-control escapes or styled diagnostic labels. Retry-cause control characters are
escaped rather than allowed to redraw or split those lines. Progress and diagnostics stay on stderr;
a `-` output's payload stays on stdout.

## Library usage

Depend on the root `xget` package; `xget-cli` is not needed for library use.

### Ordinary exact HTTP downloads

A reusable `Client` resolves advisory metadata, freezes output assignments, then executes through the
checked native publication path:

```rust
use xget::Client;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    let client = Client::new()?;
    let report = client.download("https://example.org/file.bin")
        .to_file("file.bin")
        .run().await?;
    assert_eq!(report.completed().len(), 1);
    Ok(())
}
```

The destination must be absent in a suitable existing caller-owned parent directory. Supply a digest
with `.expect(digest)` when the caller has a whole-file requirement; optional SHA-256 measurement alone
is not an expected-content check. `download_all(inputs).to_directory(path)` plans multiple exact inputs,
and `.plan().await` exposes frozen read-only assignments before `client.execute(plan).await`. Planning
uses HEAD, never a preview payload GET, and creates no output directories.

This first ordinary Client supports exact HTTP inputs and serial files, not provider collections,
trees, parallel ranges, aggregate resume or overwrite. Unsupported inputs fail explicitly. Redirects,
automatic retries, content decompression and ambient proxies are disabled. Clones share request/body
admission and connection reuse. Each request has a fixed 15-second deadline for admission waiting,
connection and headers together, but no payload/inactivity timeout. Retained open bodies can exhaust
admission; drop or consume them to release capacity. Use a caller-owned Tokio runtime with I/O and
time enabled. See the [`client` module](src/client.rs) for selection, version and failure contracts. The CLI still uses the
legacy engine below; its feature flags do not enable new Client provider adapters.

### Existing parallel/resumable API

The existing `get(url)` / `from(source)` API remains available. Start with `get(url)` and finish with
`.write(output)`:

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

### Checked native-file publication

[`Transfer`](src/transfer.rs) accepts an already-open ordered body, zero or more whole-file digest
requirements, and a separate optional measurement. Clean EOF, settled writes and the declared length
when known precede atomic absent-only publication; every configured requirement must match.

SHA-256 measurement is enabled by default. Disabling measurement does not disable requirements, and
choosing neither performs no hidden recovery hash. Reports distinguish measured digests from matched
requirements. When retained identities agree but no strong content anchor was recorded, recovery
returns `NotRevalidatable` rather than trusting identity alone. This entrance is separate from the legacy `get`/`from` pipeline;
the current CLI has not switched to it.

The advanced draft keeps existing `RequiredSha256` calls working. Successful receipts now expose
read-only `partial()`, `checks()`, `required()` and `state()` views; `required()` is an optional SHA-256
compatibility view. Run/stop/reconciliation return concrete boxed failures. New code should inspect the
algorithm-general check results rather than infer requirements from a SHA-256 compatibility field.

The native entrance requires a caller-owned Tokio runtime, an existing caller-owned parent without
group/other write permission, and Unix hardlink support. It retains private staging for reconciliation,
not automatic reopening of a one-shot source, and promises process-interruption recovery rather than
power-loss durability. See the [external API test](tests/transfer.rs) for a complete executable call.

### Borrowed range readers

`Transfer::from_reader(&reader)` accepts repeatable, known-size access supplied by an application.
The reader names its opening future, body and error for the actual borrow lifetime. Local/borrowed
components are allowed; a Send helper uses explicit named lifetime-family bounds rather than requiring
all application data to be static. See [`transfer::reader`](src/transfer/reader.rs) for a complete
implementation and generic helper.

Each opened response must match the full requested window and frozen representation/version evidence.
An unversioned candidate requires a whole-payload SHA-256, SHA-512 or BLAKE3 expectation; size,
measurement alone and MD5/SHA-1 do not authorize assembly. The reader implementer must uphold an
enforced-version contract throughout each body. This is not source authenticity.

Up to four ranges write bounded prefixes into the native stage. The shared in-order checker follows
the contiguous settled-write frontier while other bodies are still running, not only after the slowest
range finishes. Clean range EOF, exact length, settled writes and all requirements precede absent-only
publication. Failures retain typed causes and partial facts. Retained state does not persist a source
session pin or resume/reopen ranges automatically. This advanced entrance has not enabled parallel
ranges in the ordinary HTTP Client or replaced the legacy CLI engine.

### Checked live output

The same `Transfer::from_body` policy can release ordered bytes with `.into_body()` or drain them
into a borrowed writer with `.stream_to(&mut writer)?.run().await`. These entrances do not create
files, reopen sources, spawn a pump or shut down the caller's writer. They need no runtime beyond
what the supplied body and writer require. Pinned writers use `.stream_to_pinned(...)`.

Released prefixes cannot be recalled and are not expectation-verified. A late source error, length
failure or required-check mismatch is reported before clean stream completion. Consume the stream
through EOF and then call its consuming `finish()` to obtain a success receipt; reading exactly the
declared count is insufficient. Writer success additionally requires a successful flush. Body counters
and accepted write counts remain separate, including on failure. Cancellation stops further polling,
not arbitrary physical I/O already owned by a caller's writer.

See [`transfer::live`](src/transfer/live.rs) for runnable examples and the
[public integration tests](tests/transfer_live.rs) for borrowed/local sources, writer backpressure and
terminal failures. These advanced entrances do not replace the legacy CLI streaming path.

## How it works

The legacy engine writes chunks straight to their offsets in a sparse `<file>.xget`. One verifier
reads the contiguous prefix in order, hashes it when enabled, and gates on the exact length before
renaming it into place. That
same `.xget` is the resume control: it records the ranges already written, so a re-run picks up each
chunk where it stopped and folds the existing bytes into the same verify pass. Before resuming, xget
compares the saved and current resource validators (`ETag`, `Last-Modified`, or an IPFS CID); a mismatch
discards the partial rather than stitching versions together. If either validator is absent, resume
falls back to the length alone, which cannot detect a same-length resource change.

## Differences from libxget-js

xget keeps the shape of the original but changes some behavior on purpose. Where libxget-js buffered
chunks in memory (`--cache-size`, `--show-cache`), the legacy Rust engine writes chunks to their offsets
in a sparse file and reads the contiguous prefix for enabled hashing. Downloads are length-gated;
checksums are optional unless selected by a requirement or source metadata. This trades memory
buffering for file I/O, not a claim of unlimited parallelism.

Resume uses saved ranges and compares resource validators when available, rather than blindly
appending. There is no `--start-pos` or `--force-append`, because those operations do not preserve the
whole-resource checking model. Other supported features include parallel chunks, retries, mirrors,
custom headers and pluggable sources, with S3 and IPFS added.

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option.
