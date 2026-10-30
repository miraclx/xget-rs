# xget

Download files, choose what to fetch from collections, and check their contents.

xget is a command-line downloader and Rust library. It can inspect remote files before downloading,
require a checksum before saving a finished file, and continue eligible interrupted downloads.
Existing files are left untouched unless you explicitly request replacement.

File downloads require Unix. HTTP URLs must point directly to the file: redirects, automatic retries
and decompression are not supported. See the
[compatibility details](cli/README.md#optional-providers) before using it for a particular service.

## Install and try it

From source, using Rust 1.97.1:

```console
git clone https://github.com/miraclx/xget-rs.git
cd xget-rs
cargo install --locked --path cli
```

Download a small public text file:

```console
url=https://raw.githubusercontent.com/rust-lang/rust/master/LICENSE-MIT
xget "$url" -o rust-license.txt
```

You get progress, a `saved rust-license.txt` receipt, its measured SHA-256 and a completion summary.
The measured checksum describes the downloaded bytes; supply an expected checksum to verify that they
are the contents you intended.

The 0.2 release candidate is in preparation. See [installation](docs/installation.md) for source
features and archive instructions, and [release notes](CHANGELOG.md) for changes.

## What you can do

### Choose where files go

```console
xget "$url" -o another-license.txt
xget "$url" -D downloads
```

`-o` chooses a file; `-D` chooses a directory. Without either, xget uses the current directory.
It refuses to replace an existing file by default. Use `--fresh --overwrite` only when a newly
fetched, checked replacement is what you want.

### Require a checksum

For the sample file above:

```console
xget "$url" -o checked-license.txt \
  --expect sha256:b71bd43a069ca0641a9ecfe585ca7b3c53b5cc1608f8b68321168698e28b5ea1
```

The finished destination appears only after the transfer and required checks succeed. If the source
changes, this expectation fails instead. For your own files, use a complete checksum from a trusted
source. [Checksum options and local sidecars](cli/README.md#require-an-exact-digest) cover other algorithms
and checksum files.

### Inspect and choose from a collection

With the [`github` feature enabled](docs/installation.md#build-from-source), list a release's assets
and select the ones you need. For example, choose ripgrep's Linux x86-64 archive:

```console
release=https://github.com/BurntSushi/ripgrep/releases/tag/14.1.1
xget list "$release"
xget "$release" --select '*x86_64*linux*.tar.gz' -D assets
```

Use `--all` to choose all release assets. Selection matches original file paths, not renamed outputs.
[The CLI guide](cli/README.md#select-among-alternatives) explains patterns and literal file IDs.

### Preview a download

```console
xget "$url" -D preview --dry-run
```

See the planned files and destinations without downloading payloads or creating output directories.
The preview still requests remote metadata.

### Continue interrupted work

Repeat the original command. xget looks for matching unfinished work before discovering files again;
eligible HTTP downloads can reuse a checked partial prefix. Other sources use whole-file responses.
Not every failure is resumable.

For a particular saved run, `xget info STATE` shows its details. The
[recovery guide](cli/README.md#inspect-and-continue-unfinished-work) shows how to resume it with the
original inputs or deliberately start over. Incomplete payloads remain in private workspaces,
not at the final filename.

### Stream to another program

```console
set -o pipefail
xget "$url" -o - | wc -c
```

Stdout contains only payload bytes; progress and diagnostics go to stderr. A late failure cannot
retract bytes already sent to the next program, so use `pipefail` to retain the failure status.

## What you can download

| Source | Input |
| --- | --- |
| HTTP(S) | Direct file URLs |
| S3 | An object, or an explicit prefix with `--tree` |
| SwissTransfer | A share's files |
| WebDAV | An explicit directory tree with `--tree` |
| GitHub | Release assets, with an explicit selection |
| IPFS | An exact CID/path through an HTTP gateway |

HTTP is available in the default source build. The other adapters are enabled with Cargo features;
see [installation](docs/installation.md) and the [source reference](cli/README.md#optional-providers).
Directory listing is not website crawling. Files download one at a time; eligible HTTP files use up
to four ranges within a file.

## Use from Rust

The `xget` library provides the same downloads without the CLI's parser or terminal UI:

```rust
use xget::Client;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    let client = Client::new()?;
    let report = client.download("https://raw.githubusercontent.com/rust-lang/rust/master/LICENSE-MIT")
        .to_file("library-license.txt")
        .run().await?;
    assert_eq!(report.completed().len(), 1);
    Ok(())
}
```

Run this on Tokio and choose a new output path in a directory you own.
The [library guide](docs/library.md) includes the complete Cargo setup, expected checksums,
inspection and selection, progress observers, recovery, and API documentation.

## Documentation

- [CLI guide](cli/README.md): commands, checks, selection, output and recovery.
- [Library guide](docs/library.md): application setup and API learning path.
- [Installation](docs/installation.md): features, platforms and archive verification.
- [Release notes](CHANGELOG.md): API/CLI migration and the 0.2 scope.

Licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
