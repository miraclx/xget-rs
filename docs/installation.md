# Install xget

## Build from source

Use Rust 1.97.1, the toolchain pinned by this repository:

```console
git clone https://github.com/miraclx/xget-rs.git
cd xget-rs
cargo install --locked --path cli
```

This installs the `xget` command from the `xget-cli` package. The root `xget` package is the Rust
library; you do not need the CLI package to use the library.

The default source build supports HTTP. Enable additional sources as needed:

```console
cargo install --locked --path cli --features github
cargo install --locked --path cli --features s3
cargo install --locked --path cli --features "s3 ipfs swisstransfer webdav github"
```

See the [source reference](../cli/README.md#optional-providers) for supported inputs and service-specific
requirements. S3 uses the caller's AWS SDK configuration; enabling a feature does not supply credentials.

To try the checkout without installing, run the ordinary command through Cargo:

```console
cargo run -- https://raw.githubusercontent.com/rust-lang/rust/master/LICENSE-MIT -o rust-license.txt
```

Or build an optimized executable without installing it:

```console
cargo build --release --package xget-cli --bin xget
```

Older Rust versions are not claimed. File downloads require Unix hardlink support and a suitable
caller-owned destination directory. Linux and macOS are the native archive targets. Windows compilation
has been checked, but native Windows file publication and portable-engine Wasm support are not established.

## Release archives

The 0.2 release candidate is being prepared; use the source instructions above for a currently
available installation path. Actual published versions and assets appear on
[GitHub Releases](https://github.com/miraclx/xget-rs/releases). A candidate build is not a published release,
and a GitHub archive release does not imply crates.io or docs.rs publication.

The full-feature candidate archive edition includes `s3 ipfs swisstransfer webdav github`:

| Target | Native validation host |
| --- | --- |
| `x86_64-unknown-linux-gnu` | Ubuntu 24.04 x64 |
| `aarch64-unknown-linux-gnu` | Ubuntu 24.04 ARM64 |
| `x86_64-apple-darwin` | macOS 15 Intel |
| `aarch64-apple-darwin` | macOS 15 Apple Silicon |

These are candidate validation hosts, not minimum supported OS versions. No older glibc/macOS floor, musl/static,
universal macOS, native Windows or Wasm artifact is promised. Each candidate must pass its native
extracted-archive checks; see its provenance for the actual results.

After a version is published, download the archive for your target, `SHA256SUMS` and `provenance.json`
from that same release. For example, with GitHub CLI on Apple Silicon:

```console
gh release download v0.2.0-rc.1 --repo miraclx/xget-rs \
  --pattern 'xget-0.2.0-rc.1-aarch64-apple-darwin.tar.gz' \
  --pattern SHA256SUMS --pattern provenance.json
```

Use a new empty download directory. Compare the selected archive's hash with its entry in
`SHA256SUMS`, using `shasum -a 256` on macOS or `sha256sum` on Linux, before extracting it into another
new empty directory. Follow the enclosed `INSTALL.txt`, check `./xget --version`, and choose a new
installation path rather than blindly replacing an existing executable.

Checksums detect changed bytes; they are not publisher authentication, notarization or a reproducible-build
claim. Compiled adapters and source fixtures do not mean every external service ran on every archive host.

## Match a particular candidate

Use the source commit recorded in its provenance. `--locked` requires the committed lockfile, but local
Cargo configuration can still override dependencies. Candidate builds use the locked Git graph without
this workspace's local sibling overrides. An external Rust application owns its own dependency resolution
and lockfile; see the [library setup guide](library.md#application-setup).

Build manifests, compiler/image details, dependency revisions and the complete acceptance procedure are
maintainer material in the [release guide](../scripts/release/README.md). API/CLI migration notes are in the
[changelog](../CHANGELOG.md).
