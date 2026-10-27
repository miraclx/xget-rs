# xget

xget is a Rust fetch library and command-line tool. It resolves bounded source metadata, freezes the
local output plan, fetches and checks file bytes, and by default publishes only to an absent
destination. Explicit replacement requires `-f` and the native ownership checks.

**0.2.0-rc.1 preparation:** this checkout is a candidate source tree, not evidence of a published
release. The [release notes](CHANGELOG.md) describe the frozen 0.2 scope. Check the
[GitHub releases](https://github.com/miraclx/xget-rs/releases) for actual published versions and assets;
CI candidate artifacts alone are not releases. This is a breaking pre-1.0 API/CLI revision, not a 0.1.x
patch or a 1.0 stability promise.

Files execute serially; eligible exact HTTP uses up to four pinned ranges within one file. Live output
uses a checked stream without native publication.

> **Know before downloading:** ordinary HTTP redirects, automatic retries and decompression are not
> supported. HEAD 403 fails; the existing 405/501 exception permits unknown-length, unpinned metadata
> without a preview GET. A server that advertises but ignores ranges fails rather than falling back to
> a whole response. Native file output requires Unix and a
> suitable caller-owned parent. Incomplete payloads remain in private native workspaces, not a supported
> destination-adjacent partial-file inspection interface. `.xget-runs` paths are metadata, not payloads.
> HTTP interoperability and visible-partial enhancements are deferred beyond 0.2 stable.

## Install

### Source checkout

From this workspace, install the default HTTP CLI or run it without installing:

```console
cargo install --locked --path cli
cargo run -- https://raw.githubusercontent.com/rust-lang/rust/master/LICENSE-MIT -o rust-license.txt
```

Provider features are off by default in source builds and library dependencies. Forward the features
you need, or use the same five-feature set planned for the CLI archives:

```console
cargo install --locked --path cli --features s3
cargo install --locked --path cli --features "s3 ipfs swisstransfer webdav github"
```

These are the supported compiled adapters, not a promise to accept arbitrary delivery URLs. `ipfs`
enables exact `ipfs://CID[/path]` inputs; other adapter limits are listed [below](#optional-providers).
Use the candidate commit identified in its provenance when checking a particular artifact. `--locked`
requires the committed lockfile; inspect Cargo configuration separately, because local path overrides
can still apply. Distributed candidates use the locked Git graph without the workspace's local overrides.

Build without installing:

```console
cargo build --release --package xget-cli --bin xget
```

Rust 1.97.1 is the pinned and exercised toolchain. Older compilers are not claimed. The executable is
named `xget`; the Cargo package is `xget-cli`, and library imports remain `xget`.

### Candidate archives

The planned 0.2.0-rc.1 archive edition enables **`s3 ipfs swisstransfer webdav github`**. It does not
change the source/library defaults. Each target must pass an extracted-archive smoke before acceptance:

| target | native validation host |
| --- | --- |
| `x86_64-unknown-linux-gnu` | Ubuntu 24.04 x64 |
| `aarch64-unknown-linux-gnu` | Ubuntu 24.04 ARM64 |
| `x86_64-apple-darwin` | macOS 15 Intel |
| `aarch64-apple-darwin` | macOS 15 Apple Silicon |

This table is the candidate validation matrix, not a statement that all artifacts have passed yet.
Build hosts are not minimum supported OS versions. No musl/static, older glibc/macOS, universal macOS,
native Windows or Wasm artifact is promised. Windows compilation is not native publication proof.

**After a release is published**, obtain its target archive, `SHA256SUMS` and provenance from the same
release. For example, on Apple Silicon, in a new empty download directory with GitHub CLI installed:

```console
gh release download v0.2.0-rc.1 --repo miraclx/xget-rs \
  --pattern 'xget-0.2.0-rc.1-aarch64-apple-darwin.tar.gz' \
  --pattern SHA256SUMS --pattern provenance.json
```

Verify the archive's entry in `SHA256SUMS` with `shasum -a 256` on macOS or `sha256sum` on Linux before
extracting into another new empty directory. Follow the enclosed install instructions, inspect
`./xget --version`, and choose where to install it rather than overwriting an existing executable
blindly. Checksums detect changed bytes; they are not publisher authentication, notarization or a
reproducible-build claim. Registry installation and docs.rs publication are separate tracks and are
not implied by a GitHub archive release.

## Quick start

Try a small public text file in a new directory:

```console
mkdir xget-try
cd xget-try
xget https://raw.githubusercontent.com/rust-lang/rust/master/LICENSE-MIT -o rust-license.txt
```

Success reports `saved rust-license.txt`, a measured SHA-256 and `done 1 file`. Measurement is not a
required expected-content check. The public branch URL may change; use your own authorized reference
and complete digest when the expected contents matter.

For the remaining examples, replace URLs and expected digests with those for a resource you are
authorized to fetch.

Download one exact HTTP file to the current directory:

```console
xget https://example.com/big.iso
```

This writes `big.iso` and reports `saved big.iso`, its size and the measured SHA-256. The name comes from the
final percent-decoded component of the URL path. A `Content-Disposition` filename is not used. If the
URL has no usable final component, name the output explicitly.

Choose an output file or a destination directory:

```console
xget https://example.com/big.iso -o big.iso
xget https://example.com/big.iso -D downloads
```

`-o` and `-D` are mutually exclusive. There is no positional output: `xget URL OUTPUT` is not the
current syntax. Native publication in this command is absent-only, so an existing destination is an
error and is left untouched. Choose a new path, move the old file, or explicitly request guarded
replacement with `-f`. The replacement still requires clean EOF, length and required-check success;
`-f` never permits two members of one run to target the same file or inode.

Stream to stdout instead of publishing a file:

```console
set -o pipefail
xget https://example.com/big.iso -o - | sha256sum
```

Live output is checked while it runs, but bytes already written to stdout cannot be retracted. A late
source error, length mismatch or required-check failure still exits non-zero. `pipefail` makes the
pipeline report that failure. Success reports `delivered` to stdout, not a published file. Explicit JSON
retains `published_files: 0` in its final record.
Live output creates no owned run or native destination, so `--fresh`, `--restart` and `--overwrite`
are rejected for `-o -`, including live previews.

### Require an exact digest

Pass a complete `algorithm:hex` value with `--expect`:

```console
xget https://example.com/big.iso -o big.iso \
  --expect sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
```

The digest above is a complete example value, not a claim about the example URL. The default command
accepts only `ALGORITHM:HEX`; it does not accept a bare digest or a checksum URL. Supported algorithms
are `md5`, `sha1`, `sha256`, `sha512` and `blake3`. A required check must match before native
publication, and `--expect` needs exactly one selected file.

`-s` is optional measurement, not a requirement. The default is `sha256`; `-s none` disables
measurement, but any `--expect` requirement is still computed and enforced. Conflicts between
requirements for the same algorithm fail before payload opening. MD5 and SHA-1 are compatibility
checks, not strong content anchors.

### Use a local checksum sidecar

`--expect-file` reads a bounded local checksum file:

```console
printf '%s  big.iso\n' '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08' > big.iso.sha256
xget https://example.com/big.iso -o big.iso --expect-file big.iso.sha256 --fresh
```

Records are `algorithm:hex`, or bare hex with `--expect-file-algorithm` (SHA-256 when omitted).
A record may be followed by two spaces, or a space and an asterisk, and the exact original member
path. The path is the `source_path` printed by `xget list`, not the local output name or a basename.
A sidecar with one unlabelled record applies to the selected file; a multi-record sidecar must label
every record.
Duplicate or conflicting records are rejected. The sidecar must be a regular non-symlink local file,
at most 65536 bytes and 256 records, with paths at most 4096 UTF-8 bytes. Stdin and remote sidecars are
unsupported. Bounded sidecar opening is Unix-only; elsewhere use an inline `--expect`.

Use exactly one selected file with a sidecar, including a labelled multi-entry document. A native file
target additionally requires `--fresh` and exactly one original input. This prevents a sidecar from
silently changing a collection selection or matching old work. `--fresh` only bypasses continuation;
it does not authorize overwrite, deletion or cleanup. `-o -` with a sidecar does not require
`--fresh`.

### Select among alternatives

A catalogue such as a GitHub release, with the `github` feature, can contain several assets. Inspect
it first:

```console
xget list https://github.com/owner/repo/releases/tag/v1.2.3
xget https://github.com/owner/repo/releases/tag/v1.2.3 \
  --select '*linux*x86_64*' -D downloads
xget https://github.com/owner/repo/releases/tag/v1.2.3 --all -D downloads
```

`xget list` reports every alternative and needs no selector. An acquisition request for a GitHub
release catalogue requires an explicit choice in the current CLI: `--all`, `--select-id`, or a
positive `--select` with one original input. `--all` chooses every alternative, including a singleton
catalogue, and still honors `--select`, `--exclude` and `--select-id` criteria. It grants no overwrite
authority. The library's `.all()` expresses the same policy.

`--select` and `--exclude` match the original source-relative path, case-sensitively by Unicode scalar.
`*` stays within one path component, `?` consumes one scalar, and a complete `**` component consumes
zero or more components. Each positive include must match at least one file in its complete original
input scope before exclusions. Matching includes form a union; no includes means every path, and
exclusions apply afterward. A positive include on an empty file scope is an error; an exclusion-only
genuine empty scope remains a no-op. A literal `--select-id` uses the `selector_token` from `list`: a
SwissTransfer file UUID, a
full S3 key, a WebDAV absolute URI path, or a GitHub numeric asset ID, depending on the source.

The CLI selection flags require exactly one original input, even if several inputs would filter down
to one file. Use separate invocations for mixed selections; unfiltered multi-input acquisition remains
supported. Patterns are bounded at
64 includes and exclusions combined, 1024 UTF-8 bytes per spelling, 64 components and 16384 combined
spelling bytes.

## Default command

```console
xget [OPTIONS] [URL]...
xget list [OPTIONS] [URL]...
xget info <STATE> [--json]
xget resume <STATE> --input KEY=REFERENCE [--input KEY=REFERENCE] [OPTIONS]
```

The default command and `list` accept one or more positional references and an explicit `--tree URL`.
Inputs can be exact HTTP URLs, S3 or IPFS references, SwissTransfer share references, GitHub release
catalogues and, with `--tree`, supported directory services. Unsupported shapes fail explicitly.

The table describes the acquisition command. `list` shares input, selection, access, gateway and
`--json` options; `info` is offline with `<STATE>` and `--json`; `resume` uses `<STATE>`, repeatable
`--input`, access, gateway, authority and presentation options.

| flag | behavior |
| --- | --- |
| `-o, --output FILE` | exact one-file target; `-` means checked live stdout |
| `-D, --directory DIR` | directory root; conflicts with `-o` |
| `--dry-run` | resolve and freeze the real plan; no payload or native work |
| `--fresh` | discover anew instead of continuing matching unfinished work; no overwrite or cleanup |
| `--restart` | a new attempt for pending receiving work in an existing matching frozen run; retains old attempts |
| `-f, --overwrite` | guarded replacement of a captured incumbent after checks; no intra-run aliases |
| `-s, --checksum ALGO` | optional measurement: `none`, `md5`, `sha1`, `sha256`, `sha512`, `blake3` |
| `--expect ALGORITHM:HEX` | required whole-file digest, checked before publication |
| `--expect-file FILE` | local bounded checksum sidecar |
| `--expect-file-algorithm ALGO` | algorithm for bare sidecar hex; requires `--expect-file` |
| `--tree URL` | explicit tree input; repeatable, alternative to positional references |
| `--select PATTERN` | include original-path pattern; repeatable |
| `--exclude PATTERN` | exclude original-path pattern; repeatable |
| `--select-id TOKEN` | exact literal member token; one per invocation |
| `--all` | explicitly choose every alternative in one catalogue; requires one original input |
| `-H, --header 'NAME: VALUE'` | transient access for one original HTTP/WebDAV input; transport-owned headers are rejected |
| `--ipfs-gateway URL` | HTTP(S) gateway access for exact IPFS inputs; requires `ipfs` |
| `--progress MODE` | `auto`, `bar`, `plain`, `json` or `none`; conflicts with `--json` |
| `--style STYLE` | `classic` (default), `fine` or `solid`; human terminal glyphs and palette only |
| `--json` | versioned records instead of plain lines |
| `--bits` | human speed in bits; byte counts remain bytes |
| `--raw-sizes` | raw byte sizes in human output |

Human presentation is the default. `--progress auto` uses an xprogress ribbon on terminal stderr and
complete plain snapshots when captured. Explicit `plain` updates on a terminal and uses complete lines
otherwise; `bar` also falls back to plain when captured. `--style classic` is the Unicode default;
`fine` and `solid` change only the terminal glyphs and palette. ASCII-locale fallback remains available,
and a nonempty `NO_COLOR` disables color without changing filenames or progress facts.

The active region keeps its ribbon scale fixed within a terminal-width profile, with received bytes,
or sink-accepted bytes for live output, on separate metric rows. Numbers growing do not shrink the
track; resize recomposes the owned region on the next observed update. Rate and ETA use current-attempt
source bytes, never restored bytes. A ranged file uses the same ribbon with its actual windows in
increasing offset order, separated by slim dividers; filled tails do not fill a missing head. A restored
prefix occupies a separate segment, not a new requested window. Whole bodies have one received track,
not invented chunks. `available prefix` separately labels the contiguous acknowledged written prefix,
independent of hashing or apparent file length. Neither the received map nor availability is verification.
Reaching 100% still awaits clean completion, required checks and publication or sink flush. Sizes use
xbytes IEC bytes; `--bits` changes speed only, and `--raw-sizes` wins.

Each successful file has a readable `saved` or `delivered` receipt, distinctly labeled required matches
and optional measurements, and one elapsed-time summary. An identical required/measured digest is
printed once with both roles; different digests retain their own complete values and provenance. No-hash success has no checksum
boilerplate. A locally revalidated file is labeled `revalidated`, with recorded checks and an explicit
warning that its remote version was not checked. Unpinned sources without a matched strong required
checksum carry an actionable warning; a measured digest alone is not an expected-content pin.

`--progress none` suppresses starts and intermediate progress, not receipts, checks, warnings, summaries,
recovery paths or errors. Ordinary successful downloads do not dump state paths or internal events.
Failures preserve acknowledged completed-file counts, available partial/uncertain facts and actual
recovery locations in one diagnosis. `info` and explicit `resume` retain the full state context;
`list` prints actual selector operands, and inspection/preview labels are not execution receipts.
Names and paths are visibly escaped. Copy helpers use POSIX quoting for printable UTF-8 and are labeled
`bash/zsh` when controls or native Unix bytes require ANSI-C quoting.

Download, resume and dry-run presentation stays on stderr. Metadata commands write to stdout; live
stdout remains payload only. JSON is opt-in with `--json` or `--progress json` where supported and
retains its existing events, fields, correlation and byte/second units. Records include
`schema_version: 1`; this README does not define a separate compatibility policy for that envelope.

There is no `--sequential` flag or `--chunks` counter. The common coordinator selects eligible pinned
ranges; `--restart` and `-f` are the separate guarded policies described below.

### Output planning and native publication

With no target flag, files land under `.` using their planned names. `-D DIR` groups collection
members under the destination root and preserves source-relative paths and namespaces where needed.
`-o FILE` requires exactly one selected file with no namespace. A request that resolves to several
files needs `-D`, the default directory target, or a narrower selection.

Native publication in the default command is absent-only unless `-f` is supplied. It requires Unix
hardlink support and a suitable caller-owned parent directory. An existing parent must be owned by the caller, must have
owner read, write and execute permission, and must not be group- or other-writable; missing directory
roots are created with a requested mode of `0700`. Without `-f`, a present destination is an error even
when its contents match. Fresh work checks all frozen targets before opening any payload or creating
run state; publication still refuses a target that appears after that check. To replace a file from a
newly discovered request, use `--fresh --overwrite` deliberately. Clean EOF, the declared length when known, settled writes, sealing and every
required digest precede publication. Non-Unix hosts get a typed unsupported result for native file
publication; `-o -` does not create a native file.

`-f` captures a regular incumbent's device/inode under the cooperating destination lease and checks
that identity again before replacement. A symlink, changed incumbent or alias of another frozen target
is refused. If the destination was absent at capture, a file that appears later is not replaced.
Replacement is an atomic rename under cooperating ownership, not an inode compare-and-swap against
arbitrary noncooperating writers. A failure before commit preserves the incumbent; dropping a waiter
is not rollback once a physical operation has been submitted. An overwrite preview reports requested
replacement but has no execution authority.

### Metadata-only inspection

`--dry-run` performs the real resolution and planning work, then stops. Human output shows `would save`
for each file, with its source path, destination, length, required digests and measurement policy, then
`planned N files; not executed`. For `-o -`, it shows `would stream` with the live target and check
proposals. With JSON selected, these are `Planned` or `PlannedLive` records. Neither mode opens a payload,
creates an output directory, starts a native stage or touches the run store. Both still make source
metadata requests, so dry-run is not an offline mode.

`xget list` (alias `xget ls`) resolves complete bounded metadata and prints member facts without
opening payloads or creating outputs. Entries include the original source path, namespace, length,
member token when supplied by the source, version evidence and any required digests. Human output
prints a copyable `--select-id=...` action only for one original input whose scope supports selection.
An exact S3 input already selects its file; mixed-input listings retain identity tokens without guessing
their original selection scope. A token's presence alone is not selection authority. Directories are
inspection-only and are never materialized as empty outputs. `list` does not assign local destination
names; use `--dry-run` for the frozen plan. `--json` retains the versioned metadata records on stdout.

### Inspect and continue unfinished work

Choose the operation from the evidence you actually have:

| situation | next step |
| --- | --- |
| interrupted request, same references and options | repeat the original command to match unique unfinished frozen work |
| want to inspect a printed state path without network access | `xget info STATE`; metadata only, no content revalidation |
| want a specific saved run | `xget resume STATE --input KEY=REFERENCE` for every key shown by `info` |
| deliberately want fresh discovery | `--fresh`; old work remains, no overwrite authority |
| pending receiving attempt needs a new attempt | `--restart`; frozen members/checks stay, old attempts remain |
| deliberately want to replace an incumbent from fresh discovery | `--fresh --overwrite`; checked guarded replacement, not cleanup |
| the expected digest was wrong | correct it with the trusted value and choose fresh work; replaying the frozen bad check cannot fix it |

Not every failure is resumable. A missing native state, absent strong recovery anchor, changed version,
closed stage or uncertain publication can prevent continuation. Follow the reported diagnosis rather
than treating Restart or Fresh as a repair or deletion command.

Owned file runs keep bounded frozen metadata under the designated output root's `.xget-runs` store.
Repeating a matching request looks for one unfinished run before fresh discovery or HEAD requests. It
reuses that run's members, assignments, versions and check policy. Multiple matches return the actual
candidate paths instead of choosing one; a fully completed prior run is not an unfinished match.
Completed members are revalidated rather than refetched, including when `--restart` or `-f` is supplied.

`xget info <STATE>` is offline. It opens the owned aggregate state path without a Client, transport,
credential discovery, payload hashing or execution lease. It reports each member's binding key,
destination, length, status, measurement and required digests. Human output says `offline metadata only;
content not revalidated`; JSON includes `content_revalidated: false`. Neither grants execution
authority. The state is an owned run record, not an incomplete payload. Old `.xget` partials are not imported, migrated or cleaned automatically.

Usually, repeat the original download request for automatic matching. To choose a particular saved run,
use explicit resume; rebinding is not rediscovery:

```console
xget info downloads/.xget-runs/RUN_ID
xget resume downloads/.xget-runs/RUN_ID \
  --input KEY=https://example.com/big.iso
```

Replace `RUN_ID` and `KEY` with the values printed by `info`. Supply every member key exactly once.
Keys come from `info`; they are not row numbers or selectors. Duplicate, unknown or missing bindings
are request errors. The supplied reference must reconstruct the
frozen original intent, including namespace, spelling and any selection patterns. `resume` does not
search for a newer version, choose a latest asset, or change the destination. `--input` splits at the
first `=`, so a reference may contain further `=` characters.

`--restart` asks the common coordinator to start a distinct attempt only for pending receiving work in
a matching frozen run. It keeps old payloads, control bytes and uncertainty records; it does not reset
or delete them. A missing match is an error before discovery, not permission to fetch latest. Closed,
foreign attempts, missing recorded native state and publication uncertainty are not bypassed by this
flag. Restart
still uses the frozen members, versions, assignments and check policy. It grants no overwrite authority;
add `-f` only when guarded replacement of a pending member's incumbent is intended. The same two flags
are available on explicit `resume`.

`--restart` conflicts with `--fresh`, `--dry-run` and local sidecars. Native local sidecars retain their
separate Fresh/one-original/one-selected-file restriction. `--fresh` and `-f` can be combined for a
freshly discovered checked replacement, but neither implies the other or grants cleanup permission.

State is flushed and atomically replaced, but not fsynced. This covers process interruption, not
power-loss durability. The store retains at most 128 aggregate records. Under capacity pressure it
retires fully acknowledged single-attempt completion metadata, not native workspaces, payloads or
stable locks. A retired completed run's `info` path may become missing. Unfinished runs and histories
with possibly uncertain earlier attempts remain; a store full of that evidence refuses new work rather
than deleting it. Completed metadata whose payload parent has moved does not block matching an
unrelated unfinished request, but selected reentry and direct inspection still validate that parent.
A dropped waiter is not a rollback: submitted physical work may still finish without a returned receipt.
Retain the printed state path and inspect it rather than starting a blind replacement.

Exit status is `0` for success and `130` for an interrupt. Rejected usage, request, selection and layout
errors use `2`; runtime, I/O and library failures generally use `1`.

## Optional providers

All provider features are off by default. The common Client resolves only what its enabled adapters
support and fails unsupported inputs rather than falling back to HTTP.

| feature | input | current limits |
| --- | --- | --- |
| none | exact `http(s)://` | fixed metadata deadline; no automatic redirect, retry, decompression or ambient proxy |
| `s3` | `s3://bucket/key`, `--tree s3://bucket/prefix` | ordered whole objects; no partial-byte range resume |
| `swisstransfer` | SwissTransfer share reference | bounded collection; ordered whole member responses |
| `webdav` | `--tree https://host/path/` | slash-ending DAV root without query or fragment; never website crawling |
| `github` | GitHub release catalogue | explicit `--all`, member or path selection |
| `ipfs` | exact `ipfs://CID[/path]` | ordered whole gateway responses; raw-block CID checks, no UnixFS DAG verification |

For S3, the CLI constructs the AWS SDK from the default configuration when an S3 input is present. The
library by itself performs no credential, profile or instance-metadata discovery. Standard SDK
configuration controls endpoints; the common command has no `--endpoint-url` flag. Credentials and
access policy are the caller's and the SDK's responsibility, not an added xget authentication layer.
S3 metadata is bounded and is not a bucket snapshot.

SwissTransfer, WebDAV and GitHub support are compiled in through their feature flags. They do not add
provider-specific commands: inputs and selections use the same `list`, `--select`, `--exclude` and
`--select-id` surface. No provider-owned scheduler exists.

IPFS uses the same Client and CLI flow. The library's `Client::builder().ipfs_gateway("https://gateway.example")`
and the CLI's `--ipfs-gateway https://gateway.example` select access; the default is `https://ipfs.io`.
The library does not discover gateways from the environment or a local Kubo configuration. A selector
is the canonical CID plus preserved encoded exact path, without `ipfs://`, and must agree with the
original reference. Raw blocks require the CID's supported complete digest even with `-s none`.
DAG-PB/UnixFS file responses are gateway-trusted: a root node's digest is not a flat-file checksum,
and xget does not reconstruct or verify the DAG. Supply a separate whole-file expectation when needed.
Trees and directory paths are unsupported; gateway redirects are not followed. Saved state records
CID/path identity and frozen checks, not the gateway URL, so a later Client can renew gateway access.

The common Client has fixed admission limits: 128 original inputs and 128 combined file and directory
entries per resolution, 1 MiB of accounted retained metadata, and a 15-second deadline for admission
waiting, connection and headers together. It uses up to four active requests or bodies by default.
Retained open bodies consume admission until consumed or dropped. There is no payload or inactivity
timeout. Automatic redirects, retries, content decompression and ambient proxies are disabled for
ordinary HTTP. Unsupported capabilities fail explicitly; no alternate engine is selected.

Exact HTTP planning uses HEAD, never a preview GET. When HEAD explicitly advertises byte ranges, a
known nonzero size and a strong ETag of at most 256 bytes, the common Client may reuse up to four
pinned ranges for an eligible continuation. Every response must repeat the frozen pin and match its
complete requested window and total. A changed pin or ignored range fails without an HTTP fallback.
Other inputs use ordered whole responses. S3 and SwissTransfer do not promise partial-byte resume.

## Library

The workspace contains two packages: root **`xget`** is the reusable library; **`xget-cli`** in
[`cli/`](cli/) installs the **`xget`** executable. The library has no argument parser, terminal renderer
or dotenv setup. Both consumers use the same Client coordinator; there is no legacy command or
compatibility engine. Depend on `xget`, not `xget-cli`, for library use.

### Common Client

For rendered API documentation matching this checkout, run:

```console
cargo doc --locked --no-deps --package xget --open
```

Start with the rendered crate-level guide, then `client::Download`, `Input`, `Inventory` and `SavedRun`;
advanced body/readers come afterward. A version-matched hosted rustdoc URL will require separate
publication; the older docs.rs release is not a guide to this candidate API.
The [crate-level source guide](src/lib.rs) also contains the authoritative recipe combining a
required digest, optional measurement, input-scoped headers, metadata inspection, zero-file handling,
exactly-one selection and consuming the selected `File`. It also links the deeper contracts and shows
how an application supplies its configured S3 SDK client. The guide uses `no_run` examples so doctests
compile them without contacting example URLs; the convenience call below omits required checks deliberately.

For a complete external application, place a checkout named `xget-source` next to a new
`xget-example` directory. Use the candidate commit identified in the artifact provenance when matching
a candidate. Put this in `xget-example/Cargo.toml` and the Rust example below in
`xget-example/src/main.rs`:

```toml
[package]
name = "xget-example"
version = "0.0.0"
edition = "2024"
rust-version = "1.97.1"

[dependencies]
xget = { path = "../xget-source" }
tokio = { version = "1", features = ["macros", "rt", "net", "time"] }
```

Tokio is a direct application dependency: the macro and current-thread runtime need `macros`/`rt`;
`net`/`time` enable the I/O and timer drivers used by HTTP. xget does not create a runtime for you.
Run `cargo run` in that new application directory with an absent `rust-license.txt` destination.
The path form is usable before registry publication; it is not a crates.io installation recipe.
An external application resolves and owns its own lockfile. Commit that lockfile for repeatable builds;
a root Git revision alone does not pin unqualified transitive Git siblings for a new consumer.

```rust
use xget::Client;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    let client = Client::new()?;
    let report = client.download("https://raw.githubusercontent.com/rust-lang/rust/master/LICENSE-MIT")
        .to_file("rust-license.txt")
        .run().await?;
    assert_eq!(report.completed().len(), 1);
    Ok(())
}
```

`Client::new()` performs no network, dotenv, proxy or credential discovery. `download` accepts an
`Input`, and `download_all` accepts several. By default, `.to_file(path)` requires one selected file
and an absent destination; `.to_directory(path)` preserves collection structure.
`.measurement(...)` selects optional hashing, while `.expect(digest)` and
`.require(digest, provenance)` add required checks. `.plan()` returns the frozen assignments without
executing them.

`client.resolve(input)` and `client.resolve_all(inputs)` return bounded metadata inventories.
`inventory.files()` preserves original source paths, member tokens, versions and requirements. Pass a
consumed `File` or `inventory.into_files()` back to `download`/`download_all` to retain collection
context. When inspecting first, handle an inventory with zero files before constructing a new
`download_all` request: an empty original-input iterator is invalid, whereas a genuine empty scope
resolved from an original input is a successful no-op. Directory metadata does not become a file job.
`Input::parse(reference)` recognizes supported references; `Input::tree(reference)` is the explicit
tree form. With `webdav` enabled, use a slash-ending HTTP(S) root without query or fragment, such as
`Input::tree("https://host.example/dav/files/")?`. This is DAV listing, not website crawling.
`Input::include`, `Input::exclude`, `Input::select_id` and `Input::all` are the
library selection entrances. `Download::all()` explicitly accepts a complete catalogue of alternatives.

The Client clones share request and body admission and connection reuse. Use a caller-owned Tokio
runtime with I/O and time enabled. See the [`client` module](src/client.rs) for the full resolution,
layout and failure contracts.

### Borrowed execution observers

`client::Download::run_with`, `Client::execute_with` and `client::Resume::run_with` borrow an observer
on the executing task. It can hold local state without `Send`, `Sync`, `Clone` or `'static` bounds:

```rust
use xget::{Client, client::{Event, Observer}};

struct CountFiles<'a>(&'a mut usize);

impl Observer for CountFiles<'_> {
    fn observe(&mut self, event: Event<'_>) {
        if let Event::FileCompleted { .. } = event {
            *self.0 += 1;
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    let client = Client::new()?;
    let mut completed = 0;
    let mut observer = CountFiles(&mut completed);
    let report = client.download("https://example.org/file.bin")
        .to_file("file.bin").run_with(&mut observer).await?;
    assert_eq!(completed, report.completed().len());
    Ok(())
}
```

Events distinguish frozen metadata, byte-work facts, individual publications and the returned run
outcome. `ProgressFacts` separates logical totals, validated restored bytes and current-attempt work.
Its `available()` is the contiguous acknowledged settled prefix, independent of hashing: range tails
do not fill a head gap. `windows()` exposes at most four actual offset-ordered current-attempt reader
windows, with received and written counts, extent certainty and reception state. Whole ordered bodies
have no synthetic windows. `Reception::CleanEof` describes reception only, not checks or publication.
These facts are observations, not persisted permission to reuse bytes or a guarantee against concurrent
owner changes. Hash counts are zero when no checks are active. Reaching a length proves neither EOF nor
matched checks. Reconciled publications emit no invented attempt progress; a file completion can precede an
aggregate checkpoint failure, so use the returned terminal result to judge the whole run.

Callbacks must return promptly. Drop or callback panic promises no terminal callback, rollback or
successful pause; submitted physical work retains its ownership until settlement. Names and paths
need the caller's escaping/redaction policy. Ordinary `run` and `execute` use the same path with a no-op
observer; the CLI builds its native progress reporting on these events.

### Owned runs

`SavedRun::inspect(path)` is offline and exposes safe member binding keys. Continue with
`client.resume(saved)` plus one `.input(key, original_input)` per member, then `.run()`. References must
match frozen intent. Completed files are revalidated by recorded strong content and native identity, not
by name, size, modification time, or equal foreign bytes. A completion without a strong recorded anchor
returns `NotRevalidatable` rather than inventing a fingerprint. Weak-only and no-hash policy cannot
create a recovery fingerprint. Use `.fresh(Fresh)` for deliberately new work; it is not overwrite or
cleanup. The ordinary `Download` and `Resume` builders separately accept `.restart(Restart)` for a new
owned receiving attempt and `.overwrite(Overwrite)` for guarded replacement. Neither marker can grant
reconciliation authority to a no-hash completion or turn a closed attempt back into receiving work.
`Client::execute(plan)` and the public advanced transfer entrances keep their absent-only defaults.

See [`client::SavedRun`](src/client/saved.rs) for saved-state formats, binding errors and the precise
revalidation rules.

### Pre-release API migration

`MemberToken` is no longer `Copy`. Borrow its selector text with `as_str`, or make an explicit cheap
clone when retaining it. Its meaning remains provider-scoped; it is not an occurrence-local `FileId`,
a credential or proof that remote bytes are immutable.

Ordinary whole-body failures now use `client::SourceError` around the concrete built-in transport
causes. Code explicitly naming `Failure<HttpError>` or the corresponding live-body failure must use
the ordinary source-error family instead. Exact-HTTP range-reader errors retain their separate HTTP
family. Earlier root `Source`, `get`, `from`, `Download`, `Output`, `Report`, `Probe`, `Progress`,
`ByteRange`, `ByteStream`, `Error` and control-inspection APIs are no longer compiled or exported.
Use `Client` for remote references and `Transfer` for application-owned bodies or repeatable readers;
plans and reports belong to those operations. `Checksum` parsing remains available for CLI and caller
measurement selection. The old engine and test sources remain physically unchanged, excluded from the
package and automatic test discovery rather than deleted or exposed as a compatibility feature.

Textual `BindingKey` and `RunId` values are versioned workspace correlation, not execution authority.
A parsed key still needs an inspected matching run and original input binding. Moving a workspace does
not promise to preserve its identifier. Older records stay readable but may lack a reconstructable
selection recipe or receiving authority; inspection does not invent either. No active legacy partial
is migrated, overwritten or imported automatically.

### Advanced transfer entrances

[`Transfer`](src/transfer.rs) is the checked native publication layer used by the common Client. It
accepts an already-open ordered body, zero or more whole-file requirements, and an optional separate
measurement. Required checks, clean EOF, settled writes and the declared length precede default
absent-only publication. Ordinary Client builders expose separate guarded replacement authority; the
public advanced entrance does not inherit that permission.
`Transfer::from_reader` supports borrowed repeatable range sessions, and
`Transfer::from_body` supports checked live output. Deep contracts and runnable examples live with the
modules:

- [`transfer::reader`](src/transfer/reader.rs) for borrowed range readers and resumed prefixes.
- [`transfer::live`](src/transfer/live.rs) for released prefixes, receipts and writer settlement.
- [`tests/transfer.rs`](tests/transfer.rs) for a complete native publication call.
- [`tests/transfer_live.rs`](tests/transfer_live.rs) for live output and backpressure cases.

These entrances do not add a scheduler, source reopening, automatic cleanup, fsync, or a universal
filesystem promise. Native publication and local sidecar opening require Unix support.

## How it works

### Common Client

Resolution debits bounded metadata budgets before selection, then freezes each member, its namespace,
version evidence, requirements and destination. Execution handles files serially. For eligible exact
HTTP, a range reader follows the contiguous settled-write frontier while bodies run; the verifier does
not wait for the slowest range before checking earlier bytes. Eligible continuation revalidates a
recorded strong prefix and fetches only the suffix. Other inputs use ordered whole responses.

The native stage writes private partial data, enforces checks and length, seals the result, and only
then publishes. Publication is absent-only unless guarded replacement is explicitly requested with `-f`.
A required-check failure therefore
leaves no newly published file. The destination's parent must already satisfy the ownership and mode
rules, except for missing directory roots created during execution.

## Historical harness and media

The retained [fault harness](harness/README.md), GIFs and [`media/download.tape`](media/download.tape)
describe the retired command surface. They are historical artifacts, not runnable modern CLI examples,
a current reproducibility recipe or evidence for the current Client path. Do not run the old scenario
suite or regenerate the recording against the modern executable; no flag-compatibility mode exists.

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option.
