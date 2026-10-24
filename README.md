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

### Borrowed Client observers

`client::Download::run_with`, `Client::execute_with` and `client::Resume::run_with` accept a
mutable, borrowed `client::Observer`. Callbacks run synchronously on the executing task, so an
observer can borrow local state without `Send`, `Sync`, `Clone` or `'static` bounds:

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

# async fn example() -> Result<(), Box<dyn core::error::Error>> {
let client = Client::new()?;
let mut completed = 0;
let mut observer = CountFiles(&mut completed);
let report = client.download("https://example.org/file.bin")
    .to_file("file.bin").run_with(&mut observer).await?;
assert_eq!(completed, report.completed().len());
# Ok(())
# }
```

Events distinguish frozen run/file metadata, byte-work facts, individual acknowledged publications,
and the returned run outcome. `ProgressFacts` separates logical received/written/hashed bytes from
validated restored bytes and current-attempt source/write counts. Hash counts are zero when no
checks are active; counts reaching a length do not prove clean EOF, matched checks or publication.
Revalidated prior publications emit completion without invented attempt progress. A file completion
can precede an aggregate checkpoint failure, so use the terminal result to judge the whole run.

Callbacks must return promptly; slow callbacks delay execution. There is no reporting worker or event
queue. Dropping the future promises neither a terminal callback nor rollback or a successful pause;
submitted native work retains its ownership until settlement. Metadata paths and names need the
caller's own escaping/redaction policy. Existing `run` and `execute` methods follow the same path
with a no-op `()` observer. This library API does not change the legacy CLI reporter or live opening.

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

By default the destination must be absent in a suitable existing caller-owned parent directory. Supply a digest
with `.expect(digest)` when the caller has a whole-file requirement; optional SHA-256 measurement alone
is not an expected-content check. `download_all(inputs).to_directory(path)` plans multiple exact inputs,
and `.plan().await` exposes frozen read-only assignments before `client.execute(plan).await`. Planning
uses HEAD, never a preview payload GET, and creates no output directories.

This ordinary Client supports exact HTTP inputs, optional bounded SwissTransfer collections, serial
files and owned frozen-run continuation. Exact HTTP uses up to four parallel windows when HEAD
explicitly advertises byte ranges, a known nonzero size and a strong ETag of at most 256 bytes.
Otherwise it keeps ordered whole-response execution. Every range must repeat the frozen pin and match
its complete requested window and total; ignored ranges or changed pins fail without fallback.
`Client::open` and SwissTransfer remain ordered whole responses. Unsupported inputs fail explicitly. Redirects,
automatic retries, content decompression and ambient proxies are disabled. Clones share request/body
admission and connection reuse. Each request has a fixed 15-second deadline for admission waiting,
connection and headers together, but no payload/inactivity timeout. Retained open bodies can exhaust
admission; drop or consume them to release capacity. Use a caller-owned Tokio runtime with I/O and
time enabled. See the [`client` module](src/client.rs) for selection, version and failure contracts. The CLI still uses the
legacy engine below; its feature flags do not enable new Client provider adapters.

### SwissTransfer collections (library feature)

Enable `swisstransfer` on the library dependency. The compiled legacy CLI has not adopted this adapter;
there is no SwissTransfer CLI feature or provider-specific scheduler.

`client.resolve(Input::parse(share)?)` returns a complete bounded inventory without opening object
payloads. Inspect `inventory.files()`, then pass a consumed `File` or `inventory.into_files()` back to
`download`/`download_all`; the original collection context and nested paths survive that selection.
Materialization selects all members by default, including a genuine empty no-op. A zero-byte member
is still a real file. Literal `File::member_token()` values support repeatable ID selection:

```rust
use std::path::Path;
use xget::Client;

async fn download_member(client: &Client, share: &str, token: &str, root: &Path)
    -> Result<(), Box<dyn core::error::Error>>
{
    let report = client.download(share).select_id(token)
        .to_directory(root).run().await?;
    assert_eq!(report.completed().len(), 1);
    Ok(())
}
```

A token is the adapter's share-scoped file UUID, not an inventory row number, path or access URL.
Stale IDs fail. Path includes and exclusions can further narrow the original member paths, as below.
`to_file(path)` requires exactly one selected file. Mixed exact/share inputs use the same frozen plan and serial
checked native execution. Singleton shares retain nested paths. Multiple unnamed shares have the
honest readable fallback `SwissTransfer`; equal groups fail rather than merging or gaining opaque
suffixes. Use `Input::under("share-one")` and distinct namespaces for multiple shares.

Discovery is limited to 128 total members and 1 MiB of accounted retained metadata per request,
including members removed by selection. Share HTML is capped at 2 MiB, access metadata at 64 KiB.
The shared 15-second async discovery deadline is not CPU preemption of synchronous parsing. Supported
bootstrap shapes must be complete; missing, denied or unsupported metadata is an error, not a truncated
executable inventory. No source title field is claimed.

Access is acquired only when opening a selected member. The whole identity response must match the
manifest length and carry a strong ETag. Its first pin is checkpointed before native submission;
subsequent access must preserve it. Only an expired lease rejected during opening can authorize one
renewal, not a live denial or a healthy body's later expiry. Retained records contain no signed object
URLs or cookies. Frozen-run continuation uses original occurrence/member mappings and fresh access,
without manifest rediscovery or partial-byte reuse. Other provider catalogues and trees remain separate
work.

### Common Client S3 objects and prefix inventories

Enable the library's `s3` feature and supply a caller-configured `aws_sdk_s3::Client`. Construction
loads no AWS configuration, profiles or `.env`; the supplied SDK owns credentials, endpoint, retry and
connection policies. These are not the HTTP adapter's defaults. The legacy CLI above is unchanged;
this section describes the common Rust Client, not new CLI tree flags.

```rust
use xget::{Client, Input};

async fn fetch_prefix(sdk: aws_sdk_s3::Client)
    -> Result<(), Box<dyn core::error::Error>>
{
    let client = Client::builder().s3_client(sdk).build()?;
    let input = Input::tree("s3://example-bucket/archive/")?;
    let inventory = client.resolve(input).await?;
    for marker in inventory.directories() {
        println!("directory metadata: {}", marker.source_path());
    }
    if inventory.files().len() == 0 {
        return Ok(());
    }
    client.download_all(inventory.into_files())
        .to_directory("downloads").run().await?;
    Ok(())
}
```

`Input::parse("s3://bucket/key")` is exact: HEAD metadata followed by a whole conditional GET, never
listing after missing/denied lookup. A known versionId and opaque strong ETag are preserved separately
from key identity and payload requirements. An ETag is not an expected MD5 or recovery content anchor.
S3 stays whole mode, with no partial-byte reuse. Frozen continuation opens conditional whole bodies
without relisting or a new HEAD; checked native publication and owned recovery are the common path.

`Input::tree` requests a literal prefix and complete bounded ListObjectsV2 pagination. Listing is not
a point-in-time snapshot. Each object's listing length/ETag must agree with its conditional HEAD, then
its GET must preserve that HEAD evidence. Source paths are relative to the prefix's last complete slash
ancestor: `dir/fo` lists `dir/foo` as `foo`; the genuine member selector remains `dir/foo`. Full-key
selectors are provider-scoped, not synthetic URLs or occurrence-local FileIds.

Zero-byte slash-ending objects are genuine inspection-only `inventory.directories()` entries. They
share count/text budgets with files, but do not create file jobs or empty directories. Directory-only
unconfigured/exclusion-only requests are zero-file no-ops with no output root. Nonempty slash-key
payloads fail explicitly; request that exact object with a safe explicit file target instead.

`resolve_all` shares one 128-original-input bound, 128 combined discovered file/directory entries,
1 MiB accounted metadata and a 15-second async discovery deadline across inputs. Each S3 metadata
response has a separate enforced 2 MiB HTTP body-data cap before SDK collection/XML deserialization,
including chunked responses. HEAD Content-Length still describes object size; successful GET payloads
are not metadata-capped. Each listing asks for 129 keys and accepts at most 128 pages, with real,
nonrepeating continuation tokens. These limits are not a total transport/RSS or parser CPU guarantee.
Client clones share request admission, SDK/HTTP pools and body ownership, not run identity.

Pre-release API changes: `MemberToken` is no longer Copy; use an explicit cheap Clone or borrow its
provider-scoped value. Ordinary whole-body source failures use `client::SourceError` around concrete
HTTP/S3 causes; explicit `Failure<HttpError>` annotations need updating. HTTP range-reader and existing
legacy Source/get/from/Download/Output/Report families retain their separate contracts.

### Bounded WebDAV trees

Enable the library-only `webdav` feature and use an explicit tree input. An ordinary HTTP URL remains
exact; failure never turns it into a crawl:

```rust
use xget::{Client, Input};

async fn fetch_dav() -> Result<(), Box<dyn core::error::Error>> {
    let client = Client::new()?;
    let input = Input::tree("https://example.org/dav/archive/")?;
    let report = client.download(input).to_directory("downloads").run().await?;
    println!("published {} files", report.completed().len());
    Ok(())
}
```

The root must end in `/`. Discovery uses complete Depth-1 PROPFIND responses, then traverses the
returned immediate collections under the same origin and root. Absolute URI-path hrefs and same-origin
absolute URLs are supported; relative hrefs, redirects, query/fragment access, dot paths, encoded
separators, aliases and incomplete property/status responses fail explicitly. Caller-supplied userinfo
in the original root is explicit access configuration for that same origin. Returned hrefs may not
supply credentials or change origin. Credentials are neither inferred nor persisted in saved records;
the exact original reference must be resupplied for continuation, so URL-userinfo rotation is not
supported. HTTP sends credentials without encryption; use HTTPS except for an intentionally trusted
HTTP endpoint.

Discovery shares the Client's count/text/admission limits and 15-second async deadline. Additional
bounds are 128 listings, 64 source components, 256 KiB per control body, 1 MiB total control bytes and
16384 XML nodes per document. DTD processing is disabled. These are input/work bounds, not a hard
synchronous CPU deadline, total RSS guarantee or point-in-time tree snapshot.

`Inventory::directories()` retains inspection-only `WebDavCollection` facts; empty directories are not
restored or persisted as jobs. A genuine directory-only request is a zero-file no-op. Literal file
selectors use genuine scoped href paths; path patterns match decoded original relative source paths.
Whole GETs must match the frozen known length and any discovered strong ETag. Without a strong pin,
consistency remains `Unpinned`; optional hashing is not an expected digest or source authenticity.
There is no WebDAV range assembly or partial-byte reuse.

V4 saved records retain the member href/source path and check/version evidence, never the access root
or credentials. Explicit continuation rebinds the original root and opens the frozen member directly,
without another PROPFIND, HEAD or selector replay. Native reconciliation still requires its recorded
strong content and filesystem identity. The compiled legacy CLI is unchanged in this prefix.

### Include and exclude original paths

Use Input-local intent when one request combines multiple references. Download's `include`/`exclude`
conveniences require exactly one original input, not merely one resulting file:

```rust
use std::path::Path;
use xget::{Client, Input};

async fn download_selected(client: &Client, reference: &str, root: &Path)
    -> Result<(), Box<dyn core::error::Error>>
{
    let input = Input::parse(reference)?.include("**/*.bin")?.exclude("private/**")?;
    let report = client.download(input).to_directory(root).run().await?;
    println!("published {} files", report.completed().len());
    Ok(())
}
```

Patterns match the whole ORIGINAL source-relative path, case-sensitively by Unicode scalar. `*` stays
within one component, `?` consumes one scalar, and a complete `**` component consumes zero or more
components. Every positive include must match at least one file in the complete original scope before
exclusions; duplicates may match the same file. Matching includes form a union; no includes means all
paths. Exclusions apply afterward. Namespaces,
normalized collision keys and assigned filenames are not matching input. A remote exact-HTTP ancestor
such as `con` is routing data, not a local filename; its spelling does not prevent an explicit safe
output. Actual destination names still undergo the same conservative local validation. Collection
hierarchies are validated completely even when patterns would exclude an unsafe member.

This deliberately small grammar has no bracket/brace/extglob forms, escapes, absolute/dot paths,
backslashes, colons or control characters in patterns. Fixed limits per input are 64 includes and
exclusions combined, 1024 UTF-8 bytes per spelling, 64 components and 16384 combined spelling bytes.
Configured matching also requires each original source path to fit 4096 UTF-8 bytes and 64
slash-separated components. Exceeding that separate source-path ceiling is an inventory-limit error,
even for one exact HTTP input with an explicit output name. Pattern text participates in the existing
request/discovery metadata accounting. Discovery remains complete and bounded before filtering; exclusions cannot hide an over-limit inventory. A genuinely
empty discovered inventory remains a no-op only without positive criteria. A positive on an empty
file scope, or filtering a nonempty input to no files, is an error even if another input still matches.
Literal identities are validated against complete metadata before path subtraction, and the complete
accounted charge survives both filters. Selecting a frozen File subset preserves already-proved
criteria; newly appended includes require matches in that subset.

Automatic continuation matches the original ordered pattern intent before discovery and restores its
frozen members without rerunning patterns against changed metadata. Explicit `resume(...).input(...)`
bindings must reconstruct the same original pattern intent and namespace, as well as the same original
reference. New V4 records can retain the bounded nonsecret scope/namespace/ordered-pattern recipe,
with separate original-reference-only and full binding fingerprints. `SavedMember::binding_input`
checks a caller-supplied original reference and reconstructs that intent without discovery or pattern
reevaluation; pass its Input to `Resume::input`. Older/default records reconstruct only when their full
binding agrees; complex older intent returns an actionable need-original-intent error rather than
guessing. No raw reference, credentials or renewed access is recovered from disk. Unconfigured
request fingerprints retain their existing byte streams. This is library selection; the compiled
legacy CLI has not adopted these flags.

### Owned run continuation

Ordinary file runs retain bounded frozen metadata under the designated output root's `.xget-runs`
store. A repeated matching request first looks for one unfinished run, before issuing fresh HEAD
or collection-discovery requests. It keeps that run's members, assignments, versions and check policy. Multiple matches return
explicit actual resume paths rather than choosing by timestamp; a fully completed prior run is not an
unfinished match. `download(...).fresh(xget::client::Fresh)` deliberately bypasses continuation, but
neither overwrites existing outputs nor deletes old work.

Use the actual path from `report.saved_run()` or a failure's partial report. `SavedRun::inspect(path)`
is offline: no Client, credentials, network request, payload hashing or execution lease. Its safe
`members()` expose run-scoped binding keys. Continue with `client.resume(saved).input(key, original_input)`
for each frozen member, then consume `.run().await`. Exact original reference intent, including namespace,
query and spelling, must match; cross-reference renewal is not guessed. A selected File supplied as a
binding must also agree with the frozen version/length and cannot add or replace required checks.
Raw references, credentials and foreign transport errors are not saved in the record. Ordinary targets
and saved-state locators containing `..` are refused; use a resolved path rather than relying on lexical
parent reduction across a possible symlink.

Completed files are revalidated using their recorded strong content anchor and native identity, not
filename/size/mtime or equal foreign bytes. A completion without a strong recorded anchor returns
`NotRevalidatable`; it does not trigger a hidden hash. Ordered pending files, including SwissTransfer,
use fresh whole responses with frozen strong ETag enforcement when available; their old partials remain
untouched. Eligible exact-HTTP range runs instead retain their frozen mode and native reader state.
Reentry checks that state and rehashes an already-recorded strong proper prefix before fetching only
the suffix, without a fresh HEAD. Weak/no-hash, missing, unanchored, foreign and closed receiving
states do not authorize reuse or silently fall back to refetching. Use the explicit `Fresh` entrance
for deliberately new work, not overwrite or cleanup. V1/V2 aggregate records keep whole-response
semantics; V3 records can freeze range mode. V4 additionally stores genuine S3 key/version evidence
and optional bounded nonsecret binding intent; it does not persist directory jobs or reader authority
for S3. A fresh selected File's changed range advertisement is
not new request intent and cannot upgrade/downgrade that stored mode or bypass unfinished work.

`PublishedFile::range_stats()` reports restored bytes and current-attempt source/acknowledged writes.
Its native receipt counts the logical assembled file. Offline-reconciled completions have no invented
current-attempt range statistics. Ordinary Client cancellation here is future drop, not a successful
cooperative-pause API.

Aggregate ownership excludes cooperating same-root aggregate runs while active and stays with submitted physical work after
a waiter is dropped. Drop is not rollback: publication can finish without a returned receipt. Retain the
actual run path and inspect/continue it rather than starting a blind replacement. State is flushed and
atomically replaced, not fsynced; process-interruption recovery is not power-loss durability. The store
is bounded and has no automatic cleanup. Explicit Restart and Overwrite are described below; legacy
import remains unsupported.
See [`client::SavedRun`](src/client/saved.rs) for the precise binding/error contracts.

### Guarded Restart and Overwrite (library)

These are separate execution permissions in `xget::client`, not new CLI behavior:

```rust,no_run
use xget::{Client, client::{Overwrite, Restart}};

# async fn example() -> Result<(), Box<dyn core::error::Error>> {
let client = Client::new()?;
// Requires one matching unfinished frozen run. It never discovers a replacement request.
let report = client.download("https://example.org/file.bin")
    .to_file("file.bin").restart(Restart).overwrite(Overwrite).run().await?;
# Ok(())
# }
```

`Download::restart(Restart)` and `Resume::restart(Restart)` create new native attempts for pending
receiving work. Old data, control records, unconfirmed extents and attempt history remain intact.
The old receiving witness must match the frozen destination, check policy, filesystem identities and,
for ranges, the exact nonclosed session. Restart does not reuse bytes or require a proper-prefix
anchor. Restarting an existing attempt still needs a frozen strong version or required strong digest
rather than optional measurement. Closed, sealed, publication-intent, foreign and older one-shot states without recorded
receiving authority refuse. Completed members are strongly revalidated, not refetched; nohash or
weak-only completion still returns `NotRevalidatable`.

A Download restart with no unique unfinished match fails without HEAD or discovery. Combining Fresh
and Restart is a sticky request error in either order, and Restart preview is refused. Fresh is new
intent, not cleanup, restart or overwrite. An ordinary preview Plan carries no replacement authority;
`Client::execute`/`execute_with` and public advanced Transfer entrances remain absent-only.

`Download::overwrite(Overwrite)` and `Resume::overwrite(Overwrite)` explicitly allow replacement only
for the consumed execution. Native acquisition captures a nonsymlink regular incumbent descriptor
under the destination-directory lease and checks its identity and the complete frozen run target
set again at commit. Inode aliases of another frozen target refuse. Other hardlinks can remain as
older native witnesses; replacing one destination name does not rewrite those witnesses. If capture
found no destination, a later foreign file is not silently upgraded into replacement permission.

Clean EOF, declared length, settled writes, sealing and all requirements still precede publication.
Wrong checks, late body errors and a stop observed before commit preserve the incumbent. Replacement
atomically renames a private link to the checked new inode and retains the native data witness. This
is a cooperating-writer guarantee, not arbitrary-writer inode CAS or an all-file transaction. Dropped
submitted work retains descriptors and leases through settlement and may finish without a receipt.
Lost acknowledgement stays explicit; reconciliation still needs recorded strong content and exact
new-inode evidence, never equal foreign bytes or a hidden hash. Supply Overwrite again for continuation;
a saved incumbent is evidence, not permission. Native V4 records persist receiving and Closed intent
before sealing can take effect, while preserving older native readers. These native records are
separate from the existing aggregate V4 provider format. No cleanup, fsync, successful pause or
power-loss durability is added.

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
publication. Failures retain typed causes and partial facts.

Retain a clone of the pending operation's `state()` before starting. A later
`Transfer::from_reader(&reader).resume_from(state)` can continue a receiving stage with a recorded
strong proper-prefix witness. It reacquires both native leases, checks the frozen size, representation,
version, destination, policy and file identity, and rehashes the prefix before fetching only the suffix.
The witness uses already-configured SHA-256/SHA-512/BLAKE3 work; weak-only or no-hash policy does not
silently gain a recovery hash. Even a previously received full payload must establish actual suffix
EOF. Sealed, mismatched and published stages never reopen writable. Older one-shot records remain
inspectable through reconciliation, not reader continuation.

`run_until(signal)` stops source admission and settles submitted work. A stop during sealing settles
the seal without starting publication; its closed state is not reader-resumable. Once publication is
submitted, stopping cannot undo it. `receipt.stats()` separates restored bytes from this attempt's
source bytes and acknowledged payload writes. The ordinary exact-HTTP Client reuses this coordinator
for eligible pinned ranges; custom readers remain a distinct application-owned entrance. Neither has
replaced the legacy CLI engine.

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
