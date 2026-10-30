# Use xget from Rust

For the shortest download example, see the [README](../README.md#use-from-rust). This guide
covers application setup, inspection and selection, observers, continuation and advanced transfers.

The workspace contains two packages: root **`xget`** is the reusable library; **`xget-cli`** in
[`cli/`](../cli/) installs the **`xget`** executable. The library has no argument parser, terminal renderer
or dotenv setup. Both consumers use the same Client coordinator; there is no legacy command or
compatibility engine. Depend on `xget`, not `xget-cli`, for library use.

## Application setup

For rendered API documentation matching this checkout, run:

```console
cargo doc --locked --no-deps --package xget --open
```

Start with the rendered crate-level guide, then `client::Download`, `Input`, `Inventory` and `SavedRun`;
advanced body/readers come afterward. A version-matched hosted rustdoc URL will require separate
publication; the older docs.rs release is not a guide to this candidate API.
The [crate-level source guide](../src/lib.rs) also contains the authoritative recipe combining a
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
runtime with I/O and time enabled. See the [`client` module](../src/client.rs) for the full resolution,
layout and failure contracts.

## Borrowed execution observers

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

## Owned runs

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

See [`client::SavedRun`](../src/client/saved.rs) for saved-state formats, binding errors and the precise
revalidation rules.

## Pre-release API migration

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

## Advanced transfer entrances

[`Transfer`](../src/transfer.rs) is the checked native publication layer used by the common Client. It
accepts an already-open ordered body, zero or more whole-file requirements, and an optional separate
measurement. Required checks, clean EOF, settled writes and the declared length precede default
absent-only publication. Ordinary Client builders expose separate guarded replacement authority; the
public advanced entrance does not inherit that permission.
`Transfer::from_reader` supports borrowed repeatable range sessions, and
`Transfer::from_body` supports checked live output. Deep contracts and runnable examples live with the
modules:

- [`transfer::reader`](../src/transfer/reader.rs) for borrowed range readers and resumed prefixes.
- [`transfer::live`](../src/transfer/live.rs) for released prefixes, receipts and writer settlement.
- [`tests/transfer.rs`](../tests/transfer.rs) for a complete native publication call.
- [`tests/transfer_live.rs`](../tests/transfer_live.rs) for live output and backpressure cases.

These entrances do not add a scheduler, source reopening, automatic cleanup, fsync, or a universal
filesystem promise. Native publication and local sidecar opening require Unix support.

## How it works

### Resolution and execution

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
