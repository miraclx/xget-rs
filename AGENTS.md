# AGENTS.md

`xget` is a reusable fetch library. `xget-cli` is the workspace's command-line package and installs the
`xget` executable. Both use edition 2024 and the pinned Rust 1.97.1 toolchain.

- Read `STYLE.md` before writing code. It is the contract, including the narrow private-pipeline
  exception. `../craft/REVIEW.md` defines review evidence, severity and closure vocabulary.
- `notes/craft-backlog.md` tracks open work. Historical reviews are not rewritten when a later fix lands.
- The approved redesign is broader than the implemented surface. Do not call the ordinary Client,
  inventory, multi-input selection, provider adapters or new recovery UX implemented until they exist.

## Workspace boundary

- Root package `xget`: `src/`, library integration tests in `tests/`, transport sources and the advanced
  native `transfer` module. No argument parser, progress renderer or dotenv setup belongs here.
- Package `xget-cli`: `cli/src/`, executable `xget`. Its single-command `main.rs` retains the documented
  module-tree exception. That is not permission to add provider orchestration to main.
- The old working files under `src/bin/xget/` are preserved provisional work, excluded from root binary
  discovery and packaging. They are not the compiled CLI. Do not accidentally add their SwissTransfer
  feature, synthetic file locators or provider-owned scheduler through a move or broad staging operation.
- No `mod.rs`. Unit tests stay in sibling `*_tests.rs` files. Private child tests use `#[path]`; do not
  widen production visibility or move their declarations to the crate root just for access.

The existing `Source`, `get`, `from`, `Download`, `Output` and `Report` library APIs remain available.
`Source` uses native async trait methods and is not object-safe. The CLI's concrete enum dispatch is
intentional; source futures need not be Send.

## Two dependency graphs

The library uses Git `xbytes`; the CLI uses Git `xprogress` and the library. The ignored local
`.cargo/config.toml` overrides both siblings through paths. Local working-copy success and locked-Git
success are different evidence. A sibling commit or a green CI run does not establish both.

Run Cargo from an isolated export's own cwd when checking the pinned graph. `--manifest-path` alone
still inherits configuration from the caller's cwd. Inspect actual metadata sources/revisions. Do not
silently advance a dependency pin to an unrelated or unavailable revision. Check xget and Freyr callers
before removing a public sibling API; Freyr uses xprogress rows, block drawing and Dump.

## Gates

Before each commit, validate the exact staged prefix, not merely the larger working tree. Keep build
and install targets outside checkout; do not overwrite a user's executable.

```console
cargo test --workspace --all-targets
cargo test --workspace --doc
cargo clippy --workspace --all-targets -- -D warnings
cargo +nightly fmt --check
cargo sort --workspace --check
```

Also run individual source features and all features, including doctests and strict Clippy, on both
actual dependency graphs. Warning-denied docs, an external library consumer, local CLI installation and
actual executable smoke checks are workspace-boundary gates. Package listing must exclude the old CLI
and provisional provider tree from the library package.

Rust 1.97.1 is the exercised support baseline. The previous advertised 1.85 was not proven. Do not
claim a lower MSRV without the corresponding toolchain/dependency/feature validation.

## Legacy engine invariants

**Verify as bytes arrive.** The in-order verifier runs alongside fetchers on one task, chasing a
contiguous frontier. Received progress is not verified progress. The Barrier/Notify regression in
`tests/engine.rs` forces tails before a partial head and verifies that head before its body can finish.
Do not replace this with a hash pass that starts only after the slowest chunk completes.

**The partial is not the finished file.** `<output>.xget` contains data, a text trailer and a 32-byte
`XGETCTL1` footer. `Writer::finish` removes trailer/footer before rename. Fresh allocation and truncation
belong to `Writer::create`, before control writing; `Writer::open` preserves resumed data and control.

**Resume is automatic.** `--restart` opts out. When both validators exist they must agree; when either
is missing, legacy resume falls back to length and cannot detect a same-length change. Five unit cases
pin that policy. Do not import this fallback into stronger new version/identity guarantees.

**No fsync.** Flushing covers process interruption, not power-loss durability. Do not invent a stronger
crash guarantee. The subtractive over-serve check, `len > range.end - offset`, avoids integer wrap; do
not replace it with a comparison that admits a wrapped offset.

**Scratch ownership.** Temporary stdout/pipe/discard scratches are `xget-<pid>-<counter>.xget` and are
removed by their guard. Startup sweeping only targets that temporary naming pattern with dead PIDs.
Persistent file partials must never be swept. Test actual CLI runs with their own empty cwd/TMPDIR.

## Advanced native-file invariants

Body-based `Transfer` is separate from the ordinary Client and legacy CLI. Same-algorithm requirement
conflicts are sticky before effects; agreeing values merge bounded provenance. All configured checks
share one traversal, independently of optional measurement. SHA-256 measurement is the default; None
cannot disable requirements, and no requirements plus None performs no hashing.

Clean EOF, declared length when known, settled writes, sealing and all required matches precede
absent-only publication. Existing destinations are not overwritten, even when their contents match.
Reports separate matched requirements from measurement. Advanced failures are concrete typed boxes,
not erased causes; borrowed/local values and conditional native Send remain supported.

The final physical-operation registration owns both leases/descriptors through queued and running
blocking work. Dropping a waiter cannot unlock them early. `FlockLease` explicitly unlocks at final
owner destruction, with acquiring-PID protection; incidental inherited descriptor references must not
prolong a settled parent's authority or let a child's destructor unlock a live parent. Stable lock names
are never unlinked. Release notification follows guard cleanup.

Reconciliation requires recorded strong content and inode/device evidence; equal foreign bytes are
not adopted, and absent destinations are not republished. Matching identities without a strong recorded
anchor return NotRevalidatable, with no hidden fingerprint. MD5/SHA-1 are compatibility checks, not
strong guards. V2 state writes remain bounded and preserve the V1 reader; legacy XGETCTL1 is unchanged.
Keep publication uncertainty and unconfirmed write extents
explicit. No one-shot source reopening, automatic cleanup, fsync or universal filesystem promise exists.
Unix hardlink support, a caller-owned Tokio runtime and a suitable existing caller-owned parent are
required. Non-Unix returns a typed unsupported result. Windows compilation is not native runtime proof.

## Transport scope

HTTP validates the full requested response window and declared length. The current S3 policy instead
trusts a matching Content-Range start, otherwise matching response length; the engine checks final
chunk completion. The seven actual SDK loopback tests pin that limited policy, not strict HTTP parity
or version continuity. Their client uses explicit fixture credentials/region/endpoint without ambient
AWS configuration or metadata discovery. Do not substitute a handwritten response struct for SDK I/O.

`s3` and `ipfs` gate library sources; the CLI forwards those capabilities. The root's vestigial
aws-credential-types/aws-smithy-types feature aliases do not independently enable S3 code. SwissTransfer
is a later common-library adapter, not a feature of this workspace boundary.

## CLI contract and known carries

- `-s` means checksum, not curl's silent flag. Quiet output uses `--progress none`.
- Human sizes and default rates use bytes. `--bits` changes human speed only; raw byte counts win.
  `DisplayFormat` owns that policy. JSON counts/rates remain bytes/bytes-per-second, ETA seconds and
  end elapsed milliseconds, independent of human flags.
- JSON events are progress, retry, end and verified; verified is emitted only when hashing occurs.
  Start is a reporter callback, not a JSON event. Do not call this a versioned machine schema yet.
- Bars explicitly select stderr, ignoring the library demo Dump override. Captured non-TTY stderr
  makes the bar Hidden, never stdout Dump. Isolated child tests and actual CLI payload tests cover both
  empty and `1` inherited override values.
- The current CLI compares `--expect` after the old engine returns. A mismatch can leave a published
  file; it is not rollback or check-before-publication. The separate native entrance has the stronger
  required-check ordering but has not replaced this flow.
- Two inherited carries remain open: retry JSON does not fully encode control characters, and non-TTY
  Plain still uses updating carriage-return/ANSI controls. The Ctrl-C newline fix does not close either.

## Harness and protected work

The Docker fault harness is manual, not CI. Missing Docker/AWS/GNU timeout prerequisites must fail,
not satisfy a negative scenario. Rust loopback tests are not a complete resume/retry/mirror/live-S3
harness run. Never retry the previously denied harness cleanup through another tool, project or agent.
Do not stop unrelated services/containers/processes or remove volumes.

Leave owner `.DS_Store`, `src/.DS_Store`, `file1.sh`, `file2.sh` and active executables/downloads/partials
alone, especially under `/Volumes/d3vm3d1a/maks/stage`. Downloaded data is untrusted: use new empty data
directories, keep scripts elsewhere and run Python readers with `-I`. No credential/login bypass or
signed URL/cookie persistence into reports.

## Review and delivery

Keep auto mode; do not enable plan mode unless the user explicitly asks. Commit small independently
reviewed, exact-prefix-verified steps. The user authorized protected branches and reviewed draft PRs in
xget/xbytes/xprogress, not merges, tags, releases or craft publication. Craft stays private.

After implementation is complete, reconvene all six read-only review types for the integrated library,
CLI and docs: xget-api-craft, xget-async-ownership, xget-concurrency, xget-performance, xget-cli-docs and
xget-rustdoc. The rustdoc owner uses Steve Klabnik and BurntSushi as documentation quality references;
API craft co-reviews contract accuracy.
Slice reviews do not substitute for that final review. Fix findings, rerun gates and re-review before
final acceptance. They are maintainer-inspired lenses, not endorsements by the named maintainers.

Existing GIFs are historical, not new runtime evidence. Only media/download.tape is currently recorded
as reproducible. Do not ship new media without its tape and inspected frames. Errors remain lowercase,
without trailing punctuation, and should tell the user what to do next.
