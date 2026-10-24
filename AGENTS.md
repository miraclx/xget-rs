# AGENTS.md

`xget`: a chunked, parallel, resumable, verified fetch engine over a pluggable byte source. lib + bin, one
crate, edition 2024, toolchain pinned to 1.97.1.

- `STYLE.md` is the contract. It is not a summary of the house rules, it is the house rules. Read it before
  writing a line. It has one carve-out already granted: a one-shot private pipeline may stay as free
  functions, in House rules.
- `miraclx/REVIEW.md` is how this code gets reviewed. The severity, verdict and closure vocabulary lives
  there. `notes/craft-backlog.md` is what is open.
- The open items are the work. `notes/craft-backlog.md` is the list. Nothing here is a task.

## The trap that will bite you first

`Cargo.toml` depends on `xbytes` and `xprogress` as **unpinned git deps**. `.cargo/config.toml` (gitignored,
local only) overrides them with `paths = ["../xbytes-rs", "../xprogress-rs"]`.

So a local build runs against the **sibling working copies**, while `Cargo.lock` pins the **remote revs**.
These are two different bodies of code and they drift silently.

```
Cargo.lock pins   xprogress 418aa05a (2026-08-30)  where DrawTarget::interject exists
local override    xprogress-rs HEAD 8f2665c (2026-09-08)  where it was cut
```

A sibling commit can break this crate's build while **CI stays green**, because CI has no override and uses
the lock. Never trust green CI to mean your local tree compiles, and never trust a green local build to mean
the lock is satisfiable. After touching either sibling, `cargo update -p xprogress` and re-run the build.

`unreachable_pub` is not enabled, so `plan::plan_range` and `plan::plan_resume` are `pub` in a private
module and not nameable outside the crate. That is current state, not intent.

## Gates

All four, before every commit. This is the definition of done.

```
cargo test
cargo clippy --workspace --all-targets -- -D warnings
cargo +nightly fmt --check        # nightly: group_imports and imports_granularity are nightly-only
cargo sort --workspace --check
```

`harness/` is the fault-injection rig: docker compose (go-httpbin, MinIO, toxiproxy), `up.sh` then
`scenarios.sh`. It is **manual and not in CI** (`ci.yml:77-79`). It is the only thing that proves resume,
retry, mirror failover and the S3 paths actually work, so a green test suite does not cover it. It needs
`docker`, the `aws` CLI, and `timeout` (Homebrew coreutils on macOS, absent from a stock Mac).

## Invariants

**Verify-as-you-scatter.** An in-order hasher runs concurrently with the chunk fetchers on the same task,
chasing a contiguous frontier (`engine.rs:647`). `received` advances the lead, `wrote` advances the done.
This is why the crate can return a verified file without waiting for the slowest chunk, and why it is
single-task structured concurrency: source futures need not be `Send`.

**The `.xget` file is the download.** A file sink scatters into `<output>.xget` and renames it into place
only on success (`engine.rs:226`). Layout is `[data 0..total][text trailer][32-byte footer]`, magic
`XGETCTL1` (`control.rs:4`, `:26`, `:28`). The rename happens after `Writer::finish` truncates trailer and footer, so
**before the rename the `.xget` is not the finished bytes**.

**Resume is automatic.** No flag. Presence of a valid `.xget` beside the output resumes it; `--restart` opts
out. `-c/--continue` and `--no-bar` were removed; `--stream` became `--sequential`.

**`validators_agree` is the splice guard** (`engine.rs:354`). If both an ETag and a Last-Modified are
present they must match; a resource that changed between runs is never resumed into, because the old bytes
would be spliced onto the new. If either side is absent it returns `true` and falls back to length alone.
That fallback is the security-relevant default and it currently has **no test** (`craft-backlog.md`, B2).

**Ordering rule that lives in the wrong place**: `allocate_fresh` must run before `Writer::create`, or
`create` truncates the trailer. The invariant is on the caller (`engine.rs:137`), not on `Writer`. Tracked
as H3.

**No `fsync`.** Control updates are flushed to the OS, never fsynced. Survives process interruption, may
lose the last checkpoints on power loss. Deliberate, documented at `control.rs:122-125`. The README makes no
crash-durability promise, which is why the decision is defensible.

**Scratch naming.** A `-`, `/dev/null`, or pipe output keeps nothing: its scratch is a throwaway
`xget-<pid>-<n>.xget` in the temp dir, removed by `ScratchGuard` on drop. A file sink's `.xget` never matches
that pattern and is never swept. `sweep_orphans` runs fire-and-forget at startup (`main.rs:175`) and
deletes only temp scratches whose pid is dead.

**The over-serve guard** subtracts rather than compares (`engine.rs:523`), so the `u64` cannot wrap. Do not
"simplify" it to `len > range.len()`.

## Layout

No `mod.rs` anywhere. `<module>.rs` with submodules in a sibling `<module>/`. Unit tests in a sibling
`<module>_tests.rs`, integration in `tests/`.

One inconsistency, currently load-bearing: the **lib** declares test modules plainly (`mod plan_tests;`)
and the **bin** uses `#[path = "bar_tests.rs"] mod bar_tests;` because those tests need `super` access to
private items. Same-directory files either way. Not to be "tidied" without checking the `super` reach.

`main.rs` is one flat file and that is a documented, excused deviation from the CLI-is-a-module-tree rule
(whose single-command exception it is), because `xget` is
a single command with no subcommands.

**"Cache" means the `.xget` scratch plus its embedded control trailer.** There is no in-memory chunk cache.
`--cache-size`, `--show-cache`, `--start-pos` and `--force-append` were dropped as divergences from
libxget-js; the live list is `README.md:175-184`.

## Features

`s3` and `ipfs` gate the two optional sources. `aws-credential-types` and `aws-smithy-types` are vestigial:
they enable deps that only `feature = "s3"` code compiles, so they do nothing alone. Every other build in the
repo, including CI and the harness, uses `--features "s3 ipfs"`.

`rust-version = "1.85"` in `Cargo.toml` but the toolchain is pinned to `1.97.1`. Known inconsistency.

## Things you will get wrong if nobody tells you

- **`-s` is `--checksum`.** It collides with curl's `-s = --silent`. Never add a quiet `-s`; route quiet
  through `--progress none`.
- **Speed is bytes by default.** `MiB/s`, matching the size readout, because a file tool shows bytes/s.
  `--bits` opts into `Mbps`, the network-link convention. Both are correct in their context; the choice is
  one-screen coherence.
- **`Source` is not object-safe** (`async fn` in a trait). `AnySource` hand-writes three dispatches with
  repeated `#[cfg]`. That is why, not laziness. H6, watch-point, not a fix.
- **Verification happens in the bin, not the engine** (`main.rs:272-280`). The engine computes the digest
  into `Report`; comparing it against `--expect` is the CLI's job.
- **`--progress json` writes to stderr**, never stdout, so it cannot corrupt piped output. Four events
  (`start`, `progress`, `retry`, `end`) plus a fifth, `verified`, emitted only in json mode.

## Docs

`README.md` carries five vhs GIFs. Only `media/download.tape` exists; the other four gifs have no tape and
are not reproducible. Record with `vhs media/download.tape`, judge the frames yourself before shipping one.

Errors are the next instruction to the user, lowercase, no trailing punctuation (STYLE.md, error handling). Keep it
that way: a bailed message that names the fix is the standard the reviews hold it to.

`PARITY.md` was deleted in `59aff40`; its content was folded into `README.md:175-184`. Recover the
historical checklist with `git show a46b3d4:PARITY.md` if you need the ➖ rows.