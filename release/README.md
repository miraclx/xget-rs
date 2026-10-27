# Nonpublishing RC candidates

`.github/workflows/release.yml` prepares expiring Actions artifacts. It has no tag
trigger, release token, write grant, GitHub Release step or registry publication.
No tag, draft, public release, merge or publication is authorized by this code.

Candidate builds run on relevant `main` pushes. A manual `workflow_dispatch` also
uses the selected default-branch commit; non-default refs cannot enter the build
job. PRs run provisional Python helper tests only, not an accepted candidate build.
This avoids treating a PR head or a synthetic merge revision as accepted push CI.
Workflow dispatch has not been performed as part of this implementation.

## Build and evidence flow

Four native rows are mandatory:

| Target | Runner |
| --- | --- |
| `x86_64-unknown-linux-gnu` | `ubuntu-24.04` |
| `aarch64-unknown-linux-gnu` | `ubuntu-24.04-arm` |
| `x86_64-apple-darwin` | `macos-15-intel` |
| `aarch64-apple-darwin` | `macos-15` |

The workflow uses Python 3.11, Rust 1.97.1, an initially empty external Cargo
home, an external `RUNNER_TEMP` target, two Cargo build jobs, no incremental
compilation and at most two native workers concurrently. Checkout credentials
are not persisted. Cargo configuration files and compiler/profile overrides
are refused, not silently used. There is no dependency or target cache restore.
The exact optimized build is:

```console
cargo +1.97.1 build --release --locked --package xget-cli --bin xget --features "s3 ipfs swisstransfer webdav github" --target TARGET
```

`artifact.py build` first reads locked Cargo metadata and checks the resolved Git
siblings against `Cargo.lock`. It builds once, does not strip or rebuild, and
packages the final executable, both project licenses, `INSTALL.txt` and
`build.json`. Every archive is named `xget-VERSION-TARGET.tar.gz`, without a `v`
prefix. The aligned source versions must be `0.2.0-rc.N`.

Each `build.json` records source SHA/tree, lock SHA-256, version, repository,
workflow run/attempt/ref, compiler/Cargo output, exact command/features, actual
Cargo metadata Git revisions, runner label/image version, binary SHA-256 and
native inspection output. Linux records ELF dynamic dependencies and symbol
versions; macOS records Mach-O load commands/deployment target and dependencies.
This does not establish an older minimum OS. No Windows-native, musl/static,
universal macOS or Wasm runtime claim is made.

`smoke.py` extracts the archive into a fresh empty temporary directory, admitting
only five flat regular members. A fixed-header gzip/USTAR preflight rejects
PAX/GNU extensions before `tarfile` can consume their metadata. It streams and
discards bodies in bounded chunks, counting all expanded headers, bodies, padding
and end bytes against 512 MiB, separately from the compressed-file size limit.
Traversal, links, special files, duplicates, missing members, set-ID modes and
nonzero trailing data are refused before any extraction writes. Payload type,
actual size, claimed size and cap are checked before hashing. This is not a hard
CPU/RSS promise for Python or gzip internals. Scripts remain outside archive/data
directories. Python readers use `-I -B`; no Python
module is imported from archive contents or a data cwd. The extracted executable
is hash-bound before and after smoke and is never replaced by a rebuilt copy.
Two concurrent fixed-size output drains enforce one shared 4 MiB retained budget
while receiving stdout and stderr, including PTY stdout and newline-free output.
Overflow fails the case, kills/reaps the owned CLI and joins both readers. The
outer 240-second smoke deadline owns a dedicated process group: SIGINT offers a
five-second cleanup opportunity, then SIGKILL targets only that owned group.
A timeout remains failure and records separate timeout evidence; forced cleanup
is not proof of native work settlement. Its receipt records archive/binary hashes
and concrete observed case facts:

- version, main/list/info/resume help and native architecture;
- exact checked HTTP bytes/digest, required mismatch with no final destination,
  incumbent-byte and inode preservation;
- captured human receipts without CR/ANSI, JSON byte/rate/millisecond facts with
  actual progress samples, and live payload stdout separated from diagnostics;
- real native PTY download/rendering at a recorded width;
- controlled SIGINT exit 130 before publication, an observed real prefix-state
  checkpoint, offline `info` with no requests, explicit binding-based range
  continuation, nonzero restored bytes, suffix-only requests and exact final bytes;
- actual refusals for ordinary redirects, HEAD denial and advertised-but-ignored
  ranges. These are negative tests, not newly supported interoperability.

Range interruption is event-gated: tails finish while the first response waits;
the first prefix is then released and its checkpoint is observed before SIGINT.
The held response cannot complete the file first. Bounded waits fail closed.
Only owned loopback fixtures and fresh temporary data are used. Each initial
payload case gets a separate new empty data directory, distinct from extraction
and scripts. Only explicit incumbent and continuation cases reuse their own data.
Saved-state containment compares canonical paths, so a native `/private/tmp`
receipt is not rejected merely because Python created the owned root via `/tmp`.
Missing, nondirectory and genuinely external/link-escaped states still fail. An
interruption containment failure retains the reported state path and events.
No private service, credential, user executable, existing download or Docker
harness is used.

The acceptance job waits at most 90 minutes for `.github/workflows/ci.yml` on the
**same push SHA**, not a previous commit or a PR head. It uses only read-only
Actions API requests and requires the expected four CI jobs, all successful in
the selected run attempt. Every API redirect is fatal, including same-host HTTPS
moves, unrelated HTTPS destinations and HTTP downgrades. Update the repository
reference explicitly rather than forwarding the Actions credential. Network-free
fake-token tests cover this policy; no exploited GitHub API leak is claimed. Empty, duplicate, skipped, failed, cancelled or paginated
job sets cannot pass. Its receipt records the CI URL, ID, attempt and source.
Existing source CI all-feature fixture tests support the compiled provider
manifest; live providers have not thereby run on every native archive host.

`artifact.py aggregate` requires exactly four distinct expected target manifests,
consistent source/toolchain/features/Git provenance, exact archive and receipt
hashes, complete successful smoke cases and nonzero prefix restoration. It also
re-extracts every archive to compare its binary/internal metadata/licenses/install
text. It copies, rather than rebuilds, the accepted bytes into one bundle with:

- all four archives, manifests and smoke receipts;
- `provenance.json`, including exact-source CI evidence;
- `SHA256SUMS` covering every payload, including provenance, but not itself.

Internal metadata never hashes its containing archive. The outer manifest and
smoke receipt bind that archive. Checksums provide traceability and corruption
checking, not publisher authentication, notarization or reproducible-build proof.
Both per-target candidates and the accepted bundle expire after 14 days. A failed,
missing or cancelled row prevents an accepted bundle; partial worker artifacts
are not a release. No subsequent publication path is implemented.

## Local helper checks and current evidence

From the repository cwd, with an installed Python 3.11 or newer:

```console
python3.11 -I -B -m unittest discover -s release -p '*_tests.py' -v
```

The validator tests intentionally package synthetic nonexecuted binary bytes.
They test malformed/missing/duplicate/mismatched manifests, source CI selection,
hashes, internal/outer metadata, failed or unanchored smoke and safe extraction.
The fixture tests make real owned loopback HTTP requests. Neither kind certifies
an xget executable or a native archive. `build` and `aggregate` are workflow
entrypoints expecting GitHub and `RUNNER_TEMP` environment, not general local
release commands.

Implementation validation actually run locally: 45 helper/fixture tests passed
with both Python 3.11 and Python 3.14, including Python AST syntax parsing and
static workflow-policy assertions. These assertions are not a YAML/Actions parser
or an executed workflow. The initial system `python3` was Python 3.9 and failed to
import `tomllib`; it is not a supported interpreter.

A development-only run against an existing first-party default-feature debug CLI
completed all 13 smoke cases, including actual PTY rendering, exit 130, offline
info with no requests, a nonzero recorded prefix and correct final publication.
The review-fix rerun restored 253952 bytes with the bounded output drains. An
initial development run found the `/tmp` versus `/private/tmp` containment bug;
its failing receipt was preserved, and canonical-alias/escape regressions were
added before rerunning. A subsequent owned-fixture keep-alive client reset was
made unnecessary by explicit connection-close responses; the final development
rerun completed without that traceback. This is **not** optimized, full-feature
or extracted-archive acceptance evidence, and no archive receipt was fabricated.

Not run by these helpers as part of this implementation: Cargo/build/install/
source gates, any native archive smoke, a four-host Actions candidate, the live
Actions API polling gate, a cold newcomer archive install or live provider
services. The separate existing debug CLI is development evidence only. Authored
archive assertions are not observed native archive results.
The existing source CI workflow is only one gate: this pipeline does not replace
the project's full two-dependency-graph/source/package/consumer/doc gates or
independent release review. Do not call a candidate accepted until those separate
requirements and an actual successful complete artifact run are recorded.
