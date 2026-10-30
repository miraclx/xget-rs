# Project contracts and agent workflow

`xget` is a reusable fetch library. `xget-cli` is the workspace's command-line package and installs the
`xget` executable. Both use edition 2024 and the pinned Rust 1.97.1 toolchain.

- Read `docs/STYLE.md` before writing code. It is the contract, including the narrow private-pipeline
  exception. `../craft/REVIEW.md` defines review evidence, severity and closure vocabulary.
- `.claude/notes/craft-backlog.md` tracks open work. Historical reviews are not rewritten when a later fix lands.
- The ordinary Client implements exact HTTP, optional bounded SwissTransfer inventories and caller-SDK
  S3 exact objects/explicit prefix inventories. Shared bounded resolution, literal/original-path selection,
  inspection-only directory metadata, frozen layout, serial checked publication/live opening and owned
  continuation use the same coordinator. Eligible exact HTTP uses anchored-prefix range recovery;
  S3, Swiss, optional WebDAV trees, GitHub release catalogues and exact IPFS stay whole mode.
  The modern CLI consumes this coordinator, including scoped access headers, catalogue choice and
  separate guarded Restart/Overwrite authority; it has no legacy command or alternate engine.

## Workspace boundary

- Root package `xget`: `src/`, library integration tests in `tests/`, transport sources and the advanced
  native `transfer` module. No argument parser, progress renderer or dotenv setup belongs here.
- Package `xget-cli`: `cli/src/`, executable `xget`. `main.rs` parses, composes transports and dispatches;
  download/list/info/resume modules own commands. No provider orchestration or scheduler belongs in main.
  Archived `cli/src/expect.rs`, `main_tests.rs`, `progress.rs` and `progress/` remain physically intact
  but are uncompiled and excluded from the CLI package, as is any archived `legacy.rs`. Do not add a
  legacy command, compatibility feature or second engine.
- The old working files under `src/bin/xget/` are preserved provisional work, excluded from root binary
  discovery and packaging. They are not the compiled CLI. Do not accidentally add their SwissTransfer
  feature, synthetic file locators or provider-owned scheduler through a move or broad staging operation.
- No `mod.rs`. Unit tests stay in sibling `*_tests.rs` files. Private child tests use `#[path]`; do not
  widen production visibility or move their declarations to the crate root just for access.

The compiled library exposes the common Client and advanced Transfer entrances, plus Input,
Inventory/File, SavedRun and Checksum parsing. Earlier root Source/get/from/Download/Output/Report,
Probe/Progress/ByteRange/ByteStream/Error families and old control helpers are no longer compiled or
exported. The corresponding engine/source/control/builder/plan files and their old tests remain
physically unchanged and package-excluded pending cleanup. Cargo disables automatic integration-test
discovery and enumerates only client_saved, transfer, transfer_live and transfer_reader.
Do not delete, move, rename or empty retained files, or reintroduce them through a legacy feature/shim.
Modern native V1/V2/V3/V4 readers remain compiled and packaged; they are not the retired XGETCTL1 engine.
Native publication requires Unix support. Whole portable-engine Wasm support has not been established.

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

Release tooling under `scripts/release/` requires Python 3.11 or newer. Run its sibling tests with
`python3.11 -I -B -m unittest discover -s scripts/release -p '*_tests.py' -v`. The candidate workflow is
nonpublishing: four native optimized extracted-archive smokes and exact-source CI precede aggregate
acceptance. Helper tests, source tests and runner labels are not archive execution evidence. Tags,
GitHub releases and registry publication still require their separate explicit authorization.

Rust 1.97.1 is the exercised support baseline. The previous advertised 1.85 was not proven. Do not
claim a lower MSRV without the corresponding toolchain/dependency/feature validation.

## Advanced native-file invariants

Body-based `Transfer` is the native layer used by the ordinary Client and modern CLI. Same-algorithm requirement
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
strong guards. Bounded V1/V2 one-shot, V3 reader and V4 authority records retain their readers; retired
XGETCTL1 files are not imported or altered.
Keep publication uncertainty and unconfirmed write extents
explicit. No one-shot source reopening, automatic cleanup, fsync or universal filesystem promise exists.
Unix hardlink support, a caller-owned Tokio runtime and a suitable existing caller-owned parent are
required. Non-Unix returns a typed unsupported result. Windows compilation is not native runtime proof.

## Advanced range-reader invariants

`Transfer::from_reader` borrows one lifetime-specific repeatable session. Enforced representation/version
contracts or a required SHA-256/SHA-512/BLAKE3 value authorize assembly; optional measurement, weak hashes
and length alone do not. Every response window and evidence must match, and late errors after the last
requested byte still fail. Up to four ranges write bounded prefixes. The checker follows the contiguous
settled-write frontier while bodies run, not a post-slowest-range hash pass. Stage readback uses the same
physical-operation registration and leases as writes, including after waiter drop.

`ProgressFacts::available` observes the contiguous acknowledged settled prefix independently of hashing
and before hash readback delay. At most four actual offset-ordered `WindowFacts` distinguish received,
written, extent certainty and `Reception`; whole bodies have no synthetic windows. Restored prefix,
EOF, matched checks and publication remain separate facts. No observation grants persisted reuse or
concurrent-owner-mutation safety. Drained native write failure must not remain hidden behind an earlier
source-only refusal; other stop/length/response-validation precedence stays intact.

Reader/check/body/error families remain concrete and conditionally Send. Generic Send helpers name one
borrow's F/B/E families with equality bounds; do not replace that with an HRTB or static requirement.
V3 reader records freeze session size/representation/version, destination, check policy and data
identity. `resume_from` reacquires both leases and rehashes a recorded strong proper prefix before
suffix admission. Prefix witnesses finalize copies of already-active SHA-256/SHA-512/BLAKE3 states;
weak/no-hash policy cannot invent a fingerprint. Proper-prefix evidence never proves EOF. Completed
records must cover the frozen size. Reader statistics separate restored bytes from current-attempt
source bytes and physical writes; native partials remain logical assembled facts.

Persist closed/sealing intent before sealing can take effect. Stops settle submitted work and are
observed again after sealing, before publication; a closed stage never reopens writable. Queued/running
physical operations retain registrations after waiter drop. V1/V2 one-shot records remain decodable,
not writable reader authority. Eligible ordinary exact HTTP reuses this coordinator; custom readers
remain a distinct caller-owned entrance. The modern CLI uses the ordinary Client entrance.

## Owned aggregate continuation

Ordinary exact-HTTP and enabled collection runs use the designated root's bounded `.xget-runs` store. Unique unfinished
matching precedes mutable discovery; complete prior runs do not match and ambiguity exposes actual
paths. Request fingerprints are private intent matching, not source identity, authorization or reopen
capabilities. Records freeze members, assignments, validators and check policies, never raw access
references/cookies/foreign errors. Offline inspection has no execution lease or payload/network effects.

An aggregate registration enters every native acquisition closure before submission and stays in the
operation-owning Arc, as it does for manifest I/O. Dropping the waiter must not unlock the store while
queued/running physical work remains. Stable leases are never unlinked; final-owner acquiring-PID
protection still applies. Expected destination/check policy must bind native reconciliation to that member.

Native attempts are recorded before execution. Missing intent/evidence is uncertainty, not refetch or
publication authority. Completed entries require recorded strong content plus identity; nohash/weak-only
completion remains NotRevalidatable. Whole-mode pending work uses fresh ordered responses with frozen
ETag checks and leaves old partials untouched. V3 aggregate records can freeze exact-HTTP range mode;
V1/V2 retain whole mode. Range reentry supplies the recorded native state to the existing reader's
leased prefix authority, without fresh HEAD or a refetch fallback for missing/unanchored/closed state.
Explicit HEAD byte-range support, known nonzero size and a strong ETag fitting the 256-byte reader
bound select ranges. Longer legitimate pins retain whole mode. Every conditional response must match
its full window/total, identity encoding, length and frozen strong ETag. No new scheduler or payload
probe exists; live opening and Swiss remain whole mode. Range stats separate restored/current source/
physical writes from logical native partials; reconciled completions do not invent attempt statistics.
Fresh bypasses continuation, not overwrite or cleanup. Restart is separate explicit receiving-attempt
authority; no legacy import or successful pause is claimed. Atomic normalized metadata checksums are not hidden payload fingerprints or authenticated state.

## Common collection adapter

The optional `swisstransfer` adapter feeds the ordinary Client coordinator and is forwarded by the CLI
feature. No synthetic member URL, provider scheduler, singleton flattening or opaque final root exists.
Members carry share-scoped repeatable tokens separately from occurrence-local FileId and renewable
access. Original occurrences map to zero/one/many frozen members; V2 saved records persist that mapping
and source paths, while V1 exact records keep V1 checkpoint semantics. Match unfinished intent before
mutable discovery/selection. Supplied selected files must agree with frozen member/version/check facts.

Complete discovery debits shared count/text bounds before selection. Whole opening responses require
identity encoding, exact manifest-compatible length and a strong ETag. Checkpoint the first pin before
native submission; renewed access cannot silently select latest. One expired-denial renewal is bounded;
a live denial is not renewal permission. Keep signatures/cookies/remote diagnostics out of records and
source chains. Foreign parser temporary wiping and hard CPU deadline preemption are not promised.

## Common S3 and bounded shared discovery

`ClientBuilder::s3_client` consumes a caller-configured SDK without ambient configuration loading.
The SDK retains caller credential/endpoint/retry policy, not HTTP defaults. Metadata/header deadlines
and request/body admission are shared across Client clones. Exact missing/denied objects never list.
Prefix ListObjectsV2 pagination is literal and complete, with real nonrepeating tokens, not a snapshot.
Each member's conditional HEAD/whole GET preserves known versionId and opaque strong ETag; an ETag is
not expected MD5 or a recovery content anchor. No S3 range/partial-byte reuse is introduced.

Before SDK collection/XML parsing, each S3 metadata/error response body is limited to 2 MiB, including
chunked responses. Successful GET payloads are not capped; HEAD Content-Length describes object size.
Rejected streams drop before admission releases, even when a typed SDK error is retained. This is not
an all-transport/RSS or synchronous CPU guarantee. The common retained budget is independently 1 MiB,
with 128 combined files/directory entries and 128 original inputs across resolve_all or planning.

Zero-byte slash markers are genuine inspection-only directory metadata, conditionally HEAD-checked.
They do not materialize empty directories or become persisted jobs. Directory-only unconfigured or
exclusion-only scopes create no output. Nonempty slash payloads require exact acquisition plus an
explicit safe filename. Whole discovery is charged before both literal and path selection; the public
metadata receipt retains that full charge. MemberToken is compact and non-Copy, with explicit shared
zeroizing S3 key ownership and canonical Swiss UUID spelling. Ordinary whole-body errors now use
SourceError; HTTP range errors retain their separate concrete family.

V4 aggregate records preserve genuine S3 key/version evidence and optional nonsecret binding intent:
scope, namespace, ordered patterns and an original-reference-only fingerprint, never raw references
or credentials. SavedMember::binding_input verifies both that fingerprint and the existing full binding
before restoring intent without discovery/matching. Older/default records work only when the full
binding agrees; complex older recipes require the original explicit Input. V1/V2/V3 encoding is retained.
Saved key correlation and declared requirement provenance APIs from the parent remain available.

## WebDAV tree adapter

The optional `webdav` adapter uses explicit HTTP(S) tree intent, complete Depth-1 PROPFIND
and scoped same-origin/root hrefs. No crawling or exact-to-tree fallback. Directory facts are inspection
metadata, not filesystem jobs. Shared complete accounting and positive/literal selection apply before
execution. Whole responses repeat known length and any strong pin; an absent pin remains Unpinned,
not a hidden snapshot or range-reuse capability.

Roots end in slash; relative hrefs, redirects, query/fragments, normalized dot paths and
encoded-separator/decoded-path aliases are refused. Explicit original-root userinfo is same-origin
caller access, not member identity; returned hrefs cannot add credentials or change origin. Original
references are resupplied exactly, not persisted or silently rotated. Control bodies
are capped at 256 KiB each and 1 MiB per traversal, XML at 16384 nodes with DTD disabled, listings at
128 and source depth at 64, within shared discovery/admission bounds. No hard parser CPU/RSS promise.
V4 remote tag 3 stores scoped href/source-path evidence and frozen intent, not an access root or
credentials. Reentry opens the recorded member without new listing/HEAD/selection or a partial fallback.
The parent S3/Swiss formats and advanced native APIs retain their separate contracts; the CLI forwards
WebDAV support through the same common coordinator.

## Original-path selection

Input-local include/exclude intent is parsed once and shared immutably across discovered members.
Download conveniences require one original input entry; mixed requests use Input-local intent.
Matching is case-sensitive on original Unicode scalars, with slash components, local `*`/`?`, and
whole-component recursive `**`. No normalization, namespace matching or larger pattern language.
Fixed per-input bounds are 64 combined patterns, 1024 UTF-8 bytes each, 64 components and 16384 combined
spelling bytes. The iterative matcher has polynomial work, not a latency/RSS guarantee.

Debit complete discovery and pattern text before filtering. Validate collection hierarchies completely,
but do not apply local filename/device/role restrictions to exact HTTP routing ancestors. Only matcher
bounds of 4096 UTF-8 bytes and 64 slash-separated source components apply there; exceeding them is
an inventory-limit error. Local assignment retains its own validation. Preserve the pre-filter
accounted metadata amount. Every positive criterion must match the complete original file scope before
exclusions; a positive on empty is unmatched. Exclusion-only/unconfigured genuine empty is a no-op;
a nonempty occurrence filtered to none is an error even when another occurrence still matches.
Already-checked criteria survive selecting a frozen File subset; newly appended positives need subset
matches. Literal identity is checked against complete metadata before path subtraction.

Frame ordered include/exclude spellings in intent/bindings without changing no-pattern fingerprints.
Match frozen work before discovery or selector reevaluation. Explicit bindings reconstruct original
reference, namespace and pattern intent; V4 can retain a bounded nonsecret recipe, while older complex
bindings need their original explicit intent. Literal member tokens,
version/requirements and stored range mode retain their separate authority. The CLI applies the same
selection through input-local intent, with its selection conveniences restricted to one original input.

## Transport scope

HTTP validates the full requested response window and declared length. Common S3 uses conditional
ordered whole-object responses, not the retired S3 Source's partial-range policy. Real SDK loopback
fixtures supply explicit credentials/region/endpoints without ambient AWS discovery; do not replace
SDK I/O with a handwritten response struct or claim partial-byte S3 recovery.

`s3` and `ipfs` gate common Client adapters; the CLI forwards those capabilities. The root's vestigial
aws-credential-types/aws-smithy-types feature aliases do not independently enable S3 code. SwissTransfer is a separate optional library
adapter; the modern CLI forwards it along with WebDAV and GitHub. Exact IPFS uses the common
gateway adapter, not the older IPFS Source.

## CLI contract and known carries

- Default acquisition, `list` (`ls`), offline `info` and explicit `resume` share the common Client.
  There is no legacy command, positional output, remote checksum sidecar or alternate engine.
- `-o FILE` asserts one selected file, `-D DIR` assigns a directory root, and `-o -` releases checked
  irreversible stdout prefixes. Live output rejects Fresh/Restart/Overwrite authority even in previews.
- `--expect algorithm:hex` adds a requirement checked before native publication. `-s` is independent optional
  measurement, not curl's silent flag; None cannot disable requirements. Quiet progress uses
  `--progress none`, which retains final results and failures.
- Local sidecars require one selected file and bounded regular non-symlink Unix opening: 65536 bytes,
  256 records and 4096-byte paths. Native sidecars also require Fresh and one original input.
  Stdin/remote sidecars are unsupported; labels match exact original source paths, not renamed outputs.
- Human sizes and default rates use bytes. `--bits` changes human speed only; `--raw-sizes` wins.
  JSON counts/rates remain bytes/bytes-per-second and elapsed times remain milliseconds.
- Human output is the default, formatted directly from typed facts, not event labels plus JSON or
  foreign Debug/source chains. Receipts distinguish required matches, optional measurements, local
  revalidation and live sink delivery. None suppresses starts/progress, not results, checks or errors.
  Ordinary success omits state/event boilerplate; failures retain one diagnosis, acknowledged completed
  counts, partial/uncertain/checkpoint facts and actual copyable recovery paths. Info/resume retain state.
- JSON uses `schema_version: 1` and PascalCase kinds with structured facts. Do not claim a separate
  schema compatibility policy or change machine events/fields/order/units/sampling for human polish.
  Keep machine RunFailed plus Error. Metadata goes to stdout; acquisition/resume diagnostics to stderr.
  Escape human controls/bidi and preserve native bytes; display escaping is not shell operand quoting.
- xbytes owns byte/bits/raw units. xprogress bars explicitly target terminal stderr, ignoring the demo
  Dump override. One received frontier is not verification. Budget the whole active row, shrink optional
  ETA/rate/bar and use complete snapshots when required counts cannot fit. Plain updates only on TTY;
  captured human output has no CR/ANSI styling. Payload stdout stays separate. Human recent-source
  rates exclude restoration; the existing machine sampler retains its own semantics.
- `--header` applies only to one original HTTP/WebDAV input. Explicit resume renews headers only when
  every binding resupplies the same original reference spelling. Access is not persisted in state/events.
- `--ipfs-gateway` supplies access, never identity; exact IPFS still uses frozen CID/path requirements.
  Disabled features fail explicitly. S3 SDK composition belongs only in main and runs only for S3 input.
- Repeating an original request matches unfinished frozen work before discovery. Explicit `resume`
  reconstructs every member binding without new discovery; duplicate, missing and unknown keys fail.
- Ctrl-C exits 130 after runtime shutdown; it is not a successful pause or rollback receipt. Submitted
  physical work may settle after command cancellation. Reporter failure is nonzero, not publication undo.

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

The README is a product front page: explain what xget does and demonstrate useful tasks before
implementation contracts. Keep consequential limits beside the relevant task, with full CLI, library,
installation and release detail in linked guides. Review reader motivation, first-screen clarity and
working examples separately from factual accuracy; a technically correct manual is not automatically
an approachable introduction.

Existing GIFs and docs/media/download.tape are historical, not current runtime or reproducibility evidence.
Do not regenerate the retained tape against the modern CLI. Do not ship new media without its tape and
inspected frames. Errors remain lowercase, without trailing punctuation, and should tell the user what
to do next.
