# Release notes

## 0.2.0-rc.1 preparation

This is the source description for the first 0.2 release candidate, not a publication receipt. Actual
published versions, target archives, checksums and provenance belong to the
[GitHub release](https://github.com/miraclx/xget-rs/releases). CI candidates are nonpublishing builds.
Registry publication is separate; do not infer that these versions are available on crates.io.

### What 0.2 contains

- One common Client for bounded discovery, original-path/literal selection, frozen output planning,
  required checks, native publication and owned continuation. Files execute serially.
- Exact HTTP, plus optional S3 objects/prefixes, SwissTransfer collections, WebDAV trees, GitHub release
  catalogues and exact IPFS gateway inputs. The planned CLI archive edition enables all five provider
  features: `s3 ipfs swisstransfer webdav github`; source and library defaults remain off.
- Eligible pinned exact-HTTP range assembly and proper-prefix recovery, using up to four real windows.
  Other ordinary adapters use ordered whole responses, not partial-byte recovery.
- Default absent-only publication, early occupied-target refusal and separate guarded Overwrite/Restart
  authority. Clean EOF, length, settled writes, sealing and every required check precede publication.
- Human CLI receipts and Classic/Fine/Solid terminal styles. Range/restored reception is displayed
  separately from the available contiguous settled prefix. Neither is verification or publication.
- Opt-in JSON with `schema_version: 1`, offline saved-run inspection, and explicit complete input
  bindings for a chosen run. Measurement is independent of required checks.

### Breaking migration from the earlier surface

The earlier root Source/get/from/Download/Output/Report/Probe/Progress/ByteRange/ByteStream/Error and
control helpers are no longer compiled or exported. Use `Client` for remote references and `Transfer`
for application-owned bodies or repeatable readers. `MemberToken` is non-Copy; ordinary whole-body
failures use `client::SourceError`. See [API migration](docs/library.md#pre-release-api-migration).

The Cargo library package is `xget`; the command-line package is `xget-cli`, installing `xget`. The CLI
has no legacy command, positional output operand, remote checksum sidecar, old chunk/sequential flags
or alternate engine. Use `-o FILE`, `-D DIR`, `--expect algorithm:hex` and the current help. `-s` selects
optional measurement, not silent mode. `--progress none` keeps results and errors.

Modern native V1/V2/V3/V4 readers remain supported; older records do not gain missing authority.
Retired XGETCTL1 partials are not imported, migrated or deleted. Native payloads remain in private
workspaces; `.xget-runs` paths describe aggregate metadata, not incomplete payload filenames. Completion
metadata can be retired under bounded capacity pressure without deleting payloads or native workspaces.

### Known limits and platform scope

Ordinary redirects, automatic opening retries, content decompression and ambient proxies are disabled.
HEAD 403 fails. HEAD 405/501 permits unknown-length, unpinned metadata without a planning GET; the
ordinary whole GET is opened only during execution. An advertised but ignored range fails without
whole-mode fallback. This is not a general curl/Wget replacement for arbitrary delivery URLs.

HTTP interoperability and a supported visible/inspectable partial-file interface are accepted later
work, explicitly outside the frozen 0.2 functional scope through stable. There is no successful-pause,
fsync/power-loss durability, arbitrary-writer inode-CAS or universal-filesystem promise. Checked live
stdout prefixes cannot be retracted after a late failure.

The candidate matrix is GNU Linux x64/ARM64 on Ubuntu 24.04 and macOS 15 Intel/Apple Silicon. A target
is accepted only after its actual optimized archive is extracted and smoked on that native host. Host
labels are not minimum OS support guarantees. No native Windows, musl/static, older OS floor, universal
macOS or portable Wasm artifact is claimed. See [installation](docs/installation.md) and each accepted
candidate's provenance for actual results, features and dependency revisions.

### RC feedback

Report the candidate version, target, source commit, exact command with access references/headers
redacted, exit code, complete nonsecret diagnosis and whether the destination existed beforehand.
Keep private saved records and downloaded data local; they are not required issue attachments. Do not
post cookies, signed URLs, authorization headers or credential-bearing references.

Reproducible corruption, unintended replacement, access leaks, wrong success or misleading recovery
block promotion. A correction receives a new candidate/version and new verified artifacts, not moved
tags or replaced published assets. Stable 0.2.0 needs a stable-version build, not a relabeled RC binary.
New recordings are optional; the retained GIFs/tape are historical, not current execution evidence.
