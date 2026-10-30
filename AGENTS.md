# Working on xget

`xget` is the Rust library; `xget-cli` in `cli/` installs the `xget` command.
Both use edition 2024 and the pinned Rust 1.97.1 toolchain.

Before changing code or documentation, read:

- [Style contract](docs/STYLE.md).
- [Project contracts and workflow](.claude/project.md), including the required validation and review gates.
- `../craft/REVIEW.md` for review evidence, severity and closure vocabulary.

Keep auto mode. Do not enter plan mode or create a worktree unless the user asks.
Preserve unrelated working changes, active executables, downloads and native partials.
Do not delete, move, rename or re-enable the retained inactive source files.
Do not run or clean up the historical Docker harness without separate authorization.
Tags, releases, registry publication and protection changes require their own authority.

Use the common Client coordinator, not a second engine or provider-owned scheduler.
Keep CLI parsing and terminal presentation in `cli/`, not the library.
Never confuse received bytes, settled availability, checks and completed publication.

Validate the exact staged prefix before committing, on both actual dependency graphs.
Keep build/install targets outside the checkout and within the documented disk budget.
The full feature, docs, consumer, package, runtime and final-review gates are in the project contract.

The README introduces the product through useful tasks. Detailed CLI, library, installation and
maintainer material belong in their linked guides, not before a reader's first download.
