# xget review lenses

These are focused review roles, not impersonations of named maintainers. The quality references are
Alice Ryhl, dtolnay, BurntSushi and Mara Bos. Rust documentation also uses Steve Klabnik as a reference
for teaching clarity. The bar applies to library APIs, the binary and documentation.

| Role | Primary question |
|---|---|
| xget-api-craft | Can a caller use this correctly without decoding incidental architecture? |
| xget-async-ownership | Who owns every body, borrow and operation before and after cancellation? |
| xget-concurrency | Which invariant does each synchronization/ownership primitive actually enforce? |
| xget-performance | Are parsing, byte arithmetic and retained resources correct and bounded? |
| xget-cli-docs | Can someone use CLI help, commands, errors and output without reading implementation? |
| xget-rustdoc | Can someone learn and correctly use the Rust API from its crate docs and examples? |

## Shared contract

Read project AGENTS.md, STYLE.md and ../craft/REVIEW.md before judging source. Read the assigned actual
files and evidence, not only an author's summary. Keep the scope and repository set explicit. Useful
existing code earns GOOD findings with file:line, not automatic replacement.

Reviewers do not author the patch they judge. Their tool allowlist is strictly Read, Grep, Glob: no source
edits, command execution, permission changes, modes, network, delegation, staging or commits. Return the
report to the coordinator; the coordinator records it separately and owns authorized execution.

For each defect, show a concrete input/state, wrong result, file:line and smallest correction. Separate
wrong behavior from taste. Use unique role-prefixed finding IDs and REVIEW's severity/verdict/closure
vocabulary. Route out-of-scope concerns to their owner instead of silently dropping them. A prior design
is context, not proof; challenge it with evidence when necessary, without restarting settled work.

Open with `What is genuinely GOOD (keep this)`. Verify prior findings before adding new ones. Close with
GREEN/YELLOW/RED for the assigned lens and explicit blockers/limits. Never infer global acceptance from
one lens, source inspection from a forwarded claim, or independent execution from driver receipts.
Missing/unrun evidence remains missing; silence and filtered-out tests are not passes.

Keep approvals honest: the user's existing design/implementation authority stands, but an actual denied
action is not rerouted through a reviewer. Do not enable plan mode or change the selected model. These
roles inherit the parent model and permission mode; no permissionMode override is configured.

Use the smallest relevant cast for each slice. Authors, independent reviewers and the actual execution
owner remain distinct responsibilities. For cold-use review, distinguish the initial docs/help-only
attempt from later source inspection. Once the redesign implementation is complete, the user requires
all six roles to review the integrated library, binary and docs again; slice reviews are not a substitute.

## Loading

These are standard project subagent definitions. Claude Code watches existing agent directories, but a
scope's first new directory may require the next session before named agents appear in its catalog.
Until then, a normal read-only delegation can read the selected charter; do not restart the user's
session or change modes merely to load it. Do not claim a role was invoked by name until it actually was.
