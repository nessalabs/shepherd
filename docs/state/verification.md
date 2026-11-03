---
id: "verification"
title: "Check the evidence"
kind: "guide"
status: "reference"
summary: "Keep source review, bounded models, negative controls and implementation tests separate."
parent: "shepherd"
sources: ["AGENTS.md", "docs/TEST_COVERAGE.md", "docs/VALIDATION.md", "tools/check_unsafe.py"]
sourceRevision: "e56c5e583793ad7d69b4dd4738857909ca8007ad"
diagramLinks: {}
---

# Verify this atlas

The source checkpoint on each page identifies the Shepherd code reviewed. It is
not a claim that the existing historical validation report was rerun. The bounded
models represent selected ownership, ordering and accounting rules; TLC executes
those TLA+ abstractions, not Rust functions or OS syscalls.

## Reproduce checks

Use a built Nessa Docs checkout with its canonical checker cache installed:

```sh
NESSA_DOCS_ROOT=/absolute/path/to/nessa-docs node scripts/state-docs/verify.mjs --write
```

The runner validates the catalog, runs unmodified models, then applies the
persisted controls in memory. Each control must break its named semantic invariant
with no unchecked rule. It saves results separately and writes only unmodified
baseline receipts into page metadata. It never installs a checker automatically.

See [saved results](../../verification/state-models/results.json) for machine-readable evidence and
[the review record](../../verification/state-models/review.md) for adversarial review and dispositions.
The controls document the exact weakened guard and expected failed property.
[Browser results](../../verification/state-models/browser-results.json) and a
[background-task screenshot](../../verification/state-models/background-tasks.png)
record the local renderer check. This is browser harness evidence, not an MCP host check.

## Current implementation checks

Current checks on 2026-11-03 reviewed source
`e56c5e583793ad7d69b4dd4738857909ca8007ad`:

| Check | Result and scope |
| --- | --- |
| Domain and application Rust unit tests | 81 passed, zero failures |
| Infrastructure and public-facade Rust unit tests | 42 passed, zero failures, on this macOS host |
| Background cancellation example | `cargo check -p shepherd --example background_task_cleanup` passed; example not executed |
| Unsafe inventory | 57 sites in 10 files match the register |
| Browser smoke | All 14 pages and 10 diagrams rendered without errors; critical inspector details reviewed |
| Catalog and TLA+ baselines | 14 pages accepted; 5 models passed all 13 configured invariants |
| Negative controls | All 13 controls broke their expected semantic invariant; no unchecked rules |

TLC 1.7.4 explored 133 states across five independent models; the largest had 46 states. These are explored finite-model state counts, not application workload limits.

The Rust commands were `cargo test -p shepherd-domain -p shepherd-app --lib`
and `cargo test -p shepherd-infra -p shepherd --lib`, using an isolated Rust
1.99.1 toolchain. An earlier attempt could not find `rustc`; that unavailable
attempt is superseded by these successful runs. The unit suites include some
native macOS adapter probes, but are not the complete platform integration suite.
The [logs](../../verification/state-models/test-results.md) retain command/result
summaries. Production Rust is unchanged; the added example is supporting usage code.

## Checks still requiring their environments

Run the workspace Rust suite, Clippy and format checks with a complete Rust
installation. Existing fixture suites cover launch, scope, output, observation,
usage, blocking, services, kept processes, resources and adversarial cleanup.
Linux privileged cgroup tests and Windows Job/handle tests need their respective
hosts. Stress, soak, leak, heap and platform release qualification were not rerun
here. Consult [test environments](../TEST_ENVIRONMENT.md), [test coverage](../TEST_COVERAGE.md) and
[historical validation](../VALIDATION.md) for their commands and historical evidence.

## Reading the model results

A baseline pass establishes its configured properties only within the stated
bounds and assumptions. A negative-control failure shows that a selected bad
transition is detectable; it does not establish complete invariant coverage.
No liveness claim, unlimited-request proof or automatic Rust/model conformance is
made. The review record lists remaining abstraction limits separately from issues
that were corrected before completion.
