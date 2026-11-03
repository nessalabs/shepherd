---
id: "statistics"
title: "Measure resource use"
kind: "contract"
status: "implemented"
summary: "Measurements retain missing-data and identity limits rather than treating absence as zero."
parent: "shepherd"
sources: ["crates/shepherd-app/src/usage.rs", "crates/shepherd-app/src/supervisor.rs", "crates/shepherd-domain/src/stats.rs", "crates/shepherd-infra/src/usage.rs", "docs/AGGREGATE_STATISTICS.md"]
sourceRevision: "e56c5e583793ad7d69b4dd4738857909ca8007ad"
diagramLinks: {}
---

# Measure resource use

A process list can show a recent root CPU/RSS sample, while a workspace view may
need the usage of a whole tree or contained scope. Those are different APIs with
different measurement boundaries. Do not add overlapping root, tree and group
measurements and call the result total application usage.

## Cached managed-process samples

The supervisor has one shared sampler. The builder configures its interval, with
a minimum of one millisecond. `stats` returns the last observation, and `NotReady`
before the first sample; it does not force a fresh sample. CPU 1.0 represents one
fully used core and uptime is measured at the sample time. Platform capabilities
state which fields are unsupported.

## On-demand tree and scope usage

`tree_usage` selects visible descendants then measures them without taking
ownership. `refresh_tree_usage` checks the root identity. Totals compare individual
members with their previous identity and native start marker. A first sample has
no CPU rate. Missing or failed measurements retain explicit errors and contributor
counts, so a partial total is not disguised as complete coverage.

Scope usage follows backend accounting: native cgroup/Job data where available,
or sampled group usage on macOS. Unsupported backends return an explicit error.
Tree snapshots are not atomic and processes can appear or disappear between
selection and measurement. Memory fields such as resident bytes and physical
footprint have different meanings; consult the linked aggregate guide before
presenting them as interchangeable values.

These values are contracts over returned observations. No persistent statistics
state machine is invented here. The sampler's shutdown ordering belongs to
[admission and shutdown](admission.md); OS measurement correctness needs the
platform tests listed in [verification](verification.md).
