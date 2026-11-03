---
id: "inventory"
title: "Find a feature owner"
kind: "guide"
status: "reference"
summary: "Every public capability and supporting area has a destination and a stated verification boundary."
parent: "shepherd"
sources: ["crates/shepherd/src/lib.rs", "crates/shepherd/src/blocking.rs", "crates/shepherd-app/src/lib.rs", "Cargo.toml"]
sourceRevision: "e56c5e583793ad7d69b4dd4738857909ca8007ad"
diagramLinks: {}
---

# Feature and owner inventory

This map follows the public facade and application modules at the recorded source
revision. It includes test infrastructure as supporting evidence, not as shipped
product behavior. A linked model checks selected design rules, not every function
on its page.

| Caller goal or area | Owning code | Explanation and model disposition |
| --- | --- | --- |
| Configure backend, publisher and sampling | `shepherd/src/lib.rs` | [System](README.md), [platforms](platforms.md); configuration contract |
| Create scopes, spawn, observe accepted work, shutdown | `shepherd-app/src/supervisor.rs` | [Admission](admission.md); source-reviewed concurrency |
| Terminate, wait, quarantine, correct and prune | `shepherd-domain/src/scope.rs`, `process.rs` | [Scope cleanup](scope-cleanup.md); direct model |
| Run async closure with automatic cleanup | `shepherd-app/src/supervisor.rs` | [Scope cleanup](scope-cleanup.md); shared evidence rules |
| Background command, timeout, cancel, result | `shepherd-app/src/background_task/task.rs` | [Background tasks](background-tasks.md); direct finalization model |
| Named services, readiness, restart and stop | `shepherd-app/src/service.rs` | [Services](services.md); direct adoption model, retry/readiness source review |
| Restart delay and failure limit | `shepherd-domain/src/restart.rs` | [Services](services.md); pure arithmetic contract |
| Durable kept record, claim, adoption and listing | `shepherd-app/src/kept.rs`, `shepherd-infra/src/kept/` | [Kept processes](kept-processes.md); direct publication/reread model |
| Captured stdout/stderr and tails | `shepherd-infra/src/output.rs` | [Output](output.md); direct byte-accounting model |
| Read-only snapshots, trees, refresh | `shepherd-app/src/observation.rs` | [Observation](observation.md); returned-value contract |
| Cached stats and aggregate usage | `shepherd-app/src/usage.rs`, `supervisor.rs` | [Statistics](statistics.md); measurement contract |
| Synchronous facade and run deadline | `shepherd/src/blocking.rs` | [Blocking](blocking.md); shared scope/output owners |
| Domain and integration events, waiters, history | `shepherd-app/src/dispatch.rs`, infra waiters/publisher | [Events/history](events-history.md); shared pruning rules |
| Unix, cgroup, Windows and null adapters | `shepherd-infra/src/backend/` | [Platforms](platforms.md); OS implementation boundary |
| Specs, IDs, errors, capabilities, exits | `shepherd-domain/src/` | Contracts attached to their owning feature; no invented lifecycle |
| Fixtures, stress, leak and heap checks | `fixtures/`, `shepherd-test-support`, `tools/` | [Verification](verification.md); synthetic workloads |
| Architecture decisions and unsafe register | `docs/`, `tools/ddd-arch-check` | Preserved existing documents, linked as evidence |

The models intentionally do not stand in for native containment tests, heap/FD
leak probes, async race tests, or production workload qualification. Unmodeled
behavior remains visible in each owning page rather than being labeled verified.
