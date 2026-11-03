---
id: "blocking"
title: "Call from synchronous Rust"
kind: "flow"
status: "implemented"
summary: "Synchronous wrappers choose a safe runtime path and preserve separate cleanup evidence."
parent: "shepherd"
sources: ["crates/shepherd/src/blocking.rs", "fixtures/tests/blocking_integration.rs"]
sourceRevision: "e56c5e583793ad7d69b4dd4738857909ca8007ad"
diagramLinks: {}
diagramDetails: [{"diagram": 0, "node": "Refused", "section": "Choose the appropriate entry point"}, {"diagram": 0, "node": "Cleanup", "section": "Execution deadline versus total call duration"}]
---

# Call from synchronous Rust

A command-line tool without Tokio can use `BlockingSupervisor` to run a command
and wait for its result. The wrapper owns a small multi-thread runtime or borrows
a supplied runtime handle. It must not naively call `block_on` on a thread already
driving that runtime.

```mermaid
flowchart TD
    Call[Synchronous caller] --> Context{Runtime context}
    Context --> Outside[Outside runtime: drive future]
    Context --> Multi[Same multi-thread runtime: release worker and join helper]
    Context --> Refused[Unsupported context: refuse before admission]
    Outside --> Run[Spawn and wait within execution deadline]
    Multi --> Run
    Run --> Cleanup[Separate scope cleanup and output budget]
    Cleanup --> Result[Return outcome and cleanup evidence]
```

## Choose the appropriate entry point

Same-runtime multi-thread calls use `block_in_place` before joining a helper.
Same-runtime `LocalSet` calls are unsupported and are refused before process
admission. A current-thread handle cannot be driven by its already-running thread.
Use the async API in those contexts or move blocking work to `spawn_blocking` as
supported by the surrounding runtime.

## Execution deadline versus total call duration

`run` applies its deadline to spawn plus wait, not just the child runtime after
spawn. Expiry triggers scope cleanup. Cleanup and output collection have separate
budgets, so total elapsed time can exceed the execution deadline. Natural root
exit also cleans the scope so surviving descendants are not ignored.

Inspect `all_verified()` and the retained output status. A returned timeout tells
you why execution waiting stopped; it does not erase unverified cleanup evidence.
The blocking facade shares [scope ownership](scope-cleanup.md) and
[output contracts](output.md), rather than owning another process lifecycle.
