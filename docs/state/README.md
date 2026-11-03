---
id: "shepherd"
title: "Start with Shepherd"
kind: "system"
status: "implemented"
summary: "Start with a caller goal, then follow process ownership, cleanup and observation."
parent: null
sources: ["crates/shepherd/src/lib.rs", "crates/shepherd-app/src/lib.rs", "README.md"]
sourceRevision: "e56c5e583793ad7d69b4dd4738857909ca8007ad"
diagramLinks: {"Facade": "blocking", "Supervisor": "admission", "Tasks": "background-tasks", "Services": "services", "Kept": "kept-processes", "Domain": "scope-cleanup", "Backend": "platforms", "Observer": "observation"}
---

# Shepherd

Shepherd starts operating-system processes and keeps responsibility for cleaning
them up. For example, an application can start a development server, use it while
a workspace is open, and ask Shepherd to stop the server and its contained
children when the workspace closes. A successful request to stop is not yet proof
that those processes are gone; the cleanup report supplies that evidence.

There are three choices for running work. Use an ordinary scope for a group that
must end together, a background task for a single command with a timeout and
retained output, or a service for a named program that should restart. Kept
services can deliberately survive their owner and therefore need a durable record.
An orderly release and a failed termination have different record outcomes; follow
the kept-process page before deciding what a later owner can recover.

```mermaid
flowchart TD
    Caller[Application] --> Facade[Public and blocking APIs]
    Facade --> Supervisor[Scope and process supervisor]
    Facade --> Tasks[Background command worker]
    Facade --> Services[Named service worker]
    Tasks --> Supervisor
    Services --> Supervisor
    Services --> Kept[Kept process records]
    Supervisor --> Domain[Ownership and exit evidence]
    Supervisor --> Backend[OS process backend]
    Kept --> Backend
    Caller --> Observer[Read-only tree and usage observer]
```

## Choose a goal

- [Run and stop a group of processes](scope-cleanup.md).
- [Understand a spawn racing with shutdown](admission.md).
- [Run a background command and keep its result](background-tasks.md).
- [Keep a named service ready](services.md), optionally [across owner restarts](kept-processes.md).
- [Read command output](output.md) or [inspect a process tree](observation.md).
- [Measure resource use](statistics.md), or [call from synchronous Rust](blocking.md).
- [Choose platform guarantees](platforms.md) and [understand events and history](events-history.md).

## Where responsibility lives

The domain crate records ownership and exit evidence without OS calls. The
application crate serializes operations and owns workers. Infrastructure adapters
perform signals, waits, output capture and measurements. The public crate wires
them together. Caller policy includes which programs may run, concurrency limits,
persistent application records and when shutdown should begin.

The [feature inventory](inventory.md) maps the full public surface and support
folders. The [verification report](verification.md) separates source review,
executed checks and platform tests that were not run here.
