---
id: "events-history"
title: "Observe events and history"
kind: "flow"
status: "implemented"
summary: "Events notify observers after ownership transitions; completed lookup history is bounded."
parent: "shepherd"
sources: ["crates/shepherd-app/src/dispatch.rs", "crates/shepherd-app/src/registry.rs", "crates/shepherd-app/src/supervisor.rs", "crates/shepherd-infra/src/waiters.rs", "crates/shepherd-infra/src/publisher.rs"]
sourceRevision: "e56c5e583793ad7d69b4dd4738857909ca8007ad"
diagramLinks: {}
---

# Observe lifecycle changes

A caller waiting for a process exit should not need to race the exact moment the
process finishes. Shepherd retains recent completed results and wakes waiters
from committed domain events. This is in-memory notification, not a durable event
log or a guarantee that every external subscriber receives every event.

```mermaid
sequenceDiagram
    participant Domain as Scope aggregate
    participant Dispatch as Event dispatcher
    participant Waiter as Exit waiters
    participant Registry as Registry cleanup
    participant Publisher as Integration publisher
    Domain->>Dispatch: Committed lifecycle events
    Dispatch->>Waiter: Record exit and notify
    Dispatch->>Registry: Prune only verified terminal ownership
    Dispatch->>Publisher: Translate integration event
```

## Failure and correction

Handlers run in deterministic order. An error is logged and does not prevent later
handlers from running. A failed wait can produce unverified exit evidence; the
notifier checks that the registry still contains that uncertainty before
publishing an older failure. This avoids resurrecting a stale unverified result
after a verified correction was processed and removed.

## Retention is bounded

Completed exits, scope reports, cancellation reports and unclaimed completed
output retain up to 256 entries per category. Keep returned results or take an
output observer when longer retention matters. Live or unverified ownership must
not be evicted to satisfy a history bound. An observed scope retains its own
original cleanup report independently of bounded lookup history.

The default integration publisher does nothing. A configured broadcast publisher
has its own delivery behavior and is not a persistent audit trail. Applications
that need durable history must save it themselves. Model coverage is shared with
[scope cleanup](scope-cleanup.md) for pruning evidence; dispatch scheduling,
broadcast lag and eviction correctness remain implementation test concerns.
