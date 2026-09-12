# 0018 — Cleanup serialization, quarantine, bounded history, isolated observers

Accepted. Refines 0002, 0005, 0007, 0008 and 0017 without weakening verified outcomes.

Scope termination serializes with admitted spawns and other scope terminations.
It retains the registry entry until the final containment sweep succeeds. Repeat
calls return the recorded report. Shutdown is serialized and retryable: cancellation
or a failed scope sweep cannot turn a second call into an empty false success.
After a containment sweep, unresolved roots are observed again through their monitors.

A failed wait remains CleanupUnverified(ReapFailed). Such terminal records are
quarantined: not pruned, not allowed to close the aggregate as verified, and included
in future scope/shutdown reports. The backend retains its kill responsibility.
Duplicate attachment cannot overwrite an owned process. Successful root records
are pruned promptly.

Keep at most 256 completed waiter records, 256 verified scope reports, 256 completed
with_scope reports, and 256 unclaimed completed output observers. Live or unverified
ownership is not evicted to satisfy a history limit. Old completed IDs can return
UnknownProcess/UnknownScope and old unclaimed output can return None. Callers retain
ProcessExit/report values or take output observers when they need longer history.
Registered waiters keep their own channel receiver through eviction. Wait and
termination register while holding the ownership lock, including scope-wide fanout.

A cleanup worker has a scope-only synchronous kill backstop. Runtime cancellation or
panic publishes an unverified error and issues that backstop. Normal block
cancellation instead leaves the worker running full two-phase cleanup. Internal
spawn/termination/sampling tasks hold Inner only, never a user CleanupGuard.

External integration publishers run behind a bounded 64-event queue. Each publication
has a one-second timeout; full/failed publication logs loss and cannot stall internal
reap/prune. Default publication is still NoopIntegrationPublisher. The queue adds a
small fixed allocation, refining the zero-cost wording of 0007.
