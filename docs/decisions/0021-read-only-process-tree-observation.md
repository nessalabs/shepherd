# ADR 0021: Read-only process-tree observation

Status: accepted.

Process discovery applies equally to processes launched by Shepherd and externally
started programs. It does not change ownership, containment or cleanup. A separate
`ProcessObserver` application service consumes a `ProcessObservationBackend` port;
pure observation data lives in the domain. The facade's `process_observer()` factory
wires the infrastructure adapter. Existing supervisor constructors remain compatible.

`tree(os_pid)` selects a root from a fresh inventory and returns the root and observed
descendants in deterministic parent-before-child order. The tree is represented by
nodes with parent PIDs, avoiding recursive traversal/Drop on deep trees. Selection
uses a visited set to tolerate inconsistent cycles and rejects parent links known to
be temporally impossible. `snapshot()` also exposes the complete visible inventory.
Managed callers translate logical IDs using `supervisor.os_pid(id)`; external callers
pass their existing OS PID. Neither path registers observed descendants for cleanup.

The infrastructure adapter uses sysinfo 0.33.1, compatible with Rust 1.83, with only
its system feature. It collects basic process metadata, not environments or command
lines. Linux user threads are excluded. Each request uses a fresh table to avoid stale
cached entries. Native work runs on Tokio's blocking pool, serialized per adapter by
a semaphore whose permit stays with the native operation after caller cancellation.
No worker or polling loop is retained between calls. Native calls cannot be preempted;
callers may wrap requests in a timeout without admitting concurrent scans on that adapter.

All observations are best-effort and non-atomic. The adapter cannot enumerate every
permission failure separately, and invisible processes may be silently omitted.
`NotVisible` does not establish exit. Sampling cannot recover historical children after
reparenting, or processes born and exited between samples. A detached child remains
visible if its reported ancestry remains intact; containment membership and ancestry
are different concepts.

Observed identities contain a PID and optional epoch start time with second resolution.
`refresh_tree(root)` rejects differing reported identities, but is not a native
PID-reuse guarantee. Never use observation identities to authorize signaling/reaping.
The supervision backends retain their existing native identity protections.

This change adds process topology and names only. Per-node/aggregate resource counters,
persistent historical tracking and event subscriptions are separate extensions.

Validation includes deterministic traversal, cycle, sibling exclusion, missing/reused
root and stale parent-link tests; real managed/external three-generation process chains
on the existing three-OS CI matrix; Linux thread exclusion; external-root survival after
observer disposal; and normal owner-controlled cleanup after observation.
