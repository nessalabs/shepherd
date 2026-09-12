# 0011 — cgroup v2 before exec, atomic kill, verified emptiness

Accepted; supersedes the cgroup deferral in 0006.

The Linux default attempts a private directory beneath the current cgroup v2
ancestor, then `/sys/fs/cgroup`. `SHEPHERD_CGROUP_ROOT` or
`UnixProcessBackend::with_cgroup_root` selects a delegated ancestor explicitly.
`UnixProcessBackend::new` remains an explicit process-group backend. The required
constructor returns an error rather than silently falling back.

Use the kernel's documented filesystem interface directly. We need only creation,
membership, kill, and emptiness; cgroups-rs would add controller/policy abstractions
we do not use. References: https://docs.kernel.org/admin-guide/cgroup-v2.html.

Each scope has one cgroup. Open cgroup.procs in the parent and write `0` in the
child's async-signal-safe pre_exec hook before user code can fork or call setsid.
A post-spawn move would leave an escape window. Failed membership fails exec.
Application scope operations serialize spawn against scope cleanup.
The registry entry and operation lock are published atomically under the registry
lock. Unknown IDs never allocate operation locks; verified cleanup removes its
lock, and cached completion lookups do not recreate it. Unverified scopes retain
their lock so cleanup retries remain serialized.
Creation checks shutdown admission under the same registry guard, before allocating
an id. `try_create_scope` returns ScopeCreationError::SupervisorClosed after admission
closes; the existing `create_scope` signature is preserved as a documented panic
wrapper. The wrapper panics only after releasing the registry guard, so a caught
misuse panic cannot poison cleanup state. Scopes admitted before shutdown's flag
store finish publishing before shutdown can take its registry snapshot.

Require cgroup.kill and exercise it on an empty probe. A PID sweep on older
kernels cannot make the same atomic guarantee against concurrent forks, so those
hosts use ProcessGroup. Once selected, later cgroup setup failures reject spawn;
capabilities never silently degrade while processes are owned. This refines §17.

Keep the kill descriptor open for synchronous Drop. Explicit scope cleanup checks
`cgroup.events` for `populated 0`, then removes the scope directory. Failure or a
five-second timeout returns an error; a successful signal alone is insufficient.
Empty nested cgroups are removed bottom-up before the scope directory. Traversal
uses pinned directory descriptors and refuses symlinks; cgroup control files are
never unlinked. Kernel directory removal still fails if a group becomes populated.
Scopes remain registered until both root outcomes and containment cleanup are
verified. Shutdown counts cleanup errors and retries remaining scopes on a later
call; the latest 256 completed scope reports make repeated cleanup idempotent. Failed reap
outcomes remain observable across retries instead of becoming an empty success.
Root outcomes observed while a scope drains are saved before pruning, including
when the termination caller has cancelled. Entering Draining also captures exits
already recorded while Open whose monitor has not yet pruned them; recording and
capturing use the same registry lock, without retaining ordinary Open-scope history. Retries merge this pending evidence;
verified completion removes it and retains only the bounded final report. Queued
callers also retain the verified report in their shared operation state, independent
of lookup eviction. Shutdown pins every snapshotted operation under the registry
lock before cleanup; operation state is released with its last in-flight caller.
ScopeClosed delivery is deferred until that verified commit, including for empty
scopes. The corresponding ScopeTerminated publication is scheduled once, remains
best effort, and holds only event handlers, never supervisor/backend ownership.
All integration publication uses one lazy worker and a 64-event queue. Enqueue
never waits; overflow is dropped, and each publication has a one-second timeout.
This bounds retained events and prevents a stalled publisher from retaining one
deferred task per completed scope. Dropping the last sender lets the worker drain
the bounded queue and release the publisher; it never holds a sender itself.
Root children are separately waited and reaped. Descendant zombies belong to their
OS parent/reaper; emptiness means no live descendant, not that Shepherd can wait
for arbitrary non-child processes. Tests use a subreaper to verify their reaping.

`Capabilities` is available on the supervisor. Stats support is unchanged in this
phase. Process-group cleanup remains best effort and cannot verify escaped trees.
Cgroups do not automatically kill on supervisor SIGKILL; no such backstop is claimed.

Prerequisite fixes: preserve backend wait errors, retain waiter exits even before
subscription, start monitors before dispatch, and propagate final sweep failures.

The synchronous global drop backstop attempts every retained `cgroup.kill` descriptor.
If a write fails, it also kills that scope's registered roots using retained pidfds or
matching start-time identities. This root fallback does not verify descendant cleanup;
explicit cleanup still reports the cgroup failure. Privileged CI injects a failed retained
descriptor and checks real root kill/reap alongside another healthy scope.

A failed root wait retains its backend identity and pidfd for the kill backstop.
Only a successful reap observation retires that slot; an empty cgroup proves containment,
not root reap. Unresolved wait failures remain quarantined until backend disposal or a
successful subsequent wait, without creating additional slots for repeated failures.

A root wait failure quarantines its process-group identity. Further admission is rejected,
and group signals never use that unverified PGID; hard cleanup still attempts retained
root identities. The quarantine remains even if a later root wait recovers, because root
reap does not establish that the old group ID is safe. Cgroup kill remains independently
available. On targets without a safe retained root identity, this fails closed.

A failed OS wait retains the actual Tokio Child, not just its last error. A later wait
requests another native observation through a one-entry coalescing channel; permanent
errors do not retry autonomously. The waiter owns only a weak backend-state reference.
Dropping backend state closes the channel and releases an idle failed waiter. The
supervisor's identity-safe kill backstop runs before that disposal; Child kill-on-drop
remains disabled because an errored raw PID is not proof of identity.
