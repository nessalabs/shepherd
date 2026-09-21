# Public API ownership and cancellation review

| Operation | Ownership and cancellation behavior |
| --- | --- |
| build / clone | Only user-facing handles share CleanupGuard. Building needs no runtime. IDs belong to the originating supervisor. |
| create_scope / try_create_scope | Atomically admits one lifetime scope before shutdown. try_create_scope returns ScopeCreationError afterward; create_scope panics after releasing its lock. It needs explicit cleanup even when all roots exit naturally; the containment resource may still own descendants. |
| spawn | An internal worker holds the scope operation lock through backend spawn, attachment and monitor start. Dropping the caller future detaches that worker; cleanup waits for it. Last-owner Drop sets shutdown intent, so a late spawn is killed and reaped. A canceled spawn may complete in its scope even though its ID is not delivered. |
| processes / capabilities / os_pid | Read-only snapshots; they do not transfer raw child ownership. `os_pid` stays available after an immediate natural exit (spawn may return after the monitor has already pruned) until the last 256 attachments. The bound is enforced at attach so a monitor that loses the registry race cannot grow the map. |
| stats | Reads an interval cache. Cancellation performs no OS effect. NotReady is distinct from unknown/exited and sampler failure. Sampling failure never relinquishes ownership. |
| take_output / read | Transfers an observation handle once. Clones share consumption. Retaining it retains only bounded bytes, not the child or supervisor. Root reap does not wait for inherited pipes. Read through both stream-closed flags for complete output; reader failures are observable independently of process cleanup. |
| wait | Registers under ownership lock. Dropping the future only removes that observer. The spawn monitor remains responsible for reap. Already registered waiters survive history eviction. |
| terminate | Sends grace, then force if still driven. Dropping the future does not itself signal or kill. Committed domain state and the monitor remain intact; retry is valid. Scope membership is unchanged. |
| terminate_scope | Serialized against spawn and other cleanup calls. Dropping its future stops that invocation's fanout; it does not invoke an ownership Drop kill. Retry finishes cleanup. Verified reports require root monitor outcomes and successful containment cleanup. |
| shutdown | Sets shutdown intent immediately and serializes cleanup. New scopes and spawns are rejected. Successful shutdown joins the sampler coordinator; cancellation retains its join for retry. Native observations already running in blocking workers finish independently, with at most 16 per backend and one per child; timing out an observation does not cancel its OS call. Failure/cancellation can be retried; no early flag turns failure into success. |
| with_scope / with_scope_options | Like create_scope, these panic if admission occurs after shutdown starts. The caller owns the closure future; a separate worker owns cleanup. Success, ?, partial spawn failure, panic and cancellation signal the same worker. Normal return carries both result and report; canceled callers observe wait_scope_cleanup. Nested scopes are independent. Active blocks retain externally completed results through lookup eviction. |
| wait_scope_cleanup | Observes the cleanup report; cancellation does not stop cleanup. Reports are retained within the documented history bound. |
| blocking::BlockingSupervisor | Facade driver only. Same ownership as ProcessSupervisor. `run` applies one deadline to spawn+wait, then terminate_scope with the caller's full terminate budget (not leftover crumbs). It claims the capture observer at admission so a delayed cleanup cannot lose it to the 256-entry unclaimed-output history. Spawn/wait errors do not hide a later terminate failure. `Completed`/`TimedOut` are not treated as verified cleanup unless `all_verified()` says so (`Completed` includes the wait `ProcessExit`). `with_scope` admits an observed scope (shared `wait_scope_cleanup` channel) and uses the async `ScopedProcesses` handle so authorization prunes with observation history. The body runs outside any driven future so nested spawn/wait can `block_on` (including current-thread `from_runtime`); a body panic waits for terminate_scope, publishes the report on that channel, then resumes. Same-runtime multi-thread calls, including `LocalSet`, drive via a helper thread (`block_in_place` is forbidden inside `LocalSet`). Current-thread `from_handle` is refused (deadlock / no I/O). Detaching `supervisor()` past the wrapper's runtime lifetime cannot complete verified wait/reap. Dropping an owned runtime from inside another Tokio context uses shutdown_background (unverified). |
| last supervisor Drop | Synchronous hard_kill_all, no async verification. Internal workers cannot keep this guard alive. Ordinary terminate-future Drop is deliberately separate. |
| runtime shutdown | Unfinished cleanup workers issue a scope-only sync backstop and retain an unverified error; already published verified reports are preserved. A stopped runtime cannot prove wait/reap. Explicit shutdown before runtime destruction is the verified path. |

No API exposes mutable Child, pidfd or Job Object handles. Backend ports are trusted
adapter boundaries and must honor their ownership contract and cooperative async
requirements. Unverified reap records remain quarantined rather than being erased.

Completed histories are bounded to 256 entries per category, with separate budgets
for ordinary scope reports and with_scope cleanup reports. Expired completed IDs
can be unknown; live and unverified ownership is never evicted. An owner who needs
permanent history retains returned values or consumes integration events. Events are
lossy observations, not a cleanup ledger; inspect TerminationOutcome before treating
an event as proof of reap.

Platform limits: process groups cannot contain setsid descendants. macOS start-time
signaling has a residual TOCTOU. Cgroups contain detachments but are not a security
sandbox against privileged cgroup migration, and do not kill members automatically
on creator SIGKILL. Windows KILL_ON_JOB_CLOSE applies after assignment; the suspended
create/assign sequence has an abrupt-death setup window. No fallback claims a stronger
containment capability than it implements. Non-child zombie reaping belongs to the OS
parent/subreaper; cgroup emptiness proves absence of live members, not arbitrary waitpid
ownership. Ordinary Windows graceful signaling is unsupported.
