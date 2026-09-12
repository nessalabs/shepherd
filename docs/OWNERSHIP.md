# Public API ownership and cancellation review

| Operation | Ownership and cancellation behavior |
| --- | --- |
| build / clone | Only user-facing handles share CleanupGuard. Building needs no runtime. IDs belong to the originating supervisor. |
| create_scope | Opens one lifetime scope. It needs explicit cleanup even when all roots exit naturally; the containment resource may still own descendants. |
| spawn | An internal worker holds the scope operation lock through backend spawn, attachment and monitor start. Dropping the caller future detaches that worker; cleanup waits for it. Last-owner Drop sets shutdown intent, so a late spawn is killed and reaped. A canceled spawn may complete in its scope even though its ID is not delivered. |
| processes / capabilities | Read-only snapshots; they do not transfer raw child ownership. |
| stats | Reads an interval cache. Cancellation performs no OS effect. NotReady is distinct from unknown/exited and sampler failure. Sampling failure never relinquishes ownership. |
| take_output / read | Transfers an observation handle once. Clones share consumption. Retaining it retains only bounded bytes, not the child or supervisor. Reader failures are observable independently of process cleanup. |
| wait | Registers under ownership lock. Dropping the future only removes that observer. The spawn monitor remains responsible for reap. Already registered waiters survive history eviction. |
| terminate | Sends grace, then force if still driven. Dropping the future does not itself signal or kill. Committed domain state and the monitor remain intact; retry is valid. Scope membership is unchanged. |
| terminate_scope | Serialized against spawn and other cleanup calls. Dropping its future stops that invocation's fanout; it does not invoke an ownership Drop kill. Retry finishes cleanup. Verified reports require root monitor outcomes and successful containment cleanup. |
| shutdown | Sets shutdown intent immediately and serializes cleanup. New spawns are rejected. Failure/cancellation can be retried; no early flag turns failure into success. |
| with_scope | The caller owns the closure future; a separate worker owns cleanup. Success, ?, partial spawn failure, panic and cancellation signal the same worker. Normal return carries both result and report; canceled callers observe wait_scope_cleanup. Nested scopes are independent. |
| wait_scope_cleanup | Observes the cleanup report; cancellation does not stop cleanup. Reports are retained within the documented history bound. |
| last supervisor Drop | Synchronous hard_kill_all, no async verification. Internal workers cannot keep this guard alive. Ordinary terminate-future Drop is deliberately separate. |
| runtime shutdown | Cleanup workers issue a scope-only sync backstop and retain an unverified error. A stopped runtime cannot prove wait/reap. Explicit shutdown before runtime destruction is the verified path. |

No API exposes mutable Child, pidfd or Job Object handles. Backend ports are trusted
adapter boundaries and must honor their ownership contract and cooperative async
requirements. Unverified reap records remain quarantined rather than being erased.

Completed histories are bounded to 256 entries per category. Expired completed IDs
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
