# 0017 — Closure results and independent, observable scope cleanup

Accepted. Exact primary signature:

```rust
pub async fn with_scope<T, F, Fut>(
    &self, specs: Vec<ProcessSpec>, body: F,
) -> WithScopeResult<T>
where F: FnOnce(ScopedProcesses) -> Fut, Fut: Future<Output = T>;

pub struct WithScopeResult<T> {
    pub scope: ProcessScopeId,
    pub result: Result<T, SpawnError>,
    pub termination: Result<ScopeTerminationReport, TerminateError>,
}
```

The closure may borrow caller data. If it returns Result<U,E>, that value remains
T; `?` inside the closure does not discard E or bypass cleanup. A partial initial
spawn failure skips the closure, retains SpawnError, and still cleans the scope.
`with_scope_options` additionally accepts TerminateOptions.

The closure runs in the caller's future. A separate worker, started before the
first spawn, awaits the block-exit signal. Normal return, error, unwind and caller
cancellation all drop the same signal guard. The worker owns Inner only, performs
two-phase cleanup and reap, and publishes a report. `wait_scope_cleanup(scope)`
retrieves it after cancellation while the supervisor/runtime remain available.
Normal return waits for that same report. Cancellation cannot return a value to a
caller that discarded its future; the retained report is the observation path.
Lookup history retains the most recent 256 verified completed scope reports. Pending
and failed/unverified cleanup reports are not evicted. A receiver registered before
completion retains its result even after the lookup history expires that scope ID;
new lookups of expired verified scopes return UnknownScope. A separate observer
awaits the cleanup worker's JoinHandle and publishes TerminateError on worker panic;
a retained sender can therefore never hide a cleanup panic as a permanently pending
report while the runtime remains running.

Spawns run in owned application workers so cancellation cannot interrupt the
backend-spawn-to-monitor ownership handoff. Scope operations serialize cleanup
against an admitted spawn; the guard worker waits for any such spawn to complete.
Operation locks are allocated only for created scopes and removed after verified
cleanup, after publishing the cached report. Queued callers recheck that report
under the retained operation lock; completed or unknown lookups do not recreate
per-scope locks.
Ordinary shutdown closes admission, then waits for already admitted spawns to attach
and enter their scope's normal graceful termination and descendant sweep. A late
spawn must not hard-kill unrelated scopes while they are still owed grace. Last
supervisor Drop has a separate ownership flag: it hard-kills synchronously, and a
spawn completing after that Drop repeats the sweep and reaps its root. Internal
workers never retain its CleanupGuard. This is distinct from dropping terminate(),
which merely stops driving that invocation and does not issue a Drop kill.

Nested blocks create independent sibling scopes. Inner completion does not end the
outer scope. Canceling a future awaiting a nested block drops both guards, and each
scope cleans independently. Escaping a ScopedProcesses value does not extend the
scope lifetime; subsequent spawn is rejected after block cleanup. Scoped wait and
output access accept only process IDs returned in that block's initial process list
or by its scoped spawn method. The handle retains these IDs through process reap,
so completed local output remains accessible and completed sibling IDs cannot
bypass the scope boundary after registry pruning. The initial process list remains
a snapshot; dynamic spawns are tracked separately for observation access. Scoped
spawn, wait and output access lazily discard membership IDs once registry ownership,
retained waiter results and retained output have all expired. This prevents an
active block's sequential spawn/wait cycles from accumulating permanent dynamic
history, while preserving live/quarantined processes and every retained observation.
An idle handle may retain its last active peak until its next scoped operation; the
caller-owned initial process snapshot is unchanged.

Verified cancellation requires a running runtime and cooperative backend ports.
Abrupt runtime/process shutdown cannot await or return verified cleanup; OS limits
and fallback behavior remain explicit. No claim of async Drop is made.

The in-memory terminate-cancellation test uses Tokio's paused clock: cancellation
occurs at 5 ms, before 20 ms grace expiry, then the unchanged liveness assertion
observes another 50 ms. Wall-clock scheduler delays must not let force escalation
occur before the cancellation being tested. Real-process tests retain real time.

The cleanup worker publishes its result before disarming its synchronous backstop.
A separate JoinHandle observer only translates join failures; runtime shutdown after
worker completion cannot leave the retained report pending. A cross-runtime test
verifies completed cleanup remains observable without a join observer.
