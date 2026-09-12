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

Spawns run in owned application workers so cancellation cannot interrupt the
backend-spawn-to-monitor ownership handoff. Scope operations serialize cleanup
against an admitted spawn; the guard worker waits for any such spawn to complete.
Last supervisor Drop sets shutdown intent and hard-kills synchronously. Internal
workers never retain its CleanupGuard. This is distinct from dropping terminate(),
which merely stops driving that invocation and does not issue a Drop kill.

Nested blocks create independent sibling scopes. Inner completion does not end the
outer scope. Canceling a future awaiting a nested block drops both guards, and each
scope cleans independently. Escaping a ScopedProcesses value does not extend the
scope lifetime; subsequent spawn is rejected after block cleanup.

Verified cancellation requires a running runtime and cooperative backend ports.
Abrupt runtime/process shutdown cannot await or return verified cleanup; OS limits
and fallback behavior remain explicit. No claim of async Drop is made.

The in-memory terminate-cancellation test uses Tokio's paused clock: cancellation
occurs at 5 ms, before 20 ms grace expiry, then the unchanged liveness assertion
observes another 50 ms. Wall-clock scheduler delays must not let force escalation
occur before the cancellation being tested. Real-process tests retain real time.
