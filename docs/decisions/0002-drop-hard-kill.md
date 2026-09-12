# 0002 — Drop of the last supervisor handle hard-kills remaining work

## Status

Accepted (implemented). Addresses Codex P1 on `unix.rs` (`kill_on_drop(false)`
plus no `Drop` safety net).

## Context

Tokio `Child::kill_on_drop` fires when the `Child` is dropped. Shepherd moves
`Child` into a waiter task so it can be reaped asynchronously; that task outlives
the caller's supervisor handle. `kill_on_drop(true)` would therefore kill on
*waiter end*, not on *supervisor drop*. Monitor tasks also hold `Arc<Inner>`, so
a `Drop` on `Inner` would never run while children are still being waited on.

## Decision

Split user-facing ownership from monitor ownership:

- `ProcessSupervisor` holds `Arc<Inner>` **and** `Arc<CleanupGuard>`.
- Monitor tasks clone `Inner` only.
- `CleanupGuard::drop` (last user-facing handle) calls
  `ProcessBackend::hard_kill_all()` — a synchronous, non-blocking whole-tree
  `SIGKILL` / `killpg`. It does not await or verify.
- After an explicit `shutdown`, the same `Drop` still calls `hard_kill_all`
  (idempotent) but does not warn.

Success remains the async `terminate` / `terminate_scope` / `shutdown` path.
Drop is the documented unverified backstop (`CleanupUnverified` /
`DroppedWithoutShutdown` in spirit; the caller who dropped the handle is gone).

## Consequences

- A forgotten `shutdown` cannot leave long-running children behind.
- `hard_kill_all` is a new required method on `ProcessBackend`.
- Cloning a supervisor (e.g. into `terminate_scope` tasks) keeps the guard
  alive until those user-facing clones finish — correct.
