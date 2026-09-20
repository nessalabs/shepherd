# 0022 — Synchronous blocking entry point

Status: accepted.

Callers that are not inside an async context could not use Shepherd: every
lifecycle operation on `ProcessSupervisor` is async, and Tokio panics if
`block_on` is invoked from a thread that is already driving a runtime. Desktop
hosts such as a synchronous Tauri command therefore reimplemented spawn, wait,
group kill and reap by hand.

## Decision

The facade crate exposes `shepherd::blocking` behind the `blocking` feature
(enabled by default). The module does not change the async API and does not
enter `shepherd-domain`.

`BlockingSupervisor` is a thin driver around the existing `ProcessSupervisor`:

- `new` / `BlockingSupervisorBuilder` own a two-worker multi-thread runtime so
  monitor and cleanup tasks keep running, and so a `with_scope` body can
  `block_in_place` while other workers reap.
- `from_handle` borrows a caller runtime (the Tauri "bring your own handle"
  case). The handle's runtime must outlive the supervisor.
- `from_runtime` takes ownership of a `Runtime`. This is the supported way to
  wrap a current-thread runtime from ordinary synchronous code, because
  `Runtime::block_on` drives I/O and timers.

Driving a future never calls `block_on` from inside the same Tokio context:

| Caller context | Action |
| --- | --- |
| No current handle | `Runtime::block_on` or `Handle::block_on` |
| Same multi-thread runtime | `tokio::task::block_in_place` + `Handle::block_on` |
| A different runtime | a scoped helper thread calls `block_on` outside Tokio |

A current-thread handle used from the thread that is already driving that
runtime cannot block safely; Tokio panics. That combination is unsupported.
Use an owned multi-thread supervisor, or call from a thread that is not the
current-thread driver.

`with_scope` / `with_scope_options` accept a synchronous closure. The body
runs through the same async scope guard (ADR 0017). A panic is caught so
cleanup can finish, then resumed. Nested blocks remain independent scopes.

`run` / `run_with_options` bound the *whole* attempt — spawn and wait, not
only reading output. On expiry they `terminate_scope` and require the usual
verified group/job reap. Callers express a cleared environment and a byte-capped
capture through `ProcessSpec` (`EnvPolicy::Clear`, `OutputMode::Capture`).

## Consequences

- Sync callers keep the same ownership invariant: every process belongs to a
  scope until exit is confirmed and resources are reaped.
- Tokio remains an unconditional facade/app dependency. The feature flag only
  gates the blocking module.
- Last-handle Drop is still an unverified hard-kill. Verified shutdown still
  requires `shutdown()` while the runtime is alive.
- Dropping an owned runtime from inside another Tokio context uses
  `Runtime::shutdown_background` so Tokio does not panic. Remaining monitor
  tasks are abandoned; that is the same unverified path as dropping without
  `shutdown()`.
- Tests cover NullBackend contracts, real processes on the three-OS CI matrix,
  closed-stdio hangs on Unix, cleared environments, byte-capped capture,
  process-group descendants, and both current-thread and multi-thread nesting.
