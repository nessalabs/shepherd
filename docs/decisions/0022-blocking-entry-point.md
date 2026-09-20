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

A current-thread `Handle` is refused at drive time: `Handle::block_on` does
not run that scheduler's I/O or timers, and using it from the driver thread
deadlocks. `from_runtime` (owned `Runtime::block_on`) is the supported
current-thread path from ordinary synchronous code. Same-runtime current-thread
calls panic with that explanation instead of hanging.

`with_scope` / `with_scope_options` accept a synchronous closure. The body
runs on the calling thread **outside** any driven future: each `spawn` /
`wait` / `terminate` is its own `block_on`. That is required so a
current-thread runtime taken via `from_runtime` can still spawn and wait
from the body — putting the body *inside* `Runtime::block_on` made those
nested calls see `Handle::try_current` and panic (nested `block_on`
deadlocks on current-thread). A panic is caught so cleanup can finish
(`terminate_scope` + bounded `wait_scope_cleanup` history), then resumed.
Nested blocks remain independent scopes.

`run` / `run_with_options` bound the *whole* attempt — spawn and wait, not
only reading output. Cleanup always uses the caller's `TerminateOptions`,
never leftover deadline crumbs, so a process that exits at T−1ms still gets
a verified group reap. Spawn or wait errors do not hide a later
`terminate_scope` failure. On expiry they `terminate_scope` and the host
checks `all_verified()` / `into_verified()`. Callers express a cleared
environment and a byte-capped capture through `ProcessSpec`
(`EnvPolicy::Clear`, `OutputMode::Capture`).

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
