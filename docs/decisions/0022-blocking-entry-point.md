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
| Same multi-thread runtime, scheduler task | `block_in_place` hands off the scheduler worker before a scoped helper drives the future and is joined |
| Same multi-thread runtime, `LocalSet` | Tokio refuses `block_in_place` before the helper starts; use the async supervisor or call the blocking facade from `spawn_blocking` |
| A different runtime | a scoped helper thread calls `block_on` outside Tokio |

A current-thread `Handle` is refused at drive time: `Handle::block_on` does
not run that scheduler's I/O or timers, and using it from the driver thread
deadlocks. `from_runtime` (owned `Runtime::block_on`) is the supported
current-thread path from ordinary synchronous code. Same-runtime current-thread
calls panic with that explanation instead of hanging.

Moving the future to a helper alone does not release its caller's scheduler
worker. A one-worker runtime, or a runtime whose workers all enter blocking
methods together, cannot run `spawn_owned` while those callers join helpers.
The same-runtime multi-thread branch must enter `block_in_place` **before**
starting and joining the helper. A runtime timer cannot detect this deadlock;
the regression runs the actual scheduler calls in an OS child with an external
watchdog that kills and reaps that child on failure.

| Ordering | Required result | Regression |
| --- | --- | --- |
| `from_handle` + `spawn` in a `tokio::spawn` task, one worker | Worker handed off; spawn, wait, scoped cleanup and shutdown complete with verified outcomes | `from_handle_single_worker_releases_scheduler` |
| Every worker enters the same-runtime facade together | Each caller hands off its worker; all independently owned scopes complete | `from_handle_saturated_workers_release_scheduler` |
| Same-runtime `LocalSet` calls a blocking method | Panic before process admission; async cleanup remains usable | `from_handle_inside_localset_refuses_before_spawn` |
| No current runtime or a different runtime | Existing direct/helper drive path remains usable | Existing synchronous, owned-runtime and current-thread caller tests |

`with_scope` / `with_scope_options` accept a synchronous closure. The body
runs on the calling thread **outside** any driven future: each `spawn` /
`wait` / `terminate` is its own `block_on`. That is required so a
current-thread runtime taken via `from_runtime` can still spawn and wait
from the body — putting the body *inside* `Runtime::block_on` made those
nested calls see `Handle::try_current` and panic (nested `block_on`
deadlocks on current-thread). Admission uses the supervisor's observed-scope
channel so `wait_scope_cleanup` can register before the body returns and a
later verified `terminate_scope` can replace a failed first cleanup. The
body handle is the async `ScopedProcesses` type, so authorization prunes
with live membership and the 256-entry observation histories instead of
retaining every id. A panic is caught so cleanup can finish
(`terminate_scope` + shared `wait_scope_cleanup` channel), then resumed.
Nested blocks remain independent scopes.

Both entry points finish observed scopes through the application cleanup owner.
The facade drives that operation before returning or resuming a body panic.

### Active scoped invocation ownership

The active invocation owns its admission's cleanup backstop and original report
channel, bound to the originating supervisor. A scope ID is lookup identity;
bounded history is not the active invocation's cleanup owner. Async and blocking
scopes carry this same application owner through body execution and cleanup.
Blocking admission itself is driven through the existing runtime driver so its
context refusal precedes every scope, channel, process, or body effect.

| Ordering | Required owner and result | Evidence |
| --- | --- | --- |
| Runtime driver refuses LocalSet or borrowed current-thread handle | Refuse before polling scope admission; no scope ID consumed and no body executed | Empty and nonempty scoped runtime-refusal counterparts |
| Body externally completes its scope, then 256 other observed scopes finish | Active owner retains original verified report after both lookup histories expire | Active blocking scope history-turnover regression |
| Body returns or panics | Consume original owner through the shared cleanup routine; verified report survives independently of lookup | Normal history-turnover regression and existing original-panic cleanup regressions |
| Active owner is dropped before cleanup finishes | Original armed backstop issues scope-only hard kill and retains unverified interruption | Shared application backstop contract |

| Cleanup result | Action before returning or resuming panic | Evidence |
| --- | --- | --- |
| Retained verified report | Preserve report; no additional hard kill | Existing verified panic cleanup regression |
| Termination error or unverified report | Issue synchronous scope hard kill; retain the original result on the observation channel | Blocking panic failure and unverified regressions |
| Cleanup future interrupted | Armed `ScopeCleanupBackstop` issues hard kill and publishes interruption | Application backstop regressions |

A hard kill is a backstop, not evidence of verified reap. The original failure
or unverified report remains observable even after the body panic resumes.

`run` / `run_with_options` bound the *whole* attempt — spawn and wait, not
only reading output. The capture observer is claimed at admission so a
delayed scope sweep cannot lose it to the supervisor-wide 256-entry
unclaimed-output history. Cleanup always uses the caller's `TerminateOptions`,
never leftover deadline crumbs, so a process that exits at T−1ms still gets
a verified group reap. Spawn or wait errors do not hide a later
`terminate_scope` failure. On expiry they `terminate_scope` and the host
checks `all_verified()` / `into_verified()`. Callers express a cleared
environment and a byte-capped capture through `ProcessSpec`
(`EnvPolicy::Clear`, `OutputMode::Capture`).

## Attachment observation

Public `spawn` completion is a later boundary than attachment: its owned worker
can finish while the caller has not yet been polled. The run future must not depend
on bounded lookup history to recover an already admitted process's observer or exit.
The application exposes one owned observed-spawn task: attachment transfers the
capture observer and registers an exit receiver before monitoring starts. The
ordinary public spawn path keeps its existing unclaimed capture history.

| Ordering | Original owner and required result | Evidence |
| --- | --- | --- |
| Observed spawn attaches | One admission channel owns process id, capture and already registered exit future before monitor starts | Application attachment path |
| Caller is not polled while root exits and 257 other roots complete | Original admission observation survives capture and wait-history eviction | Blocking run scheduling regression |
| Deadline cancels the public completion await | Observed-spawn handle and admission receiver remain outside the timed attempt; scope cleanup waits for the original spawn worker | Deadline and in-flight spawn counterparts |
| Cleanup completes after a delayed spawn | Consume the original admission channel, drain original capture; no PID lookup reconstruction | Blocking run timeout path |
| Observed-spawn owner is dropped | Worker remains responsible for admitted process; unconsumed observer is released; no detach-based cleanup confirmation | Existing cancellation ownership contract |

The current integration publisher is already isolated behind its bounded lossy
queue, and ignores `ProcessSpawned`; blocking it is not a valid way to reproduce
this scheduling window. The regression delays polling the real run future while
its original worker and other lifecycles run on the supplied runtime.

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
