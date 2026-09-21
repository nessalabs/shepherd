# Architecture Decision Records

Lightweight records of implementation choices that are not obvious from the code
alone. Each file is numbered and named for the decision, not the ticket.

These ADRs cover **this implementation**, including all platform adapters, output,
statistics, async scopes and ownership hardening. They refine sketches in `docs/DESIGN.md`;
they do not replace that document.

| ID | Decision |
| --- | --- |
| [0001](./0001-application-owned-ports.md) | Driven ports live in `shepherd-app`, not the pure domain |
| [0002](./0002-drop-hard-kill.md) | Last supervisor handle Drop issues a synchronous hard-kill |
| [0003](./0003-serialized-scope-process-groups.md) | One process group per scope, created under a per-scope spawn lock |
| [0004](./0004-reuse-safe-signaling.md) | Signal via pidfd (Linux) / start-time check, never a bare recycled PID |
| [0005](./0005-unverified-on-wait-failure.md) | A failed `ProcessBackend::wait` is `CleanupUnverified(ReapFailed)` |
| [0006](./0006-direct-tokio-process-nix.md) | Unix adapter is `tokio::process` + `nix`; `process-wrap` / `cgroups-rs` deferred |
| [0007](./0007-noop-default-publisher.md) | Default integration publisher is a no-op; broadcast is opt-in |
| [0008](./0008-monitor-owned-wait.md) | Wait/reap starts at spawn in a monitor task, not a `ReaperHandler` |
| [0009](./0009-toolchain-and-quality-gates.md) | Edition 2021, MSRV 1.83, TypeScript fitness functions, 100% domain coverage |
| [0010](./0010-retain-slot-until-wait.md) | Child slot stays until `wait` consumes the exit (late monitor must not `ReapFailed`) |
| [0011](./0011-cgroup-v2-backend.md) | Pre-exec cgroup membership, atomic kill, verified emptiness |
| [0012](./0012-privileged-cgroup-ci.md) | Hosted privileged cgroup tests fail closed |
| [0013](./0013-shared-cached-statistics.md) | Shared sampler, cached observations, real per-root CPU |
| [0014](./0014-bounded-byte-output.md) | Bounded combined byte queue, capped tail, transferable observer |
| [0015](./0015-macos-libproc-observations.md) | macOS libproc stats and Mach time conversion |
| [0016](./0016-windows-job-object-backend.md) | Windows Job Object containment before execution |
| [0017](./0017-async-scopes-and-cancellation.md) | Closure result plus report, cancellation cleanup workers, independent nesting |
| [0018](./0018-ownership-quarantine-and-bounded-history.md) | Quarantine, cleanup serialization, bounded history and isolated observers |
| [0019](./0019-stable-process-group-anchor.md) | Private anchor pins one process group across root exits |
| [0020](./0020-adversarial-validation-boundaries.md) | Generated invariants, targeted loom models and resource accounting |

- [0021 — Read-only process-tree observation](0021-read-only-process-tree-observation.md)
- [0022 — Synchronous blocking entry point](0022-blocking-entry-point.md)
