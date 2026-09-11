# Shepherd — Design & Implementation Plan

> Status: **Plan / RFC** (pre-implementation). This document is the source of truth for
> the architecture. Companion documents:
> - [`DIAGRAMS.md`](./DIAGRAMS.md) — class and state diagrams.
> - [`GLOSSARY.md`](./GLOSSARY.md) — the ubiquitous language (enforced in CI).

## 1. What Shepherd is

Shepherd is a **standalone, reusable, production-quality process-supervision library**
for Rust. It provides *mechanism*, never *policy*.

Every process created through Shepherd belongs to exactly one explicit **`ProcessScope`**.
Shepherd spawns processes, associates them with scopes, tracks their lifecycle and
descendants (as far as the OS allows), measures resource usage, terminates individual
processes or whole scopes, waits for termination, reaps exited children, prevents zombies,
exposes exit information, and cleans up reliably when the owning application shuts down.

**The core invariant:**

> Every process Shepherd starts belongs to exactly one explicit lifetime scope until
> Shepherd has *confirmed* that process has exited and its resources have been reaped.

A process started through Shepherd must never become silently unowned.

### 1.1 Non-goals (hard architectural constraints)

Shepherd must **not** contain, depend on, or be coupled to:

- agents, conversations, turns, WebSockets, Nessa, ACP, or any product concept;
- task/workflow engines, health policy, restart policy, application-specific timeouts;
- persistence/event streams, authentication;
- global singleton supervisors or service locators.

The caller provides **policy** ("this process exceeds 1 GB, stop it"). Shepherd provides
**observation and mechanism** ("PID 1432 is using 1.8 GB RSS"; "terminate this scope").

## 2. Strategy: build on vetted primitives

We do **not** re-implement solved OS mechanics. We compose established crates and spend
our effort on the un-commoditized layer: the ownership model, verified cleanup, capability
honesty, resource observation, and an adversarial test suite.

| Concern | Reused primitive |
| --- | --- |
| Async spawn, process group (Unix), Job Object (Windows), kill-on-drop | [`process-wrap`](https://docs.rs/process-wrap) (Tokio frontend) |
| Linux cgroup v2 (create, place, `cgroup.kill`, controller stats) | [`cgroups-rs`](https://crates.io/crates/cgroups-rs) (with a direct `/sys/fs/cgroup` fallback) |
| pidfd, signals, `pre_exec`, `clone3`, `waitpid` | `nix` / `rustix` |
| Per-process `/proc` stats (Linux) | `procfs` or direct reads |
| Job Object accounting/stats (Windows) | `windows-sys` |
| Process stats (macOS) | `libproc` |
| Async runtime | `tokio` |
| Errors / tracing | `thiserror` / `tracing` |
| Property & concurrency tests | `proptest`, `loom` (targeted) |

### 2.1 Why `process-wrap` is not enough on its own

`process-wrap` provides **process groups** (Unix) and **Job Objects** (Windows). A bare
process group is *not* sufficient containment: a descendant that calls `setsid()` escapes
it. On **Linux** we therefore add a **cgroup v2** layer for the strong guarantee, attached
as a composable `process-wrap` `CommandWrapper` (no forking of the crate). On **Windows**
the Job Object already provides the strong guarantee. On **macOS** no cgroup / Job-Object
equivalent exists without privileged entitlements, so macOS is process-group + explicit
bookkeeping, and **says so through the `Capabilities` type** (see §9).

## 3. Domain-Driven Design (hard requirement, enforced)

DDD is a **graded requirement enforced in CI**, not a style preference. The domain is a
**pure crate** with a denylisted dependency closure, so "the domain is isolated from
infrastructure" is a build-graph fact, not a convention.

### 3.1 Bounded context & layering (hexagonal / ports & adapters)

One bounded context — **Process Supervision** — with a strict *dependencies point inward*
rule: `infra → app → domain`, never the reverse.

- **`shepherd-domain` (pure).** Entities, Value Objects, Aggregates, Domain Events, domain
  errors, and **Ports** (traits). No `tokio`, no `nix`, no OS, no `std::process`/`std::fs`.
  `#![forbid(unsafe_code)]`. Deterministic and unit-testable without spawning a process.
- **`shepherd-app`.** Application services / use-cases. Orchestrates the domain, drives the
  ports, owns async (`tokio`). Depends on `shepherd-domain` only.
- **`shepherd-infra`.** Adapters implementing the domain ports: `cgroups-rs`,
  `process-wrap`, `nix`, `windows-sys`, `libproc`, the clock, the stats sampler, output
  plumbing. This is the **Anti-Corruption Layer**: it translates OS concepts (pids, cgroup
  files, job handles, signals) into domain terms.
- **`shepherd` (facade).** Wires `app` + `infra`, re-exports the public API. This is what
  users depend on.

### 3.2 Tactical patterns mapped to the domain

- **Aggregate Root — `ProcessScope`.** The consistency boundary. Owns its `Process`
  entities; *all* mutation flows through the root. This is where "never expose raw mutable
  child ownership" and "terminate_scope can't cross scopes" become aggregate invariants.
- **Entity — `Process`** (identity `ProcessId`), living **inside** the scope aggregate. No
  external mutation, no API to move it between scopes.
- **Value Objects (immutable)** — `ProcessScopeId`, `ProcessId`, `ProcessSpec`,
  `ProcessStats`, `ProcessExit`, `TerminationOutcome`, `Signal`, `GracePeriod`,
  `Capabilities`, `OsIdentity` / `ReuseToken`.
- **Domain Events** — `ProcessSpawned`, `TerminationRequested`, `ProcessExited`,
  `ScopeClosed`, `ProcessReaped`. Raised by the aggregate; emitted at most once logically.
- **Repository — `ScopeRegistry`.** Collection-style access to `ProcessScope` aggregates
  (in-memory runtime state; the pattern still applies).
- **Ports (domain-owned traits)** — `ProcessBackend`, `Clock`, `OutputSink`.
- **No anemic model.** Behavior lives on the aggregate. The domain is a **pure state
  machine**: e.g. `ProcessScope::request_termination(now) -> Vec<Command>` returns *what to
  do* as data; the app layer executes those commands via ports and feeds results back as
  events. This purity is what makes the invariants property-testable.

### 3.3 Enforcement (three independent layers)

1. **Structural (build-graph, strongest).** `shepherd-domain` has an allowlisted dependency
   closure; it will not compile if it pulls in `tokio`, `nix`, `libc`, `cgroups-rs`,
   `process-wrap`, `windows-sys`, etc. `#![forbid(unsafe_code)]`; aggregate internals kept
   `pub(crate)`.
2. **Fitness-function scripts (TypeScript, `tools/ddd-arch-check/`), CI job `ddd-architecture`:**
   - **Dependency-boundary check** — parse `cargo metadata`; assert `shepherd-domain`'s
     transitive deps ⊆ allowlist and that the inward-only layering holds.
   - **Purity scan** — fail on forbidden imports in `shepherd-domain/src`
     (`std::process`, `std::fs`, `std::net`, `tokio`, `nix`, `unsafe`).
   - **Ubiquitous-language coverage** — every public domain type/method appears in
     `GLOSSARY.md`, and every glossary term exists in code; drift either way fails.
   - **Encapsulation check** — no `pub` fields on entities/aggregates.
   > Alternative: identical checks can live as a Rust `xtask` (single toolchain). Default is
   > TypeScript per request; the CI job simply gains a Node step.
3. **Behavioral / invariant tests (Rust).** The nine invariants (§10.1) as executable tests:
   aggregate unit tests, `proptest` for state-machine legality and no-cross-scope
   termination, `loom` for the concurrency-sensitive registry.

## 4. Workspace layout

```
crates/
  shepherd-domain/     # PURE domain (entities, VOs, aggregates, events, ports, errors)
  shepherd-app/        # application services / use-cases (async orchestration)
  shepherd-infra/      # adapters: cgroups-rs, process-wrap, nix, windows-sys, libproc
  shepherd/            # public facade: wires app + infra, re-exports API
fixtures/              # non-published pathological helper binaries (workspace member)
tools/ddd-arch-check/  # TypeScript fitness functions
tests/                 # integration / contract / isolation / stress
docs/                  # DESIGN.md, DIAGRAMS.md, GLOSSARY.md, platform guarantee table
.github/workflows/     # CI matrix + privileged cgroup workflow + stress dispatch
```

Fixtures live in a **non-published workspace member** so pathological helpers and their
dependencies can never leak into the published `shepherd` crate.

## 5. Core types (data shape first)

```rust
// shepherd-domain (illustrative, not final)
pub struct ProcessScopeId(u64);   // opaque, supervisor-assigned
pub struct ProcessId(u64);        // opaque handle — NOT the OS pid

struct OsIdentity { pid: u32, reuse_token: ReuseToken } // pidfd / start-time / handle

pub struct ProcessSpec {
    program: OsString,
    args: Vec<OsString>,
    env: EnvPolicy,               // explicit; no ambient inheritance surprises
    cwd: Option<PathBuf>,
    graceful_signal: Signal,      // default SIGTERM (configurable per spawn)
    output: OutputMode,           // see §8
}

pub struct ProcessStats {         // observations only — never verdicts
    pid: ProcessId,
    cpu_usage: f32,
    memory_rss_bytes: u64,
    virtual_memory_bytes: Option<u64>,
    peak_rss_bytes: Option<u64>,
    io_read_bytes: Option<u64>,
    io_write_bytes: Option<u64>,
    descendant_count: Option<u32>,
    uptime: Duration,
    state: ProcessState,
    sampled_at: Instant,
}

pub enum TerminationOutcome {
    ExitedNaturally { code: Option<i32>, signal: Option<Signal> },
    GracefulSuccess,
    ForcedRequired,
    Failed(TerminationError),
    CleanupUnverified(UnverifiedReason),
}

pub enum UnverifiedReason {
    DroppedWithoutShutdown,
    RuntimeShutdown,
    WaitTimedOut { waited: Duration },
    ReapFailed(ReapError),
    ProcessDisappeared,
}

pub struct ProcessExit {
    pid: ProcessId,
    code: Option<i32>,
    signal: Option<Signal>,
    outcome: TerminationOutcome,
    forced: bool,
}
```

### 5.1 Public operations (facade)

```rust
supervisor.spawn(scope_id, spec).await        -> Result<ProcessId, SpawnError>
supervisor.stats(pid).await                    -> Result<ProcessStats, StatsError>
supervisor.processes(scope_id).await           -> Result<Vec<ProcessId>, _>
supervisor.terminate(pid, options).await        -> Result<ProcessExit, TerminateError>
supervisor.terminate_scope(scope_id, options).await -> Result<ScopeTerminationReport, _>
supervisor.wait(pid).await                      -> Result<ProcessExit, _>
supervisor.shutdown().await                     -> Result<ShutdownReport, ShutdownError>
supervisor.with_scope(specs, |scope| async { .. }).await  // async scope guard (§7.3)
```

Raw mutable `Child` ownership is never exposed. All cleanup APIs are idempotent.

## 6. Identity & PID-reuse safety

`ProcessId` is a supervisor-assigned opaque handle, **distinct from the OS pid**. Each
record stores the OS pid plus a **reuse token** so a recycled pid can never be mistaken for
a live owned process:

- **Linux:** `pidfd` (kernel ≥ 5.3) — race-free "is this exact process still alive" and
  race-free signaling.
- **Windows:** the process/job handle.
- **macOS:** pid + process start-time, with `EVFILT_PROC`/`NOTE_EXIT` for exit
  notification.

Ownership is determined by **scope membership in the containment primitive** (cgroup / Job
Object / process group + bookkeeping), **not** by a PID set.

## 7. Cleanup guarantees & outcomes

### 7.1 Two-phase termination (the verified path)

```
request graceful shutdown → wait grace period → force terminate →
wait for exit → reap → report verified outcome
```

**Success is only reported after Shepherd has verified the owned processes are no longer
running and their child resources have been reaped.** Sending a signal is never, by itself,
reported as success.

### 7.2 Tiered cleanup

1. **Primary — explicit async APIs** (`terminate`, `terminate_scope`, `shutdown`): full
   two-phase teardown, verified, returns a rich typed `TerminationOutcome`. Idempotent.
2. **Supervisor-owned reaper.** Even on a fallback hard kill, the reaper keeps waiting +
   reaping and records the verified terminal state + `ProcessReaped` event — as long as the
   supervisor is alive.
3. **RAII / `Drop` safety net.** If a scope/supervisor handle is dropped without an explicit
   shutdown, `Drop` performs a **synchronous, non-blocking whole-tree hard kill** via the
   already-open containment handle (`cgroup.kill` / `TerminateJobObject` / `killpg`). It
   cannot await or verify, so it records `CleanupUnverified(DroppedWithoutShutdown)` and
   emits a `tracing` warning.
4. **Kernel backstop for abrupt death** (`Drop` never runs — `SIGKILL`/`abort`): Windows
   Job Object `KILL_ON_JOB_CLOSE`, Linux `PR_SET_PDEATHSIG`/cgroup.

### 7.3 Why not full verified cleanup in `Drop`?

`Drop` is synchronous and cannot `.await`; blocking a `waitpid` loop in `Drop` on a Tokio
worker can deadlock the runtime, and `Drop` frequently runs during runtime shutdown (no
reactor) or panic unwinding. `Drop` also cannot return a `Result`, so it cannot surface a
typed outcome. Therefore the **full, verified, typed** cleanup is delivered through the
explicit async path and the **async scope guard** (`with_scope`), which runs verified
cleanup on *any* block exit — normal return, `?` error, or cancellation — while still in
async context. `Drop` is only the last-ditch honest backstop.

### 7.4 Cancellation safety

Dropping a *future* returned by `terminate().await` (caller cancels) must **not** trigger a
kill or corrupt ownership: state transitions are committed under the short registry lock,
not mid-future. Only dropping the owning **scope/supervisor handle** arms the tier-3 hard
kill. These two "drops" are kept distinct and tested.

## 8. stdout / stderr (bytes, never assume UTF-8)

Two jobs are separated:

- **Shepherd always drains** the OS pipe (so a slow/absent consumer can never freeze the
  child or block termination).
- **The caller consumes** via a **bounded queue** of byte chunks (Option A) with an
  **overflow policy of drop-oldest + a dropped-bytes counter**, plus an optional **capped
  tail capture** (Option C) for post-mortem. (Chosen: **A+C**.)

Output plumbing never blocks termination; on kill, readers stop cleanly even mid-flood.

## 9. Resource monitoring (interval polling)

- **A single shared sampler task per supervisor** (not one thread per process) walks live
  processes every configurable `stats_interval`, caches the latest `ProcessStats` per
  process; `stats(pid)` returns the last cached sample. Optional on-demand fresh read.
- Designed so **hundreds of processes remain reasonable**.
- Shepherd exposes **observations, not policy** — enough for a caller to implement rules
  like "memory > 2 GB", "CPU > 95% for 10 min", "tree unexpectedly growing".

## 10. Platform implementations & capability honesty

`ProcessBackend` is the domain port; each platform is an adapter returning a runtime
`Capabilities` value. Guarantees are values, never prose lies.

| Capability | Linux (cgroup v2) | Linux (fallback) | macOS | Windows |
| --- | --- | --- | --- | --- |
| Root process tracking | yes | yes | yes | yes |
| Descendant cleanup | yes (`cgroup.kill`) | best-effort (`killpg`) | best-effort | yes (Job Object) |
| Detached-child (`setsid`) containment | **yes** | **no** | **no** | **yes** |
| CPU stats | yes | yes | yes (libproc) | yes |
| RSS stats | yes | yes | yes | yes |
| Peak RSS | yes | yes (`VmHWM`) | limited | yes |
| I/O stats | yes (`io.stat`) | per-proc | limited | yes (Job accounting) |
| Force termination | yes | yes | yes | yes |

The table is finalized against the *actual* implementation before 1.0. macOS never claims
cgroup/Job-Object-level containment.

### 10.1 Invariants (executable)

1. Every successfully spawned process has exactly one scope.
2. A process cannot move scopes.
3. A closed/terminating scope cannot silently accept new processes.
4. Once cleanup reports success, no owned process from that scope remains alive.
5. Terminating one scope never affects another scope.
6. Repeating termination does not cause invalid state.
7. Exit events are emitted at most once logically.
8. Every owned child eventually reaches a reaped terminal state.
9. Internal bookkeeping does not retain completed processes forever.

## 11. Error taxonomy

Typed `thiserror` enums per operation: `SpawnError`, `TerminateError`, `StatsError`,
`ReapError`, `ShutdownError`, plus domain errors (`ScopeClosed`, `UnknownProcess`,
`UnknownScope`). Each carries enough context to branch on. Failure injection points (§13)
make each reachable in tests.

## 12. Testing strategy

CI output distinguishes **portable contract tests**, **platform backend tests**,
**privileged containment tests**, and **stress tests**.

- **Portable contract tests** — on a `null`/fake backend: state machine legality,
  idempotence, scope-isolation logic. No real processes.
- **Platform backend tests** — real processes per OS.
- **Isolation test (flagship)** — `terminate_scope(A)` provably leaves scope B untouched.
- **Process trees** — one child, many children, grandchildren, parent-exits-first, detached
  (`setsid`) children, multiple unrelated scopes.
- **Termination** — graceful, ignores-graceful, forced, repeated, after-exit, concurrent,
  during-start, during-descendant-spawn, supervisor-shutdown-while-running.
- **Cleanup** — no zombies, children reaped, descendants stopped, FDs released, repeated
  create/kill cycles leak nothing; large repeated lifecycle loops.
- **Resource monitoring** — fixtures that allocate/grow memory, burn CPU, idle, do I/O,
  spawn increasing children; assert within tolerances.
- **Output stress** — huge stdout/stderr, both at once, binary, non-consuming caller,
  termination during flood.
- **Race conditions** — exit-between-lookup-and-signal, PID-reuse, child-exits-during-stats,
  terminate-vs-spawn, terminate-vs-natural-exit, shutdown-vs-terminate.
- **Property / concurrency** — `proptest` for invariants 1–9; `loom` for the registry.
- **Failure injection** — OS spawn fails, stats fails, signal fails, process disappears,
  metadata unavailable, output reader fails, internal worker exits.

Tests must attempt to **break** the implementation, not merely exercise happy paths. Tests
are never weakened to make CI green.

## 13. Failure injection design

Internals are structured so the `ProcessBackend`/`Clock`/`OutputSink` ports can be replaced
by fault-injecting fakes in `shepherd-domain`/`shepherd-app` tests, and so real backends
have seams to simulate spawn/stat/signal/reap failures.

## 14. CI

Matrix: `ubuntu-latest`, `macos-latest`, `windows-latest`. Jobs, clearly separated:

- `fmt` — `cargo fmt --check`
- `clippy` — `cargo clippy --all-targets --all-features -D warnings`
- `ddd-architecture` — the fitness functions (§3.3)
- `contract` — portable contract tests
- `backend` — platform backend + process-tree tests
- `resource` — resource-monitoring tests
- `doc-tests` — `cargo test --doc`
- **`privileged-cgroup`** — real cgroup `cgroup.kill` + detached-child containment. Runs on
  a privileged/self-hosted runner **or** a privileged container step on hosted runners.
  Documented for exactly what it validates; **not** skipped-and-called-complete.
- **`stress`** — `workflow_dispatch`-triggered heavy loops.

Linux hosted-runner cgroup capability is determined empirically; where hosted runners can't
create a usable cgroup, the normal Linux tests still run on the fallback path and the
capability gap is made explicit.

## 15. Phased sequencing (scaffold first, verifiable units)

0. **Scaffold** — workspace + 4-crate split, `GLOSSARY.md`, error/type stubs, CI skeleton,
   `ddd-arch-check`, fixtures harness. *(Benefits every later phase.)*
1. **Domain core** — types + lifecycle state machine + `null` backend + contract tests.
2. **Linux backend** — cgroup v2 + process-group fallback + isolation/termination tests.
3. **Stats sampler** — interval polling + resource fixtures/tests.
4. **Output plumbing** — bounded queue + tail capture + output-stress tests.
5. **macOS + Windows backends** — validated via CI.
6. **Hardening** — property/stress/race suites; finalize the guarantee table.

Platform backend implementation is delegated to subagents to keep the main context clean.

## 16. Open decisions to confirm

- Fitness functions in **TypeScript** (default) vs Rust `xtask`.
- Availability of a **privileged/self-hosted Linux runner** for `privileged-cgroup` (else
  containerize on hosted runners).
- Confirm **edition 2021 / MSRV 1.83** (installed toolchain) vs edition 2024.

## 17. Known limitations (current plan)

- macOS containment is weaker than Linux/Windows (no cgroups/Job Objects without privileged
  entitlements); surfaced via `Capabilities` and documented.
- cgroup `cgroup.kill` requires Linux ≥ 5.14; older kernels fall back to sweeping
  `cgroup.procs` with per-pid `SIGKILL`.
- Hosted CI runners may not permit privileged cgroup operations; covered by a separate job.

## 18. Deliverables (definition of done)

Architecture summary · public API summary · platform implementation summary · tests added ·
CI jobs added · guarantees proven locally vs only via CI · known OS limitations · zombie/FD
leak inspection after integration tests · public-API cancellation-safety & ownership review.
Any guarantee an OS cannot provide is exposed through capabilities/types/errors rather than
faked.
