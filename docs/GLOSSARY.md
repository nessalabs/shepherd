# Shepherd — Ubiquitous Language (Glossary)

This glossary is the agreed **ubiquitous language** of the Process Supervision bounded
context. It is **enforced**: the `ddd-architecture` CI job checks that every public
type/method in `shepherd-domain` appears here, and that every term here exists in code.
Drift in either direction fails the build. Keep this file in lockstep with the domain.

See [`DESIGN.md`](./DESIGN.md) for architecture and [`DIAGRAMS.md`](./DIAGRAMS.md) for
class/state diagrams.

## Building-block stereotypes

| Stereotype | Meaning in Shepherd |
| --- | --- |
| Aggregate Root | The consistency boundary; the only entry point for mutating what it owns. |
| Entity | Has identity and a lifecycle; only reachable through its aggregate root. |
| Value Object | Immutable, compared by value; carries no identity. |
| Domain Event | An immutable fact that happened in the domain; emitted at most once logically. |
| Repository | Collection-style access to aggregates. |
| Port | A driven trait owned by the application layer (`shepherd-app`); implemented by an infrastructure adapter. Kept out of the pure domain crate so it stays zero-async and zero-dependency. |
| Adapter / ACL | Infrastructure implementation of a port; translates OS concepts to domain terms. |
| Application Service | Orchestrates the domain and drives ports; owns async. |

## Core terms

### ProcessSupervisor
**Application Service.** The top-level entry point an application constructs and owns.
Coordinates spawning, stats, termination, waiting, reaping, and shutdown across scopes.
Never a global singleton.

### ProcessScope
**Aggregate Root.** The unit of ownership and consistency boundary. Owns one or more root
`Process` entities and enforces the ownership invariants. `terminate_scope` acts on exactly
one scope and can never affect another.

### ProcessScopeId
**Value Object.** Opaque, supervisor-assigned identifier of a `ProcessScope`.

### Process
**Entity.** A single supervised OS process, identified by a `ProcessId`, living inside
exactly one `ProcessScope`. Never exposed as raw mutable child ownership.

### ProcessId
**Value Object.** Opaque, supervisor-assigned handle for a `Process`. Distinct from the OS
pid, to defeat PID reuse.

### OsIdentity
**Value Object.** The OS-level identity of a process: its pid plus a `ReuseToken`.

### ReuseToken
**Value Object.** A discriminator (pidfd / process start-time / OS handle) that detects OS
PID reuse so a recycled pid is never mistaken for a live owned process.

### ProcessSpec
**Value Object.** The immutable specification for spawning a process: program, args, env
policy, working directory, graceful signal, and output mode. Carries no product policy.

### EnvPolicy
**Value Object.** Explicit environment-variable policy for a spawned process.

### OutputMode
**Value Object.** How stdout/stderr are handled: bounded byte-chunk queue (drop-oldest with
a dropped-bytes counter) and/or capped tail capture. Bytes only; never assumes UTF-8.

### ProcessStats
**Value Object.** A point-in-time observation of a process: CPU usage, RSS, virtual memory,
peak RSS, I/O bytes, descendant count, uptime, state, and sample time. Observation only,
never a verdict.

### ProcessState
**Value Object.** The observed OS run state of a process at sample time (e.g. running,
sleeping, zombie).

### ProcessLifecycle
**Value Object (state machine).** The supervised lifecycle state of a `Process`: Spawning,
Running, GracefulRequested, Forcing, ExitedUnreaped, Reaped, SpawnFailed.

### ScopeState
**Value Object (state machine).** The lifecycle state of a `ProcessScope`: Open, Draining,
Closed. A non-Open scope rejects new processes.

### ProcessExit
**Value Object.** The recorded terminal result of a process: exit code, signal, whether
force was required, and the `TerminationOutcome`.

### TerminationOutcome
**Value Object (enum).** The verified result of a termination or exit:
`ExitedNaturally`, `GracefulSuccess`, `ForcedRequired`, `Failed`, `CleanupUnverified`.
Success is only reported after verified non-existence and reap.

### UnverifiedReason
**Value Object (enum).** Why cleanup could not be verified: `DroppedWithoutShutdown`,
`RuntimeShutdown`, `WaitTimedOut`, `ReapFailed`, `ProcessDisappeared`.

### Signal
**Value Object.** A termination signal (e.g. graceful SIGTERM, force SIGKILL). On Windows,
maps to Job Object soft-close / terminate semantics.

### GracePeriod
**Value Object.** The duration to wait after a graceful request before escalating to force.

### Capabilities
**Value Object.** The runtime-detected guarantees of a platform backend: descendant
containment strength, and support for CPU / RSS / peak RSS / I/O stats and force
termination. Guarantees are values, never prose.

### Containment
**Value Object (enum).** The mechanism enforcing whole-tree cleanup, and thus how strong
containment is: cgroup v2, Job Object, POSIX process group, or none.

### Support
**Value Object (enum).** Whether a particular statistic is supported by a backend.

### RawStats
**Value Object.** A raw resource sample produced by a backend, before the supervisor adds
identity and uptime to form a `ProcessStats`.

### RawExit
**Value Object.** Raw exit information reported by a backend after a process is reaped
(code, terminating signal, core-dump flag).

### ScopeTerminationReport
**Value Object.** The aggregated per-process `TerminationOutcome`s produced by
`terminate_scope`.

### ShutdownReport
**Value Object.** The aggregated result of `shutdown` across all scopes.

## Domain events

### DomainEvent
**Domain Event (enum).** The umbrella type of in-process facts returned by aggregate
transitions and dispatched to same-context handlers.

### ProcessSpawned
**Domain Event.** A process was successfully spawned into a scope.

### TerminationRequested
**Domain Event.** Termination was requested for a process or scope.

### ProcessExited
**Domain Event.** A process was observed to have exited (before reaping).

### ProcessReaped
**Domain Event.** An exited process's OS resources were reaped; its terminal state is
confirmed.

### ScopeClosed
**Domain Event.** A scope reached the Closed state; all its processes are reaped.

## Event handling

### EventDispatcher
**Application Service (mediator).** Routes domain events, after the transition is committed,
to the registered in-process `EventHandler`s in deterministic order. In-process only; no
message bus.

### EventHandler
**Port.** Interface for a focused handler that reacts to specific domain events using only
injected ports (dependency inversion). Concrete handlers: `WaitNotifierHandler`,
`RegistryPruneHandler`, `IntegrationTranslator`. Wait/reap is owned by the per-spawn
monitor task, not a handler (see `docs/decisions/0008-monitor-owned-wait.md`).

### WaitNotifierHandler
**Handler.** On `ProcessReaped`, wakes pending `wait(pid)` callers via `Waiters`.

### RegistryPruneHandler
**Handler.** On `ProcessReaped`/`ScopeClosed`, prunes bookkeeping and releases the scope's
containment resource.

### IntegrationTranslator
**Handler.** Maps the externally-meaningful subset of domain events into `IntegrationEvent`s
and publishes them via `IntegrationEventPublisher`.

## Integration events

### IntegrationEvent
**Value Object (enum).** A lifecycle fact published across Shepherd's boundary to another
bounded context (the consuming application's policy layer). Decoupled from internal
invariants; delivery is bounded and lossy-tolerant.

### IntegrationEventPublisher
**Port (outbound).** Interface for publishing `IntegrationEvent`s to the consuming
application. A slow/absent publisher never affects Shepherd's internal state.

## Ports (application-owned driven traits)

### ProcessBackend
**Port.** The platform abstraction: spawn, sample, terminate, terminate_scope, reap, and
report capabilities. Implemented per OS in `shepherd-infra`.

### Clock
**Port.** Time source (now / sleep), so grace periods and sampling are deterministic in
tests.

### OutputSink
**Port.** Destination for drained stdout/stderr byte chunks.

### Waiters
**Port.** Registry that lets `wait(pid)` callers be woken when a process reaches its reaped
terminal state.

## Repository

### ScopeRegistry
**Repository.** Collection-style access to `ProcessScope` aggregates held by a supervisor.

## Errors (typed)

### SpawnError
Typed error for failed process creation.

### TerminateError
Typed error for failed termination.

### StatsError
Typed error for failed stats collection.

### ReapError
Typed error for failed reaping.

### ShutdownError
Typed error for failed supervisor shutdown.

### HandlerError
Typed error surfaced by an `EventHandler`; logged via tracing and never swallowed.

### ScopeClosed
Domain error returned when spawning into a Draining/Closed scope.

### UnknownProcess
Domain error for an unknown `ProcessId`.

### UnknownScope
Domain error for an unknown `ProcessScopeId`.

### DomainError
The umbrella pure-domain error enum (scope closed, unknown process/scope, invalid
transition). Contains no I/O errors.

### InvalidTransition
A domain error indicating an illegal lifecycle transition was attempted.

### ProcessOutput
Application observation handle for a shared consuming byte queue and post-mortem tail.

### OutputStream
Application value distinguishing stdout and stderr.

### OutputChunk
Application value carrying a stream tag and raw bytes.

### OutputSnapshot
Application value containing drained chunks, tail, overflow count, reader errors and EOF flags.

### WithScopeResult
Application value carrying the closure result or initial spawn error alongside a separate cleanup result.

### ScopedProcesses
Application access handle for a fresh block scope; escaping it does not extend scope lifetime.
