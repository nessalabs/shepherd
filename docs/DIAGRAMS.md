# Shepherd — Class & State Diagrams

Companion to [`DESIGN.md`](./DESIGN.md). Diagrams use [Mermaid](https://mermaid.js.org/)
and render on GitHub. DDD stereotypes are shown with `<<...>>` annotations.

---

## 1. Layer / dependency diagram (hexagonal)

Dependencies point **inward only**: `infra → app → domain`. Enforced by the
`ddd-architecture` CI job.

```mermaid
flowchart TD
    subgraph facade["shepherd (facade crate)"]
        F[Public API re-exports + wiring]
    end
    subgraph app["shepherd-app (application)"]
        SUP[ProcessSupervisor<br/>application service]
        SAMP[StatsSampler]
        MON[per-spawn monitor<br/>wait + reap]
        PORT[Ports: ProcessBackend / Clock / Waiters / OutputSink / EventHandler / IntegrationEventPublisher]
    end
    subgraph domain["shepherd-domain (PURE)"]
        AGG[ProcessScope&nbsp;«Aggregate Root»]
        ENT[Process&nbsp;«Entity»]
        VO[Value Objects]
        EV[Domain Events]
    end
    subgraph infra["shepherd-infra (adapters / ACL)"]
        LIN[UnixProcessBackend<br/>tokio::process + nix<br/>cgroup v2 later]
        WIN[WindowsBackend<br/>Job Object + windows-sys]
        MAC[MacBackend<br/>process group + libproc]
        NUL[NullBackend&nbsp;test fake]
        CLK[SystemClock]
        OUT[PipeOutputSink]
    end

    F --> SUP
    SUP --> AGG
    SUP --> PORT
    SAMP --> PORT
    MON --> PORT
    AGG --> ENT
    AGG --> VO
    AGG --> EV
    LIN -. implements .-> PORT
    WIN -. implements .-> PORT
    MAC -. implements .-> PORT
    NUL -. implements .-> PORT
    CLK -. implements .-> PORT
    OUT -. implements .-> PORT
    F --> infra
```

> The domain never depends on `app`, `infra`, or any OS crate (and has no async). The driven
> **ports live in `shepherd-app`**; adapters in `shepherd-infra` *implement* them; wiring
> happens only in the facade.

---

## 2. Class diagram — domain model

```mermaid
classDiagram
    class ProcessSupervisor {
        <<Application Service>>
        -ScopeRegistry registry
        -ProcessBackend backend
        -StatsSampler sampler
        +spawn(scope_id, spec) ProcessId
        +stats(pid) ProcessStats
        +processes(scope_id) Vec~ProcessId~
        +terminate(pid, opts) ProcessExit
        +terminate_scope(scope_id, opts) ScopeTerminationReport
        +wait(pid) ProcessExit
        +shutdown() ShutdownReport
        +with_scope(specs, closure) T
    }

    class ScopeRegistry {
        <<Repository>>
        +insert(ProcessScope)
        +get(ProcessScopeId) ProcessScope
        +remove(ProcessScopeId)
        +scopes() Vec~ProcessScopeId~
    }

    class ProcessScope {
        <<Aggregate Root>>
        -ProcessScopeId id
        -ScopeState state
        -Map processes
        +spawn(spec) Result~ProcessId~
        +request_termination(now) Vec~Command~
        +record_exit(pid, RawExit) Vec~DomainEvent~
        +record_reaped(pid) Vec~DomainEvent~
        +close() Vec~DomainEvent~
        +is_open() bool
    }

    class Process {
        <<Entity>>
        -ProcessId id
        -OsIdentity os
        -ProcessLifecycle state
        -ProcessSpec spec
        -Instant started_at
    }

    class ProcessSpec {
        <<Value Object>>
        +OsString program
        +Vec~OsString~ args
        +EnvPolicy env
        +Option~PathBuf~ cwd
        +Signal graceful_signal
        +OutputMode output
    }

    class ProcessStats {
        <<Value Object>>
        +ProcessId pid
        +f32 cpu_usage
        +u64 memory_rss_bytes
        +Option~u64~ virtual_memory_bytes
        +Option~u64~ peak_rss_bytes
        +Option~u64~ io_read_bytes
        +Option~u64~ io_write_bytes
        +Option~u32~ descendant_count
        +Duration uptime
        +ProcessState state
        +Instant sampled_at
    }

    class ProcessExit {
        <<Value Object>>
        +ProcessId pid
        +Option~i32~ code
        +Option~Signal~ signal
        +TerminationOutcome outcome
        +bool forced
    }

    class TerminationOutcome {
        <<Value Object / enum>>
        ExitedNaturally
        GracefulSuccess
        ForcedRequired
        Failed
        CleanupUnverified
    }

    class Capabilities {
        <<Value Object>>
        +Containment descendant_containment
        +Support cpu
        +Support rss
        +Support peak_rss
        +Support io
        +bool force_termination
    }

    class ProcessBackend {
        <<Port>>
        +spawn(scope, spec) Spawned
        +sample(os) RawStats
        +terminate(os, signal)
        +terminate_scope(scope, signal)
        +reap(os) Option~RawExit~
        +capabilities() Capabilities
    }
    class Clock {
        <<Port>>
        +now() Instant
        +sleep(Duration)
    }
    class OutputSink {
        <<Port>>
        +on_stdout(bytes)
        +on_stderr(bytes)
    }

    class DomainEvent {
        <<Domain Event / enum>>
        ProcessSpawned
        TerminationRequested
        ProcessExited
        ScopeClosed
        ProcessReaped
    }

    ProcessSupervisor --> ScopeRegistry
    ProcessSupervisor ..> ProcessBackend
    ProcessSupervisor ..> Clock
    ScopeRegistry "1" o-- "*" ProcessScope
    ProcessScope "1" *-- "*" Process : owns
    Process --> ProcessSpec
    Process --> ProcessStats : sampled
    Process --> ProcessExit : on exit
    ProcessExit --> TerminationOutcome
    ProcessBackend --> Capabilities
    ProcessScope ..> DomainEvent : raises
    ProcessSpec --> OutputSink
```

Notes:
- `ProcessScope` is the **consistency boundary**; `Process` is only reachable *through* it
  (composition `*--`). External code holds `ProcessId`, never a `Process`/`Child`.
- `ProcessBackend`, `Clock`, `OutputSink` are **application-owned driven ports** (in
  `shepherd-app`, not the domain); the concrete backends (`LinuxBackend`, …) implement them in
  `shepherd-infra`.

---

## 3. State diagram — process lifecycle

```mermaid
stateDiagram-v2
    [*] --> Spawning
    Spawning --> Running : spawn ok
    Spawning --> SpawnFailed : spawn error

    Running --> ExitedUnreaped : natural exit detected
    Running --> GracefulRequested : terminate(graceful)
    Running --> Forcing : terminate(force) / hard shutdown

    GracefulRequested --> ExitedUnreaped : exited within grace
    GracefulRequested --> Forcing : grace period elapsed
    GracefulRequested --> GracefulRequested : terminate() again (idempotent)

    Forcing --> ExitedUnreaped : exited
    Forcing --> Forcing : terminate() again (idempotent)

    ExitedUnreaped --> Reaped : reap (waitpid / pidfd / handle)

    Reaped --> [*] : pruned after outcome observed
    SpawnFailed --> [*]

    note right of ExitedUnreaped
        Exit info captured but not yet
        confirmed reaped. Zombie until reap.
    end note
    note right of Reaped
        Only here can GracefulSuccess /
        ForcedRequired be reported.
        Terminal + idempotent.
    end note
```

Illegal transitions are unrepresentable in the type/state machine. Terminal states
(`Reaped`, `SpawnFailed`) make repeated `terminate`/`wait`/`shutdown` calls no-ops that
return the recorded outcome.

---

## 4. State diagram — scope lifecycle (aggregate)

```mermaid
stateDiagram-v2
    [*] --> Open
    Open --> Open : spawn(spec) / process exits
    Open --> Draining : terminate_scope() / shutdown()
    Draining --> Draining : process exits + reaped
    Draining --> Closed : all processes reaped
    Closed --> [*]

    note right of Draining
        No new processes accepted.
        spawn() -> Err(ScopeClosed)
        (invariant #3)
    end note
    note right of Closed
        ScopeClosed domain event emitted.
        terminate_scope() is idempotent here.
    end note
```

---

## 5. Sequence diagram — `terminate_scope` (verified two-phase)

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller
    participant S as ProcessSupervisor (app)
    participant A as ProcessScope (aggregate)
    participant B as ProcessBackend (infra)
    participant M as monitor (since spawn)

    C->>S: terminate_scope(scope_id, opts)
    S->>A: begin_scope_termination + request_termination
    A-->>S: [TerminationRequested ...]
    S->>B: signal each live pid (graceful)
    Note over S: await grace period (Clock)
    alt survivors remain
        S->>B: signal SIGKILL / signal_scope
    end
    S->>M: await waiters (monitor already waiting)
    M->>B: wait(spawned) already in flight
    B-->>M: RawExit
    M->>A: record_exit + record_reaped
    A-->>M: [ProcessExited, ProcessReaped]
    A->>A: close() when all reaped
    S-->>C: ScopeTerminationReport { per-process TerminationOutcome }
```

Success in the report means **verified**: processes confirmed gone *and* reaped — never
merely "signal sent". A path that cannot confirm returns
`CleanupUnverified(<reason>)` rather than a false success.

---

## 6. State diagram — cleanup tiers (where an outcome comes from)

```mermaid
flowchart TD
    START{How did teardown happen?} 
    START -->|explicit terminate/shutdown .await| P1[Full two-phase, verified]
    START -->|async with_scope block exit / cancel| P1
    START -->|handle dropped, supervisor alive| P2[Drop hard-kill + reaper finishes]
    START -->|handle dropped, supervisor gone| P3[Drop hard-kill only]
    START -->|SIGKILL / abort, Drop never runs| P4[Kernel backstop]

    P1 --> O1[GracefulSuccess / ForcedRequired / ExitedNaturally]
    P2 --> O2[Verified reaped async; event recorded]
    P3 --> O3[CleanupUnverified DroppedWithoutShutdown]
    P4 --> O4[OS kills tree: JobObject KILL_ON_JOB_CLOSE / cgroup / PDEATHSIG]
```

---

## 7. Class diagram — event handling (domain vs integration)

In-process **domain events** drive same-context handlers that depend only on ports;
**integration events** cross Shepherd's boundary through an outbound, decoupled publisher.

```mermaid
classDiagram
    class EventDispatcher {
        <<Application Service (mediator)>>
        -List handlers
        +dispatch(events) Result
    }
    class EventHandler {
        <<Port>>
        +handle(event) Result~HandlerError~
    }
    class WaitNotifierHandler {
        <<Handler>>
        -Waiters waiters
        +handle(event) Result
    }
    class RegistryPruneHandler {
        <<Handler>>
        -ScopeRegistry registry
        +handle(event) Result
    }
    class IntegrationTranslator {
        <<Handler>>
        -IntegrationEventPublisher publisher
        +handle(event) Result
    }
    class IntegrationEventPublisher {
        <<Port (outbound)>>
        +publish(IntegrationEvent)
    }
    class IntegrationEvent {
        <<Value Object / enum>>
        ProcessTerminated
        ScopeTerminated
    }

    EventDispatcher o-- "*" EventHandler
    WaitNotifierHandler ..|> EventHandler
    RegistryPruneHandler ..|> EventHandler
    IntegrationTranslator ..|> EventHandler
    WaitNotifierHandler ..> Waiters : depends on port
    RegistryPruneHandler ..> ScopeRegistry : depends on port
    IntegrationTranslator ..> IntegrationEventPublisher : depends on port
    IntegrationEventPublisher ..> IntegrationEvent : publishes
    EventDispatcher ..> DomainEvent : routes
```

Notes:
- All handlers depend on **ports**, never concretions (dependency inversion), so each is
  isolated and testable with fakes; wiring/injection happens only in the facade.
- `IntegrationTranslator` maps the externally-meaningful *subset* of domain events to
  integration events. A slow/absent `IntegrationEventPublisher` cannot affect Shepherd's
  internal invariants.

---

## 8. Sequence diagram — event dispatch (in-process domain events → optional integration)

```mermaid
sequenceDiagram
    autonumber
    participant APP as ProcessSupervisor (app)
    participant AGG as ProcessScope (aggregate)
    participant D as EventDispatcher (mediator)
    participant H2 as WaitNotifierHandler
    participant H3 as RegistryPruneHandler
    participant IT as IntegrationTranslator
    participant PUB as IntegrationEventPublisher

    Note over APP: monitor task already waiting since spawn (ADR 0008)
    APP->>APP: backend.wait(spawned)
    APP->>AGG: record_exit + record_reaped
    AGG-->>APP: [ProcessExited, ProcessReaped]
    Note over APP: transition committed under lock, then lock released
    APP->>D: dispatch([ProcessExited, ProcessReaped])
    D->>H2: handle(ProcessReaped) -> wake wait(pid)
    D->>H3: handle(ProcessReaped) -> prune + release resource
    D->>IT: handle(ProcessReaped)
    IT->>PUB: publish(ProcessTerminated)
    Note over IT,PUB: bounded, lossy-tolerant; cannot stall Shepherd
    Note over D: handler error -> typed HandlerError + tracing, never swallowed
```
