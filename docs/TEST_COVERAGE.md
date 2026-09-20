# Verification coverage and remaining gaps

See [test environment settings](TEST_ENVIRONMENT.md) for all supported tuning variables, defaults, and validation.

The audit found that the long stress runner repeated one serial sleeping-root case,
real-process coverage mostly exercised the current-thread runtime, machine coverage
relied on moving latest images, and property tests hard-coded 64 cases even when the
caller requested more. Those gaps are addressed below without weakening existing
FD/handle, owned-zombie, containment, or domain-coverage assertions.

| Area | Added verification |
| --- | --- |
| Scheduling | Both current-thread and two-worker Tokio runtimes, each with four blocking workers; four concurrent scopes with independent verified cleanup. |
| Mixed lifecycle | Deterministic seeded selection among forced sleepers, natural exits, actual output overflow, scope-body cancellation, partial spawn failure, and concurrent scopes. First six iterations guarantee every scenario executes. |
| Resource accounting | Warm every scenario, all four bounded blocking workers concurrently, and native sampling before the baseline; wait for output EOF; require final descriptors/handles no higher than baseline and no owned Unix zombies. |
| Real launch boundaries | Empty/quoted/backslash/Unicode arguments, Unicode/spaced working directory, explicit/cleared environments, repeated invalid-cwd failures followed by a successful spawn in the same scope. CI injects an environment sentinel that must be removed in the child. |
| OS resource exhaustion | Separate Unix helper lowers RLIMIT_NOFILE, consumes real descriptors until EMFILE, requires spawn failure with no registered process, restores resources and verifies successful spawn/reap. The test runner's limits are untouched. |
| Hardware and toolchains | Existing latest-image suites plus Linux ARM64, Intel macOS, Windows ARM64, Ubuntu 22.04 and Windows 2022; optimized workspace tests and Rust 1.83 execution. Each compatibility job prints its actual host/compiler. |
| libc | Full workspace tests execute native Linux musl binaries in addition to GNU/Linux tests. |
| Long stress | Six named OS/architecture images × two runtimes, configurable bounded iteration count and seed; 512 property cases; per-job timeouts; logs and regression artifacts retained even on failure. |
| Combined application soak | Two to four scopes run changing helpers and output floods while readers slow down or drop, observations are cancelled, partial launches fail, scope bodies are cancelled, and spawn races shutdown. Both Tokio runtimes must finish with verified cleanup, bounded post-warm-up RSS, no descriptor/handle growth, and no owned zombies. |
| Blocking / sync entry | NullBackend contracts that each pin a distinct failure or nesting rule (not flavor-duplicates of the same path). Chaos: mixed timeout/panic/self-terminate/natural-exit on one supervisor; shared current-thread `from_runtime` under concurrent `block_on` + `run` timers; wait-failure is `!all_verified`; timeout + containment failure is `Terminate` not a fake `TimedOut`; panic still exposes a failed cleanup via `wait_scope_cleanup`; scoped-report history evicts at 256. Real processes: the OS cases that differ (closed-stdio hang, cleared-env sleeper, group descendants, missing program) plus one mixed timeout/spawn-fail/exit/`with_scope` storm. |
| Unsafe boundaries | Safe wrappers replace raw calls where practical; platform Clippy requires safety comments, and CI checks the [unsafe register](UNSAFE_CODE.md) against source. |
| CI definitions | Pinned actionlint checks workflow syntax/expressions on every PR. |

The bounded mixed stress test runs under ordinary workspace CI. The long workflow
remains manual so each pull request does not automatically launch twelve long jobs.
Example local reproduction:

```sh
SHEPHERD_STRESS_ITERATIONS=2000 SHEPHERD_STRESS_SEED=20260912 SHEPHERD_STRESS_RUNTIME=both cargo test -p shepherd-fixtures --test leak_stress long_create_kill -- --ignored --nocapture
PROPTEST_CASES=512 cargo test -p shepherd --test properties
```

Scenario indices in failure logs/counts are: 0 forced sleeper, 1 natural binary exit,
2 output overflow, 3 cancelled scope body, 4 partial spawn failure, 5 four-scope fanout.
A mixed cycle can create multiple processes; counts are cycles, not process totals.
Native sampling and the complete bounded blocking pool are explicitly warmed to
distinguish runtime initialization from leaks. Blocking workers remain alive during
the measurement (one-hour keep-alive, longer than the workflow timeout). This avoids
counting lazy worker growth as a process-resource leak while retaining the strict
zero-growth assertion. Windows logs include read-only native handle-type snapshots;
all platforms emit counts every 500 cycles. Workflow inputs can select Windows/macOS and one runtime for focused reproduction.
An external Python watchdog enforces progress/overall deadlines independently of Tokio,
captures macOS native thread samples on a stall, and retains diagnostics before failing.
Its success, nonzero-exit, early-EOF and timeout paths run in CI and before long stress.
A timeout is never treated as verified cleanup; the hosted runner cleans up after failure.

Still not proven by this matrix:

- Arbitrary kernel versions, cgroup controller/delegation combinations, systemd policies,
  containers, restricted procfs mounts, or enterprise endpoint-security software.
- Full machine crash/power loss and every abrupt supervisor-death window. Existing
  runtime/Drop/backstop tests and documented OS limitations still apply.
- Permission-denied process inventories across all users and historical ancestry after
  reparenting; process-tree observations remain best-effort.
- Real forced PID reuse at wraparound, physical memory exhaustion, and every Windows
  handle-quota failure. Deterministic identity/failure tests are narrower evidence.
- Every possible thread/OS interleaving. Loom models selected in-memory synchronization;
  seeded scenarios make failures reproducible but do not control the OS scheduler.

The machine labels come from GitHub's [hosted runner reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
Actual execution results must be recorded on the PR; declaring a matrix is not proof
that those machines passed.

## Failure found by the expanded matrix

Intel macOS multi-thread stress stalled in native Command::spawn while another worker
waited on backend state. The external watchdog captured the native stacks. Darwin's
non-atomic pipe/CLOEXEC setup permits concurrent fork paths to inherit each other's
exec-handshake pipe. Shepherd now serializes its native root and anchor spawns across
backend instances on macOS, releasing the gate before reacquiring backend state.
This gate coordinates Shepherd's own spawn paths; unrelated application fork/spawn
implementations do not participate in it. The mixed fanout stress is the native
regression, run on both macOS architectures with an independent watchdog.

## Combined soak limits

`application_soak` runs six warm-up rounds and six measured rounds in ordinary CI.
The manual workflow defaults to 200 measured rounds on each OS/runtime combination.
It samples the supervisor process's resident memory after each round, allowing at
most 64 MiB above the warm baseline. This catches sustained large growth; it does
not prove that every allocation is freed or detect every small leak. Native handles
and descriptors retain the stricter final zero-growth assertion.

The seed varies scope counts, polling delays, and shutdown timing. It records the
workload choices, not an exact replay of thread or OS scheduling. Longer runs use
the external watchdog and retain progress and stall diagnostics.

See [heap lifetime verification](HEAP_VERIFICATION.md) for LeakSanitizer controls,
live-allocation bounds in a long-lived supervisor, and direct ownership-release tests.
