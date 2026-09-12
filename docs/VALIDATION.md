# DESIGN §18 deliverables and skeptical review

## Architecture and API

The workspace keeps infra → app → domain. Domain code forbids unsafe and has only
the positively allowlisted thiserror/proc-macro dependency closure. Application-owned
ports drive spawn, signal, wait/reap, sample, scope cleanup, synchronous kill and
output observation. The facade selects real platform adapters and a no-op publisher.
ADRs 0001–0020 and DIAGRAMS.md describe the implemented decisions.

Public operations: create_scope, spawn, processes, capabilities, cached stats, wait,
terminate, terminate_scope, shutdown, take_output, with_scope/with_scope_options and
wait_scope_cleanup. Raw Child ownership is never exposed. WithScopeResult preserves
the closure value or initial spawn error alongside an independent cleanup result.
Nested blocks create independent scopes. OWNERSHIP.md reviews every public operation.

## Platforms and test evidence

| Claim | Evidence |
| --- | --- |
| Linux cgroup v2 contains setsid descendants | Privileged hosted sudo tests passed in runs 34674051633, 34674491652 and 34674557216: detached child, isolation, parent-exits-first, last-owner Drop, and adopted-descendant reap. |
| Linux fallback reports ProcessGroup | Explicit UnixProcessBackend::new capability assertion, ordinary-directory rejection for required cgroups, and unprivileged real Unix tests. |
| Windows defaults to Job Object | Real windows-latest tree/isolation/orphan/Drop tests and a distinct suspended-root KILL_ON_JOB_CLOSE test passed in run 34674491652 and again in 34674557216. |
| macOS reports weaker containment and real stats | Local ARM macOS and macos-latest tests exercise process groups, libproc RSS and Mach-converted CPU. Peak RSS/I/O remain Unsupported. |
| Stats are cached observations | Cache-stability test plus CPU/RSS growth and I/O counter tests. Linux and Windows executed real counters in CI; macOS CPU/RSS executed locally and in CI. |
| Flooding cannot block cleanup | Exact binary/non-UTF-8 bytes, stdout/stderr/both floods, overflow accounting, capped tail, non-consumption, discard and reader-failure tests. Real output tests pass on all three OSes. |
| Scope guard cleanup is cancellation-safe on a running runtime | Success, ?, partial spawn failure, abort, direct future drop, panic, nested cleanup and runtime-interruption tests. Phase E ran real-process cleanup on all OSes in run 34674557216. |
| Supervisor Drop differs from terminate-future Drop | Separate real owner-drop tests and an in-flight terminate future canceled while its stubborn child remains alive. |
| Domain coverage and architecture | Local 100% line coverage, positive dependency allowlist, purity/glossary/encapsulation check, MSRV 1.83 clippy and workspace tests. Final Phase F CI status is recorded below. |

CI run links: [Phase A](https://github.com/nessalabs/shepherd/actions/runs/34674051633),
[platforms](https://github.com/nessalabs/shepherd/actions/runs/34674491652),
[async scopes](https://github.com/nessalabs/shepherd/actions/runs/34674557216).
Cross-clippy for Linux and Windows was also run locally; those checks are explicitly
compilation evidence, not substitutes for the runtime jobs above.

## Hardening and leak inspection

Proptest generates domain traces and contract-backend lifecycles for invariants 1–9.
Tests exercise duplicate attachment, closed-scope rejection, scope isolation, repeated
termination, event idempotence, reap, and pruning. Loom models actual ScopeRegistry
mutations and InMemoryWaiters registration/publication under a substituted model
mutex; it does not model Tokio or the OS implementation itself.

Deterministic failure/race tests cover spawn failure and cancellation during admitted
spawn, signal failure, lost waiters, sampler failure/panic, waiter panic, reader
failure, exit during sampling, exit between lookup and signal, mismatched reuse
tokens, terminate versus natural exit/spawn/shutdown, final sweep failure, and stalled
external publishers. Histories are bounded while active waiters retain completion.
A 600-root fanout test crosses that history bound without losing observations.

The local ARM macOS stress run completed **2,000 create/capture/kill/reap cycles**
inside one process. `/dev/fd` counted **10 descriptors before and 10 after**. `ps`
found no owned zombies. Warm-up precedes the baseline; final count must be no larger.
Linux uses `/proc/self/fd`; Windows uses GetProcessHandleCount, plus job tests verify
ActiveProcesses=0. Unix zombie inspection filters ps by the test process's parent ID,
so unrelated host processes cannot create false positives.

```sh
cargo test --workspace
cargo fmt --all --check
cargo +1.83.0 clippy --workspace --all-targets --all-features -- -D warnings
(cd tools/ddd-arch-check && node --experimental-strip-types check.ts)
cargo llvm-cov -p shepherd-domain --fail-under-lines 100
cargo test -p shepherd-app --lib loom_
RUSTFLAGS='--cfg shepherd_loom' cargo test -p shepherd-infra --lib loom_
SHEPHERD_STRESS_ITERATIONS=2000 cargo test -p shepherd-fixtures --test leak_stress long_create_kill -- --ignored --nocapture
```

The manual stress workflow runs long loops on Linux, macOS and Windows. Privileged
cgroup prerequisites and a fail-closed command are in ADR 0012. Final Phase F CI and
manual stress execution are pending at this document revision; they must be recorded
before the hardening checkbox is closed.

## OS limits

Process groups do not contain setsid escapes, and macOS start-time signaling retains
a TOCTOU. Cgroups are not a security boundary against privileged migration and do not
kill automatically on creator SIGKILL. Windows kernel close protection starts after
job assignment, leaving an abrupt-death window during suspended creation. No generic
Windows graceful signal is invented. Non-child zombie reap belongs to the OS parent
or subreaper. Runtime shutdown cannot prove asynchronous wait/reap and produces an
unverified result. Unsupported measurements remain Unsupported/None. These limits
are reflected in the implementation table, errors and ownership review.
