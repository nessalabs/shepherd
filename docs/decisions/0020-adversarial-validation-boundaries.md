# 0020 — Generated domain traces, actual lock models, and in-process leak accounting

Accepted. Proptest lives in the facade's test dependencies and drives both the pure
domain API and NullBackend contracts. The domain keeps only its thiserror dependency
closure, now positively allowlisted by the architecture check. Entity and aggregate
field privacy is checked too. Domain unit tests cover new guards; 100% line coverage
remains mandatory.

Loom explores registry mutation interleavings using the actual ScopeRegistry, and
waiter registration/publication using the actual InMemoryWaiters with its mutex and
Arc substituted under cfg(shepherd_loom). This does not purport to verify Tokio's
internal watch implementation or kernel scheduling. A custom cfg avoids Tokio's own
cfg(loom), which intentionally removes process support.

Leak checks run many lifecycles inside one test process after a warm-up. Linux counts
/proc/self/fd, macOS /dev/fd, Windows GetProcessHandleCount. The final count must not
exceed baseline. Unix ps inspects only children of the test process for zombies,
avoiding unrelated host processes. Job tests additionally require ActiveProcesses=0
and wait on retained descendant handles. workflow_dispatch runs long loops on all
three OSes, plus cancellation/race/property tests.

Privileged cgroup tests remain a separate mandatory fail-closed job. Cross-compiling
is recorded separately from executing a backend; neither ignored tests nor empty
platform test binaries count as runtime evidence.
