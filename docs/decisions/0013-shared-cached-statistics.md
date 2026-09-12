# 0013 — Shared interval sampler and cached per-root observations

Accepted. One lazily started coordinator polls concurrent ProcessBackend sampling
futures. Each live root has at most one in-flight observation, with no per-root
task or thread and no backlog of missed intervals. A shared interval ticker skips
missed ticks, discovers new roots independently of pending observations, and
starts eligible roots at most once per tick. Completed observations publish
independently, so a stalled root cannot delay healthy roots or new arrivals.
Reaped roots have their pending observations cancelled on the next tick. The
coordinator retains only a Weak reference between scheduling/publication steps;
it never owns a CleanupGuard. Construction outside a runtime remains valid.
`SupervisorBuilder::stats_interval` defaults to one second and clamps zero to 1 ms.

`stats(pid)` reads the cache only. `NotReady` distinguishes the initial interval
from unknown/exited processes. Sampling failures are cached StatsError values and
never relinquish process ownership. Each sample call has a one-second timeout;
a slow sampler cannot block cleanup or other observations. Panics are isolated
as sampling errors. Backends must yield during asynchronous work: neither timeouts
nor concurrency can preempt a synchronously blocking poll. Uptime is the sample-time uptime, making
staleness observable. No fresh-read API is added yet.

CPU is a fraction of one core, not a percentage of the machine: 1.0 is one core.
Linux uses deltas of /proc stat user+system ticks and monotonic elapsed time;
the first observation establishes a baseline. RSS/peak/virtual memory use status,
I/O uses read_bytes/write_bytes in /proc io. These are per-root observations even
under cgroups; descendant_count is None. Missing OS data produces an error.
Unsupported platforms continue to report Unsupported rather than invented usage.

The registry is checked again before publishing a completed sample. Reaping drops
cache entries and CPU baselines. Hundreds of roots share a task, not OS threads.

The cache contract test runs on every CI platform, including Windows with this
phase's NullBackend fallback. It verifies caching and post-exit invalidation,
without claiming real Windows resource measurements. Real CPU/RSS observations
are tested on Unix and real I/O counters on Linux in this phase.
