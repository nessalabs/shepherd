# 0015 — macOS process groups, libproc statistics, Mach time conversion

Accepted. The Unix adapter remains the macOS backend. It never claims detached
containment. Tokio Child::wait supplies exit/reap notification, so a separate
EVFILT_PROC watcher would duplicate ownership and is unnecessary (ADR 0008).

libproc BSDInfo supplies the process start time in microseconds for identity
checks. TaskInfo supplies resident/virtual memory and user+system CPU counters.
Those counters are Mach absolute units; convert through mach_timebase_info before
dividing by monotonic elapsed time. mach2 provides the maintained Mach bindings.
The resource tests exercise both CPU usage and increasing resident memory on ARM.
A macOS-only single-thread burner regression averages at least eight fresh 250 ms
intervals and checks a generous 0.05..=2 core range. This catches both a missing
and a duplicate conversion on Apple Silicon without assuming an exact one-core
instantaneous sample. Intel's 1:1 timebase cannot distinguish those errors.
Peak RSS and I/O remain Unsupported/None. Process state is Unknown on this adapter.

A start-time check still has a lookup-to-signal race; this is best-effort process
group containment, never a cgroup/pidfd-equivalent guarantee. Metadata failure
cannot establish an exact process identity.

Unit evidence (XNU main inspected 2026-09-12, revision
`f6217f891ac0bb64f3d375211650a4c1ff8ca1ea`):

- [`fill_taskprocinfo`](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/osfmk/kern/bsd_kern.c#L1055-L1063)
  assigns `recount_times_mach.rtm_user` and `rtm_system` directly to the total CPU
  counters. These are raw Mach time values, requiring one timebase conversion.
- [`proc_pidtaskinfo`](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/proc_info.c#L866-L877)
  fills the public structure directly; the
  [`PROC_PIDTASKINFO` response](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/proc_info.c#L2152-L2162)
  copies that structure out without converting the counters to nanoseconds.

On local Apple Silicon macOS 26.6 with timebase 125/3, the existing conversion
reported approximately one core for the single-thread burner. Removing it would
report approximately 0.024 cores; applying it twice would report approximately
41.7 cores. Preserve the conversion and verify units against the specific libproc
flavor instead of assuming all macOS time fields use nanoseconds.
