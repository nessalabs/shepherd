# 0015 — macOS process groups, libproc statistics, Mach time conversion

Accepted. The Unix adapter remains the macOS backend. It never claims detached
containment. Tokio Child::wait supplies exit/reap notification, so a separate
EVFILT_PROC watcher would duplicate ownership and is unnecessary (ADR 0008).

libproc BSDInfo supplies the process start time in microseconds for identity
checks. TaskInfo supplies resident/virtual memory and user+system CPU counters.
Those counters are Mach absolute units; convert through mach_timebase_info before
dividing by monotonic elapsed time. mach2 provides the maintained Mach bindings.
The resource tests exercise both CPU usage and increasing resident memory on ARM.
Peak RSS and I/O remain Unsupported/None. Process state is Unknown on this adapter.

A start-time check still has a lookup-to-signal race; this is best-effort process
group containment, never a cgroup/pidfd-equivalent guarantee. Metadata failure
cannot establish an exact process identity.

References: libproc::proc_pid::pidinfo and Apple's XNU fill_taskprocinfo in
osfmk/kern/bsd_kern.c (https://github.com/apple-oss-distributions/xnu).
