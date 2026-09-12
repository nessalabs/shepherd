# 0016 — Windows Job Objects before root execution

Accepted; supersedes the Windows NullBackend default in 0006.

Use Tokio/std command construction and windows-sys. Every root is created with
CREATE_SUSPENDED, assigned to its scope's Job Object, then resumed. The adapter
retains an exact process handle for signals and samples. Creation time is the
plain domain discriminator; all OS operations use the retained handle. Setup
failure force-kills and waits the suspended child before returning an error.

Tokio does not expose the primary thread handle. A Toolhelp thread snapshot taken
while the new process is suspended identifies its initial thread for ResumeThread.
This avoids undocumented NtResumeProcess. The unreaped process handle prevents PID
reuse during setup. As with any multi-call suspended-create/assign sequence,
abrupt supervisor termination before assignment has a window; no kernel backstop
is claimed until assignment. A future STARTUPINFOEX job-list implementation can
close that window with a separate ADR.

Each job has KILL_ON_JOB_CLOSE and no breakaway flags. Explicit cleanup uses
TerminateJobObject and waits for ActiveProcesses==0 before releasing the job.
Root handles are waited separately. Tests distinguish explicit termination from
closing the final job handle itself. Nested jobs supported by current Windows are
required; assignment failures are returned rather than silently using NullBackend.

Windows has no general SIGTERM equivalent. Graceful signaling returns a typed
Signal error; the supervisor's grace interval still allows natural exit before
force. Force reports ForcedRequired only when the observed exit matches the force
exit code and a kill was requested. There is no invented graceful console signal.

CPU uses per-process GetProcessTimes deltas (100 ns ticks), RSS/peak working set use
K32GetProcessMemoryInfo, and I/O uses GetProcessIoCounters logical transfer bytes.
Job accounting is used for containment emptiness, not mislabeled per-root stats.
Virtual memory and descendant counts remain None. Output shares the bounded byte
adapter. The Windows CI matrix executes real tree, orphan, isolation, Drop,
kernel-close, resource and output tests.
