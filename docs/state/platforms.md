---
id: "platforms"
title: "Choose platform guarantees"
kind: "contract"
status: "implemented"
summary: "Containment and accounting differ by OS; inspect capabilities before relying on them."
parent: "shepherd"
sources: ["crates/shepherd-domain/src/capability.rs", "crates/shepherd-infra/src/backend/unix.rs", "crates/shepherd-infra/src/backend/cgroup.rs", "crates/shepherd-infra/src/backend/windows.rs", "docs/UNSAFE_CODE.md", "docs/TEST_ENVIRONMENT.md"]
sourceRevision: "e56c5e583793ad7d69b4dd4738857909ca8007ad"
diagramLinks: {}
---

# Choose guarantees the platform can provide

Stopping a process group is not the same as containing every descendant. A program
may detach from a Unix process group. Before promising that closing a workspace
removes all of its descendants, inspect `supervisor.capabilities()` and the selected
backend.

| Backend | Root wait/reap | Detached containment | Cleanup boundary |
| --- | --- | --- | --- |
| Linux cgroup v2 | Supported | Supported inside the cgroup | cgroup kill and accounting |
| Linux process-group fallback | Supported | Not guaranteed | Best-effort group signal |
| macOS process group | Supported | Not guaranteed | Best-effort group signal |
| Windows Job Object | Supported | Supported within the Job policy | Job termination |

Linux chooses containment at construction. Process-group scopes keep a private
anchor until cleanup to stabilize group identity. Windows has no generic graceful
signal; force follows the configured grace interval. Kept processes are supported
on Linux and macOS; unsupported platforms return an explicit error.

## Limits callers must retain

Cgroup containment does not promise cleanup after abrupt supervisor SIGKILL.
Runtime teardown can prevent asynchronous reaping even after a hard-kill request.
macOS cached peak RSS and I/O differ from Linux/Windows support; unsupported fields
must not be displayed as zero. Observation permissions and PID reuse limitations
remain separate from managed-process identity protections.

The unsafe-code register records native pre-exec, pidfd, accounting and handle
boundaries. This atlas does not execute or prove those OS operations. Linux
privileged cgroup and Windows Job/handle qualification require their own hosts;
see [verification](verification.md) for the local evidence and unrun commands.
