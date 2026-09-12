# Measuring a program and its helpers

Status: implemented on this branch. Platform CI verifies the native adapters.
The APIs below collect observations on demand; they do not run background polling.

A program often starts other programs to help it work. Looking only at the first
program can hide most of the CPU and memory being used. Shepherd can show
the individual processes and a useful total.

New to processes? Read the [short primer](primer/processes-and-resources.md) first.

## What changed

Shepherd can show a root process and its visible descendants, even when it did not
start them. It can also report resource measurements for individual roots it owns.
The new `tree_usage` and `scope_usage` APIs add aggregate observations without changing
process ownership. Existing `tree` and managed-root `stats` behavior is preserved.

## Two questions we answer

### What is this program using now?

Start with any OS process ID. Find its visible descendants, measure the processes,
and return both the tree and totals. Use the same API whether Shepherd started the
root or another application did.

```text
Build launcher              CPU: 0.1 cores   Memory:  20 MiB
|-- Compiler                CPU: 0.8 cores   Memory: 200 MiB
`-- Compiler                CPU: 0.6 cores   Memory: 180 MiB
                            -------------           -------
Observed total              CPU: 1.5 cores   Memory: 400 MiB*

* Sum of resident memory; shared memory may be counted more than once.
```

Tree usage provides CPU time, resident memory, the number of observed processes,
and sampling times. Comparing two snapshots provides CPU usage. Keep each process's measurements available
so a caller can explain the total or display the largest users.

This is a view of what we can see now. A helper that exits between observations may
never appear. A helper whose parent changes may no longer be connected to the root.
Permission restrictions can hide processes entirely. Do not call the result complete.

### What has this Shepherd scope used?

A scope is the collection of programs Shepherd manages together. On supported
systems, ask the operating system for the scope's own accounting.

```text
Shepherd scope
|-- Program A -- helpers
`-- Program B -- helpers
         |
         v
Operating-system accounting
         |
         v
Scope measurements, including historical usage where supported
```

This can retain usage from processes that have already exited. It follows container
membership, which is different from following parent-child links. A scope can contain
several independent roots.

Use Linux cgroup accounting and Windows Job Object accounting where available.
macOS and Linux process-group fallback should report unsupported kernel scope
counters explicitly. They can still provide sampled observations, as described
for macOS below.

## How this would work on macOS

Research checked on September 12, 2026. The per-process probe results below are
local evidence; CI also exercises the Rust implementation on Intel and Apple Silicon.

**Keep `ProcessScopeId` as the caller's identifier. Build current totals from individual
process measurements.** macOS has the pieces for this, although they do not give us
an exact lifetime counter for each Shepherd scope.

### Finding the right processes

Shepherd already associates a managed scope with a process group. Its **PGID** is
the operating system's number for that group. Apple provides `proc_listpgrppids`
to list group members, alongside APIs for listing children and all processes.
[Apple's process-query interfaces](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/libsyscall/wrappers/libproc/libproc.h)

```text
Managed scope                            External program
ScopeId                                  Root PID
   |                                        |
Shepherd's process-group ID              Visible parent-child links
   |                                        |
Current group members                    Root + visible descendants
   +-------------------+--------------------+
                       |
             Measure each unique process
                       |
             Current totals + missing data
```

For a managed scope, group membership is a better starting point than ancestry
alone: a helper can stay in the group after its parent exits. Exclude Shepherd's
private anchor process, which exists to keep the group identity stable, from user
workload totals. Check that the scope still refers to the same group during collection.

For an external root, use the existing read-only tree discovery. Do not move the
process into a group. Do not use its PGID as a shortcut: that group might also
contain unrelated programs.

These remain different selection rules. A child that successfully calls `setsid`
creates a new group and leaves the old one. It may still appear in an ancestry
snapshot while its parent link remains visible. Report which selection rule was
used; do not silently combine the two sets.
[Apple's session-creation manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/setsid.2.html)

### Measuring their usage

Extend the macOS measurement adapter to observed identities without registering
those processes for cleanup. Take two samples to calculate CPU usage, check identity around reads, and
report processes that could not be measured. Keep sampling off the async executor
and bound concurrent work.

**Use `proc_pid_rusage` as the macOS measurement source.** We checked the
Apple implementation and ran a [small reproducible probe](research/macos-rusage-probe.c)
on Apple Silicon, macOS 26.6 (25G72). It supplies the fields we need without taking
ownership of the target. The Rust adapter now uses this API, with record-version fallback and native unit tests.

| Fields | How we should use them |
| --- | --- |
| `ri_user_time`, `ri_system_time` | Per-process CPU counters. Convert Mach time units using the host timebase, then divide the change by elapsed time for cores used. |
| `ri_resident_size` | Current resident bytes; keep the shared-memory caveat when summing. |
| `ri_phys_footprint` | Separate physical-footprint measure in bytes. Do not call it RSS. |
| `ri_lifetime_max_phys_footprint` | Per-process peak footprint, available in V4. Do not sum it into a claimed simultaneous scope peak. |
| `ri_diskio_bytesread`, `ri_diskio_byteswritten` | Disk-I/O byte counters in V2 and newer. They are not all application read/write transfers. |
| `ri_proc_start_abstime` | An additional native identity check for sampling. Keep it in its own clock domain; it is not a Unix timestamp. |

Apple's [record definitions](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/sys/resource.h),
[counter collection](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/osfmk/kern/bsd_kern.c#L1248),
and [CPU source](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/osfmk/kern/task.c#L6808)
explain the fields. The disk counters can also be zero when internal I/O accounting
is unavailable, so a successful query alone does not prove that no disk activity
occurred. Do not claim stronger availability evidence than the API provides.

The local probe found:

- CPU time was **0.398691 seconds** through `getrusage` and **0.398696 seconds**
  through Mach-converted `proc_pid_rusage`. Treating the raw value as nanoseconds
  would incorrectly report **0.009569 seconds**. This host's conversion was 125/3.
- Touching 64 MiB raised resident memory by about 64 MiB. An 8 MiB file write followed
  by `fsync` raised the disk-write counter by 8 MiB; logical writes differed.
- Record versions V0 through V4 succeeded. Querying our independently spawned child
  worked while it was live and after exit while still waiting to be reaped.
- After reaping, the query returned `ESRCH` (no such process). PID 1 returned `EPERM`
  (permission denied). This is not a promise of access to every same-user process.

Request V4 with a correctly sized, initialized record. If a supported older system
rejects that flavor with `EINVAL`, retry V2, then V0, and mark fields absent from the
successful version unsupported. Do not retry permission or vanished-process errors
as version problems. Test those fallback paths explicitly; this Mac did not require
them. Apple's [query implementation](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/proc_info.c#L3820)
checks access, and its [version dispatch](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/kern_resource.c#L3414)
selects the returned record size. The native wrapper must follow the SDK's buffer ABI.

The `ri_child_*` fields are accumulated child-accounting fields, not a live tree
snapshot. Keep them out of per-process sums to avoid counting descendant work twice.
[Apple's child-accounting update](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/kern_resource.c#L1865)
shows child and descendant counters being combined.

This API still does not discover membership or create a scope counter. We must select
processes separately and handle missing samples. It also does not justify delaying
reaping to collect metrics. If we retain history, label it observed history: final
work and entire short-lived workers can be missed.

### Why not use another macOS group counter?

**`getrusage(RUSAGE_CHILDREN)` is not a scope counter.** It reports usage for the
calling process's terminated, waited-for children. It cannot select a Shepherd
scope or an arbitrary external tree, and does not describe the live helpers we
want to display. Changing who waits for children would also interfere with lifecycle
ownership, so this feature must not introduce another reaper.
[Apple's resource-usage manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/getrusage.2.html)

**Coalitions exist, but are not our backend.** These are macOS kernel
resource groups. Apple's kernel checks privileged coalition membership for coalition
creation and control. That makes them unsuitable as freely created per-scope
containers for an ordinary embedded library. This is a design conclusion from the
kernel interface, not a claim that macOS has no group accounting internally.
[Apple's coalition access checks](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/sys_coalition.c#L220)

### What we need to verify on Macs

Run on both Apple Silicon and Intel: two independent scopes, helpers remaining after
root exit, a helper leaving its group, concurrent cleanup, and an external root that
shares a group with unrelated processes. Verify anchor exclusion, PID identity
changes, permission failures, and CPU units. A missing sample must not become zero
or block cleanup. These tests establish the implementation's behavior; the source
research alone does not.

## Make the numbers understandable

Each result must say what was measured, when, and whether it is a current value,
an accumulated counter, or a recorded peak.

| Measurement | Meaning we should expose |
| --- | --- |
| CPU usage | Average cores used between two samples. The first sample has no rate yet. |
| CPU time | Accumulated processing time. State whether exited members are included. |
| Resident memory | Memory currently in RAM for the measured processes. Summed values can count shared pages repeatedly. |
| Container memory | The OS's container accounting. Label it separately from resident memory. |
| Peak memory | A recorded high point for a defined measure. Never present a sum of individual peaks as the tree's simultaneous peak. |
| I/O | Label the source: storage traffic and all read/write transfers are different measurements. |
| Process count | Count processes; label any OS counter that includes threads as a task count instead. |

Missing measurements must not become zero. If a visible process disappears or cannot
be measured, return the usable measurements and identify the affected fields as
partial. Report how many processes contributed to each total. This does not tell us
how many processes were invisible during discovery.

Keep observed totals and container counters separate, even when they describe the
same workload. Their values need not match.

## Using the APIs

```rust,no_run
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let observer = shepherd::process_observer();
let first = observer.tree_usage(12345).await?;
tokio::time::sleep(std::time::Duration::from_millis(250)).await;
let next = observer.refresh_tree_usage(&first).await?;
let totals = next.usage.totals(Some(&first.usage));
println!("CPU cores: {:?}", totals.cpu_cores.value);
println!("Resident bytes: {:?}", totals.resident_bytes.value);
println!("Processes measured: {}", totals.resident_bytes.contributors);
# Ok(())
# }
```

The first snapshot has CPU counters but no CPU rate. `totals(None)` reports CPU rate
as unavailable. Each rate uses that process's elapsed sampling time. New identities,
counter resets, and non-increasing timestamps do not produce a rate. Missing members
stay in `entries` with an error. A failed query does not imply that a process exited.

For a managed scope, call `supervisor.scope_usage(scope_id).await`:

- `ScopeUsage::Sampled` on macOS contains process-group measurements. Compare its
  snapshots with `totals` in the same way. The private anchor is excluded.
- `ScopeUsage::Accounting` on Linux cgroups and Windows Jobs contains native counters.
  Check `source` for their meaning. Linux `member_count` counts tasks, including
  threads; Windows counts processes. Linux I/O is block traffic; Windows includes
  the Job's read/write transfers. Optional unavailable fields remain `None`.
- Linux process-group fallback and custom adapters without accounting return
  `ObservationError::Unsupported`. Use `tree_usage` for observed root usage there.

Native sampling is bounded to four concurrent usage operations across adapters.
Canceling the caller does not release a slot while its native work is still running.
Collection does not hold scope cleanup locks. Cleanup can race a query and make it
fail; no measurement is a cleanup verdict.

Keep a returned snapshot if it is needed later. There is no automatic final sample
or permanent history. Query before cleanup to obtain a final observation when
possible; it is not guaranteed to include every last CPU cycle. Once a container is
removed, its counters cannot be retrieved through this API. Never delay cleanup to
preserve accounting.

## Verification

CI runs native usage and tree tests on Linux, macOS, and Windows. Dedicated Intel
and Apple Silicon jobs check CPU units, memory growth, version fallback, scope
membership, and the live/zombie/reaped query contract. Privileged Linux CI exercises
real cgroup accounting. Unit tests cover partial results, identity changes, duplicate
PIDs, counter resets, and canceled native operations.

For each step, test a root with busy helpers, helpers that exit during sampling,
unavailable measurements, identity changes, and concurrent cleanup. Check that CPU
rates use the elapsed sampling interval and that memory totals include each observed
process once. Run real workloads on Linux, macOS, and Windows; unsupported values
must remain distinguishable from zero.

This proposal covers measurement only. Resource limits, automatic restarts, and
interactive terminals are separate work.
