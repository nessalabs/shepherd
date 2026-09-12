# Processes and resource use: a short primer

A **process** is a running instance of a program. Opening an editor starts a
process; the editor may start other processes to check spelling or run a compiler.
The operating system gives each process a number called a **PID**.

## Parents, children, and trees

When one process starts another, we call them parent and child. Children can start
children of their own. Pick a starting process, called the root, to view its tree.

```text
Editor (root, PID 100)
|-- Spell checker (child, PID 101)
`-- Build tool (child, PID 102)
    `-- Compiler (grandchild, PID 103)
```

The root and all its descendants make up this tree. Looking only at the editor's
memory would miss the compiler's memory.

A tree changes while we look at it. Processes start and exit, and children can get
a different parent. PIDs are reused after processes exit, so a PID alone is not a
permanent identity.

## A scope is a different kind of group

Shepherd uses a **scope** to manage programs with a shared lifetime. Finishing the
scope asks Shepherd to clean up its programs and verify the cleanup outcome.

```text
One Shepherd scope
+---------------------------+
| Web server -- helpers     |
| Test runner -- helpers    |
+---------------------------+
```

The server and test runner need not be parent and child. They belong together
because the caller placed them in the same scope.

Linux **cgroups** and Windows **Job Objects** are operating-system containers that
can group processes for control and accounting. Other backends use process groups,
which offer weaker containment. Container membership and family relationships are
not interchangeable.

Viewing a process tree does not require managing it. Shepherd can observe a program
started elsewhere without gaining the right or responsibility to stop it.

## Reading the measurements

- **CPU usage** describes how busy processors were during an interval. One core
  fully busy is 1.0 cores; two fully busy cores are 2.0.
- **CPU time** adds up processing work. Two cores busy for one second contribute
  about two seconds of CPU time.
- **Resident memory** is memory a process currently has in RAM. Some pages may be
  shared, so adding process values can count the same physical memory twice.
- **I/O** measures data read or written. The meaning depends on whether the counter
  includes storage only or also transfers such as pipes.

An **aggregate** combines measurements for a group. The choice of measurement matters:

```text
At 10:00:   Worker A uses 200 MiB, worker B uses  20 MiB -> 220 MiB
At 10:01:   Worker A uses  20 MiB, worker B uses 200 MiB -> 220 MiB

Each worker's individual peak: 200 MiB
Sum of individual peaks:       400 MiB
Largest observed total:        220 MiB
```

Those two peaks answer different questions. Likewise, current CPU usage and total
CPU time are useful, but cannot substitute for each other.

For the macOS approach, see [how scope measurements would work on Macs](../AGGREGATE_STATISTICS.md#how-this-would-work-on-macos).

Return to the [aggregate statistics proposal](../AGGREGATE_STATISTICS.md).
