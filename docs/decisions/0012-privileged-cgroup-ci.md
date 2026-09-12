# 0012 — Hosted Linux privileged containment tests fail closed

Accepted; closes DESIGN §16 runner decision.

Build fixtures as the hosted runner user, then run the compiled test binary with
sudo inside a dedicated `/sys/fs/cgroup/shepherd-ci` ancestor. Require cgroup v2,
writable membership, and cgroup.kill. Missing prerequisites fail the job. The
ordinary workspace suite lists these privileged tests as ignored; the separate
privileged job always runs them with `--ignored --test-threads=1`.

The tests cover setsid containment, cross-scope isolation, parent-exits-first,
last-owner Drop and adopted-descendant reaping. No privileged result is inferred
from an unprivileged or cross-compilation pass.

Local Linux reproduction (from the repository):

```sh
cargo test -p shepherd-fixtures --test cgroup_integration --no-run
sudo mkdir /sys/fs/cgroup/shepherd-ci
test_bin=$(find target/debug/deps -maxdepth 1 -name 'cgroup_integration-*' -type f -executable | head -n 1)
sudo env SHEPHERD_CGROUP_ROOT=/sys/fs/cgroup/shepherd-ci "$test_bin" --ignored --test-threads=1 --nocapture
sudo rmdir /sys/fs/cgroup/shepherd-ci
```

On hosts which forbid delegation, this command fails. Such hosts correctly use
ProcessGroup in ordinary operation; use a delegated host to prove cgroup behavior.
