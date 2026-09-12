# Test environment settings

`shepherd-test-support::TestEnvironment` owns test configuration parsing. Tests
read their settings once at the boundary and pass typed values to their runners.
The crate is unpublished and is only a development dependency of library crates.

| Variable | Default | Accepted values / purpose |
| --- | --- | --- |
| `SHEPHERD_STRESS_RUNTIME` | `both` | `current`, `multi`, or `both`; selects Tokio runtime flavors |
| `SHEPHERD_STRESS_SEED` | `42` | Unsigned 64-bit integer, including zero; reproducible scenario ordering |
| `SHEPHERD_STRESS_ITERATIONS` | `2000` | `6..=100000`; cycles per runtime in the ignored long test |
| `PROPTEST_CASES` | `64` in `properties.rs` | Positive 32-bit integer; generated cases per property |
| `SHEPHERD_CGROUP_ROOT` | Required for privileged tests | Nonempty native path to a writable delegated cgroup v2 ancestor |

Missing optional settings use their defaults. Empty, malformed, non-Unicode
numeric/enum values, and out-of-range values fail with the variable name. Paths
retain the OS representation. Settings unrelated to the selected test are not read.
The smoke stress test always runs 36 cycles per selected runtime, ignoring the long
test's iteration override. Seeds and runtime selection apply to both modes.

`PROPTEST_CASES` is also recognized by proptest itself; other proptest-based test
binaries retain their existing framework defaults. The table's 64-case default
belongs to Shepherd's `properties.rs` suite. Proptest's own replay/persistence
settings remain managed by that framework.

Production cgroup discovery also recognizes `SHEPHERD_CGROUP_ROOT`. Its optional
fallback behavior remains in the infrastructure adapter; it does not depend on
test support. Privileged tests require an explicit root to avoid accidental fallback.

Run a reproducible long test:

```sh
SHEPHERD_STRESS_RUNTIME=multi SHEPHERD_STRESS_SEED=42 SHEPHERD_STRESS_ITERATIONS=2000 \
  cargo test --locked -p shepherd-fixtures --test leak_stress long_create_kill -- --ignored --nocapture
```

The manual stress workflow exposes iterations, seed, and runtime as inputs and
maps them to these same variables. Its Python watchdog has separate command-line
options (`python tools/run-stress.py --help`), not environment settings.

## Child-launch protocol

These names live in `shepherd_test_support::probe`. They are set by the launch
integration test, not user tuning knobs:

| Variable | Purpose |
| --- | --- |
| `SHEPHERD_PROBE_VALUE` | Verify a Unicode value containing spaces arrives unchanged |
| `SHEPHERD_PROBE_CWD` | Expected child working directory, represented as a native path |
| `SHEPHERD_PROBE_ABSENT` | Must be absent in the child; CI sets a parent sentinel to test removal |

The child intentionally reads these directly to verify the actual inherited
environment. Add new test tuning settings to `TestEnvironment`, its parser tests,
and this table together. Parser tests inject a lookup function and never change
the shared process environment.
