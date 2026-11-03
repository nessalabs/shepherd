# Executed implementation checks

Source: `e56c5e583793ad7d69b4dd4738857909ca8007ad`. Run date: 2026-11-03.
Host: macOS arm64. Rust: isolated official Rust 1.99.1 toolchain.

| Command | Result | Log |
| --- | --- | --- |
| `cargo test -p shepherd-domain -p shepherd-app --lib` | 29 application + 52 domain tests passed; 0 failures | [output](shepherd-rust-tests.log) |
| `cargo test -p shepherd-infra -p shepherd --lib` | 41 infrastructure + 1 facade tests passed; 0 failures | [output](shepherd-adapter-tests.log) |
| `cargo check -p shepherd --example background_task_cleanup` | Passed; example was not executed | [output](shepherd-example-check.log) |
| `python3 tools/check_unsafe.py` | Unsafe inventory matches: 57 sites in 10 files | Exact one-line result recorded here |

The first attempt using a partial preexisting Cargo installation failed to locate
`rustc` before compilation. After installing an isolated verified official toolchain,
the successful commands above ran. No global toolchain was changed.

These are unit suites, including native probes selected on this macOS host. They
do not replace the fixture integration suites, privileged Linux cgroup checks,
Windows Job/handle checks, or stress/leak/heap/release qualification. Those were
not run for this documentation change. TLA+ execution is separately recorded in
[results.json](results.json) and does not execute Rust code.
