# shepherd

A Rust command-line application.

## Requirements

- [Rust](https://www.rust-lang.org/tools/install) (stable toolchain, with `cargo`)

The Cloud Agent development environment (see [`.cursor/environment.json`](.cursor/environment.json))
provisions these automatically.

## Common commands

```sh
cargo build            # compile the crate
cargo run -- <name>    # run the CLI, e.g. `cargo run -- Ada`
cargo test             # run unit and integration tests
cargo fmt --check      # verify formatting
cargo clippy           # lint
```

## Example

```sh
$ cargo run -- Ada
Hello, Ada, from shepherd!

$ cargo run
Hello from shepherd!
```

## Project layout

| Path            | Purpose                                              |
| --------------- | ---------------------------------------------------- |
| `src/lib.rs`    | Core library logic (unit tested).                    |
| `src/main.rs`   | Binary entry point / CLI wiring.                     |
| `tests/cli.rs`  | End-to-end integration tests over the compiled binary. |
