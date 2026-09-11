use std::process::Command;

/// Exercises the compiled binary end to end: build via Cargo, run it with an
/// argument, and assert on stdout.
#[test]
fn runs_and_greets_the_argument() {
    let output = Command::new(env!("CARGO_BIN_EXE_shepherd"))
        .arg("world")
        .output()
        .expect("failed to run the shepherd binary");

    assert!(output.status.success(), "binary exited with failure");

    let stdout = String::from_utf8(output.stdout).expect("stdout was not valid UTF-8");
    assert_eq!(stdout.trim_end(), "Hello, world, from shepherd!");
}

#[test]
fn runs_without_arguments() {
    let output = Command::new(env!("CARGO_BIN_EXE_shepherd"))
        .output()
        .expect("failed to run the shepherd binary");

    assert!(output.status.success(), "binary exited with failure");

    let stdout = String::from_utf8(output.stdout).expect("stdout was not valid UTF-8");
    assert_eq!(stdout.trim_end(), "Hello from shepherd!");
}
