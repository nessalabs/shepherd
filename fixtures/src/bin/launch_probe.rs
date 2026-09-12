//! Asserts the child-side representation of launch arguments, cwd and environment.
fn main() {
    let expected = [
        "",
        "two words",
        "quote\"inside",
        "back\\slash\\",
        "日本語 🐑",
    ];
    assert_eq!(std::env::args().skip(1).collect::<Vec<_>>(), expected);
    assert_eq!(
        std::env::var(shepherd_test_support::probe::VALUE).unwrap(),
        "value with spaces 🐑"
    );
    assert!(std::env::var_os(shepherd_test_support::probe::ABSENT).is_none());
    let expected_dir = std::env::var_os(shepherd_test_support::probe::CWD).unwrap();
    assert_eq!(
        std::env::current_dir().unwrap().canonicalize().unwrap(),
        std::fs::canonicalize(expected_dir).unwrap()
    );
}
