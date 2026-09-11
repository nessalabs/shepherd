//! Core library for the `shepherd` application.
//!
//! The logic lives here so it can be unit-tested independently of the binary
//! entry point.

/// Builds a greeting for the given name.
///
/// An empty or whitespace-only name falls back to a generic greeting so the
/// output is always well-formed.
pub fn greeting(name: &str) -> String {
    let name = name.trim();
    if name.is_empty() {
        "Hello from shepherd!".to_string()
    } else {
        format!("Hello, {name}, from shepherd!")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greets_a_named_user() {
        assert_eq!(greeting("Ada"), "Hello, Ada, from shepherd!");
    }

    #[test]
    fn trims_surrounding_whitespace() {
        assert_eq!(greeting("  Grace  "), "Hello, Grace, from shepherd!");
    }

    #[test]
    fn falls_back_when_name_is_blank() {
        assert_eq!(greeting("   "), "Hello from shepherd!");
    }
}
