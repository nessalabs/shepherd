//! Opaque, supervisor-assigned identifiers.

/// Opaque identifier of a [`ProcessScope`](crate::ProcessScope).
///
/// Assigned by the supervisor; carries no OS meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProcessScopeId(u64);

impl ProcessScopeId {
    /// Creates a scope id from a raw value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl core::fmt::Display for ProcessScopeId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "scope#{}", self.0)
    }
}

/// Opaque handle for a supervised [`Process`](crate::Process).
///
/// Distinct from the OS pid, so a recycled pid can never be mistaken for a live handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProcessId(u64);

impl ProcessId {
    /// Creates a process id from a raw value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl core::fmt::Display for ProcessId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "proc#{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn scope_id_roundtrips_and_displays() {
        let id = ProcessScopeId::new(7);
        assert_eq!(id.get(), 7);
        assert_eq!(id.to_string(), "scope#7");
    }

    #[test]
    fn process_id_roundtrips_and_displays() {
        let id = ProcessId::new(42);
        assert_eq!(id.get(), 42);
        assert_eq!(id.to_string(), "proc#42");
    }

    #[test]
    fn ids_are_ordered_and_hashable() {
        let mut set = BTreeSet::new();
        set.insert(ProcessId::new(3));
        set.insert(ProcessId::new(1));
        set.insert(ProcessId::new(1));
        let ordered: Vec<_> = set.iter().map(|p| p.get()).collect();
        assert_eq!(ordered, vec![1, 3]);
        assert!(ProcessScopeId::new(1) < ProcessScopeId::new(2));
    }
}
