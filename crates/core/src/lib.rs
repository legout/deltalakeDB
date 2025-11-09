//! deltalakedb-core
//!
//! Core domain models and actions for the SQL-backed Delta Lake metadata plane.

#![warn(missing_docs)]

/// Placeholder module to ensure the crate compiles.
pub mod placeholder {
    /// Placeholder function.
    pub fn hello() -> &'static str {
        "Hello from deltalakedb-core"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_hello() {
        assert_eq!(placeholder::hello(), "Hello from deltalakedb-core");
    }
}
