//! deltalakedb-sql
//!
//! SQL engine adapters (Postgres, SQLite, DuckDB) behind a consistent interface.

#![warn(missing_docs)]

/// Placeholder module to ensure the crate compiles.
pub mod placeholder {
    /// Placeholder function.
    pub fn hello() -> &'static str {
        "Hello from deltalakedb-sql"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_hello() {
        assert_eq!(placeholder::hello(), "Hello from deltalakedb-sql");
    }
}
