use deltalakedb_catalog::{schema_ddl, schema_drop_ddl, Dialect};
use sqlx::PgPool;

/// Resets the catalog to a clean state: drops every `dl_*` table in
/// foreign-key-safe order, then re-creates the Postgres schema from
/// `deltalakedb_catalog`. The DDL itself lives in one place — this is just the
/// sqlx execution wrapper for the Postgres-backed integration tests.
pub async fn reset_catalog(pool: &PgPool) -> Result<(), sqlx::Error> {
    for stmt in schema_drop_ddl() {
        sqlx::query(stmt).execute(pool).await?;
    }

    for stmt in schema_ddl(Dialect::Postgres) {
        sqlx::query(stmt).execute(pool).await?;
    }

    Ok(())
}
