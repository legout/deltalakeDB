use std::path::PathBuf;

use thiserror::Error;
use url::Url;

/// Supported DeltaSQL engines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltasqlEngine {
    /// Postgres-backed catalog.
    Postgres,
    /// SQLite-backed catalog.
    Sqlite,
    /// DuckDB-backed catalog.
    Duckdb,
}

/// Parsed DeltaSQL URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltasqlUri {
    /// Target engine config.
    pub engine: DeltasqlEngine,
    /// Engine-specific settings.
    pub config: EngineConfig,
}

/// Engine-specific configuration derived from the URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineConfig {
    /// Postgres database/schema/table triple.
    Postgres {
        /// Database name.
        database: String,
        /// Schema name.
        schema: String,
        /// Table name within the schema.
        table: String,
    },
    /// SQLite file + table name.
    Sqlite {
        /// Path to the SQLite file.
        path: PathBuf,
        /// Table name stored in query params.
        table: String,
    },
    /// DuckDB file + table name.
    Duckdb {
        /// Path to the DuckDB database file.
        path: PathBuf,
        /// Catalog table to access.
        table: String,
    },
}

/// Errors produced when parsing DeltaSQL URIs.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeltasqlUriError {
    /// URI failed basic parsing or used an unexpected scheme.
    #[error("invalid deltasql URI: {0}")]
    Invalid(String),
    /// Required component (engine/database/schema/table) missing.
    #[error("missing required component: {0}")]
    MissingComponent(&'static str),
    /// Engine not recognized.
    #[error("unsupported deltasql engine '{0}'")]
    UnsupportedEngine(String),
}

impl DeltasqlUri {
    /// Parses the provided string into a `DeltasqlUri`.
    pub fn parse(input: &str) -> Result<Self, DeltasqlUriError> {
        let url = Url::parse(input).map_err(|err| DeltasqlUriError::Invalid(err.to_string()))?;
        if url.scheme() != "deltasql" {
            return Err(DeltasqlUriError::Invalid(
                "URI must start with deltasql://".into(),
            ));
        }
        let engine = url
            .host_str()
            .ok_or(DeltasqlUriError::MissingComponent("engine"))?;
        let engine = match engine {
            "postgres" => DeltasqlEngine::Postgres,
            "sqlite" => DeltasqlEngine::Sqlite,
            "duckdb" => DeltasqlEngine::Duckdb,
            other => return Err(DeltasqlUriError::UnsupportedEngine(other.into())),
        };

        let config = match engine {
            DeltasqlEngine::Postgres => parse_postgres(&url)?,
            DeltasqlEngine::Sqlite => parse_sqlite_like(&url, DeltasqlEngine::Sqlite)?,
            DeltasqlEngine::Duckdb => parse_sqlite_like(&url, DeltasqlEngine::Duckdb)?,
        };

        Ok(Self { engine, config })
    }
}

fn parse_postgres(url: &Url) -> Result<EngineConfig, DeltasqlUriError> {
    let mut segments = url
        .path_segments()
        .map(|segments| segments.filter(|s| !s.is_empty()).collect::<Vec<_>>())
        .unwrap_or_default();
    if segments.len() != 3 {
        return Err(DeltasqlUriError::Invalid(
            "postgres URI must look like deltasql://postgres/<db>/<schema>/<table>".into(),
        ));
    }
    let table = segments.pop().unwrap().to_string();
    let schema = segments.pop().unwrap().to_string();
    let database = segments.pop().unwrap().to_string();
    Ok(EngineConfig::Postgres {
        database,
        schema,
        table,
    })
}

fn parse_sqlite_like(url: &Url, engine: DeltasqlEngine) -> Result<EngineConfig, DeltasqlUriError> {
    let table = url
        .query_pairs()
        .find_map(|(key, value)| (key == "table").then(|| value.to_string()))
        .ok_or(DeltasqlUriError::MissingComponent("table query parameter"))?;
    let raw_path = url.path();
    if raw_path.is_empty() {
        return Err(DeltasqlUriError::MissingComponent("database path"));
    }
    // Trim single leading slash to support absolute paths encoded as `//path`.
    let normalized = if raw_path.starts_with('/') {
        raw_path.trim_start_matches('/')
    } else {
        raw_path
    };
    let mut path = PathBuf::new();
    path.push(format!("/{}", normalized));
    match engine {
        DeltasqlEngine::Sqlite => Ok(EngineConfig::Sqlite { path, table }),
        DeltasqlEngine::Duckdb => Ok(EngineConfig::Duckdb { path, table }),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_postgres_uri() {
        let uri = DeltasqlUri::parse("deltasql://postgres/mydb/public/mytable").unwrap();
        assert_eq!(uri.engine, DeltasqlEngine::Postgres);
        match uri.config {
            EngineConfig::Postgres {
                database,
                schema,
                table,
            } => {
                assert_eq!(database, "mydb");
                assert_eq!(schema, "public");
                assert_eq!(table, "mytable");
            }
            _ => panic!("unexpected config"),
        }
    }

    #[test]
    fn parses_sqlite_uri() {
        let uri = DeltasqlUri::parse("deltasql://sqlite//var/data/meta.db?table=mytable").unwrap();
        assert_eq!(uri.engine, DeltasqlEngine::Sqlite);
        match uri.config {
            EngineConfig::Sqlite { path, table } => {
                assert_eq!(path, PathBuf::from("/var/data/meta.db"));
                assert_eq!(table, "mytable");
            }
            _ => panic!("unexpected config"),
        }
    }

    #[test]
    fn parses_duckdb_uri() {
        let uri = DeltasqlUri::parse("deltasql://duckdb//tmp/catalog.duckdb?table=tbl").unwrap();
        assert_eq!(uri.engine, DeltasqlEngine::Duckdb);
        match uri.config {
            EngineConfig::Duckdb { path, table } => {
                assert_eq!(path, PathBuf::from("/tmp/catalog.duckdb"));
                assert_eq!(table, "tbl");
            }
            _ => panic!("unexpected config"),
        }
    }

    #[test]
    fn rejects_missing_table() {
        let err = DeltasqlUri::parse("deltasql://sqlite//tmp/db").unwrap_err();
        assert_eq!(
            err,
            DeltasqlUriError::MissingComponent("table query parameter")
        );
    }

    #[test]
    fn rejects_unknown_engine() {
        let err = DeltasqlUri::parse("deltasql://oracle/foo").unwrap_err();
        assert_eq!(err, DeltasqlUriError::UnsupportedEngine("oracle".into()));
    }
}
