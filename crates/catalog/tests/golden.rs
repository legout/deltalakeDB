//! Golden snapshots: the SQL text and bind order of every catalog query, per
//! dialect, plus the schema DDL. These pin the dialect renderings against
//! future drift — a snapshot review shows exactly what changed.

use deltalakedb_catalog::{
    active_files_query, latest_metadata_query, latest_protocol_query, schema_ddl,
    schema_drop_ddl, table_names, CatalogParam, Dialect,
};
use insta::assert_snapshot;

const DIALECTS: [Dialect; 3] = [Dialect::Postgres, Dialect::Sqlite, Dialect::DuckDb];

fn params_string(params: &[CatalogParam]) -> String {
    params
        .iter()
        .map(|p| match p {
            CatalogParam::TableId => "TableId",
            CatalogParam::Version => "Version",
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[test]
fn golden_protocol_query() {
    for d in DIALECTS {
        let q = latest_protocol_query(d);
        assert_snapshot!(
            format!("protocol_{d:?}"),
            format!("-- params: [{}]\n{}", params_string(q.params), q.sql)
        );
    }
}

#[test]
fn golden_metadata_query() {
    for d in DIALECTS {
        let q = latest_metadata_query(d);
        assert_snapshot!(
            format!("metadata_{d:?}"),
            format!("-- params: [{}]\n{}", params_string(q.params), q.sql)
        );
    }
}

#[test]
fn golden_active_files_query() {
    for d in DIALECTS {
        let q = active_files_query(d);
        assert_snapshot!(
            format!("active_files_{d:?}"),
            format!("-- params: [{}]\n{}", params_string(q.params), q.sql)
        );
    }
}

#[test]
fn golden_schema_ddl() {
    for d in DIALECTS {
        assert_snapshot!(
            format!("schema_ddl_{d:?}"),
            schema_ddl(d).join("\n")
        );
    }
}

#[test]
fn golden_schema_drop() {
    assert_snapshot!("schema_drop", schema_drop_ddl().join("\n"));
}

#[test]
fn golden_table_names() {
    assert_snapshot!("table_names", table_names().join("\n"));
}

// ---------------------------------------------------------------------------
// Structural invariants — the properties that actually bite when they drift
// ---------------------------------------------------------------------------

#[test]
fn active_files_bind_cardinality_matches_dialect() {
    // Postgres: 2 named params ($1/$2), each referenced twice.
    assert_eq!(
        active_files_query(Dialect::Postgres).params,
        &[CatalogParam::TableId, CatalogParam::Version]
    );
    // SQLite / DuckDB: 4 positional params (one pair per table reference).
    assert_eq!(
        active_files_query(Dialect::Sqlite).params,
        &[
            CatalogParam::TableId,
            CatalogParam::Version,
            CatalogParam::TableId,
            CatalogParam::Version,
        ]
    );
    assert_eq!(
        active_files_query(Dialect::DuckDb).params,
        &[
            CatalogParam::TableId,
            CatalogParam::Version,
            CatalogParam::TableId,
            CatalogParam::Version,
        ]
    );
}

#[test]
fn postgres_uses_named_placeholders_positional_use_question_mark() {
    for q in [
        latest_protocol_query(Dialect::Postgres),
        latest_metadata_query(Dialect::Postgres),
        active_files_query(Dialect::Postgres),
    ] {
        assert!(q.sql.contains("$1"), "postgres sql must contain $1");
        assert!(!q.sql.contains(" ? "), "postgres sql must not use ? placeholders");
    }
    for d in [Dialect::Sqlite, Dialect::DuckDb] {
        assert!(active_files_query(d).sql.contains('?'));
        assert!(latest_protocol_query(d).sql.contains('?'));
        assert!(latest_metadata_query(d).sql.contains('?'));
    }
}

#[test]
fn every_query_uses_only_table_id_and_version() {
    for d in DIALECTS {
        for q in [
            latest_protocol_query(d),
            latest_metadata_query(d),
            active_files_query(d),
        ] {
            for p in q.params {
                assert!(matches!(p, CatalogParam::TableId | CatalogParam::Version));
            }
        }
    }
}

#[test]
fn table_names_matches_drop_order_set() {
    // Same set of tables, drop order is the reverse-dependency refinement.
    let mut names: Vec<_> = table_names().to_vec();
    names.sort();
    let mut drops: Vec<_> = schema_drop_ddl()
        .iter()
        .map(|s| s.trim_start_matches("DROP TABLE IF EXISTS ").to_string())
        .collect::<Vec<_>>();
    drops.sort();
    assert_eq!(names, drops);
}
