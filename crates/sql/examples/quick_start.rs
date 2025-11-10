//! Quick Start Example for deltalakedb-sql
//!
//! This example demonstrates the basic usage of SQL adapters
//! for Delta Lake metadata storage.

use deltalakedb_sql::{AdapterFactory, DatabaseConfig};
use deltalakedb_core::Table;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🚀 DeltaLake SQL Adapters Quick Start");
    println!("=====================================");

    // Example 1: In-memory SQLite
    println!("\n1. Creating SQLite in-memory adapter...");
    let sqlite_config = DatabaseConfig {
        url: "sqlite::memory:".to_string(),
        pool_size: 1,
        timeout: 30,
        ssl_enabled: false,
    };

    let sqlite_adapter = AdapterFactory::create_adapter(sqlite_config).await?;
    println!("✅ SQLite adapter created successfully");
    println!("   Database type: {}", sqlite_adapter.database_type());

    // Example 2: PostgreSQL (if available)
    if let Ok(pg_url) = std::env::var("POSTGRES_URL") {
        println!("\n2. Creating PostgreSQL adapter...");
        let pg_config = DatabaseConfig {
            url: pg_url.clone(),
            pool_size: 10,
            timeout: 30,
            ssl_enabled: true,
        };

        match AdapterFactory::create_adapter(pg_config).await {
            Ok(pg_adapter) => {
                println!("✅ PostgreSQL adapter created successfully");
                println!("   Database type: {}", pg_adapter.database_type());

                // Test PostgreSQL connection
                let pg_healthy = pg_adapter.health_check().await?;
                println!("   PostgreSQL health: {}", pg_healthy);
            }
            Err(e) => {
                println!("❌ PostgreSQL adapter creation failed: {}", e);
            }
        }
    }

    // Example 3: DuckDB in-memory
    println!("\n3. Creating DuckDB in-memory adapter...");
    let duckdb_config = DatabaseConfig {
        url: "duckdb://memory".to_string(),
        pool_size: 5,
        timeout: 30,
        ssl_enabled: false,
    };

    let duckdb_adapter = AdapterFactory::create_adapter(duckdb_config).await?;
    println!("✅ DuckDB adapter created successfully");
    println!("   Database type: {}", duckdb_adapter.database_type());

    // Example 4: Basic table operations
    println!("\n4. Basic table operations...");

    let table = Table {
        id: Uuid::new_v4(),
        table_path: "/demo/delta_table".to_string(),
        table_name: "demo_table".to_string(),
        table_uuid: Uuid::new_v4(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    // Create table with SQLite adapter
    let created_table = sqlite_adapter.create_table(&table).await?;
    println!("✅ Table created: {}", created_table.table_name);

    // Read table back
    let read_table = sqlite_adapter.read_table(created_table.id).await?;
    println!("✅ Table read: {:?}", read_table.map(|t| t.table_name));

    // List all tables
    let tables = sqlite_adapter.list_tables(None, None).await?;
    println!("✅ Total tables: {}", tables.len());

    // Example 5: Connection pool statistics
    println!("\n5. Connection pool statistics...");
    let stats = sqlite_adapter.pool_stats().await?;
    println!("   Total connections: {}", stats.total_connections);
    println!("   Active connections: {}", stats.active_connections);
    println!("   Idle connections: {}", stats.idle_connections);
    println!("   Utilization: {:.1}%", stats.utilization_percent);

    // Example 6: Database information
    println!("\n6. Database information...");
    let version = sqlite_adapter.database_version().await?;
    println!("   Database version: {}", version);

    let hints = sqlite_adapter.get_optimization_hints().await?;
    println!("   Optimization hints:");
    for (i, hint) in hints.iter().enumerate() {
        println!("     {}. {}", i + 1, hint);
    }

    // Example 7: Health checks
    println!("\n7. Health checks...");
    let healthy = sqlite_adapter.health_check().await?;
    println!("   Health check: {}", if healthy { "✅ Healthy" } else { "❌ Unhealthy" });

    let can_connect = sqlite_adapter.test_connection().await?;
    println!("   Connection test: {}", if can_connect { "✅ Connected" } else { "❌ Failed" });

    println!("\n🎉 Quick start completed successfully!");
    println!("=====================================");
    println!("Next steps:");
    println!("- Explore the full API documentation");
    println!("- Check out the integration tests");
    println!("- Configure your production database");
    println!("- Start building Delta Lake applications!");

    Ok(())
}