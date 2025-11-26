mod http_executor;
mod sharded_provider;
mod sqlite_interval;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::error::Result;
use datafusion::prelude::*;
use sharded_provider::{ShardMetadata, ShardedSqliteProvider};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Sharded Time-Series SQLite Query System with HTTP Transport ===\n");

    // Define the schema for our time-series data
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("endpoint", DataType::Utf8, false),
        Field::new("count", DataType::Int64, false),
    ]));

    // Define shards with their HTTP endpoints and time ranges
    let shards = vec![
        ShardMetadata::new(
            "shard_2023_11_25".to_string(),
            "http://localhost:8001/query".to_string(),
            1700870400,  // 2023-11-25 00:00:00
            1700956799,  // 2023-11-25 23:59:59
        ),
        ShardMetadata::new(
            "shard_2023_11_26".to_string(),
            "http://localhost:8002/query".to_string(),
            1700956800,  // 2023-11-26 00:00:00
            1701043199,  // 2023-11-26 23:59:59
        ),
        ShardMetadata::new(
            "shard_2023_11_27".to_string(),
            "http://localhost:8003/query".to_string(),
            1701043200,  // 2023-11-27 00:00:00
            1701129599,  // 2023-11-27 23:59:59
        ),
    ];

    // Create the sharded provider
    let table = ShardedSqliteProvider::new(
        schema.clone(),
        shards,
        "events".to_string(),
        "ts_ns".to_string(),
    );

    // Register with DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("events", Arc::new(table))?;

    println!("📊 System initialized with 3 shards\n");
    println!("--- Example Queries ---\n");

    // Example 1: Simple query with time filter
    println!("🔍 Query 1: SELECT with time filter (should prune shards)\n");
    let query1 = "SELECT service_name, endpoint, count
                  FROM events
                  WHERE ts_ns > 1700900000
                  LIMIT 10";

    println!("SQL: {}\n", query1);

    let df = ctx.sql(query1).await?;
    println!("Logical Plan:");
    println!("{}\n", df.logical_plan().display_indent());

    // Example 2: Aggregate query with GROUP BY
    println!("🔍 Query 2: Aggregate with GROUP BY (pushdown to shards)\n");
    let query2 = "SELECT service_name, SUM(count) as total
                  FROM events
                  WHERE ts_ns > 1700900000
                  GROUP BY service_name";

    println!("SQL: {}\n", query2);

    let df = ctx.sql(query2).await?;
    println!("Logical Plan:");
    println!("{}\n", df.logical_plan().display_indent());

    // Example 3: Query with INTERVAL (will be transformed to SQLite date functions)
    println!("🔍 Query 3: Query with INTERVAL expressions\n");
    println!("Note: DataFusion INTERVAL syntax will be transformed to SQLite date/datetime functions\n");
    println!("Example transformation:");
    println!("  ts_ns + INTERVAL '1 day'  ->  datetime(ts_ns, '+1 days')");
    println!("  ts_ns + INTERVAL '5 mins' ->  datetime(ts_ns, '+5 minutes')\n");

    println!("✅ System ready to accept queries!");
    println!("\n📝 Notes:");
    println!("  - Each HTTP endpoint should accept POST /query with JSON: {{\"sql\": \"SELECT ...\"}}");
    println!("  - Response should be JSON: {{\"rows\": [{{\"col\": val, ...}}, ...]}}");
    println!("  - Shard pruning happens based on time range filters");
    println!("  - Aggregates are pushed down to shards (Map phase)");
    println!("  - DataFusion combines results (Reduce phase)");
    println!("  - INTERVAL expressions are transformed to SQLite-compatible functions");

    Ok(())
}
