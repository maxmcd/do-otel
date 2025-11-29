mod http_executor;
mod sharded_aggregation_rule;
mod sharded_provider;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::OptimizerRule;
use datafusion::prelude::*;
use sharded_aggregation_rule::{ShardedAggregationRule, ShardedQueryPlanner};
use sharded_provider::{ShardMetadata, ShardedSqliteProvider};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Sharded Time-Series SQLite Query System ===\n");

    // Define the schema for our time-series data
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("endpoint", DataType::Utf8, false),
        Field::new("status_code", DataType::Int64, false),
    ]));

    let hour_ms = 3600 * 1_000;
    let now_ms = 1700870400;
    // Define shards with their HTTP endpoints and time ranges
    let shards = vec![
        ShardMetadata::new(
            "0".to_string(),
            "http://localhost:8088/shard/0".to_string(),
            now_ms,
            now_ms + 1 * hour_ms,
        ),
        ShardMetadata::new(
            "1".to_string(),
            "http://localhost:8088/shard/1".to_string(),
            now_ms + 1 * hour_ms,
            now_ms + 2 * hour_ms,
        ),
        ShardMetadata::new(
            "2".to_string(),
            "http://localhost:8088/shard/2".to_string(),
            now_ms + 2 * hour_ms,
            now_ms + 3 * hour_ms,
        ),
    ];

    // Create the sharded provider
    let table = ShardedSqliteProvider::new(
        schema.clone(),
        shards,
        "events".to_string(),
        "ts_ms".to_string(),
    );

    // Get default optimizer rules and prepend our sharded aggregation rule
    let default_rules = datafusion::optimizer::Optimizer::new().rules;
    let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
        vec![Arc::new(ShardedAggregationRule::new("events".to_string()))];
    rules.extend(default_rules);

    // Create session with our optimizer rule and custom query planner
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_optimizer_rules(rules)
        .with_query_planner(Arc::new(ShardedQueryPlanner::new()))
        .build();

    let ctx = SessionContext::new_with_state(state);
    ctx.register_table("events", Arc::new(table))?;

    println!("📊 System initialized with 3 shards\n");
    println!("--- Example Queries ---\n");

    // Example 1: Simple query - will hit all shards (no time filter)
    println!("🔍 Query 1: SELECT * (no time filter - all shards)\n");
    let query1 = "SELECT service_name, endpoint, count FROM events LIMIT 10";
    println!("SQL: {}\n", query1);

    let df = ctx.sql(query1).await?;
    let physical_plan = df.create_physical_plan().await?;
    println!("Physical Plan:");
    println!(
        "{}\n",
        datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
    );

    // Example 2: Query with time filter - should prune to 1 shard
    println!("🔍 Query 2: SELECT with time filter (prunes to 1 shard)\n");
    let query2 = "SELECT service_name, endpoint, count 
                  FROM events 
                  WHERE ts_ms >= 1700870400 AND ts_ms < 1700956800
                  LIMIT 10";
    println!("SQL: {}\n", query2);

    let df = ctx.sql(query2).await?;
    let physical_plan = df.create_physical_plan().await?;
    println!("Physical Plan:");
    println!(
        "{}\n",
        datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
    );

    // Example 3: Query spanning 2 shards
    println!("🔍 Query 3: SELECT spanning 2 shards\n");
    let query3 = "SELECT service_name, endpoint, count 
                  FROM events 
                  WHERE ts_ms >= 1700900000 AND ts_ms < 1701000000
                  LIMIT 10";
    println!("SQL: {}\n", query3);

    let df = ctx.sql(query3).await?;
    let physical_plan = df.create_physical_plan().await?;
    println!("Physical Plan:");
    println!(
        "{}\n",
        datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
    );

    // Example 4: Aggregate query (now pushed down to shards!)
    println!("🔍 Query 4: Aggregate (pushed down to shards with final aggregation)\n");
    let query4 = "SELECT service_name, SUM(count) as total
                  FROM events
                  GROUP BY service_name";
    println!("SQL: {}\n", query4);

    let df = ctx.sql(query4).await?;
    let physical_plan = df.create_physical_plan().await?;
    println!("Physical Plan:");
    println!(
        "{}\n",
        datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
    );

    // Example 5: Simple COUNT(*) aggregate
    println!("🔍 Query 5: Simple COUNT(*) aggregate\n");
    let query5 = "SELECT COUNT(*) FROM events";
    println!("SQL: {}\n", query5);

    let df = ctx.sql(query5).await?;
    let physical_plan = df.create_physical_plan().await?;
    println!("Physical Plan:");
    println!(
        "{}\n",
        datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
    );

    // Example 6: Time bucketing with computed GROUP BY
    println!("🔍 Query 6: Time bucketing with computed GROUP BY\n");
    let query6 = r#"SELECT
    (ts_ms / 1000) AS bucket_index,
    MIN(ts_ms) AS bucket_min_ts,
    MAX(ts_ms) AS bucket_max_ts,
    SUM(count) AS total_count
FROM events
GROUP BY bucket_index"#;
    println!("SQL: {}\n", query6);

    let df = ctx.sql(query6).await?;
    let physical_plan = df.create_physical_plan().await?;
    println!("Physical Plan:");
    println!(
        "{}\n",
        datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
    );

    println!("✅ Compilation and planning successful!");
    println!("\n📝 Notes:");
    println!("  - Shard pruning works based on time filters");
    println!("  - Aggregates (COUNT, SUM, MIN, MAX) are pushed down to shards");
    println!("  - Computed GROUP BY expressions (e.g., ts_ms / 1000) are pushed to shards");
    println!("  - Final aggregation combines partial results from shards");
    println!("\n⚠️  To actually execute queries, start HTTP shard servers on ports 8088-8003");

    let df = ctx.sql(query6).await?;
    df.show().await?;
    Ok(())
}
