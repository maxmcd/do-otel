use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use std::sync::Arc;

use crate::http_executor::HttpSqliteExecutor;

// ============================================================================
// SHARD METADATA
// ============================================================================

#[derive(Debug, Clone)]
pub struct ShardMetadata {
    pub endpoint_url: String,
    pub start_time: i64,
    pub end_time: i64,
}

impl ShardMetadata {
    pub fn new(endpoint_url: String, start_time: i64, end_time: i64) -> Self {
        Self {
            endpoint_url,
            start_time,
            end_time,
        }
    }

    pub fn overlaps(&self, min_time: Option<i64>, max_time: Option<i64>) -> bool {
        let query_min = min_time.unwrap_or(i64::MIN);
        let query_max = max_time.unwrap_or(i64::MAX);

        self.end_time >= query_min && self.start_time <= query_max
    }
}

// ============================================================================
// TIME RANGE EXTRACTION FROM FILTERS
// ============================================================================

pub fn extract_time_range(filters: &[Expr], time_column: &str) -> (Option<i64>, Option<i64>) {
    let mut min_time = None;
    let mut max_time = None;

    for filter in filters {
        match filter {
            Expr::BinaryExpr(binary) => {
                if binary.op == datafusion::logical_expr::Operator::And {
                    // Handle compound AND expressions by recursively extracting from both sides
                    let (left_min, left_max) =
                        extract_time_range(&[*binary.left.clone()], time_column);
                    let (right_min, right_max) =
                        extract_time_range(&[*binary.right.clone()], time_column);
                    min_time = match (left_min, right_min) {
                        (Some(l), Some(r)) => Some(l.max(r)),
                        (Some(l), None) => Some(l),
                        (None, Some(r)) => Some(r),
                        (None, None) => None,
                    };
                    max_time = match (left_max, right_max) {
                        (Some(l), Some(r)) => Some(l.min(r)),
                        (Some(l), None) => Some(l),
                        (None, Some(r)) => Some(r),
                        (None, None) => None,
                    };
                } else if let Expr::Column(col) = &*binary.left {
                    if col.name == time_column {
                        if let Expr::Literal(scalar, _) = &*binary.right {
                            if let Ok(val) = scalar.to_string().parse::<i64>() {
                                match binary.op {
                                    datafusion::logical_expr::Operator::Gt => {
                                        // Exclusive lower bound: add 1 to make it inclusive
                                        let adjusted = val.saturating_add(1);
                                        min_time = Some(
                                            min_time.map_or(adjusted, |t: i64| t.max(adjusted)),
                                        );
                                    }
                                    datafusion::logical_expr::Operator::GtEq => {
                                        min_time = Some(min_time.map_or(val, |t: i64| t.max(val)));
                                    }
                                    datafusion::logical_expr::Operator::Lt => {
                                        // Exclusive upper bound: subtract 1 to make it inclusive
                                        let adjusted = val.saturating_sub(1);
                                        max_time = Some(
                                            max_time.map_or(adjusted, |t: i64| t.min(adjusted)),
                                        );
                                    }
                                    datafusion::logical_expr::Operator::LtEq => {
                                        max_time = Some(max_time.map_or(val, |t: i64| t.min(val)));
                                    }
                                    datafusion::logical_expr::Operator::Eq => {
                                        min_time = Some(val);
                                        max_time = Some(val);
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    (min_time, max_time)
}

// ============================================================================
// SINGLE SHARD TABLE PROVIDER
// ============================================================================

/// A table provider for a single SQLite shard accessed over HTTP
#[derive(Debug)]
pub struct SingleShardProvider {
    schema: SchemaRef,
    table_name: String,
    executor: Arc<HttpSqliteExecutor>,
}

impl SingleShardProvider {
    pub fn new(schema: SchemaRef, table_name: String, executor: Arc<HttpSqliteExecutor>) -> Self {
        Self {
            schema,
            table_name,
            executor,
        }
    }

    /// Build SQL query from projection and filters
    fn build_sql(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> String {
        // Build SELECT clause
        let select_clause = if let Some(proj) = projection {
            if proj.is_empty() {
                "*".to_string()
            } else {
                proj.iter()
                    .map(|&i| self.schema.field(i).name().clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        } else {
            "*".to_string()
        };

        let mut sql = format!("SELECT {} FROM {}", select_clause, self.table_name);

        // Add WHERE clause from filters
        let where_clauses: Vec<String> = filters.iter().filter_map(Self::expr_to_sql).collect();
        if !where_clauses.is_empty() {
            sql.push_str(&format!(" WHERE {}", where_clauses.join(" AND ")));
        }

        // Add LIMIT
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        sql
    }

    /// Convert DataFusion expression to SQL string
    fn expr_to_sql(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(col) => Some(col.name.clone()),

            Expr::BinaryExpr(binary) => {
                let left = Self::expr_to_sql(&binary.left)?;
                let right = Self::expr_to_sql(&binary.right)?;
                let op = format!("{}", binary.op);
                Some(format!("{} {} {}", left, op, right))
            }

            Expr::Literal(scalar, _) => match scalar {
                datafusion::scalar::ScalarValue::Int64(Some(v)) => Some(v.to_string()),
                datafusion::scalar::ScalarValue::Float64(Some(v)) => Some(v.to_string()),
                datafusion::scalar::ScalarValue::Utf8(Some(s)) => {
                    Some(format!("'{}'", s.replace('\'', "''")))
                }
                datafusion::scalar::ScalarValue::Boolean(Some(b)) => {
                    Some(if *b { "1" } else { "0" }.to_string())
                }
                _ => None,
            },

            _ => None,
        }
    }
}

#[async_trait]
impl TableProvider for SingleShardProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if Self::expr_to_sql(f).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sql = self.build_sql(projection, filters, limit);

        // Determine output schema based on projection
        let output_schema = if let Some(proj) = projection {
            if proj.is_empty() {
                self.schema.clone()
            } else {
                let fields: Vec<_> = proj.iter().map(|&i| self.schema.field(i).clone()).collect();
                Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
            }
        } else {
            self.schema.clone()
        };

        Ok(self.executor.create_exec(sql, output_schema))
    }
}

// ============================================================================
// SHARDED SQLITE TABLE PROVIDER
// ============================================================================

#[derive(Debug)]
pub struct ShardedSqliteProvider {
    schema: SchemaRef,
    shards: Vec<ShardMetadata>,
    table_name: String,
    time_column: String,
}

impl ShardedSqliteProvider {
    pub fn new(
        schema: SchemaRef,
        shards: Vec<ShardMetadata>,
        table_name: String,
        time_column: String,
    ) -> Self {
        Self {
            schema,
            shards,
            table_name,
            time_column,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn shards(&self) -> &[ShardMetadata] {
        &self.shards
    }

    pub fn time_column(&self) -> &str {
        &self.time_column
    }

    fn get_relevant_shards(&self, filters: &[Expr]) -> Vec<ShardMetadata> {
        let (min_time, max_time) = extract_time_range(filters, &self.time_column);

        self.shards
            .iter()
            .filter(|shard| shard.overlaps(min_time, max_time))
            .cloned()
            .collect()
    }
}

#[async_trait]
impl TableProvider for ShardedSqliteProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if SingleShardProvider::expr_to_sql(f).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Prune shards based on time filters
        let relevant_shards = self.get_relevant_shards(filters);

        // Handle empty shard list
        if relevant_shards.is_empty() {
            return Ok(Arc::new(EmptyExec::new(self.schema.clone())));
        }

        // Create execution plans for each shard
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        for shard in relevant_shards {
            let executor = Arc::new(HttpSqliteExecutor::new(shard.endpoint_url.clone()));

            let shard_provider =
                SingleShardProvider::new(self.schema.clone(), self.table_name.clone(), executor);

            let plan = shard_provider
                .scan(state, projection, filters, limit)
                .await?;
            plans.push(plan);
        }

        // If only one shard, return it directly
        if plans.len() == 1 {
            return Ok(plans.into_iter().next().unwrap());
        }

        // Combine with UnionExec
        UnionExec::try_new(plans)
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_executor::HttpSqliteExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::*;

    fn test_shards() -> Vec<ShardMetadata> {
        vec![
            ShardMetadata::new("http://localhost:8001".into(), 1000, 1999),
            ShardMetadata::new("http://localhost:8002".into(), 2000, 2999),
            ShardMetadata::new("http://localhost:8003".into(), 3000, 3999),
        ]
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts_ns", DataType::Int64, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("endpoint", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]))
    }

    fn create_test_context() -> SessionContext {
        let table = ShardedSqliteProvider::new(
            test_schema(),
            test_shards(),
            "events".to_string(),
            "ts_ns".to_string(),
        );
        let ctx = SessionContext::new();
        ctx.register_table("events", Arc::new(table)).unwrap();
        ctx
    }

    /// Extract SQL queries from HttpSqliteExec nodes in a physical plan
    fn extract_shard_sql(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        let mut queries = Vec::new();
        collect_shard_sql(plan, &mut queries);
        queries
    }

    fn collect_shard_sql(plan: &Arc<dyn ExecutionPlan>, queries: &mut Vec<String>) {
        if let Some(http_exec) = plan.as_any().downcast_ref::<HttpSqliteExec>() {
            queries.push(http_exec.sql().to_string());
        }
        for child in plan.children() {
            collect_shard_sql(child, queries);
        }
    }

    // ========================================================================
    // SHARD METADATA TESTS
    // ========================================================================

    #[test]
    fn test_shard_overlaps_full_overlap() {
        let shard = ShardMetadata::new("http://x".into(), 1000, 2000);
        assert!(shard.overlaps(Some(500), Some(2500)));
    }

    #[test]
    fn test_shard_overlaps_partial_start() {
        let shard = ShardMetadata::new("http://x".into(), 1000, 2000);
        assert!(shard.overlaps(Some(1500), Some(2500)));
    }

    #[test]
    fn test_shard_overlaps_partial_end() {
        let shard = ShardMetadata::new("http://x".into(), 1000, 2000);
        assert!(shard.overlaps(Some(500), Some(1500)));
    }

    #[test]
    fn test_shard_overlaps_no_overlap_before() {
        let shard = ShardMetadata::new("http://x".into(), 1000, 2000);
        assert!(!shard.overlaps(Some(100), Some(500)));
    }

    #[test]
    fn test_shard_overlaps_no_overlap_after() {
        let shard = ShardMetadata::new("http://x".into(), 1000, 2000);
        assert!(!shard.overlaps(Some(2500), Some(3000)));
    }

    #[test]
    fn test_shard_overlaps_unbounded() {
        let shard = ShardMetadata::new("http://x".into(), 1000, 2000);
        assert!(shard.overlaps(None, None));
    }

    // ========================================================================
    // SHARD PRUNING TESTS
    // ========================================================================

    #[tokio::test]
    async fn test_no_filter_queries_all_shards() {
        let ctx = create_test_context();
        let df = ctx.sql("SELECT * FROM events").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let queries = extract_shard_sql(&plan);
        assert_eq!(queries.len(), 3, "Should query all 3 shards when no filter");
    }

    #[tokio::test]
    async fn test_filter_prunes_to_single_shard() {
        let ctx = create_test_context();
        // Use range that's clearly within shard_0 (1000-1999)
        let df = ctx
            .sql("SELECT * FROM events WHERE ts_ns >= 1000 AND ts_ns < 1500")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let queries = extract_shard_sql(&plan);
        assert_eq!(queries.len(), 1, "Should query only 1 shard");
    }

    #[tokio::test]
    async fn test_filter_prunes_to_two_shards() {
        let ctx = create_test_context();
        let df = ctx
            .sql("SELECT * FROM events WHERE ts_ns >= 1500 AND ts_ns < 2500")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let queries = extract_shard_sql(&plan);
        assert_eq!(queries.len(), 2, "Should query 2 shards");
    }

    #[tokio::test]
    async fn test_exact_timestamp_routes_to_single_shard() {
        let ctx = create_test_context();
        let df = ctx
            .sql("SELECT * FROM events WHERE ts_ns = 2500")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let queries = extract_shard_sql(&plan);
        assert_eq!(queries.len(), 1, "Exact match should route to 1 shard");
    }

    // ========================================================================
    // FILTER PUSHDOWN TESTS
    // ========================================================================

    #[tokio::test]
    async fn test_where_clause_pushed_to_sql() {
        let ctx = create_test_context();
        // Use range that's clearly within shard_0 (1000-1999)
        let df = ctx
            .sql("SELECT * FROM events WHERE ts_ns >= 1000 AND ts_ns < 1500")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let queries = extract_shard_sql(&plan);
        assert_eq!(queries.len(), 1);
        assert!(queries[0].contains("WHERE"), "SQL should contain WHERE");
        assert!(
            queries[0].contains("ts_ns >= 1000"),
            "SQL should contain filter"
        );
    }

    #[tokio::test]
    async fn test_projection_pushed_to_sql() {
        let ctx = create_test_context();
        let df = ctx
            .sql("SELECT service_name, count FROM events")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let queries = extract_shard_sql(&plan);
        for sql in &queries {
            assert!(
                sql.contains("service_name"),
                "SQL should select service_name"
            );
            assert!(sql.contains("count"), "SQL should select count");
            assert!(!sql.contains("SELECT *"), "SQL should not be SELECT *");
        }
    }

    // ========================================================================
    // AGGREGATE PUSHDOWN TESTS
    // These document expected behavior - some are ignored until implemented
    // ========================================================================

    #[tokio::test]
    async fn test_without_optimizer_aggregate_not_pushed_down() {
        // Without the optimizer rule, aggregates are NOT pushed
        let ctx = create_test_context();
        let df = ctx
            .sql("SELECT service_name, SUM(count) as total FROM events GROUP BY service_name")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let queries = extract_shard_sql(&plan);
        for sql in &queries {
            // Without optimizer, shards just get columns, no aggregation
            assert!(
                !sql.to_uppercase().contains("SUM("),
                "Without optimizer, SUM should NOT be pushed: {}",
                sql
            );
            assert!(
                !sql.to_uppercase().contains("GROUP BY"),
                "Without optimizer, GROUP BY should NOT be pushed: {}",
                sql
            );
        }
    }

    // Tests WITH the optimizer rule are in sharded_aggregation_rule.rs
}
