use arrow_schema::TimeUnit;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableScan};
use datafusion::optimizer::OptimizerConfig;
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion::prelude::*;
use datafusion_sql::unparser::Unparser;
use datafusion_sql::unparser::dialect::SqliteDialect;
use futures::stream;
use rusqlite::Connection;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

// ============================================================================
// SHARD METADATA
// ============================================================================

#[derive(Debug, Clone)]
pub struct ShardMetadata {
    pub path: String,
    pub start_time: i64,
    pub end_time: i64,
}

impl ShardMetadata {
    pub fn new(path: String, start_time: i64, end_time: i64) -> Self {
        Self {
            path,
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
                if let Expr::Column(col) = &*binary.left {
                    if col.name == time_column {
                        if let Expr::Literal(scalar, _) = &*binary.right {
                            if let Ok(val) = scalar.to_string().parse::<i64>() {
                                match binary.op {
                                    datafusion::logical_expr::Operator::Gt
                                    | datafusion::logical_expr::Operator::GtEq => {
                                        min_time = Some(min_time.map_or(val, |t: i64| t.max(val)));
                                    }
                                    datafusion::logical_expr::Operator::Lt
                                    | datafusion::logical_expr::Operator::LtEq => {
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
// SHARDED SQLITE TABLE PROVIDER
// ============================================================================

#[derive(Debug)]
pub struct ShardedSqliteTable {
    schema: SchemaRef,
    shards: Vec<ShardMetadata>,
    table_name: String,
    time_column: String,
}

impl ShardedSqliteTable {
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
}

#[async_trait]
impl TableProvider for ShardedSqliteTable {
    fn as_any(&self) -> &dyn Any {
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
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (min_time, max_time) = extract_time_range(filters, &self.time_column);

        let relevant_shards: Vec<ShardMetadata> = self
            .shards
            .iter()
            .filter(|shard| shard.overlaps(min_time, max_time))
            .cloned()
            .collect();

        println!(
            "Shard pruning: {} of {} shards selected for time range {:?} to {:?}",
            relevant_shards.len(),
            self.shards.len(),
            min_time,
            max_time
        );

        Ok(Arc::new(RemoteSqliteExecutionPlan::new(
            relevant_shards,
            self.schema.clone(),
            self.table_name.clone(),
            filters.to_vec(),
            projection.cloned(),
            limit,
        )))
    }
}

// ============================================================================
// REMOTE SQLITE EXECUTION PLAN
// ============================================================================

pub struct RemoteSqliteExecutionPlan {
    shards: Vec<ShardMetadata>,
    schema: SchemaRef,
    table_name: String,
    filters: Vec<Expr>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl RemoteSqliteExecutionPlan {
    pub fn new(
        shards: Vec<ShardMetadata>,
        schema: SchemaRef,
        table_name: String,
        filters: Vec<Expr>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(shards.len()),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            shards,
            schema,
            table_name,
            filters,
            projection,
            limit,
            properties,
        }
    }

    fn build_sql_query(&self) -> Result<String> {
        let mut query = format!("SELECT ");

        if let Some(proj) = &self.projection {
            let columns: Vec<String> = proj
                .iter()
                .map(|idx| format!("\"{}\"", self.schema.field(*idx).name()))
                .collect();
            query.push_str(&columns.join(", "));
        } else {
            query.push('*');
        }

        query.push_str(&format!(" FROM \"{}\"", self.table_name));

        if !self.filters.is_empty() {
            query.push_str(" WHERE ");
            let filter_strs: Vec<String> = self.filters.iter().map(|f| format!("{}", f)).collect();
            query.push_str(&filter_strs.join(" AND "));
        }

        if let Some(lim) = self.limit {
            query.push_str(&format!(" LIMIT {}", lim));
        }

        Ok(query)
    }
}

impl fmt::Debug for RemoteSqliteExecutionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteSqliteExecutionPlan")
            .field("shards", &self.shards.len())
            .field("table_name", &self.table_name)
            .finish()
    }
}

impl DisplayAs for RemoteSqliteExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RemoteSqliteExec: {} shards, table={}",
            self.shards.len(),
            self.table_name
        )
    }
}

impl ExecutionPlan for RemoteSqliteExecutionPlan {
    fn name(&self) -> &str {
        "RemoteSqliteExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let shard = self.shards[partition].clone();
        let schema = self.schema.clone();
        let sql = self.build_sql_query()?;

        println!("Executing on shard {}: {}", shard.path, sql);

        let stream = stream::once(async move {
            tokio::task::spawn_blocking(move || {
                let conn = Connection::open(&shard.path)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let mut stmt = conn
                    .prepare(&sql)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let mut rows = stmt
                    .query([])
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let mut time_values = Vec::new();
                let mut severity_values = Vec::new();
                let mut count_values = Vec::new();

                while let Some(row) = rows
                    .next()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                {
                    if schema.fields().len() >= 1 {
                        let time: i64 = row
                            .get(0)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        time_values.push(time);
                    }
                    if schema.fields().len() >= 2 {
                        let severity: String = row
                            .get(1)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        severity_values.push(severity);
                    }
                    if schema.fields().len() >= 3 {
                        let count: i64 = row
                            .get(2)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        count_values.push(count);
                    }
                }

                let mut columns: Vec<ArrayRef> = Vec::new();

                if schema.fields().len() >= 1 {
                    columns.push(Arc::new(Int64Array::from(time_values)));
                }
                if schema.fields().len() >= 2 {
                    columns.push(Arc::new(StringArray::from(severity_values)));
                }
                if schema.fields().len() >= 3 {
                    columns.push(Arc::new(Int64Array::from(count_values)));
                }

                Ok(RecordBatch::try_new(schema, columns)?)
            })
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

// ============================================================================
// AGGREGATE PUSHDOWN OPTIMIZER RULE
// ============================================================================

#[derive(Debug)]
pub struct PushdownAggregateRule;

impl PushdownAggregateRule {
    pub fn new() -> Self {
        Self
    }

    fn can_pushdown_aggregate(&self, agg: &datafusion::logical_expr::Aggregate) -> bool {
        for expr in &agg.aggr_expr {
            match expr {
                Expr::AggregateFunction(func) => {
                    let func_name = func.func.name().to_lowercase();
                    if !matches!(func_name.as_str(), "count" | "sum" | "min" | "max" | "avg") {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        true
    }

    fn rewrite_aggregate_plan(
        &self,
        agg: &datafusion::logical_expr::Aggregate,
        scan: &TableScan,
    ) -> Result<Option<LogicalPlan>> {
        if !self.can_pushdown_aggregate(agg) {
            return Ok(None);
        }

        println!(
            "Pushing down aggregate to remote SQLite shards for table: {}",
            scan.table_name
        );

        Ok(None)
    }
}

impl OptimizerRule for PushdownAggregateRule {
    fn name(&self) -> &str {
        "pushdown_aggregate_rule"
    }

    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        Some(datafusion::optimizer::ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<datafusion::common::tree_node::Transformed<LogicalPlan>> {
        use datafusion::common::tree_node::Transformed;

        match &plan {
            LogicalPlan::Aggregate(agg) => {
                if let LogicalPlan::TableScan(scan) = agg.input.as_ref() {
                    if let Some(new_plan) = self.rewrite_aggregate_plan(agg, scan)? {
                        return Ok(Transformed::yes(new_plan));
                    }
                }
            }
            _ => {}
        }

        Ok(Transformed::no(plan))
    }
}

// ============================================================================
// UTILITY: PLAN TO SQL CONVERSION
// ============================================================================

/// Convert a DataFusion logical plan to SQLite SQL
///
/// IMPORTANT: Use the unoptimized logical plan, as DataFusion's optimizer
/// can create projection nodes that generate invalid SQL with subqueries.
pub fn plan_to_sqlite_sql(plan: &LogicalPlan) -> Result<String> {
    let dialect = SqliteDialect {};
    let unparser = Unparser::new(&dialect);
    let ast = unparser.plan_to_sql(plan)?;
    Ok(ast.to_string())
}

/// Helper to generate valid SQL for pushing queries to SQLite shards
/// This uses the unoptimized plan to avoid subquery issues
pub async fn query_to_shard_sql(ctx: &SessionContext, query: &str) -> Result<String> {
    let df = ctx.sql(query).await?;
    // Use unoptimized plan - it generates simpler, valid SQL
    plan_to_sqlite_sql(df.logical_plan())
}

// ============================================================================
// MAIN EXAMPLE
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Sharded Time-Series SQLite Query System ===\n");

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("endpoint", DataType::Utf8, false),
        Field::new("count", DataType::Int64, false),
    ]));

    let shards = vec![
        ShardMetadata::new("shard_2023_11_25.db".to_string(), 1700870400, 1700956799),
        ShardMetadata::new("shard_2023_11_26.db".to_string(), 1700956800, 1701043199),
        ShardMetadata::new("shard_2023_11_27.db".to_string(), 1701043200, 1701129599),
    ];

    let table = ShardedSqliteTable::new(
        schema.clone(),
        shards,
        "events".to_string(),
        "ts_ns".to_string(),
    );

    let ctx = SessionContext::new();
    ctx.register_table("events", Arc::new(table))?;

    // println!("--- Query 1: Simple SELECT with time filter ---");
    // let df = ctx
    //     .sql("SELECT time, severity FROM events WHERE time > 1700900000")
    //     .await?;

    // println!("Logical Plan:");
    // println!("{}", df.logical_plan().display_indent());
    // println!();

    // println!("--- Query 2: Aggregate with GROUP BY ---");
    // let df_agg = ctx
    //     .sql("SELECT severity, COUNT(*) as cnt FROM events WHERE time > 1700900000 GROUP BY severity")
    //     .await?;

    // println!("Logical Plan:");
    // println!("{}", df_agg.logical_plan().display_indent());
    // println!();

    println!("--- Query 3: Demonstrate SQL generation for shard query ---");

    // Try with optimized plan
    // let df = ctx
    //     .sql("SELECT endpoint, COUNT(*) FROM events WHERE ts_ns > 100 GROUP BY endpoint")
    //     .await?;

    let df = ctx
        .sql(
            "
    SELECT 
  date_bin(interval '5 minutes', ts_ns) as time_bucket,
  service_name,
  COUNT(*)
FROM events
WHERE ts_ns > now() - interval '6 hours'
GROUP BY 1, 2
ORDER BY 1 DESC;",
        )
        .await?;

    println!("Unoptimized logical plan:");
    println!("{}\n", df.logical_plan().display_indent());

    let optimized_plan = df.clone().into_optimized_plan()?;
    println!("Optimized logical plan:");
    println!("{}\n", optimized_plan.display_indent());

    // Try generating SQL from both
    let sql_unoptimized = plan_to_sqlite_sql(df.logical_plan())?;
    println!("Generated SQL (unoptimized plan):\n{}\n", sql_unoptimized);

    let sql_optimized = plan_to_sqlite_sql(&optimized_plan)?;
    println!("Generated SQL (optimized plan):\n{}\n", sql_optimized);

    println!("System initialized successfully!");
    println!("\nNote: This is a demonstration. To execute actual queries,");
    println!("you would need to create the SQLite shard databases with data.");

    Ok(())
}
