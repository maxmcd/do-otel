//! Optimizer rule that rewrites aggregations over sharded tables into
//! two-phase aggregations (partial per-shard + final combine).

use crate::http_executor::HttpSqliteExecutor;
use crate::sharded_provider::ShardedSqliteProvider;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::Result as DataFusionResult;
use datafusion::common::tree_node::Transformed;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::AggregateUDF;
use datafusion::logical_expr::{Aggregate, Expr, Extension, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use std::sync::Arc;

/// Optimizer rule that pushes aggregations down to shards.
///
/// Transforms:
/// ```text
/// Aggregate(group_by=[col], aggr=[COUNT(*)])
///   └── TableScan(sharded_table)
/// ```
///
/// Into:
/// ```text
/// FinalAggregate(group_by=[col], aggr=[SUM(partial_count)])
///   └── ShardedAggregateNode(group_by=[col], aggr=[COUNT(*) AS partial_count])
/// ```
///
/// The ShardedAggregateNode is a custom logical node that, when converted to
/// physical plan, creates per-shard HttpSqliteExec nodes with aggregate SQL.
#[derive(Debug)]
pub struct ShardedAggregationRule {
    /// Name of the sharded table to match
    table_name: String,
}

impl ShardedAggregationRule {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }

    /// Check if a logical plan is a TableScan of our sharded table
    fn is_sharded_table_scan(&self, plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::TableScan(scan) => scan.table_name.table() == self.table_name,
            _ => false,
        }
    }

    /// Check if all group expressions can be pushed to SQLite
    /// Supports: columns, literals, and simple arithmetic (col op literal)
    fn has_pushable_group_by(&self, group_expr: &[Expr]) -> bool {
        group_expr.iter().all(|e| is_pushable_expr(e))
    }

    /// Check if an aggregate function is supported for pushdown
    fn is_supported_aggregate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::AggregateFunction(agg) => {
                let name = agg.func.name();
                matches!(name, "count" | "sum" | "min" | "max")
            }
            Expr::Alias(alias) => self.is_supported_aggregate(&alias.expr),
            _ => false,
        }
    }

    /// Check if all aggregates in the plan are supported
    fn all_aggregates_supported(&self, aggr_expr: &[Expr]) -> bool {
        aggr_expr.iter().all(|e| self.is_supported_aggregate(e))
    }

    /// Try to rewrite an Aggregate node for pushdown
    fn try_rewrite_aggregate(
        &self,
        aggregate: &Aggregate,
        input: &LogicalPlan,
        filters: Vec<Expr>,
    ) -> Option<LogicalPlan> {
        // Only handle pushable GROUP BY expressions
        if !self.has_pushable_group_by(&aggregate.group_expr) {
            return None;
        }

        // Only handle supported aggregate functions
        if !self.all_aggregates_supported(&aggregate.aggr_expr) {
            return None;
        }

        // Create the custom ShardedAggregate node
        let sharded_agg = ShardedAggregate {
            group_expr: aggregate.group_expr.clone(),
            aggr_expr: aggregate.aggr_expr.clone(),
            input: input.clone(),
            filters,
            schema: aggregate.schema.clone(),
        };

        Some(LogicalPlan::Extension(Extension {
            node: std::sync::Arc::new(sharded_agg),
        }))
    }
}

impl OptimizerRule for ShardedAggregationRule {
    fn name(&self) -> &str {
        "sharded_aggregation_pushdown"
    }

    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        // Apply bottom-up so we catch aggregates before other rules transform them
        Some(datafusion::optimizer::ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        // Match: Aggregate -> (Filter)* -> TableScan(sharded_table)
        if let LogicalPlan::Aggregate(aggregate) = &plan {
            let mut current = aggregate.input.as_ref();
            let mut filters = Vec::new();

            // Traverse through Filter nodes
            loop {
                match current {
                    LogicalPlan::Filter(filter) => {
                        filters.push(filter.predicate.clone());
                        current = filter.input.as_ref();
                    }
                    LogicalPlan::Projection(proj) => {
                        current = proj.input.as_ref();
                    }
                    _ => break,
                }
            }

            // Check if we reached our sharded table
            if self.is_sharded_table_scan(current) {
                if let Some(rewritten) = self.try_rewrite_aggregate(aggregate, current, filters) {
                    return Ok(Transformed::yes(rewritten));
                }
            }
        }

        Ok(Transformed::no(plan))
    }
}

// ============================================================================
// SHARDED AGGREGATE NODE
// ============================================================================

use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::UserDefinedLogicalNodeCore;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Custom logical node representing an aggregate that should be pushed to shards.
///
/// When this node is converted to a physical plan, it will:
/// 1. Create per-shard partial aggregates (with SQL containing GROUP BY/COUNT/etc)
/// 2. Union the partial results
/// 3. Apply a final aggregate to combine partials
#[derive(Debug, Clone)]
pub struct ShardedAggregate {
    /// GROUP BY expressions (must be simple columns)
    pub group_expr: Vec<Expr>,
    /// Aggregate expressions (COUNT, SUM, etc)
    pub aggr_expr: Vec<Expr>,
    /// The underlying table scan
    pub input: LogicalPlan,
    /// Filter predicates to apply
    pub filters: Vec<Expr>,
    /// Output schema
    pub schema: DFSchemaRef,
}

impl Hash for ShardedAggregate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.group_expr.hash(state);
        self.aggr_expr.hash(state);
        self.filters.hash(state);
    }
}

impl PartialEq for ShardedAggregate {
    fn eq(&self, other: &Self) -> bool {
        self.group_expr == other.group_expr
            && self.aggr_expr == other.aggr_expr
            && self.filters == other.filters
    }
}

impl Eq for ShardedAggregate {}

impl PartialOrd for ShardedAggregate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Just use equality for ordering - we don't need real ordering
        if self == other {
            Some(std::cmp::Ordering::Equal)
        } else {
            None
        }
    }
}

impl UserDefinedLogicalNodeCore for ShardedAggregate {
    fn name(&self) -> &str {
        "ShardedAggregate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = self.group_expr.clone();
        exprs.extend(self.aggr_expr.clone());
        exprs.extend(self.filters.clone());
        exprs
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ShardedAggregate: group_by=[{}], aggr=[{}]",
            self.group_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            self.aggr_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        let group_count = self.group_expr.len();
        let aggr_count = self.aggr_expr.len();

        Ok(Self {
            group_expr: exprs[..group_count].to_vec(),
            aggr_expr: exprs[group_count..group_count + aggr_count].to_vec(),
            filters: exprs[group_count + aggr_count..].to_vec(),
            input: inputs.into_iter().next().unwrap(),
            schema: self.schema.clone(),
        })
    }
}

// ============================================================================
// PHYSICAL PLANNER FOR SHARDED AGGREGATE
// ============================================================================

/// Physical planner that handles ShardedAggregate nodes
pub struct ShardedAggregatePlanner;

#[async_trait]
impl ExtensionPlanner for ShardedAggregatePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn datafusion::logical_expr::UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        // Check if this is our ShardedAggregate node
        let Some(sharded_agg) = node.as_any().downcast_ref::<ShardedAggregate>() else {
            return Ok(None);
        };

        // Get the table provider to access shard information
        let LogicalPlan::TableScan(scan) = &sharded_agg.input else {
            return Err(datafusion::error::DataFusionError::Plan(
                "ShardedAggregate input must be a TableScan".to_string(),
            ));
        };

        // Get the ShardedSqliteProvider from the catalog
        let table_ref = &scan.table_name;
        let table = session_state
            .catalog_list()
            .catalog(table_ref.catalog().unwrap_or("datafusion"))
            .ok_or_else(|| datafusion::error::DataFusionError::Plan("Catalog not found".into()))?
            .schema(table_ref.schema().unwrap_or("public"))
            .ok_or_else(|| datafusion::error::DataFusionError::Plan("Schema not found".into()))?
            .table(table_ref.table())
            .await?
            .ok_or_else(|| datafusion::error::DataFusionError::Plan("Table not found".into()))?;

        let sharded_provider = table
            .as_any()
            .downcast_ref::<ShardedSqliteProvider>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(
                    "Expected ShardedSqliteProvider".to_string(),
                )
            })?;

        // Build the SQL for each shard
        let sql = build_aggregate_sql(
            sharded_provider.table_name(),
            &sharded_agg.group_expr,
            &sharded_agg.aggr_expr,
            &sharded_agg.filters,
            &sharded_agg.schema,
        );

        // Build output schema for the shard queries (must match logical schema)
        let output_schema = build_output_schema(&sharded_agg.schema)?;

        // Prune shards based on time filters using the provider's pruning logic
        use crate::sharded_provider::extract_time_range;
        let (min_time, max_time) =
            extract_time_range(&sharded_agg.filters, sharded_provider.time_column());

        let relevant_shards: Vec<_> = sharded_provider
            .shards()
            .iter()
            .filter(|shard| shard.overlaps(min_time, max_time))
            .collect();

        // Handle empty shard list
        if relevant_shards.is_empty() {
            return Ok(Some(Arc::new(
                datafusion::physical_plan::empty::EmptyExec::new(output_schema),
            )));
        }

        // Create HttpSqliteExec for each relevant shard
        let mut shard_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        for shard in relevant_shards {
            let executor = Arc::new(HttpSqliteExecutor::new(shard.endpoint_url.clone()));
            let exec = executor.create_exec(sql.clone(), output_schema.clone());
            shard_plans.push(exec);
        }

        // If only one shard, return it directly (no need for union or final aggregation)
        if shard_plans.len() == 1 {
            return Ok(Some(shard_plans.into_iter().next().unwrap()));
        }

        // Union all shard results
        let union_exec: Arc<dyn ExecutionPlan> = UnionExec::try_new(shard_plans)?;

        // Build final aggregation to combine partial results
        let final_exec = build_final_aggregate(
            union_exec,
            &sharded_agg.group_expr,
            &sharded_agg.aggr_expr,
            output_schema.clone(),
        )?;

        Ok(Some(final_exec))
    }
}

/// Build SQL with GROUP BY and aggregates for a shard
fn build_aggregate_sql(
    table_name: &str,
    group_expr: &[Expr],
    aggr_expr: &[Expr],
    filters: &[Expr],
    schema: &DFSchemaRef,
) -> String {
    let mut select_parts = Vec::new();
    let mut group_by_parts = Vec::new();

    // Add GROUP BY expressions to SELECT and GROUP BY clauses
    for (idx, expr) in group_expr.iter().enumerate() {
        let output_name = schema.field(idx).name();
        let quoted_name = format!("\"{}\"", output_name);

        if let Some(expr_sql) = expr_to_sql(expr) {
            // SELECT expr AS "output_name"
            select_parts.push(format!("{} AS {}", expr_sql, quoted_name));
            // GROUP BY uses the expression directly
            group_by_parts.push(expr_sql);
        }
    }

    let group_count = group_expr.len();

    // Add aggregate expressions with proper output names from schema
    for (idx, expr) in aggr_expr.iter().enumerate() {
        let output_name = schema.field(group_count + idx).name();
        let agg_sql = expr_to_aggregate_sql(expr, output_name);
        select_parts.push(agg_sql);
    }

    let mut sql = format!("SELECT {} FROM {}", select_parts.join(", "), table_name);

    // Add WHERE clause
    if !filters.is_empty() {
        let filter_strs: Vec<String> = filters.iter().filter_map(expr_to_filter_sql).collect();
        if !filter_strs.is_empty() {
            sql.push_str(&format!(" WHERE {}", filter_strs.join(" AND ")));
        }
    }

    // Add GROUP BY clause
    if !group_by_parts.is_empty() {
        sql.push_str(&format!(" GROUP BY {}", group_by_parts.join(", ")));
    }

    sql
}

/// Convert an aggregate expression to SQL with the given output name
fn expr_to_aggregate_sql(expr: &Expr, output_name: &str) -> String {
    // Quote the output name to handle special characters
    let quoted_name = format!("\"{}\"", output_name);

    match expr {
        Expr::AggregateFunction(agg) => {
            let func_name = agg.func.name().to_uppercase();
            match func_name.as_str() {
                "COUNT" => format!("COUNT(*) AS {}", quoted_name),
                "SUM" => {
                    if let Some(Expr::Column(col)) = agg.params.args.first() {
                        format!("SUM({}) AS {}", col.name, quoted_name)
                    } else {
                        format!("SUM(*) AS {}", quoted_name)
                    }
                }
                "MIN" => {
                    if let Some(Expr::Column(col)) = agg.params.args.first() {
                        format!("MIN({}) AS {}", col.name, quoted_name)
                    } else {
                        format!("MIN(*) AS {}", quoted_name)
                    }
                }
                "MAX" => {
                    if let Some(Expr::Column(col)) = agg.params.args.first() {
                        format!("MAX({}) AS {}", col.name, quoted_name)
                    } else {
                        format!("MAX(*) AS {}", quoted_name)
                    }
                }
                _ => format!("{}(*) AS {}", func_name, quoted_name),
            }
        }
        Expr::Alias(alias) => expr_to_aggregate_sql(&alias.expr, output_name),
        _ => format!("NULL AS {}", quoted_name),
    }
}

/// Convert a filter expression to SQL
fn expr_to_filter_sql(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        Expr::BinaryExpr(binary) => {
            let left = expr_to_filter_sql(&binary.left)?;
            let right = expr_to_filter_sql(&binary.right)?;
            Some(format!("{} {} {}", left, binary.op, right))
        }
        Expr::Literal(scalar, _) => match scalar {
            datafusion::scalar::ScalarValue::Int64(Some(v)) => Some(v.to_string()),
            datafusion::scalar::ScalarValue::Float64(Some(v)) => Some(v.to_string()),
            datafusion::scalar::ScalarValue::Utf8(Some(s)) => {
                Some(format!("'{}'", s.replace('\'', "''")))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Check if an expression can be pushed down to SQLite
/// Supports: columns, literals, aliases, and simple arithmetic expressions
fn is_pushable_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Literal(_, _) => true,
        Expr::Alias(alias) => is_pushable_expr(&alias.expr),
        Expr::BinaryExpr(binary) => {
            // Only allow arithmetic operations
            use datafusion::logical_expr::Operator;
            matches!(
                binary.op,
                Operator::Plus
                    | Operator::Minus
                    | Operator::Multiply
                    | Operator::Divide
                    | Operator::Modulo
            ) && is_pushable_expr(&binary.left)
                && is_pushable_expr(&binary.right)
        }
        Expr::Cast(cast) => {
            // Allow CAST of pushable expressions to simple types
            is_pushable_expr(&cast.expr)
        }
        _ => false,
    }
}

/// Convert a pushable expression to SQL string
fn expr_to_sql(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        Expr::Literal(scalar, _) => match scalar {
            datafusion::scalar::ScalarValue::Int64(Some(v)) => Some(v.to_string()),
            datafusion::scalar::ScalarValue::Float64(Some(v)) => Some(v.to_string()),
            datafusion::scalar::ScalarValue::Utf8(Some(s)) => {
                Some(format!("'{}'", s.replace('\'', "''")))
            }
            datafusion::scalar::ScalarValue::Int32(Some(v)) => Some(v.to_string()),
            datafusion::scalar::ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
            _ => None,
        },
        Expr::Alias(alias) => expr_to_sql(&alias.expr),
        Expr::BinaryExpr(binary) => {
            let left = expr_to_sql(&binary.left)?;
            let right = expr_to_sql(&binary.right)?;
            Some(format!("({} {} {})", left, binary.op, right))
        }
        Expr::Cast(cast) => {
            let inner = expr_to_sql(&cast.expr)?;
            // SQLite CAST syntax
            let type_str = match cast.data_type {
                datafusion::arrow::datatypes::DataType::Int64 => "INTEGER",
                datafusion::arrow::datatypes::DataType::Int32 => "INTEGER",
                datafusion::arrow::datatypes::DataType::Float64 => "REAL",
                datafusion::arrow::datatypes::DataType::Utf8 => "TEXT",
                _ => return None,
            };
            Some(format!("CAST({} AS {})", inner, type_str))
        }
        _ => None,
    }
}

/// Build output schema for the aggregate query using the logical schema
fn build_output_schema(logical_schema: &DFSchemaRef) -> DataFusionResult<SchemaRef> {
    // Get the inner Arrow Schema from DFSchema
    Ok(Arc::new(Schema::new(
        logical_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    )))
}

/// Build final AggregateExec to combine partial results from shards
///
/// For COUNT: final = SUM(partial_counts)
/// For SUM: final = SUM(partial_sums)
/// For MIN: final = MIN(partial_mins)
/// For MAX: final = MAX(partial_maxes)
fn build_final_aggregate(
    input: Arc<dyn ExecutionPlan>,
    group_expr: &[Expr],
    aggr_expr: &[Expr],
    schema: SchemaRef,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    use datafusion::physical_expr::expressions::Column;

    // Build GROUP BY expressions for physical plan
    // For computed expressions, shards return them as columns with the schema field name
    let group_by_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
        group_expr
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                let field = schema.field(idx);
                (
                    Arc::new(Column::new(field.name(), idx))
                        as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                    field.name().clone(),
                )
            })
            .collect();

    let physical_group_by = PhysicalGroupBy::new_single(group_by_exprs);
    let group_count = group_expr.len();

    // Build final aggregate expressions
    // Each aggregate from a shard needs to be combined with the appropriate final function
    let mut final_aggr_exprs: Vec<Arc<AggregateFunctionExpr>> = Vec::new();

    // Schema for the aggregate builder should be the input (union) schema
    let input_schema = input.schema();

    for (idx, expr) in aggr_expr.iter().enumerate() {
        let col_idx = group_count + idx;
        let field = schema.field(col_idx);
        let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new(field.name(), col_idx));

        // Determine the final aggregate function based on the original aggregate
        let final_func = get_final_aggregate_function(expr)?;

        let aggr_expr = AggregateExprBuilder::new(final_func, vec![col_expr])
            .schema(input_schema.clone())
            .alias(field.name())
            .build()?;

        final_aggr_exprs.push(Arc::new(aggr_expr));
    }

    // Create the final AggregateExec
    let final_exec = AggregateExec::try_new(
        AggregateMode::Single,
        physical_group_by,
        final_aggr_exprs,
        vec![None; aggr_expr.len()], // No filter expressions
        input,
        schema,
    )?;

    Ok(Arc::new(final_exec))
}

/// Get the appropriate final aggregate function for combining partial results
fn get_final_aggregate_function(expr: &Expr) -> DataFusionResult<Arc<AggregateUDF>> {
    match expr {
        Expr::AggregateFunction(agg) => {
            let name = agg.func.name();
            match name {
                // COUNT partial results need to be SUMmed
                "count" => Ok(sum_udaf()),
                // SUM partial results need to be SUMmed
                "sum" => Ok(sum_udaf()),
                // MIN partial results need MIN
                "min" => Ok(min_udaf()),
                // MAX partial results need MAX
                "max" => Ok(max_udaf()),
                _ => Err(datafusion::error::DataFusionError::Plan(format!(
                    "Unsupported aggregate function for final combine: {}",
                    name
                ))),
            }
        }
        Expr::Alias(alias) => get_final_aggregate_function(&alias.expr),
        _ => Err(datafusion::error::DataFusionError::Plan(
            "Expected aggregate function expression".to_string(),
        )),
    }
}

/// Custom query planner that includes our extension planner
pub struct ShardedQueryPlanner {
    default_planner: DefaultPhysicalPlanner,
}

impl std::fmt::Debug for ShardedQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedQueryPlanner").finish()
    }
}

impl ShardedQueryPlanner {
    pub fn new() -> Self {
        let extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            vec![Arc::new(ShardedAggregatePlanner)];
        Self {
            default_planner: DefaultPhysicalPlanner::with_extension_planners(extension_planners),
        }
    }
}

#[async_trait]
impl QueryPlanner for ShardedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.default_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sharded_provider::{ShardMetadata, ShardedSqliteProvider};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::*;
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts_ns", DataType::Int64, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("endpoint", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]))
    }

    fn test_shards() -> Vec<ShardMetadata> {
        vec![
            ShardMetadata::new("http://localhost:8001".into(), 1000, 1999),
            ShardMetadata::new("http://localhost:8002".into(), 2000, 2999),
        ]
    }

    fn create_test_context_with_rule() -> SessionContext {
        let table = ShardedSqliteProvider::new(
            test_schema(),
            test_shards(),
            "events".to_string(),
            "ts_ns".to_string(),
        );

        // Get default optimizer rules and prepend ours
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
        ctx.register_table("events", Arc::new(table)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_rule_matches_simple_count() {
        let ctx = create_test_context_with_rule();
        let df = ctx.sql("SELECT COUNT(*) FROM events").await.unwrap();

        // Get the optimized logical plan
        let optimized_plan = df.into_optimized_plan().unwrap();
        let plan_str = format!("{}", optimized_plan.display_indent());

        println!("Optimized plan:\n{}", plan_str);

        // The plan should contain our custom node
        assert!(
            plan_str.contains("ShardedAggregate"),
            "Plan should contain ShardedAggregate node: {}",
            plan_str
        );
    }

    #[tokio::test]
    async fn test_rule_matches_count_with_group_by() {
        let ctx = create_test_context_with_rule();
        let df = ctx
            .sql("SELECT service_name, COUNT(*) FROM events GROUP BY service_name")
            .await
            .unwrap();

        let optimized_plan = df.into_optimized_plan().unwrap();
        let plan_str = format!("{}", optimized_plan.display_indent());

        println!("Optimized plan:\n{}", plan_str);

        assert!(
            plan_str.contains("ShardedAggregate"),
            "Plan should contain ShardedAggregate: {}",
            plan_str
        );
    }

    #[tokio::test]
    async fn test_rule_handles_computed_group_by() {
        let ctx = create_test_context_with_rule();
        // Computed GROUP BY expression should now be pushed down
        let df = ctx
            .sql("SELECT ts_ns / 1000 as bucket, COUNT(*) FROM events GROUP BY bucket")
            .await
            .unwrap();

        let optimized_plan = df.into_optimized_plan().unwrap();
        let plan_str = format!("{}", optimized_plan.display_indent());

        println!("Optimized plan:\n{}", plan_str);

        // Should contain our custom node (computed GROUP BY is now supported)
        assert!(
            plan_str.contains("ShardedAggregate"),
            "Computed GROUP BY should be pushed: {}",
            plan_str
        );
    }

    // ========================================================================
    // PHYSICAL PLAN TESTS
    // ========================================================================

    use crate::http_executor::HttpSqliteExec;

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

    #[tokio::test]
    async fn test_physical_plan_has_count_sql() {
        let ctx = create_test_context_with_rule();
        let df = ctx.sql("SELECT COUNT(*) FROM events").await.unwrap();

        let physical_plan = df.create_physical_plan().await.unwrap();
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
        );
        println!("Physical plan:\n{}", plan_str);

        let queries = extract_shard_sql(&physical_plan);
        println!("Shard queries: {:?}", queries);

        assert!(!queries.is_empty(), "Should have shard queries");
        for sql in &queries {
            assert!(
                sql.to_uppercase().contains("COUNT("),
                "Shard SQL should contain COUNT: {}",
                sql
            );
        }
    }

    #[tokio::test]
    async fn test_physical_plan_has_group_by_sql() {
        let ctx = create_test_context_with_rule();
        let df = ctx
            .sql("SELECT service_name, COUNT(*) FROM events GROUP BY service_name")
            .await
            .unwrap();

        let physical_plan = df.create_physical_plan().await.unwrap();
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
        );
        println!("Physical plan:\n{}", plan_str);

        let queries = extract_shard_sql(&physical_plan);
        println!("Shard queries: {:?}", queries);

        assert!(!queries.is_empty(), "Should have shard queries");
        for sql in &queries {
            assert!(
                sql.to_uppercase().contains("COUNT("),
                "Shard SQL should contain COUNT: {}",
                sql
            );
            assert!(
                sql.to_uppercase().contains("GROUP BY"),
                "Shard SQL should contain GROUP BY: {}",
                sql
            );
        }
    }

    #[tokio::test]
    async fn test_physical_plan_has_final_aggregation() {
        let ctx = create_test_context_with_rule();
        let df = ctx.sql("SELECT COUNT(*) FROM events").await.unwrap();

        let physical_plan = df.create_physical_plan().await.unwrap();
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
        );
        println!("Physical plan:\n{}", plan_str);

        // Verify final aggregation is present (SUM of partial counts)
        assert!(
            plan_str.contains("AggregateExec"),
            "Plan should contain AggregateExec for final aggregation: {}",
            plan_str
        );

        // Verify shards receive COUNT queries
        let queries = extract_shard_sql(&physical_plan);
        assert!(!queries.is_empty(), "Should have shard queries");
        for sql in &queries {
            assert!(
                sql.to_uppercase().contains("COUNT("),
                "Shard SQL should contain COUNT: {}",
                sql
            );
        }
    }

    #[tokio::test]
    async fn test_sum_aggregate_pushdown_with_final_sum() {
        let ctx = create_test_context_with_rule();
        let df = ctx
            .sql("SELECT service_name, SUM(count) FROM events GROUP BY service_name")
            .await
            .unwrap();

        let physical_plan = df.create_physical_plan().await.unwrap();
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
        );
        println!("Physical plan:\n{}", plan_str);

        // Verify shards receive SUM queries
        let queries = extract_shard_sql(&physical_plan);
        assert!(!queries.is_empty(), "Should have shard queries");
        for sql in &queries {
            assert!(
                sql.to_uppercase().contains("SUM("),
                "Shard SQL should contain SUM: {}",
                sql
            );
            assert!(
                sql.to_uppercase().contains("GROUP BY"),
                "Shard SQL should contain GROUP BY: {}",
                sql
            );
        }

        // Verify final aggregation is present
        assert!(
            plan_str.contains("AggregateExec"),
            "Plan should contain AggregateExec for final aggregation: {}",
            plan_str
        );
    }

    // ========================================================================
    // COMPUTED GROUP BY TESTS
    // ========================================================================

    #[tokio::test]
    async fn test_computed_group_by_division() {
        let ctx = create_test_context_with_rule();
        let df = ctx
            .sql("SELECT ts_ns / 1000 AS bucket, COUNT(*), MIN(ts_ns), MAX(ts_ns) FROM events GROUP BY bucket")
            .await
            .unwrap();

        let physical_plan = df.create_physical_plan().await.unwrap();
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
        );
        println!("Physical plan:\n{}", plan_str);

        let queries = extract_shard_sql(&physical_plan);
        println!("Shard queries: {:?}", queries);

        println!("{:?}", queries[0]);

        assert!(!queries.is_empty(), "Should have shard queries");
        for sql in &queries {
            // Verify the division expression is pushed
            assert!(
                sql.contains("/ 1000") || sql.contains("/1000"),
                "Shard SQL should contain division: {}",
                sql
            );
            assert!(
                sql.to_uppercase().contains("GROUP BY"),
                "Shard SQL should contain GROUP BY: {}",
                sql
            );
            assert!(
                sql.to_uppercase().contains("COUNT("),
                "Shard SQL should contain COUNT: {}",
                sql
            );
            assert!(
                sql.to_uppercase().contains("MIN("),
                "Shard SQL should contain MIN: {}",
                sql
            );
            assert!(
                sql.to_uppercase().contains("MAX("),
                "Shard SQL should contain MAX: {}",
                sql
            );
        }
    }

    #[tokio::test]
    async fn test_join() {
        let ctx = create_test_context_with_rule();
        let df = ctx
            .sql("SELECT * FROM events where http_status = 200")
            .await
            .unwrap();
        let physical_plan = df.create_physical_plan().await.unwrap();
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
        );
        println!("Physical plan:\n{}", plan_str);
    }

    #[tokio::test]
    async fn test_computed_group_by_subtraction_and_division() {
        let ctx = create_test_context_with_rule();
        // Common time bucketing pattern: (ts - base) / interval
        let df = ctx
            .sql("SELECT (ts_ns - 1000) / 100 AS bucket, SUM(count) FROM events GROUP BY bucket")
            .await
            .unwrap();

        let physical_plan = df.create_physical_plan().await.unwrap();
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
        );
        println!("Physical plan:\n{}", plan_str);

        let queries = extract_shard_sql(&physical_plan);
        println!("Shard queries: {:?}", queries);

        assert!(!queries.is_empty(), "Should have shard queries");
        for sql in &queries {
            // Verify the complex expression is pushed
            assert!(
                sql.contains("- 1000") || sql.contains("-1000"),
                "Shard SQL should contain subtraction: {}",
                sql
            );
            assert!(
                sql.contains("/ 100") || sql.contains("/100"),
                "Shard SQL should contain division: {}",
                sql
            );
            assert!(
                sql.to_uppercase().contains("SUM("),
                "Shard SQL should contain SUM: {}",
                sql
            );
        }
    }

    #[tokio::test]
    async fn test_time_series_bucket_query() {
        let ctx = create_test_context_with_rule();
        // Realistic time-series bucketing query
        let df = ctx
            .sql(
                r#"
                SELECT
                    (ts_ns / 1000) AS bucket_index,
                    MIN(ts_ns) AS bucket_min_ts,
                    MAX(ts_ns) AS bucket_max_ts,
                    SUM(count) AS total_count
                FROM events
                GROUP BY bucket_index
            "#,
            )
            .await
            .unwrap();

        let physical_plan = df.create_physical_plan().await.unwrap();
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
        );
        println!("Physical plan:\n{}", plan_str);

        let queries = extract_shard_sql(&physical_plan);
        println!("Shard queries: {:?}", queries);

        assert!(!queries.is_empty(), "Should have shard queries");
        for sql in &queries {
            assert!(
                sql.contains("/ 1000"),
                "Shard SQL should contain bucket expression: {}",
                sql
            );
            assert!(
                sql.to_uppercase().contains("MIN("),
                "Shard SQL should contain MIN: {}",
                sql
            );
            assert!(
                sql.to_uppercase().contains("MAX("),
                "Shard SQL should contain MAX: {}",
                sql
            );
            assert!(
                sql.to_uppercase().contains("SUM("),
                "Shard SQL should contain SUM: {}",
                sql
            );
        }

        // Verify final aggregation
        assert!(
            plan_str.contains("AggregateExec"),
            "Plan should contain final AggregateExec: {}",
            plan_str
        );
    }
}
