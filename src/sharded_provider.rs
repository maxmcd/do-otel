use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_federation::FederatedTableProviderAdaptor;
use datafusion_federation::sql::{RemoteTableRef, SQLFederationProvider, SQLTableSource};
use std::sync::Arc;

use crate::http_executor::HttpSqliteExecutor;

// ============================================================================
// SHARD METADATA
// ============================================================================

#[derive(Debug, Clone)]
pub struct ShardMetadata {
    pub name: String,
    pub endpoint_url: String,
    pub start_time: i64,
    pub end_time: i64,
}

impl ShardMetadata {
    pub fn new(name: String, endpoint_url: String, start_time: i64, end_time: i64) -> Self {
        Self {
            name,
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

    fn get_relevant_shards(&self, filters: &[Expr]) -> Vec<ShardMetadata> {
        let (min_time, max_time) = extract_time_range(filters, &self.time_column);

        let relevant_shards: Vec<ShardMetadata> = self
            .shards
            .iter()
            .filter(|shard| shard.overlaps(min_time, max_time))
            .cloned()
            .collect();

        println!(
            "🔍 Shard pruning: {} of {} shards selected for time range {:?} to {:?}",
            relevant_shards.len(),
            self.shards.len(),
            min_time,
            max_time
        );

        for shard in &relevant_shards {
            println!("  ✓ {}: {} [{} - {}]", shard.name, shard.endpoint_url, shard.start_time, shard.end_time);
        }

        relevant_shards
    }

    fn create_federated_provider(
        &self,
        shards: Vec<ShardMetadata>,
    ) -> Result<Arc<dyn TableProvider>> {
        // Create a federation provider for the first shard
        // In a real implementation, you'd want to union across all shards
        if shards.is_empty() {
            return Err(DataFusionError::Plan(
                "No shards available for query".to_string(),
            ));
        }

        // For now, create a simple union of all shard providers
        // In practice, you might want to use UnionExec or custom logic
        let shard = &shards[0];
        let executor = Arc::new(HttpSqliteExecutor::new(
            shard.name.clone(),
            shard.endpoint_url.clone(),
        ));

        let fed_provider = Arc::new(SQLFederationProvider::new(executor));
        let table_source = Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            RemoteTableRef::parse_with_default_dialect(&self.table_name)?,
            Arc::clone(&self.schema),
        ));

        Ok(Arc::new(FederatedTableProviderAdaptor::new(table_source)))
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
            .map(|_| TableProviderFilterPushDown::Inexact)
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

        // Create a federated provider for the relevant shards
        let provider = self.create_federated_provider(relevant_shards)?;

        // Delegate to the federated provider's scan
        provider.scan(state, projection, filters, limit).await
    }
}
