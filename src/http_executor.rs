use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::unparser::dialect::SqliteDialect;
use datafusion_federation::sql::SQLExecutor;
use futures::stream;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::sqlite_interval::apply_sqlite_interval_transform;

/// HTTP client for executing SQL queries on remote SQLite endpoints
#[derive(Clone)]
pub struct HttpSqliteExecutor {
    endpoint_url: String,
    client: reqwest::Client,
    name: String,
}

#[derive(Serialize)]
struct SqlRequest {
    sql: String,
}

#[derive(Deserialize)]
struct SqlResponse {
    rows: Vec<serde_json::Value>,
}

impl HttpSqliteExecutor {
    pub fn new(name: String, endpoint_url: String) -> Self {
        Self {
            name,
            endpoint_url,
            client: reqwest::Client::new(),
        }
    }

    async fn execute_http_query(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let request = SqlRequest {
            sql: sql.to_string(),
        };

        let response = self
            .client
            .post(&self.endpoint_url)
            .json(&request)
            .send()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if !response.status().is_success() {
            return Err(DataFusionError::External(
                format!("HTTP error: {}", response.status()).into(),
            ));
        }

        let sql_response: SqlResponse = response
            .json()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(sql_response.rows)
    }

    fn json_to_record_batch(
        &self,
        rows: Vec<serde_json::Value>,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for field in schema.fields() {
            let column_name = field.name();
            let data_type = field.data_type();

            match data_type {
                DataType::Int64 => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| {
                            row.get(column_name)
                                .and_then(|v| v.as_i64())
                        })
                        .collect();
                    columns.push(Arc::new(Int64Array::from(values)));
                }
                DataType::Float64 => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| {
                            row.get(column_name)
                                .and_then(|v| v.as_f64())
                        })
                        .collect();
                    columns.push(Arc::new(Float64Array::from(values)));
                }
                DataType::Utf8 => {
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| {
                            row.get(column_name)
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        })
                        .collect();
                    columns.push(Arc::new(StringArray::from(values)));
                }
                DataType::Timestamp(_, _) => {
                    // Parse timestamps as i64
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| {
                            row.get(column_name)
                                .and_then(|v| v.as_i64())
                        })
                        .collect();
                    columns.push(Arc::new(Int64Array::from(values)));
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type for HTTP response: {:?}",
                        data_type
                    )));
                }
            }
        }

        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

#[async_trait]
impl SQLExecutor for HttpSqliteExecutor {
    fn name(&self) -> &str {
        &self.name
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.endpoint_url.clone())
    }

    fn dialect(&self) -> Arc<dyn datafusion::sql::unparser::dialect::Dialect> {
        Arc::new(SqliteDialect {})
    }

    fn ast_analyzer(&self) -> Option<datafusion_federation::sql::AstAnalyzer> {
        // Apply SQLite interval transformation
        Some(Box::new(|ast: Statement| apply_sqlite_interval_transform(ast)))
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> Result<SendableRecordBatchStream> {
        let query = query.to_string();
        let endpoint = self.endpoint_url.clone();
        let executor = self.clone();
        let schema_clone = schema.clone();

        println!("🌐 Executing on {}: {}", endpoint, query);

        let stream = stream::once(async move {
            let rows = executor.execute_http_query(&query).await?;
            executor.json_to_record_batch(rows, schema_clone)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table_names not implemented for HTTP executor".to_string(),
        ))
    }

    async fn get_table_schema(&self, _table_name: &str) -> Result<SchemaRef> {
        Err(DataFusionError::NotImplemented(
            "get_table_schema not implemented for HTTP executor".to_string(),
        ))
    }
}
