use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::stream;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

#[derive(Serialize)]
struct SqlRequest {
    sql: String,
}

#[derive(Deserialize)]
struct SqlResponse {
    rows: Vec<Vec<serde_json::Value>>,
}

/// HTTP client for executing SQL queries on remote SQLite endpoints
#[derive(Debug, Clone)]
pub struct HttpSqliteExecutor {
    endpoint_url: String,
    client: reqwest::Client,
}

impl HttpSqliteExecutor {
    pub fn new(endpoint_url: String) -> Self {
        Self {
            endpoint_url,
            client: reqwest::Client::new(),
        }
    }

    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    /// Create an execution plan that will run the given SQL
    pub fn create_exec(self: &Arc<Self>, sql: String, schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(HttpSqliteExec::new(Arc::clone(self), sql, schema))
    }

    async fn execute_http_query(&self, sql: &str) -> Result<SqlResponse> {
        let request = SqlRequest {
            sql: sql.to_string(),
        };

        println!("🌐 Executing on {}: {}", self.endpoint_url, sql);

        let response = self
            .client
            .post(&self.endpoint_url)
            .json(&request)
            .send()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(DataFusionError::Execution(format!(
                "HTTP error {}: {}",
                status, error_text
            )));
        }

        response
            .json::<SqlResponse>()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn json_to_record_batch(
        &self,
        response: SqlResponse,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let rows = response.rows;

        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            match field.data_type() {
                DataType::Int64 => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| row.get(col_idx).and_then(|v| v.as_i64()))
                        .collect();
                    columns.push(Arc::new(Int64Array::from(values)));
                }
                DataType::Float64 => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| row.get(col_idx).and_then(|v| v.as_f64()))
                        .collect();
                    columns.push(Arc::new(Float64Array::from(values)));
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        if let Some(v) = row.get(col_idx) {
                            if let Some(s) = v.as_str() {
                                builder.append_value(s);
                            } else if v.is_null() {
                                builder.append_null();
                            } else {
                                builder.append_value(v.to_string());
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                DataType::Timestamp(_, _) => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| row.get(col_idx).and_then(|v| v.as_i64()))
                        .collect();
                    columns.push(Arc::new(Int64Array::from(values)));
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type: {:?}",
                        field.data_type()
                    )));
                }
            }
        }

        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

// ============================================================================
// HTTP SQLITE EXECUTION PLAN
// ============================================================================

/// Physical execution plan that executes SQL over HTTP
#[derive(Debug)]
pub struct HttpSqliteExec {
    executor: Arc<HttpSqliteExecutor>,
    sql: String,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl HttpSqliteExec {
    pub fn new(executor: Arc<HttpSqliteExecutor>, sql: String, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            executor,
            sql,
            schema,
            properties,
        }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}

impl DisplayAs for HttpSqliteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "HttpSqliteExec: endpoint={}, sql={}",
                    self.executor.endpoint_url(),
                    self.sql
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "HttpSqliteExec: endpoint={}, sql={}",
                    self.executor.endpoint_url(),
                    self.sql
                )
            }
        }
    }
}

impl ExecutionPlan for HttpSqliteExec {
    fn name(&self) -> &str {
        "HttpSqliteExec"
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let executor = Arc::clone(&self.executor);
        let sql = self.sql.clone();
        let schema = self.schema.clone();

        let stream = stream::once(async move {
            let response = executor.execute_http_query(&sql).await?;
            executor.json_to_record_batch(response, schema)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
