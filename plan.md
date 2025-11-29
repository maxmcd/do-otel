# Sqlotel: Sharded SQLite Query Pushdown Plan

## Problem Statement

Build a system that runs DataFusion SQL queries and pushes down analytical queries (including aggregations) to SQLite time-series shards over HTTP, avoiding raw-row transfer over the network.

## Research Summary

Two approaches were explored:

### sqlarge (Custom Everything)

**Architecture:**
- Custom `TableProvider` with manual SQL string building
- Custom `OptimizerRule` (ShardedAggregationRule) for logical two-phase aggregation
- Custom `PhysicalOptimizerRule` (ShardAggregatePushdown) for physical plan rewriting
- Custom `ExecutionPlan` (HttpSqliteExec) for HTTP transport

**What Works:**
- Two-phase aggregation pushdown for COUNT/SUM/MIN/MAX
- Shard pruning based on time filters
- Full end-to-end HTTP→JSON→Arrow pipeline

**Limitations:**
- Brittle SQL string manipulation (substring parsing like `" FROM "`, `" WHERE "`)
- String matching to detect existing aggregates (`"COUNT("`, `"GROUP BY"`)
- Deep coupling to DataFusion internals (partial_*, AggregateMode, schema field positions)
- Skips pushdown when child is UnionExec (multi-shard case is fragile)
- Duplicated shard-pruning logic in multiple places

### sqlotel (Federation Libraries)

**Architecture:**
- `HttpSqliteExecutor` implementing `SQLExecutor` trait from datafusion-federation
- `SQLFederationProvider` + `SQLTableSource` for automatic SQL generation
- `ShardedSqliteProvider` for shard selection and union of per-shard plans

**What Works:**
- Clean HTTP executor with proper abstraction
- Automatic SQL generation via library (no manual string building)
- SQLite dialect handling and interval transforms
- Proper shard pruning based on time ranges

**Limitations:**
- **No cross-shard aggregate pushdown** - currently unions raw rows and aggregates locally
- Missing the map/reduce decomposition logic (COUNT→SUM across shards)
- Federation library designed for single-source pushdown, not homogeneous sharded tables

## Key Insight

DataFusion's `TableProvider::scan()` only sees projection, filters, and limit—**not aggregations**. This is why:
- sqlotel can't push aggregates down without additional work
- sqlarge had to implement a custom `OptimizerRule` to intercept `Aggregate` nodes

## Recommended Path Forward

**Hybrid approach: Use sqlotel's federation stack + port sqlarge's aggregation logic**

This gives us:
- sqlotel's maintainability (no SQL string hacking)
- sqlarge's correct semantics (map/reduce aggregation across shards)

---

## Implementation Plan

### Phase 1: Stabilize Current sqlotel Foundation

**Tasks:**
1. Add tests for basic shard pruning
   - Verify only relevant shards are queried based on time filters
   - Test edge cases: single shard, all shards, no matching shards

2. Add tests for HTTP transport
   - Validate JSON→Arrow conversion for all supported types
   - Test error handling for failed HTTP requests

3. Clean up logging
   - Make shard selection logging configurable
   - Add query logging in HttpSqliteExecutor for debugging

### Phase 2: Implement Logical Sharded Aggregation Rule

Port a simplified version of sqlarge's `ShardedAggregationRule`:

**2.1 Create `sharded_aggregation_rule.rs`**

```rust
// Core structure
struct ShardedAggregationRule {
    table_name: String,      // e.g., "events"
    time_column: String,     // e.g., "ts_ns"  
    shards: Vec<ShardMetadata>,
}

impl OptimizerRule for ShardedAggregationRule {
    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig) 
        -> Result<Transformed<LogicalPlan>>;
}
```

**2.2 Pattern Matching**

Match logical plans of the form:
```
Aggregate
  └── Filter (optional, may be multiple)
        └── Projection (optional)
              └── TableScan("events")
```

Traverse through Filter/Projection nodes to:
- Collect filter predicates
- Find the underlying TableScan
- Verify it's our sharded table

**2.3 Aggregation Decomposition**

Port the `AggregationDecomposition` concept from sqlarge:

| Original | Partial (per-shard) | Final (combine) |
|----------|---------------------|-----------------|
| `COUNT(*)` | `COUNT(*) AS partial_count_N` | `SUM(partial_count_N)` |
| `COUNT(col)` | `COUNT(col) AS partial_count_N` | `SUM(partial_count_N)` |
| `SUM(col)` | `SUM(col) AS partial_sum_N` | `SUM(partial_sum_N)` |
| `MIN(col)` | `MIN(col) AS partial_min_N` | `MIN(partial_min_N)` |
| `MAX(col)` | `MAX(col) AS partial_max_N` | `MAX(partial_max_N)` |
| `AVG(col)` | `SUM(col), COUNT(col)` | `SUM(sum)/SUM(count)` |

**2.4 Plan Rewriting**

Transform:
```
Aggregate(group_by=[service_name], aggr=[COUNT(*)])
  └── TableScan(events)
```

Into:
```
FinalAggregate(group_by=[service_name], aggr=[SUM(partial_count_0)])
  └── Union
        ├── PartialAggregate(group_by=[service_name], aggr=[COUNT(*) AS partial_count_0])
        │     └── TableScan(shard_0)  // via SQLTableSource
        ├── PartialAggregate(...)
        │     └── TableScan(shard_1)
        └── PartialAggregate(...)
              └── TableScan(shard_2)
```

**Key difference from sqlarge:** Don't build SQL strings manually. Create logical plans where each shard has `Aggregate + Scan`, and let the federation library generate the SQL.

**2.5 Per-Shard Table Sources**

For each relevant shard, create:
```rust
let executor = HttpSqliteExecutor::new(shard.name, shard.endpoint_url);
let fed_provider = SQLFederationProvider::new(executor);
let table_source = SQLTableSource::new_with_schema(fed_provider, table_ref, schema);
```

Then build a logical scan over this source.

### Phase 3: Integration and Testing

**3.1 Register the Optimizer Rule**

```rust
let state = SessionStateBuilder::new()
    .with_config(config)
    .with_default_features()
    .with_optimizer_rule(Arc::new(ShardedAggregationRule::new(...)))
    .build();
```

**3.2 Table-Driven Tests**

Port sqlarge's test structure:

```rust
struct TestCase {
    input_sql: String,
    expected_shard_sql_contains: Vec<&str>,  // What each shard SQL should contain
    expected_shard_sql_excludes: Vec<&str>,  // What it should NOT contain
}

let test_cases = vec![
    TestCase {
        input_sql: "SELECT COUNT(*) FROM events",
        expected_shard_sql_contains: vec!["COUNT("],
        expected_shard_sql_excludes: vec![],
    },
    TestCase {
        input_sql: "SELECT service_name, COUNT(*) FROM events GROUP BY service_name",
        expected_shard_sql_contains: vec!["COUNT(", "GROUP BY", "service_name"],
        expected_shard_sql_excludes: vec![],
    },
    // Complex GROUP BY - should NOT push down the grouping
    TestCase {
        input_sql: "SELECT CAST((ts_ns - 1705276800000000000) / 60000000000 AS INTEGER) as minute, COUNT(*) FROM events GROUP BY minute",
        expected_shard_sql_contains: vec!["ts_ns"],  // Just fetches the column
        expected_shard_sql_excludes: vec!["GROUP BY"],  // Grouping happens locally
    },
];
```

**3.3 Instrumentation**

Add SQL capture mechanism in `HttpSqliteExecutor`:
```rust
impl HttpSqliteExecutor {
    // For testing: capture all executed queries
    pub fn with_query_log(self, log: Arc<Mutex<Vec<String>>>) -> Self;
}
```

### Phase 4: Handle Edge Cases

**4.1 Complex GROUP BY Expressions**

For expressions like `CAST((ts_ns - base) / interval AS INTEGER)`:
- Option A: Don't push down the GROUP BY; let shards return raw data
- Option B: Detect known patterns and transform to SQLite-compatible syntax

Start with Option A (conservative), add Option B incrementally.

**4.2 Filters with Time Bounds**

Ensure filters are:
1. Used for shard pruning (selecting which shards to query)
2. Pushed down to each shard's SQL (via federation library)
3. Applied in DataFusion as final safety check (Inexact pushdown)

**4.3 Single Shard Optimization**

When only one shard matches:
- Skip the Union wrapper
- Return the single shard's plan directly

### Phase 5: Remove Redundant Code

Once the new architecture is working:

1. Simplify `ShardedSqliteProvider`
   - It no longer needs to create per-shard providers in `scan()`
   - The optimizer rule handles shard-aware planning

2. Consider if `ShardedSqliteProvider` should just be a thin wrapper that:
   - Registers the table with the correct schema
   - Lets the optimizer rule handle all shard logic

---

## File Structure (Target)

```
sqlotel/src/
├── main.rs                      # Entry point, examples
├── http_executor.rs             # HttpSqliteExecutor (existing, enhanced)
├── sharded_provider.rs          # ShardedSqliteProvider (simplified)
├── sharded_aggregation_rule.rs  # NEW: Logical optimizer rule
├── aggregation_decomposition.rs # NEW: Partial/final agg logic
├── sqlite_interval.rs           # SQLite interval transforms (existing)
└── test_utils.rs                # NEW: Test helpers, SQL capture
```

---

## Success Criteria

1. **Aggregations are pushed to shards**: For `SELECT COUNT(*) FROM events`, each shard receives SQL containing `COUNT(*)`, not `SELECT *`

2. **Results are correct**: Final aggregated results match what you'd get from a single non-sharded database

3. **Shard pruning works**: Time-filtered queries only hit relevant shards

4. **No manual SQL building**: All SQL generation handled by datafusion-federation/SQLTableSource

5. **Test coverage**: Table-driven tests verify pushdown behavior for:
   - Simple COUNT(*)
   - COUNT(*) with GROUP BY column
   - SUM/MIN/MAX
   - Queries with WHERE clauses
   - Multi-shard and single-shard cases

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Federation library doesn't push all aggregates | Start with known-working cases (COUNT, SUM, MIN, MAX); add more incrementally |
| DataFusion version changes break pattern matching | Pin DataFusion version; add version-specific tests |
| Complex GROUP BY expressions can't be pushed | Explicitly don't push these; document as limitation |
| SQLite dialect differences cause incorrect SQL | Use existing `apply_sqlite_interval_transform`; add more transforms as needed |

---

## Non-Goals (For Now)

- Window functions pushdown
- JOIN pushdown across shards
- Subquery pushdown
- Custom aggregation functions
- Dynamic shard discovery (shards are configured statically)

---

## References

- sqlarge codebase: `../sqlarge/`
- DataFusion optimizer docs: https://docs.rs/datafusion/latest/datafusion/optimizer/
- datafusion-federation: https://github.com/datafusion-contrib/datafusion-federation
