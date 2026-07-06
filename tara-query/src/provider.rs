//! `VesselTableProvider` — DataFusion `TableProvider` over Tara's chunk store.
//!
//! ## DataFusion 54 note
//!
//! DataFusion 54 removed `as_any()` from `TableProvider`, `ExecutionPlan`,
//! and many other traits, using Rust trait upcasting instead. Do not add
//! `as_any` to these impls — it will not compile.
//!
//! ## Call sequence for a filtered query
//!
//! 1. `supports_filters_pushdown(&[&Expr])` — DataFusion asks which WHERE
//!    filters we can help with. We return `Inexact` for `timestamp_us`
//!    comparisons; everything else is `Unsupported`.
//!
//! 2. `scan(filters: &[Expr])` — called once per query with the accepted
//!    filters. We extract time bounds, prune the chunk index, and return a
//!    `TaraExecutionPlan` over the surviving chunks. No file I/O here.
//!
//! 3. `TaraExecutionPlan::execute()` — called per partition during execution.
//!    Opens the Arrow IPC files and streams `RecordBatch`es to DataFusion.

use std::fs::File;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use futures::stream;
use tara_store::chunk::ChunkMeta;
use tara_store::index::ChunkIndex;
use tracing::info;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Test-only instrumentation: counts how many chunk files were actually opened.
/// Reset with `CHUNKS_READ.store(0, Ordering::SeqCst)` before each test.
pub static CHUNKS_READ: AtomicUsize = AtomicUsize::new(0);
// ── Schema ────────────────────────────────────────────────────────────────────

/// Vessel position schema using DataFusion's bundled Arrow types.
///
/// `tara-store` uses standalone `arrow`; `tara-query` uses `datafusion::arrow`.
/// These may be different versions with incompatible type boundaries.
/// We declare the schema here so every type in `tara-query` comes from one
/// consistent Arrow version. Must match what `tara-ingest` wrote exactly.
pub fn vessel_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("mmsi", DataType::UInt32, false),
        Field::new(
            "timestamp_us",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("latitude", DataType::Float64, false),
        Field::new("longitude", DataType::Float64, false),
        Field::new("sog", DataType::Float32, true),
        Field::new("cog", DataType::Float32, true),
        Field::new("heading", DataType::UInt16, true),
        Field::new("mobile_type", DataType::Utf8, false),
        Field::new("nav_status", DataType::Utf8, true),
        Field::new("ship_type", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

// ── TableProvider ─────────────────────────────────────────────────────────────

/// DataFusion `TableProvider` backed by Tara's chunk index.
///
/// Owns no Arrow data — only the index metadata and schema.
/// Register with a `SessionContext` to expose `vessel_positions` as SQL.
#[derive(Debug)]
pub struct VesselTableProvider {
    index: Arc<ChunkIndex>,
    schema: SchemaRef,
}

impl VesselTableProvider {
    pub fn new(index: Arc<ChunkIndex>) -> Self {
        Self {
            index,
            schema: vessel_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for VesselTableProvider {
    // NOTE: no as_any — removed in DataFusion 54, uses trait upcasting instead

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Declare which WHERE-clause filters we can use for chunk pruning.
    ///
    /// `Inexact` — we use the filter to skip chunks, but DataFusion still
    /// applies it as a post-scan filter to guarantee row-level correctness.
    /// We may return extra rows (from chunks that partially overlap the range);
    /// DataFusion's FilterExec removes them. This is always safe.
    ///
    /// `Unsupported` — DataFusion handles the filter entirely; we see nothing.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let result = filters
            .iter()
            .map(|expr| {
                if is_timestamp_us_range_comparison(expr) {
                    info!("pushdown accepted: {:?}", expr);
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();
        Ok(result)
    }

    /// Prune chunks with accepted filters; return a plan over survivors.
    ///
    /// Called once during physical planning with only the filters we accepted
    /// as `Inexact`. Must not do file I/O — runs on the planning thread.
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let (time_min, time_max) = extract_time_bounds(filters);

        let chunks: Vec<ChunkMeta> = if time_min.is_some() || time_max.is_some() {
            let start = time_min.unwrap_or(i64::MIN);
            let end = time_max.unwrap_or(i64::MAX);
            let pruned = self.index.query_time_range(start, end);
            info!(
                "time range [{}, {}]: {} → {} chunks",
                start,
                end,
                self.index.len(),
                pruned.len()
            );
            pruned.into_iter().cloned().collect()
        } else {
            info!("no time filter: scanning all {} chunks", self.index.len());
            self.index.all_chunks().into_iter().cloned().collect()
        };

        Ok(Arc::new(TaraExecutionPlan::new(
            chunks,
            self.schema.clone(),
            projection.cloned(),
        )))
    }
}

// ── Filter helpers ────────────────────────────────────────────────────────────

/// Returns true if `expr` is a `<`, `<=`, `>`, or `>=` comparison
/// with `timestamp_us` as the left-hand column.
fn is_timestamp_us_range_comparison(expr: &Expr) -> bool {
    use datafusion::logical_expr::Operator;
    let Expr::BinaryExpr(bin) = expr else {
        return false;
    };
    if !matches!(
        bin.op,
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) {
        return false;
    }
    matches!(bin.left.as_ref(), Expr::Column(c) if c.name == "timestamp_us")
}

/// Extract tightest `[time_min, time_max]` from pushed-down filters.
///
/// DataFusion splits compound AND predicates into separate `Expr` elements
/// in the `filters` slice — no recursion into AND nodes needed here.
///
/// Handles two literal forms DataFusion may use after type coercion:
/// - `TimestampMicrosecond` — from `arrow_cast(...)` or timestamp literals
/// - `Int64` — from raw integer literals in some query forms
fn extract_time_bounds(filters: &[Expr]) -> (Option<i64>, Option<i64>) {
    use datafusion::logical_expr::Operator;

    let mut lo: Option<i64> = None;
    let mut hi: Option<i64> = None;

    for expr in filters {
        let Expr::BinaryExpr(bin) = expr else { continue };
        let Expr::Column(col) = bin.left.as_ref() else { continue };
        if col.name != "timestamp_us" { continue; }

        let val: i64 = match bin.right.as_ref() {
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(v), _), _) => *v,
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v,
            _ => continue,
        };

        match bin.op {
            Operator::GtEq | Operator::Gt => lo = Some(lo.map_or(val, |m| m.max(val))),
            Operator::LtEq | Operator::Lt => hi = Some(hi.map_or(val, |m| m.min(val))),
            _ => {}
        }
    }

    (lo, hi)
}

// ── ExecutionPlan ─────────────────────────────────────────────────────────────

/// Physical plan over a pruned set of chunk files.
///
/// Created by `scan()` during planning; files are opened lazily in `execute()`.
#[derive(Debug)]
struct TaraExecutionPlan {
    chunks: Vec<ChunkMeta>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl TaraExecutionPlan {
    fn new(chunks: Vec<ChunkMeta>, schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let out_schema: SchemaRef = match &projection {
            Some(idx) => Arc::new(schema.project(idx).unwrap()),
            None => schema.clone(),
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(out_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self { chunks, schema, projection, properties }
    }

    fn output_schema(&self) -> SchemaRef {
        match &self.projection {
            Some(idx) => Arc::new(self.schema.project(idx).unwrap()),
            None => self.schema.clone(),
        }
    }
}

impl DisplayAs for TaraExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "TaraExecutionPlan: {} chunks", self.chunks.len())
    }
}

impl ExecutionPlan for TaraExecutionPlan {
    // NOTE: no as_any — removed in DataFusion 54

    fn name(&self) -> &str { "TaraExecutionPlan" }

    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Stream Arrow batches from pruned chunk files, opened lazily.
    fn execute(&self, _: usize, _: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let chunks = self.chunks.clone();
        let projection = self.projection.clone();
        let out_schema = self.output_schema();

        let iter = chunks.into_iter().flat_map(move |meta| {
            read_chunk(&meta.path, projection.as_deref()).unwrap_or_default()
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            out_schema,
            stream::iter(iter.map(Ok::<RecordBatch, DataFusionError>)),
        )))
    }
}

// ── I/O ───────────────────────────────────────────────────────────────────────

/// Open one Arrow IPC file, apply projection, return all batches.
/// Returns empty Vec on any error — corrupt chunks are skipped silently.
fn read_chunk(path: &std::path::Path, projection: Option<&[usize]>) -> anyhow::Result<Vec<RecordBatch>> {
    CHUNKS_READ.fetch_add(1, Ordering::SeqCst);
    let reader = datafusion::arrow::ipc::reader::FileReader::try_new(
        File::open(path)?,
        projection.map(|p| p.to_vec()),
    )?;
    Ok(reader.into_iter().filter_map(|b| b.ok()).collect())
}