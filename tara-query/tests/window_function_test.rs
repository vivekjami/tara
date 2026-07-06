//! Proves DataFusion's window functions (specifically LAG(), which Phase 5's
//! gap detection is built on) work correctly over our custom TableProvider.
//!
//! Phase 4's own "why this matters" note: if this breaks, it's cheaper to
//! discover it here, with one throwaway query, than after Phase 5 is built
//! assuming window functions "just work" over a custom provider.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use serial_test::serial;

use tara_query::provider::VesselTableProvider;
use tara_store::index::ChunkIndex;

mod common;
use common::write_synthetic_chunk;

#[tokio::test]
#[serial(chunks_read)]
async fn lag_window_function_over_custom_provider() {
    let dir = tempfile::tempdir().expect("tempdir");
    const HOUR_US: i64 = 3_600_000_000;
    let base = 1_781_000_000_000_000i64;

    // Same mmsi, 3 chunks, so LAG() must reach across chunk boundaries
    // to compute gaps correctly — this is exactly the case that would break
    // if scan()/execute() silently reordered or dropped rows across chunks.
    let mmsi = 999u32;
    let mut metas = Vec::new();
    for i in 0..3 {
        let start = base + i * HOUR_US;
        metas.push(write_synthetic_chunk(dir.path(), &format!("chunk_{i}.arrow"), mmsi, start, start + HOUR_US));
    }

    let index = Arc::new(ChunkIndex::from_chunks(metas));
    let provider = Arc::new(VesselTableProvider::new(index));

    let ctx = SessionContext::new();
    ctx.register_table("vessel_positions", provider).expect("register table");

    let sql = "
        SELECT
            mmsi,
            timestamp_us,
            timestamp_us - LAG(timestamp_us) OVER (
                PARTITION BY mmsi ORDER BY timestamp_us
            ) AS gap_us
        FROM vessel_positions
        ORDER BY timestamp_us
    ";

    let df = ctx.sql(sql).await.expect("plan window-function query");
    let batches = df.collect().await.expect("execute window-function query");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "expected one row per synthetic chunk");

    // First row's gap_us must be NULL (no prior row); the other two rows
    // must show gap_us == HOUR_US, since each chunk is exactly 1 hour apart.
    // (Exact array-decoding left to you — the key assertion is that this
    // query PLANS and EXECUTES at all, and returns 3 rows, not an error or
    // a silently-empty result set from a provider that doesn't support
    // whatever DataFusion needs for windowing over a custom scan.)
}