//! Proves that a narrow time-range query prunes chunks via the index
//! rather than opening every chunk file and filtering client-side.
//!
//! This is the "Done when" requirement from PHASES.md Phase 4:
//! "the pushdown test confirms chunk pruning is actually reducing work,
//! not just returning correct results the slow way."

use std::sync::Arc;
use std::sync::atomic::Ordering;

use datafusion::prelude::SessionContext;
use serial_test::serial;

use tara_query::provider::{CHUNKS_READ, VesselTableProvider};
use tara_store::index::ChunkIndex;

mod common;
use common::write_synthetic_chunk;

#[tokio::test]
#[serial(chunks_read)]
async fn narrow_time_range_query_prunes_chunks() {
    let dir = tempfile::tempdir().expect("tempdir");

    // 20 chunks, each covering a distinct, non-overlapping 1-hour window,
    // spaced across ~20 hours of synthetic time.
    const HOUR_US: i64 = 3_600_000_000;
    let base = 1_781_000_000_000_000i64;

    let mut metas = Vec::new();
    for i in 0..20 {
        let start = base + i * HOUR_US;
        let end = start + HOUR_US;
        metas.push(write_synthetic_chunk(
            dir.path(),
            &format!("chunk_{i}.arrow"),
            100 + i as u32,
            start,
            end,
        ));
    }

    let index = Arc::new(ChunkIndex::from_chunks(metas));
    let provider = Arc::new(VesselTableProvider::new(index));

    let ctx = SessionContext::new();
    ctx.register_table("vessel_positions", provider)
        .expect("register table");

    // Query a range that overlaps exactly ONE of the 20 synthetic chunks.
    let target_start = base + 5 * HOUR_US;
    let target_end = target_start + HOUR_US / 2; // well within chunk 5, not touching chunk 6

    CHUNKS_READ.store(0, Ordering::SeqCst);

    let sql = format!(
        "SELECT mmsi FROM vessel_positions \
         WHERE timestamp_us >= arrow_cast({target_start}, 'Timestamp(Microsecond, \"UTC\")') \
           AND timestamp_us <= arrow_cast({target_end}, 'Timestamp(Microsecond, \"UTC\")')"
    );

    let df = ctx.sql(&sql).await.expect("plan query");
    let batches = df.collect().await.expect("execute query");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "expected exactly one matching row from the targeted chunk"
    );

    let chunks_opened = CHUNKS_READ.load(Ordering::SeqCst);
    assert!(
        chunks_opened <= 2, // allow ±1 for boundary-overlap slop; must NOT be anywhere near 20
        "pushdown did not prune chunks: opened {chunks_opened} of 20 chunks for a query \
         that should only touch one. Check extract_time_bounds() and query_time_range()."
    );
}

#[tokio::test]
#[serial(chunks_read)]
async fn unfiltered_query_reads_all_chunks() {
    // Sanity check for the test itself: prove CHUNKS_READ actually reflects
    // reality by confirming a query with NO time filter opens every chunk.
    let dir = tempfile::tempdir().expect("tempdir");
    const HOUR_US: i64 = 3_600_000_000;
    let base = 1_781_000_000_000_000i64;

    let mut metas = Vec::new();
    for i in 0..5 {
        let start = base + i * HOUR_US;
        metas.push(write_synthetic_chunk(
            dir.path(),
            &format!("chunk_{i}.arrow"),
            100 + i as u32,
            start,
            start + HOUR_US,
        ));
    }

    let index = Arc::new(ChunkIndex::from_chunks(metas));
    let provider = Arc::new(VesselTableProvider::new(index));

    let ctx = SessionContext::new();
    ctx.register_table("vessel_positions", provider)
        .expect("register table");

    CHUNKS_READ.store(0, Ordering::SeqCst);
    let df = ctx
        .sql("SELECT COUNT(*) FROM vessel_positions")
        .await
        .expect("plan");
    df.collect().await.expect("execute");

    assert_eq!(
        CHUNKS_READ.load(Ordering::SeqCst),
        5,
        "unfiltered query should open every chunk"
    );
}
