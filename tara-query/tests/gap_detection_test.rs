//! Validates gap_detection_query() against hand-constructed sequences with
//! known, intentionally-designed gaps — independent of real-world data
//! noise. See PHASES.md Phase 5.

use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int64Type;
use datafusion::prelude::SessionContext;

use tara_query::provider::VesselTableProvider;
use tara_query::queries::gap_detection_query;
use tara_store::index::ChunkIndex;

mod common;
use common::write_synthetic_chunk;

const SIX_HOURS_US: i64 = 21_600_000_000;
const ONE_HOUR_US: i64 = 3_600_000_000;
const FIVE_MIN_US: i64 = 300_000_000;

#[tokio::test]
async fn gap_detection_matches_hand_designed_sequences() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = 1_781_000_000_000_000i64;

    let mut metas = Vec::new();
    let mut chunk_n = 0;
    let mut write = |mmsi: u32, ts: i64| {
        let name = format!("chunk_{chunk_n}.arrow");
        chunk_n += 1;
        metas.push(write_synthetic_chunk(dir.path(), &name, mmsi, ts, ts + 1));
    };

    // MMSI 100: regular 5-min reports, then ONE deliberate 8-hour gap,
    // then reporting resumes. Expect exactly one flagged gap: 8 hours,
    // between the 4th report and the 5th.
    let mmsi_100_reports = [
        base,
        base + FIVE_MIN_US,
        base + 2 * FIVE_MIN_US,
        base + 3 * FIVE_MIN_US,
        base + 3 * FIVE_MIN_US + 8 * ONE_HOUR_US, // <- 8h gap from previous
        base + 3 * FIVE_MIN_US + 8 * ONE_HOUR_US + FIVE_MIN_US,
    ];
    for &ts in &mmsi_100_reports {
        write(100, ts);
    }

    // MMSI 200: perfectly regular hourly reports, max gap 1h — well under
    // the 6h threshold. Expect ZERO flagged gaps (no false positives).
    for i in 0..5 {
        write(200, base + i * ONE_HOUR_US);
    }

    // MMSI 300: exactly one report, ever. Expect ZERO flagged gaps —
    // LAG() has nothing to compare against for the only row in its
    // partition, and this must not crash or produce a phantom gap.
    write(300, base);

    let index = Arc::new(ChunkIndex::from_chunks(metas));
    let provider = Arc::new(VesselTableProvider::new(index));

    let ctx = SessionContext::new();
    ctx.register_table("vessel_positions", provider)
        .expect("register table");

    let sql = gap_detection_query(SIX_HOURS_US);
    let df = ctx.sql(&sql).await.expect("plan gap detection query");
    let batches = df.collect().await.expect("execute gap detection query");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "expected exactly one flagged gap across all three vessels (mmsi 100's 8h gap only)"
    );

    // Locate the single result row and check its shape, not just its count.
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one non-empty batch");

    let mmsi_col = batch
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::UInt32Type>();
    let gap_start_col = batch.column(1).as_primitive::<Int64Type>();
    let gap_end_col = batch.column(2).as_primitive::<Int64Type>();
    let gap_us_col = batch.column(3).as_primitive::<Int64Type>();

    assert_eq!(
        mmsi_col.value(0),
        100,
        "flagged gap should belong to mmsi 100"
    );
    assert_eq!(
        gap_start_col.value(0),
        base + 3 * FIVE_MIN_US,
        "gap_start_us should be the last report before the gap"
    );
    assert_eq!(
        gap_end_col.value(0),
        base + 3 * FIVE_MIN_US + 8 * ONE_HOUR_US,
        "gap_end_us should be the first report after the gap"
    );
    assert_eq!(
        gap_us_col.value(0),
        8 * ONE_HOUR_US,
        "gap_us should be exactly 8 hours"
    );
}
