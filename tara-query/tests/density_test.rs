//! Validates density_query() + the h3_cell_res5 UDF against hand-constructed
//! positions with expected H3 cells computed independently via h3o directly
//! — not by trusting the UDF's own output as ground truth. See PHASES.md
//! Phase 5.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{Int64Type, UInt64Type};
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;
use h3o::{LatLng, Resolution};

use tara_query::h3_udf::H3CellRes5;
use tara_query::provider::VesselTableProvider;
use tara_query::queries::density_query;
use tara_store::index::ChunkIndex;

mod common;

/// Two positions close enough to land in the SAME H3 res-5 cell (a few
/// hundred meters apart, well within a ~250km² cell), and a third position
/// far enough away (different ocean, different continent) to guarantee a
/// different cell — independent of exact H3 boundary math.
const POS_A: (f64, f64) = (48.4500, -5.0900); // Ushant area
const POS_B: (f64, f64) = (48.4510, -5.0910); // ~150m from POS_A, same region
const POS_C: (f64, f64) = (1.3000, 103.8000); // Singapore Strait — different cell, guaranteed

fn expected_cell(lat: f64, lon: f64) -> u64 {
    u64::from(
        LatLng::new(lat, lon)
            .expect("valid coords")
            .to_cell(Resolution::Five),
    )
}

#[tokio::test]
async fn density_query_matches_hand_designed_positions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = 1_781_000_000_000_000i64;

    // Reuse write_synthetic_chunk's timestamp/mmsi params; lat/lon in that
    // helper are currently hardcoded to POS_A-equivalent values (see
    // common.rs), so for this test we write chunks directly rather than
    // through the shared helper, to control lat/lon per-row.
    let mut metas = Vec::new();

    // 3 reports at/near POS_A (2 different vessels + 1 repeat of the first,
    // to exercise both report_count and COUNT(DISTINCT mmsi)):
    //   mmsi 100 @ POS_A, mmsi 100 @ POS_B (same cell, same vessel again),
    //   mmsi 200 @ POS_A (same cell, different vessel)
    // 1 report far away at POS_C: mmsi 300.
    let rows = [
        (100u32, POS_A),
        (100u32, POS_B),
        (200u32, POS_A),
        (300u32, POS_C),
    ];

    for (chunk_n, (mmsi, (lat, lon))) in rows.iter().enumerate() {
        let name = format!("chunk_{chunk_n}.arrow");
        let ts = base + chunk_n as i64 * 60_000_000; // 1 min apart, doesn't matter for this test
        metas.push(common::write_synthetic_chunk_at_position(
            dir.path(),
            &name,
            *mmsi,
            ts,
            *lat,
            *lon,
        ));
    }

    let index = Arc::new(ChunkIndex::from_chunks(metas));
    let provider = Arc::new(VesselTableProvider::new(index));

    let ctx = SessionContext::new();
    ctx.register_table("vessel_positions", provider)
        .expect("register table");
    ctx.register_udf(ScalarUDF::from(H3CellRes5::new()));

    let sql = density_query(base - 3_600_000_000, base + 3_600_000_000);
    let df = ctx.sql(&sql).await.expect("plan density query");
    let batches = df.collect().await.expect("execute density query");

    // Collect (cell -> (report_count, vessel_count)) from all batches.
    // NOTE: `cell` is UInt64 (our UDF's declared return_type), but
    // COUNT(*) and COUNT(DISTINCT ...) both return Int64 in DataFusion —
    // these are NOT the same type, despite counts always being
    // non-negative. Mixing them up here originally caused a downcast
    // panic ("primitive array") rather than a silent wrong answer, which
    // is the better failure mode, but worth calling out since it's an
    // easy assumption to get wrong again elsewhere.
    let mut results: HashMap<u64, (i64, i64)> = HashMap::new();
    for batch in &batches {
        let cells = batch.column(0).as_primitive::<UInt64Type>();
        let reports = batch.column(1).as_primitive::<Int64Type>();
        let vessels = batch.column(2).as_primitive::<Int64Type>();
        for i in 0..batch.num_rows() {
            results.insert(cells.value(i), (reports.value(i), vessels.value(i)));
        }
    }

    let cell_ab = expected_cell(POS_A.0, POS_A.1);
    let cell_c = expected_cell(POS_C.0, POS_C.1);

    assert_ne!(
        cell_ab, cell_c,
        "sanity check on the test itself: POS_A/B and POS_C must be different cells"
    );

    assert_eq!(
        results.len(),
        2,
        "expected exactly 2 distinct H3 cells in the results"
    );

    let (ab_reports, ab_vessels) = results
        .get(&cell_ab)
        .unwrap_or_else(|| panic!("expected cell {cell_ab} (POS_A/POS_B area) in results"));
    assert_eq!(
        *ab_reports, 3,
        "cell A/B should have 3 total reports (100@A, 100@B, 200@A)"
    );
    assert_eq!(
        *ab_vessels, 2,
        "cell A/B should have 2 distinct vessels (100 and 200)"
    );

    let (c_reports, c_vessels) = results
        .get(&cell_c)
        .unwrap_or_else(|| panic!("expected cell {cell_c} (POS_C, Singapore Strait) in results"));
    assert_eq!(*c_reports, 1, "cell C should have exactly 1 report");
    assert_eq!(
        *c_vessels, 1,
        "cell C should have exactly 1 distinct vessel"
    );
}
