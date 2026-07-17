//! Hand-written SQL queries proving the Phase 4 TableProvider is genuinely
//! composable — these are pure SQL, with no Rust-side post-processing of
//! results. See PHASES.md Phase 5.

/// Builds the gap-detection query: flags every consecutive pair of position
/// reports for the same vessel where the time between them exceeds
/// `threshold_us` (microseconds).
///
/// Implemented with `LAG()` over `PARTITION BY mmsi ORDER BY timestamp_us`.
/// The first report for each vessel has no prior row to compare against —
/// `LAG()` returns NULL for it, `NULL > threshold_us` evaluates to NULL
/// (not true), and the row is naturally excluded by the WHERE clause with
/// no special-casing required.
///
/// IMPORTANT: `timestamp_us` is `Timestamp(Microsecond, UTC)`, not a plain
/// integer. Subtracting two `Timestamp` values in SQL can produce an
/// `Interval` type rather than a raw microsecond count, which would silently
/// break both the `WHERE gap_us > {threshold_us}` comparison and any
/// downstream code expecting a plain integer. To avoid relying on implicit
/// coercion, every timestamp is explicitly cast to `Int64` (raw epoch
/// microseconds) via `arrow_cast` before arithmetic — this makes `gap_us`,
/// `gap_start_us`, and `gap_end_us` guaranteed plain integers, checkable by
/// a straightforward assertion in the validation test.
///
/// NOTE: threshold_us is trusted, internal, config-driven input (not
/// user-supplied), so plain string interpolation is fine here. If this
/// ever takes a value from an HTTP request body or similar, switch to
/// DataFusion bind parameters ($1 + with_param_values) instead.
pub fn gap_detection_query(threshold_us: i64) -> String {
    format!(
        "SELECT mmsi, gap_start_us, gap_end_us, gap_us
         FROM (
             SELECT
                 mmsi,
                 arrow_cast(LAG(timestamp_us) OVER (PARTITION BY mmsi ORDER BY timestamp_us), 'Int64') AS gap_start_us,
                 arrow_cast(timestamp_us, 'Int64') AS gap_end_us,
                 arrow_cast(timestamp_us, 'Int64')
                     - arrow_cast(LAG(timestamp_us) OVER (PARTITION BY mmsi ORDER BY timestamp_us), 'Int64') AS gap_us
             FROM vessel_positions
         ) t
         WHERE gap_us > {threshold_us}
         ORDER BY gap_us DESC"
    )
}

/// Builds the vessel-density query: for a given time window, counts total
/// position reports and distinct vessels per H3 resolution-5 cell.
///
/// Structurally different from `gap_detection_query` on purpose (per
/// PHASES.md Phase 5): this uses a scalar UDF (`h3_cell_res5`) + `GROUP BY`
/// rather than a window function, to prove the query layer supports more
/// than the one shape it was originally built around.
///
/// Requires `h3_cell_res5` to be registered on the `SessionContext` before
/// this SQL is run — see `crate::h3_udf::H3CellRes5`.
///
/// NOTE: same threshold/window caveat as gap_detection_query — start_us and
/// end_us are trusted, internal, config-driven values, so plain string
/// interpolation is used rather than bind parameters.
pub fn density_query(start_us: i64, end_us: i64) -> String {
    format!(
        "SELECT
             h3_cell_res5(latitude, longitude) AS cell,
             COUNT(*) AS report_count,
             COUNT(DISTINCT mmsi) AS vessel_count
         FROM vessel_positions
         WHERE timestamp_us >= arrow_cast({start_us}, 'Timestamp(Microsecond, \"UTC\")')
           AND timestamp_us <= arrow_cast({end_us}, 'Timestamp(Microsecond, \"UTC\")')
         GROUP BY cell
         ORDER BY vessel_count DESC"
    )
}
