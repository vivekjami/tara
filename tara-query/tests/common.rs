//! Shared test helpers for tara-query integration tests.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{Float32Array, Float64Array, StringArray, TimestampMicrosecondArray, UInt16Array, UInt32Array};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;

use tara_query::provider::vessel_schema;
use tara_store::chunk::ChunkMeta;

/// Writes one synthetic Arrow IPC chunk file covering `[start_us, end_us)`,
/// containing a single row for `mmsi` at timestamp `start_us`.
///
/// Matches the real `ChunkMeta` in `tara-store::chunk` (no `Default` derive,
/// so every field is filled explicitly). `h3_cells` is left empty since
/// these tests only exercise time-range pushdown, not spatial pruning.
pub fn write_synthetic_chunk(dir: &std::path::Path, name: &str, mmsi: u32, start_us: i64, end_us: i64) -> ChunkMeta {
    let schema = vessel_schema();
    let path: PathBuf = dir.join(name);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![mmsi])),
            Arc::new(TimestampMicrosecondArray::from(vec![start_us]).with_timezone("UTC")),
            Arc::new(Float64Array::from(vec![48.45])),
            Arc::new(Float64Array::from(vec![-5.09])),
            Arc::new(Float32Array::from(vec![Some(12.3)])),
            Arc::new(Float32Array::from(vec![Some(88.0)])),
            Arc::new(UInt16Array::from(vec![Some(90)])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec![Some("Under way")])),
            Arc::new(StringArray::from(vec![Some("Cargo")])),
            Arc::new(StringArray::from(vec![Some("TEST VESSEL")])),
        ],
    )
    .expect("build synthetic RecordBatch");

    let file = std::fs::File::create(&path).expect("create chunk file");
    let mut writer = FileWriter::try_new(file, &schema).expect("create IPC writer");
    writer.write(&batch).expect("write batch");
    writer.finish().expect("finish IPC file");

    ChunkMeta {
        path,
        date: "2026-06-10".into(),
        time_min_us: start_us,
        time_max_us: end_us,
        mmsi_count: 1,
        row_count: 1,
        h3_cells: vec![],
    }
}

/// Like `write_synthetic_chunk`, but with an explicit (latitude, longitude)
/// instead of the hardcoded Ushant-area position. Needed for density-query
/// tests, which care about position, not just mmsi/timestamp.
#[allow(clippy::too_many_arguments)]
pub fn write_synthetic_chunk_at_position(
    dir: &std::path::Path,
    name: &str,
    mmsi: u32,
    start_us: i64,
    lat: f64,
    lon: f64,
) -> ChunkMeta {
    let schema = vessel_schema();
    let path: PathBuf = dir.join(name);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![mmsi])),
            Arc::new(TimestampMicrosecondArray::from(vec![start_us]).with_timezone("UTC")),
            Arc::new(Float64Array::from(vec![lat])),
            Arc::new(Float64Array::from(vec![lon])),
            Arc::new(Float32Array::from(vec![Some(12.3)])),
            Arc::new(Float32Array::from(vec![Some(88.0)])),
            Arc::new(UInt16Array::from(vec![Some(90)])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec![Some("Under way")])),
            Arc::new(StringArray::from(vec![Some("Cargo")])),
            Arc::new(StringArray::from(vec![Some("TEST VESSEL")])),
        ],
    )
    .expect("build synthetic RecordBatch");

    let file = std::fs::File::create(&path).expect("create chunk file");
    let mut writer = FileWriter::try_new(file, &schema).expect("create IPC writer");
    writer.write(&batch).expect("write batch");
    writer.finish().expect("finish IPC file");

    ChunkMeta {
        path,
        date: "2026-06-10".into(),
        time_min_us: start_us,
        time_max_us: start_us + 1,
        mmsi_count: 1,
        row_count: 1,
        h3_cells: vec![],
    }
}