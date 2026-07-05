//! Index builder — scans ingested Arrow IPC chunks and computes `ChunkMeta`.
//!
//! This is run once (or after each new day is ingested) to produce the
//! `tara.index` file that the query server loads at startup.
//!
//! For each chunk file the builder:
//!   1. Opens the Arrow IPC file and reads all record batches
//!   2. Scans the `timestamp_us` column for min/max
//!   3. Counts distinct MMSIs
//!   4. Converts every (lat, lon) pair to an H3 resolution-5 cell
//!   5. Deduplicates the cell set
//!   6. Writes a `ChunkMeta` without keeping the row data in memory

use crate::chunk::ChunkMeta;
use crate::index::ChunkIndex;
use anyhow::{Context, Result};
use arrow::array::{Float64Array, TimestampMicrosecondArray, UInt32Array};
use arrow::ipc::reader::FileReader;
use h3o::{CellIndex, LatLng, Resolution};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// H3 resolution used for spatial indexing.
/// Resolution 5 → cells ~252km² each, ~289km edge-to-edge.
/// Fine enough to prune regional queries; coarse enough that each chunk
/// covers only a small number of cells, keeping index size small.
const H3_RESOLUTION: Resolution = Resolution::Five;

/// Scan all chunk files under `chunks_root` and build a `ChunkIndex`.
///
/// `chunks_root` is expected to contain per-date subdirectories, each
/// containing `.arrow` files — the structure produced by `tara-ingest`.
///
/// ```text
/// chunks_root/
/// ├── 2026-06-10/
/// │   ├── chunk_000000.arrow
/// │   └── ...
/// └── 2026-06-11/
///     └── ...
/// ```
pub fn build_index(chunks_root: &Path) -> Result<ChunkIndex> {
    let chunk_files = discover_chunks(chunks_root)?;
    info!("Building index over {} chunk files", chunk_files.len());

    let mut metas: Vec<ChunkMeta> = Vec::with_capacity(chunk_files.len());

    for (i, (path, date)) in chunk_files.iter().enumerate() {
        if (i + 1) % 100 == 0 {
            info!("  Processed {}/{} chunks", i + 1, chunk_files.len());
        }
        match build_chunk_meta(path, date) {
            Ok(meta) => metas.push(meta),
            Err(e) => warn!("Skipping {:?}: {}", path, e),
        }
    }

    info!(
        "Index built: {} chunks, {} dates",
        metas.len(),
        metas.iter().map(|m| m.date.as_str()).collect::<HashSet<_>>().len()
    );

    Ok(ChunkIndex::from_chunks(metas))
}

/// Compute `ChunkMeta` for a single Arrow IPC file.
/// Opens the file, scans every record batch, then closes it.
/// Peak memory = one batch at a time (100k rows × ~92 bytes ≈ 9MB).
fn build_chunk_meta(path: &Path, date: &str) -> Result<ChunkMeta> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("Cannot open {:?}", path))?;

    let reader = FileReader::try_new(file, None)
        .with_context(|| format!("Not a valid Arrow IPC file: {:?}", path))?;

    let mut time_min = i64::MAX;
    let mut time_max = i64::MIN;
    let mut mmsi_set: HashSet<u32> = HashSet::new();
    let mut cell_set: HashSet<u64> = HashSet::new();
    let mut row_count: u32 = 0;

    for batch_result in reader {
        let batch = batch_result
            .with_context(|| format!("Error reading batch from {:?}", path))?;

        row_count += batch.num_rows() as u32;

        // ── Timestamp min/max ─────────────────────────────────────────────
        // Column index 1 = timestamp_us (see schema.rs for column order)
        let ts_col = batch
    .column(1)
    .as_any()
    .downcast_ref::<TimestampMicrosecondArray>()
    .with_context(|| "timestamp_us column is not TimestampMicrosecond — schema mismatch")?;

for i in 0..ts_col.len() {
    let val = ts_col.value(i);
    if val < time_min { time_min = val; }
    if val > time_max { time_max = val; }
}

        // ── Distinct MMSIs ────────────────────────────────────────────────
        // Column index 0 = mmsi
        let mmsi_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .with_context(|| "mmsi column is not UInt32")?;

        for val in mmsi_col.values().iter() {
            mmsi_set.insert(*val);
        }

        // ── H3 cell coverage ─────────────────────────────────────────────
        // Columns 2 and 3 = latitude, longitude
        let lat_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .with_context(|| "latitude column is not Float64")?;

        let lon_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .with_context(|| "longitude column is not Float64")?;

        for i in 0..batch.num_rows() {
            let lat = lat_col.value(i);
            let lon = lon_col.value(i);

            if let Ok(ll) = LatLng::new(lat, lon) {
                let cell: CellIndex = ll.to_cell(H3_RESOLUTION);
                cell_set.insert(u64::from(cell));
            }
        }
    }

    if row_count == 0 {
        anyhow::bail!("Chunk file is empty");
    }

    

    Ok(ChunkMeta {
        path: path.to_path_buf(),
        date: date.to_string(),
        time_min_us: time_min,
        time_max_us: time_max,
        mmsi_count: mmsi_set.len() as u32,
        row_count,
        h3_cells: cell_set.into_iter().collect(),
    })
}

/// Walk `chunks_root` and return all `.arrow` files with their date string.
/// Returns pairs of `(absolute_path, "YYYY-MM-DD")`.
/// Files are sorted by (date, filename) so index order is deterministic.
fn discover_chunks(chunks_root: &Path) -> Result<Vec<(PathBuf, String)>> {
    let mut results: Vec<(PathBuf, String)> = Vec::new();

    for entry in std::fs::read_dir(chunks_root)? {
        let entry = entry?;
        let dir_path = entry.path();

        if !dir_path.is_dir() {
            continue; // skip tara.index and any other files at root level
        }

        let date = dir_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        // Skip directories that don't look like YYYY-MM-DD
        if date.len() != 10 || date.chars().nth(4) != Some('-') {
            continue;
        }

        for chunk_entry in std::fs::read_dir(&dir_path)? {
            let chunk_entry = chunk_entry?;
            let chunk_path = chunk_entry.path();

            if chunk_path.extension().and_then(|e| e.to_str()) == Some("arrow") {
                results.push((chunk_path, date.clone()));
            }
        }
    }

    if results.is_empty() {
        anyhow::bail!("No .arrow files found under {:?}", chunks_root);
    }

    // Sort by (date, path) for deterministic index ordering
    results.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
    Ok(results)
}