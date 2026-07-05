//! `ChunkMeta` — a lightweight description of one Arrow IPC chunk file.
//!
//! A `ChunkMeta` is everything Tara needs to decide whether to open a chunk
//! during query execution, without actually opening it. The index is built
//! entirely from `ChunkMeta` values; the data files are only opened when a
//! chunk survives pruning.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Metadata describing one Arrow IPC chunk file.
///
/// All time values are microseconds since the Unix epoch in UTC, matching
/// the `timestamp_us` column in the Arrow schema.
///
/// `h3_cells` contains the set of H3 resolution-5 cell IDs that cover
/// every position in this chunk. Resolution 5 gives cells roughly 250km²
/// each — coarse enough to keep the cell set small per chunk, fine enough
/// to prune meaningfully for regional queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    /// Path to the `.arrow` file, relative to the repo root.
    pub path: PathBuf,

    /// Calendar date this chunk belongs to, e.g. `"2026-06-10"`.
    /// Derived from the parent directory name during index build.
    pub date: String,

    /// Earliest `timestamp_us` value in this chunk.
    pub time_min_us: i64,

    /// Latest `timestamp_us` value in this chunk.
    pub time_max_us: i64,

    /// Number of distinct MMSI values in this chunk.
    pub mmsi_count: u32,

    /// Total number of rows in this chunk.
    pub row_count: u32,

    /// H3 resolution-5 cell IDs covering all positions in this chunk.
    /// Used for spatial pruning: if none of these cells overlap the query
    /// region, the chunk can be skipped without opening the file.
    pub h3_cells: Vec<u64>,
}

impl ChunkMeta {
    /// Returns true if this chunk's time range overlaps `[start_us, end_us]`.
    /// This is the primary pruning predicate for time-range queries.
    #[inline]
    pub fn overlaps_time(&self, start_us: i64, end_us: i64) -> bool {
        self.time_min_us <= end_us && self.time_max_us >= start_us
    }

    /// Returns true if this chunk contains any position in the given H3 cells.
    /// Used as a secondary pruning predicate after time overlap is confirmed.
    #[inline]
    pub fn overlaps_cells(&self, query_cells: &[u64]) -> bool {
        // For the cell counts we expect (tens to low hundreds per chunk),
        // a linear scan is faster than building a HashSet per call.
        self.h3_cells.iter().any(|c| query_cells.contains(c))
    }

    /// Returns true if this chunk's time range overlaps AND it covers at
    /// least one of the given H3 cells. Used for spatiotemporal queries.
    #[inline]
    pub fn overlaps_spatiotemporal(&self, start_us: i64, end_us: i64, cells: &[u64]) -> bool {
        self.overlaps_time(start_us, end_us) && self.overlaps_cells(cells)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(time_min: i64, time_max: i64, cells: Vec<u64>) -> ChunkMeta {
        ChunkMeta {
            path: PathBuf::from("test/chunk.arrow"),
            date: "2026-06-10".into(),
            time_min_us: time_min,
            time_max_us: time_max,
            mmsi_count: 10,
            row_count: 1000,
            h3_cells: cells,
        }
    }

    #[test]
    fn test_time_overlap_standard() {
        let meta = make_meta(100, 200, vec![]);
        assert!(meta.overlaps_time(150, 300)); // query starts inside chunk
        assert!(meta.overlaps_time(50, 150)); // query ends inside chunk
        assert!(meta.overlaps_time(50, 300)); // chunk fully inside query
        assert!(meta.overlaps_time(100, 200)); // exact match
    }

    #[test]
    fn test_time_overlap_boundary() {
        let meta = make_meta(100, 200, vec![]);
        // Touching boundaries are overlapping — a query ending exactly at
        // chunk start, or starting exactly at chunk end, is inclusive.
        assert!(meta.overlaps_time(50, 100)); // query ends at chunk start
        assert!(meta.overlaps_time(200, 300)); // query starts at chunk end
    }

    #[test]
    fn test_time_no_overlap() {
        let meta = make_meta(100, 200, vec![]);
        assert!(!meta.overlaps_time(201, 300)); // query entirely after
        assert!(!meta.overlaps_time(0, 99)); // query entirely before
    }

    #[test]
    fn test_cell_overlap() {
        let meta = make_meta(0, 1, vec![10, 20, 30]);
        assert!(meta.overlaps_cells(&[20, 40])); // 20 is shared
        assert!(!meta.overlaps_cells(&[40, 50])); // no overlap
        assert!(!meta.overlaps_cells(&[])); // empty query
    }

    #[test]
    fn test_spatiotemporal_requires_both() {
        let meta = make_meta(100, 200, vec![10, 20]);
        // Time matches, cells match → true
        assert!(meta.overlaps_spatiotemporal(150, 250, &[20]));
        // Time matches, cells don't → false
        assert!(!meta.overlaps_spatiotemporal(150, 250, &[99]));
        // Time doesn't, cells match → false
        assert!(!meta.overlaps_spatiotemporal(300, 400, &[20]));
    }
}
