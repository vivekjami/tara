//! `ChunkIndex` — the in-memory index over all chunk metadata.
//!
//! The index is the answer to "which chunks do I need to open for this query?"
//! It holds one `ChunkMeta` per Arrow IPC file and exposes range-query methods
//! that return only the chunks that could contain matching rows.
//!
//! The index is built once by [`crate::builder`], serialized to
//! `data/chunks/tara.index` as JSON, and loaded at server startup.
//! Query execution never touches the index file again — it lives in RAM.

use crate::chunk::ChunkMeta;
use anyhow::Result;
use std::path::Path;

/// In-memory index over all ingested chunk files.
#[derive(Debug, Clone)]
pub struct ChunkIndex {
    chunks: Vec<ChunkMeta>,
}

impl ChunkIndex {
    /// Build an index from a pre-computed list of chunk metadata.
    /// Called by [`crate::builder`] after scanning chunk files.
    pub fn from_chunks(chunks: Vec<ChunkMeta>) -> Self {
        Self { chunks }
    }

    /// Load a previously serialized index from a JSON file.
    pub fn load(path: &Path) -> Result<Self> {
        let json = std::fs::read_to_string(path)?;
        let chunks: Vec<ChunkMeta> = serde_json::from_str(&json)?;
        Ok(Self { chunks })
    }

    /// Serialize the index to a JSON file.
    /// The file is written atomically: to a `.tmp` file first, then renamed,
    /// so a partial write never leaves a corrupted index on disk.
    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(&self.chunks)?;
        let tmp = path.with_extension("index.tmp");
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Total number of chunks in the index.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Return all chunks whose time range overlaps `[start_us, end_us]`.
    ///
    /// This is a linear scan over chunk metadata — no data files are opened.
    /// At 1,177 chunks, a linear scan takes nanoseconds and is faster than
    /// any tree structure given the small index size.
    pub fn query_time_range(&self, start_us: i64, end_us: i64) -> Vec<&ChunkMeta> {
        self.chunks
            .iter()
            .filter(|m| m.overlaps_time(start_us, end_us))
            .collect()
    }

    /// Return chunks whose time range overlaps AND which cover at least one
    /// of the given H3 cells.
    pub fn query_spatiotemporal(
        &self,
        start_us: i64,
        end_us: i64,
        cells: &[u64],
    ) -> Vec<&ChunkMeta> {
        self.chunks
            .iter()
            .filter(|m| m.overlaps_spatiotemporal(start_us, end_us, cells))
            .collect()
    }

    /// Return all chunks belonging to a specific calendar date.
    /// Used by the ingestor to check which days are already indexed.
    pub fn chunks_for_date(&self, date: &str) -> Vec<&ChunkMeta> {
        self.chunks.iter().filter(|m| m.date == date).collect()
    }

    /// Summary statistics for logging and diagnostics.
    pub fn stats(&self) -> IndexStats {
        let total_rows: u64 = self.chunks.iter().map(|m| m.row_count as u64).sum();
        let dates: std::collections::HashSet<&str> =
            self.chunks.iter().map(|m| m.date.as_str()).collect();
        IndexStats {
            chunk_count: self.chunks.len(),
            total_rows,
            date_count: dates.len(),
        }
    }

    /// Return references to all chunks — used when no filter can be pushed down.
    pub fn all_chunks(&self) -> Vec<&ChunkMeta> {
        self.chunks.iter().collect()
    }
}

/// Summary statistics returned by [`ChunkIndex::stats`].
#[derive(Debug)]
pub struct IndexStats {
    pub chunk_count: usize,
    pub total_rows: u64,
    pub date_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn meta(time_min: i64, time_max: i64, date: &str, cells: Vec<u64>) -> ChunkMeta {
        ChunkMeta {
            path: PathBuf::from(format!("chunks/{}/chunk.arrow", date)),
            date: date.into(),
            time_min_us: time_min,
            time_max_us: time_max,
            mmsi_count: 5,
            row_count: 100,
            h3_cells: cells,
        }
    }

    fn test_index() -> ChunkIndex {
        ChunkIndex::from_chunks(vec![
            meta(0, 1000, "2026-06-10", vec![1, 2]),
            meta(800, 1800, "2026-06-10", vec![2, 3]),
            meta(2000, 3000, "2026-06-11", vec![4, 5]),
        ])
    }

    #[test]
    fn test_time_range_query_returns_overlapping_chunks() {
        let idx = test_index();
        let result = idx.query_time_range(900, 1500);
        // Chunk 0 (0–1000) and chunk 1 (800–1800) both overlap 900–1500
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_time_range_query_excludes_non_overlapping() {
        let idx = test_index();
        let result = idx.query_time_range(5000, 9000);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_spatiotemporal_prunes_by_cell() {
        let idx = test_index();
        // Time range hits chunks 0 and 1, but only chunk 1 has cell 3
        let result = idx.query_spatiotemporal(900, 1500, &[3]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].h3_cells, vec![2, 3]);
    }

    #[test]
    fn test_chunks_for_date() {
        let idx = test_index();
        assert_eq!(idx.chunks_for_date("2026-06-10").len(), 2);
        assert_eq!(idx.chunks_for_date("2026-06-11").len(), 1);
        assert_eq!(idx.chunks_for_date("2026-06-12").len(), 0);
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let idx = test_index();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tara.index");
        idx.save(&path).unwrap();
        let loaded = ChunkIndex::load(&path).unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.query_time_range(0, 500).len(), 1);
    }
}
