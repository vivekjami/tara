use std::collections::HashSet;

/// Tracks (mmsi, timestamp_us) pairs seen so far within the current ingest run.
/// The dedup key is exactly what Phase 0 identified: same vessel, same second.
/// Memory cost: ~12 bytes per unique (mmsi, timestamp) pair.
/// For 17.8M rows with 37% duplicates → ~11M unique keys → ~132MB RAM worst case.
/// This is acceptable for a single-machine ingest of one day's data.
pub struct DedupFilter {
    seen: HashSet<(u32, i64)>,
}

impl DedupFilter {
    pub fn new() -> Self {
        // Pre-allocate for roughly the expected unique count to avoid rehashing
        Self {
            seen: HashSet::with_capacity(12_000_000),
        }
    }

    /// Returns true if this (mmsi, timestamp_us) is new — should be kept.
    /// Returns false if already seen — should be dropped.
    pub fn is_new(&mut self, mmsi: u32, timestamp_us: i64) -> bool {
        self.seen.insert((mmsi, timestamp_us))
    }

    pub fn _seen_count(&self) -> usize {
        self.seen.len()
    }
}
