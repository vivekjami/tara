//! # tara-store
//!
//! The core storage and indexing layer for Tara.
//!
//! Provides:
//! - [`schema`] — the canonical Arrow schema for AIS position records
//! - [`chunk`] — the `ChunkMeta` type describing one Arrow IPC file
//! - [`index`] — the `ChunkIndex` that answers range queries without opening data files
//! - [`builder`] — scans ingested chunks on disk and builds the index

pub mod builder;
pub mod chunk;
pub mod index;
pub mod schema;
pub mod telemetry;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
