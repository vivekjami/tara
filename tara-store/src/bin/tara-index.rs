//! `tara-index` — build or rebuild the Tara chunk index.
//!
//! Run this once after ingestion, and again whenever new days are added.
//!
//! Usage:
//! ```bash
//! cargo run --release --bin tara-index -- \
//!     --chunks data/chunks \
//!     --output data/chunks/tara.index
//! ```

use anyhow::Result;
use std::path::PathBuf;
use tara_store::builder::build_index;
use tracing::info;

fn main() -> Result<()> {
    tara_store::telemetry::init_telemetry("tara")?;

    let args: Vec<String> = std::env::args().collect();
    let chunks_dir = PathBuf::from(
        args.iter()
            .position(|a| a == "--chunks")
            .and_then(|i| args.get(i + 1))
            .unwrap_or(&"data/chunks".to_string()),
    );
    let output_path = PathBuf::from(
        args.iter()
            .position(|a| a == "--output")
            .and_then(|i| args.get(i + 1))
            .unwrap_or(&"data/chunks/tara.index".to_string()),
    );

    info!("Building index from {:?}", chunks_dir);
    let index = build_index(&chunks_dir)?;

    let stats = index.stats();
    info!(
        "Index complete: {} chunks, {} days, {} total rows",
        stats.chunk_count, stats.date_count, stats.total_rows
    );

    index.save(&output_path)?;
    info!("Index written to {:?}", output_path);

    Ok(())
}
