//! Minimal CLI for running SQL queries against the Tara chunk store.
//!
//! Usage:
//! ```bash
//! cargo run --release --bin tara-query-cli -- \
//!     --index data/chunks/tara.index \
//!     --sql "SELECT COUNT(*) FROM vessel_positions"
//! ```

use anyhow::Result;
use std::path::Path;
use tara_query::context::TaraContext;
use tara_store::index::ChunkIndex;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();

    let index_path = args
        .iter()
        .position(|a| a == "--index")
        .and_then(|i| args.get(i + 1))
        .expect("Usage: tara-query-cli --index <path> --sql <query>");

    let sql = args
        .iter()
        .position(|a| a == "--sql")
        .and_then(|i| args.get(i + 1))
        .expect("Usage: tara-query-cli --index <path> --sql <query>");

    let index = ChunkIndex::load(Path::new(index_path))?;
    let ctx = TaraContext::new(index).await?;

    println!("Query: {}", sql);
    let batches = ctx.query(sql).await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("Result: {} rows in {} batches", total_rows, batches.len());

    if let Some(first) = batches.first() {
        let preview_rows = first.num_rows().min(10);
        println!("\nFirst {} rows:", preview_rows);
        for i in 0..preview_rows {
            let schema = first.schema();
            let mut row_parts = Vec::new();
            for col_idx in 0..first.num_columns() {
                let name = schema.field(col_idx).name();
                let arr = first.column(col_idx);
                // Use Arrow's debug representation for the single-row slice
                row_parts.push(format!("{}: {:?}", name, arr.slice(i, 1)));
            }
            println!("  [{}] {}", i, row_parts.join(", "));
        }
    }

    Ok(())
}
