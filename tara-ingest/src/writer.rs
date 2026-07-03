use crate::dedup::DedupFilter;
use crate::parse::{parse_row, rows_to_record_batch, AisRow};
use anyhow::Result;
use std::path::Path;
use tracing::info;

/// How many cleaned rows to accumulate before flushing a chunk to disk.
/// 1-hour time buckets at median 7s inter-report gap × 5,151 vessels
/// ≈ ~2.6M rows/hour total. We flush every 100k rows to keep memory bounded
/// and chunk files at a reasonable size. This will be replaced in Phase 2
/// with time-based chunking; for Phase 1 the goal is just correct output.
const BATCH_SIZE: usize = 100_000;

pub async fn ingest_file(input: &Path, output_dir: &Path) -> Result<()> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)       // we handle the header manually
        .flexible(true)           // some rows have trailing commas
        .from_path(input)?;

    // Read and clean the header line (the CSV has a leading '# ')
    let mut records = rdr.records();
    let raw_header = records.next()
        .ok_or_else(|| anyhow::anyhow!("Empty file"))??;
    let headers = {
        let mut h = csv::StringRecord::new();
        for field in raw_header.iter() {
            h.push_field(field.trim().trim_start_matches('#').trim());
        }
        h
    };

    let mut dedup = DedupFilter::new();
    let mut batch: Vec<AisRow> = Vec::with_capacity(BATCH_SIZE);
    let mut chunk_index: usize = 0;
    let mut total_read: u64 = 0;
    let mut total_written: u64 = 0;
    let mut total_skipped_filter: u64 = 0;
    let mut total_skipped_dedup: u64 = 0;

    for result in records {
        let record = result?;
        total_read += 1;

        if total_read % 1_000_000 == 0 {
            info!(
                "Progress: {}M rows read, {}M written, {} chunks flushed",
                total_read / 1_000_000,
                total_written / 1_000_000,
                chunk_index
            );
        }

        // Parse and filter
        let row = match parse_row(&record, &headers) {
            Some(r) => r,
            None => {
                total_skipped_filter += 1;
                continue;
            }
        };

        // Deduplicate
        if !dedup.is_new(row.mmsi, row.timestamp_us) {
            total_skipped_dedup += 1;
            continue;
        }

        total_written += 1;
        batch.push(row);

        if batch.len() >= BATCH_SIZE {
            flush_chunk(&batch, output_dir, chunk_index)?;
            chunk_index += 1;
            batch.clear();
        }
    }

    // Flush remaining rows
    if !batch.is_empty() {
        flush_chunk(&batch, output_dir, chunk_index)?;
        chunk_index += 1;
    }

    info!(
        "Ingest complete: {} rows read, {} written ({} filtered, {} deduped) in {} chunks",
        total_read, total_written, total_skipped_filter, total_skipped_dedup, chunk_index
    );

    Ok(())
}

fn flush_chunk(rows: &[AisRow], output_dir: &Path, index: usize) -> Result<()> {
    let batch = rows_to_record_batch(rows)?;
    let path = output_dir.join(format!("chunk_{:06}.arrow", index));

    // Write as Arrow IPC format (feather/arrow file) — fast, schema-preserving,
    // readable by any Arrow implementation. Replaced by Parquet in Phase 2.
    let file = std::fs::File::create(&path)?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(file, batch.schema().as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(())
}