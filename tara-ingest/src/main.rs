//! # tara-ingest
//!
//! Ingests raw Danish AIS CSV data into Arrow IPC chunk files.
//!
//! ## Modes
//!
//! **Single file** — ingest one day's CSV:
//! ```bash
//! tara-ingest --input data/aisdk-2026-06-10/aisdk-2026-06-10.csv --output data/chunks
//! ```
//!
//! **Directory** — ingest all days found under a root directory:
//! ```bash
//! tara-ingest --input-dir data/ --output data/chunks
//! ```
//!
//! In both modes, output is written to `<output>/<YYYY-MM-DD>/chunk_NNNNNN.arrow`.
//! Re-running directory mode skips days whose output directory already exists,
//! making multi-day ingestion safely resumable.
//!
//! This is for the individual ingest step and bulk data for data folder
//! RUST_LOG=info cargo run --release --bin tara-ingest -- \
//!    --input data/aisdk-2026-06-10/aisdk-2026-06-10.csv \
//!    --output data/chunks
//!
//! Next one :
//! RUST_LOG=info cargo run --release --bin tara-ingest -- \
//!    --input-dir data/ \
//!    --output data/chunks
//!
//!

mod dedup;
mod parse;
mod writer;

use anyhow::{Result, bail};
use std::path::{Path, PathBuf};
use tracing::info;

// ── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(String::as_str) {
        Some("--input") => run_single_file(&args).await,
        Some("--input-dir") => run_directory(&args).await,
        _ => bail!(
            "Usage:\n  \
             tara-ingest --input <file.csv> --output <dir>\n  \
             tara-ingest --input-dir <data-dir> --output <dir>"
        ),
    }
}

// ── Modes ─────────────────────────────────────────────────────────────────────

/// Ingest a single CSV file.
/// The output directory is derived from the filename:
/// `aisdk-2026-06-10.csv` → `<output_root>/2026-06-10/`
async fn run_single_file(args: &[String]) -> Result<()> {
    let input = require_arg(args, "--input", 2)?;
    let output_root = optional_arg(args, "--output", 4, "data/chunks");

    let date = extract_date(&input)?;
    let output_dir = output_root.join(&date);
    std::fs::create_dir_all(&output_dir)?;

    info!("Mode: single file");
    info!("Input:  {:?}", input);
    info!("Output: {:?}", output_dir);

    writer::ingest_file(&input, &output_dir).await
}

/// Ingest all `aisdk-YYYY-MM-DD/` directories found under `input_dir`.
/// Directories are processed in chronological order (lexicographic sort on
/// YYYY-MM-DD names). Days whose output directory already exists are skipped,
/// so this mode is safely resumable after interruption.
async fn run_directory(args: &[String]) -> Result<()> {
    let input_dir = require_arg(args, "--input-dir", 2)?;
    let output_root = optional_arg(args, "--output", 4, "data/chunks");

    let day_dirs = collect_day_dirs(&input_dir)?;

    info!("Mode: directory ({} days found)", day_dirs.len());
    info!("Input root:  {:?}", input_dir);
    info!("Output root: {:?}", output_root);

    for day_dir in &day_dirs {
        ingest_one_day(day_dir, &output_root).await?;
    }

    info!("All days complete");
    Ok(())
}

// ── Per-day logic ─────────────────────────────────────────────────────────────

/// Ingest a single `aisdk-YYYY-MM-DD/` directory into `output_root/YYYY-MM-DD/`.
/// Skips silently if the output directory already exists and is non-empty.
async fn ingest_one_day(day_dir: &Path, output_root: &Path) -> Result<()> {
    // The date string lives in the directory name: "aisdk-2026-06-10" → "2026-06-10"
    let date = day_dir
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.trim_start_matches("aisdk-"))
        .unwrap_or("unknown")
        .to_string();

    let output_dir = output_root.join(&date);

    // Resumability: skip if already done
    if output_dir.exists() && std::fs::read_dir(&output_dir)?.next().is_some() {
        info!("Skipping {} — already ingested", date);
        return Ok(());
    }

    // The CSV name mirrors the directory name: aisdk-2026-06-10/aisdk-2026-06-10.csv
    let csv_path = day_dir.join(format!("aisdk-{}.csv", date));

    if !csv_path.exists() {
        info!("Skipping {} — CSV not found at {:?}", date, csv_path);
        return Ok(());
    }

    std::fs::create_dir_all(&output_dir)?;
    info!("Ingesting {} ...", date);
    writer::ingest_file(&csv_path, &output_dir).await
}

// ── Directory discovery ───────────────────────────────────────────────────────

/// Collect all `aisdk-*` subdirectories under `root`, sorted chronologically.
/// Non-matching entries (e.g. the `chunks/` output directory) are silently ignored.
fn collect_day_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(root)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("aisdk-"))
                    .unwrap_or(false)
        })
        .collect();

    if dirs.is_empty() {
        bail!("No aisdk-* directories found in {:?}", root);
    }

    // Lexicographic sort = chronological order because names are YYYY-MM-DD
    dirs.sort();
    Ok(dirs)
}

// ── Argument helpers ──────────────────────────────────────────────────────────

/// Return the argument at `index` as a `PathBuf`, or error with a message
/// referencing `flag` so the user knows which flag was missing.
fn require_arg(args: &[String], flag: &str, index: usize) -> Result<PathBuf> {
    args.get(index)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("{} requires a path argument", flag))
}

/// Return the argument at `index` as a `PathBuf`, or fall back to `default`.
fn optional_arg(args: &[String], _flag: &str, index: usize, default: &str) -> PathBuf {
    args.get(index)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default))
}

// ── Date extraction ───────────────────────────────────────────────────────────

/// Extract "YYYY-MM-DD" from a CSV path.
///
/// ```text
/// data/aisdk-2026-06-10/aisdk-2026-06-10.csv  →  "2026-06-10"
/// ```
///
/// Fails fast with a clear message if the filename does not match the expected
/// `aisdk-YYYY-MM-DD.csv` pattern, rather than silently producing a wrong date.
fn extract_date(path: &Path) -> Result<String> {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("Cannot read filename stem from {:?}", path))?;

    let date = stem.strip_prefix("aisdk-").ok_or_else(|| {
        anyhow::anyhow!(
            "Filename {:?} does not match expected pattern aisdk-YYYY-MM-DD.csv",
            stem
        )
    })?;

    // Sanity-check the extracted date looks like YYYY-MM-DD before trusting it
    if date.len() != 10 || date.chars().nth(4) != Some('-') || date.chars().nth(7) != Some('-') {
        bail!("Extracted date {:?} does not look like YYYY-MM-DD", date);
    }

    Ok(date.to_string())
}

// ── Tests ─────────────────────────────────────────────────────────────────────
// Note that these are my tests and you might have you own path and filename conventions, so they may fail for you.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_date_standard_path() {
        let path = Path::new("data/aisdk-2026-06-10/aisdk-2026-06-10.csv");
        assert_eq!(extract_date(path).unwrap(), "2026-06-10");
    }

    #[test]
    fn test_extract_date_rejects_wrong_prefix() {
        let path = Path::new("data/something-2026-06-10.csv");
        assert!(extract_date(path).is_err());
    }

    #[test]
    fn test_extract_date_rejects_malformed_date() {
        let path = Path::new("data/aisdk-20260610/aisdk-20260610.csv");
        assert!(extract_date(path).is_err());
    }

    #[test]
    fn test_collect_day_dirs_sorts_chronologically() {
        #[allow(clippy::useless_vec)]
        // clippy suggests using an array literal here but the test intends to
        // call `sort()` on a mutable collection; silence the lint to keep
        // the test clear and stable.
        // Lexicographic order on YYYY-MM-DD is chronological order —
        // verify the sort produces the right sequence given known inputs.
        let mut dirs = vec![
            PathBuf::from("data/aisdk-2026-06-12"),
            PathBuf::from("data/aisdk-2026-06-10"),
            PathBuf::from("data/aisdk-2026-06-11"),
        ];
        dirs.sort();
        assert_eq!(dirs[0], PathBuf::from("data/aisdk-2026-06-10"));
        assert_eq!(dirs[1], PathBuf::from("data/aisdk-2026-06-11"));
        assert_eq!(dirs[2], PathBuf::from("data/aisdk-2026-06-12"));
    }
}
