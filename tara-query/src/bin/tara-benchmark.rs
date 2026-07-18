use std::env;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use datafusion::logical_expr::ScalarUDF;
use tara_query::context::TaraContext;
use tara_query::h3_udf::H3CellRes5;
use tara_query::queries::density_query;
use tara_store::index::ChunkIndex;

#[tokio::main]
async fn main() -> Result<()> {
    tara_store::telemetry::init_telemetry("tara")?;

    let config = BenchmarkConfig::from_args()?;
    let index = ChunkIndex::load(&config.index_path)
        .with_context(|| format!("failed to load index at {:?}", config.index_path))?;
    let windows = BenchmarkWindows::from_index(&index)?;

    let index_stats = index.stats();
    println!(
        "index_chunks={}, index_rows={}, index_dates={}",
        index_stats.chunk_count, index_stats.total_rows, index_stats.date_count
    );

    let load_started = Instant::now();
    let ctx = TaraContext::new(index)
        .await
        .context("failed to initialize TaraContext")?;
    let load_elapsed = load_started.elapsed();
    println!("context_load_ms={:.3}", load_elapsed.as_secs_f64() * 1000.0);

    ctx.session.register_udf(ScalarUDF::from(H3CellRes5::new()));

    let pushdown_query = pushdown_count_query(windows.pushdown_start_us, windows.pushdown_end_us);
    let gap_query = gap_detection_query_with_window(
        6 * 60 * 60 * 1_000_000,
        windows.gap_start_us,
        windows.gap_end_us,
    );
    let density_sql = density_query(windows.density_start_us, windows.density_end_us);

    match config.benchmark {
        BenchmarkKind::All => {
            bench_query(
                &ctx,
                "pushdown_count",
                &pushdown_query,
                config.warmup_runs,
                config.iterations,
            )
            .await?;
            bench_query(
                &ctx,
                "gap_detection",
                &gap_query,
                config.warmup_runs,
                config.iterations,
            )
            .await?;
            bench_query(
                &ctx,
                "density_query",
                &density_sql,
                config.warmup_runs,
                config.iterations,
            )
            .await?;
        }
        BenchmarkKind::PushdownCount => {
            bench_query(
                &ctx,
                "pushdown_count",
                &pushdown_query,
                config.warmup_runs,
                config.iterations,
            )
            .await?;
        }
        BenchmarkKind::GapDetection => {
            bench_query(
                &ctx,
                "gap_detection",
                &gap_query,
                config.warmup_runs,
                config.iterations,
            )
            .await?;
        }
        BenchmarkKind::DensityQuery => {
            bench_query(
                &ctx,
                "density_query",
                &density_sql,
                config.warmup_runs,
                config.iterations,
            )
            .await?;
        }
    }

    Ok(())
}

struct BenchmarkConfig {
    index_path: PathBuf,
    iterations: usize,
    warmup_runs: usize,
    benchmark: BenchmarkKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BenchmarkKind {
    All,
    PushdownCount,
    GapDetection,
    DensityQuery,
}

impl BenchmarkConfig {
    fn from_args() -> Result<Self> {
        let args: Vec<String> = env::args().collect();
        let mut index_path = PathBuf::from("data/chunks/tara.index");
        let mut iterations = 5usize;
        let mut warmup_runs = 1usize;
        let mut benchmark = BenchmarkKind::All;

        let mut i = 1usize;
        while i < args.len() {
            match args[i].as_str() {
                "--index" => {
                    i += 1;
                    let value = args.get(i).context("--index requires a path")?;
                    index_path = PathBuf::from(value);
                }
                "--iterations" => {
                    i += 1;
                    let value = args.get(i).context("--iterations requires a number")?;
                    iterations = value
                        .parse()
                        .with_context(|| format!("invalid iteration count: {}", value))?;
                }
                "--warmup" => {
                    i += 1;
                    let value = args.get(i).context("--warmup requires a number")?;
                    warmup_runs = value
                        .parse()
                        .with_context(|| format!("invalid warmup count: {}", value))?;
                }
                "--benchmark" => {
                    i += 1;
                    let value = args.get(i).context("--benchmark requires a value")?;
                    benchmark = match value.as_str() {
                        "all" => BenchmarkKind::All,
                        "pushdown" => BenchmarkKind::PushdownCount,
                        "gap" => BenchmarkKind::GapDetection,
                        "density" => BenchmarkKind::DensityQuery,
                        _ => anyhow::bail!(
                            "unknown benchmark '{}'; expected all|pushdown|gap|density",
                            value
                        ),
                    };
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => {
                    anyhow::bail!("unknown argument: {}", other);
                }
            }
            i += 1;
        }

        Ok(Self {
            index_path,
            iterations,
            warmup_runs,
            benchmark,
        })
    }
}

async fn bench_query(
    ctx: &TaraContext,
    name: &str,
    sql: &str,
    warmup_runs: usize,
    iterations: usize,
) -> Result<()> {
    println!("starting benchmark={}", name);

    for _ in 0..warmup_runs {
        let _ = ctx.query(sql).await.context("warmup execution failed")?;
    }

    let mut samples_ms = Vec::with_capacity(iterations);
    let mut rows_returned = 0usize;

    for _ in 0..iterations {
        let started = Instant::now();
        let batches = ctx.query(sql).await.context("execution failed")?;
        rows_returned = batches.iter().map(|batch| batch.num_rows()).sum();
        samples_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }

    samples_ms.sort_by(f64::total_cmp);
    let min = samples_ms[0];
    let median = samples_ms[samples_ms.len() / 2];
    let max = samples_ms[samples_ms.len() - 1];
    let mean = samples_ms.iter().copied().sum::<f64>() / samples_ms.len() as f64;

    println!(
        "benchmark={}, iterations={}, rows={}, min_ms={:.3}, median_ms={:.3}, mean_ms={:.3}, max_ms={:.3}",
        name, iterations, rows_returned, min, median, mean, max
    );

    Ok(())
}

fn print_usage() {
    eprintln!(
        "Usage: tara-benchmark [--index data/chunks/tara.index] [--iterations N] [--warmup N] [--benchmark all|pushdown|gap|density]"
    );
}

struct BenchmarkWindows {
    pushdown_start_us: i64,
    pushdown_end_us: i64,
    gap_start_us: i64,
    gap_end_us: i64,
    density_start_us: i64,
    density_end_us: i64,
}

impl BenchmarkWindows {
    fn from_index(index: &ChunkIndex) -> Result<Self> {
        let mut chunks = index.all_chunks().into_iter();
        let first = chunks
            .next()
            .context("index is empty; cannot derive benchmark windows")?;

        let mut min_time = first.time_min_us;
        let mut max_time = first.time_max_us;

        for chunk in chunks {
            min_time = min_time.min(chunk.time_min_us);
            max_time = max_time.max(chunk.time_max_us);
        }

        let one_hour = 60 * 60 * 1_000_000i64;
        let six_hours = 6 * one_hour;
        let one_day = 24 * one_hour;

        let pushdown_start_us = min_time;
        let pushdown_end_us = (min_time + six_hours).min(max_time);
        let gap_start_us = min_time;
        let gap_end_us = (min_time + one_day).min(max_time);
        let density_start_us = min_time;
        let density_end_us = (min_time + one_day).min(max_time);

        Ok(Self {
            pushdown_start_us,
            pushdown_end_us,
            gap_start_us,
            gap_end_us,
            density_start_us,
            density_end_us,
        })
    }
}

fn pushdown_count_query(start_us: i64, end_us: i64) -> String {
    format!(
        "SELECT COUNT(*) AS row_count FROM vessel_positions WHERE timestamp_us >= arrow_cast({start_us}, 'Timestamp(Microsecond, \"UTC\")') AND timestamp_us <= arrow_cast({end_us}, 'Timestamp(Microsecond, \"UTC\")')"
    )
}

fn gap_detection_query_with_window(threshold_us: i64, start_us: i64, end_us: i64) -> String {
    format!(
        "SELECT mmsi, gap_start_us, gap_end_us, gap_us
         FROM (
             SELECT
                 mmsi,
                 arrow_cast(LAG(timestamp_us) OVER (PARTITION BY mmsi ORDER BY timestamp_us), 'Int64') AS gap_start_us,
                 arrow_cast(timestamp_us, 'Int64') AS gap_end_us,
                 arrow_cast(timestamp_us, 'Int64')
                     - arrow_cast(LAG(timestamp_us) OVER (PARTITION BY mmsi ORDER BY timestamp_us), 'Int64') AS gap_us
             FROM vessel_positions
             WHERE timestamp_us >= arrow_cast({start_us}, 'Timestamp(Microsecond, \"UTC\")')
               AND timestamp_us <= arrow_cast({end_us}, 'Timestamp(Microsecond, \"UTC\")')
         ) t
         WHERE gap_us > {threshold_us}
         ORDER BY gap_us DESC"
    )
}