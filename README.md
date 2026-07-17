# Tara

Vessel Trajectory Intelligence Engine

Tara is a Rust workspace for ingesting AIS vessel position data, storing it as Arrow IPC chunks, and querying it through DataFusion SQL with temporal and spatial pruning.

The core components are:

- `tara-ingest`: parses daily AIS CSV files into Arrow chunks.
- `tara-store`: persists chunk metadata and the chunk index.
- `tara-query`: exposes the chunk store to DataFusion and provides SQL helpers.
- `tara-server`: a thin HTTP entry point.

## Current status

The repository currently builds cleanly and passes tests, Clippy, and release builds on the real workspace data.

Verified commands:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
cargo test
```

## Real-data verification

The workspace includes a real indexed dataset under `data/chunks/`.

The current real-data index stats reported by the benchmark harness are:

- `index_chunks=1243`
- `index_rows=123600571`
- `index_dates=11`
- `context_load_ms=4.966`

A representative real-data query benchmark already measured on this workspace is:

- `pushdown_count`: `rows=1`, `min_ms=210.232`, `median_ms=222.733`, `mean_ms=221.194`, `max_ms=230.617`
- `gap_detection`: `rows=242`, `min_ms=13315.045`, `median_ms=13624.056`, `mean_ms=14270.222`, `max_ms=15871.566`
- `density_query`: `rows=1083`, `min_ms=13671.149`, `median_ms=13701.285`, `mean_ms=13729.496`, `max_ms=13816.054`

The benchmark harness is implemented in `tara-query/src/bin/tara-benchmark.rs` and can be run directly against the real chunk index.

## Reproduce benchmarks

Use these commands from the repository root:

```bash
cargo run -q -p tara-query --bin tara-benchmark -- --benchmark pushdown --iterations 3 --warmup 1
cargo run -q -p tara-query --bin tara-benchmark -- --benchmark gap --iterations 3 --warmup 1
cargo run -q -p tara-query --bin tara-benchmark -- --benchmark density --iterations 3 --warmup 1
```

For a full run of all three queries in one pass:

```bash
cargo run -q -p tara-query --bin tara-benchmark -- --iterations 3 --warmup 1
```

On the current workspace data, the full run completes in roughly 30 seconds.

## Query example

```sql
SELECT mmsi, gap_start_us, gap_end_us, gap_us
FROM (
	SELECT
		mmsi,
		arrow_cast(LAG(timestamp_us) OVER (PARTITION BY mmsi ORDER BY timestamp_us), 'Int64') AS gap_start_us,
		arrow_cast(timestamp_us, 'Int64') AS gap_end_us,
		arrow_cast(timestamp_us, 'Int64')
			- arrow_cast(LAG(timestamp_us) OVER (PARTITION BY mmsi ORDER BY timestamp_us), 'Int64') AS gap_us
	FROM vessel_positions
) t
WHERE gap_us > 21600000000
ORDER BY gap_us DESC;
```

## Notes

- The project documentation in `.idea/ARCHITECTURE.md` and `.idea/PHASES.md` explains the design and remaining hardening scope.
- Some test helpers still use `unwrap()` internally; those are test-only and remain part of the final hardening pass.
