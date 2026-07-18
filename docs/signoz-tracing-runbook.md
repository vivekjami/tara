# SigNoz Tracing Runbook for Tara

This runbook matches the current SigNoz installation flow and the current Tara workspace wiring. SigNoz now installs through Foundry; the old `install.sh` path is deprecated and should not be used.

The integration in Tara currently covers the pipeline pieces that exist in this repo:

- `tara-ingest` for CSV parse and chunk flush
- `tara-store` for index build
- `tara-query` for DataFusion query planning and execution
- `tara-query --bin tara-benchmark` for the benchmark harness

The query path records `chunks_scanned` and `chunks_total` on the trace spans so you can see pushdown effectiveness directly in SigNoz.

## 1. Start SigNoz with Foundry

Use the current self-host flow from SigNoz docs. On Windows, run this inside WSL 2 with Docker Engine available there.

```bash
curl -fsSL https://signoz.io/foundry.sh | bash

cat > casting.yaml <<'YAML'
apiVersion: v1alpha1
kind: Installation
metadata:
  name: signoz
spec:
  deployment:
    flavor: compose
    mode: docker
YAML

foundryctl cast -f casting.yaml
```

Copy the whole `casting.yaml` block exactly. Do not stop after `deployment:` or try to type the file from memory; the `name does not match any of the regexes: '^x-'` error is what you get when the generated Compose file is built from an incomplete or malformed YAML payload.

If you are reusing an older SigNoz deployment, follow the migration guide in `signoz/deploy/MIGRATION.md` instead of trying to mix the old install script with Foundry.

Expected endpoints:

- UI: `http://localhost:8080`
- OTLP gRPC: `localhost:4317`
- OTLP HTTP: `localhost:4318`

Verify the stack is up:

```bash
docker ps
```

Done when the SigNoz UI opens and the Traces page is available.

## 2. Confirm Tara still builds

Run this from the Tara repo root:

```bash
cd /home/vivek/tara
cargo check --workspace
```

Done when the workspace finishes cleanly.

## 3. Run the benchmark harness with tracing enabled

This is the most useful runtime check because it exercises the same query paths used for the current benchmark numbers in the repo.

```bash
cd /home/vivek/tara
RUST_LOG=info cargo run -p tara-query --bin tara-benchmark -- --iterations 3 --warmup 1
```

In SigNoz, filter on:

```text
service.name = tara
```

Then compare these traces:

- `pushdown_count`
- `gap_detection`
- `density_query`

What to check:

- `pushdown_count` should show a much smaller `chunks_scanned` value.
- `gap_detection` should usually scan far more chunks because the window function reduces pushdown benefit.
- `density_query` should be compared against `gap_detection` to see whether it has the same root cause or a different one.

## 4. Run the query CLI for a single traced SQL statement

Use this when you want one clear trace for a single query rather than the benchmark loop.

```bash
cd /home/vivek/tara
RUST_LOG=info cargo run -p tara-query --bin tara-query-cli -- --index data/chunks/tara.index --sql "SELECT COUNT(*) FROM vessel_positions"
```

This is the easiest way to confirm the query layer is emitting a single root span with the provider spans beneath it.

## 5. Trace ingest and index maintenance paths

These commands cover the rest of the pipeline.

```bash
cd /home/vivek/tara
RUST_LOG=info cargo run -p tara-ingest -- --input data/aisdk-2026-06-10/aisdk-2026-06-10.csv --output data/chunks

RUST_LOG=info cargo run -p tara-store --bin tara-index -- --chunks data/chunks --output data/chunks/tara.index
```

Use these when you want to verify the ingest and indexing spans in SigNoz as well.

## 6. If you need the legacy compose output

Foundry generates Compose files under `pours/deployment/`. If you want to inspect or run them directly, check the generated files rather than editing them by hand.

```bash
docker compose -f pours/deployment/compose.yaml ps
docker compose -f pours/deployment/compose.yaml logs -f signoz-signoz-0
```

## 7. What success looks like

A complete run should give you:

- one root span for the Tara binary you ran
- child spans for ingest, index build, or query execution
- `chunks_scanned` and `chunks_total` on the query spans
- an obvious difference between selective queries and window-function-heavy queries

If you do not see data in SigNoz, verify that something is listening on `localhost:4317` and that the Tara process can reach it from WSL.

