# spectra-bench

Performance CLI for Spectra matrix scenarios and capacity experiments. Run write/query capacity baselines and smoke regression; compare adapter-direct vs full-stack paths (BM-SW0 vs BM-SW1).

## Role

- **Zero-loss paced durable:** BM-SW8 — L2 `PersistOverflow::Block`, `batch_max=2048`, paced `--offered-rate`; highest passing zero-drop rate
- **Batched durable ceiling:** BM-SW7 — L2 `PersistConfig` via `--batch-max`; unbounded `*_now` + `flush_persist`
- **Protocol floor:** BM-SW5/SW6 — single-row durable Spectra→DW
- **Enqueue write:** BM-SW0..SW4 — `achieved_*_ops_per_sec` (not durable on remote)
- **Query:** BM-SQ0..SQ3 — `query_*_ms` percentiles
- **Smoke:** BM-S0..S3 — scenario timings

Docs: [PERFORMANCE.md](../docs/bench/PERFORMANCE.md). Full experiment catalog and study notes come from AWS campaign runs.

## Usage

```bash
export CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=target-spectra-bench
cargo run -p spectra-bench -- experiments

# Smoke (not decision-grade)
cargo run -p spectra-bench -- run --experiment bm-sw1 --storage mem --topology embedded
```

### Remote backends (smoke against a URL)

```bash
export SPECTRA_CLICKHOUSE_URL=http://127.0.0.1:8123
cargo run -p spectra-bench --features clickhouse -- \
  run --experiment bm-sw7 --storage clickhouse --topology remote-ingest --batch-max 2048

# BM-SW8: one offered rate (local). Campaigns omit this flag and sweep until the first failing cell.
cargo run -p spectra-bench --release --features clickhouse -- \
  run --experiment bm-sw8 --storage clickhouse --topology remote-ingest --offered-rate 25000

# BM-SW8 embedded SQLite (same experiment; campaign cell uses this storage)
cargo run -p spectra-bench --release --features sqlite -- \
  run --experiment bm-sw8 --storage sqlite --topology embedded --offered-rate 80

export SPECTRA_TENSORBASE_URL=tcp://127.0.0.1:9528
cargo run -p spectra-bench --features tensorbase -- \
  run --experiment bm-sq1 --storage tensorbase --topology remote-ingest
```

Decision-grade AWS campaigns (co-located and multi-DW / BM-SW7) run on EC2 via the operator campaign. Runs write to `profiling/spectra-bench/reports/` as local scratch (gitignored). Decision-grade AWS results aren't committed to this repo.

## Sweep parameters

| Param | CLI flag | Env var | Default |
|-------|----------|---------|---------|
| Prefill depth | `--prefill` | `SPECTRA_BENCH_PREFILL` | per experiment |
| Prefill sweep | `--prefill-sweep` | — | `1000,10000,100000,1000000` |
| Query iterations | `--query-iters` | `SPECTRA_BENCH_QUERY_ITERS` | 1000 |
| Duration | `--duration-secs` | `SPECTRA_BENCH_DURATION_SECS` | 30 |
| Concurrency | `--concurrency` | `SPECTRA_BENCH_CONCURRENCY` | 256 |
| Bench clients | `--bench-clients` | `SPECTRA_BENCH_CLIENT_COUNT` | 1 |
| Client index | — | `SPECTRA_BENCH_CLIENT_INDEX` | 0 |
| DW count | — | `SPECTRA_BENCH_DW_N` | 1 |
| Batch max (SW7) | `--batch-max` | `SPECTRA_BENCH_BATCH_MAX` | capacity: 512 or 2048 |
| Offered rate (SW8) | `--offered-rate` | `SPECTRA_BENCH_OFFERED_RATE` | single cell; omit to sweep |
| Offered sweep (SW8) | — | `SPECTRA_BENCH_OFFERED_RATE_SWEEP` | `5000,10000,15000,20000,25000`; stop at first fail |
| Hardware label | — | `SPECTRA_BENCH_HARDWARE` | stamp `aws-t3-xlarge` on AWS |

## Report schema

```json
{
  "experiment": "bm-sw7",
  "matrix": { "storage": "clickhouse", "topology": "remote-ingest", "telemetry": "off" },
  "hardware": "aws-t3-xlarge",
  "batch_max": 2048,
  "path": "l2-batch",
  "durable_counter_ops_per_sec": 37974.4,
  "visibility_confirmed": true
}
```

BM-SW8 cells add `offered_rate`, `zero_loss`, `visibility_p95_ms`, `persist_queue_drops`, and `durable_rate_ratio` when present. A cell fails on any queue drop, adapter error, visibility timeout, or durable rate below 98% of offered rate. The campaign aggregate publishes `highest_passing_offered_rate`.

## Status

Capacity harness covers mem/sqlite/tensorbase/clickhouse. Decision-grade numbers are AWS campaign JSON only.
