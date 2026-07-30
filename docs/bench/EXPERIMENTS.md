# Spectra bench experiments

Pre-registered experiment IDs, sweep parameters, and runner commands.

**Methodology:** [PERFORMANCE_STUDY.md](PERFORMANCE_STUDY.md). **CLI:** [spectra-bench/README.md](../../spectra-bench/README.md).

Decision-grade capacity lives on **AWS** (`*-aws-t3-xlarge.json`). Local CLI runs are smoke only.

---

## Validation status

| Tier | Storage | Status | Environment |
|------|---------|--------|-------------|
| **Durable L2 batch (BM-SW7)** | clickhouse | **Measured** | `aws-t3-xlarge` multidw |
| **Enqueue + query baselines** | mem, sqlite, tensorbase, clickhouse | **Measured** | `aws-t3-xlarge` co-located |
| **Smoke regression** | mem, sqlite | **Shipped** (v0.1.0) | verify-release.sh + AWS |

JSON: [`profiling/spectra-bench/reports/`](../../profiling/spectra-bench/reports/).

---

## Matrix dimensions

| Dimension | Values |
|-----------|--------|
| `storage` | `mem`, `sqlite`, `tensorbase`, `clickhouse` |
| `transport` | `direct` (bench default) |
| `telemetry` | `off`, `console-ndjson` (needs `--features telemetry-console`) |
| `topology` | `embedded` (mem/sqlite), `remote-ingest` (tensorbase/clickhouse) |

Remote rows require `SPECTRA_TENSORBASE_URL` / `SPECTRA_CLICKHOUSE_URL` and **`--topology remote-ingest`**.

---

## Metric tracks

| Track | IDs | Role |
|-------|-----|------|
| Batched durable (primary) | BM-SW7 | Spectra→DW L2 batch capacity |
| Protocol floor | BM-SW5, BM-SW6 | Single-row durable shape (historical) |
| Enqueue write | BM-SW0..SW4 | Not durable on remote |
| Query | BM-SQ0..SQ3 | Prefill + timed query |
| Smoke | BM-S0..S3 | Regression timings |

---

## Sweep parameters

| Param | CLI flag | Env var | Default / notes |
|-------|----------|---------|-----------------|
| Prefill depth | `--prefill <N>` | `SPECTRA_BENCH_PREFILL` | experiment default |
| Prefill sweep | `--prefill-sweep <list>` | `SPECTRA_BENCH_PREFILL_SWEEP` | matrix script |
| Query iterations | `--query-iters <K>` | `SPECTRA_BENCH_QUERY_ITERS` | 1000 |
| Duration | `--duration-secs <S>` | `SPECTRA_BENCH_DURATION_SECS` | 30 |
| Concurrency | `--concurrency <C>` | `SPECTRA_BENCH_CONCURRENCY` | 256 |
| Bench clients | `--bench-clients <bc>` | `SPECTRA_BENCH_CLIENT_COUNT` | 1 |
| Client index | — | `SPECTRA_BENCH_CLIENT_INDEX` | 0 |
| DW instance count | — | `SPECTRA_BENCH_DW_N` | 1 |
| Shard URL | — | `SPECTRA_CLICKHOUSE_URL_{i}` / `SPECTRA_TENSORBASE_URL_{i}` | — |
| Host util JSON | — | `SPECTRA_BENCH_HOST_UTIL_JSON` | optional |
| Binding tier override | — | `SPECTRA_BENCH_BINDING_TIER` | `dw` / `client-cpu` / `unset` |
| Hardware label | — | `SPECTRA_BENCH_HARDWARE` | stamp `aws-t3-xlarge` on AWS |
| L2 batch_max (BM-SW7) | `--batch-max` | `SPECTRA_BENCH_BATCH_MAX` | capacity: **512 or 2048** |
| Writer process count | — | `SPECTRA_BENCH_WRITER_N` | falls back to `CLIENT_COUNT` |
| Writer ladder (infra) | — | `SPECTRA_BENCH_WRITER_LADDER` | `1,2` |
| Batch sweep (infra) | — | `SPECTRA_BENCH_BATCH_SWEEP` | **`512,2048`** |

---

## Question coverage matrix

| Question | Primary experiments |
|----------|---------------------|
| Batched durable Spectra→DW ceiling | **BM-SW7** |
| Single-row durable floor | BM-SW5 / BM-SW6 |
| Adapter vs full-stack enqueue | BM-SW0, BM-SW1 |
| Concurrency / multi-writer enqueue | BM-SW2, BM-SW3 |
| Event enqueue hammer | BM-SW4 |
| Query latency / depth / filters | BM-SQ0..SQ3 |
| Query validation / paging clamp overhead (correctness + capacity note) | BM-SQ1, BM-SQ3 (re-measure after hardening; `MAX_EVENT_QUERY_LIMIT=1000`) |
| Remote TLS connect gate (correctness-only) | Unit tests (`remote_security`); no BM-ID — scheme check is fail-closed, not a capacity path |
| Smoke emit/persist regression | BM-S0..S3 |

---

## Experiment catalog

### Track A — Write

| ID | Summary | Role |
|----|---------|------|
| **bm-sw7** | Batched durable counter (L2 `PersistConfig` via `--batch-max`; `*_now` + `flush_persist`) | **Capacity primary** (`path=l2-batch`) |
| **bm-sw5** | Durable single-row counter (adapter-direct) | Protocol floor |
| **bm-sw6** | Durable single-row event (adapter-direct) | Protocol floor |
| **bm-sw0** | Adapter-direct counter firehose | Enqueue comparison |
| **bm-sw1** | Full-stack counter firehose | Enqueue (not durable on remote) |
| **bm-sw2** | Concurrency saturation | Enqueue |
| **bm-sw3** | Multi-writer scaling (`--bench-clients`) | Enqueue; many writers → one URL |
| **bm-sw4** | Event append firehose | Enqueue |

See [PERFORMANCE_STUDY.md](PERFORMANCE_STUDY.md) and [`infra/aws/spectra-multidw/`](../../infra/aws/spectra-multidw/).

### Track B — Query

| ID | Summary | Pass criteria |
|----|---------|---------------|
| **bm-sq0** | Metric query at depth=0 | reports `query_metrics_ms` |
| **bm-sq1** | Metric query after prefill | reports `query_metrics_ms` vs depth |
| **bm-sq2** | Label filter query (hit vs miss) | reports filter delta |
| **bm-sq3** | Event query after prefill | reports `query_events_ms` vs depth |

### Track C — Smoke regression

| ID | Scenario | Storage forced |
|----|----------|----------------|
| **bm-s0** | `emit_only_bench` | mem + embedded |
| **bm-s1** | `persist_roundtrip_bench` | mem + embedded |
| **bm-s2** | `persist_roundtrip_bench` | sqlite + embedded |
| **bm-s3** | `query_range_bench(10)` | mem + embedded |

---

## Run commands

```bash
export CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=target-spectra-bench

# List experiments
cargo run -p spectra-bench -- experiments

# Smoke — mem embedded (not decision-grade)
cargo run -p spectra-bench -- run --experiment bm-sw1 \
  --storage mem --topology embedded

# AWS durable L2 batch (decision-grade)
cd infra/aws/spectra-multidw
export SPECTRA_MULTIDW_DW_KIND=clickhouse
export SPECTRA_BENCH_DW_N=1
export SPECTRA_BENCH_BATCH_SWEEP=512,2048
./provision.sh && ./bootstrap.sh
./deploy-and-run.sh
./fetch-reports.sh
./teardown.sh
```

**Flush / timing:** BM-SW7 uses `flush_persist` then visibility. BM-SW5/SW6 use awaited adapter writes. Full-stack enqueue (SW1) measures end-to-end enqueue latency on remote; durability coverage is BM-SW7.

---

## Report schema

Capacity experiments emit JSON with fields such as:

```json
{
  "experiment": "bm-sw7",
  "matrix": { "storage": "clickhouse", "topology": "remote-ingest", "telemetry": "off" },
  "hardware": "aws-t3-xlarge",
  "n": 1,
  "path": "l2-batch",
  "batch_max": 2048,
  "writer_n": 1,
  "durable_counter_ops_per_sec": 37974.4,
  "visibility_confirmed": true,
  "binding_tier": "unset"
}
```

Depth sweeps emit a JSON **array** of one object per depth cell. Commit only `*-aws-*.json` reports.
