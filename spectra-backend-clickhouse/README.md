# spectra-backend-clickhouse

Remote ClickHouse storage adapter for Spectra metrics and events. Enable via the `spectra` feature `clickhouse` for network-backed analytics storage; implements the ClickHouse client boundary for metrics and events.

## Role

- `ClickHouseMetricsBackend` and `ClickHouseEventsBackend`
- Enabled via the `spectra` feature `clickhouse`
- Canonical tables: `spectra_metrics`, `spectra_events` (JSON labels/fields as String columns)

## Connect

```rust
use std::sync::Arc;
use spectra::{ClickHouseEventsBackend, ClickHouseMetricsBackend, Spectra};

let url = std::env::var("SPECTRA_CLICKHOUSE_URL")?; // prefer https://…; http:// needs SPECTRA_ALLOW_INSECURE_REMOTE=1
let metrics = ClickHouseMetricsBackend::connect(&url).await?;
let events = ClickHouseEventsBackend::connect(&url).await?;
let _spectra = Spectra::builder()
    .metrics_backend(Arc::new(metrics))
    .events_backend(Arc::new(events))
    .build()?;
```

## Runnable

```bash
docker compose -f docker-compose.dev.yml up -d clickhouse
export SPECTRA_ALLOW_INSECURE_REMOTE=1   # local plaintext only
export SPECTRA_CLICKHOUSE_URL=http://127.0.0.1:8123
cargo run -p uf-spectra --example quickstart_clickhouse_emit --features clickhouse
```

See [`spectra/README.md` — How to run examples](../spectra/README.md#how-to-run-examples) and repository [`SECURITY.md`](../SECURITY.md).

Integration tests: set `SPECTRA_CLICKHOUSE_URL` (and `SPECTRA_ALLOW_INSECURE_REMOTE=1` for plaintext) and run `cargo test -p spectra-backend-clickhouse -- --ignored`.

## Status

Shipped in tag `v0.1.0`. `query_aggregate` computes chart aggregates in ClickHouse with one `GROUP BY` query per chart, capped at 10,000 groups.
