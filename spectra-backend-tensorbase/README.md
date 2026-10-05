# spectra-backend-tensorbase

Scale-out TensorBase storage adapter (ClickHouse-compatible native protocol). Enable via the `spectra` feature `tensorbase` for remote ingest and multi-node metric/event persistence; implements the TensorBase wire protocol boundary.

## Role

- `TensorBaseMetricsBackend` and `TensorBaseEventsBackend`
- Enabled via the `spectra` feature `tensorbase`
- Uses the official `clickhouse` Rust client against TensorBase's ClickHouse-compatible TCP/HTTP endpoint
- DDL uses `ENGINE = BaseStorage` (TensorBase dialect)

## Connect

```rust
use std::sync::Arc;
use spectra::{Spectra, TensorBaseEventsBackend, TensorBaseMetricsBackend};

// Native protocol default port 9528:
let metrics = TensorBaseMetricsBackend::connect_host("127.0.0.1").await?;
let events = TensorBaseEventsBackend::connect_host("127.0.0.1").await?;
let _spectra = Spectra::builder()
    .metrics_backend(Arc::new(metrics))
    .events_backend(Arc::new(events))
    .build()?;

// Or explicit URL (prefer tcp+tls:// / https://; plaintext needs SPECTRA_ALLOW_INSECURE_REMOTE=1):
let url = std::env::var("SPECTRA_TENSORBASE_URL")?;
let metrics = TensorBaseMetricsBackend::connect(&url).await?;
let events = TensorBaseEventsBackend::connect(&url).await?;
let _spectra = Spectra::builder()
    .metrics_backend(Arc::new(metrics))
    .events_backend(Arc::new(events))
    .build()?;
```

**Note:** [TensorBase upstream](https://github.com/tensorbase/tensorbase) is maintenance-only; the trait boundary allows swapping engines without changing emit code.

## Runnable

```bash
export SPECTRA_ALLOW_INSECURE_REMOTE=1   # local plaintext only
export SPECTRA_TENSORBASE_URL=tcp://127.0.0.1:9528
cargo run -p uf-spectra --example quickstart_tensorbase_emit --features tensorbase
```

See [`spectra/README.md` — How to run examples](../spectra/README.md#how-to-run-examples) and repository [`SECURITY.md`](../SECURITY.md).

Integration tests: set `SPECTRA_TENSORBASE_URL` (and `SPECTRA_ALLOW_INSECURE_REMOTE=1` for plaintext) and run `cargo test -p spectra-backend-tensorbase -- --ignored`.

## Status

Shipped in tag `v0.1.0`. `query_aggregate` sends the same ClickHouse-dialect `GROUP BY` SQL as the ClickHouse adapter; a TensorBase build without those JSON functions returns a storage error.
