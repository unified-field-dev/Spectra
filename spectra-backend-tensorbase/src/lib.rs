//! Scale-out TensorBase storage adapter (ClickHouse-compatible protocol).
//!
//! Enable with the `spectra` feature `tensorbase`. Uses the official `clickhouse` Rust client
//! against TensorBase's compatible endpoint.
//!
//! - [`TensorBaseMetricsBackend::connect_host`] — hostname (default port `9528`)
//! - [`TensorBaseMetricsBackend::connect`] / [`TensorBaseEventsBackend::connect`] — explicit URL
//! - [`default_url`] — build `tcp://host:9528` for native protocol
//! - Set `SPECTRA_TENSORBASE_URL` for integration tests (`cargo test -- --ignored`).
//! - DDL uses `ENGINE = BaseStorage` (TensorBase dialect, not MergeTree).
//! - `query_aggregate` sends the same ClickHouse-dialect `GROUP BY` SQL as the ClickHouse
//!   adapter. A TensorBase build without those JSON functions returns a storage error.

mod ddl;
mod events;
mod metrics;

pub use ddl::default_url;
pub use events::TensorBaseEventsBackend;
pub use metrics::TensorBaseMetricsBackend;
