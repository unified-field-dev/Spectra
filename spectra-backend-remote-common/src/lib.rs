//! Shared remote storage logic for ClickHouse-protocol Spectra backends.
//!
//! **Internal** — used by `spectra-backend-clickhouse` and `spectra-backend-tensorbase`;
//! not re-exported from the public `spectra` crate.
//!
//! # Stack position
//!
//! Internal library between `spectra_core` storage traits and engine-specific adapter crates.
//! Provides HTTP/native client wiring, DDL execution, and shared insert/query paths.
//!
//! # Entry points
//!
//! - [`RemoteClient::connect`] — ClickHouse-protocol client wrapper
//! - [`RemoteMetricsBackend::connect`] / [`RemoteEventsBackend::connect`] — parameterized backends
//! - [`MetricInsertRow`] / [`EventInsertRow`] — row shapes for streaming inserts
//! - [`redact_url_credentials`] — scrub connection secrets from error text
//! - [`RemoteTransportSecurity`] — TLS-required remote URL policy (`SPECTRA_ALLOW_INSECURE_REMOTE`)
//! - [`query_sql`] helpers — validated SQL fragments (`escape_str`, `scope_clause`, …)
//!
//! # Prerequisites and gotchas
//!
//! - Expects canonical table names from `spectra_core` (`spectra_metrics`, `spectra_events`).
//! - Query builders validate metric/table/field identifiers and clamp event `limit`/`offset`
//!   (`spectra_core::MAX_EVENT_QUERY_LIMIT`).
//! - Storage errors pass through [`redact_url_credentials`] so URL userinfo is not echoed.
//! - Remote connect rejects plaintext `http://` / `tcp://` unless
//!   `SPECTRA_ALLOW_INSECURE_REMOTE=1` (prefer `https://` / `tcp+tls://`).
//! - `query_aggregate` runs one `GROUP BY` query per chart ([`query_sql::aggregate_sql`]) and
//!   returns at most [`query_sql::AGGREGATE_GROUP_LIMIT`] groups.
//! - Label filtering on metric range queries happens client-side after fetch.
//!
//! See also: the `spectra` crate documentation map (`cargo doc -p uf-spectra --open`).

mod client;
mod events;
mod mem_store;
mod metrics;
pub mod query_sql;
mod remote_security;

pub use client::{
    datetime_to_ch_ts, parse_rfc3339_ts, redact_url_credentials, EventInsertRow, MetricInsertRow,
    RemoteClient,
};
pub use events::RemoteEventsBackend;
pub use metrics::RemoteMetricsBackend;
pub use remote_security::{RemoteTransportSecurity, ALLOW_INSECURE_REMOTE_ENV};
