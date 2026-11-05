//! Application-provided sink for synchronous metric and event fan-out.
//!
//! Implement this trait when your application needs a **publish path** (message bus, RPC, or
//! composite adapter) in addition to or instead of async storage persist. Wire the sink with
//! `Spectra::builder().sink(...)` (see `spectra` / `spectra-runtime`).
//!
//! # Publisher role (publish-consume)
//!
//! In a publisher process, the sink is the bridge out of Spectra:
//!
//! 1. Declare schemas with `spectra_schema!` / `spectra_metric!` (each expansion emits a
//!    `*Payload` / `*_TOPIC` pair beside the typed helper).
//! 2. Implement [`SpectraSink`]: map each emit to a topic payload shaped like [`crate::MetricEmit`]
//!    / [`crate::SpectraEvent`] and publish on your bus.
//! 3. Wire `.sink(Arc::new(your_sink)).persist_disabled().build()` so the publisher does **not**
//!    open the analytics database.
//!
//! Consumer processes subscribe on the bus and write storage via `try_record_counter_now` /
//! `try_log_event_now` (or a storage backend). The `spectra` crate documents the full
//! publisher/consumer split under **Getting started → Publish-consume**. Spectra does not embed a bus;
//! [Photon](https://github.com/unified-field-dev/photon) (`uf-photon`) is a common host choice
//! for durable pub/sub, and the `uf-spectra-photon` crate ships a ready-made Photon publish sink
//! and a batched, consumer-group storage writer.
//!
//! ```ignore
//! use std::sync::Arc;
//! use spectra_core::SpectraSink;
//! // Host binary also depends on `spectra` for Spectra::builder() and backends.
//!
//! struct BusPublishSink { /* Photon handle, channel, … */ }
//!
//! impl SpectraSink for BusPublishSink {
//!     fn record_counter(&self, name: &str, labels: &[(&str, &str)], delta: i64) {
//!         // Build a topics::* payload and publish asynchronously — do not block.
//!         let _ = (name, labels, delta);
//!     }
//!     fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
//!         let _ = (name, labels, value);
//!     }
//!     fn log_event(&self, table: &str, fields: &serde_json::Value) {
//!         let _ = (table, fields);
//!     }
//! }
//! ```
//!
//! # Design notes
//!
//! - Invoked on the emit thread unless buffering ([`crate::emit_buffer`]) or async persist
//!   defers replay. Prefer non-blocking publish (spawn, channel, Photon buffering).
//! - [`crate::try_record_counter`] and related emit functions no-op when re-entering sink
//!   dispatch to prevent loops.
//! - Pair with [`crate::RecordingSink`] in tests or [`crate::ChainedSink`] to fan out to
//!   multiple handlers.
//! - For dual-path (transport **and** local persist), omit `persist_disabled` so the runtime
//!   wraps your sink with storage persist.
//!
//! # Examples
//!
//! ```
//! use spectra_core::SpectraSink;
//! use serde_json::json;
//!
//! struct PrintSink;
//!
//! impl SpectraSink for PrintSink {
//!     fn record_counter(&self, name: &str, labels: &[(&str, &str)], delta: i64) {
//!         println!("counter {name} +{delta} {labels:?}");
//!     }
//!     fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
//!         println!("gauge {name} = {value} {labels:?}");
//!     }
//!     fn log_event(&self, table: &str, fields: &serde_json::Value) {
//!         println!("event {table}: {fields}");
//!     }
//! }
//!
//! let sink = PrintSink;
//! sink.record_counter("requests_total", &[("method", "GET")], 1);
//! sink.log_event("request_log", &json!({"status": 200}));
//! ```

/// Synchronous fan-out target for metrics and structured events.
///
/// Implement this trait for a transport publish adapter, telemetry mirror, or other destination
/// that should receive emits. In distributed mode this is the **publisher** boundary — see the
/// [module docs](self).
///
/// Implementations run inline unless a surrounding buffer or adapter moves work off-thread, so
/// handlers should avoid blocking.
///
/// # Examples
///
/// ```
/// use serde_json::Value;
/// use spectra_core::SpectraSink;
///
/// struct PrintSink;
///
/// impl SpectraSink for PrintSink {
///     fn record_counter(&self, name: &str, labels: &[(&str, &str)], delta: i64) {
///         println!("{name} +{delta} {labels:?}");
///     }
///
///     fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
///         println!("{name} = {value} {labels:?}");
///     }
///
///     fn log_event(&self, table: &str, fields: &Value) {
///         println!("{table}: {fields}");
///     }
/// }
///
/// let sink = PrintSink;
/// sink.record_counter("requests_total", &[("method", "GET")], 1);
/// ```
pub trait SpectraSink: Send + Sync {
    /// Increment (or decrement) a counter.
    fn record_counter(&self, name: &str, labels: &[(&str, &str)], delta: i64);

    /// Set a gauge value.
    fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64);

    /// Append a structured event row.
    fn log_event(&self, table: &str, fields: &serde_json::Value);
}
