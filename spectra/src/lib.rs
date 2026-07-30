//! Spectra is a Rust observability library for typed **metrics and counters**, structured
//! **event logs**, and pluggable storage.
//!
//! Wire a backend once with [`Spectra::builder()`], declare schemas with [`spectra_metric!`] and
//! [`spectra_schema!`], then emit and query through the typed helpers those macros expand.
//! Storage engines stay behind thin async ports — swap `mem`, `sqlite`, ClickHouse, or
//! TensorBase without changing emit code.
//!
//! ## Features
//!
//! - **Typed schema and metric DSL** — declare counters and event tables with classification
//!   metadata (PII, console safety) via [`spectra_metric!`] and [`spectra_schema!`]. Each
//!   macro expands inventory registration, typed `*Recorder` / `*Logger` helpers, and
//!   transport `*Payload` / `*_TOPIC` DTOs.
//! - **Schema DSL controls** — set emit defaults on the schema: `level`, `default_sample_rate`,
//!   `coalesce_ms` (gauges), and field `classification` (`pii`, `safe_for_console`). See
//!   [`spectra_schema!`] and [`spectra_metric!`]. Runtime env/TOML overrides live on
//!   [`spectra_core::SpectraConfig`].
//! - **Composable storage** — inject backends on the builder: in-memory (`mem`), durable embedded
//!   (`sqlite`), or remote (`clickhouse`, `tensorbase`).
//! - **Emit controls** — global and per-name level gates, sampling, optional request/job buffers,
//!   and async batched persist (overrides schema defaults). `SPECTRA_GATE=0` is ignored unless
//!   `SPECTRA_GATE_FORCE_OFF=1` is also set (fail-closed).
//! - **Query input validation** — metric/table/field tokens must match
//!   [`validate_spectra_ident`]; invalid filter fields return [`Error::Config`] before SQL runs.
//! - **Query paging clamps** — event queries clamp `limit`/`offset` to
//!   [`MAX_EVENT_QUERY_LIMIT`] / [`MAX_EVENT_QUERY_OFFSET`].
//! - **Remote TLS** — ClickHouse/TensorBase connect requires `https://` or `tcp+tls://` unless
//!   `SPECTRA_ALLOW_INSECURE_REMOTE=1` (plaintext `http://` / `tcp://`, development/CI only).
//! - **Field classification helpers** — [`mask_field_value`] for host/UI display of PII columns
//!   (query authz remains a host/Gauge concern; see repository `SECURITY.md`).
//! - **Diagnostics** — library crates emit structured `tracing` events; hosts initialize a
//!   `tracing_subscriber` (see the `quickstart` example). Remote storage errors redact URL
//!   credentials before surfacing.
//! - **Query API** — read metrics and events through [`SpectraRouter`] with label and time-range
//!   filters.
//! - **Direct or distributed wiring** — one process can write storage, or many publishers can
//!   fan out to a consumer process that owns the database (see
//!   [Publish-consume](#publish-consume-two-binaries)).
//!
//! *Declarative schemas and typed logging APIs — same surface from embedded to multi-service.*
//!
//! # Getting started
//!
//! You always emit the same way (`CacheHitsRecorder::record(...)`, etc.). What changes is
//! **which process writes the database**. Storage is **embedded** (mem/sqlite) or **remote**
//! (ClickHouse/TensorBase) — configured on [`Spectra::builder()`].
//!
//! ## Choose how data reaches storage
//!
//! - **[Direct persist](#direct-persist-one-binary)** — one binary emits and stores. Start here.
//! - **[Publish-consume](#publish-consume-two-binaries)** — many app processes *publish* onto a
//!   bus; a separate **consumer** binary writes storage.
//! - **[Dual-path](#dual-path-optional)** — one process both publishes and stores (mirroring).
//!   Optional.
//!
//! After you pick a wiring shape, continue with [schemas](#4-declare-and-link-schemas)
//! (shared by every shape).
//!
//! ## Direct persist (one binary)
//!
//! This process emits metrics/events **and** writes them to storage. There is no second binary
//! and no message bus.
//!
//! ```text
//! Your app ──emit──► Spectra ──async persist──► mem / SQLite / ClickHouse / TensorBase
//! ```
//!
//! | Backend | Types | Feature | When to use |
//! |---------|-------|---------|-------------|
//! | In-memory | [`MemMetricsBackend`] / [`MemEventsBackend`] | `mem` (default) | Local experiments |
//! | SQLite | [`SqliteMetricsBackend`] / [`SqliteEventsBackend`] | `sqlite` | Durable single host |
//! | ClickHouse | [`ClickHouseMetricsBackend`] / [`ClickHouseEventsBackend`] | `clickhouse` | Remote analytics |
//! | TensorBase | [`TensorBaseMetricsBackend`] / [`TensorBaseEventsBackend`] | `tensorbase` | ClickHouse-compatible scale-out |
//!
//! **In-memory** (both backends are required):
//!
//! ```no_run
//! # #[cfg(feature = "mem")]
//! # async fn demo() -> spectra::Result<()> {
//! use std::sync::Arc;
//! use spectra::{MemEventsBackend, MemMetricsBackend, Spectra};
//!
//! let spectra = Spectra::builder()
//!     .metrics_backend(Arc::new(MemMetricsBackend::new()))
//!     .events_backend(Arc::new(MemEventsBackend::new()))
//!     .embedded()
//!     .build()?;
//! # let _ = spectra;
//! # Ok(())
//! # }
//! ```
//!
//! **High-throughput DW ingest** — raise L2 batch size on the builder (not env vars):
//!
//! ```no_run
//! # #[cfg(feature = "mem")]
//! # async fn demo() -> spectra::Result<()> {
//! use std::sync::Arc;
//! use std::time::Duration;
//! use spectra::{MemEventsBackend, MemMetricsBackend, PersistConfig, Spectra};
//!
//! let spectra = Spectra::builder()
//!     .metrics_backend(Arc::new(MemMetricsBackend::new()))
//!     .events_backend(Arc::new(MemEventsBackend::new()))
//!     .persist(PersistConfig {
//!         batch_max: 2048,
//!         batch_wait: Duration::from_millis(5),
//!         ..PersistConfig::default()
//!     })
//!     .build()?;
//! // Web / scripts: use try_record_*_now / helpers (enqueue L2).
//! // Scripts that must exit only after durable writes:
//! spectra.flush_persist().await?;
//! # Ok(())
//! # }
//! ```
//!
//! Canonical path for **web** and **Write Now**: `*_now` → L2 batched persist. Prefer that over
//! `request_scope`, which drops undrained emits on panic or early exit.
//!
//! **ClickHouse** (omit `.embedded()`; constructors are async):
//!
//! ```no_run
//! # #[cfg(feature = "clickhouse")]
//! # async fn demo() -> spectra::Result<()> {
//! use std::sync::Arc;
//! use spectra::{ClickHouseEventsBackend, ClickHouseMetricsBackend, Spectra};
//!
//! let url = "https://clickhouse.example:8443"; // or SPECTRA_CLICKHOUSE_URL
//! // Local plaintext: SPECTRA_ALLOW_INSECURE_REMOTE=1 + http://127.0.0.1:8123
//! let metrics = ClickHouseMetricsBackend::connect(url).await?;
//! let events = ClickHouseEventsBackend::connect(url).await?;
//! let spectra = Spectra::builder()
//!     .metrics_backend(Arc::new(metrics))
//!     .events_backend(Arc::new(events))
//!     .build()?;
//! # let _ = spectra;
//! # Ok(())
//! # }
//! ```
//!
//! Runnable:
//! `cargo run -p uf-spectra --example quickstart_schema_emit --features mem`,
//! `cargo run -p uf-spectra --example quickstart_sqlite --features sqlite`,
//! `cargo run -p uf-spectra --example quickstart_clickhouse_emit --features clickhouse`.
//! Then jump to [schemas](#4-declare-and-link-schemas).
//!
//! ## Publish-consume (two binaries)
//!
//! Use this when many services emit telemetry but you do **not** want each of them to open
//! ClickHouse (or SQLite) itself. Instead:
//!
//! 1. Each app process is a **publisher** — it emits into Spectra, which hands the emit to your
//!    [`SpectraSink`], which publishes onto a message bus.
//! 2. A separate **consumer** process (or fleet) subscribes on that bus and writes storage.
//!
//! ```text
//! Publisher binary(ies) ──emit──► SpectraSink ──publish──► bus (e.g. Photon)
//! Consumer binary       ──subscribe──► decode ──► try_*_now / storage backends
//! ```
//!
//! Spectra does **not** ship a bus. Your host owns that piece. A common choice is
//! [Photon](https://github.com/unified-field-dev/photon) (`uf-photon` on crates.io).
//!
//! ### What you create
//!
//! | Piece | Purpose |
//! |-------|---------|
//! | Shared schema crate | Same `spectra_*!` modules (`mod`-linked) on both sides |
//! | Publisher binary | `[[bin]]` or crate that emits; **no** DB writes |
//! | Consumer binary | Another `[[bin]]` or crate that owns storage |
//! | Bus | Photon, NATS, Kafka, … — host-provided |
//!
//! ### Shared setup (both binaries)
//!
//! Put `spectra_schema!` / `spectra_metric!` declarations in a shared crate both binaries depend
//! on. `mod` each schema file so macros expand helpers, topic DTOs, and inventory registration
//! (see [§ 4](#4-declare-and-link-schemas)).
//!
//! - **Helpers** — typed emit API (publisher uses these)
//! - **Topic payloads** — `*Payload` / `*_TOPIC` beside each schema (publisher publishes these)
//! - **Consumer persist** — after decode, call [`try_record_counter_at`] / [`try_log_event_at`]
//!   with the envelope timestamp (or a storage backend)
//!
//! ### Publisher binary
//!
//! Add a second binary in your workspace (for example `src/bin/telemetry_publisher.rs`, or a
//! dedicated crate). In that process:
//!
//! 1. Implement [`SpectraSink`]: for each emit, build a topic `*Payload` and publish it on
//!    your bus. Keep the methods non-blocking (spawn a task, enqueue, or use Photon buffering).
//! 2. Wire Spectra with `.sink(...).persist_disabled().build()` so this process does **not**
//!    write the analytics database. (The builder still requires dummy or unused backends.)
//! 3. Emit with typed helpers exactly as in direct persist.
//!
//! ```ignore
//! use std::sync::Arc;
//! use spectra::{
//!     MemEventsBackend, MemMetricsBackend, Spectra, SpectraSink,
//! };
//!
//! /// Your bus adapter — replace the body with Photon (or another) publish calls.
//! struct BusPublishSink;
//!
//! impl SpectraSink for BusPublishSink {
//!     fn record_counter(&self, name: &str, labels: &[(&str, &str)], delta: i64) {
//!         // 1) Map name → schema module *Payload (see topics beside each schema).
//!         // 2) Publish asynchronously — do not block the emit thread.
//!         let _ = (name, labels, delta);
//!     }
//!     fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
//!         let _ = (name, labels, value);
//!     }
//!     fn log_event(&self, table: &str, fields: &serde_json::Value) {
//!         let _ = (table, fields);
//!     }
//! }
//!
//! # async fn boot() -> spectra::Result<()> {
//! let sink = Arc::new(BusPublishSink);
//! let _spectra = Spectra::builder()
//!     .metrics_backend(Arc::new(MemMetricsBackend::new()))
//!     .events_backend(Arc::new(MemEventsBackend::new()))
//!     .sink(sink as Arc<dyn SpectraSink>)
//!     .persist_disabled() // publisher does not open ClickHouse
//!     .build()?;
//!
//! // Same emit API as direct persist:
//! // CacheHitsRecorder::record(1, serde_json::json!({"region":"us"}));
//! # Ok(())
//! # }
//! ```
//!
//! Runnable sketch (in-memory stand-in for the bus):
//! `cargo run -p uf-spectra --example quickstart_publish_only --features mem`.
//! API detail: [`SpectraSink`], [`SpectraBuilder::sink`], [`SpectraBuilder::persist_disabled`].
//!
//! ### Consumer binary
//!
//! Create a **different** binary (for example `src/bin/telemetry_consumer.rs`). This process
//! owns the database:
//!
//! 1. Build Spectra with real backends and **persist left on** (direct-persist `.build()`).
//! 2. Start your bus subscriber (Photon `start_executor` / `#[subscribe]`, etc.).
//! 3. On each message: deserialize to a `*Payload` or [`MetricEmit`] / [`SpectraEvent`],
//!    then call [`try_record_counter_at`] / [`try_log_event_at`] with the envelope `ts`
//!    (or write the storage backend directly).
//!
//! ```ignore
//! use std::sync::Arc;
//! use spectra::{
//!     try_record_counter_at, ClickHouseEventsBackend, ClickHouseMetricsBackend, Spectra,
//! };
//!
//! # async fn boot_consumer() -> spectra::Result<()> {
//! let url = std::env::var("SPECTRA_CLICKHOUSE_URL")?;
//! let spectra = Spectra::builder()
//!     .metrics_backend(Arc::new(ClickHouseMetricsBackend::connect(&url).await?))
//!     .events_backend(Arc::new(ClickHouseEventsBackend::connect(&url).await?))
//!     .build()?; // persist ON — this process writes storage
//!
//! // Pseudocode for your bus callback (Photon subscriber, etc.):
//! // let emit = decode_metric_emit(bytes)?;
//! // let labels = label_pairs_from_json(&emit.labels);
//! // let ts = emit.ts.unwrap_or_else(chrono::Utc::now);
//! // try_record_counter_at(&emit.name, &labels, emit.delta.unwrap_or(1), ts);
//!
//! let _ = spectra; // keep handle alive; run subscriber loop until shutdown
//! # Ok(())
//! # }
//! ```
//!
//! Runnable sketch (decode → `try_record_counter_at` without a live broker):
//! `cargo run -p uf-spectra --example quickstart_consume_forward --features mem`.
//!
//! ### Run both
//!
//! 1. Start the **consumer** (and your bus / Photon) so subscriptions are ready.
//! 2. Start one or more **publishers**.
//! 3. Query storage from the consumer process (or any process that shares the same DB).
//!
//! For Photon you typically set `PHOTON_TRANSPORT_KEY` (base64 of 32 bytes) and a broker URL
//! such as `PHOTON_NATS_URL`. See Photon’s own README for adapter wiring.
//! See the crate README **How to run examples** for the local sketch pair and production notes.
//!
//! ## Dual-path (optional)
//!
//! Same process both publishes through a [`SpectraSink`] **and** persists to storage. Use when
//! you want a bus mirror without moving writes to another binary.
//!
//! Wire with `.sink(transport).build()` — omit `persist_disabled` so storage still receives emits.
//!
//! Runnable: `cargo run -p uf-spectra --example quickstart_transport --features mem`.
//!
//! ## Runnable examples
//!
//! Canonical teaching path (see crate README for full runbooks):
//!
//! | Example | Topology | Features | Notes |
//! |---------|----------|----------|-------|
//! | `quickstart_schema_emit` | Direct persist (embedded) | `mem` | Typed helpers + query |
//! | `quickstart_publish_only` | Publish-consume (publisher) | `mem` | Standalone sketch |
//! | `quickstart_consume_forward` | Publish-consume (consumer) | `mem` | Standalone sketch |
//! | `quickstart_clickhouse_emit` | Direct persist (remote) | `clickhouse` | Needs `SPECTRA_CLICKHOUSE_URL` |
//!
//! ```bash
//! cargo run -p uf-spectra --example quickstart_schema_emit --features mem
//! cargo run -p uf-spectra --example quickstart_publish_only --features mem
//! cargo run -p uf-spectra --example quickstart_consume_forward --features mem
//! # docker compose -f docker-compose.dev.yml up -d clickhouse
//! SPECTRA_ALLOW_INSECURE_REMOTE=1 SPECTRA_CLICKHOUSE_URL=http://127.0.0.1:8123 \
//!   cargo run -p uf-spectra --example quickstart_clickhouse_emit --features clickhouse
//! ```
//!
//! ## 4. Declare and link schemas
//!
//! Typed emit helpers expand from the same macros that register schema metadata. Required for
//! every wiring shape. No `build.rs` or `OUT_DIR` includes.
//!
//! ```toml
//! [dependencies]
//! spectra = { package = "uf-spectra", git = "https://github.com/deathbreakfast/spectra.git", tag = "v0.1.0", features = ["mem"] }
//! chrono = { version = "0.4", features = ["serde"] }
//! serde = { version = "1", features = ["derive"] }
//! serde_json = "1"
//! ```
//!
//! Typical application layout:
//!
//! ```text
//! my-app/
//! ├── Cargo.toml
//! └── src/
//!     ├── schemas/
//!     │   ├── mod.rs
//!     │   ├── cache_hits.rs
//!     │   └── request_log.rs
//!     └── main.rs
//! ```
//!
//! ```ignore
//! // src/schemas/mod.rs — one `mod` line per schema file (links inventory + helpers + topics)
//! pub mod cache_hits;
//! pub mod request_log;
//!
//! pub use cache_hits::CacheHitsRecorder;
//! pub use request_log::RequestLogLogger;
//! ```
//!
//! ## 5. Declare metric and event schemas
//!
//! Schemas are the typed contracts Spectra stores and queries. Share them across publisher and
//! consumer binaries in publish-consume (depend on the same schema crate).
//!
//! - **Metric schema** ([`spectra_metric!`]) — a named measurement family; optional `level`,
//!   `default_sample_rate`, and `coalesce_ms` (gauges).
//! - **Event schema** ([`spectra_schema!`]) — a typed structured log table with classified
//!   columns; optional `level` and `default_sample_rate` (`coalesce_ms` is not valid on events).
//!
//! Full field tables: [`spectra_metric!`] / [`spectra_schema!`]. Runtime overrides (env/TOML):
//! [`spectra_core::SpectraConfig`] and the emit-gate table in the crate README.
//!
//! ```ignore
//! // src/schemas/cache_hits.rs
//! use spectra::spectra_metric;
//!
//! spectra_metric! {
//!     CacheHits {
//!         store: "default",       // logical store for routing
//!         name: "cache_hits",     // registry key + query_metrics.metric_name
//!         version: "0.1.0",
//!         description: "Counter for cache hit events",
//!         level: Debug,
//!         default_sample_rate: 0.1,
//!     }
//! }
//! ```
//!
//! ```ignore
//! // src/schemas/request_log.rs
//! use spectra::spectra_schema;
//!
//! spectra_schema! {
//!     RequestLog {
//!         store: "default",
//!         table: "request_log",   // registry key + query_events.table
//!         version: "0.1.0",
//!         description: "Structured request debug events",
//!         level: Debug,
//!         default_sample_rate: 0.25,
//!         fields: [
//!             message: {
//!                 r#type: String, // String | i64 | f64 | bool
//!                 classification: { pii: false, safe_for_console: true },
//!             },
//!         ],
//!     }
//! }
//! ```
//!
//! Helper names come from the schema identifier:
//! `CacheHits` → `CacheHitsRecorder`, `RequestLog` → `RequestLogLogger`.
//! Each expansion also emits `*Payload` and `*_TOPIC` for publish-consume transport.
//!
//! ## 6. Emit metrics and events
//!
//! **Direct persist / dual-path:** call helpers in the same process that owns (or mirrors) storage.
//!
//! **Publish-consume:** call helpers only in the **publisher**. The consumer persists with
//! [`try_record_counter_at`] / [`try_log_event_at`] after decode (pass envelope `ts`).
//!
//! ```ignore
//! use crate::schemas::{CacheHitsRecorder, RequestLogLogger};
//!
//! CacheHitsRecorder::record(1, serde_json::json!({ "region": "us-west" }));
//! RequestLogLogger::log("request handled".to_string());
//! ```
//!
//! This crate's CI demo helpers ([`helpers`]) come from `platform_smoke_*` schemas only —
//! define product schemas in **your** application.
//!
//! ## 7. Query persisted data
//!
//! Storage persist is asynchronous. Wait briefly (or poll) before querying.
//!
//! **Direct persist / dual-path:** query on the same process that wrote storage.
//!
//! **Publish-consume:** query from the **consumer** (or any process connected to the same
//! database) — not from the publisher.
//!
//! ```no_run
//! # #[cfg(feature = "mem")]
//! # async fn demo(spectra: spectra::Spectra) -> spectra::Result<()> {
//! use spectra_core::{current_emit_ts, EventsQueryFilter, MetricsQueryRange};
//!
//! tokio::time::sleep(std::time::Duration::from_millis(80)).await;
//! let now = current_emit_ts();
//!
//! let points = spectra
//!     .router()
//!     .query_metrics(MetricsQueryRange {
//!         metric_name: "cache_hits".into(),
//!         start: now - chrono::Duration::seconds(5),
//!         end: now + chrono::Duration::seconds(1),
//!         label_matchers: vec![],
//!     })
//!     .await?;
//!
//! let rows = spectra
//!     .router()
//!     .query_events(EventsQueryFilter {
//!         table: "request_log".into(),
//!         start: Some(now - chrono::Duration::seconds(5)),
//!         end: Some(now + chrono::Duration::seconds(1)),
//!         ..Default::default()
//!     })
//!     .await?;
//! # let _ = (points, rows);
//! # Ok(())
//! # }
//! ```
//!
//! Prefer row queries. Event chart aggregates (`query_aggregate`) are stubbed on most backends.
//!
//! Configuration (emit gates, sampling, buffers) is documented on
//! [`spectra_core::SpectraConfig`].
//!
//! # Notes
//!
//! - Enable backend features explicitly (`mem` is on by default; `sqlite`, `clickhouse`, and
//!   `tensorbase` are optional).
//! - Product schemas belong in **your application**; this crate registers CI demo
//!   `platform_smoke_*` schemas only.
//! - Pin consumption via git tag (for example `v0.1.0`). Configure storage on
//!   [`Spectra::builder()`].
//! - Event chart aggregates (`spectra_core::EventStorageBackend::query_aggregate`) are stubbed on most backends.
//! - Schema modules must be `mod`-linked into the binary or `inventory` will not see them.
//! - Schema macros and helpers call through the `spectra` crate — pin `spectra` as a direct
//!   dependency (no separate `spectra-core` pin required for the default recipe).

// Allow schema macros to resolve `::spectra::…` inside this crate.
extern crate self as spectra;

mod schemas;

pub mod helpers;
pub mod topics;

/// Re-export for schema macro `inventory::submit!` expansions.
pub use spectra_core::inventory;
pub use spectra_core::{
    self, clamp_event_paging, mask_field_value, try_log_event_at, try_log_event_now,
    try_record_counter, try_record_counter_at, try_record_counter_now, try_record_gauge_at,
    try_record_gauge_now, validate_spectra_ident, ChainedSink, Error, FieldClassification,
    LoggingKind, MetricEmit, RecordingSink, Result, SchemaFieldMetadata, SchemaMetadata,
    SchemaMetadataInit, SchemaRegistry, SpectraEvent, SpectraLevel, SpectraRouter, SpectraSink,
    MAX_EVENT_QUERY_LIMIT, MAX_EVENT_QUERY_OFFSET, PII_MASK,
};
pub use spectra_macros::{spectra_metric, spectra_schema};
pub use spectra_runtime::{
    PersistConfig, PersistHandle, PersistOverflow, Spectra, SpectraBuilder, StoragePersistSink,
};

#[cfg(feature = "mem")]
pub use spectra_backend_mem::{MemEventsBackend, MemMetricsBackend};

#[cfg(feature = "sqlite")]
pub use spectra_backend_sqlite::{SqliteEventsBackend, SqliteMetricsBackend};

#[cfg(feature = "tensorbase")]
pub use spectra_backend_tensorbase::{TensorBaseEventsBackend, TensorBaseMetricsBackend};

#[cfg(feature = "clickhouse")]
pub use spectra_backend_clickhouse::{ClickHouseEventsBackend, ClickHouseMetricsBackend};
