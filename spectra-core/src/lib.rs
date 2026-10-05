//! Shared ports and types for Spectra metrics and structured event logs.
//!
//! Applications usually depend on the `spectra` crate, which re-exports this surface.
//! The binary wires a [`SpectraSink`] at boot via [`set_sink`].
//!
//! # Features
//!
//! - **Emit entry points** — [`try_record_counter`], [`try_record_gauge`], [`try_log_event`]
//!   (and `*_now` / `*_at` variants) with emit gating and optional buffering.
//! - **Emit gate / config** — [`SpectraConfig`], [`install_config`]; env `SPECTRA_GATE` /
//!   `SPECTRA_GATE_FORCE_OFF`, level, and sampling overrides.
//! - **Query DTOs** — [`MetricsQuery`], [`EventQuery`], grid filters, and map helpers.
//! - **Input validation** — [`validate_spectra_ident`] for metric/table/field tokens;
//!   [`clamp_event_paging`] bounds event query `limit`/`offset`.
//! - **Field classification** — [`FieldClassification`] plus [`mask_field_value`] for host/UI
//!   display (PII / console safety). Query authz remains a host concern.
//!
//! # Entry points
//!
//! | Concern | API |
//! |---------|-----|
//! | Wire sink | [`set_sink`] |
//! | Emit | [`try_record_counter`], [`try_log_event`], … |
//! | Gate | [`install_config`], [`SpectraConfig::from_env`] |
//! | Mask PII | [`mask_field_value`] |
//! | Validate names | [`validate_spectra_ident`] |
//! | Page bounds | [`clamp_event_paging`], [`MAX_EVENT_QUERY_LIMIT`] |
//!
//! # Re-entrancy
//!
//! [`try_record_counter`] and [`try_log_event`] no-op when called while already inside sink
//! dispatch, preventing loops when a sink handler re-emits telemetry.
//!
//! # Notes
//!
//! Spectra does not implement session auth or Gauge permissions. Invalid emit names are
//! dropped (debug log); invalid query identifiers return [`Error::Config`] from backends
//! that validate before SQL.

mod aggregate;
mod classification;
mod config;
mod dispatcher;
mod emit_buffer;
mod entry;
mod error;
mod event_filter;
mod gate;
mod query;
mod query_map;
mod registry;
mod rootcause;
mod router;
mod schema;
mod sink;
mod sinks;
mod storage;
mod test_util;
mod topic;
mod types;
mod validate;

pub use classification::{mask_field_value, FieldClassification, PII_MASK};
pub use config::{install_config, EmitPolicy, NameOverride, SpectraConfig};
pub use emit_buffer::{
    current_emit_ts, drain, is_active, job_enabled, request_enabled, request_scope, with_emit_ts,
    worker_scope, BufferedEmit,
};
pub use entry::{
    set_sink, try_log_event, try_log_event_at, try_log_event_now, try_record_counter,
    try_record_counter_at, try_record_counter_now, try_record_gauge, try_record_gauge_at,
    try_record_gauge_now,
};
pub use error::{Error, Result};
pub use event_filter::{
    finalize_event_rows, matches_filter_item, paginate_event_rows, row_matches_filter,
    row_matches_partition, sort_event_rows,
};
pub use query::{
    EventAggregateRequest, EventAggregateResult, EventAggregationSpec, EventExploreView,
    EventGridRow, EventMeasure, EventQuery, EventQueryResult, GridColumnDto, GridFilterItem,
    GridFilterModel, GridFilterOperator, GridLogicOperator, GridPaginationModel, GridSortDirection,
    GridSortItem, LabelMatcher, MetricPointDto, MetricsQuery, MetricsQueryResult, PartitionKind,
    SchemaDetailDto, SchemaFieldDto, SchemaListItem, SliceDto, StatCardDto, TimeSeriesDto,
};
pub use query_map::{
    aggregate_event_rows, aggregate_request_to_filter, aggregate_rows_to_result,
    event_query_to_filter, list_schemas, metrics_query_to_range, pivot_from_cells,
    points_to_metrics_result, rows_to_event_result, schema_detail, PivotCell, PIVOT_BLANK_LABEL,
};
pub use registry::{
    collect_distinct_spectra_store_names, LoggingKind, SchemaFieldMetadata, SchemaMetadata,
    SchemaMetadataInit, SchemaRegistry, SpectraLevel,
};
pub use rootcause::{
    elapsed_ms, enabled as rootcause_enabled, persist_queue_drop_count, record_ndjson_append,
    record_persist_queue_drop, record_storage_batch_write_events,
    record_storage_batch_write_metrics, record_storage_write_events, record_storage_write_metrics,
    RootcauseSnapshot,
};
pub use router::SpectraRouter;
pub use schema::{EVENTS_TABLE, METRICS_TABLE};
pub use sink::SpectraSink;
pub use sinks::{ChainedSink, CountingSink, NdjsonFileSink, NoOpSink, RecordingSink};
pub use storage::{
    EventRow, EventStorageBackend, EventWriteRow, EventsAggregateFilter, EventsQueryFilter,
    MetricPoint, MetricWriteRow, MetricsQueryRange, MetricsStorageBackend, NoOpEventBackend,
    NoOpMetricsBackend, SharedEventBackend, SharedMetricsBackend, StorageEngineType,
};
pub use test_util::{reset_config_and_sink_for_test, GLOBAL_TEST_LOCK};
pub use topic::{event_topic, metric_topic};
pub use types::{MetricEmit, MetricKind, SpectraEvent};
pub use validate::{
    clamp_event_paging, is_valid_spectra_ident, validate_spectra_ident, MAX_EVENT_QUERY_LIMIT,
    MAX_EVENT_QUERY_OFFSET, MAX_SPECTRA_IDENT_LEN,
};

/// Re-export for macro `inventory::submit!` in downstream crates.
pub use quark::inventory;
