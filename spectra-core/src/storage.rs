//! Storage backend traits and query filter types for metrics and events.
//!
//! # Design notes
//!
//! Adapters implement these traits in `spectra-backend-*` crates and inject them through
//! [`spectra_runtime::SpectraBuilder`]. The default trait methods provide safe stubs so
//! partial implementations can ship incrementally.
//!
//! # Lifecycle example
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use chrono::Utc;
//! # use serde_json::json;
//! # use spectra_core::{
//! #     EventStorageBackend, MetricsStorageBackend, NoOpEventBackend, NoOpMetricsBackend,
//! #     MetricsQueryRange, EventsQueryFilter,
//! # };
//! # async fn demo() -> spectra_core::Result<()> {
//! let metrics = Arc::new(NoOpMetricsBackend);
//! let events = Arc::new(NoOpEventBackend);
//!
//! metrics.record_counter("cache_hits", &json!({}), 1, Utc::now()).await?;
//! events.append_row("request_log", &json!({"ok": true}), Utc::now(), None).await?;
//!
//! let points = metrics.query_range(MetricsQueryRange {
//!     metric_name: "cache_hits".into(),
//!     start: Utc::now() - chrono::Duration::hours(1),
//!     end: Utc::now(),
//!     label_matchers: vec![],
//! }).await?;
//! let rows = events.query_rows(EventsQueryFilter {
//!     table: "request_log".into(),
//!     ..Default::default()
//! }).await?;
//! # let _ = (points, rows);
//! # Ok(())
//! # }
//! ```
//!
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::error::Result;

/// Storage engine identifier for query routing and UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageEngineType {
    /// No-op backend (discards writes, returns empty reads).
    NoOp,
    /// In-memory backend.
    Mem,
    /// SQLite embedded backend.
    Sqlite,
    /// TensorBase backend.
    TensorBase,
    /// ClickHouse backend.
    ClickHouse,
}

/// Time-series query parameters.
#[derive(Debug, Clone)]
pub struct MetricsQueryRange {
    /// Metric family name.
    pub metric_name: String,
    /// Inclusive range start timestamp.
    pub start: DateTime<Utc>,
    /// Inclusive range end timestamp.
    pub end: DateTime<Utc>,
    /// Label equality matchers.
    pub label_matchers: Vec<crate::query::LabelMatcher>,
}

/// Row filter for event log queries (adapter input).
#[derive(Debug, Clone, Default)]
pub struct EventsQueryFilter {
    /// Event table name.
    pub table: String,
    /// Optional inclusive range start.
    pub start: Option<DateTime<Utc>>,
    /// Optional inclusive range end.
    pub end: Option<DateTime<Utc>>,
    /// Optional partition granularity (`"hourly"` or `"daily"`).
    pub partition: Option<String>,
    /// Maximum rows to return.
    pub limit: Option<u32>,
    /// Row offset for pagination.
    pub offset: Option<u32>,
    /// Column to sort by.
    pub sort_field: Option<String>,
    /// Whether sort is descending.
    pub sort_desc: bool,
    /// Structured row filter model.
    pub filter: crate::query::GridFilterModel,
}

/// Aggregate query for chart views (adapter input).
#[derive(Debug, Clone)]
pub struct EventsAggregateFilter {
    /// Event table name.
    pub table: String,
    /// Inclusive range start timestamp.
    pub start: DateTime<Utc>,
    /// Inclusive range end timestamp.
    pub end: DateTime<Utc>,
    /// Optional partition granularity (`"hourly"` or `"daily"`).
    pub partition: Option<String>,
    /// Structured row filter model.
    pub filter: crate::query::GridFilterModel,
    /// Aggregation measure.
    pub measure: crate::query::EventMeasure,
    /// Field to sum when measure is sum.
    pub measure_field: Option<String>,
    /// Time bucket width in seconds.
    pub time_bucket_secs: Option<u64>,
    /// Field to group by for slice views.
    pub group_by_field: Option<String>,
    /// Ordered row-key fields for Table pivot.
    pub row_fields: Vec<String>,
    /// Optional column pivot field for Table.
    pub pivot_field: Option<String>,
    /// Explore chart view that selects TimeSeries vs Slices vs Pivot result shape.
    pub view: crate::query::EventExploreView,
}

impl EventsAggregateFilter {
    /// Bucket width used when `time_bucket_secs` is unset.
    pub const DEFAULT_TIME_BUCKET_SECS: u64 = 3600;

    /// Effective time bucket width in seconds (never zero).
    #[must_use]
    pub fn bucket_secs(&self) -> u64 {
        self.time_bucket_secs
            .unwrap_or(Self::DEFAULT_TIME_BUCKET_SECS)
            .max(1)
    }

    /// Trimmed slice group-by field, or `None` when unset or blank.
    #[must_use]
    pub fn group_by_field_name(&self) -> Option<&str> {
        self.group_by_field
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    /// Trimmed, non-blank Table row-key fields in request order.
    #[must_use]
    pub fn row_field_names(&self) -> Vec<String> {
        self.row_fields
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Trimmed Table pivot field, or `None` when unset or blank.
    #[must_use]
    pub fn pivot_field_name(&self) -> Option<&str> {
        self.pivot_field
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }
}

/// Metric point for query results.
#[derive(Debug, Clone)]
pub struct MetricPoint {
    /// Sample timestamp.
    pub ts: DateTime<Utc>,
    /// Sample value.
    pub value: f64,
    /// Label set as JSON.
    pub labels: Value,
}

/// Event row for query results.
#[derive(Debug, Clone)]
pub struct EventRow {
    /// Event timestamp.
    pub ts: DateTime<Utc>,
    /// Event field payload.
    pub fields: Value,
}

/// One metrics write row for batch insert (counter or gauge).
#[derive(Debug, Clone)]
pub struct MetricWriteRow {
    /// Metric family name.
    pub name: String,
    /// `"counter"` or `"gauge"`.
    pub kind: &'static str,
    /// Counter delta or gauge value as JSON number.
    pub value: Value,
    /// Label set as JSON.
    pub labels: Value,
    /// Sample timestamp.
    pub ts: DateTime<Utc>,
    /// Optional correlation identifier.
    pub correlation_id: Option<String>,
}

/// One event write row for batch insert.
#[derive(Debug, Clone)]
pub struct EventWriteRow {
    /// Event table name.
    pub table: String,
    /// Event field payload.
    pub fields: Value,
    /// Event timestamp.
    pub ts: DateTime<Utc>,
    /// Optional correlation identifier.
    pub correlation_id: Option<String>,
}

/// Metrics storage: subscribers call `record_*`; explore UI calls `query_range`.
#[async_trait]
pub trait MetricsStorageBackend: Send + Sync {
    /// Returns the engine type for this backend.
    fn engine_type(&self) -> StorageEngineType;

    /// Records a counter increment.
    async fn record_counter(
        &self,
        name: &str,
        labels: &Value,
        delta: i64,
        ts: DateTime<Utc>,
    ) -> Result<()>;

    /// Records a gauge sample.
    async fn record_gauge(
        &self,
        name: &str,
        labels: &Value,
        value: f64,
        ts: DateTime<Utc>,
    ) -> Result<()>;

    /// Queries a time range of metric points.
    async fn query_range(&self, _query: MetricsQueryRange) -> Result<Vec<MetricPoint>> {
        Ok(Vec::new())
    }

    /// Batch write metrics (default: per-row dispatch).
    async fn record_metrics_batch(&self, rows: &[MetricWriteRow]) -> Result<()> {
        for row in rows {
            if row.kind == "counter" {
                let delta = row.value.as_i64().unwrap_or(0);
                self.record_counter(&row.name, &row.labels, delta, row.ts)
                    .await?;
            } else {
                let value = row.value.as_f64().unwrap_or(0.0);
                self.record_gauge(&row.name, &row.labels, value, row.ts)
                    .await?;
            }
        }
        Ok(())
    }
}

/// Event storage: subscribers call `append_row`; explore UI calls `query_rows`.
#[async_trait]
pub trait EventStorageBackend: Send + Sync {
    /// Returns the engine type for this backend.
    fn engine_type(&self) -> StorageEngineType;

    /// Appends one event row.
    async fn append_row(
        &self,
        table: &str,
        fields: &Value,
        ts: DateTime<Utc>,
        correlation_id: Option<&str>,
    ) -> Result<()>;

    /// Queries event rows matching the filter.
    async fn query_rows(&self, _filter: EventsQueryFilter) -> Result<Vec<EventRow>> {
        Ok(Vec::new())
    }

    /// Queries aggregated chart data.
    async fn query_aggregate(
        &self,
        _filter: EventsAggregateFilter,
    ) -> Result<crate::query::EventAggregateResult> {
        Ok(crate::query::EventAggregateResult::TimeSeries {
            series: Vec::new(),
            headline: Vec::new(),
        })
    }

    /// Batch append event rows (default: per-row dispatch).
    async fn append_rows_batch(&self, rows: &[EventWriteRow]) -> Result<()> {
        for row in rows {
            self.append_row(
                &row.table,
                &row.fields,
                row.ts,
                row.correlation_id.as_deref(),
            )
            .await?;
        }
        Ok(())
    }
}

/// No-op metrics backend (default before host wiring).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpMetricsBackend;

#[async_trait]
impl MetricsStorageBackend for NoOpMetricsBackend {
    fn engine_type(&self) -> StorageEngineType {
        StorageEngineType::NoOp
    }

    async fn record_counter(&self, _: &str, _: &Value, _: i64, _: DateTime<Utc>) -> Result<()> {
        Ok(())
    }

    async fn record_gauge(&self, _: &str, _: &Value, _: f64, _: DateTime<Utc>) -> Result<()> {
        Ok(())
    }
}

/// No-op event backend (default before host wiring).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpEventBackend;

#[async_trait]
impl EventStorageBackend for NoOpEventBackend {
    fn engine_type(&self) -> StorageEngineType {
        StorageEngineType::NoOp
    }

    async fn append_row(
        &self,
        _: &str,
        _: &Value,
        _: DateTime<Utc>,
        _: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }
}

/// Shared handle to a metrics storage backend.
pub type SharedMetricsBackend = Arc<dyn MetricsStorageBackend>;
/// Shared handle to an event storage backend.
pub type SharedEventBackend = Arc<dyn EventStorageBackend>;
