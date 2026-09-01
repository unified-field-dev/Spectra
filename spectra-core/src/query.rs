//! UI/server boundary query types for Spectra explore views.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Metric explore request (host server fn boundary).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsQuery {
    /// Metric family name to query.
    pub metric: String,
    /// Inclusive range start timestamp.
    pub start: DateTime<Utc>,
    /// Inclusive range end timestamp.
    pub end: DateTime<Utc>,
    /// Optional step size in seconds for downsampling.
    pub step_secs: Option<u64>,
    /// Label equality matchers applied to the series.
    #[serde(default)]
    pub label_matchers: Vec<LabelMatcher>,
}

/// Label key-value equality matcher for metric queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelMatcher {
    /// Label key.
    pub key: String,
    /// Label value.
    pub value: String,
}

/// One labeled time series returned to the explore UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesDto {
    /// Series label set as JSON.
    pub labels: Value,
    /// Ordered points in the requested time range.
    pub points: Vec<MetricPointDto>,
}

/// Single metric sample in a time series.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPointDto {
    /// Sample timestamp.
    pub ts: DateTime<Utc>,
    /// Sample value.
    pub value: f64,
}

/// Headline stat card shown above charts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatCardDto {
    /// Card label.
    pub label: String,
    /// Formatted display value.
    pub value: String,
}

/// Metric explore response payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsQueryResult {
    /// Time series to render.
    pub series: Vec<TimeSeriesDto>,
    /// Headline summary cards.
    pub headline: Vec<StatCardDto>,
}

/// Event explore view selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EventExploreView {
    /// Paginated event log grid.
    #[default]
    EventLog,
    /// Time-bucketed time series.
    TimeSeries,
    /// Line chart view.
    LineChart,
    /// Pie chart view.
    PieChart,
    /// Bar chart view.
    BarChart,
}

/// Mirrors MUI `GridPaginationModel`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridPaginationModel {
    /// Zero-based page index.
    pub page: u32,
    /// Rows per page.
    pub page_size: u32,
}

impl Default for GridPaginationModel {
    fn default() -> Self {
        Self {
            page: 0,
            page_size: 50,
        }
    }
}

/// Sort direction for grid columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GridSortDirection {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

/// Single column sort specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridSortItem {
    /// Column field name.
    pub field: String,
    /// Sort direction.
    pub sort: GridSortDirection,
}

/// Logic operator combining multiple grid filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum GridLogicOperator {
    /// All filter items must match.
    #[default]
    And,
    /// Any filter item may match.
    Or,
}

/// Filter operator for a single grid column.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridFilterOperator {
    /// Exact equality.
    Equals,
    /// Inequality.
    DoesNotEqual,
    /// Substring containment.
    Contains,
    /// Prefix match.
    StartsWith,
    /// Suffix match.
    EndsWith,
    /// Field is empty or null.
    IsEmpty,
    /// Field is non-empty.
    IsNotEmpty,
    /// Numeric or lexical greater than.
    GreaterThan,
    /// Numeric or lexical greater than or equal.
    GreaterThanOrEqual,
    /// Numeric or lexical less than.
    LessThan,
    /// Numeric or lexical less than or equal.
    LessThanOrEqual,
}

/// One filter predicate on a grid column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridFilterItem {
    /// Column field name.
    pub field: String,
    /// Comparison operator.
    pub operator: GridFilterOperator,
    /// Operand value (type depends on operator).
    pub value: Value,
}

/// Full filter model for event log queries.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GridFilterModel {
    /// Structured filter items.
    #[serde(default)]
    pub items: Vec<GridFilterItem>,
    /// How to combine `items`.
    #[serde(default)]
    pub logic_operator: GridLogicOperator,
    /// Quick-search tokens applied across columns.
    #[serde(default)]
    pub quick_filter_values: Vec<String>,
}

/// Time partition granularity for event queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartitionKind {
    /// Hourly partitions.
    Hourly,
    /// Daily partitions.
    Daily,
}

/// Row-oriented event log query (DataGrid / event log view).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventQuery {
    /// Event table name.
    pub table: String,
    /// Inclusive range start timestamp.
    pub start: DateTime<Utc>,
    /// Inclusive range end timestamp.
    pub end: DateTime<Utc>,
    /// Optional partition granularity.
    pub partition: Option<PartitionKind>,
    /// Pagination settings.
    pub pagination: GridPaginationModel,
    /// Column sort order (first entry wins).
    pub sort: Vec<GridSortItem>,
    /// Row filter model.
    pub filter: GridFilterModel,
}

/// Column descriptor for event log grids.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridColumnDto {
    /// Field key matching row JSON.
    pub field: String,
    /// Human-readable column header.
    pub header_name: String,
}

/// One row in an event log grid response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventGridRow {
    /// Stable row identifier.
    pub id: String,
    /// Event timestamp.
    pub ts: DateTime<Utc>,
    /// Event field payload.
    pub fields: Value,
}

/// Event log query response payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventQueryResult {
    /// Grid rows for the current page.
    pub rows: Vec<EventGridRow>,
    /// Column definitions.
    pub columns: Vec<GridColumnDto>,
    /// Total matching row count (may exceed `rows.len()`).
    pub row_count: u64,
}

/// Aggregation measure for chart views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventMeasure {
    /// Count matching rows.
    Count,
    /// Sum a numeric field.
    Sum,
}

/// Aggregation parameters for event chart queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventAggregationSpec {
    /// Measure to compute.
    pub measure: EventMeasure,
    /// Field to sum when `measure` is [`EventMeasure::Sum`].
    pub measure_field: Option<String>,
    /// Time bucket width in seconds for time-series views.
    pub time_bucket_secs: Option<u64>,
    /// Field to group by for slice views.
    pub group_by_field: Option<String>,
}

/// Event chart aggregate request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventAggregateRequest {
    /// Event table name.
    pub table: String,
    /// Inclusive range start timestamp.
    pub start: DateTime<Utc>,
    /// Inclusive range end timestamp.
    pub end: DateTime<Utc>,
    /// Optional partition granularity.
    pub partition: Option<PartitionKind>,
    /// Row filter model.
    pub filter: GridFilterModel,
    /// Target chart view.
    pub view: EventExploreView,
    /// Aggregation specification.
    pub aggregation: EventAggregationSpec,
}

/// One slice in a pie or bar chart response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceDto {
    /// Slice label.
    pub label: String,
    /// Slice value.
    pub value: f64,
}

/// Event aggregate query response (shape depends on view).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventAggregateResult {
    /// Time-series or line-chart payload.
    TimeSeries {
        /// Series to render.
        series: Vec<TimeSeriesDto>,
        /// Headline summary cards.
        headline: Vec<StatCardDto>,
    },
    /// Pie or bar chart payload.
    Slices {
        /// Category slices.
        slices: Vec<SliceDto>,
        /// Headline summary cards.
        headline: Vec<StatCardDto>,
    },
}

/// Schema catalog DTO for spectra-app UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaListItem {
    /// Table or metric name.
    pub table_or_metric: String,
    /// Optional schema description.
    pub description: Option<String>,
    /// `"event"` or `"metric"`.
    pub logging_kind: String,
    /// Whether the schema supports explore queries.
    pub can_query: bool,
}

/// Full schema detail for the explore UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDetailDto {
    /// Table or metric name.
    pub table_or_metric: String,
    /// Optional schema description.
    pub description: Option<String>,
    /// `"event"` or `"metric"`.
    pub logging_kind: String,
    /// Schema version string.
    pub version: String,
    /// Registered field metadata.
    pub fields: Vec<SchemaFieldDto>,
    /// Whether the schema supports explore queries.
    pub can_query: bool,
}

/// Field metadata in a schema detail response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaFieldDto {
    /// Field name.
    pub name: String,
    /// Rust type name as registered.
    pub rust_type: String,
    /// Serialized [`FieldClassification`](crate::FieldClassification) label.
    pub classification: String,
}
