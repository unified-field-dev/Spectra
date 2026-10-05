//! Map UI query DTOs to adapter filters and results.

use chrono::Utc;
use serde_json::{json, Value};

use crate::query::{
    EventAggregateRequest, EventAggregateResult, EventExploreView, EventGridRow, EventMeasure,
    EventQuery, EventQueryResult, GridColumnDto, GridSortDirection, MetricsQuery,
    MetricsQueryResult, PivotRowDto, SchemaDetailDto, SchemaFieldDto, SchemaListItem, SliceDto,
    StatCardDto, TimeSeriesDto,
};
use crate::registry::{LoggingKind, SchemaMetadata, SchemaRegistry};
use crate::storage::{EventRow, EventsAggregateFilter, EventsQueryFilter, MetricPoint};

/// Converts a UI event query DTO into a storage adapter filter.
pub fn event_query_to_filter(q: &EventQuery) -> EventsQueryFilter {
    let sort = q.sort.first();
    EventsQueryFilter {
        table: q.table.clone(),
        start: Some(q.start),
        end: Some(q.end),
        partition: q.partition.map(|p| match p {
            crate::query::PartitionKind::Hourly => "hourly".to_string(),
            crate::query::PartitionKind::Daily => "daily".to_string(),
        }),
        limit: Some(q.pagination.page_size),
        offset: Some(q.pagination.page * q.pagination.page_size),
        sort_field: sort.map(|s| s.field.clone()),
        sort_desc: sort
            .map(|s| s.sort == GridSortDirection::Desc)
            .unwrap_or(true),
        filter: q.filter.clone(),
    }
}

/// Converts an aggregate request DTO into a storage adapter filter.
pub fn aggregate_request_to_filter(req: &EventAggregateRequest) -> EventsAggregateFilter {
    EventsAggregateFilter {
        table: req.table.clone(),
        start: req.start,
        end: req.end,
        partition: req.partition.map(|p| match p {
            crate::query::PartitionKind::Hourly => "hourly".to_string(),
            crate::query::PartitionKind::Daily => "daily".to_string(),
        }),
        filter: req.filter.clone(),
        measure: req.aggregation.measure,
        measure_field: req.aggregation.measure_field.clone(),
        time_bucket_secs: req.aggregation.time_bucket_secs,
        group_by_field: req.aggregation.group_by_field.clone(),
        row_fields: req.aggregation.row_fields.clone(),
        pivot_field: req.aggregation.pivot_field.clone(),
        view: req.view,
    }
}

/// Maps storage event rows into a UI event query result.
pub fn rows_to_event_result(table: &str, rows: Vec<EventRow>, row_count: u64) -> EventQueryResult {
    let meta = SchemaRegistry::global().get_schema(table);
    let columns = meta.map(schema_columns).unwrap_or_else(|| {
        vec![GridColumnDto {
            field: "ts".into(),
            header_name: "Timestamp".into(),
        }]
    });
    let grid_rows = rows
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            let id = r
                .fields
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("row-{i}"));
            EventGridRow {
                id,
                ts: r.ts,
                fields: r.fields,
            }
        })
        .collect();
    EventQueryResult {
        rows: grid_rows,
        columns,
        row_count,
    }
}

fn schema_columns(meta: &SchemaMetadata) -> Vec<GridColumnDto> {
    let mut cols = vec![GridColumnDto {
        field: "ts".into(),
        header_name: "Timestamp".into(),
    }];
    for f in &meta.fields {
        cols.push(GridColumnDto {
            field: f.name.clone(),
            header_name: f.name.clone(),
        });
    }
    cols
}

/// Lists all registered schemas as catalog DTOs.
pub fn list_schemas() -> Vec<SchemaListItem> {
    SchemaRegistry::global()
        .list_schemas()
        .into_iter()
        .filter_map(|name| {
            SchemaRegistry::global()
                .get_schema(name)
                .map(|m| schema_list_item(m))
        })
        .collect()
}

fn schema_list_item(m: &SchemaMetadata) -> SchemaListItem {
    SchemaListItem {
        table_or_metric: m.table_or_metric.clone(),
        description: m.description.clone(),
        logging_kind: match m.logging_kind {
            LoggingKind::Event => "event".into(),
            LoggingKind::Metric => "metric".into(),
        },
        can_query: true,
    }
}

/// Returns full schema detail for a table or metric name.
pub fn schema_detail(name: &str) -> Option<SchemaDetailDto> {
    SchemaRegistry::global()
        .get_schema(name)
        .map(|m| SchemaDetailDto {
            table_or_metric: m.table_or_metric.clone(),
            description: m.description.clone(),
            logging_kind: match m.logging_kind {
                LoggingKind::Event => "event".into(),
                LoggingKind::Metric => "metric".into(),
            },
            version: m.version.clone(),
            fields: m
                .fields
                .iter()
                .map(|f| SchemaFieldDto {
                    name: f.name.clone(),
                    rust_type: f.rust_type.clone(),
                    classification: format!("{:?}", f.classification),
                })
                .collect(),
            can_query: true,
        })
}

/// Converts a UI metrics query DTO into a storage range filter.
pub fn metrics_query_to_range(q: &MetricsQuery) -> crate::storage::MetricsQueryRange {
    crate::storage::MetricsQueryRange {
        metric_name: q.metric.clone(),
        start: q.start,
        end: q.end,
        label_matchers: q.label_matchers.clone(),
    }
}

/// Maps storage metric points into a UI metrics query result.
pub fn points_to_metrics_result(points: Vec<MetricPoint>) -> MetricsQueryResult {
    let series = vec![TimeSeriesDto {
        labels: json!({}),
        points: points
            .iter()
            .map(|p| crate::query::MetricPointDto {
                ts: p.ts,
                value: p.value,
            })
            .collect(),
    }];
    let headline = metrics_headline(&points);
    MetricsQueryResult { series, headline }
}

fn metrics_headline(points: &[MetricPoint]) -> Vec<StatCardDto> {
    if points.is_empty() {
        return vec![
            StatCardDto {
                label: "Points".into(),
                value: "0".into(),
            },
            StatCardDto {
                label: "Max".into(),
                value: "—".into(),
            },
            StatCardDto {
                label: "Last".into(),
                value: "—".into(),
            },
        ];
    }
    let max = points
        .iter()
        .map(|p| p.value)
        .fold(f64::NEG_INFINITY, f64::max);
    let last = points.last().map(|p| p.value).unwrap_or(0.0);
    vec![
        StatCardDto {
            label: "Points".into(),
            value: points.len().to_string(),
        },
        StatCardDto {
            label: "Max".into(),
            value: format!("{max:.2}"),
        },
        StatCardDto {
            label: "Last".into(),
            value: format!("{last:.2}"),
        },
    ]
}

/// Maps aggregate storage rows into a UI chart result for the requested view.
pub fn aggregate_rows_to_result(
    view: EventExploreView,
    rows: Vec<Value>,
    measure: EventMeasure,
) -> EventAggregateResult {
    match view {
        EventExploreView::TimeSeries | EventExploreView::LineChart => {
            let mut points = Vec::new();
            for row in &rows {
                let bucket = row
                    .get("bucket")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let value = row.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let ts = chrono::DateTime::parse_from_rfc3339(&bucket)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());
                points.push(crate::query::MetricPointDto { ts, value });
            }
            let headline = vec![
                StatCardDto {
                    label: "Buckets".into(),
                    value: points.len().to_string(),
                },
                StatCardDto {
                    label: "Total".into(),
                    value: format!("{:.0}", points.iter().map(|p| p.value).sum::<f64>()),
                },
                StatCardDto {
                    label: "Measure".into(),
                    value: format!("{measure:?}"),
                },
            ];
            EventAggregateResult::TimeSeries {
                series: vec![TimeSeriesDto {
                    labels: json!({}),
                    points,
                }],
                headline,
            }
        }
        EventExploreView::PieChart | EventExploreView::BarChart => {
            let slices: Vec<SliceDto> = rows
                .iter()
                .map(|row| SliceDto {
                    label: row
                        .get("label")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?")
                        .to_string(),
                    value: row.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0),
                })
                .collect();
            let total: f64 = slices.iter().map(|s| s.value).sum();
            let headline = vec![
                StatCardDto {
                    label: "Slices".into(),
                    value: slices.len().to_string(),
                },
                StatCardDto {
                    label: "Total".into(),
                    value: format!("{total:.0}"),
                },
                StatCardDto {
                    label: "Measure".into(),
                    value: format!("{measure:?}"),
                },
            ];
            EventAggregateResult::Slices { slices, headline }
        }
        EventExploreView::EventLog => EventAggregateResult::TimeSeries {
            series: Vec::new(),
            headline: Vec::new(),
        },
        EventExploreView::Table => EventAggregateResult::Pivot {
            row_fields: Vec::new(),
            column_keys: Vec::new(),
            rows: Vec::new(),
            headline: Vec::new(),
        },
    }
}

/// Aggregates in-memory event rows into a chart result for `view`.
///
/// Mem and SQLite backends call this after [`crate::EventStorageBackend::query_rows`].
/// Pie/Bar without `group_by_field` return empty slices. Sum without a numeric
/// `measure_field` contributes `0.0` per row. Table (`Pivot`) groups by
/// `row_fields` and optionally spreads `pivot_field` values into columns, capping
/// distinct pivot keys at the top 50 by measure plus an `(other)` bucket.
///
/// # Examples
///
/// ```
/// use chrono::{Duration, Utc};
/// use serde_json::json;
/// use spectra_core::{
///     aggregate_event_rows, EventExploreView, EventMeasure, EventRow, EventsAggregateFilter,
///     EventAggregateResult, GridFilterModel,
/// };
///
/// let end = Utc::now();
/// let start = end - Duration::hours(1);
/// let rows = vec![
///     EventRow {
///         ts: end - Duration::minutes(5),
///         fields: json!({"severity": "info", "value": 10}),
///     },
///     EventRow {
///         ts: end - Duration::minutes(4),
///         fields: json!({"severity": "warn", "value": 3}),
///     },
/// ];
/// let filter = EventsAggregateFilter {
///     table: "demo.events".into(),
///     start,
///     end,
///     partition: None,
///     filter: GridFilterModel::default(),
///     measure: EventMeasure::Count,
///     measure_field: None,
///     time_bucket_secs: None,
///     group_by_field: Some("severity".into()),
///     row_fields: vec![],
///     pivot_field: None,
///     view: EventExploreView::PieChart,
/// };
/// let result = aggregate_event_rows(filter.view, &filter, &rows);
/// match result {
///     EventAggregateResult::Slices { slices, .. } => {
///         assert_eq!(slices.len(), 2);
///     }
///     other => panic!("expected slices, got {other:?}"),
/// }
/// ```
pub fn aggregate_event_rows(
    view: EventExploreView,
    filter: &EventsAggregateFilter,
    rows: &[EventRow],
) -> EventAggregateResult {
    match view {
        EventExploreView::EventLog => EventAggregateResult::TimeSeries {
            series: Vec::new(),
            headline: Vec::new(),
        },
        EventExploreView::TimeSeries | EventExploreView::LineChart => {
            let bucket_secs = filter.bucket_secs();
            let mut buckets: std::collections::BTreeMap<i64, f64> =
                std::collections::BTreeMap::new();
            for row in rows {
                let aligned = align_bucket_ts(row.ts, bucket_secs);
                let contrib =
                    row_measure_value(row, filter.measure, filter.measure_field.as_deref());
                *buckets.entry(aligned).or_insert(0.0) += contrib;
            }
            let mapped: Vec<Value> = buckets
                .into_iter()
                .map(|(epoch, value)| {
                    let ts =
                        chrono::DateTime::<Utc>::from_timestamp(epoch, 0).unwrap_or(filter.end);
                    json!({
                        "bucket": ts.to_rfc3339(),
                        "value": value,
                    })
                })
                .collect();
            aggregate_rows_to_result(view, mapped, filter.measure)
        }
        EventExploreView::PieChart | EventExploreView::BarChart => {
            let Some(group_field) = filter.group_by_field_name() else {
                return EventAggregateResult::Slices {
                    slices: Vec::new(),
                    headline: Vec::new(),
                };
            };
            let mut groups: std::collections::BTreeMap<String, f64> =
                std::collections::BTreeMap::new();
            let mut saw_field = false;
            for row in rows {
                let Some(raw) = row.fields.get(group_field) else {
                    continue;
                };
                saw_field = true;
                let label = match raw {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                let contrib =
                    row_measure_value(row, filter.measure, filter.measure_field.as_deref());
                *groups.entry(label).or_insert(0.0) += contrib;
            }
            if !saw_field {
                return EventAggregateResult::Slices {
                    slices: Vec::new(),
                    headline: Vec::new(),
                };
            }
            let mapped: Vec<Value> = groups
                .into_iter()
                .map(|(label, value)| json!({ "label": label, "value": value }))
                .collect();
            aggregate_rows_to_result(view, mapped, filter.measure)
        }
        EventExploreView::Table => aggregate_table_pivot(filter, rows),
    }
}

const PIVOT_COLUMN_CAP: usize = 50;

/// Label used for a missing or JSON `null` Table row/pivot value.
pub const PIVOT_BLANK_LABEL: &str = "(blank)";

fn field_label(raw: &Value) -> String {
    match raw {
        Value::String(s) => s.clone(),
        Value::Null => PIVOT_BLANK_LABEL.into(),
        other => other.to_string(),
    }
}

/// One pre-grouped Table cell fed to [`pivot_from_cells`].
#[derive(Debug, Clone, PartialEq)]
pub struct PivotCell {
    /// Row-key labels, one per [`EventsAggregateFilter::row_field_names`] entry, in order.
    pub row_values: Vec<String>,
    /// Pivot-column label. Ignored when the filter has no pivot field; `None` maps to
    /// [`PIVOT_BLANK_LABEL`] when it does.
    pub pivot_key: Option<String>,
    /// Measure contribution (count or sum) for this row/pivot pair.
    pub value: f64,
}

fn aggregate_table_pivot(
    filter: &EventsAggregateFilter,
    rows: &[EventRow],
) -> EventAggregateResult {
    let row_fields = filter.row_field_names();
    let pivot_field = filter.pivot_field_name();
    let cells = rows.iter().filter_map(|row| {
        let row_values = row_fields
            .iter()
            .map(|field| row.fields.get(field).map(field_label))
            .collect::<Option<Vec<_>>>()?;
        let pivot_key = pivot_field.map(|pf| {
            row.fields
                .get(pf)
                .map(field_label)
                .unwrap_or_else(|| PIVOT_BLANK_LABEL.into())
        });
        Some(PivotCell {
            row_values,
            pivot_key,
            value: row_measure_value(row, filter.measure, filter.measure_field.as_deref()),
        })
    });
    pivot_from_cells(filter, cells)
}

/// Builds a Table (`Pivot`) result from cells that are already grouped or partially grouped.
///
/// Cells sharing a row key and pivot key are summed. With a pivot field, columns are ordered
/// by descending total (ties by label) and capped at the top 50 plus an `(other)` column.
/// Without one, the single column is the measure (`Count`, or the summed field name).
/// Returns an empty pivot when the filter has no row fields.
///
/// [`aggregate_event_rows`] uses this for in-memory rows; SQL backends feed it their
/// `GROUP BY` output so both paths share column capping and headline cards.
pub fn pivot_from_cells(
    filter: &EventsAggregateFilter,
    cells: impl IntoIterator<Item = PivotCell>,
) -> EventAggregateResult {
    let row_fields = filter.row_field_names();
    if row_fields.is_empty() {
        return EventAggregateResult::Pivot {
            row_fields: Vec::new(),
            column_keys: Vec::new(),
            rows: Vec::new(),
            headline: Vec::new(),
        };
    }
    let has_pivot = filter.pivot_field_name().is_some();

    let measure_header = match filter.measure {
        EventMeasure::Count => "Count".to_string(),
        EventMeasure::Sum => filter.measure_field.clone().unwrap_or_else(|| "Sum".into()),
    };

    // (row_key, pivot_key) -> value
    let mut grouped: std::collections::HashMap<(Vec<String>, String), f64> =
        std::collections::HashMap::new();
    let mut pivot_totals: std::collections::HashMap<String, f64> = std::collections::HashMap::new();

    for cell in cells {
        let pivot_key = if has_pivot {
            cell.pivot_key.unwrap_or_else(|| PIVOT_BLANK_LABEL.into())
        } else {
            measure_header.clone()
        };
        *grouped
            .entry((cell.row_values, pivot_key.clone()))
            .or_insert(0.0) += cell.value;
        *pivot_totals.entry(pivot_key).or_insert(0.0) += cell.value;
    }
    let mut cells = grouped;

    let mut column_keys: Vec<String> = if has_pivot {
        let mut ordered: Vec<(String, f64)> = pivot_totals.into_iter().collect();
        ordered.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        if ordered.len() > PIVOT_COLUMN_CAP {
            let keep: Vec<String> = ordered
                .iter()
                .take(PIVOT_COLUMN_CAP)
                .map(|(k, _)| k.clone())
                .collect();
            let mut capped_cells: std::collections::HashMap<(Vec<String>, String), f64> =
                std::collections::HashMap::new();
            for ((row_key, pivot_key), value) in cells {
                let key = if keep.iter().any(|k| k == &pivot_key) {
                    pivot_key
                } else {
                    "(other)".into()
                };
                *capped_cells.entry((row_key, key)).or_insert(0.0) += value;
            }
            cells = capped_cells;
            let mut keys = keep;
            keys.push("(other)".into());
            keys
        } else {
            ordered.into_iter().map(|(k, _)| k).collect()
        }
    } else {
        vec![measure_header]
    };

    if column_keys.is_empty() {
        column_keys.push("Count".into());
    }

    let mut row_map: std::collections::BTreeMap<Vec<String>, Vec<f64>> =
        std::collections::BTreeMap::new();
    for ((row_key, pivot_key), value) in cells {
        let entry = row_map
            .entry(row_key)
            .or_insert_with(|| vec![0.0; column_keys.len()]);
        if let Some(idx) = column_keys.iter().position(|k| k == &pivot_key) {
            entry[idx] += value;
        }
    }

    let pivot_rows: Vec<PivotRowDto> = row_map
        .into_iter()
        .map(|(row_values, cells)| PivotRowDto { row_values, cells })
        .collect();

    let total: f64 = pivot_rows.iter().flat_map(|r| r.cells.iter()).sum();
    let headline = vec![
        StatCardDto {
            label: "Rows".into(),
            value: pivot_rows.len().to_string(),
        },
        StatCardDto {
            label: "Columns".into(),
            value: column_keys.len().to_string(),
        },
        StatCardDto {
            label: "Total".into(),
            value: format!("{total:.0}"),
        },
    ];

    EventAggregateResult::Pivot {
        row_fields,
        column_keys,
        rows: pivot_rows,
        headline,
    }
}

fn align_bucket_ts(ts: chrono::DateTime<Utc>, bucket_secs: u64) -> i64 {
    let secs = bucket_secs as i64;
    let epoch = ts.timestamp();
    epoch - epoch.rem_euclid(secs)
}

fn row_measure_value(row: &EventRow, measure: EventMeasure, measure_field: Option<&str>) -> f64 {
    match measure {
        EventMeasure::Count => 1.0,
        EventMeasure::Sum => measure_field
            .and_then(|field| row.fields.get(field))
            .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
            .unwrap_or(0.0),
    }
}

#[cfg(test)]
mod aggregate_event_rows_tests {
    use super::*;
    use crate::query::GridFilterModel;
    use chrono::Duration;

    fn filter(
        view: EventExploreView,
        measure: EventMeasure,
        measure_field: Option<&str>,
        group_by: Option<&str>,
        bucket: Option<u64>,
    ) -> EventsAggregateFilter {
        let end = Utc::now();
        EventsAggregateFilter {
            table: "t".into(),
            start: end - Duration::hours(1),
            end,
            partition: None,
            filter: GridFilterModel::default(),
            measure,
            measure_field: measure_field.map(str::to_string),
            time_bucket_secs: bucket,
            group_by_field: group_by.map(str::to_string),
            row_fields: Vec::new(),
            pivot_field: None,
            view,
        }
    }

    fn table_filter(
        measure: EventMeasure,
        measure_field: Option<&str>,
        row_fields: &[&str],
        pivot_field: Option<&str>,
    ) -> EventsAggregateFilter {
        let end = Utc::now();
        EventsAggregateFilter {
            table: "t".into(),
            start: end - Duration::hours(1),
            end,
            partition: None,
            filter: GridFilterModel::default(),
            measure,
            measure_field: measure_field.map(str::to_string),
            time_bucket_secs: None,
            group_by_field: None,
            row_fields: row_fields.iter().map(|s| (*s).to_string()).collect(),
            pivot_field: pivot_field.map(str::to_string),
            view: EventExploreView::Table,
        }
    }

    fn row(minutes_ago: i64, severity: &str, value: i64) -> EventRow {
        EventRow {
            ts: Utc::now() - Duration::minutes(minutes_ago),
            fields: json!({"severity": severity, "value": value}),
        }
    }

    #[test]
    fn aggregate_event_rows_count_timeseries_happy() {
        let f = filter(
            EventExploreView::TimeSeries,
            EventMeasure::Count,
            None,
            None,
            Some(3600),
        );
        let rows = vec![row(5, "info", 10), row(4, "warn", 3), row(3, "info", 5)];
        match aggregate_event_rows(f.view, &f, &rows) {
            EventAggregateResult::TimeSeries { series, .. } => {
                let total: f64 = series
                    .iter()
                    .flat_map(|s| s.points.iter())
                    .map(|p| p.value)
                    .sum();
                assert!((total - 3.0).abs() < f64::EPSILON, "total={total}");
            }
            other => panic!("expected time series: {other:?}"),
        }
    }

    #[test]
    fn aggregate_event_rows_sum_timeseries_happy() {
        let f = filter(
            EventExploreView::LineChart,
            EventMeasure::Sum,
            Some("value"),
            None,
            Some(3600),
        );
        let rows = vec![row(5, "info", 10), row(4, "warn", 3), row(3, "info", 5)];
        match aggregate_event_rows(f.view, &f, &rows) {
            EventAggregateResult::TimeSeries { series, .. } => {
                let total: f64 = series
                    .iter()
                    .flat_map(|s| s.points.iter())
                    .map(|p| p.value)
                    .sum();
                assert!((total - 18.0).abs() < f64::EPSILON, "total={total}");
            }
            other => panic!("expected time series: {other:?}"),
        }
    }

    #[test]
    fn aggregate_event_rows_group_by_slices_happy() {
        let f = filter(
            EventExploreView::PieChart,
            EventMeasure::Count,
            None,
            Some("severity"),
            None,
        );
        let rows = vec![row(5, "info", 10), row(4, "warn", 3), row(3, "info", 5)];
        match aggregate_event_rows(f.view, &f, &rows) {
            EventAggregateResult::Slices { slices, .. } => {
                assert!(slices.len() >= 2, "{slices:?}");
                let labels: Vec<_> = slices.iter().map(|s| s.label.as_str()).collect();
                assert!(labels.contains(&"info"));
                assert!(labels.contains(&"warn"));
            }
            other => panic!("expected slices: {other:?}"),
        }
    }

    #[test]
    fn aggregate_event_rows_pie_missing_groupby_sad() {
        let f = filter(
            EventExploreView::PieChart,
            EventMeasure::Count,
            None,
            None,
            None,
        );
        let rows = vec![row(1, "info", 1)];
        match aggregate_event_rows(f.view, &f, &rows) {
            EventAggregateResult::Slices { slices, headline } => {
                assert!(slices.is_empty());
                assert!(headline.is_empty());
            }
            other => panic!("expected empty slices: {other:?}"),
        }
    }

    #[test]
    fn aggregate_event_rows_empty_table_sad() {
        let f = filter(
            EventExploreView::TimeSeries,
            EventMeasure::Count,
            None,
            None,
            Some(3600),
        );
        match aggregate_event_rows(f.view, &f, &[]) {
            EventAggregateResult::TimeSeries { series, .. } => {
                assert!(
                    series.iter().all(|s| s.points.is_empty())
                        || series.is_empty()
                        || series
                            .iter()
                            .flat_map(|s| &s.points)
                            .map(|p| p.value)
                            .sum::<f64>()
                            == 0.0
                );
            }
            other => panic!("expected time series: {other:?}"),
        }
    }

    #[test]
    fn aggregate_request_to_filter_copies_view() {
        let end = Utc::now();
        let req = EventAggregateRequest {
            table: "t".into(),
            start: end - Duration::hours(1),
            end,
            partition: None,
            filter: GridFilterModel::default(),
            view: EventExploreView::BarChart,
            aggregation: crate::query::EventAggregationSpec {
                measure: EventMeasure::Count,
                measure_field: None,
                time_bucket_secs: Some(60),
                group_by_field: Some("severity".into()),
                row_fields: vec![],
                pivot_field: None,
            },
        };
        let mapped = aggregate_request_to_filter(&req);
        assert_eq!(mapped.view, EventExploreView::BarChart);
        assert_eq!(mapped.group_by_field.as_deref(), Some("severity"));
    }

    #[test]
    fn aggregate_event_rows_table_pivot_happy() {
        let f = table_filter(EventMeasure::Count, None, &["severity"], Some("outcome"));
        let rows = vec![
            EventRow {
                ts: Utc::now() - Duration::minutes(5),
                fields: json!({"severity": "info", "value": 10, "outcome": "allow"}),
            },
            EventRow {
                ts: Utc::now() - Duration::minutes(4),
                fields: json!({"severity": "warn", "value": 3, "outcome": "deny"}),
            },
            EventRow {
                ts: Utc::now() - Duration::minutes(3),
                fields: json!({"severity": "info", "value": 5, "outcome": "allow"}),
            },
        ];
        match aggregate_event_rows(f.view, &f, &rows) {
            EventAggregateResult::Pivot {
                row_fields,
                column_keys,
                rows: pivot_rows,
                ..
            } => {
                assert_eq!(row_fields, vec!["severity".to_string()]);
                assert!(column_keys.iter().any(|k| k == "allow"));
                assert!(column_keys.iter().any(|k| k == "deny"));
                let info = pivot_rows
                    .iter()
                    .find(|r| r.row_values == ["info"])
                    .expect("info row");
                let allow_idx = column_keys.iter().position(|k| k == "allow").unwrap();
                assert!((info.cells[allow_idx] - 2.0).abs() < f64::EPSILON);
            }
            other => panic!("expected pivot: {other:?}"),
        }
    }

    #[test]
    fn aggregate_event_rows_table_flat_happy() {
        let f = table_filter(EventMeasure::Sum, Some("value"), &["severity"], None);
        let rows = vec![row(5, "info", 10), row(4, "warn", 3), row(3, "info", 5)];
        match aggregate_event_rows(f.view, &f, &rows) {
            EventAggregateResult::Pivot {
                column_keys,
                rows: pivot_rows,
                ..
            } => {
                assert_eq!(column_keys, vec!["value".to_string()]);
                let info = pivot_rows
                    .iter()
                    .find(|r| r.row_values == ["info"])
                    .expect("info");
                assert!((info.cells[0] - 15.0).abs() < f64::EPSILON);
            }
            other => panic!("expected flat pivot: {other:?}"),
        }
    }

    #[test]
    fn pivot_from_cells_merges_cells_and_blanks_missing_pivot() {
        let f = table_filter(EventMeasure::Count, None, &["region"], Some("severity"));
        let cell = |region: &str, pivot: Option<&str>, value: f64| PivotCell {
            row_values: vec![region.to_string()],
            pivot_key: pivot.map(str::to_string),
            value,
        };
        let result = pivot_from_cells(
            &f,
            [
                cell("us", Some("info"), 2.0),
                cell("us", Some("info"), 3.0),
                cell("eu", None, 1.0),
            ],
        );
        match result {
            EventAggregateResult::Pivot {
                column_keys,
                rows: pivot_rows,
                headline,
                ..
            } => {
                assert_eq!(column_keys, vec!["info", PIVOT_BLANK_LABEL]);
                let cells: Vec<(Vec<String>, Vec<f64>)> = pivot_rows
                    .into_iter()
                    .map(|r| (r.row_values, r.cells))
                    .collect();
                assert_eq!(
                    cells,
                    vec![
                        (vec!["eu".to_string()], vec![0.0, 1.0]),
                        (vec!["us".to_string()], vec![5.0, 0.0]),
                    ]
                );
                assert!(headline
                    .iter()
                    .any(|c| c.label == "Total" && c.value == "6"));
            }
            other => panic!("expected pivot: {other:?}"),
        }
    }

    #[test]
    fn pivot_from_cells_without_pivot_field_ignores_pivot_key() {
        let f = table_filter(EventMeasure::Sum, Some("bytes"), &["region"], None);
        let result = pivot_from_cells(
            &f,
            [PivotCell {
                row_values: vec!["us".into()],
                pivot_key: Some("stray".into()),
                value: 4.0,
            }],
        );
        match result {
            EventAggregateResult::Pivot { column_keys, .. } => {
                assert_eq!(column_keys, vec!["bytes"]);
            }
            other => panic!("expected pivot: {other:?}"),
        }
    }

    #[test]
    fn aggregate_filter_helpers_normalize_blank_and_zero() {
        let mut f = table_filter(EventMeasure::Count, None, &[" a ", "", "b"], Some("  "));
        assert_eq!(f.row_field_names(), vec!["a", "b"]);
        assert_eq!(f.pivot_field_name(), None);
        assert_eq!(f.group_by_field_name(), None);
        assert_eq!(
            f.bucket_secs(),
            EventsAggregateFilter::DEFAULT_TIME_BUCKET_SECS
        );
        f.time_bucket_secs = Some(0);
        assert_eq!(f.bucket_secs(), 1);
        f.group_by_field = Some(" severity ".into());
        assert_eq!(f.group_by_field_name(), Some("severity"));
    }

    #[test]
    fn aggregate_event_rows_table_no_row_fields_sad() {
        let f = table_filter(EventMeasure::Count, None, &[], None);
        let rows = vec![row(5, "info", 10)];
        match aggregate_event_rows(f.view, &f, &rows) {
            EventAggregateResult::Pivot {
                rows: pivot_rows, ..
            } => {
                assert!(pivot_rows.is_empty());
            }
            other => panic!("expected empty pivot: {other:?}"),
        }
    }
}
