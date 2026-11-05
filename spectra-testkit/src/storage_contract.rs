//! Shared [`MetricsStorageBackend`] and [`EventStorageBackend`] contract checks for adapters.

use std::sync::Arc;

use chrono::{Duration, Utc};
use serde_json::json;
use spectra::spectra_core::LabelMatcher;
use spectra::spectra_core::{
    EventAggregateResult, EventExploreView, EventMeasure, EventStorageBackend,
    EventsAggregateFilter, EventsQueryFilter, GridFilterItem, GridFilterModel, GridFilterOperator,
    MetricsQueryRange, MetricsStorageBackend, Result, StorageEngineType, StorageStats,
};

/// Run the shared storage port contract against one metrics/events backend pair.
pub async fn run_storage_contract(
    metrics: Arc<dyn MetricsStorageBackend>,
    events: Arc<dyn EventStorageBackend>,
) -> Result<()> {
    counter_roundtrip(metrics.as_ref()).await?;
    gauge_roundtrip(metrics.as_ref()).await?;
    event_roundtrip(events.as_ref()).await?;
    event_equals_filter(events.as_ref()).await?;
    event_contains_filter(events.as_ref()).await?;
    event_sort_and_pagination(events.as_ref()).await?;
    event_partition_filter(events.as_ref()).await?;
    event_aggregate_views(events.as_ref()).await?;
    event_aggregate_beyond_query_cap(events.as_ref()).await?;
    event_stats_roundtrip(events.as_ref()).await?;
    metric_stats_roundtrip(metrics.as_ref()).await?;
    label_filter_hit(metrics.as_ref()).await?;
    label_filter_miss(metrics.as_ref()).await?;
    time_range_empty(metrics.as_ref()).await?;
    // Remote engines prune with an asynchronous, uncapped mutation, so only the embedded
    // engines can assert exact bounded results here.
    if matches!(
        events.engine_type(),
        StorageEngineType::Mem | StorageEngineType::Sqlite
    ) {
        event_prune(events.as_ref()).await?;
    }
    if matches!(
        metrics.engine_type(),
        StorageEngineType::Mem | StorageEngineType::Sqlite
    ) {
        metrics_prune(metrics.as_ref()).await?;
    }
    Ok(())
}

async fn counter_roundtrip(metrics: &dyn MetricsStorageBackend) -> Result<()> {
    let ts = Utc::now();
    metrics
        .record_counter("contract_hits", &json!({"env": "test"}), 3, ts)
        .await?;
    let points = metrics
        .query_range(MetricsQueryRange {
            metric_name: "contract_hits".into(),
            start: ts - Duration::seconds(1),
            end: ts + Duration::seconds(1),
            label_matchers: vec![],
        })
        .await?;
    assert_eq!(points.len(), 1, "counter roundtrip");
    assert!((points[0].value - 3.0).abs() < f64::EPSILON);
    Ok(())
}

async fn gauge_roundtrip(metrics: &dyn MetricsStorageBackend) -> Result<()> {
    let ts = Utc::now();
    metrics
        .record_gauge("contract_load", &json!({"host": "a"}), 0.75, ts)
        .await?;
    let points = metrics
        .query_range(MetricsQueryRange {
            metric_name: "contract_load".into(),
            start: ts - Duration::seconds(1),
            end: ts + Duration::seconds(1),
            label_matchers: vec![],
        })
        .await?;
    assert_eq!(points.len(), 1, "gauge roundtrip");
    assert!((points[0].value - 0.75).abs() < f64::EPSILON);
    Ok(())
}

async fn event_roundtrip(events: &dyn EventStorageBackend) -> Result<()> {
    let ts = Utc::now();
    events
        .append_row("contract_log", &json!({"msg": "ok"}), ts, None)
        .await?;
    let rows = events
        .query_rows(EventsQueryFilter {
            table: "contract_log".into(),
            start: Some(ts - Duration::seconds(1)),
            end: Some(ts + Duration::seconds(1)),
            ..Default::default()
        })
        .await?;
    assert_eq!(rows.len(), 1, "event roundtrip");
    Ok(())
}

async fn event_equals_filter(events: &dyn EventStorageBackend) -> Result<()> {
    let ts = Utc::now();
    let table = "contract_filter_eq";
    events
        .append_row(table, &json!({"region": "us-west"}), ts, None)
        .await?;
    events
        .append_row(table, &json!({"region": "eu-central"}), ts, None)
        .await?;
    let rows = events
        .query_rows(EventsQueryFilter {
            table: table.into(),
            start: Some(ts - Duration::seconds(1)),
            end: Some(ts + Duration::seconds(1)),
            filter: GridFilterModel {
                items: vec![GridFilterItem {
                    field: "region".into(),
                    operator: GridFilterOperator::Equals,
                    value: json!("us-west"),
                }],
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    assert_eq!(rows.len(), 1, "event equals filter");
    assert_eq!(rows[0].fields["region"], "us-west");
    Ok(())
}

async fn event_contains_filter(events: &dyn EventStorageBackend) -> Result<()> {
    let ts = Utc::now();
    let table = "contract_filter_contains";
    events
        .append_row(table, &json!({"msg": "hello world"}), ts, None)
        .await?;
    events
        .append_row(table, &json!({"msg": "goodbye"}), ts, None)
        .await?;
    let rows = events
        .query_rows(EventsQueryFilter {
            table: table.into(),
            start: Some(ts - Duration::seconds(1)),
            end: Some(ts + Duration::seconds(1)),
            filter: GridFilterModel {
                items: vec![GridFilterItem {
                    field: "msg".into(),
                    operator: GridFilterOperator::Contains,
                    value: json!("WORLD"),
                }],
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    assert_eq!(rows.len(), 1, "event contains filter");
    Ok(())
}

async fn event_sort_and_pagination(events: &dyn EventStorageBackend) -> Result<()> {
    let ts = Utc::now();
    let table = "contract_sort_page";
    events
        .append_row(table, &json!({"name": "b"}), ts, None)
        .await?;
    events
        .append_row(
            table,
            &json!({"name": "a"}),
            ts + Duration::milliseconds(1),
            None,
        )
        .await?;
    events
        .append_row(
            table,
            &json!({"name": "c"}),
            ts + Duration::milliseconds(2),
            None,
        )
        .await?;
    let rows = events
        .query_rows(EventsQueryFilter {
            table: table.into(),
            start: Some(ts - Duration::seconds(1)),
            end: Some(ts + Duration::seconds(1)),
            sort_field: Some("name".into()),
            sort_desc: false,
            limit: Some(2),
            offset: Some(0),
            ..Default::default()
        })
        .await?;
    assert_eq!(rows.len(), 2, "event sort pagination length");
    assert_eq!(rows[0].fields["name"], "a");
    assert_eq!(rows[1].fields["name"], "b");
    Ok(())
}

async fn event_partition_filter(events: &dyn EventStorageBackend) -> Result<()> {
    let ts = Utc::now();
    let table = "contract_partition";
    events
        .append_row(table, &json!({"partition": "hourly", "msg": "h"}), ts, None)
        .await?;
    events
        .append_row(table, &json!({"partition": "daily", "msg": "d"}), ts, None)
        .await?;
    let rows = events
        .query_rows(EventsQueryFilter {
            table: table.into(),
            start: Some(ts - Duration::seconds(1)),
            end: Some(ts + Duration::seconds(1)),
            partition: Some("hourly".into()),
            ..Default::default()
        })
        .await?;
    assert_eq!(rows.len(), 1, "event partition filter");
    assert_eq!(rows[0].fields["msg"], "h");
    Ok(())
}

async fn label_filter_hit(metrics: &dyn MetricsStorageBackend) -> Result<()> {
    let ts = Utc::now();
    metrics
        .record_counter("contract_labeled", &json!({"region": "us-west"}), 1, ts)
        .await?;
    let points = metrics
        .query_range(MetricsQueryRange {
            metric_name: "contract_labeled".into(),
            start: ts - Duration::seconds(1),
            end: ts + Duration::seconds(1),
            label_matchers: vec![LabelMatcher {
                key: "region".into(),
                value: "us-west".into(),
            }],
        })
        .await?;
    assert_eq!(points.len(), 1, "label filter hit");
    Ok(())
}

async fn label_filter_miss(metrics: &dyn MetricsStorageBackend) -> Result<()> {
    let ts = Utc::now();
    metrics
        .record_counter(
            "contract_labeled_miss",
            &json!({"region": "us-west"}),
            1,
            ts,
        )
        .await?;
    let points = metrics
        .query_range(MetricsQueryRange {
            metric_name: "contract_labeled_miss".into(),
            start: ts - Duration::seconds(1),
            end: ts + Duration::seconds(1),
            label_matchers: vec![LabelMatcher {
                key: "region".into(),
                value: "eu-central".into(),
            }],
        })
        .await?;
    assert!(points.is_empty(), "label filter miss");
    Ok(())
}

async fn time_range_empty(metrics: &dyn MetricsStorageBackend) -> Result<()> {
    let ts = Utc::now();
    metrics
        .record_counter("contract_time_range", &json!({}), 1, ts)
        .await?;
    let points = metrics
        .query_range(MetricsQueryRange {
            metric_name: "contract_time_range".into(),
            start: ts - Duration::hours(2),
            end: ts - Duration::hours(1),
            label_matchers: vec![],
        })
        .await?;
    assert!(points.is_empty(), "time range empty");
    Ok(())
}

/// Every chart view must aggregate real rows, not return an empty stub.
async fn event_aggregate_views(events: &dyn EventStorageBackend) -> Result<()> {
    let ts = Utc::now();
    let table = "contract_aggregate";
    for fields in [
        json!({"severity": "info", "region": "us", "bytes": 10}),
        json!({"severity": "warn", "region": "us", "bytes": 5}),
        json!({"severity": "info", "region": "eu", "bytes": 7}),
        // No severity; string bytes must not count toward Sum.
        json!({"region": "eu", "bytes": "1"}),
    ] {
        events.append_row(table, &fields, ts, None).await?;
    }
    let base = EventsAggregateFilter {
        table: table.into(),
        start: ts - Duration::seconds(1),
        end: ts + Duration::seconds(1),
        partition: None,
        filter: GridFilterModel::default(),
        measure: EventMeasure::Count,
        measure_field: None,
        time_bucket_secs: Some(60),
        group_by_field: None,
        row_fields: Vec::new(),
        pivot_field: None,
        view: EventExploreView::TimeSeries,
    };

    let count = events.query_aggregate(base.clone()).await?;
    assert_eq!(series_total(&count), 4.0, "aggregate time series count");

    let sum = events
        .query_aggregate(EventsAggregateFilter {
            view: EventExploreView::LineChart,
            measure: EventMeasure::Sum,
            measure_field: Some("bytes".into()),
            ..base.clone()
        })
        .await?;
    assert_eq!(series_total(&sum), 22.0, "aggregate time series sum");

    let by_severity = EventsAggregateFilter {
        view: EventExploreView::PieChart,
        group_by_field: Some("severity".into()),
        ..base.clone()
    };
    let pie = events.query_aggregate(by_severity.clone()).await?;
    assert_eq!(
        slices(&pie),
        vec![
            ("(blank)".to_string(), 1.0),
            ("info".to_string(), 2.0),
            ("warn".to_string(), 1.0)
        ],
        "aggregate pie count buckets rows without the group field as (blank)"
    );

    let bar = events
        .query_aggregate(EventsAggregateFilter {
            view: EventExploreView::BarChart,
            group_by_field: Some("region".into()),
            measure: EventMeasure::Sum,
            measure_field: Some("bytes".into()),
            ..base.clone()
        })
        .await?;
    assert_eq!(
        slices(&bar),
        vec![("eu".to_string(), 7.0), ("us".to_string(), 15.0)],
        "aggregate bar sum"
    );

    let filtered = events
        .query_aggregate(EventsAggregateFilter {
            filter: GridFilterModel {
                items: vec![GridFilterItem {
                    field: "region".into(),
                    operator: GridFilterOperator::Equals,
                    value: json!("us"),
                }],
                ..Default::default()
            },
            ..by_severity.clone()
        })
        .await?;
    assert_eq!(
        slices(&filtered),
        vec![("info".to_string(), 1.0), ("warn".to_string(), 1.0)],
        "aggregate honors the grid filter"
    );

    let missing = events
        .query_aggregate(EventsAggregateFilter {
            group_by_field: Some("no_such_field".into()),
            ..by_severity
        })
        .await?;
    match missing {
        EventAggregateResult::Slices { slices, headline } => {
            assert!(
                slices.is_empty() && headline.is_empty(),
                "missing group field"
            );
        }
        other => panic!("expected slices for missing group field, got {other:?}"),
    }

    let pivot = events
        .query_aggregate(EventsAggregateFilter {
            view: EventExploreView::Table,
            row_fields: vec!["region".into()],
            pivot_field: Some("severity".into()),
            ..base
        })
        .await?;
    match pivot {
        EventAggregateResult::Pivot {
            row_fields,
            column_keys,
            rows,
            ..
        } => {
            assert_eq!(row_fields, vec!["region".to_string()]);
            assert_eq!(
                column_keys,
                vec!["info", "(blank)", "warn"],
                "pivot columns"
            );
            let cells: Vec<(Vec<String>, Vec<f64>)> =
                rows.into_iter().map(|r| (r.row_values, r.cells)).collect();
            assert_eq!(
                cells,
                vec![
                    (vec!["eu".to_string()], vec![1.0, 1.0, 0.0]),
                    (vec!["us".to_string()], vec![1.0, 0.0, 1.0]),
                ],
                "pivot cells"
            );
        }
        other => panic!("expected pivot, got {other:?}"),
    }
    Ok(())
}

/// Aggregates count every matching row, even past the 1,000-row event query cap.
async fn event_aggregate_beyond_query_cap(events: &dyn EventStorageBackend) -> Result<()> {
    const ROWS: u32 = 1100;
    let ts = Utc::now();
    let table = "contract_aggregate_large";
    for i in 0..ROWS {
        let severity = if i % 2 == 0 { "info" } else { "warn" };
        events
            .append_row(table, &json!({"severity": severity}), ts, None)
            .await?;
    }
    let base = EventsAggregateFilter {
        table: table.into(),
        start: ts - Duration::seconds(1),
        end: ts + Duration::seconds(1),
        partition: None,
        filter: GridFilterModel::default(),
        measure: EventMeasure::Count,
        measure_field: None,
        time_bucket_secs: Some(60),
        group_by_field: None,
        row_fields: Vec::new(),
        pivot_field: None,
        view: EventExploreView::TimeSeries,
    };
    let count = events.query_aggregate(base.clone()).await?;
    assert_eq!(series_total(&count), f64::from(ROWS), "count past row cap");

    let pie = events
        .query_aggregate(EventsAggregateFilter {
            group_by_field: Some("severity".into()),
            view: EventExploreView::PieChart,
            ..base
        })
        .await?;
    let mut got = slices(&pie);
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        got,
        vec![("info".into(), 550.0), ("warn".into(), 550.0)],
        "pie slices past row cap"
    );
    Ok(())
}

fn series_total(result: &EventAggregateResult) -> f64 {
    match result {
        EventAggregateResult::TimeSeries { series, .. } => series
            .iter()
            .flat_map(|s| s.points.iter())
            .map(|p| p.value)
            .sum(),
        other => panic!("expected time series, got {other:?}"),
    }
}

fn slices(result: &EventAggregateResult) -> Vec<(String, f64)> {
    match result {
        EventAggregateResult::Slices { slices, .. } => {
            slices.iter().map(|s| (s.label.clone(), s.value)).collect()
        }
        other => panic!("expected slices, got {other:?}"),
    }
}

async fn event_stats_roundtrip(events: &dyn EventStorageBackend) -> Result<()> {
    let missing = events.event_stats("contract_stats_missing").await?;
    assert_eq!(missing, StorageStats::default(), "unknown table is empty");

    let base = Utc::now();
    let table = "contract_stats_events";
    // Written out of order so min/max cannot just be first/last insert.
    let times = [
        base + Duration::seconds(5),
        base,
        base + Duration::seconds(10),
    ];
    for ts in times {
        events.append_row(table, &json!({"n": 1}), ts, None).await?;
    }
    events
        .append_row(
            "contract_stats_other",
            &json!({}),
            base - Duration::hours(1),
            None,
        )
        .await?;
    let stats = events.event_stats(table).await?;
    assert_eq!(stats.rows, 3, "event stats row count");
    assert_eq!(stats.first_ts, Some(base), "event stats first_ts");
    assert_eq!(
        stats.last_ts,
        Some(base + Duration::seconds(10)),
        "event stats last_ts"
    );
    Ok(())
}

async fn metric_stats_roundtrip(metrics: &dyn MetricsStorageBackend) -> Result<()> {
    let missing = metrics.metric_stats("contract_stats_missing").await?;
    assert_eq!(missing, StorageStats::default(), "unknown metric is empty");

    let base = Utc::now();
    let name = "contract_stats_metric";
    metrics
        .record_counter(name, &json!({}), 1, base + Duration::seconds(7))
        .await?;
    metrics.record_gauge(name, &json!({}), 2.0, base).await?;
    metrics
        .record_counter(
            "contract_stats_other_metric",
            &json!({}),
            1,
            base - Duration::hours(1),
        )
        .await?;
    let stats = metrics.metric_stats(name).await?;
    assert_eq!(stats.rows, 2, "metric stats row count");
    assert_eq!(stats.first_ts, Some(base), "metric stats first_ts");
    assert_eq!(
        stats.last_ts,
        Some(base + Duration::seconds(7)),
        "metric stats last_ts"
    );
    Ok(())
}

async fn event_prune(events: &dyn EventStorageBackend) -> Result<()> {
    let now = Utc::now();
    let old = now - Duration::days(10);
    let cutoff = now - Duration::days(1);
    for _ in 0..3 {
        events
            .append_row("contract_prune", &json!({"age": "old"}), old, None)
            .await?;
    }
    events
        .append_row("contract_prune", &json!({"age": "new"}), now, None)
        .await?;
    events
        .append_row("contract_prune_other", &json!({"age": "old"}), old, None)
        .await?;

    let first = events.prune_before("contract_prune", cutoff, 2).await?;
    assert_eq!((first.deleted, first.more), (2, true), "capped prune");
    let second = events.prune_before("contract_prune", cutoff, 2).await?;
    assert_eq!((second.deleted, second.more), (1, false), "final prune");

    let rows = |table: &str| EventsQueryFilter {
        table: table.into(),
        start: Some(old - Duration::days(1)),
        end: Some(now + Duration::seconds(1)),
        ..Default::default()
    };
    let kept = events.query_rows(rows("contract_prune")).await?;
    assert_eq!(kept.len(), 1, "recent row survives prune");
    let other = events.query_rows(rows("contract_prune_other")).await?;
    assert_eq!(other.len(), 1, "other table untouched");
    Ok(())
}

async fn metrics_prune(metrics: &dyn MetricsStorageBackend) -> Result<()> {
    let now = Utc::now();
    let old = now - Duration::days(10);
    metrics
        .record_counter("contract_prune_hits", &json!({}), 1, old)
        .await?;
    metrics
        .record_counter("contract_prune_hits", &json!({}), 1, now)
        .await?;
    let outcome = metrics.prune_before(now - Duration::days(1), 100).await?;
    assert!(outcome.deleted >= 1 && !outcome.more, "metrics prune");
    let points = metrics
        .query_range(MetricsQueryRange {
            metric_name: "contract_prune_hits".into(),
            start: old - Duration::days(1),
            end: now + Duration::seconds(1),
            label_matchers: vec![],
        })
        .await?;
    assert_eq!(points.len(), 1, "recent metric survives prune");
    Ok(())
}
