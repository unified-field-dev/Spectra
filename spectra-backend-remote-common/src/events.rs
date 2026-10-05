//! Remote events storage backend (ClickHouse-compatible protocol).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use spectra_core::{
    aggregate_event_rows, aggregate_rows_to_result, pivot_from_cells, Error, EventAggregateResult,
    EventExploreView, EventRow, EventStorageBackend, EventWriteRow, EventsAggregateFilter,
    EventsQueryFilter, PivotCell, Result, StorageEngineType,
};

use crate::client::{
    datetime_to_ch_ts, map_remote, parse_rfc3339_ts, EventInsertRow, RemoteClient,
};
use crate::mem_store::MemStore;
use crate::query_sql;

#[derive(Clone)]
enum EventsInner {
    Remote(Arc<RemoteClient>),
    Mem(Arc<MemStore>),
}

/// Parameterized remote events backend shared by ClickHouse and TensorBase adapters.
pub struct RemoteEventsBackend {
    inner: EventsInner,
    engine: StorageEngineType,
}

impl RemoteEventsBackend {
    /// Connect to a remote engine and ensure the events table exists.
    pub async fn connect(url: &str, engine: StorageEngineType, events_ddl: &str) -> Result<Self> {
        let client = Arc::new(RemoteClient::connect(url).await?);
        client.execute(events_ddl).await?;
        Ok(Self {
            inner: EventsInner::Remote(client),
            engine,
        })
    }

    /// Connect scoped to one database, creating it first if needed — physical per-store
    /// isolation (a distinct database per `store:` name rather than one shared database).
    /// See [`crate::RemoteMetricsBackend::connect_in_database`] for the two-phase rationale.
    ///
    /// # Errors
    ///
    /// Returns [`spectra_core::Error::Config`] when `database` is not a valid Spectra
    /// identifier, or a storage error when connect, `CREATE DATABASE`, or table DDL fails.
    pub async fn connect_in_database(
        url: &str,
        database: &str,
        engine: StorageEngineType,
        events_ddl: &str,
    ) -> Result<Self> {
        spectra_core::validate_spectra_ident(database)?;
        let bootstrap = RemoteClient::connect(url).await?;
        bootstrap
            .execute(&format!("CREATE DATABASE IF NOT EXISTS {database}"))
            .await?;
        let client = Arc::new(RemoteClient::connect_scoped(url, database).await?);
        client.execute(events_ddl).await?;
        Ok(Self {
            inner: EventsInner::Remote(client),
            engine,
        })
    }

    /// In-memory backend for unit tests (no remote server required).
    pub fn in_memory_for_test(engine: StorageEngineType) -> Self {
        Self {
            inner: EventsInner::Mem(Arc::new(MemStore::new())),
            engine,
        }
    }
}

#[async_trait]
impl EventStorageBackend for RemoteEventsBackend {
    fn engine_type(&self) -> StorageEngineType {
        self.engine
    }

    async fn append_row(
        &self,
        table: &str,
        fields: &Value,
        ts: DateTime<Utc>,
        correlation_id: Option<&str>,
    ) -> Result<()> {
        match &self.inner {
            EventsInner::Mem(store) => store.append_row(table, fields, ts),
            EventsInner::Remote(client) => {
                let mut insert = client.insert_events().await?;
                insert
                    .write(&EventInsertRow {
                        table_name: table.to_string(),
                        fields: fields.to_string(),
                        ts: datetime_to_ch_ts(ts),
                        correlation_id: correlation_id.map(str::to_string),
                    })
                    .await
                    .map_err(map_remote)?;
                insert.end().await.map_err(map_remote)?;
                Ok(())
            }
        }
    }

    async fn append_rows_batch(&self, rows: &[EventWriteRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        match &self.inner {
            EventsInner::Mem(store) => {
                for row in rows {
                    store.append_row(&row.table, &row.fields, row.ts)?;
                }
                Ok(())
            }
            EventsInner::Remote(client) => {
                let mut insert = client.insert_events().await?;
                for row in rows {
                    insert
                        .write(&EventInsertRow {
                            table_name: row.table.clone(),
                            fields: row.fields.to_string(),
                            ts: datetime_to_ch_ts(row.ts),
                            correlation_id: row.correlation_id.clone(),
                        })
                        .await
                        .map_err(map_remote)?;
                }
                insert.end().await.map_err(map_remote)?;
                Ok(())
            }
        }
    }

    async fn query_rows(&self, filter: EventsQueryFilter) -> Result<Vec<EventRow>> {
        match &self.inner {
            EventsInner::Mem(store) => store.query_rows(filter),
            EventsInner::Remote(client) => {
                let scope = query_sql::scope_clause(&filter)?;
                let extra = query_sql::filter_where_clause(&filter.filter)?;
                let order = query_sql::order_clause(&filter)?;
                let paging = query_sql::paging_clause(&filter);
                let sql = format!(
                    "SELECT fields, ts FROM spectra_events WHERE {scope}{extra} {order} {paging}"
                );
                let rows = client.query_event_rows(&sql).await?;
                let mut out = Vec::new();
                for (fields, ts) in rows {
                    out.push(EventRow {
                        ts: parse_rfc3339_ts(&ts)?,
                        fields: serde_json::from_str(&fields)?,
                    });
                }
                Ok(out)
            }
        }
    }

    async fn query_aggregate(&self, filter: EventsAggregateFilter) -> Result<EventAggregateResult> {
        match &self.inner {
            EventsInner::Mem(store) => {
                let rows = store.query_rows(EventsQueryFilter {
                    table: filter.table.clone(),
                    start: Some(filter.start),
                    end: Some(filter.end),
                    partition: filter.partition.clone(),
                    filter: filter.filter.clone(),
                    ..Default::default()
                })?;
                Ok(aggregate_event_rows(filter.view, &filter, &rows))
            }
            EventsInner::Remote(client) => {
                let Some(sql) = query_sql::aggregate_sql(&filter)? else {
                    return Ok(aggregate_event_rows(filter.view, &filter, &[]));
                };
                let groups = client.query_aggregate_rows(&sql).await?;
                aggregate_result_from_groups(&filter, groups)
            }
        }
    }
}

/// Shapes `(label, value)` rows from [`query_sql::aggregate_sql`] into the chart result for
/// `filter.view`, matching what [`aggregate_event_rows`] returns for the same rows in memory.
fn aggregate_result_from_groups(
    filter: &EventsAggregateFilter,
    groups: Vec<(String, f64)>,
) -> Result<EventAggregateResult> {
    match filter.view {
        EventExploreView::EventLog => Ok(aggregate_event_rows(filter.view, filter, &[])),
        EventExploreView::TimeSeries | EventExploreView::LineChart => {
            let mut buckets = groups
                .into_iter()
                .map(|(label, value)| {
                    let ts = label
                        .parse::<i64>()
                        .ok()
                        .and_then(|epoch| DateTime::<Utc>::from_timestamp(epoch, 0))
                        .ok_or_else(|| {
                            Error::internal(format!(
                                "invalid aggregate bucket {label:?} \
                                 (operation=query_aggregate, table={})",
                                filter.table
                            ))
                        })?;
                    Ok((ts, value))
                })
                .collect::<Result<Vec<_>>>()?;
            buckets.sort_by_key(|(ts, _)| *ts);
            let rows = buckets
                .into_iter()
                .map(|(ts, value)| json!({ "bucket": ts.to_rfc3339(), "value": value }))
                .collect();
            Ok(aggregate_rows_to_result(filter.view, rows, filter.measure))
        }
        EventExploreView::PieChart | EventExploreView::BarChart => {
            if groups.is_empty() {
                return Ok(EventAggregateResult::Slices {
                    slices: Vec::new(),
                    headline: Vec::new(),
                });
            }
            let mut groups = groups;
            groups.sort_by(|a, b| a.0.cmp(&b.0));
            let rows = groups
                .into_iter()
                .map(|(label, value)| json!({ "label": label, "value": value }))
                .collect();
            Ok(aggregate_rows_to_result(filter.view, rows, filter.measure))
        }
        EventExploreView::Table => {
            let has_pivot = filter.pivot_field_name().is_some();
            let expected = filter.row_field_names().len() + usize::from(has_pivot);
            let cells = groups
                .into_iter()
                .map(|(label, value)| {
                    let mut parts: Vec<String> = serde_json::from_str(&label)?;
                    if parts.len() != expected {
                        return Err(Error::internal(format!(
                            "aggregate table key has {} labels, expected {expected} \
                             (operation=query_aggregate, table={})",
                            parts.len(),
                            filter.table
                        )));
                    }
                    let pivot_key = if has_pivot { parts.pop() } else { None };
                    Ok(PivotCell {
                        row_values: parts,
                        pivot_key,
                        value,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(pivot_from_cells(filter, cells))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use spectra_core::{EventMeasure, GridFilterModel};

    fn agg(view: EventExploreView) -> EventsAggregateFilter {
        let end = Utc::now();
        EventsAggregateFilter {
            table: "t".into(),
            start: end - Duration::hours(1),
            end,
            partition: None,
            filter: GridFilterModel::default(),
            measure: EventMeasure::Count,
            measure_field: None,
            time_bucket_secs: Some(60),
            group_by_field: Some("severity".into()),
            row_fields: vec!["region".into()],
            pivot_field: Some("severity".into()),
            view,
        }
    }

    #[test]
    fn time_series_groups_sort_by_bucket() {
        let groups = vec![
            ("1791238560".to_string(), 2.0),
            ("1791238500".to_string(), 5.0),
        ];
        match aggregate_result_from_groups(&agg(EventExploreView::TimeSeries), groups)
            .expect("time series")
        {
            EventAggregateResult::TimeSeries { series, headline } => {
                let points: Vec<(i64, f64)> = series[0]
                    .points
                    .iter()
                    .map(|p| (p.ts.timestamp(), p.value))
                    .collect();
                assert_eq!(points, vec![(1_791_238_500, 5.0), (1_791_238_560, 2.0)]);
                assert!(headline
                    .iter()
                    .any(|c| c.label == "Total" && c.value == "7"));
            }
            other => panic!("expected time series, got {other:?}"),
        }
    }

    #[test]
    fn time_series_rejects_non_epoch_bucket() {
        let err = aggregate_result_from_groups(
            &agg(EventExploreView::LineChart),
            vec![("2026-10-05".to_string(), 1.0)],
        )
        .expect_err("bad bucket");
        assert!(matches!(err, Error::Internal(ref m) if m.contains("invalid aggregate bucket")));
    }

    #[test]
    fn slices_sort_by_label_and_empty_has_no_headline() {
        let filter = agg(EventExploreView::PieChart);
        match aggregate_result_from_groups(
            &filter,
            vec![("warn".to_string(), 1.0), ("info".to_string(), 3.0)],
        )
        .expect("slices")
        {
            EventAggregateResult::Slices { slices, .. } => {
                let labels: Vec<&str> = slices.iter().map(|s| s.label.as_str()).collect();
                assert_eq!(labels, vec!["info", "warn"]);
            }
            other => panic!("expected slices, got {other:?}"),
        }
        match aggregate_result_from_groups(&filter, Vec::new()).expect("empty slices") {
            EventAggregateResult::Slices { slices, headline } => {
                assert!(slices.is_empty() && headline.is_empty());
            }
            other => panic!("expected slices, got {other:?}"),
        }
    }

    #[test]
    fn table_decodes_row_and_pivot_labels() {
        let groups = vec![
            (r#"["us","info"]"#.to_string(), 2.0),
            (r#"["us","(blank)"]"#.to_string(), 1.0),
        ];
        match aggregate_result_from_groups(&agg(EventExploreView::Table), groups).expect("pivot") {
            EventAggregateResult::Pivot {
                column_keys, rows, ..
            } => {
                assert_eq!(column_keys, vec!["info", "(blank)"]);
                assert_eq!(rows[0].row_values, vec!["us"]);
                assert_eq!(rows[0].cells, vec![2.0, 1.0]);
            }
            other => panic!("expected pivot, got {other:?}"),
        }
    }

    #[test]
    fn table_rejects_wrong_label_arity_and_bad_json() {
        let filter = agg(EventExploreView::Table);
        let arity = aggregate_result_from_groups(&filter, vec![(r#"["us"]"#.to_string(), 1.0)])
            .expect_err("arity");
        assert!(matches!(arity, Error::Internal(ref m) if m.contains("expected 2")));
        let json = aggregate_result_from_groups(&filter, vec![("not json".to_string(), 1.0)])
            .expect_err("json");
        assert!(matches!(json, Error::Json(_)));
    }

    #[tokio::test]
    async fn in_memory_aggregate_counts_rows() {
        let backend = RemoteEventsBackend::in_memory_for_test(StorageEngineType::ClickHouse);
        let now = Utc::now();
        for severity in ["info", "info", "warn"] {
            backend
                .append_row("t", &json!({ "severity": severity }), now, None)
                .await
                .expect("append");
        }
        let filter = EventsAggregateFilter {
            start: now - Duration::seconds(1),
            end: now + Duration::seconds(1),
            ..agg(EventExploreView::PieChart)
        };
        match backend.query_aggregate(filter).await.expect("aggregate") {
            EventAggregateResult::Slices { slices, .. } => {
                let got: Vec<(&str, f64)> =
                    slices.iter().map(|s| (s.label.as_str(), s.value)).collect();
                assert_eq!(got, vec![("info", 2.0), ("warn", 1.0)]);
            }
            other => panic!("expected slices, got {other:?}"),
        }
    }
}
