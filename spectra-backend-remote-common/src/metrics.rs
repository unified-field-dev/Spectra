//! Remote metrics storage backend (ClickHouse-compatible protocol).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use spectra_core::{
    MetricPoint, MetricWriteRow, MetricsQueryRange, MetricsStorageBackend, Result,
    StorageEngineType,
};

use crate::client::{
    datetime_to_ch_ts, map_remote, parse_rfc3339_ts, MetricInsertRow, RemoteClient,
};
use crate::mem_store::MemStore;
use crate::query_sql;

#[derive(Clone)]
enum MetricsInner {
    Remote(Arc<RemoteClient>),
    Mem(Arc<MemStore>),
}

/// Parameterized remote metrics backend shared by ClickHouse and TensorBase adapters.
pub struct RemoteMetricsBackend {
    inner: MetricsInner,
    engine: StorageEngineType,
}

impl RemoteMetricsBackend {
    /// Connect to a remote engine and ensure the metrics table exists.
    pub async fn connect(url: &str, engine: StorageEngineType, metrics_ddl: &str) -> Result<Self> {
        let client = Arc::new(RemoteClient::connect(url).await?);
        client.execute(metrics_ddl).await?;
        Ok(Self {
            inner: MetricsInner::Remote(client),
            engine,
        })
    }

    /// Connect scoped to one database, creating it first if needed — physical per-store
    /// isolation (a distinct database per `store:` name rather than one shared database).
    ///
    /// Two-phase: an unscoped bootstrap connection issues `CREATE DATABASE IF NOT EXISTS`
    /// (a client already scoped to a not-yet-existing database can fail on any query,
    /// including the one that would create it), then a database-scoped connection is
    /// reused for the metrics table DDL and every subsequent read/write.
    ///
    /// # Errors
    ///
    /// Returns [`spectra_core::Error::Config`] when `database` is not a valid Spectra
    /// identifier, or a storage error when connect, `CREATE DATABASE`, or table DDL fails.
    pub async fn connect_in_database(
        url: &str,
        database: &str,
        engine: StorageEngineType,
        metrics_ddl: &str,
    ) -> Result<Self> {
        spectra_core::validate_spectra_ident(database)?;
        let bootstrap = RemoteClient::connect(url).await?;
        bootstrap
            .execute(&format!("CREATE DATABASE IF NOT EXISTS {database}"))
            .await?;
        let client = Arc::new(RemoteClient::connect_scoped(url, database).await?);
        client.execute(metrics_ddl).await?;
        Ok(Self {
            inner: MetricsInner::Remote(client),
            engine,
        })
    }

    /// In-memory backend for unit tests (no remote server required).
    pub fn in_memory_for_test(engine: StorageEngineType) -> Self {
        Self {
            inner: MetricsInner::Mem(Arc::new(MemStore::new())),
            engine,
        }
    }
}

#[async_trait]
impl MetricsStorageBackend for RemoteMetricsBackend {
    fn engine_type(&self) -> StorageEngineType {
        self.engine
    }

    async fn record_counter(
        &self,
        name: &str,
        labels: &Value,
        delta: i64,
        ts: DateTime<Utc>,
    ) -> Result<()> {
        match &self.inner {
            MetricsInner::Mem(store) => store.record_counter(name, labels, delta, ts),
            MetricsInner::Remote(client) => {
                let mut insert = client.insert_metrics().await?;
                insert
                    .write(&MetricInsertRow {
                        name: name.to_string(),
                        kind: "counter".into(),
                        value: delta as f64,
                        labels: labels.to_string(),
                        ts: datetime_to_ch_ts(ts),
                        correlation_id: None,
                    })
                    .await
                    .map_err(map_remote)?;
                insert.end().await.map_err(map_remote)?;
                Ok(())
            }
        }
    }

    async fn record_gauge(
        &self,
        name: &str,
        labels: &Value,
        value: f64,
        ts: DateTime<Utc>,
    ) -> Result<()> {
        match &self.inner {
            MetricsInner::Mem(store) => store.record_gauge(name, labels, value, ts),
            MetricsInner::Remote(client) => {
                let mut insert = client.insert_metrics().await?;
                insert
                    .write(&MetricInsertRow {
                        name: name.to_string(),
                        kind: "gauge".into(),
                        value,
                        labels: labels.to_string(),
                        ts: datetime_to_ch_ts(ts),
                        correlation_id: None,
                    })
                    .await
                    .map_err(map_remote)?;
                insert.end().await.map_err(map_remote)?;
                Ok(())
            }
        }
    }

    async fn record_metrics_batch(&self, rows: &[MetricWriteRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        match &self.inner {
            MetricsInner::Mem(store) => {
                for row in rows {
                    if row.kind == "counter" {
                        let delta = row.value.as_i64().unwrap_or(0);
                        store.record_counter(&row.name, &row.labels, delta, row.ts)?;
                    } else {
                        let value = row.value.as_f64().unwrap_or(0.0);
                        store.record_gauge(&row.name, &row.labels, value, row.ts)?;
                    }
                }
                Ok(())
            }
            MetricsInner::Remote(client) => {
                let mut insert = client.insert_metrics().await?;
                for row in rows {
                    let (kind, value) = if row.kind == "counter" {
                        ("counter", row.value.as_i64().unwrap_or(0) as f64)
                    } else {
                        ("gauge", row.value.as_f64().unwrap_or(0.0))
                    };
                    insert
                        .write(&MetricInsertRow {
                            name: row.name.clone(),
                            kind: kind.into(),
                            value,
                            labels: row.labels.to_string(),
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

    async fn query_range(&self, query: MetricsQueryRange) -> Result<Vec<MetricPoint>> {
        match &self.inner {
            MetricsInner::Mem(store) => store.query_range(query),
            MetricsInner::Remote(client) => {
                spectra_core::validate_spectra_ident(&query.metric_name)?;
                let start = query.start.to_rfc3339();
                let end = query.end.to_rfc3339();
                let sql = format!(
                    "SELECT value, labels, ts FROM spectra_metrics \
                     WHERE name = '{}' AND ts >= '{}' AND ts <= '{}' ORDER BY ts ASC",
                    query_sql::escape_str(&query.metric_name)?,
                    query_sql::escape_str(&start)?,
                    query_sql::escape_str(&end)?
                );
                let rows = client.query_metric_rows(&sql).await?;
                let mut out = Vec::new();
                for (value, labels, ts) in rows {
                    out.push(MetricPoint {
                        ts: parse_rfc3339_ts(&ts)?,
                        value,
                        labels: serde_json::from_str(&labels)?,
                    });
                }
                if !query.label_matchers.is_empty() {
                    out.retain(|p| {
                        query.label_matchers.iter().all(|m| {
                            p.labels
                                .get(&m.key)
                                .and_then(|v| v.as_str())
                                .map(|v| v == m.value)
                                .unwrap_or(false)
                        })
                    });
                }
                Ok(out)
            }
        }
    }
}
