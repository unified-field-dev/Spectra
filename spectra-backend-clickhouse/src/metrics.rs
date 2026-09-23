//! ClickHouse metrics storage adapter.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use spectra_backend_remote_common::RemoteMetricsBackend;
use spectra_core::{
    MetricPoint, MetricWriteRow, MetricsQueryRange, MetricsStorageBackend, Result,
    StorageEngineType,
};

/// Remote ClickHouse metrics storage.
///
/// [`connect`](Self::connect) opens the ClickHouse client and creates the Spectra metrics table
/// when needed. A running Spectra instance requires both this backend and
/// `ClickHouseEventsBackend`; remote storage does not use `.embedded()`.
///
/// # Examples
///
/// Public crate wiring through `Spectra::builder()` (requires the `spectra` crate with the
/// `clickhouse` feature):
///
/// ```ignore
/// use std::sync::Arc;
/// use spectra::{ClickHouseEventsBackend, ClickHouseMetricsBackend, Spectra};
///
/// # async fn start() -> spectra::Result<()> {
/// let url = "https://clickhouse.example:8443";
/// // Local plaintext: SPECTRA_ALLOW_INSECURE_REMOTE=1 + http://127.0.0.1:8123
/// let spectra = Spectra::builder()
///     .metrics_backend(Arc::new(ClickHouseMetricsBackend::connect(url).await?))
///     .events_backend(Arc::new(ClickHouseEventsBackend::connect(url).await?))
///     .build()?;
/// # let _ = spectra;
/// # Ok(())
/// # }
/// ```
pub struct ClickHouseMetricsBackend(RemoteMetricsBackend);

impl ClickHouseMetricsBackend {
    /// Connect to ClickHouse over HTTP or native protocol and ensure metrics tables exist.
    ///
    /// `url` is typically `https://host:8443` (or `http://host:8123` with
    /// `SPECTRA_ALLOW_INSECURE_REMOTE=1`). The call is async and executes DDL, so the
    /// ClickHouse credentials need table-creation permission.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # async fn example() -> spectra_core::Result<()> {
    /// use spectra_backend_clickhouse::ClickHouseMetricsBackend;
    ///
    /// let backend = ClickHouseMetricsBackend::connect("https://clickhouse.example:8443").await?;
    /// # let _ = backend;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_in_store(url, "default").await
    }

    /// Connect scoped to one Spectra `store:` name — physical per-store isolation via a
    /// dedicated `spectra_{store}` ClickHouse database on the same server, created if
    /// needed. See [`spectra_backend_remote_common::RemoteMetricsBackend::connect_in_database`]
    /// for the two-phase connect this builds on.
    ///
    /// # Errors
    ///
    /// Returns an error when `store` is not a valid Spectra identifier, or when connect,
    /// `CREATE DATABASE`, or table DDL fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # async fn example() -> spectra_core::Result<()> {
    /// use spectra_backend_clickhouse::ClickHouseMetricsBackend;
    ///
    /// let backend =
    ///     ClickHouseMetricsBackend::connect_in_store("https://clickhouse.example:8443", "counter")
    ///         .await?;
    /// # let _ = backend;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_in_store(url: &str, store: &str) -> Result<Self> {
        let database = format!("spectra_{store}");
        Ok(Self(
            RemoteMetricsBackend::connect_in_database(
                url,
                &database,
                StorageEngineType::ClickHouse,
                &crate::ddl::metrics_ddl(),
            )
            .await?,
        ))
    }

    /// In-memory stub that preserves the ClickHouse engine type for storage-contract tests.
    ///
    /// # Examples
    ///
    /// ```
    /// use spectra_backend_clickhouse::ClickHouseMetricsBackend;
    /// use spectra_core::{MetricsStorageBackend, StorageEngineType};
    ///
    /// let backend = ClickHouseMetricsBackend::in_memory_stub();
    /// assert_eq!(backend.engine_type(), StorageEngineType::ClickHouse);
    /// ```
    pub fn in_memory_stub() -> Self {
        Self(RemoteMetricsBackend::in_memory_for_test(
            StorageEngineType::ClickHouse,
        ))
    }

    /// In-memory stub for unit tests in this crate.
    #[cfg(test)]
    pub fn in_memory_for_test() -> Self {
        Self::in_memory_stub()
    }
}

#[async_trait]
impl MetricsStorageBackend for ClickHouseMetricsBackend {
    fn engine_type(&self) -> StorageEngineType {
        self.0.engine_type()
    }

    async fn record_counter(
        &self,
        name: &str,
        labels: &Value,
        delta: i64,
        ts: DateTime<Utc>,
    ) -> Result<()> {
        self.0.record_counter(name, labels, delta, ts).await
    }

    async fn record_gauge(
        &self,
        name: &str,
        labels: &Value,
        value: f64,
        ts: DateTime<Utc>,
    ) -> Result<()> {
        self.0.record_gauge(name, labels, value, ts).await
    }

    async fn record_metrics_batch(&self, rows: &[MetricWriteRow]) -> Result<()> {
        self.0.record_metrics_batch(rows).await
    }

    async fn query_range(&self, query: MetricsQueryRange) -> Result<Vec<MetricPoint>> {
        self.0.query_range(query).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use serde_json::json;

    #[tokio::test]
    async fn clickhouse_metrics_roundtrip_in_memory() {
        let backend = ClickHouseMetricsBackend::in_memory_for_test();
        let ts = Utc::now();
        backend
            .record_counter("hits", &json!({}), 2, ts)
            .await
            .expect("write");
        let points = backend
            .query_range(MetricsQueryRange {
                metric_name: "hits".into(),
                start: ts - Duration::seconds(1),
                end: ts + Duration::seconds(1),
                label_matchers: vec![],
            })
            .await
            .expect("query");
        assert_eq!(points.len(), 1);
    }

    #[tokio::test]
    #[ignore = "requires SPECTRA_CLICKHOUSE_URL"]
    async fn clickhouse_metrics_integration() {
        let url = std::env::var("SPECTRA_CLICKHOUSE_URL").expect("SPECTRA_CLICKHOUSE_URL");
        let backend = ClickHouseMetricsBackend::connect(&url)
            .await
            .expect("connect");
        let ts = Utc::now();
        backend
            .record_counter("integration_hits", &json!({}), 1, ts)
            .await
            .expect("write");
        let points = backend
            .query_range(MetricsQueryRange {
                metric_name: "integration_hits".into(),
                start: ts - Duration::seconds(5),
                end: ts + Duration::seconds(5),
                label_matchers: vec![],
            })
            .await
            .expect("query");
        assert!(!points.is_empty());
    }

    #[tokio::test]
    async fn connect_in_store_rejects_invalid_store_name_sad() {
        // Validation runs before any network I/O, so this is a plain (non-`#[ignore]`)
        // unit test even though `connect_in_store` is otherwise a remote-connect API.
        let result = ClickHouseMetricsBackend::connect_in_store(
            "https://clickhouse.example:8443",
            "bad; store",
        )
        .await;
        let err = result
            .err()
            .expect("invalid store identifier must be rejected");
        assert!(matches!(err, spectra_core::Error::Config(_)));
    }

    #[tokio::test]
    #[ignore = "requires SPECTRA_CLICKHOUSE_URL"]
    async fn clickhouse_metrics_connect_in_store_isolates_databases_integration() {
        let url = std::env::var("SPECTRA_CLICKHOUSE_URL").expect("SPECTRA_CLICKHOUSE_URL");
        let store_a =
            ClickHouseMetricsBackend::connect_in_store(&url, "isolation_integration_test_a")
                .await
                .expect("connect store a");
        let store_b =
            ClickHouseMetricsBackend::connect_in_store(&url, "isolation_integration_test_b")
                .await
                .expect("connect store b");
        let ts = Utc::now();
        store_a
            .record_counter("isolation_integration_hits", &json!({}), 1, ts)
            .await
            .expect("write store a");

        let seen_in_b = store_b
            .query_range(MetricsQueryRange {
                metric_name: "isolation_integration_hits".into(),
                start: ts - Duration::seconds(5),
                end: ts + Duration::seconds(5),
                label_matchers: vec![],
            })
            .await
            .expect("query store b");
        assert!(
            seen_in_b.is_empty(),
            "store a's row must not be visible from store b's database"
        );
    }
}
