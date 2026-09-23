//! TensorBase metrics storage adapter.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use spectra_backend_remote_common::RemoteMetricsBackend;
use spectra_core::{
    MetricPoint, MetricWriteRow, MetricsQueryRange, MetricsStorageBackend, Result,
    StorageEngineType,
};

/// Scale-out TensorBase metrics storage over its ClickHouse-compatible protocol.
///
/// Use [`connect`](Self::connect) with a full protocol URL or
/// [`connect_host`](Self::connect_host) for the default native port (`9528`). Pair this type
/// with `TensorBaseEventsBackend` when building the runtime.
///
/// # Examples
///
/// Public crate wiring through `Spectra::builder()` (requires the `spectra` crate with the
/// `tensorbase` feature):
///
/// ```ignore
/// use std::sync::Arc;
/// use spectra::{Spectra, TensorBaseEventsBackend, TensorBaseMetricsBackend};
///
/// # async fn start() -> spectra::Result<()> {
/// let url = "tcp+tls://tensorbase.example:9440";
/// // Local plaintext: SPECTRA_ALLOW_INSECURE_REMOTE=1 + tcp://127.0.0.1:9528
/// let spectra = Spectra::builder()
///     .metrics_backend(Arc::new(TensorBaseMetricsBackend::connect(url).await?))
///     .events_backend(Arc::new(TensorBaseEventsBackend::connect(url).await?))
///     .build()?;
/// # let _ = spectra;
/// # Ok(())
/// # }
/// ```
pub struct TensorBaseMetricsBackend(RemoteMetricsBackend);

impl TensorBaseMetricsBackend {
    /// Connect with a full TensorBase protocol URL and ensure metrics tables exist.
    ///
    /// Accepts `tcp+tls://` / `https://` (or plaintext `tcp://` / `http://` with
    /// `SPECTRA_ALLOW_INSECURE_REMOTE=1`). The call is async and executes DDL.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # async fn example() -> spectra_core::Result<()> {
    /// use spectra_backend_tensorbase::TensorBaseMetricsBackend;
    ///
    /// let backend = TensorBaseMetricsBackend::connect("tcp+tls://127.0.0.1:9440").await?;
    /// # let _ = backend;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_in_store(url, "default").await
    }

    /// Connect scoped to one Spectra `store:` name — physical per-store isolation via a
    /// dedicated `spectra_{store}` database on the same server, created if needed. See
    /// [`spectra_backend_remote_common::RemoteMetricsBackend::connect_in_database`] for the
    /// two-phase connect this builds on.
    ///
    /// No `spectra-uf-runtime`-style host installer wires this in yet (TensorBase has no
    /// live host installer) — this is the same connect primitive ClickHouse's per-store
    /// wiring uses, added here for parity since both share this crate's remote-protocol
    /// plumbing.
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
    /// use spectra_backend_tensorbase::TensorBaseMetricsBackend;
    ///
    /// let backend =
    ///     TensorBaseMetricsBackend::connect_in_store("tcp+tls://127.0.0.1:9440", "counter")
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
                StorageEngineType::TensorBase,
                &crate::ddl::metrics_ddl(),
            )
            .await?,
        ))
    }

    /// Connect using a host name and the default native port (`9528`).
    ///
    /// Equivalent to [`connect`](Self::connect) with `tcp://{host}:9528`
    /// (requires `SPECTRA_ALLOW_INSECURE_REMOTE=1` for that plaintext scheme).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # async fn example() -> spectra_core::Result<()> {
    /// use spectra_backend_tensorbase::TensorBaseMetricsBackend;
    ///
    /// let backend = TensorBaseMetricsBackend::connect_host("127.0.0.1").await?;
    /// # let _ = backend;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_host(host: &str) -> Result<Self> {
        Self::connect(&crate::ddl::default_url(host)).await
    }

    /// In-memory stub that preserves the TensorBase engine type for storage-contract tests.
    ///
    /// # Examples
    ///
    /// ```
    /// use spectra_backend_tensorbase::TensorBaseMetricsBackend;
    /// use spectra_core::{MetricsStorageBackend, StorageEngineType};
    ///
    /// let backend = TensorBaseMetricsBackend::in_memory_stub();
    /// assert_eq!(backend.engine_type(), StorageEngineType::TensorBase);
    /// ```
    pub fn in_memory_stub() -> Self {
        Self(RemoteMetricsBackend::in_memory_for_test(
            StorageEngineType::TensorBase,
        ))
    }

    /// In-memory stub for unit tests in this crate.
    #[cfg(test)]
    pub fn in_memory_for_test() -> Self {
        Self::in_memory_stub()
    }
}

#[async_trait]
impl MetricsStorageBackend for TensorBaseMetricsBackend {
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
    async fn tensorbase_metrics_roundtrip_in_memory() {
        let backend = TensorBaseMetricsBackend::in_memory_for_test();
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
    #[ignore = "requires SPECTRA_TENSORBASE_URL"]
    async fn tensorbase_metrics_integration() {
        let url = std::env::var("SPECTRA_TENSORBASE_URL").expect("SPECTRA_TENSORBASE_URL");
        let backend = TensorBaseMetricsBackend::connect(&url)
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
        let result = TensorBaseMetricsBackend::connect_in_store(
            "tcp+tls://tensorbase.example:9440",
            "bad;name",
        )
        .await;
        let err = result
            .err()
            .expect("invalid store identifier must be rejected");
        assert!(matches!(err, spectra_core::Error::Config(_)));
    }
}
