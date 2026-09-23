//! TensorBase events storage adapter.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use spectra_backend_remote_common::RemoteEventsBackend;
use spectra_core::{
    EventAggregateResult, EventRow, EventStorageBackend, EventWriteRow, EventsAggregateFilter,
    EventsQueryFilter, Result, StorageEngineType,
};

/// Scale-out TensorBase structured-event storage over its ClickHouse-compatible protocol.
///
/// Use [`connect`](Self::connect) with a full protocol URL or
/// [`connect_host`](Self::connect_host) for the default native port (`9528`). Pair this type
/// with `TensorBaseMetricsBackend` when building the runtime.
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
pub struct TensorBaseEventsBackend(RemoteEventsBackend);

impl TensorBaseEventsBackend {
    /// Connect with a full TensorBase protocol URL and ensure event tables exist.
    ///
    /// Accepts `tcp+tls://` / `https://` (or plaintext `tcp://` / `http://` with
    /// `SPECTRA_ALLOW_INSECURE_REMOTE=1`). The call is async and executes DDL.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # async fn example() -> spectra_core::Result<()> {
    /// use spectra_backend_tensorbase::TensorBaseEventsBackend;
    ///
    /// let backend = TensorBaseEventsBackend::connect("tcp+tls://127.0.0.1:9440").await?;
    /// # let _ = backend;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_in_store(url, "default").await
    }

    /// Connect scoped to one Spectra `store:` name — physical per-store isolation via a
    /// dedicated `spectra_{store}` database on the same server, created if needed. See
    /// [`spectra_backend_remote_common::RemoteEventsBackend::connect_in_database`] for the
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
    /// use spectra_backend_tensorbase::TensorBaseEventsBackend;
    ///
    /// let backend =
    ///     TensorBaseEventsBackend::connect_in_store("tcp+tls://127.0.0.1:9440", "counter")
    ///         .await?;
    /// # let _ = backend;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_in_store(url: &str, store: &str) -> Result<Self> {
        let database = format!("spectra_{store}");
        Ok(Self(
            RemoteEventsBackend::connect_in_database(
                url,
                &database,
                StorageEngineType::TensorBase,
                &crate::ddl::events_ddl(),
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
    /// use spectra_backend_tensorbase::TensorBaseEventsBackend;
    ///
    /// let backend = TensorBaseEventsBackend::connect_host("127.0.0.1").await?;
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
    /// use spectra_backend_tensorbase::TensorBaseEventsBackend;
    /// use spectra_core::{EventStorageBackend, StorageEngineType};
    ///
    /// let backend = TensorBaseEventsBackend::in_memory_stub();
    /// assert_eq!(backend.engine_type(), StorageEngineType::TensorBase);
    /// ```
    pub fn in_memory_stub() -> Self {
        Self(RemoteEventsBackend::in_memory_for_test(
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
impl EventStorageBackend for TensorBaseEventsBackend {
    fn engine_type(&self) -> StorageEngineType {
        self.0.engine_type()
    }

    async fn append_row(
        &self,
        table: &str,
        fields: &Value,
        ts: DateTime<Utc>,
        correlation_id: Option<&str>,
    ) -> Result<()> {
        self.0.append_row(table, fields, ts, correlation_id).await
    }

    async fn append_rows_batch(&self, rows: &[EventWriteRow]) -> Result<()> {
        self.0.append_rows_batch(rows).await
    }

    async fn query_rows(&self, filter: EventsQueryFilter) -> Result<Vec<EventRow>> {
        self.0.query_rows(filter).await
    }

    async fn query_aggregate(&self, filter: EventsAggregateFilter) -> Result<EventAggregateResult> {
        self.0.query_aggregate(filter).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn tensorbase_events_roundtrip_in_memory() {
        let backend = TensorBaseEventsBackend::in_memory_for_test();
        let ts = Utc::now();
        backend
            .append_row("req", &json!({"x": 1}), ts, None)
            .await
            .expect("write");
        let rows = backend
            .query_rows(EventsQueryFilter {
                table: "req".into(),
                ..Default::default()
            })
            .await
            .expect("query");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    #[ignore = "requires SPECTRA_TENSORBASE_URL"]
    async fn tensorbase_events_integration() {
        let url = std::env::var("SPECTRA_TENSORBASE_URL").expect("SPECTRA_TENSORBASE_URL");
        let backend = TensorBaseEventsBackend::connect(&url)
            .await
            .expect("connect");
        let ts = Utc::now();
        backend
            .append_row("integration_req", &json!({"ok": true}), ts, None)
            .await
            .expect("write");
        let rows = backend
            .query_rows(EventsQueryFilter {
                table: "integration_req".into(),
                ..Default::default()
            })
            .await
            .expect("query");
        assert!(!rows.is_empty());
    }

    #[tokio::test]
    async fn connect_in_store_rejects_invalid_store_name_sad() {
        let result = TensorBaseEventsBackend::connect_in_store(
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
