//! ClickHouse events storage adapter.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use spectra_backend_remote_common::RemoteEventsBackend;
use spectra_core::{
    EventAggregateResult, EventRow, EventStorageBackend, EventWriteRow, EventsAggregateFilter,
    EventsQueryFilter, Result, StorageEngineType,
};

/// Remote ClickHouse structured-event storage.
///
/// [`connect`](Self::connect) opens the ClickHouse client and creates the Spectra events table
/// when needed. Pair it with `ClickHouseMetricsBackend` because `Spectra::builder()` requires
/// both storage paths.
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
pub struct ClickHouseEventsBackend(RemoteEventsBackend);

impl ClickHouseEventsBackend {
    /// Connect to ClickHouse over HTTP or native protocol and ensure event tables exist.
    ///
    /// `url` is typically `https://host:8443` (or `http://host:8123` with
    /// `SPECTRA_ALLOW_INSECURE_REMOTE=1`). The call is async and executes DDL, so the
    /// ClickHouse credentials need table-creation permission.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # async fn example() -> spectra_core::Result<()> {
    /// use spectra_backend_clickhouse::ClickHouseEventsBackend;
    ///
    /// let backend = ClickHouseEventsBackend::connect("https://clickhouse.example:8443").await?;
    /// # let _ = backend;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_in_store(url, "default").await
    }

    /// Connect scoped to one Spectra `store:` name — physical per-store isolation via a
    /// dedicated `spectra_{store}` ClickHouse database on the same server, created if
    /// needed. See [`spectra_backend_remote_common::RemoteEventsBackend::connect_in_database`]
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
    /// use spectra_backend_clickhouse::ClickHouseEventsBackend;
    ///
    /// let backend =
    ///     ClickHouseEventsBackend::connect_in_store("https://clickhouse.example:8443", "counter")
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
                StorageEngineType::ClickHouse,
                &crate::ddl::events_ddl(),
            )
            .await?,
        ))
    }

    /// In-memory stub that preserves the ClickHouse engine type for storage-contract tests.
    ///
    /// # Examples
    ///
    /// ```
    /// use spectra_backend_clickhouse::ClickHouseEventsBackend;
    /// use spectra_core::{EventStorageBackend, StorageEngineType};
    ///
    /// let backend = ClickHouseEventsBackend::in_memory_stub();
    /// assert_eq!(backend.engine_type(), StorageEngineType::ClickHouse);
    /// ```
    pub fn in_memory_stub() -> Self {
        Self(RemoteEventsBackend::in_memory_for_test(
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
impl EventStorageBackend for ClickHouseEventsBackend {
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
    async fn clickhouse_events_roundtrip_in_memory() {
        let backend = ClickHouseEventsBackend::in_memory_for_test();
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
    #[ignore = "requires SPECTRA_CLICKHOUSE_URL"]
    async fn clickhouse_events_integration() {
        let url = std::env::var("SPECTRA_CLICKHOUSE_URL").expect("SPECTRA_CLICKHOUSE_URL");
        let backend = ClickHouseEventsBackend::connect(&url)
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
}
