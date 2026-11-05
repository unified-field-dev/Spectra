//! Publisher sink → Photon group topic → batched consumer → storage, all in process.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde_json::{json, Value};
use spectra_backend_mem::{MemEventsBackend, MemMetricsBackend};
use spectra_core::{
    Error, EventStorageBackend, EventWriteRow, MetricWriteRow, MetricsStorageBackend,
    SharedEventBackend, SharedMetricsBackend, SpectraRouter, SpectraSink, StorageEngineType,
};
use spectra_photon::{
    all_shards, shard_range, BatchConfig, BatchedStorageConsumer, GroupSubscription,
    PhotonPublishSink, SpectraEnvelope,
};

const DEV_KEY: &str = "cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=";

fn photon() -> photon::Photon {
    std::env::set_var("PHOTON_TRANSPORT_KEY", DEV_KEY);
    photon::Photon::builder().auto_registry().build().unwrap()
}

fn ts() -> DateTime<Utc> {
    Utc::now()
}

fn counter(name: &str, n: i64) -> SpectraEnvelope {
    SpectraEnvelope::Counter {
        name: name.into(),
        labels: json!({}),
        delta: n,
        ts: ts(),
    }
}

/// Backend that records the size of every batch call and can fail one metric name.
#[derive(Default)]
struct RecordingBackend {
    metric_batches: Mutex<Vec<usize>>,
    event_batches: Mutex<Vec<usize>>,
    fail_metric: Option<&'static str>,
}

#[async_trait]
impl MetricsStorageBackend for RecordingBackend {
    fn engine_type(&self) -> StorageEngineType {
        StorageEngineType::Mem
    }
    async fn record_counter(
        &self,
        _: &str,
        _: &Value,
        _: i64,
        _: DateTime<Utc>,
    ) -> spectra_core::Result<()> {
        Ok(())
    }
    async fn record_gauge(
        &self,
        _: &str,
        _: &Value,
        _: f64,
        _: DateTime<Utc>,
    ) -> spectra_core::Result<()> {
        Ok(())
    }
    async fn record_metrics_batch(&self, rows: &[MetricWriteRow]) -> spectra_core::Result<()> {
        if let Some(bad) = self.fail_metric {
            if rows.iter().any(|r| r.name == bad) {
                return Err(Error::Config("injected failure".into()));
            }
        }
        self.metric_batches.lock().push(rows.len());
        Ok(())
    }
}

#[async_trait]
impl EventStorageBackend for RecordingBackend {
    fn engine_type(&self) -> StorageEngineType {
        StorageEngineType::Mem
    }
    async fn append_row(
        &self,
        _: &str,
        _: &Value,
        _: DateTime<Utc>,
        _: Option<&str>,
    ) -> spectra_core::Result<()> {
        Ok(())
    }
    async fn append_rows_batch(&self, rows: &[EventWriteRow]) -> spectra_core::Result<()> {
        self.event_batches.lock().push(rows.len());
        Ok(())
    }
}

fn recording_router(backend: &Arc<RecordingBackend>) -> Arc<SpectraRouter> {
    let metrics: SharedMetricsBackend = Arc::clone(backend) as SharedMetricsBackend;
    let events: SharedEventBackend = Arc::clone(backend) as SharedEventBackend;
    Arc::new(SpectraRouter::with_defaults(metrics, events))
}

async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_sink_to_consumer_lands_in_mem_backends() {
    let photon = photon();
    let metrics = Arc::new(MemMetricsBackend::new());
    let events = Arc::new(MemEventsBackend::new());
    let router = Arc::new(SpectraRouter::with_defaults(
        Arc::clone(&metrics) as SharedMetricsBackend,
        Arc::clone(&events) as SharedEventBackend,
    ));
    let consumer = BatchedStorageConsumer::new(router, BatchConfig::default());
    let _sub = GroupSubscription::start(&photon, &all_shards(), consumer.clone());
    tokio::time::sleep(Duration::from_millis(50)).await;

    let sink = PhotonPublishSink::new(photon.clone());
    for i in 0..20 {
        sink.record_counter(
            "hits",
            &[("route", if i % 2 == 0 { "/a" } else { "/b" })],
            1,
        );
    }
    for i in 0..5 {
        sink.record_gauge("depth", &[], f64::from(i));
    }
    for i in 0..7 {
        sink.log_event("audit", &json!({"n": i}));
    }

    wait_for("32 rows written", || consumer.stats().rows_written >= 32).await;
    consumer.flush().await.unwrap();

    assert_eq!(sink.stats().dropped, 0);
    assert_eq!(sink.stats().published, 32);
    assert_eq!(metrics.metric_stats("hits").await.unwrap().rows, 20);
    assert_eq!(metrics.metric_stats("depth").await.unwrap().rows, 5);
    assert_eq!(events.event_stats("audit").await.unwrap().rows, 7);
    assert_eq!(consumer.stats().batches_failed, 0);
}

#[tokio::test]
async fn size_threshold_splits_batches() {
    let backend = Arc::new(RecordingBackend::default());
    let consumer = BatchedStorageConsumer::new(
        recording_router(&backend),
        BatchConfig {
            batch_max: 4,
            batch_wait: Duration::from_secs(60),
            queue_max: 64,
        },
    );
    for i in 0..10 {
        consumer.ingest(counter("m", i)).await.unwrap();
    }
    consumer.flush().await.unwrap();
    assert_eq!(*backend.metric_batches.lock(), vec![4, 4, 2]);
    assert_eq!(consumer.stats().rows_written, 10);
}

#[tokio::test]
async fn time_threshold_flushes_partial_batch() {
    let backend = Arc::new(RecordingBackend::default());
    let consumer = BatchedStorageConsumer::new(
        recording_router(&backend),
        BatchConfig {
            batch_max: 1000,
            batch_wait: Duration::from_millis(30),
            queue_max: 64,
        },
    );
    for i in 0..3 {
        consumer.ingest(counter("m", i)).await.unwrap();
    }
    // No explicit flush: the timer alone must write the partial batch.
    wait_for("timed flush", || consumer.stats().rows_written == 3).await;
    assert_eq!(*backend.metric_batches.lock(), vec![3]);
}

#[tokio::test]
async fn failed_batch_is_counted_and_later_batches_still_write() {
    let backend = Arc::new(RecordingBackend {
        fail_metric: Some("bad"),
        ..RecordingBackend::default()
    });
    let consumer = BatchedStorageConsumer::new(
        recording_router(&backend),
        BatchConfig {
            batch_max: 2,
            batch_wait: Duration::from_secs(60),
            queue_max: 64,
        },
    );
    consumer.ingest(counter("bad", 1)).await.unwrap();
    consumer.ingest(counter("bad", 1)).await.unwrap();
    consumer.ingest(counter("good", 1)).await.unwrap();
    consumer.ingest(counter("good", 1)).await.unwrap();
    consumer.flush().await.unwrap();

    let stats = consumer.stats();
    assert_eq!((stats.batches_failed, stats.rows_failed), (1, 2));
    assert_eq!((stats.batches_written, stats.rows_written), (1, 2));
}

#[tokio::test]
async fn shutdown_drains_queue_and_then_refuses_input() {
    let backend = Arc::new(RecordingBackend::default());
    let consumer = BatchedStorageConsumer::new(
        recording_router(&backend),
        BatchConfig {
            batch_max: 100,
            batch_wait: Duration::from_secs(60),
            queue_max: 64,
        },
    );
    for i in 0..5 {
        consumer.ingest(counter("m", i)).await.unwrap();
    }
    consumer.shutdown().await;
    assert_eq!(consumer.stats().rows_written, 5);
    assert!(consumer.ingest(counter("m", 1)).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn two_consumers_with_disjoint_shards_split_the_stream_without_duplicates() {
    let photon = photon();
    let a_backend = Arc::new(RecordingBackend::default());
    let b_backend = Arc::new(RecordingBackend::default());
    let config = BatchConfig {
        batch_max: 8,
        batch_wait: Duration::from_millis(20),
        queue_max: 256,
    };
    let a = BatchedStorageConsumer::new(recording_router(&a_backend), config.clone());
    let b = BatchedStorageConsumer::new(recording_router(&b_backend), config);
    let _sub_a = GroupSubscription::start(&photon, &shard_range(0..=7), a.clone());
    let _sub_b = GroupSubscription::start(&photon, &shard_range(8..=15), b.clone());
    tokio::time::sleep(Duration::from_millis(50)).await;

    let sink = PhotonPublishSink::new(photon.clone());
    let total = 200u64;
    for i in 0..total {
        sink.record_counter(&format!("series_{i}"), &[], 1);
    }

    wait_for("all rows written", || {
        a.stats().rows_written + b.stats().rows_written >= total
    })
    .await;
    a.flush().await.unwrap();
    b.flush().await.unwrap();

    let (wa, wb) = (a.stats().rows_written, b.stats().rows_written);
    assert_eq!(wa + wb, total, "every emit written exactly once");
    assert!(wa > 0 && wb > 0, "both consumers took part: {wa} / {wb}");
}

#[tokio::test(flavor = "multi_thread")]
async fn in_memory_subscription_starts_at_the_live_tail() {
    let photon = photon();
    let sink = PhotonPublishSink::new(photon.clone());
    for _ in 0..5 {
        sink.record_counter("early", &[], 1);
    }
    wait_for("publish", || sink.stats().published == 5).await;

    let backend = Arc::new(RecordingBackend::default());
    let consumer = BatchedStorageConsumer::new(recording_router(&backend), BatchConfig::default());
    let _sub = GroupSubscription::start(&photon, &all_shards(), consumer.clone());
    tokio::time::sleep(Duration::from_millis(300)).await;
    consumer.flush().await.unwrap();
    assert_eq!(
        consumer.stats().rows_written,
        0,
        "earlier emits are not replayed"
    );
}
