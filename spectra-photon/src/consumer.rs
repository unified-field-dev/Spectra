//! Consumer side: batches envelopes by size or time and writes them through the router.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use spectra_core::{
    EventWriteRow, MetricWriteRow, SharedEventBackend, SharedMetricsBackend, SpectraRouter,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::envelope::SpectraEnvelope;

/// Batch and queue settings for [`BatchedStorageConsumer`].
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Flush as soon as this many envelopes are collected. Default `256`.
    pub batch_max: usize,
    /// Flush a partial batch this long after its first envelope arrived. Default `50ms`.
    pub batch_wait: Duration,
    /// Envelopes buffered ahead of the writer; `ingest` waits when it is full. Default `4096`.
    pub queue_max: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_max: 256,
            batch_wait: Duration::from_millis(50),
            queue_max: 4096,
        }
    }
}

impl BatchConfig {
    fn normalized(self) -> Self {
        Self {
            batch_max: self.batch_max.max(1),
            batch_wait: self.batch_wait,
            queue_max: self.queue_max.max(1),
        }
    }
}

#[derive(Default)]
struct ConsumerCounters {
    rows_written: AtomicU64,
    batches_written: AtomicU64,
    rows_failed: AtomicU64,
    batches_failed: AtomicU64,
}

/// Counters for [`BatchedStorageConsumer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerStats {
    /// Rows the storage backends accepted.
    pub rows_written: u64,
    /// Batch calls (one per backend per flush) that succeeded.
    pub batches_written: u64,
    /// Rows in batch calls that failed. They are not retried.
    pub rows_failed: u64,
    /// Batch calls that failed.
    pub batches_failed: u64,
}

enum Msg {
    Envelope(SpectraEnvelope),
    Flush(oneshot::Sender<()>),
}

/// Why [`BatchedStorageConsumer`] refused an envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerClosed;

impl std::fmt::Display for ConsumerClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("spectra batched storage consumer is closed")
    }
}

impl std::error::Error for ConsumerClosed {}

/// Collects [`SpectraEnvelope`]s and writes them to the router's backends in batches, using
/// `record_metrics_batch` and `append_rows_batch`.
///
/// A batch goes out when it holds `batch_max` envelopes or `batch_wait` after its first
/// envelope, whichever is first. Rows that share a backend share one batch call. A failed
/// call is logged and counted ([`Self::stats`]) and later batches keep flowing.
///
/// Handle clones share one writer task. Must be created inside a Tokio runtime.
#[derive(Clone)]
pub struct BatchedStorageConsumer {
    tx: mpsc::Sender<Msg>,
    counters: Arc<ConsumerCounters>,
    worker: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl BatchedStorageConsumer {
    /// Start a writer over `router` with `config`.
    pub fn new(router: Arc<SpectraRouter>, config: BatchConfig) -> Self {
        let config = config.normalized();
        let (tx, rx) = mpsc::channel(config.queue_max);
        let counters = Arc::new(ConsumerCounters::default());
        let worker = tokio::spawn(writer_loop(
            rx,
            router,
            config.batch_max,
            config.batch_wait,
            Arc::clone(&counters),
        ));
        Self {
            tx,
            counters,
            worker: Arc::new(tokio::sync::Mutex::new(Some(worker))),
        }
    }

    /// Queue one envelope, waiting while the queue is full.
    pub async fn ingest(&self, envelope: SpectraEnvelope) -> Result<(), ConsumerClosed> {
        self.tx
            .send(Msg::Envelope(envelope))
            .await
            .map_err(|_| ConsumerClosed)
    }

    /// Write out everything queued before this call.
    pub async fn flush(&self) -> Result<(), ConsumerClosed> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(Msg::Flush(ack_tx))
            .await
            .map_err(|_| ConsumerClosed)?;
        ack_rx.await.map_err(|_| ConsumerClosed)
    }

    /// Flush, then stop the writer. Later `ingest` calls on any clone return [`ConsumerClosed`].
    pub async fn shutdown(&self) {
        let _ = self.flush().await;
        let handle = self.worker.lock().await.take();
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Current counters.
    pub fn stats(&self) -> ConsumerStats {
        ConsumerStats {
            rows_written: self.counters.rows_written.load(Ordering::Relaxed),
            batches_written: self.counters.batches_written.load(Ordering::Relaxed),
            rows_failed: self.counters.rows_failed.load(Ordering::Relaxed),
            batches_failed: self.counters.batches_failed.load(Ordering::Relaxed),
        }
    }
}

async fn writer_loop(
    mut rx: mpsc::Receiver<Msg>,
    router: Arc<SpectraRouter>,
    batch_max: usize,
    batch_wait: Duration,
    counters: Arc<ConsumerCounters>,
) {
    let mut batch: Vec<SpectraEnvelope> = Vec::with_capacity(batch_max);
    let mut deadline: Option<Instant> = None;
    loop {
        let msg = match deadline {
            None => rx.recv().await,
            Some(at) => match tokio::time::timeout_at(at, rx.recv()).await {
                Ok(msg) => msg,
                Err(_) => {
                    write_batch(&router, std::mem::take(&mut batch), &counters).await;
                    deadline = None;
                    continue;
                }
            },
        };
        match msg {
            None => {
                write_batch(&router, std::mem::take(&mut batch), &counters).await;
                return;
            }
            Some(Msg::Envelope(envelope)) => {
                if batch.is_empty() {
                    deadline = Some(Instant::now() + batch_wait);
                }
                batch.push(envelope);
                if batch.len() >= batch_max {
                    write_batch(&router, std::mem::take(&mut batch), &counters).await;
                    deadline = None;
                }
            }
            Some(Msg::Flush(ack)) => {
                write_batch(&router, std::mem::take(&mut batch), &counters).await;
                deadline = None;
                let _ = ack.send(());
            }
        }
    }
}

fn arc_key<T: ?Sized>(arc: &Arc<T>) -> usize {
    Arc::as_ptr(arc).cast::<()>() as usize
}

#[tracing::instrument(name = "spectra.photon.write_batch", skip_all, fields(batch_len = batch.len()))]
async fn write_batch(
    router: &SpectraRouter,
    batch: Vec<SpectraEnvelope>,
    counters: &ConsumerCounters,
) {
    if batch.is_empty() {
        return;
    }
    let mut metrics: HashMap<usize, (SharedMetricsBackend, Vec<MetricWriteRow>)> = HashMap::new();
    let mut events: HashMap<usize, (SharedEventBackend, Vec<EventWriteRow>)> = HashMap::new();

    for envelope in batch {
        match envelope {
            SpectraEnvelope::Counter {
                name,
                labels,
                delta,
                ts,
            } => push_metric(
                router,
                &mut metrics,
                name,
                "counter",
                Value::from(delta),
                labels,
                ts,
            ),
            SpectraEnvelope::Gauge {
                name,
                labels,
                value,
                ts,
            } => push_metric(
                router,
                &mut metrics,
                name,
                "gauge",
                Value::from(value),
                labels,
                ts,
            ),
            SpectraEnvelope::Event { table, fields, ts } => {
                let backend = router.resolve_event(&table);
                events
                    .entry(arc_key(&backend))
                    .or_insert_with(|| (Arc::clone(&backend), Vec::new()))
                    .1
                    .push(EventWriteRow {
                        table,
                        fields,
                        ts,
                        correlation_id: None,
                    });
            }
        }
    }

    for (backend, rows) in metrics.into_values() {
        let result = backend.record_metrics_batch(&rows).await;
        account(counters, rows.len(), result, "metrics");
    }
    for (backend, rows) in events.into_values() {
        let result = backend.append_rows_batch(&rows).await;
        account(counters, rows.len(), result, "events");
    }
}

fn push_metric(
    router: &SpectraRouter,
    buckets: &mut HashMap<usize, (SharedMetricsBackend, Vec<MetricWriteRow>)>,
    name: String,
    kind: &'static str,
    value: Value,
    labels: Value,
    ts: chrono::DateTime<chrono::Utc>,
) {
    let backend = router.resolve_metrics(&name);
    buckets
        .entry(arc_key(&backend))
        .or_insert_with(|| (Arc::clone(&backend), Vec::new()))
        .1
        .push(MetricWriteRow {
            name,
            kind,
            value,
            labels,
            ts,
            correlation_id: None,
        });
}

fn account(counters: &ConsumerCounters, rows: usize, result: spectra_core::Result<()>, what: &str) {
    let rows = rows as u64;
    match result {
        Ok(()) => {
            counters.rows_written.fetch_add(rows, Ordering::Relaxed);
            counters.batches_written.fetch_add(1, Ordering::Relaxed);
        }
        Err(err) => {
            counters.rows_failed.fetch_add(rows, Ordering::Relaxed);
            counters.batches_failed.fetch_add(1, Ordering::Relaxed);
            tracing::error!(error = %err, rows, what, "spectra batch write failed");
        }
    }
}
