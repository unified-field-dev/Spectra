//! Publisher side: a [`SpectraSink`] that forwards emits to the Photon topic.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use photon::Photon;
use spectra_core::{current_emit_ts, SpectraSink};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::envelope::{labels_to_value, SpectraEnvelope};
use crate::topic::SpectraEmit;

/// Default bound on emits waiting for the publish task.
pub const DEFAULT_PUBLISH_QUEUE_MAX: usize = 8192;

#[derive(Default)]
struct PublishCounters {
    published: AtomicU64,
    dropped: AtomicU64,
    failed: AtomicU64,
}

/// Counters for [`PhotonPublishSink`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishStats {
    /// Emits Photon accepted.
    pub published: u64,
    /// Emits dropped because the hand-off queue was full or closed.
    pub dropped: u64,
    /// Emits Photon rejected.
    pub failed: u64,
}

/// [`SpectraSink`] that publishes every emit on the `spectra.emit` Photon topic.
///
/// The emit thread only does a bounded, non-blocking hand-off; a background task calls Photon.
/// When the queue is full the emit is dropped and counted ([`Self::stats`]), as with the
/// default storage persist queue.
///
/// Wire it with `Spectra::builder().sink(Arc::new(sink)).persist_disabled()` so the publisher
/// never opens the analytics database. Must be created inside a Tokio runtime.
pub struct PhotonPublishSink {
    tx: mpsc::Sender<SpectraEmit>,
    counters: Arc<PublishCounters>,
    _worker: JoinHandle<()>,
}

impl PhotonPublishSink {
    /// Sink with [`DEFAULT_PUBLISH_QUEUE_MAX`].
    pub fn new(photon: Photon) -> Self {
        Self::with_queue_max(photon, DEFAULT_PUBLISH_QUEUE_MAX)
    }

    /// Sink with an explicit hand-off queue bound (at least 1).
    pub fn with_queue_max(photon: Photon, queue_max: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<SpectraEmit>(queue_max.max(1));
        let counters = Arc::new(PublishCounters::default());
        let worker_counters = Arc::clone(&counters);
        let worker = tokio::spawn(async move {
            while let Some(emit) = rx.recv().await {
                match emit.publish_on(&photon).await {
                    Ok(_) => worker_counters.published.fetch_add(1, Ordering::Relaxed),
                    Err(err) => {
                        tracing::warn!(error = %err, "photon publish of spectra emit failed");
                        worker_counters.failed.fetch_add(1, Ordering::Relaxed)
                    }
                };
            }
        });
        Self {
            tx,
            counters,
            _worker: worker,
        }
    }

    /// Current counters.
    pub fn stats(&self) -> PublishStats {
        PublishStats {
            published: self.counters.published.load(Ordering::Relaxed),
            dropped: self.counters.dropped.load(Ordering::Relaxed),
            failed: self.counters.failed.load(Ordering::Relaxed),
        }
    }

    fn enqueue(&self, envelope: SpectraEnvelope) {
        if self.tx.try_send(SpectraEmit::new(envelope)).is_err() {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("spectra photon publish queue full or closed; dropping emit");
        }
    }
}

impl SpectraSink for PhotonPublishSink {
    fn record_counter(&self, name: &str, labels: &[(&str, &str)], delta: i64) {
        self.enqueue(SpectraEnvelope::Counter {
            name: name.to_string(),
            labels: labels_to_value(labels),
            delta,
            ts: current_emit_ts(),
        });
    }

    fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.enqueue(SpectraEnvelope::Gauge {
            name: name.to_string(),
            labels: labels_to_value(labels),
            value,
            ts: current_emit_ts(),
        });
    }

    fn log_event(&self, table: &str, fields: &serde_json::Value) {
        self.enqueue(SpectraEnvelope::Event {
            table: table.to_string(),
            fields: fields.clone(),
            ts: current_emit_ts(),
        });
    }
}
