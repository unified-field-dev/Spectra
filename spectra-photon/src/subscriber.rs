//! Photon consumer-group subscription that feeds a [`BatchedStorageConsumer`].

use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use photon::Photon;
use tokio::task::JoinHandle;

use crate::consumer::BatchedStorageConsumer;
use crate::topic::{SpectraEmit, SHARD_COUNT, TOPIC_NAME};

/// Shards `range` out of [`SHARD_COUNT`], for splitting the topic between processes.
///
/// Give each process in the group a disjoint range (for example `0..=7` and `8..=15`).
/// Out-of-range ids are dropped.
pub fn shard_range(range: RangeInclusive<u32>) -> Vec<u32> {
    range.filter(|id| *id < SHARD_COUNT).collect()
}

/// Every shard of the topic, for a single-process group.
pub fn all_shards() -> Vec<u32> {
    (0..SHARD_COUNT).collect()
}

/// Running subscription over the assigned shards. Dropping it stops the subscription.
pub struct GroupSubscription {
    task: JoinHandle<()>,
    decode_errors: Arc<AtomicU64>,
}

impl GroupSubscription {
    /// Subscribe to `shards` of the Spectra topic and ingest each event into `consumer`.
    ///
    /// Events that do not decode are logged and counted ([`Self::decode_errors`]); the stream
    /// keeps going. The stream ends when Photon closes it or [`Self::stop`] is called.
    pub fn start(photon: &Photon, shards: &[u32], consumer: BatchedStorageConsumer) -> Self {
        let mut stream = photon.subscribe_consumer_group(TOPIC_NAME, shards, HashMap::new());
        let decode_errors = Arc::new(AtomicU64::new(0));
        let errors = Arc::clone(&decode_errors);
        let task = tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                let event = match item {
                    Ok(event) => event,
                    Err(err) => {
                        tracing::warn!(error = %err, "photon spectra stream error");
                        continue;
                    }
                };
                match serde_json::from_value::<SpectraEmit>(event.payload_json) {
                    Ok(emit) => {
                        if consumer.ingest(emit.envelope).await.is_err() {
                            return;
                        }
                    }
                    Err(err) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(error = %err, "undecodable spectra emit; skipping");
                    }
                }
            }
        });
        Self {
            task,
            decode_errors,
        }
    }

    /// Events skipped because they did not decode.
    pub fn decode_errors(&self) -> u64 {
        self.decode_errors.load(Ordering::Relaxed)
    }

    /// Stop reading new events. Queued envelopes still need [`BatchedStorageConsumer::flush`].
    pub fn stop(&self) {
        self.task.abort();
    }
}

impl Drop for GroupSubscription {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_range_clamps_to_topic_shards() {
        assert_eq!(shard_range(14..=20), vec![14, 15]);
        assert_eq!(all_shards().len(), SHARD_COUNT as usize);
    }
}
