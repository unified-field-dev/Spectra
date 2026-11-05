//! Spectra over Photon: publish emits to a topic, write them to storage in batches.
//!
//! Splits a Spectra deployment into publisher processes that never open the analytics
//! database and consumer processes that own storage:
//!
//! - **Publisher:** [`PhotonPublishSink`] is a `SpectraSink` that puts each emit on the
//!   `spectra.emit` Photon topic. Wire it with `.sink(...)` and `.persist_disabled()`.
//! - **Consumer:** [`GroupSubscription`] reads the topic as a consumer group and hands each
//!   emit to a [`BatchedStorageConsumer`], which writes through `record_metrics_batch` and
//!   `append_rows_batch` on the backends registered in a `SpectraRouter`.
//! - **Distribution:** the topic has [`SHARD_COUNT`] virtual shards, routed by series name and
//!   labels (or event table). Give each consumer process a disjoint [`shard_range`] and the
//!   group splits the load while each series stays ordered on one consumer.
//!
//! Delivery is at-most-once from the consumer's side: a batch that fails to write is logged
//! and counted in [`ConsumerStats`], not retried. The subscription keeps no cursor of its own,
//! so where a restarted consumer picks up is up to the Photon storage port; the in-memory port
//! starts at the live tail and does not replay earlier emits.
//!
//! ```no_run
//! use std::sync::Arc;
//! use spectra_core::SpectraRouter;
//! use spectra_photon::{all_shards, BatchConfig, BatchedStorageConsumer, GroupSubscription};
//!
//! # async fn demo(photon: photon::Photon, router: Arc<SpectraRouter>) {
//! let consumer = BatchedStorageConsumer::new(router, BatchConfig::default());
//! let _subscription = GroupSubscription::start(&photon, &all_shards(), consumer.clone());
//! // ... later, before exit:
//! consumer.shutdown().await;
//! # }
//! ```

mod consumer;
mod envelope;
mod publish;
mod subscriber;
mod topic;

pub use consumer::{BatchConfig, BatchedStorageConsumer, ConsumerClosed, ConsumerStats};
pub use envelope::SpectraEnvelope;
pub use publish::{PhotonPublishSink, PublishStats, DEFAULT_PUBLISH_QUEUE_MAX};
pub use subscriber::{all_shards, shard_range, GroupSubscription};
pub use topic::{SpectraEmit, SHARD_COUNT, TOPIC_NAME};
