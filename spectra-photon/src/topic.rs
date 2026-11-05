//! The Photon topic that carries Spectra emits.

use photon::topic;

use crate::envelope::SpectraEnvelope;

/// Topic name shared by publishers and consumers.
pub const TOPIC_NAME: &str = "spectra.emit";

/// Virtual shard count of the topic. Consumers split these shards between them.
pub const SHARD_COUNT: u32 = 16;

/// Consumer-group topic payload: the shard routing key plus the emit itself.
#[topic(
    name = "spectra.emit",
    delivery = "group",
    shards = 16,
    shard_by = "partition_key"
)]
pub struct SpectraEmit {
    /// Routing key; see [`SpectraEnvelope`] for how it is derived.
    pub partition_key: String,
    /// The metric sample or event row.
    pub envelope: SpectraEnvelope,
}

impl SpectraEmit {
    pub(crate) fn new(envelope: SpectraEnvelope) -> Self {
        Self {
            partition_key: envelope.partition_key(),
            envelope,
        }
    }
}
