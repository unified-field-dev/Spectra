# uf-spectra-photon

Carries Spectra emits over a [Photon](https://crates.io/crates/uf-photon) consumer-group topic
and writes them to storage in batches.

- `PhotonPublishSink` publishes each emit on the `spectra.emit` topic. Use it with
  `Spectra::builder().sink(...).persist_disabled()` in processes that should not open storage.
- `GroupSubscription` plus `BatchedStorageConsumer` read the topic and write batches (by size or
  time) through the router's `record_metrics_batch` and `append_rows_batch`.
- Run several consumer processes with disjoint `shard_range`s to split the load.

See the crate docs for a consumer example and the delivery guarantees.
