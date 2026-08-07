# Spectra performance

Measured on AWS (`t3.xlarge` class and multi-DW layouts). Spectra is the analytics/ingest path from event buses into columnar stores (ClickHouse and related engines). Full scoreboards come from AWS campaign runs.

## Ingest and query

Durable batched write (BM-SW7, ClickHouse n=1, separate writer, C=64, 15s): single-writer **~38k** durable counter ops/s at `batch_max=2048`; two-writer aggregate **~48k**. The single-row protocol floor (BM-SW5) sits around **~0.8k** ops/s — that is why L2 batching exists, not a sizing path.

Co-located writer+DW hosts establish baseline ingest and query latency for a single cell. Multi-DW layouts (separate writer and warehouse hosts) are the decision-grade shape for durable write capacity under sustained load.

## Guidance

Size warehouse CPU/disk from multi-DW AWS runs, not from laptop Docker smokes. Do not size fleets from BM-SW5/SW6 or from enqueue-only BM-SW1 rows (those are not durable throughput). Keep the writer close to the bus; keep the DW sized for scan-heavy reads separately from ingest.

## How to read these results

Prefer AWS-tagged report labels when comparing deployments.
