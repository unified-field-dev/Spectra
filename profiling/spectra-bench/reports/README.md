# Spectra bench reports

Decision-grade JSON from AWS campaigns only (`SPECTRA_BENCH_HARDWARE=aws-*`).

Naming: `{experiment}-{storage}-{topology}-{hardware}.json` (or `multidw-*` for multi-DW / BM-SW7).

Do not commit non-AWS / local / WSL smoke JSON. Fetch from EC2 with `$UF_LAB_ROOT/spectra/infra/aws/spectra/fetch-reports.sh` or multidw equivalent.

Scoreboards: [`docs/bench/PERFORMANCE.md`](../../../docs/bench/PERFORMANCE.md).
