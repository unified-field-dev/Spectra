# spectra-e2e

Matrix-driven **correctness** integration tests for Spectra emit, persist, query, and transport features.

## vs inline `spectra/tests`

| | `spectra/tests` | `spectra-e2e` |
|---|---|----------------|
| Scope | Fast, crate-scoped defaults | Cross-cutting matrix |
| Storage | In-process `mem` | `mem` + `sqlite` in CI; remote on extended |
| Assertions | Unit/integration | Declarative scenario runner |

Inline tests stay for schema registry smoke and query DTO mapping.

## Behavioral contract (not bench budgets)

- platform smoke counter + event persist roundtrip
- transport dual-path (recording sink + storage)
- transport-only when persist is disabled (sad path)
- emit gate drops debug-tier metrics (sad path)
- disabled emit gate allows debug-tier metrics (happy path)
- labeled metric query hit and miss (happy / sad)
- gauge persist roundtrip
- empty metric query window (sad path)
- console NDJSON telemetry row (happy path)
- event query limit clamp (sad path — huge limit)
- reject invalid event filter field (sad path — `Error::Config`)

Performance budgets belong in [`spectra-bench`](../spectra-bench/README.md).

## CI strategy

| Trigger | Scope | Command |
|---------|-------|---------|
| Push / PR | **Core** — 12 scenarios × embedded matrix | `cargo test -p spectra-e2e` |
| Git tag `v*` | **Extended** — remote storage + live contracts | see `ci-extended.yml` |

PR CI also runs storage port contracts: `cargo test -p spectra-backend-{mem,sqlite,tensorbase,clickhouse} --test storage_contract`.

## Coverage matrix

**CI default:** `mem` + `sqlite` × `direct` + `recording` × telemetry `off` (telemetry scenario uses `console-ndjson` row).

| Scenario | Path | mem CI | sqlite CI | tensorbase (`--ignored`) | clickhouse (`--ignored`) |
|----------|------|:------:|:---------:|:------------------------:|:------------------------:|
| `platform-smoke-roundtrip` | Happy | ✓ | ✓ | ✓ | ✓ |
| `transport-dual-path` | Happy | ✓ (recording) | ✓ (recording) | — | — |
| `transport-only-no-storage` | Sad | ✓ (recording) | ✓ (recording) | — | — |
| `gate-drops-debug` | Sad | ✓ | ✓ | ✓ | ✓ |
| `label-filter-hit` | Happy | ✓ | ✓ | ✓ | ✓ |
| `label-filter-miss` | Sad | ✓ | ✓ | ✓ | ✓ |
| `gauge-roundtrip` | Happy | ✓ | ✓ | ✓ | ✓ |
| `query-time-range-empty` | Sad | ✓ | ✓ | ✓ | ✓ |
| `telemetry-console-ndjson` | Happy | ✓ | — | — | — |
| `gate-disabled-allows-debug` | Happy | ✓ | ✓ | ✓ | ✓ |
| `query-limit-clamped` | Sad | ✓ | ✓ | ✓ | ✓ |
| `query-reject-bad-filter-field` | Sad | ✓ | ✓ | ✓ | ✓ |

Remote rows use `Topology::RemoteIngest` (`remote_ingest` submodule) and require `SPECTRA_TENSORBASE_URL` / `SPECTRA_CLICKHOUSE_URL`. Soft-skip when the URL for a storage is unset.

**Not in default CI:** live remote catalog (`--ignored`) and live remote contract tests (`--include-ignored` on tag CI / AWS).

## AWS full E2E + bench

Co-located ClickHouse + TensorBase on a single EC2 instance. Full remote catalog and capacity campaigns run on AWS: provision, bootstrap, deploy-and-run e2e/bench, fetch reports, tear down. Operator runbooks stay outside this public tree.

Runnable emit examples (same smoke as `platform-smoke-roundtrip`):

```bash
export SPECTRA_CLICKHOUSE_URL=http://127.0.0.1:8123
cargo run -p uf-spectra --example quickstart_clickhouse_emit --features clickhouse

export SPECTRA_TENSORBASE_URL=tcp://127.0.0.1:9528
cargo run -p uf-spectra --example quickstart_tensorbase_emit --features tensorbase
```

## Run

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-spectra-e2e

# Core (CI default)
cargo test -p spectra-e2e

# Remote matrix (requires env URLs)
export SPECTRA_CLICKHOUSE_URL=http://127.0.0.1:8123
export SPECTRA_TENSORBASE_URL=tcp://127.0.0.1:9528
cargo test -p spectra-e2e --features clickhouse,tensorbase -- --ignored

# Live storage contracts (remote backends)
cargo test -p spectra-backend-clickhouse --test storage_contract -- --include-ignored
cargo test -p spectra-backend-tensorbase --test storage_contract -- --include-ignored

# Storage port contract (stub / PR CI)
cargo test -p spectra-backend-mem --test storage_contract
cargo test -p spectra-backend-sqlite --test storage_contract
```

## Related

- Harness: [`spectra-testkit`](../spectra-testkit/README.md)
- Benchmarks: [`spectra-bench`](../spectra-bench/README.md)
- Verification baseline: [`docs/VERIFICATION.md`](../docs/VERIFICATION.md)
