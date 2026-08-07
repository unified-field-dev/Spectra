# Verification baseline

Re-run after test-harness or coverage changes. See `./scripts/verify-release.sh` for release gates.

## Commands

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-spectra-extract

# Upstream gates
./scripts/gate-check.sh

# Format + Clippy (CI gates)
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings

# Unit + integration (exclude e2e/bench drivers)
cargo test --workspace --exclude spectra-e2e --exclude spectra-bench

# Matrix correctness
export CARGO_TARGET_DIR=target-spectra-e2e
cargo test -p spectra-e2e

# Storage port contract (PR CI)
cargo test -p spectra-backend-mem --test storage_contract
cargo test -p spectra-backend-sqlite --test storage_contract
cargo test -p spectra-backend-tensorbase --test storage_contract
cargo test -p spectra-backend-clickhouse --test storage_contract

# Release verification
./scripts/verify-release.sh

# Supply-chain (CI also runs this)
cargo deny check
# Optional complementary advisory scan
cargo audit
```

## AWS full E2E + bench (manual / tag-adjacent)

PR CI: embedded e2e + stub contracts only. Live remote catalog and capacity campaigns run on AWS.

**Correctness scenarios added for query hardening (CI + remote catalog):**

| Scenario ID | Path | Asserts |
|-------------|------|---------|
| `gate-disabled-allows-debug` | Happy | Disabled gate persists debug-tier metric |
| `query-limit-clamped` | Sad | Huge `limit` still returns ≤ emitted / clamp ceiling |
| `query-reject-bad-filter-field` | Sad | Invalid filter field → `Error::Config` |

## Test Map (security hardening)

| ID | Behavior | Primary tests | Notes |
|----|----------|---------------|-------|
| S-1 | Require TLS remote URLs | `connect_accepts_https_under_require_tls`; `require_tls_accepts_https` / `tcp_tls` | Happy `https://` / `tcp+tls://` |
| S-1 | Plaintext without opt-in | `connect_rejects_http_under_require_tls`; `require_tls_rejects_*` | Sad `Error::Config` |
| S-1 | Plaintext with opt-in | `connect_allows_http_when_insecure_allowed`; `insecure_allows_*` | Happy under `AllowInsecurePlaintext` / env |
| S-2 | Ident validation | `validate::tests::*`; catalog `query-reject-bad-filter-field` | Happy+sad charset |
| S-2 | Paging clamp | `validate::tests::clamp_honors_maxima`; catalog `query-limit-clamped` | Cap = `MAX_EVENT_QUERY_LIMIT` |
| S-2 | Gate FORCE_OFF | `config` gate tests; catalog `gate-disabled-allows-debug` | Dual env fail-closed |
| S-2 | URL redaction | `redact_tests::*` | HTTP + native stderr scrub |
| S-2 | Emit-name validation | `entry` `reject_invalid_ident` (via emit path) | Invalid names dropped |

## Documentation Map (hardening surface)

| Topic | Landing | Mid | Deep |
|-------|---------|-----|------|
| Emit gate FORCE_OFF | `uf-spectra` Features + `spectra/README` env table | `SECURITY.md` | `SpectraConfig::from_env` |
| Query ident / paging | `uf-spectra` Features | `SECURITY.md` | `validate_spectra_ident` / `clamp_event_paging` |
| Remote TLS / insecure opt-in | `SECURITY.md` + `SPECTRA_ALLOW_INSECURE_REMOTE` | clickhouse/tensorbase README | `RemoteTransportSecurity` |
| URL credential redaction | `SECURITY.md` | remote-common crate docs | `redact_url_credentials` |
| Verification gates | this file + `CONTRIBUTING.md` | rustdoc Features | e2e / AWS campaigns |

Co-located ClickHouse + TensorBase campaigns run on AWS EC2: provision, bootstrap, deploy-and-run e2e/bench, fetch reports into `profiling/spectra-bench/reports/`, tear down. Then fill scoreboards in [`docs/bench/PERFORMANCE.md`](bench/PERFORMANCE_STUDY.md) from the fetched JSON.

### Multi-DW durable write (BM-SW7 primary)

Separate writer + DW EC2s. Primary capacity experiment is **BM-SW7** (L2 batch). BM-SW5/SW6 are single-row protocol floor. Multi-DW campaigns run on AWS via the operator campaign.

Scoreboard: [`docs/bench/PERFORMANCE.md`](bench/PERFORMANCE_STUDY.md).

## Baseline results

| Check | Result |
|-------|--------|
| `cargo test --workspace --exclude spectra-e2e --exclude spectra-bench` | Run after changes |
| `cargo test -p spectra-e2e` | CI embedded matrix scenarios |
| Storage contract tests (mem/sqlite/tensorbase/clickhouse stubs) | Run after changes |
| `./scripts/verify-release.sh` | Required before release tag |

## Line coverage (CI artifact)

PR CI runs a non-blocking [`coverage`](../.github/workflows/ci.yml) job with `cargo-llvm-cov`:

```bash
# Install once
cargo install cargo-llvm-cov --locked

# Summary to stdout (CI scope — excludes e2e/bench)
./scripts/coverage.sh --summary-only

# Full workspace including e2e
./scripts/coverage.sh --full --summary-only

# LCOV for local inspection
./scripts/coverage.sh --lcov --output-path lcov.info
```

Download `coverage-lcov` from the GitHub Actions run artifacts for the CI report.

**Baseline (2026-07-08):** ~63% line coverage on the CI-scoped slice (excludes `spectra-e2e` and `spectra-bench`). Run with `--test-threads=1` under instrumentation to avoid timing flakes in `spectra-runtime` builder tests.

## Coverage notes

- Behavioral coverage matrix: [`spectra-e2e/README.md`](../spectra-e2e/README.md)
- Shared storage contract: [`spectra-testkit/src/storage_contract.rs`](../spectra-testkit/src/storage_contract.rs)
- Scenario catalog: [`spectra-testkit/src/catalog.rs`](../spectra-testkit/src/catalog.rs)
