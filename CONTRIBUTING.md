# Contributing

Thank you for improving Spectra. Before opening a PR, run the verify block below on a constrained host (`CARGO_BUILD_JOBS=1`).

## Verify

```bash
export CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=target-spectra-extract

./scripts/gate-check.sh

# Format and lint (CI gates)
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings

# Unit and integration tests (scope narrowly on shared dev machines)
cargo test -p uf-spectra-core -p spectra-macros \
           -p spectra-backend-mem -p spectra-backend-sqlite \
           -p spectra-backend-tensorbase -p spectra-backend-clickhouse \
           -p spectra-runtime -p uf-spectra
cargo test -p uf-spectra --test smoke_inventory
cargo test -p spectra-testkit
cargo test -p spectra-e2e

# Documentation (deny warnings)
RUSTDOCFLAGS="-D warnings" cargo doc -p uf-spectra --all-features --no-deps

# Rustdoc examples
cargo test --doc -p uf-spectra-core
cargo test --doc -p spectra-runtime

# Runnable examples
cargo run -p uf-spectra --example quickstart --features mem
cargo run -p uf-spectra --example quickstart_transport --features mem
cargo run -p uf-spectra --example quickstart_publish_only --features mem
cargo run -p uf-spectra --example quickstart_consume_forward --features mem
cargo run -p uf-spectra --example quickstart_sqlite --features sqlite
cargo run -p uf-spectra --example quickstart_schema_emit --features mem
cargo run -p uf-spectra --example quickstart_telemetry --features mem,telemetry-console

# Full release verification (before tagging)
./scripts/verify-release.sh
```

Remote examples require live storage URLs:

```bash
SPECTRA_ALLOW_INSECURE_REMOTE=1 SPECTRA_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  cargo run -p uf-spectra --example quickstart_clickhouse_emit --features clickhouse
SPECTRA_ALLOW_INSECURE_REMOTE=1 SPECTRA_TENSORBASE_URL=tcp://127.0.0.1:9528 \
  cargo run -p uf-spectra --example quickstart_tensorbase_emit --features tensorbase
```

Full remote gate runs on AWS EC2 (operator campaign).

## Lint policy

- Clippy: `all` + `pedantic` + `nursery`, with `-D warnings` in CI.
- Restriction lints: `unwrap_used`, `expect_used`, `dbg_macro`, `print_stdout`, `print_stderr` (tests exempt via [`clippy.toml`](clippy.toml)).
- Workspace allows stay limited to low-signal pedantic noise (casts, `must_use_candidate`, doc detail lints) — see root [`Cargo.toml`](Cargo.toml) `[workspace.lints.clippy]`.
- Prefer [`SpectraRouter::try_global`](spectra-core/src/router.rs) in library code; panicking [`SpectraRouter::global`](spectra-core/src/router.rs) is for hosts/examples after install.
- Prefer fixing code over new `#[allow(clippy::…)]`; justified exceptions go in root `Cargo.toml` or `clippy.toml`. Intentional console-mirror `eprintln!` in `spectra-runtime` carries a narrow `allow`.
- Do **not** re-allow `await_holding_lock` or `future_not_send` without fixing the underlying issue.
- Cast and doc-detail pedantic lints stay allowed until a dedicated audit/rustdoc pass; do not flip them without fixing call sites.

## Documentation expectations

- Workspace `missing_docs = deny` — all public items need doc comments.
- User-facing paths (root README, `spectra` crate rustdoc, per-crate README) should avoid internal delivery jargon.
- Add runnable examples or rustdoc `# Examples` when introducing new integrator-facing APIs.
- Run `./scripts/gate-check.sh` before any release tag.
- Library crates emit structured `tracing` events; hosts initialize `tracing_subscriber` (see `quickstart` example).

## Build guardrails

1. `export CARGO_BUILD_JOBS=1` on constrained hosts.
2. Use `CARGO_TARGET_DIR=target-spectra-extract` (or `target-spectra-e2e`, `target-spectra-bench`).
3. Scope narrowly: `-p uf-spectra-core`, `-p uf-spectra`, etc. — avoid `--workspace` unless requested.
4. One heavy `cargo` command at a time on shared dev machines.
