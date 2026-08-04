# spectra-codegen

Build-time codegen from caller-supplied schema roots.

## Audience

| Reader | Use this crate for |
|--------|-------------------|
| **Host integrators** | Invoking codegen from host `build.rs` with explicit schema paths |
| **Adapter authors** | Generated logger helpers, topic constants, and sink forwarders |

## Role

- Library API for schema discovery and Rust source emission
- Caller supplies schema roots — upstream runtime does not scan host trees

## Emit modes (`EmitKind`)

| Mode | Output file | Purpose |
|------|-------------|---------|
| `HelpersOnly` | `spectra_generated.rs` | Typed loggers/recorders calling `spectra_core` facade |
| `SinkForward` | `sink_forward.rs` | `forward_counter` / `forward_event` match arms for host composite sinks |
| `TopicsOnly` | `spectra_topics.rs` | Stable transport topic constants + payload DTOs for host publish adapters |
| Bundle merge | `spectra_generated.rs` | `generate_bundle_merged`: helpers + topics + sink_forward in one file |

Host transport integration: generate `TopicsOnly` or `generate_bundle_merged`, publish `MetricEmit` / `SpectraEvent` payloads on the generated topic constants, and wire `Spectra::builder().sink(transport).build()`.

### Host integration recipe

1. Owner crate holds schema files under `schemas/` (recursively; e.g. `schemas/spectra/`) with `spectra_schema!` / `spectra_metric!`.
2. Per-owner `*-spectra-topics` crate (or owner `build.rs`) calls `generate_bundle_merged` / `generate_topics_merged` with explicit `schemas_dir`.
3. Host binary includes schema modules for inventory + imports generated modules.
4. Wire transport via `SpectraBuilder.sink()`; Photon subscribers remain template-only.

## Status

Phase 4 — `HelpersOnly`, `SinkForward`, `TopicsOnly`, and `generate_bundle_merged` shipped.
