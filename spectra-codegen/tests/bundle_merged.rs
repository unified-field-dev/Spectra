//! Integration test: merge two fixture roots into one bundle file.

use std::path::PathBuf;

use spectra_codegen::{generate_bundle_merged, EmitKind, SpectraCodegenConfig};

#[test]
fn generate_bundle_merged_merges_helpers_topics_and_sink_forward() {
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let out_dir = tempfile::tempdir().expect("tempdir");

    let config_a = SpectraCodegenConfig {
        schemas_dir: fixtures.clone(),
        out_dir: out_dir.path().to_path_buf(),
        file_suffix: "_spectra_schema.rs",
        metric_suffix: "_spectra_metric.rs",
        emit_mode: EmitKind::HelpersOnly,
        file_prefix: Some("smoke_event"),
    };

    let config_b = SpectraCodegenConfig {
        schemas_dir: fixtures,
        out_dir: out_dir.path().to_path_buf(),
        file_suffix: "_spectra_schema.rs",
        metric_suffix: "_spectra_metric.rs",
        emit_mode: EmitKind::HelpersOnly,
        file_prefix: Some("smoke_counter"),
    };

    generate_bundle_merged(&[config_a, config_b], out_dir.path()).expect("bundle merge");

    let generated =
        std::fs::read_to_string(out_dir.path().join("spectra_generated.rs")).expect("read");
    assert!(generated.contains("pub mod helpers"));
    assert!(generated.contains("pub mod topics"));
    assert!(generated.contains("pub mod sink_forward"));
    assert!(generated.contains("SmokeEventLogger"));
    assert!(generated.contains("SmokeCounterRecorder"));
    assert!(generated.contains("forward_counter"));
    assert!(generated.contains("forward_event"));
    assert!(generated.contains("smoke_event"));
    assert!(generated.contains("smoke_counter"));
}
