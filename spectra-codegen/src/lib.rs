//! Build-time codegen for Spectra schemas → typed emit helpers and sink forwarding.

use std::fs;
use std::path::{Path, PathBuf};

mod codegen;

pub use codegen::emit::{
    emit_sink_forward, emit_sink_forward_tokens, generate_helpers, generate_topics, EmitKind,
};
pub use codegen::parser::{ParsedEventSchema, ParsedMetricSchema};

/// Configuration for Spectra code generation.
pub struct SpectraCodegenConfig {
    pub schemas_dir: PathBuf,
    pub out_dir: PathBuf,
    pub file_suffix: &'static str,
    pub metric_suffix: &'static str,
    pub emit_mode: EmitKind,
    /// When set, only schema files whose stem starts with this prefix are processed.
    pub file_prefix: Option<&'static str>,
}

/// Parse schema DSL files from a config's `schemas_dir`.
fn parse_schemas(
    config: &SpectraCodegenConfig,
) -> anyhow::Result<(Vec<ParsedEventSchema>, Vec<ParsedMetricSchema>)> {
    if !config.schemas_dir.exists() {
        anyhow::bail!(
            "schemas directory does not exist: {}",
            config.schemas_dir.display()
        );
    }

    let events = collect_files(&config.schemas_dir, config.file_suffix, config.file_prefix)?;
    let metrics = collect_files(
        &config.schemas_dir,
        config.metric_suffix,
        config.file_prefix,
    )?;

    let mut events_parsed = Vec::new();
    for path in &events {
        let content = fs::read_to_string(path)?;
        events_parsed.push(codegen::parser::parse_event_schema(&content)?);
    }

    let mut metrics_parsed = Vec::new();
    for path in &metrics {
        let content = fs::read_to_string(path)?;
        metrics_parsed.push(codegen::parser::parse_metric_schema(&content)?);
    }

    Ok((events_parsed, metrics_parsed))
}

/// Generate typed emit helpers under `out_dir` for a single config.
pub fn generate_spectra(config: SpectraCodegenConfig) -> anyhow::Result<()> {
    let (events_parsed, metrics_parsed) = parse_schemas(&config)?;

    match config.emit_mode {
        EmitKind::HelpersOnly => {
            let generated = generate_helpers(&events_parsed, &metrics_parsed)?;
            fs::write(config.out_dir.join("spectra_generated.rs"), generated)?;
        }
        EmitKind::SinkForward => {
            let sink_forward = emit_sink_forward(&events_parsed, &metrics_parsed)?;
            fs::write(config.out_dir.join("sink_forward.rs"), sink_forward)?;
        }
        EmitKind::TopicsOnly => {
            let topics = generate_topics(&events_parsed, &metrics_parsed)?;
            fs::write(config.out_dir.join("spectra_topics.rs"), topics)?;
        }
    }

    Ok(())
}

/// Merge multiple codegen passes into one `spectra_generated.rs` (helpers module).
pub fn generate_spectra_merged(
    configs: &[SpectraCodegenConfig],
    out_dir: &Path,
) -> anyhow::Result<()> {
    use proc_macro2::TokenStream;
    use quote::quote;

    let mut helper_tokens = TokenStream::new();

    for config in configs {
        let (events_parsed, metrics_parsed) = parse_schemas(config)?;
        let tokens = codegen::emit::emit_helper_tokens(&events_parsed, &metrics_parsed)?;
        helper_tokens.extend(tokens);
    }

    let file = quote! {
        pub mod helpers {
            #helper_tokens
        }
    };

    fs::write(out_dir.join("spectra_generated.rs"), file.to_string())?;
    Ok(())
}

/// Merge multiple codegen passes into one `spectra_topics.rs` (topics module).
pub fn generate_topics_merged(
    configs: &[SpectraCodegenConfig],
    out_dir: &Path,
) -> anyhow::Result<()> {
    use proc_macro2::TokenStream;
    use quote::quote;

    let mut topic_tokens = TokenStream::new();

    for config in configs {
        let (events_parsed, metrics_parsed) = parse_schemas(config)?;
        let tokens = codegen::emit::emit_topic_tokens(&events_parsed, &metrics_parsed)?;
        topic_tokens.extend(tokens);
    }

    let file = quote! {
        pub mod topics {
            #topic_tokens
        }
    };

    fs::write(out_dir.join("spectra_topics.rs"), file.to_string())?;
    Ok(())
}

/// Merge multiple codegen passes into one `spectra_generated.rs` with helpers, topics, and sink forwarding.
///
/// Host `build.rs` recipe: pass explicit schema roots; include the output from the facade crate
/// (`helpers`, `topics`, `sink_forward` modules). Upstream smoke facade uses this for
/// `platform_smoke_*` schemas only.
pub fn generate_bundle_merged(
    configs: &[SpectraCodegenConfig],
    out_dir: &Path,
) -> anyhow::Result<()> {
    use proc_macro2::TokenStream;
    use quote::quote;

    let mut helper_tokens = TokenStream::new();
    let mut topic_tokens = TokenStream::new();
    let mut all_events = Vec::new();
    let mut all_metrics = Vec::new();

    for config in configs {
        let (events_parsed, metrics_parsed) = parse_schemas(config)?;
        all_events.extend(events_parsed.clone());
        all_metrics.extend(metrics_parsed.clone());
        helper_tokens.extend(codegen::emit::emit_helper_tokens(
            &events_parsed,
            &metrics_parsed,
        )?);
        topic_tokens.extend(codegen::emit::emit_topic_tokens(
            &events_parsed,
            &metrics_parsed,
        )?);
    }

    let sink_forward_tokens = codegen::emit::emit_sink_forward_tokens(&all_events, &all_metrics)?;

    let file = quote! {
        pub mod helpers {
            #helper_tokens
        }
        pub mod topics {
            #topic_tokens
        }
        pub mod sink_forward {
            #sink_forward_tokens
        }
    };

    fs::write(out_dir.join("spectra_generated.rs"), file.to_string())?;
    Ok(())
}

/// Collect matching `*.rs` paths under `dir` recursively; results are stable-sorted.
fn collect_files(
    dir: &Path,
    suffix: &str,
    file_prefix: Option<&str>,
) -> anyhow::Result<Vec<PathBuf>> {
    let stem_suffix = suffix.strip_suffix(".rs").unwrap_or(suffix);
    let mut paths = Vec::new();
    collect_files_into(dir, stem_suffix, file_prefix, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_files_into(
    dir: &Path,
    stem_suffix: &str,
    file_prefix: Option<&str>,
    paths: &mut Vec<PathBuf>,
) -> anyhow::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files_into(&path, stem_suffix, file_prefix, paths)?;
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        if !stem.ends_with(stem_suffix) {
            continue;
        }
        if let Some(prefix) = file_prefix {
            if !stem.starts_with(prefix) {
                continue;
            }
        }
        if path.extension().is_none_or(|ext| ext != "rs") {
            continue;
        }
        paths.push(path);
    }
    Ok(())
}

#[cfg(test)]
mod collect_files_tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::collect_files;

    static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

    struct TempRoot {
        path: PathBuf,
    }

    impl TempRoot {
        fn new(label: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("spectra-codegen-{label}-{nanos}-{seq}"));
            fs::create_dir_all(&path).expect("create temp root");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn collect_files_walks_nested_directories() {
        let root = TempRoot::new("nested");
        let nested = root.path().join("spectra").join("domain");
        fs::create_dir_all(&nested).expect("nested dir");
        fs::write(
            nested.join("demo_spectra_schema.rs"),
            "spectra_schema! { Demo {} }\n",
        )
        .expect("schema");
        fs::write(
            root.path().join("top_spectra_metric.rs"),
            "spectra_metric! { Top {} }\n",
        )
        .expect("metric");
        fs::write(nested.join("ignore.txt"), "nope").expect("non-rs");

        let schemas = collect_files(root.path(), "_spectra_schema.rs", None).expect("schemas");
        assert_eq!(schemas.len(), 1);
        assert!(schemas[0].ends_with("spectra/domain/demo_spectra_schema.rs"));

        let metrics = collect_files(root.path(), "_spectra_metric.rs", None).expect("metrics");
        assert_eq!(metrics.len(), 1);
        assert!(metrics[0].ends_with("top_spectra_metric.rs"));
    }

    #[test]
    fn collect_files_honors_file_prefix_in_nested_dirs() {
        let root = TempRoot::new("prefix");
        let nested = root.path().join("spectra");
        fs::create_dir_all(&nested).expect("nested dir");
        fs::write(nested.join("keep_spectra_schema.rs"), "// keep\n").expect("keep");
        fs::write(nested.join("drop_spectra_schema.rs"), "// drop\n").expect("drop");

        let found =
            collect_files(root.path(), "_spectra_schema.rs", Some("keep_")).expect("collect");
        assert_eq!(found.len(), 1);
        assert!(found[0].ends_with("keep_spectra_schema.rs"));
    }
}
