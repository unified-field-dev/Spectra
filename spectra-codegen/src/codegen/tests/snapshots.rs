use crate::codegen::emit::{emit_sink_forward, generate_helpers, generate_topics};
use crate::codegen::parser::{parse_event_schema, parse_metric_schema};

const EVENT_SCHEMA: &str = r#"
spectra_schema! {
    RequestDebugLog {
        store: "default",
        table: "request_debug_log",
        version: "0.1.0",
        description: "Structured debug events",
        fields: [
            message: {
                r#type: String,
                classification: { pii: false, safe_for_console: true },
            },
        ],
    }
}
"#;

const METRIC_SCHEMA: &str = r#"
spectra_metric! {
    CacheHits {
        store: "default",
        name: "cache_hits",
        version: "0.1.0",
        description: "Counter for cache hits",
    }
}
"#;

#[test]
fn event_schema_codegen_snapshot() {
    let parsed = parse_event_schema(EVENT_SCHEMA).expect("parse event");
    let out = generate_helpers(std::slice::from_ref(&parsed), &[]).expect("emit");
    insta::assert_snapshot!("event_schema", out);
}

#[test]
fn metric_schema_codegen_snapshot() {
    let parsed = parse_metric_schema(METRIC_SCHEMA).expect("parse metric");
    let out = generate_helpers(&[], std::slice::from_ref(&parsed)).expect("emit");
    insta::assert_snapshot!("metric_schema", out);
}

#[test]
fn helpers_use_spectra_core_facade() {
    let ev = parse_event_schema(EVENT_SCHEMA).expect("ev");
    let met = parse_metric_schema(METRIC_SCHEMA).expect("met");
    let out = generate_helpers(&[ev], &[met]).expect("emit");
    assert!(out.contains("try_log_event_now"));
    assert!(out.contains("try_record_counter_now"));
    assert!(!out.contains("photon_macros"));
}

#[test]
fn sink_forward_generates_match_arms() {
    let ev = parse_event_schema(EVENT_SCHEMA).expect("ev");
    let met = parse_metric_schema(METRIC_SCHEMA).expect("met");
    let out = emit_sink_forward(&[ev], &[met]).expect("forward");
    assert!(out.contains("forward_counter"));
    assert!(out.contains("forward_event"));
    assert!(out.contains("cache_hits"));
    assert!(out.contains("request_debug_log"));
}

#[test]
fn topics_only_codegen_snapshot() {
    let ev = parse_event_schema(EVENT_SCHEMA).expect("ev");
    let met = parse_metric_schema(METRIC_SCHEMA).expect("met");
    let out = generate_topics(&[ev], &[met]).expect("topics");
    insta::assert_snapshot!("topics_only", out);
    assert!(out.contains("spectra.event.request_debug_log"));
    assert!(out.contains("spectra.metric.cache_hits"));
    assert!(out.contains("SpectraEvent"));
    assert!(out.contains("MetricEmit"));
    assert!(!out.contains("photon_macros"));
}
