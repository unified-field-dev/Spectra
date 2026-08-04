use crate::codegen::parser;

#[test]
fn metric_parser_ignores_sampling_keys() {
    let content = r#"
use spectra_macros::spectra_metric;

spectra_metric! {
    PhotonBacklog {
        name: "photon_backlog",
        version: "0.1.0",
        description: "backlog",
        level: Trace,
        coalesce_ms: 200,
    }
}
"#;
    let parsed = parser::parse_metric_schema(content).unwrap();
    assert_eq!(parsed.name, "photon_backlog");
}

#[test]
fn event_parser_ignores_sampling_keys() {
    let content = r#"
use spectra_macros::spectra_schema;

spectra_schema! {
    ValenceQueryLog {
        table: "valence_query_log",
        version: "0.1.0",
        description: "query log",
        level: Debug,
        default_sample_rate: 0.25,
        fields: [
            query_id: { r#type: String },
        ],
    }
}
"#;
    let parsed = parser::parse_event_schema(content).unwrap();
    assert_eq!(parsed.table, "valence_query_log");
}
