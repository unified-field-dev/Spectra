use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::parser::{ParsedEventSchema, ParsedMetricSchema};

/// What to emit from parsed schema definitions (upstream library).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitKind {
    /// Typed *Logger / *Recorder calling [`spectra_core`] facade directly.
    HelpersOnly,
    /// `forward_counter` / `forward_event` match arms for host composite sinks.
    SinkForward,
    /// Stable transport topic constants and payload DTOs for host publish adapters.
    TopicsOnly,
}

pub fn emit_helper_tokens(
    events: &[ParsedEventSchema],
    metrics: &[ParsedMetricSchema],
) -> anyhow::Result<TokenStream> {
    let mut out = TokenStream::new();
    for ev in events {
        out.extend(emit_event_helper(ev)?);
    }
    for m in metrics {
        out.extend(emit_metric_helper(m)?);
    }
    Ok(out)
}

pub fn generate_helpers(
    events: &[ParsedEventSchema],
    metrics: &[ParsedMetricSchema],
) -> anyhow::Result<String> {
    let helpers = emit_helper_tokens(events, metrics)?;
    let file = quote! {
        pub mod helpers {
            #helpers
        }
    };
    Ok(file.to_string())
}

fn emit_event_helper(ev: &ParsedEventSchema) -> anyhow::Result<TokenStream> {
    let struct_name = format_ident!("{}", ev.schema_name);
    let logger_name = format_ident!("{}Logger", ev.schema_name);
    let table_lit = &ev.table;

    let field_defs: Vec<_> = ev
        .fields
        .iter()
        .map(|f| {
            let ident = format_ident!("{}", f.name);
            let ty = map_rust_type(&f.rust_type);
            quote! { pub #ident: #ty }
        })
        .collect();

    let field_inits: Vec<_> = ev
        .fields
        .iter()
        .map(|f| {
            let ident = format_ident!("{}", f.name);
            quote! { #ident }
        })
        .collect();

    let field_params: Vec<_> = ev
        .fields
        .iter()
        .map(|f| {
            let ident = format_ident!("{}", f.name);
            let ty = map_rust_type(&f.rust_type);
            quote! { #ident: #ty }
        })
        .collect();

    let json_fields: Vec<_> = ev
        .fields
        .iter()
        .map(|f| {
            let key = &f.name;
            let ident = format_ident!("{}", f.name);
            quote! { map.insert(#key.to_string(), serde_json::json!(#ident)); }
        })
        .collect();

    Ok(quote! {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        pub struct #struct_name {
            #(#field_defs),*
        }

        pub struct #logger_name;

        impl #logger_name {
            /// Emit with an explicit timestamp (preserved through buffered drain).
            pub fn log_at(
                #(#field_params,)*
                _ts: chrono::DateTime<chrono::Utc>,
            ) {
                let mut map = serde_json::Map::new();
                #(#json_fields)*
                let fields = serde_json::Value::Object(map);
                ::spectra_core::try_log_event_now(#table_lit, &fields);
            }

            pub fn log(#(#field_params),*) {
                Self::log_at(#(#field_inits,)* chrono::Utc::now());
            }
        }
    })
}

fn emit_metric_helper(m: &ParsedMetricSchema) -> anyhow::Result<TokenStream> {
    let helper_name = format_ident!("{}Recorder", m.schema_name);
    let name_lit = &m.name;

    Ok(quote! {
        pub struct #helper_name;

        impl #helper_name {
            /// Record with an explicit emit-time timestamp.
            pub fn record_at(
                delta: i64,
                labels: serde_json::Value,
                _ts: chrono::DateTime<chrono::Utc>,
            ) {
                let label_pairs: Vec<(&str, &str)> = labels
                    .as_object()
                    .map(|obj| {
                        obj.iter()
                            .map(|(k, v)| (k.as_str(), v.as_str().unwrap_or("")))
                            .collect()
                    })
                    .unwrap_or_default();
                ::spectra_core::try_record_counter_now(#name_lit, &label_pairs, delta);
            }

            pub fn record(delta: i64, labels: serde_json::Value) {
                Self::record_at(delta, labels, chrono::Utc::now());
            }
        }
    })
}

fn map_rust_type(t: &str) -> TokenStream {
    match t {
        "String" => quote! { String },
        "i64" => quote! { i64 },
        "f64" => quote! { f64 },
        "bool" => quote! { bool },
        _ => quote! { String },
    }
}

/// Token stream for `forward_counter` / `forward_event` (nested under `sink_forward` beside `helpers`).
pub fn emit_sink_forward_tokens(
    events: &[ParsedEventSchema],
    metrics: &[ParsedMetricSchema],
) -> anyhow::Result<TokenStream> {
    let mut counter_arms = TokenStream::new();
    for m in metrics {
        let name_lit = &m.name;
        let recorder = format_ident!("{}Recorder", m.schema_name);
        counter_arms.extend(quote! {
            #name_lit => {
                use super::helpers::#recorder;
                #recorder::record_at(delta, labels, ts);
            }
        });
    }

    let mut event_arms = TokenStream::new();
    for ev in events {
        let table_lit = &ev.table;
        let logger = format_ident!("{}Logger", ev.schema_name);
        let field_args: Vec<_> = ev
            .fields
            .iter()
            .map(|f| {
                let key = &f.name;
                quote! { field_str(&fields, #key) }
            })
            .collect();
        event_arms.extend(quote! {
            #table_lit => {
                use super::helpers::#logger;
                #logger::log_at(#(#field_args,)* ts);
            }
        });
    }

    Ok(quote! {
        fn field_str(fields: &serde_json::Value, key: &str) -> String {
            fields
                .get(key)
                .and_then(|v| match v {
                    serde_json::Value::String(s) => Some(s.clone()),
                    serde_json::Value::Bool(b) => Some(b.to_string()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    _ => None,
                })
                .unwrap_or_default()
        }

        #[allow(unused_variables)]
        pub fn forward_counter(
            name: String,
            labels: serde_json::Value,
            delta: i64,
            ts: chrono::DateTime<chrono::Utc>,
        ) {
            match name.as_str() {
                #counter_arms
                _ => {}
            }
        }

        #[allow(unused_variables)]
        pub fn forward_event(
            table: String,
            fields: serde_json::Value,
            ts: chrono::DateTime<chrono::Utc>,
        ) {
            match table.as_str() {
                #event_arms
                _ => {}
            }
        }
    })
}

/// Generate `forward_counter` / `forward_event` match arms for host composite sinks.
pub fn emit_sink_forward(
    events: &[ParsedEventSchema],
    metrics: &[ParsedMetricSchema],
) -> anyhow::Result<String> {
    Ok(emit_sink_forward_tokens(events, metrics)?.to_string())
}

pub fn emit_topic_tokens(
    events: &[ParsedEventSchema],
    metrics: &[ParsedMetricSchema],
) -> anyhow::Result<TokenStream> {
    let mut out = TokenStream::new();
    for ev in events {
        out.extend(emit_event_topic(ev)?);
    }
    for m in metrics {
        out.extend(emit_metric_topic(m)?);
    }
    Ok(out)
}

pub fn generate_topics(
    events: &[ParsedEventSchema],
    metrics: &[ParsedMetricSchema],
) -> anyhow::Result<String> {
    let topics = emit_topic_tokens(events, metrics)?;
    let file = quote! {
        pub mod topics {
            #topics
        }
    };
    Ok(file.to_string())
}

fn emit_event_topic(ev: &ParsedEventSchema) -> anyhow::Result<TokenStream> {
    let payload_name = format_ident!("{}Payload", ev.schema_name);
    let topic_const = format_ident!("{}_TOPIC", to_shouty_snake(&ev.schema_name));
    let table_lit = &ev.table;
    let topic_expr = format!("spectra.event.{table_lit}");

    let field_defs: Vec<_> = ev
        .fields
        .iter()
        .map(|f| {
            let ident = format_ident!("{}", f.name);
            let ty = map_rust_type(&f.rust_type);
            quote! { pub #ident: #ty }
        })
        .collect();

    let json_fields: Vec<_> = ev
        .fields
        .iter()
        .map(|f| {
            let key = &f.name;
            let ident = format_ident!("{}", f.name);
            quote! { map.insert(#key.to_string(), serde_json::json!(self.#ident)); }
        })
        .collect();

    Ok(quote! {
        pub const #topic_const: &str = #topic_expr;

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        pub struct #payload_name {
            pub table: &'static str,
            #(#field_defs,)*
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub ts: Option<chrono::DateTime<chrono::Utc>>,
        }

        impl #payload_name {
            pub fn topic() -> &'static str {
                #topic_const
            }

            pub fn to_spectra_event(&self) -> ::spectra_core::SpectraEvent {
                let mut map = serde_json::Map::new();
                #(#json_fields)*
                let fields = serde_json::Value::Object(map);
                match self.ts {
                    Some(ts) => ::spectra_core::SpectraEvent::with_ts(#table_lit, fields, ts),
                    None => ::spectra_core::SpectraEvent::new(#table_lit, fields),
                }
            }
        }
    })
}

fn emit_metric_topic(m: &ParsedMetricSchema) -> anyhow::Result<TokenStream> {
    let payload_name = format_ident!("{}Payload", m.schema_name);
    let topic_const = format_ident!("{}_TOPIC", to_shouty_snake(&m.schema_name));
    let name_lit = &m.name;
    let topic_expr = format!("spectra.metric.{name_lit}");

    Ok(quote! {
        pub const #topic_const: &str = #topic_expr;

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        pub struct #payload_name {
            pub name: &'static str,
            pub labels: serde_json::Value,
            pub delta: i64,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub ts: Option<chrono::DateTime<chrono::Utc>>,
        }

        impl #payload_name {
            pub fn topic() -> &'static str {
                #topic_const
            }

            pub fn to_metric_emit(&self) -> ::spectra_core::MetricEmit {
                match self.ts {
                    Some(ts) => ::spectra_core::MetricEmit::counter(
                        #name_lit,
                        self.labels.clone(),
                        self.delta,
                        ts,
                    ),
                    None => ::spectra_core::MetricEmit::counter(
                        #name_lit,
                        self.labels.clone(),
                        self.delta,
                        chrono::Utc::now(),
                    ),
                }
            }
        }
    })
}

fn to_shouty_snake(s: &str) -> String {
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_uppercase());
        } else {
            out.push(c.to_ascii_uppercase());
        }
    }
    out
}
