//! Wire shape of one Spectra emit on the Photon topic.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One metric sample or event row, as carried between a publisher and a storage consumer.
///
/// The timestamp is the emit time on the publisher, so rows keep their original time however
/// late the consumer writes them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpectraEnvelope {
    /// Counter increment.
    Counter {
        /// Metric family name.
        name: String,
        /// Label set as a JSON object.
        labels: Value,
        /// Increment (may be negative).
        delta: i64,
        /// Emit time.
        ts: DateTime<Utc>,
    },
    /// Gauge sample.
    Gauge {
        /// Metric family name.
        name: String,
        /// Label set as a JSON object.
        labels: Value,
        /// Sampled value.
        value: f64,
        /// Emit time.
        ts: DateTime<Utc>,
    },
    /// Structured event row.
    Event {
        /// Event table name.
        table: String,
        /// Event fields.
        fields: Value,
        /// Emit time.
        ts: DateTime<Utc>,
    },
}

impl SpectraEnvelope {
    /// Key that selects the Photon shard: same series or table, same shard, so one consumer
    /// sees a series in emit order.
    pub(crate) fn partition_key(&self) -> String {
        match self {
            Self::Counter { name, labels, .. } | Self::Gauge { name, labels, .. } => {
                // `serde_json` objects keep sorted keys, so equal label sets give equal keys.
                format!("{name}{labels}")
            }
            Self::Event { table, .. } => table.clone(),
        }
    }
}

pub(crate) fn labels_to_value(labels: &[(&str, &str)]) -> Value {
    let mut map = Map::new();
    for (k, v) in labels {
        map.insert((*k).to_string(), Value::String((*v).to_string()));
    }
    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).expect("valid ts")
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let cases = [
            SpectraEnvelope::Counter {
                name: "hits".into(),
                labels: json!({"route": "/"}),
                delta: -3,
                ts: ts(),
            },
            SpectraEnvelope::Gauge {
                name: "depth".into(),
                labels: json!({}),
                value: 1.5,
                ts: ts(),
            },
            SpectraEnvelope::Event {
                table: "audit".into(),
                fields: json!({"who": "a"}),
                ts: ts(),
            },
        ];
        for case in cases {
            let wire = serde_json::to_value(&case).expect("serialize");
            let back: SpectraEnvelope = serde_json::from_value(wire).expect("deserialize");
            assert_eq!(back, case);
        }
    }

    #[test]
    fn partition_key_ignores_label_insertion_order() {
        let a = labels_to_value(&[("a", "1"), ("b", "2")]);
        let b = labels_to_value(&[("b", "2"), ("a", "1")]);
        let key = |labels| {
            SpectraEnvelope::Counter {
                name: "m".into(),
                labels,
                delta: 1,
                ts: ts(),
            }
            .partition_key()
        };
        assert_eq!(key(a), key(b));
    }

    #[test]
    fn unknown_kind_fails_to_decode() {
        let bad = json!({"kind": "histogram", "name": "x"});
        assert!(serde_json::from_value::<SpectraEnvelope>(bad).is_err());
    }
}
