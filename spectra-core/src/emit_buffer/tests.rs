use super::scopes::{request_scope_gated, worker_scope_gated};
use super::{current_emit_ts, drain};
use crate::sink::SpectraSink;
use crate::sinks::{NoOpSink, RecordingSink};
use chrono::Utc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Serialize tests touching process-global sink / gate state.
use crate::test_util::GLOBAL_TEST_LOCK as TEST_LOCK;

fn install_recording() -> RecordingSink {
    crate::test_util::reset_gate_disabled();
    let sink = RecordingSink::new();
    crate::set_sink(Arc::new(sink.clone()));
    sink
}

fn reset_sink() {
    crate::set_sink(Arc::new(NoOpSink));
    crate::config::reset_config_for_test();
}

/// Sink that records the `current_emit_ts()` observed at dispatch time.
#[derive(Clone)]
struct TsSink {
    seen: Arc<Mutex<Vec<chrono::DateTime<Utc>>>>,
}

impl SpectraSink for TsSink {
    fn record_counter(&self, _n: &str, _l: &[(&str, &str)], _d: i64) {
        self.seen.lock().unwrap().push(current_emit_ts());
    }
    fn record_gauge(&self, _n: &str, _l: &[(&str, &str)], _v: f64) {
        self.seen.lock().unwrap().push(current_emit_ts());
    }
    fn log_event(&self, _t: &str, _f: &serde_json::Value) {
        self.seen.lock().unwrap().push(current_emit_ts());
    }
}

#[tokio::test]
async fn request_scope_buffers_until_drain() {
    let _g = TEST_LOCK.lock().await;
    let sink = install_recording();

    let ((), records) = request_scope_gated(true, async {
        crate::try_record_counter("c", &[("k", "v")], 1);
        crate::try_log_event("t", &serde_json::json!({"a": 1}));
    })
    .await;

    assert_eq!(sink.counters(), vec![]);
    assert_eq!(sink.events(), vec![]);
    assert_eq!(records.len(), 2);

    drain(records);
    assert_eq!(sink.counters().len(), 1);
    assert_eq!(sink.events().len(), 1);
    reset_sink();
}

#[tokio::test]
async fn request_scope_disabled_passes_through() {
    let _g = TEST_LOCK.lock().await;
    let sink = install_recording();

    let ((), records) = request_scope_gated(false, async {
        crate::try_record_counter("c", &[], 1);
    })
    .await;

    assert_eq!(sink.counters().len(), 1);
    assert!(records.is_empty());
    reset_sink();
}

#[tokio::test]
async fn nested_request_scope_drains_once() {
    let _g = TEST_LOCK.lock().await;
    let sink = install_recording();

    let ((), records) = request_scope_gated(true, async {
        crate::try_record_counter("outer", &[], 1);
        let ((), inner) = request_scope_gated(true, async {
            crate::try_record_counter("inner", &[], 1);
        })
        .await;
        assert!(inner.is_empty());
    })
    .await;

    assert_eq!(sink.counters(), vec![]);
    assert_eq!(records.len(), 2);
    drain(records);
    assert_eq!(sink.counters().len(), 2);
    reset_sink();
}

#[tokio::test]
async fn now_bypass_dispatches_immediately() {
    let _g = TEST_LOCK.lock().await;
    let sink = install_recording();

    let ((), records) = request_scope_gated(true, async {
        crate::try_record_counter("buffered", &[], 1);
        crate::try_log_event_now("now", &serde_json::json!({}));
    })
    .await;

    assert_eq!(sink.events().len(), 1);
    assert_eq!(sink.events()[0].table, "now");
    assert_eq!(records.len(), 1);
    reset_sink();
}

#[tokio::test]
async fn drain_replay_does_not_rebuffer() {
    let _g = TEST_LOCK.lock().await;
    let sink = install_recording();

    let ((), records) = request_scope_gated(true, async {
        crate::try_record_counter("c", &[], 1);
    })
    .await;
    assert_eq!(records.len(), 1);

    let ((), outer) = request_scope_gated(true, async {
        drain(records);
        crate::try_record_counter("only_outer", &[], 1);
    })
    .await;
    assert_eq!(sink.counters().len(), 1);
    assert_eq!(sink.counters()[0].name, "c");
    assert_eq!(outer.len(), 1);
    reset_sink();
}

#[tokio::test]
async fn emit_time_ts_preserved_through_delay() {
    let _g = TEST_LOCK.lock().await;
    crate::test_util::reset_gate_disabled();
    let seen = Arc::new(Mutex::new(Vec::new()));
    crate::set_sink(Arc::new(TsSink { seen: seen.clone() }));

    let t0 = Utc::now();
    let ((), records) = request_scope_gated(true, async {
        crate::try_record_counter("c", &[], 1);
    })
    .await;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let drain_time = Utc::now();
    drain(records);

    let observed = seen.lock().unwrap()[0];
    assert!((observed - t0).num_milliseconds().abs() < 20);
    assert!((drain_time - observed).num_milliseconds() >= 20);
    reset_sink();
}

#[tokio::test]
async fn current_emit_ts_is_now_without_override() {
    let _g = TEST_LOCK.lock().await;
    let before = Utc::now();
    let ts = current_emit_ts();
    let after = Utc::now();
    assert!(ts >= before && ts <= after);
}

#[tokio::test]
async fn try_record_counter_at_preserves_explicit_ts() {
    let _g = TEST_LOCK.lock().await;
    crate::test_util::reset_gate_disabled();
    let seen = Arc::new(Mutex::new(Vec::new()));
    crate::set_sink(Arc::new(TsSink { seen: seen.clone() }));

    let explicit = Utc::now() - chrono::Duration::hours(3);
    crate::try_record_counter_at("c", &[("k", "v")], 1, explicit);

    let observed = seen.lock().unwrap()[0];
    assert_eq!(observed, explicit);
    reset_sink();
}

#[tokio::test]
async fn try_log_event_at_preserves_explicit_ts() {
    let _g = TEST_LOCK.lock().await;
    crate::test_util::reset_gate_disabled();
    let seen = Arc::new(Mutex::new(Vec::new()));
    crate::set_sink(Arc::new(TsSink { seen: seen.clone() }));

    let explicit = Utc::now() - chrono::Duration::minutes(45);
    crate::try_log_event_at("t", &serde_json::json!({"a": 1}), explicit);
    assert_eq!(seen.lock().unwrap()[0], explicit);
    reset_sink();
}

#[tokio::test]
async fn worker_scope_drains_blocking_at_end() {
    let _g = TEST_LOCK.lock().await;
    let sink = install_recording();

    worker_scope_gated(true, async {
        crate::try_record_counter("c", &[], 1);
        crate::try_log_event("t", &serde_json::json!({}));
        assert_eq!(sink.counters(), vec![]);
        assert_eq!(sink.events(), vec![]);
    })
    .await;

    assert_eq!(sink.counters().len(), 1);
    assert_eq!(sink.events().len(), 1);
    reset_sink();
}

#[tokio::test]
async fn worker_scope_disabled_passes_through() {
    let _g = TEST_LOCK.lock().await;
    let sink = install_recording();

    worker_scope_gated(false, async {
        crate::try_record_counter("c", &[], 1);
        assert_eq!(sink.counters().len(), 1);
    })
    .await;
    reset_sink();
}

#[tokio::test]
async fn nested_worker_scope_drains_once() {
    let _g = TEST_LOCK.lock().await;
    let sink = install_recording();

    worker_scope_gated(true, async {
        crate::try_record_counter("outer", &[], 1);
        worker_scope_gated(true, async {
            crate::try_record_counter("inner", &[], 1);
        })
        .await;
        assert_eq!(sink.counters(), vec![]);
    })
    .await;

    assert_eq!(sink.counters().len(), 2);
    reset_sink();
}

#[tokio::test]
async fn worker_scope_drains_on_error_path() {
    let _g = TEST_LOCK.lock().await;
    let sink = install_recording();

    let result: Result<(), ()> = worker_scope_gated(true, async {
        crate::try_record_counter("c", &[], 1);
        Err(())
    })
    .await;

    assert!(result.is_err());
    assert_eq!(sink.counters().len(), 1);
    reset_sink();
}

#[tokio::test]
async fn drain_aggregates_counters_when_enabled() {
    let _g = TEST_LOCK.lock().await;
    let sink = install_recording();
    std::env::set_var("SPECTRA_COUNTER_AGGREGATE", "1");

    let ((), records) = request_scope_gated(true, async {
        crate::try_record_counter("c", &[("k", "v")], 3);
        crate::try_record_counter("c", &[("k", "v")], 5);
        crate::try_record_gauge("g", &[], 1.5);
        crate::try_log_event("t", &serde_json::json!({}));
    })
    .await;

    assert_eq!(records.len(), 4);
    drain(records);
    assert_eq!(sink.counters().len(), 1);
    assert_eq!(sink.counters()[0].delta, 8);
    assert_eq!(sink.gauges().len(), 1);
    assert_eq!(sink.events().len(), 1);

    std::env::remove_var("SPECTRA_COUNTER_AGGREGATE");
    reset_sink();
}

#[tokio::test]
async fn drain_aggregation_respects_replay_guard() {
    let _g = TEST_LOCK.lock().await;
    let sink = install_recording();
    std::env::set_var("SPECTRA_COUNTER_AGGREGATE", "1");

    let ((), records) = request_scope_gated(true, async {
        crate::try_record_counter("c", &[], 1);
    })
    .await;

    let ((), outer) = request_scope_gated(true, async {
        drain(records);
        crate::try_record_counter("only_outer", &[], 1);
    })
    .await;

    assert_eq!(sink.counters().len(), 1);
    assert_eq!(sink.counters()[0].name, "c");
    assert_eq!(sink.counters()[0].delta, 1);
    assert_eq!(outer.len(), 1);

    std::env::remove_var("SPECTRA_COUNTER_AGGREGATE");
    reset_sink();
}
