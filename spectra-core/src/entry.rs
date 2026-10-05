pub use crate::dispatcher::set_sink;

/// Record a counter when a sink is configured; no-op if unset or re-entrant.
///
/// Inside an active emit-buffer scope ([`request_scope`](crate::request_scope) /
/// [`worker_scope`](crate::worker_scope)) the emit is buffered
/// (stamped with the current wall clock) and replayed off the hot path; otherwise it
/// dispatches inline.
///
/// Applications normally call generated `*Recorder` helpers, which delegate here. Use this
/// function directly for dynamic metric names or code that does not use schema macros.
/// Labels are borrowed only for the duration of the call.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use spectra_core::{
///     set_sink, try_record_counter, RecordingSink, SpectraSink,
/// };
///
/// let recording = Arc::new(RecordingSink::new());
/// set_sink(Arc::clone(&recording) as Arc<dyn SpectraSink>);
///
/// try_record_counter("cache_hits", &[("region", "us-west")], 1);
///
/// let captured = recording.recorded_counters_matching(
///     "cache_hits",
///     &[("region", "us-west")],
/// );
/// assert_eq!(captured.len(), 1);
/// assert_eq!(captured[0].delta, 1);
/// ```
pub fn try_record_counter(name: &str, labels: &[(&str, &str)], delta: i64) {
    if reject_invalid_ident(name) {
        return;
    }
    if !crate::emit_buffer::is_replaying() && crate::gate::drop_counter(name, labels) {
        return;
    }
    if crate::emit_buffer::push_counter(name, labels, delta) {
        return;
    }
    crate::dispatcher::enter_dispatch_counter(name, |sink| {
        sink.record_counter(name, labels, delta)
    });
}

/// Record a gauge when a sink is configured; no-op if unset or re-entrant.
pub fn try_record_gauge(name: &str, labels: &[(&str, &str)], value: f64) {
    if reject_invalid_ident(name) {
        return;
    }
    if !crate::emit_buffer::is_replaying() && crate::gate::drop_gauge(name, labels, value) {
        return;
    }
    if crate::emit_buffer::push_gauge(name, labels, value) {
        return;
    }
    crate::dispatcher::enter_dispatch_gauge(name, |sink| sink.record_gauge(name, labels, value));
}

/// Log a structured event when a sink is configured; no-op if unset or re-entrant.
pub fn try_log_event(table: &str, fields: &serde_json::Value) {
    if reject_invalid_ident(table) {
        return;
    }
    if !crate::emit_buffer::is_replaying() && crate::gate::drop_event(table) {
        return;
    }
    if crate::emit_buffer::push_event(table, fields) {
        return;
    }
    crate::dispatcher::enter_dispatch_event(table, |sink| sink.log_event(table, fields));
}

/// Record a counter immediately, bypassing any active emit buffer.
///
/// For critical/error telemetry that must survive a mid-scope panic (which drops the
/// buffered records) or be visible without waiting for the flush. Overuse re-introduces
/// the foreground dispatch cost the buffer removes.
pub fn try_record_counter_now(name: &str, labels: &[(&str, &str)], delta: i64) {
    if reject_invalid_ident(name) {
        return;
    }
    if crate::gate::drop_counter(name, labels) {
        return;
    }
    crate::dispatcher::enter_dispatch_counter(name, |sink| {
        sink.record_counter(name, labels, delta)
    });
}

/// Record a gauge immediately, bypassing any active emit buffer.
pub fn try_record_gauge_now(name: &str, labels: &[(&str, &str)], value: f64) {
    if reject_invalid_ident(name) {
        return;
    }
    if crate::gate::drop_gauge(name, labels, value) {
        return;
    }
    crate::dispatcher::enter_dispatch_gauge(name, |sink| sink.record_gauge(name, labels, value));
}

/// Log a structured event immediately, bypassing any active emit buffer.
pub fn try_log_event_now(table: &str, fields: &serde_json::Value) {
    if reject_invalid_ident(table) {
        return;
    }
    if crate::gate::drop_event(table) {
        return;
    }
    crate::dispatcher::enter_dispatch_event(table, |sink| sink.log_event(table, fields));
}

fn reject_invalid_ident(name: &str) -> bool {
    if crate::validate::is_valid_spectra_ident(name) {
        return false;
    }
    tracing::debug!(
        operation = "emit",
        reason = "invalid_ident",
        "dropping emit with invalid metric/table name"
    );
    true
}

/// Record a counter immediately with an explicit emit timestamp.
///
/// Sets the thread-local emit-time override for the duration of the call so sinks that
/// stamp via [`crate::current_emit_ts`] preserve `ts` (including async persist).
pub fn try_record_counter_at(
    name: &str,
    labels: &[(&str, &str)],
    delta: i64,
    ts: chrono::DateTime<chrono::Utc>,
) {
    crate::emit_buffer::with_emit_ts(ts, || try_record_counter_now(name, labels, delta));
}

/// Record a gauge immediately with an explicit emit timestamp.
pub fn try_record_gauge_at(
    name: &str,
    labels: &[(&str, &str)],
    value: f64,
    ts: chrono::DateTime<chrono::Utc>,
) {
    crate::emit_buffer::with_emit_ts(ts, || try_record_gauge_now(name, labels, value));
}

/// Log a structured event immediately with an explicit emit timestamp.
pub fn try_log_event_at(
    table: &str,
    fields: &serde_json::Value,
    ts: chrono::DateTime<chrono::Utc>,
) {
    crate::emit_buffer::with_emit_ts(ts, || try_log_event_now(table, fields));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::NoOpSink;
    use crate::{RecordingSink, SpectraSink};
    use std::sync::Arc;

    #[tokio::test]
    async fn drops_emit_with_invalid_metric_name() {
        let _g = crate::test_util::GLOBAL_TEST_LOCK.lock().await;
        crate::test_util::reset_gate_disabled();
        let recording = Arc::new(RecordingSink::new());
        set_sink(Arc::clone(&recording) as Arc<dyn SpectraSink>);
        try_record_counter("evil; DROP", &[], 1);
        assert_eq!(
            recording.recorded_counters_matching("evil; DROP", &[]),
            vec![]
        );
        try_record_counter("cache_hits", &[], 1);
        assert_eq!(
            recording
                .recorded_counters_matching("cache_hits", &[])
                .len(),
            1
        );
        set_sink(Arc::new(NoOpSink));
        crate::config::reset_config_for_test();
    }
}
