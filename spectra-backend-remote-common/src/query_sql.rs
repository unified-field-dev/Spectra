//! SQL fragments for remote event queries.
//!
//! Metric/table/field tokens are validated with [`spectra_core::validate_spectra_ident`]
//! before interpolation. String literals use [`escape_str`] (rejects NUL). Event paging uses
//! [`spectra_core::clamp_event_paging`].

use spectra_core::{
    clamp_event_paging, validate_spectra_ident, Error, EventExploreView, EventMeasure,
    EventsAggregateFilter, EventsQueryFilter, GridFilterItem, GridFilterOperator,
    GridLogicOperator, Result, PIVOT_BLANK_LABEL,
};

/// Maximum `GROUP BY` rows one aggregate query returns.
///
/// Slice and Table views keep the largest groups by measure; time-series views keep the most
/// recent buckets. Narrow the time range or widen the bucket when a chart hits this cap.
pub const AGGREGATE_GROUP_LIMIT: u32 = 10_000;

/// Escape a string for use inside a single-quoted ClickHouse SQL literal.
///
/// Doubles `\` and `'`. Rejects NUL bytes so they cannot terminate or truncate literals.
///
/// # Errors
///
/// Returns [`Error::Config`] when `s` contains a NUL byte.
pub fn escape_str(s: &str) -> Result<String> {
    if s.contains('\0') {
        return Err(Error::config(
            "invalid SQL string literal: nul byte (operation=escape_str, reason=nul)",
        ));
    }
    Ok(s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Build the table/time/partition scope clause.
///
/// # Errors
///
/// Returns [`Error::Config`] when `filter.table` is not a valid Spectra identifier.
pub fn scope_clause(filter: &EventsQueryFilter) -> Result<String> {
    validate_spectra_ident(&filter.table)?;
    let mut clauses = vec![format!("table_name = '{}'", escape_str(&filter.table)?)];
    if let Some(start) = filter.start {
        clauses.push(format!("ts >= '{}'", escape_str(&start.to_rfc3339())?));
    }
    if let Some(end) = filter.end {
        clauses.push(format!("ts <= '{}'", escape_str(&end.to_rfc3339())?));
    }
    if let Some(ref p) = filter.partition {
        clauses.push(format!(
            "JSONExtractString(fields, 'partition') = '{}'",
            escape_str(p)?
        ));
    }
    Ok(clauses.join(" AND "))
}

/// Build an additional `AND (…)` filter clause from the grid model.
///
/// # Errors
///
/// Returns [`Error::Config`] when any filter field name is not a valid identifier, or when
/// a filter value contains a NUL byte.
pub fn filter_where_clause(filter: &spectra_core::GridFilterModel) -> Result<String> {
    if filter.items.is_empty() && filter.quick_filter_values.is_empty() {
        return Ok(String::new());
    }
    let mut parts = Vec::new();
    for item in &filter.items {
        if let Some(clause) = filter_item_clause(item)? {
            parts.push(clause);
        }
    }
    if !filter.quick_filter_values.is_empty() {
        let mut q_parts = Vec::new();
        for v in &filter.quick_filter_values {
            q_parts.push(format!(
                "positionCaseInsensitive(fields, '{}') > 0",
                escape_str(v)?
            ));
        }
        if !q_parts.is_empty() {
            parts.push(format!("({})", q_parts.join(" OR ")));
        }
    }
    if parts.is_empty() {
        return Ok(String::new());
    }
    let op = match filter.logic_operator {
        GridLogicOperator::And => " AND ",
        GridLogicOperator::Or => " OR ",
    };
    Ok(format!(" AND ({})", parts.join(op)))
}

fn field_path(field: &str) -> Result<String> {
    if field == "ts" {
        return Ok("ts".to_string());
    }
    validate_spectra_ident(field)?;
    Ok(format!(
        "JSONExtractString(fields, '{}')",
        escape_str(field)?
    ))
}

fn filter_item_clause(item: &GridFilterItem) -> Result<Option<String>> {
    let path = field_path(&item.field)?;
    Ok(match item.operator {
        GridFilterOperator::Equals => {
            if let Some(v) = item.value.as_str() {
                Some(format!("{path} = '{}'", escape_str(v)?))
            } else {
                item.value
                    .as_f64()
                    .map(|v| format!("toFloat64OrZero({path}) = {v}"))
            }
        }
        GridFilterOperator::DoesNotEqual => {
            if let Some(v) = item.value.as_str() {
                Some(format!("{path} != '{}'", escape_str(v)?))
            } else {
                item.value
                    .as_f64()
                    .map(|v| format!("toFloat64OrZero({path}) != {v}"))
            }
        }
        GridFilterOperator::Contains => item
            .value
            .as_str()
            .map(|v| {
                escape_str(v).map(|esc| format!("positionCaseInsensitive({path}, '{esc}') > 0"))
            })
            .transpose()?,
        GridFilterOperator::StartsWith => item
            .value
            .as_str()
            .map(|v| escape_str(v).map(|esc| format!("startsWith(lower({path}), lower('{esc}'))")))
            .transpose()?,
        GridFilterOperator::EndsWith => item
            .value
            .as_str()
            .map(|v| escape_str(v).map(|esc| format!("endsWith(lower({path}), lower('{esc}'))")))
            .transpose()?,
        GridFilterOperator::IsEmpty => Some(format!("({path} = '' OR isNull({path}))")),
        GridFilterOperator::IsNotEmpty => Some(format!("({path} != '' AND isNotNull({path}))")),
        GridFilterOperator::GreaterThan => item
            .value
            .as_f64()
            .map(|v| format!("toFloat64OrZero({path}) > {v}")),
        GridFilterOperator::GreaterThanOrEqual => item
            .value
            .as_f64()
            .map(|v| format!("toFloat64OrZero({path}) >= {v}")),
        GridFilterOperator::LessThan => item
            .value
            .as_f64()
            .map(|v| format!("toFloat64OrZero({path}) < {v}")),
        GridFilterOperator::LessThanOrEqual => item
            .value
            .as_f64()
            .map(|v| format!("toFloat64OrZero({path}) <= {v}")),
    })
}

/// Build an `ORDER BY` clause.
///
/// # Errors
///
/// Returns [`Error::Config`] when `sort_field` is set and is not a valid identifier.
pub fn order_clause(filter: &EventsQueryFilter) -> Result<String> {
    let dir = if filter.sort_desc { "DESC" } else { "ASC" };
    let field = filter.sort_field.as_deref().unwrap_or("ts");
    if field == "ts" {
        return Ok(format!("ORDER BY ts {dir}"));
    }
    validate_spectra_ident(field)?;
    Ok(format!(
        "ORDER BY JSONExtractString(fields, '{}') {dir}, ts {dir}",
        escape_str(field)?
    ))
}

/// `LIMIT` / `OFFSET` clause after [`clamp_event_paging`].
///
/// Omits `OFFSET 0` for TensorBase SQL compatibility.
#[must_use]
pub fn limit_offset_clause(limit: u32, offset: u32) -> String {
    let (limit, offset) = clamp_event_paging(Some(limit), Some(offset));
    if offset == 0 {
        format!("LIMIT {limit}")
    } else {
        format!("LIMIT {limit} OFFSET {offset}")
    }
}

/// Clamp and format paging from an [`EventsQueryFilter`].
#[must_use]
pub fn paging_clause(filter: &EventsQueryFilter) -> String {
    let (limit, offset) = clamp_event_paging(filter.limit, filter.offset);
    limit_offset_clause(limit, offset)
}

/// Build the `GROUP BY` query behind an aggregate chart view.
///
/// The query returns two columns, `label String` and `value Float64`:
///
/// - time series / line: `label` is the bucket start as Unix seconds, aligned to the epoch
///   the same way [`spectra_core::aggregate_event_rows`] aligns in-memory rows;
/// - pie / bar: `label` is the group field value (strings unquoted, other JSON raw), and
///   rows without the field are excluded;
/// - table: `label` is a JSON array of row-key labels followed by the pivot label when a
///   pivot field is set. Rows missing any row field are excluded; a missing or `null`
///   pivot value becomes [`PIVOT_BLANK_LABEL`].
///
/// Sum reads numeric JSON values only (strings contribute `0`), matching the in-memory
/// aggregator. Results are capped at [`AGGREGATE_GROUP_LIMIT`] rows.
///
/// Returns `Ok(None)` when the view has nothing to aggregate: the event log, a slice view
/// without a group-by field, or a table without row fields.
///
/// # Errors
///
/// Returns [`Error::Config`] when the table, group-by, row, pivot, measure, or filter field
/// is not a valid Spectra identifier, or a filter value contains a NUL byte.
pub fn aggregate_sql(filter: &EventsAggregateFilter) -> Result<Option<String>> {
    let (label, required_fields, order) = match filter.view {
        EventExploreView::EventLog => return Ok(None),
        EventExploreView::TimeSeries | EventExploreView::LineChart => {
            let bucket = filter.bucket_secs();
            (
                format!(
                    "toString(intDiv(toUnixTimestamp(parseDateTimeBestEffort(ts)), {bucket}) * {bucket})"
                ),
                Vec::new(),
                "label DESC",
            )
        }
        EventExploreView::PieChart | EventExploreView::BarChart => {
            let Some(field) = filter.group_by_field_name() else {
                return Ok(None);
            };
            let key = json_key(field)?;
            (
                format!(
                    "if(JSONType(fields, {key}) = 'String', JSONExtractString(fields, {key}), \
                     JSONExtractRaw(fields, {key}))"
                ),
                vec![key],
                "value DESC, label ASC",
            )
        }
        EventExploreView::Table => {
            let row_fields = filter.row_field_names();
            if row_fields.is_empty() {
                return Ok(None);
            }
            let keys = row_fields
                .iter()
                .map(|f| json_key(f))
                .collect::<Result<Vec<_>>>()?;
            let mut parts: Vec<String> = keys.iter().map(|k| table_label_expr(k)).collect();
            if let Some(pivot) = filter.pivot_field_name() {
                let key = json_key(pivot)?;
                parts.push(format!(
                    "if(JSONHas(fields, {key}), {}, '{PIVOT_BLANK_LABEL}')",
                    table_label_expr(&key)
                ));
            }
            (
                format!("toJSONString([{}])", parts.join(", ")),
                keys,
                "value DESC, label ASC",
            )
        }
    };

    let scope = scope_clause(&EventsQueryFilter {
        table: filter.table.clone(),
        start: Some(filter.start),
        end: Some(filter.end),
        partition: filter.partition.clone(),
        ..Default::default()
    })?;
    let extra = filter_where_clause(&filter.filter)?;
    let required = required_fields
        .iter()
        .map(|key| format!(" AND JSONHas(fields, {key})"))
        .collect::<Vec<_>>()
        .concat();
    let measure = measure_expr(filter)?;
    Ok(Some(format!(
        "SELECT {label} AS label, {measure} AS value FROM spectra_events \
         WHERE {scope}{extra}{required} GROUP BY label ORDER BY {order} \
         LIMIT {AGGREGATE_GROUP_LIMIT}"
    )))
}

/// Validated, quoted JSON key literal (`'field'`).
fn json_key(field: &str) -> Result<String> {
    validate_spectra_ident(field)?;
    Ok(format!("'{}'", escape_str(field.trim())?))
}

/// Table label for one field: strings unquoted, JSON `null` as [`PIVOT_BLANK_LABEL`], other
/// JSON values raw.
fn table_label_expr(key: &str) -> String {
    format!(
        "multiIf(JSONType(fields, {key}) = 'String', JSONExtractString(fields, {key}), \
         JSONType(fields, {key}) = 'Null', '{PIVOT_BLANK_LABEL}', JSONExtractRaw(fields, {key}))"
    )
}

fn measure_expr(filter: &EventsAggregateFilter) -> Result<String> {
    Ok(match filter.measure {
        EventMeasure::Count => "toFloat64(count())".to_string(),
        EventMeasure::Sum => match filter
            .measure_field
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            // JSONExtractFloat would also parse numeric strings; the in-memory aggregator
            // counts only JSON numbers.
            Some(field) => {
                let key = json_key(field)?;
                format!(
                    "toFloat64(sum(if(JSONType(fields, {key}) IN ('Int64', 'UInt64', 'Double'), \
                     JSONExtractFloat(fields, {key}), 0)))"
                )
            }
            None => "toFloat64(0)".to_string(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use spectra_core::{
        EventsQueryFilter, GridFilterItem, GridFilterModel, GridFilterOperator,
        MAX_EVENT_QUERY_LIMIT,
    };

    #[test]
    fn scope_includes_table() {
        let filter = EventsQueryFilter {
            table: "req_log".into(),
            ..Default::default()
        };
        let scope = scope_clause(&filter).expect("scope");
        assert!(scope.contains("table_name = 'req_log'"));
    }

    #[test]
    fn scope_rejects_bad_table() {
        let filter = EventsQueryFilter {
            table: "req; DROP".into(),
            ..Default::default()
        };
        assert!(scope_clause(&filter).is_err());
    }

    #[test]
    fn filter_covers_all_operators() {
        let ops = [
            (GridFilterOperator::Equals, json!("x")),
            (GridFilterOperator::DoesNotEqual, json!("x")),
            (GridFilterOperator::Contains, json!("x")),
            (GridFilterOperator::StartsWith, json!("x")),
            (GridFilterOperator::EndsWith, json!("x")),
            (GridFilterOperator::IsEmpty, json!(null)),
            (GridFilterOperator::IsNotEmpty, json!(null)),
            (GridFilterOperator::GreaterThan, json!(1.0)),
            (GridFilterOperator::GreaterThanOrEqual, json!(1.0)),
            (GridFilterOperator::LessThan, json!(1.0)),
            (GridFilterOperator::LessThanOrEqual, json!(1.0)),
        ];
        for (operator, value) in ops {
            let model = GridFilterModel {
                items: vec![GridFilterItem {
                    field: "msg".into(),
                    operator,
                    value,
                }],
                ..Default::default()
            };
            let clause = filter_where_clause(&model).expect("filter");
            assert!(
                clause.starts_with(" AND ("),
                "expected clause for {:?}",
                model.items[0].operator
            );
        }
    }

    #[test]
    fn filter_rejects_bad_field() {
        let model = GridFilterModel {
            items: vec![GridFilterItem {
                field: "msg; DROP".into(),
                operator: GridFilterOperator::Equals,
                value: json!("x"),
            }],
            ..Default::default()
        };
        assert!(filter_where_clause(&model).is_err());
    }

    #[test]
    fn escape_quotes_and_backslashes() {
        assert_eq!(escape_str(r"a'b\c").expect("esc"), r"a\'b\\c");
    }

    #[test]
    fn escape_rejects_nul() {
        assert!(escape_str("a\0b").is_err());
    }

    #[test]
    fn order_honors_sort_field() {
        let filter = EventsQueryFilter {
            table: "t".into(),
            sort_field: Some("region".into()),
            sort_desc: true,
            ..Default::default()
        };
        let order = order_clause(&filter).expect("order");
        assert!(order.contains("JSONExtractString(fields, 'region')"));
        assert!(order.contains("DESC"));
    }

    #[test]
    fn order_rejects_bad_sort_field() {
        let filter = EventsQueryFilter {
            table: "t".into(),
            sort_field: Some("region DESC;--".into()),
            ..Default::default()
        };
        assert!(order_clause(&filter).is_err());
    }

    #[test]
    fn paging_clamps_huge_limit() {
        let clause = limit_offset_clause(u32::MAX, 0);
        assert_eq!(clause, format!("LIMIT {MAX_EVENT_QUERY_LIMIT}"));
    }

    fn agg(view: EventExploreView) -> EventsAggregateFilter {
        let end = chrono::Utc::now();
        EventsAggregateFilter {
            table: "req_log".into(),
            start: end - chrono::Duration::hours(1),
            end,
            partition: None,
            filter: GridFilterModel::default(),
            measure: EventMeasure::Count,
            measure_field: None,
            time_bucket_secs: None,
            group_by_field: None,
            row_fields: Vec::new(),
            pivot_field: None,
            view,
        }
    }

    #[test]
    fn aggregate_sql_time_series_buckets_by_epoch() {
        let sql = aggregate_sql(&EventsAggregateFilter {
            time_bucket_secs: Some(300),
            ..agg(EventExploreView::TimeSeries)
        })
        .expect("sql")
        .expect("time series aggregates");
        assert!(sql.contains("intDiv(toUnixTimestamp(parseDateTimeBestEffort(ts)), 300) * 300"));
        assert!(sql.contains("toFloat64(count()) AS value"));
        assert!(sql.contains("table_name = 'req_log'"));
        assert!(sql.contains("GROUP BY label ORDER BY label DESC"));
        assert!(sql.ends_with(&format!("LIMIT {AGGREGATE_GROUP_LIMIT}")));
    }

    #[test]
    fn aggregate_sql_time_series_defaults_bucket_to_an_hour() {
        let sql = aggregate_sql(&agg(EventExploreView::LineChart))
            .expect("sql")
            .expect("line aggregates");
        assert!(sql.contains("3600) * 3600"));
    }

    #[test]
    fn aggregate_sql_slices_require_group_field_and_sum_numbers_only() {
        let sql = aggregate_sql(&EventsAggregateFilter {
            group_by_field: Some(" region ".into()),
            measure: EventMeasure::Sum,
            measure_field: Some("bytes".into()),
            ..agg(EventExploreView::BarChart)
        })
        .expect("sql")
        .expect("bar aggregates");
        assert!(sql.contains("JSONExtractString(fields, 'region')"));
        assert!(sql.contains("AND JSONHas(fields, 'region')"));
        assert!(sql.contains("IN ('Int64', 'UInt64', 'Double')"));
        assert!(sql.contains("JSONExtractFloat(fields, 'bytes')"));
        assert!(sql.contains("ORDER BY value DESC, label ASC"));
    }

    #[test]
    fn aggregate_sql_sum_without_field_is_zero() {
        let sql = aggregate_sql(&EventsAggregateFilter {
            measure: EventMeasure::Sum,
            measure_field: Some("  ".into()),
            ..agg(EventExploreView::TimeSeries)
        })
        .expect("sql")
        .expect("aggregates");
        assert!(sql.contains("toFloat64(0) AS value"));
    }

    #[test]
    fn aggregate_sql_table_keys_rows_then_pivot() {
        let sql = aggregate_sql(&EventsAggregateFilter {
            row_fields: vec!["region".into(), "host".into()],
            pivot_field: Some("severity".into()),
            ..agg(EventExploreView::Table)
        })
        .expect("sql")
        .expect("table aggregates");
        let region = sql
            .find("JSONExtractString(fields, 'region')")
            .expect("region");
        let host = sql.find("JSONExtractString(fields, 'host')").expect("host");
        let pivot = sql.find("if(JSONHas(fields, 'severity')").expect("pivot");
        assert!(
            region < host && host < pivot,
            "row keys precede pivot: {sql}"
        );
        assert!(sql.starts_with("SELECT toJSONString(["));
        assert!(sql.contains("AND JSONHas(fields, 'region') AND JSONHas(fields, 'host')"));
        assert!(sql.contains("'(blank)'"));
    }

    #[test]
    fn aggregate_sql_applies_grid_filter() {
        let sql = aggregate_sql(&EventsAggregateFilter {
            filter: GridFilterModel {
                items: vec![GridFilterItem {
                    field: "region".into(),
                    operator: GridFilterOperator::Equals,
                    value: json!("us"),
                }],
                ..Default::default()
            },
            ..agg(EventExploreView::TimeSeries)
        })
        .expect("sql")
        .expect("aggregates");
        assert!(sql.contains(" AND (JSONExtractString(fields, 'region') = 'us')"));
    }

    #[test]
    fn aggregate_sql_none_when_nothing_to_aggregate() {
        assert!(aggregate_sql(&agg(EventExploreView::EventLog))
            .expect("sql")
            .is_none());
        assert!(aggregate_sql(&EventsAggregateFilter {
            group_by_field: Some("   ".into()),
            ..agg(EventExploreView::PieChart)
        })
        .expect("sql")
        .is_none());
        assert!(aggregate_sql(&EventsAggregateFilter {
            row_fields: vec![" ".into()],
            ..agg(EventExploreView::Table)
        })
        .expect("sql")
        .is_none());
    }

    #[test]
    fn aggregate_sql_rejects_injected_identifiers() {
        let bad = "x') OR 1=1 --";
        let cases = [
            EventsAggregateFilter {
                table: bad.into(),
                ..agg(EventExploreView::TimeSeries)
            },
            EventsAggregateFilter {
                group_by_field: Some(bad.into()),
                ..agg(EventExploreView::PieChart)
            },
            EventsAggregateFilter {
                row_fields: vec!["region".into(), bad.into()],
                ..agg(EventExploreView::Table)
            },
            EventsAggregateFilter {
                row_fields: vec!["region".into()],
                pivot_field: Some(bad.into()),
                ..agg(EventExploreView::Table)
            },
            EventsAggregateFilter {
                measure: EventMeasure::Sum,
                measure_field: Some(bad.into()),
                ..agg(EventExploreView::TimeSeries)
            },
        ];
        for filter in cases {
            let err = aggregate_sql(&filter).expect_err("injection must be rejected");
            assert!(matches!(err, Error::Config(_)), "{err:?}");
        }
    }
}
