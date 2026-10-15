#!/usr/bin/env python3
"""Fill PERFORMANCE_STUDY.md scoreboard tables from bench report JSON.

Usage: fill-bench-scoreboard.py [REPORT_DIR]

REPORT_DIR (or SPECTRA_BENCH_REPORT_DIR) defaults to profiling/spectra-bench/reports.
For decision-grade numbers, point it at a directory of AWS campaign JSON.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS = Path(
    sys.argv[1]
    if len(sys.argv) > 1
    else os.environ.get("SPECTRA_BENCH_REPORT_DIR")
    or ROOT / "profiling" / "spectra-bench" / "reports"
)
STUDY = ROOT / "docs" / "bench" / "PERFORMANCE_STUDY.md"


def load_reports() -> list[dict]:
    rows: list[dict] = []
    for path in sorted(REPORTS.glob("*.json")):
        data = json.loads(path.read_text())
        if isinstance(data, list):
            for item in data:
                item["_path"] = str(path)
                rows.append(item)
        else:
            data["_path"] = str(path)
            rows.append(data)
    return rows


def fmt(n: float | None) -> str:
    if n is None:
        return "_TBD_"
    if n >= 1000:
        return f"{n:,.0f}"
    if n >= 10:
        return f"{n:.1f}"
    return f"{n:.3f}"


def find(
    rows: list[dict],
    experiment: str,
    storage: str,
    topology: str,
    prefill: int | None = None,
) -> dict | None:
    for r in rows:
        if r.get("experiment") != experiment:
            continue
        m = r.get("matrix") or {}
        if m.get("storage") != storage or m.get("topology") != topology:
            continue
        if prefill is not None:
            sweep = r.get("sweep") or {}
            if sweep.get("prefill") != prefill:
                continue
        return r
    return None


def build_tables(rows: list[dict]) -> dict[str, str]:
    hw = "aws-t3-xlarge"
    for r in rows:
        if r.get("hardware"):
            hw = r["hardware"]
            break

    write_lines = [
        "| Storage | Topology | achieved_counter_ops_per_sec |",
        "|---------|----------|------------------------------|",
    ]
    for storage, topology in [
        ("mem", "embedded"),
        ("sqlite", "embedded"),
        ("tensorbase", "remote-ingest"),
        ("clickhouse", "remote-ingest"),
    ]:
        r = find(rows, "bm-sw1", storage, topology)
        ops = r.get("achieved_counter_ops_per_sec") if r else None
        write_lines.append(f"| {storage} | {topology} | {fmt(ops)} |")

    adapter_lines = [
        "| Storage | Topology | BM-SW0 ops/s | BM-SW1 ops/s |",
        "|---------|----------|--------------|--------------|",
    ]
    for storage, topology in [
        ("mem", "embedded"),
        ("sqlite", "embedded"),
        ("tensorbase", "remote-ingest"),
        ("clickhouse", "remote-ingest"),
    ]:
        r0 = find(rows, "bm-sw0", storage, topology)
        r1 = find(rows, "bm-sw1", storage, topology)
        sw0 = (r0 or {}).get("achieved_adapter_ops_per_sec")
        sw1 = (r1 or {}).get("achieved_counter_ops_per_sec")
        adapter_lines.append(
            f"| {storage} | {topology} | {fmt(sw0)} | {fmt(sw1)} |"
        )

    query_lines = [
        "| Storage | Topology | prefill | query_metrics_ms p50 | query_metrics_ms p95 |",
        "|---------|----------|---------|----------------------|----------------------|",
    ]
    for storage, topology in [
        ("mem", "embedded"),
        ("sqlite", "embedded"),
        ("tensorbase", "remote-ingest"),
        ("clickhouse", "remote-ingest"),
    ]:
        for prefill in (1000, 10000, 100000, 1000000):
            r = find(rows, "bm-sq1", storage, topology, prefill=prefill)
            if r is None:
                continue
            stats = (r or {}).get("query_metrics_ms") or {}
            p50 = stats.get("p50")
            p95 = stats.get("p95")
            query_lines.append(
                f"| {storage} | {topology} | {prefill:_} | {fmt(p50)} | {fmt(p95)} |"
            )

    return {
        "write": "\n".join(write_lines),
        "adapter": "\n".join(adapter_lines),
        "query": "\n".join(query_lines),
        "hw": hw,
    }


def replace_section(text: str, heading: str, table: str) -> str:
    pattern = rf"(### {re.escape(heading)}\n\n)(.*?)(\n\n### |\n\n## |\Z)"
    m = re.search(pattern, text, flags=re.S)
    if not m:
        raise SystemExit(f"section not found: {heading}")
    return text[: m.start(2)] + table + text[m.end(2) :]


def main() -> int:
    rows = load_reports()
    if not rows:
        print("No reports found under", REPORTS, file=sys.stderr)
        return 1
    tables = build_tables(rows)
    text = STUDY.read_text()
    text = replace_section(text, "Write enqueue (BM-SW1, 30s, C=16)", tables["write"])
    text = replace_section(
        text, "Adapter-direct vs full-stack (BM-SW0 vs BM-SW1)", tables["adapter"]
    )
    text = replace_section(
        text, "Query at depth (BM-SQ1, query_iters=100)", tables["query"]
    )
    text = text.replace(
        "| **Hardware** | `SPECTRA_BENCH_HARDWARE` (default campaign: `aws-t3-xlarge`) |",
        f"| **Hardware** | `{tables['hw']}` |",
    )
    # Also refresh the filled hardware row if already stamped
    text = re.sub(
        r"\| \*\*Hardware\*\* \| `aws-t3-[^`]+` \|",
        f"| **Hardware** | `{tables['hw']}` |",
        text,
        count=1,
    )
    STUDY.write_text(text)
    print(f"Updated {STUDY} from {len(rows)} report rows (hardware={tables['hw']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
