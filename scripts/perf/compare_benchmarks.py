"""
Compare two benchmark JSON snapshots and print a side-by-side diff table.

Works with both ingest (benchmark_ingest.py) and endpoint (benchmark_endpoints.py)
snapshot formats — auto-detected from JSON keys.

Usage (from project root):

    # Endpoint comparison:
    uv run python scripts/perf/compare_benchmarks.py \\
        scripts/perf/baseline_endpoints.json \\
        scripts/perf/post_endpoints.json

    # Ingest comparison:
    uv run python scripts/perf/compare_benchmarks.py \\
        scripts/perf/baseline_ingest.json \\
        scripts/perf/post_ingest.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _load(path: str) -> dict:
    p = Path(path)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    if not p.exists():
        print(f"ERROR: file not found: {p}")
        sys.exit(1)
    return json.loads(p.read_text())


def _pct(before: float, after: float) -> str:
    if before == 0:
        return "  n/a"
    delta = (after - before) / before * 100
    sign = "+" if delta > 0 else ""
    marker = " ✓" if delta < -5 else (" ✗" if delta > 5 else "  ~")
    return f"{sign}{delta:+.1f}%{marker}"


# ---------------------------------------------------------------------------
# Endpoint comparison
# ---------------------------------------------------------------------------


def _compare_endpoints(before: dict, after: dict) -> None:
    print(
        f"\n{'Endpoint':<38} {'p50 before':>10} {'p50 after':>10} {'Δ p50':>11}"
        f"  {'p95 before':>10} {'p95 after':>10} {'Δ p95':>11}"
    )
    print("-" * 108)

    before_map = {r["label"]: r for r in before.get("results", [])}
    after_map = {r["label"]: r for r in after.get("results", [])}

    labels = list(before_map)
    for lbl in labels:
        if lbl not in after_map:
            print(f"  {lbl}: missing from 'after' snapshot — skipped")
            continue
        b = before_map[lbl]
        a = after_map[lbl]

        def _ms(v):
            return f"{v * 1000:.1f} ms" if v is not None else "N/A"

        p50_b, p50_a = b.get("p50_s"), a.get("p50_s")
        p95_b, p95_a = b.get("p95_s"), a.get("p95_s")

        print(
            f"{lbl:<38} {_ms(p50_b):>10} {_ms(p50_a):>10} {_pct(p50_b or 0, p50_a or 0):>11}"
            f"  {_ms(p95_b):>10} {_ms(p95_a):>10} {_pct(p95_b or 0, p95_a or 0):>11}"
        )

    print()
    b_total = sum(r["p50_s"] for r in before_map.values() if r.get("p50_s"))
    a_total = sum(
        r["p50_s"] for r in after_map.values() if r.get("p50_s") and r["label"] in before_map
    )
    print(f"  Sum p50 before : {b_total * 1000:.1f} ms")
    print(f"  Sum p50 after  : {a_total * 1000:.1f} ms  ({_pct(b_total, a_total).strip()})")
    print()


# ---------------------------------------------------------------------------
# Ingest comparison
# ---------------------------------------------------------------------------


def _compare_ingest(before: dict, after: dict) -> None:
    print(
        f"\n{'File':<45} {'t_total bef':>11} {'t_total aft':>11} {'Δ total':>9}"
        f"  {'t_ingest bef':>12} {'t_ingest aft':>12} {'Δ ingest':>10}"
    )
    print("-" * 120)

    before_map = {r["file"]: r for r in before.get("files", [])}
    after_map = {r["file"]: r for r in after.get("files", [])}

    files = list(before_map)
    for fname in files:
        if fname not in after_map:
            print(f"  {fname}: missing from 'after' snapshot — skipped")
            continue
        b = before_map[fname]
        a = after_map[fname]

        def _s(v):
            return f"{v:.3f}s"

        print(
            f"{fname:<45} {_s(b['t_total']):>11} {_s(a['t_total']):>11} "
            f"{_pct(b['t_total'], a['t_total']):>9}  "
            f"{_s(b['t_ingest']):>12} {_s(a['t_ingest']):>12} "
            f"{_pct(b['t_ingest'], a['t_ingest']):>10}"
        )

    print("-" * 120)
    b_grand = before.get("grand_total_s", 0)
    a_grand = after.get("grand_total_s", 0)
    print(f"{'TOTAL':<45} {b_grand:.3f}s      {a_grand:.3f}s      {_pct(b_grand, a_grand).strip()}")
    print()

    # WAL
    b_wal = before.get("wal_size_before_mb", 0)
    a_wal = after.get("wal_size_before_mb", 0)
    print(f"  WAL before ingest  : {b_wal} MB  →  {a_wal} MB  ({_pct(b_wal, a_wal).strip()})")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)

    before = _load(sys.argv[1])
    after = _load(sys.argv[2])

    # Auto-detect format
    if "results" in before and before["results"] and "p50_s" in before["results"][0]:
        print("\n=== Endpoint latency comparison ===")
        print(f"  Before : {sys.argv[1]}  ({before.get('n_calls', '?')} calls/endpoint)")
        print(f"  After  : {sys.argv[2]}  ({after.get('n_calls', '?')} calls/endpoint)")
        _compare_endpoints(before, after)
    elif "files" in before:
        print("\n=== Ingest pipeline comparison ===")
        print(f"  Before : {sys.argv[1]}")
        print(f"  After  : {sys.argv[2]}")
        _compare_ingest(before, after)
    else:
        print("ERROR: Unrecognised snapshot format — expected ingest or endpoint JSON.")
        sys.exit(1)


if __name__ == "__main__":
    main()
