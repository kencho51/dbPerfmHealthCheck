"""
Phase 0 baseline benchmark — API endpoint response times.

Sends 10 requests to each key analytics endpoint and records p50/p95 response
times, so post-optimization runs can show concrete latency improvements.

Usage (from project root — API server must be running):

    # Terminal 1: start the server
    uv run fastapi dev api/main.py --port 8000

    # Terminal 2: run the benchmark
    uv run python scripts/perf/benchmark_endpoints.py

    # Target a different host/port:
    uv run python scripts/perf/benchmark_endpoints.py --base-url http://localhost:8001

Options
-------
  --base-url   Base URL of the running server (default: http://127.0.0.1:8000)
  --calls      Number of calls per endpoint (default: 10)

Output
------
  Console table with endpoint, p50, p95, min, max, and status.
  Writes a JSON snapshot to scripts/perf/baseline_endpoints.json for later diff.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# httpx is a dev dependency (pyproject.toml [dependency-groups]dev)
# ---------------------------------------------------------------------------
try:
    import httpx
except ImportError:
    print("ERROR: httpx is not installed. Run:  uv sync  to install dev dependencies.")
    sys.exit(1)

OUTPUT_JSON = PROJECT_ROOT / "scripts" / "perf" / "baseline_endpoints.json"

# ---------------------------------------------------------------------------
# Endpoints to benchmark
# Each entry: (label, path, params_dict)
# ---------------------------------------------------------------------------
ENDPOINTS: list[tuple[str, str, dict]] = [
    ("analytics/summary", "/api/analytics/summary", {}),
    ("analytics/by-host", "/api/analytics/by-host", {"top_n": 20}),
    ("analytics/by-month", "/api/analytics/by-month", {}),
    ("analytics/top-fingerprints", "/api/analytics/top-fingerprints", {"top_n": 20}),
    ("analytics/co-occurrence", "/api/analytics/co-occurrence", {}),
    ("queries/list (no filter)", "/api/queries/", {"page": 1, "page_size": 20}),
    ("queries/list (search)", "/api/queries/", {"page": 1, "page_size": 20, "search": "SELECT"}),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Return the p-th percentile from a pre-sorted list (linear interpolation)."""
    n = len(sorted_values)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_values[0]
    idx = (pct / 100) * (n - 1)
    lo = int(idx)
    hi = lo + 1
    if hi >= n:
        return sorted_values[-1]
    frac = idx - lo
    return sorted_values[lo] + frac * (sorted_values[hi] - sorted_values[lo])


def _fmt_ms(seconds: float) -> str:
    return f"{seconds * 1000:.1f} ms"


def _check_server_alive(base_url: str, timeout: float = 3.0) -> bool:
    try:
        resp = httpx.get(f"{base_url}/health", timeout=timeout)
        return resp.status_code == 200
    except httpx.ConnectError, httpx.TimeoutException:
        return False


# ---------------------------------------------------------------------------
# Core benchmark
# ---------------------------------------------------------------------------


def benchmark_endpoint(
    client: httpx.Client,
    base_url: str,
    label: str,
    path: str,
    params: dict,
    n_calls: int,
) -> dict:
    """Hit one endpoint n_calls times and return timing stats."""
    url = base_url + path
    latencies: list[float] = []
    errors: list[str] = []

    for _ in range(n_calls):
        try:
            t0 = time.perf_counter()
            resp = client.get(url, params=params, timeout=30.0)
            latency = time.perf_counter() - t0
            latencies.append(latency)
            if resp.status_code not in (200, 201):
                errors.append(f"HTTP {resp.status_code}")
        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            errors.append(str(exc))

    sorted_lat = sorted(latencies)

    return {
        "label": label,
        "path": path,
        "params": params,
        "n_calls": n_calls,
        "n_errors": len(errors),
        "p50_s": round(_percentile(sorted_lat, 50), 4),
        "p95_s": round(_percentile(sorted_lat, 95), 4),
        "min_s": round(sorted_lat[0], 4) if sorted_lat else None,
        "max_s": round(sorted_lat[-1], 4) if sorted_lat else None,
        "mean_s": round(statistics.mean(latencies), 4) if latencies else None,
        "errors": errors[:5],  # cap noise
    }


def print_results(results: list[dict]) -> None:
    print(f"\n{'Endpoint':<38} {'p50':>9} {'p95':>9} {'min':>9} {'max':>9}  {'Errors':>6}")
    print("-" * 86)
    for r in results:
        err_str = str(r["n_errors"]) if r["n_errors"] else "-"
        p50 = _fmt_ms(r["p50_s"]) if r["p50_s"] is not None else "N/A"
        p95 = _fmt_ms(r["p95_s"]) if r["p95_s"] is not None else "N/A"
        mn = _fmt_ms(r["min_s"]) if r["min_s"] is not None else "N/A"
        mx = _fmt_ms(r["max_s"]) if r["max_s"] is not None else "N/A"
        flag = " ⚠" if r["n_errors"] > 0 else ""
        print(f"{r['label']:<38} {p50:>9} {p95:>9} {mn:>9} {mx:>9}  {err_str:>6}{flag}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="API endpoint latency baseline")
    parser.add_argument(
        "--base-url", default="http://127.0.0.1:8000", help="Base URL of the running server"
    )
    parser.add_argument(
        "--calls", type=int, default=10, help="Number of calls per endpoint (default: 10)"
    )
    args = parser.parse_args()

    base_url: str = args.base_url.rstrip("/")
    n_calls: int = args.calls

    # -- Server health check --------------------------------------------------
    print(f"\nChecking server at {base_url} …")
    if not _check_server_alive(base_url):
        print(
            f"\nERROR: Cannot reach {base_url}/health\n"
            "\nStart the API server first:\n"
            "  uv run fastapi dev api/main.py --port 8000\n"
            "\nThen re-run this script.\n"
        )
        sys.exit(1)

    print(f"Server is up. Running {n_calls} calls × {len(ENDPOINTS)} endpoints …\n")

    # -- Warm-up call (first call often slower due to DB cache cold start) ----
    with httpx.Client() as client:
        print("Warming up cache (1 call to /api/analytics/summary) …")
        try:
            client.get(f"{base_url}/api/analytics/summary", timeout=30.0)
        except Exception:  # noqa: BLE001
            pass

        # -- Benchmark loop ---------------------------------------------------
        results: list[dict] = []
        for label, path, params in ENDPOINTS:
            print(f"  Benchmarking {label} …", end="", flush=True)
            r = benchmark_endpoint(client, base_url, label, path, params, n_calls)
            results.append(r)
            print(f"  p50={_fmt_ms(r['p50_s'])}  p95={_fmt_ms(r['p95_s'])}")

    # -- Print table ----------------------------------------------------------
    print_results(results)

    # -- Slowest endpoint highlight -------------------------------------------
    valid = [r for r in results if r["p95_s"] is not None]
    if valid:
        slowest = max(valid, key=lambda r: r["p95_s"])
        print(f"  Slowest (p95): {slowest['label']}  →  {_fmt_ms(slowest['p95_s'])}")
        if slowest["p95_s"] > 0.5:
            print(
                "  ⚠  p95 > 500 ms — likely DuckDB-per-request overhead or large WAL; "
                "Phase 1 + Phase 2 should significantly reduce this."
            )

    # -- Save JSON snapshot ---------------------------------------------------
    snapshot = {
        "label": "Phase 0 baseline — endpoint latency",
        "base_url": base_url,
        "n_calls": n_calls,
        "results": results,
    }
    OUTPUT_JSON.write_text(json.dumps(snapshot, indent=2))
    print(f"\n  Baseline saved → {OUTPUT_JSON.relative_to(PROJECT_ROOT)}\n")
    print("Done. Run again after Phase 2–4 optimizations to compare.\n")


if __name__ == "__main__":
    main()
