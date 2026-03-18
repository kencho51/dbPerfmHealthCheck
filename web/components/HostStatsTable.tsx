"use client";

import { useEffect, useState } from "react";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { api, type HostStatsRow, type AnalyticsFilters } from "@/lib/api";

// ── Colour helpers ────────────────────────────────────────────────────────────

/**
 * P95 threshold colouring — signals whether a host has outlier queries.
 *   green  : P95 ≤ 5  → most queries run once or twice at most
 *   amber  : P95 ≤ 50 → moderate repeat offenders
 *   red    : P95 > 50 → heavy repeat-query concentration
 */
function p95Color(p95: number): string {
  if (p95 <= 5)  return "text-emerald-600 font-semibold";
  if (p95 <= 50) return "text-amber-600   font-semibold";
  return "text-red-600 font-semibold";
}

function p95Badge(p95: number): string {
  if (p95 <= 5)  return "bg-emerald-50 border-emerald-200 text-emerald-700";
  if (p95 <= 50) return "bg-amber-50   border-amber-200   text-amber-700";
  return "bg-red-50 border-red-200 text-red-700";
}

function fmt(n: number) {
  return n.toLocaleString();
}

// ── Component ─────────────────────────────────────────────────────────────────

export function HostStatsTable({
  filters: externalFilters = {},
}: {
  filters?: AnalyticsFilters;
}) {
  const [data, setData]       = useState<HostStatsRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [topN, setTopN]       = useState<number>(20);

  useEffect(() => {
    setLoading(true);
    api.analytics
      .hostStats(topN, Object.keys(externalFilters).length ? externalFilters : undefined)
      .then(setData)
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [topN, JSON.stringify(externalFilters)]);

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <CardTitle>Host Occurrence Distribution</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">
              P50 / P95 / P99 of <code>occurrence_count</code> per host · sorted by P95 ↓
            </p>
          </div>

          {/* Top-N picker */}
          <select
            className="h-7 rounded border border-slate-200 bg-white px-2 text-xs text-slate-700 focus:outline-none focus:border-indigo-400"
            value={topN}
            onChange={(e) => setTopN(Number(e.target.value))}
          >
            {[20, 30, 50].map((n) => (
              <option key={n} value={n}>Top {n}</option>
            ))}
          </select>
        </div>
      </CardHeader>

      <CardContent>
        {loading ? (
          <div className="space-y-2">
            {[...Array(5)].map((_, i) => (
              <div key={i} className="h-7 rounded bg-slate-100 animate-pulse" />
            ))}
          </div>
        ) : data.length === 0 ? (
          <p className="text-center text-sm text-slate-400 py-6">No data</p>
        ) : (
          <>
            {/* How to read this table */}
            <div className="mb-4 rounded-lg border border-slate-200 bg-slate-50 px-4 py-3 text-xs text-slate-600 space-y-2">
              <p className="font-semibold text-slate-700">How to read this table</p>
              <p>
                Each query row in the database has an <code className="bg-white border border-slate-200 rounded px-1">occurrence_count</code> — how many times that exact query was seen across all ingested CSVs.
                The P50/P95/P99 columns show the <strong>distribution shape</strong> of those counts per host:
              </p>
              <ul className="list-disc list-inside space-y-1 pl-1">
                <li><strong>P50 (median)</strong> — half of the queries on this host repeat ≤ this many times. P50 = 1 means most queries are unique, seen only once.</li>
                <li><strong>P95</strong> — 95% of queries repeat ≤ this many times. The top 5% repeat <em>more</em>. A high P95 reveals a small number of runaway repeated queries hiding behind a healthy average.</li>
                <li><strong>P99</strong> — the worst 1% of queries. A very high P99 (e.g. 5,000) means at least one query is being called thousands of times.</li>
              </ul>
              <div className="grid grid-cols-1 gap-1 sm:grid-cols-3 mt-1">
                <div className="rounded border border-emerald-200 bg-emerald-50 px-2 py-1.5 text-emerald-800">
                  <p className="font-semibold">✓ Healthy pattern</p>
                  <p className="text-[11px] mt-0.5">P50 ≈ 1, P95 ≤ 5<br />Queries are mostly distinct — no repeat offenders.</p>
                </div>
                <div className="rounded border border-amber-200 bg-amber-50 px-2 py-1.5 text-amber-800">
                  <p className="font-semibold">⚠ Moderate concern</p>
                  <p className="text-[11px] mt-0.5">P95 between 5–50<br />Some queries repeat frequently — worth investigating.</p>
                </div>
                <div className="rounded border border-red-200 bg-red-50 px-2 py-1.5 text-red-800">
                  <p className="font-semibold">✗ Heavy-tail problem</p>
                  <p className="text-[11px] mt-0.5">P95 &gt; 50 or P99 very high<br />A few queries dominate this host — likely candidates for optimisation.</p>
                </div>
              </div>
            </div>

            {/* Legend */}
            <div className="flex items-center gap-3 mb-3 text-[10px]">
              <span className="font-medium text-slate-500 uppercase tracking-wider">P95 legend:</span>
              <span className="inline-flex items-center gap-1 rounded border px-1.5 py-0.5 bg-emerald-50 border-emerald-200 text-emerald-700">≤ 5 — low</span>
              <span className="inline-flex items-center gap-1 rounded border px-1.5 py-0.5 bg-amber-50   border-amber-200   text-amber-700">≤ 50 — moderate</span>
              <span className="inline-flex items-center gap-1 rounded border px-1.5 py-0.5 bg-red-50    border-red-200    text-red-700">&gt; 50 — high</span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-100 text-left text-xs text-slate-400">
                    <th className="pb-2 pr-3">#</th>
                    <th className="pb-2 pr-3">Host</th>
                    <th className="pb-2 pr-3 text-right">P50</th>
                    <th className="pb-2 pr-3 text-right">P95</th>
                    <th className="pb-2 pr-3 text-right hidden sm:table-cell">P99</th>
                    <th className="pb-2 pr-3 text-right hidden md:table-cell">Max</th>
                    <th className="pb-2 pr-3 text-right hidden lg:table-cell">Total Occ.</th>
                    <th className="pb-2 text-right hidden lg:table-cell">Rows</th>
                  </tr>
                </thead>
                <tbody>
                  {data.map((row, idx) => (
                    <tr key={row.host} className="border-b border-slate-50 hover:bg-slate-50">
                      <td className="py-1.5 pr-3 text-xs text-slate-400">{idx + 1}</td>
                      <td className="py-1.5 pr-3 font-mono text-xs text-slate-700 max-w-[200px] truncate">
                        {row.host}
                      </td>
                      <td className="py-1.5 pr-3 text-right tabular-nums text-slate-500">
                        {row.p50.toLocaleString()}
                      </td>
                      <td className="py-1.5 pr-3 text-right tabular-nums">
                        <span className={`inline-block rounded border px-1.5 py-0.5 text-xs ${p95Badge(row.p95)}`}>
                          {row.p95.toLocaleString()}
                        </span>
                      </td>
                      <td className={`py-1.5 pr-3 text-right tabular-nums hidden sm:table-cell ${p95Color(row.p99)}`}>
                        {row.p99.toLocaleString()}
                      </td>
                      <td className="py-1.5 pr-3 text-right tabular-nums text-slate-500 hidden md:table-cell">
                        {fmt(row.max_occ)}
                      </td>
                      <td className="py-1.5 pr-3 text-right tabular-nums text-slate-500 hidden lg:table-cell">
                        {fmt(row.total_occurrences)}
                      </td>
                      <td className="py-1.5 text-right tabular-nums text-slate-400 hidden lg:table-cell">
                        {fmt(row.row_count)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}
