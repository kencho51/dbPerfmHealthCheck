"use client";

import { useEffect, useState } from "react";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { MonthLineChart } from "@/components/charts";
import { type MonthRow, type MonthTypeRow } from "@/lib/api";

// ---------------------------------------------------------------------------
// All trend views (All types + per-type) are derived from the same
// MonthTypeRow[] source that powers Monthly Upload Stats, so numbers always
// match.  No extra API call is needed.
// ---------------------------------------------------------------------------

const TYPE_OPTIONS: { value: string; label: string }[] = [
  { value: "",                label: "All types" },
  { value: "slow_query",      label: "Slow query (SQL)" },
  { value: "slow_query_mongo",label: "Slow query (Mongo)" },
  { value: "blocker",         label: "Blocker" },
  { value: "deadlock",        label: "Deadlock" },
];

/** Pick the right upload_log count column for a given type filter. */
function pickCount(r: MonthTypeRow, type: string): number {
  switch (type) {
    case "blocker":          return r.blocker;
    case "deadlock":         return r.deadlock;
    case "slow_query":       return r.slow_query;
    case "slow_query_mongo": return r.slow_query_mongo;
    default:                 return r.total_file_rows ?? 0;
  }
}

/** Convert MonthTypeRow[] → MonthRow[] for the selected type (or "all"). */
function toMonthRows(mtRows: MonthTypeRow[], type: string): MonthRow[] {
  const sorted = [...mtRows].sort((a, b) => a.month_year.localeCompare(b.month_year));
  return sorted.map((r, i) => {
    const count     = pickCount(r, type);
    const prevCount = i > 0 ? pickCount(sorted[i - 1], type) : null;
    return {
      month_year:        r.month_year,
      row_count:         count,
      total_occurrences: count,
      row_delta:         prevCount !== null ? count - prevCount : null,
      occ_delta:         null,
    };
  });
}

export function MonthlyTrendCard({
  initialData,
  monthTypeData = [],
}: {
  initialData: MonthRow[];
  /** Pre-fetched upload_log pivot — same source as Monthly Upload Stats. */
  monthTypeData?: MonthTypeRow[];
}) {
  const [type, setType] = useState("");
  // "All types" uses initialData (filter-aware byMonth → row_count from raw_query),
  // so it matches the Total Queries KPI exactly.
  // Per-type views use monthTypeData (upload_log CSV row counts per type).
  const [data, setData] = useState<MonthRow[]>(() =>
    type === "" ? initialData : toMonthRows(monthTypeData, type)
  );

  useEffect(() => {
    setData(type === "" ? initialData : toMonthRows(monthTypeData, type));
  }, [type, initialData, monthTypeData]);

  const isFileRowMode = !type;

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <CardTitle>Monthly Trend</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">
              {isFileRowMode
                ? "Distinct query patterns per month (matches Total Queries) · ▲/▼ = month-over-month change"
                : "CSV file rows uploaded per month for selected type · ▲/▼ = month-over-month change"}
            </p>
          </div>
          <select
            className="h-7 rounded border border-slate-200 bg-white px-2 text-xs text-slate-700 focus:outline-none focus:border-indigo-400"
            value={type}
            onChange={(e) => setType(e.target.value)}
          >
            {TYPE_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>
      </CardHeader>
      <CardContent>
        <MonthLineChart data={data} />

        {/* MoM delta badges */}
        {data.filter((r) => r.row_delta !== null).length > 0 && (
          <div className="mt-4">
            <p className="text-[10px] font-medium text-slate-400 uppercase tracking-wider mb-1.5">
              Month-over-month change ({isFileRowMode ? "query patterns" : "file rows"})
            </p>
            <div className="flex flex-wrap gap-1.5">
              {data
                .filter((r) => r.row_delta !== null)
                .map((r) => {
                  const delta = r.row_delta as number;
                  const isUp   = delta > 0;
                  const isFlat = delta === 0;
                  const cls = isFlat
                    ? "bg-slate-100 text-slate-500 border-slate-200"
                    : isUp
                    ? "bg-red-50 text-red-700 border-red-200"
                    : "bg-emerald-50 text-emerald-700 border-emerald-200";
                  const arrow = isFlat ? "→" : isUp ? "▲" : "▼";
                  return (
                    <span
                      key={r.month_year}
                      className={`inline-flex items-center gap-0.5 rounded border px-1.5 py-0.5 text-[10px] font-mono ${cls}`}
                      title={`${r.month_year}: ${delta > 0 ? "+" : ""}${delta.toLocaleString()} ${isFileRowMode ? "query patterns" : "file rows"} vs prior month`}
                    >
                      <span className="text-slate-500 mr-0.5">{r.month_year}</span>
                      {arrow}{Math.abs(delta).toLocaleString()}
                    </span>
                  );
                })}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
