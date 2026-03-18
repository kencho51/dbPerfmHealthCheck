"use client";

import { useEffect, useState } from "react";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { MonthLineChart } from "@/components/charts";
import { api, type MonthRow, type QueryType, type AnalyticsFilters } from "@/lib/api";

const TYPE_OPTIONS: { value: string; label: string }[] = [
  { value: "",                label: "All types" },
  { value: "slow_query",      label: "Slow query (SQL)" },
  { value: "slow_query_mongo",label: "Slow query (Mongo)" },
  { value: "blocker",         label: "Blocker" },
  { value: "deadlock",        label: "Deadlock" },
];

export function MonthlyTrendCard({
  initialData,
  filters: externalFilters = {},
}: {
  initialData: MonthRow[];
  filters?: AnalyticsFilters;
}) {
  const [type, setType] = useState("");
  const [data, setData] = useState<MonthRow[]>(initialData);
  const [loading, setLoading] = useState(false);
  const [totalCount, setTotalCount] = useState<number | null>(null);

  useEffect(() => {
    setLoading(true);
    const merged: AnalyticsFilters = {
      ...externalFilters,
      ...(type ? { type: type as QueryType } : {}),
    };

    const hasFilters = Object.keys(merged).length > 0;

    // Fetch monthly data and total count in parallel so we can detect
    // rows excluded due to NULL month_year
    const countParams: Record<string, string> = {};
    if (merged.host)        countParams.host        = merged.host;
    if (merged.db_name)     countParams.db_name     = merged.db_name;
    if (merged.environment) countParams.environment = merged.environment;
    if (merged.source)      countParams.source      = merged.source;
    if (merged.type)        countParams.type        = merged.type;
    if (merged.month_year)  countParams.month_year  = merged.month_year;
    if (merged.system)      countParams.system      = merged.system;

    Promise.all([
      api.analytics.byMonth(hasFilters ? merged : undefined),
      hasFilters ? api.queries.count(countParams) : Promise.resolve(null),
    ])
      .then(([monthlyData, countResult]) => {
        setData(monthlyData);
        setTotalCount(countResult ? countResult.count : null);
      })
      .catch(() => { setData([]); setTotalCount(null); })
      .finally(() => setLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [type, JSON.stringify(externalFilters)]);

  // Sync initialData when no type filter is active
  useEffect(() => {
    if (!type) setData(initialData);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialData]);

  // How many queries are shown on the chart vs total
  const chartTotal = data.reduce((sum, r) => sum + r.row_count, 0);
  const excluded   = totalCount !== null ? totalCount - chartTotal : 0;

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <CardTitle>Monthly Trend</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">Query row count per month · arrows show month-over-month change (▲ up / ▼ down)</p>
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
        {loading ? (
          <div className="flex items-center justify-center h-[220px] text-xs text-slate-400">
            Loading…
          </div>
        ) : (
          <>
            <MonthLineChart data={data} />

            {/* MoM delta table — only when there are ≥2 months with delta data */}
            {data.filter((r) => r.row_delta !== null).length > 0 && (
              <div className="mt-4">
                <p className="text-[10px] font-medium text-slate-400 uppercase tracking-wider mb-1.5">
                  Month-over-month change (rows)
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
                          title={`${r.month_year}: ${delta > 0 ? "+" : ""}${delta.toLocaleString()} rows vs prior month`}
                        >
                          <span className="text-slate-500 mr-0.5">{r.month_year}</span>
                          {arrow}{Math.abs(delta).toLocaleString()}
                        </span>
                      );
                    })}
                </div>
              </div>
            )}

            {excluded > 0 && (
              <p className="mt-2 text-xs text-amber-600 bg-amber-50 border border-amber-200 rounded px-2 py-1">
                ⚠ {excluded} of {totalCount} quer{totalCount === 1 ? "y" : "ies"} not shown — no month data recorded.
              </p>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}
