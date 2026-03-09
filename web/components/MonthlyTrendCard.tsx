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
    if (merged.host)        countParams.host    = merged.host;
    if (merged.db_name)     countParams.db_name = merged.db_name;
    if (merged.environment) countParams.environment = merged.environment;
    if (merged.source)      countParams.source  = merged.source;
    if (merged.type)        countParams.type    = merged.type;

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
        <div className="flex items-center justify-between gap-3">
          <CardTitle>Monthly Trend</CardTitle>
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
