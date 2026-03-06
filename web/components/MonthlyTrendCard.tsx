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

  useEffect(() => {
    setLoading(true);
    const merged: AnalyticsFilters = {
      ...externalFilters,
      ...(type ? { type: type as QueryType } : {}),
    };
    api.analytics
      .byMonth(Object.keys(merged).length ? merged : undefined)
      .then(setData)
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  // Stringify externalFilters so the effect fires when its contents change
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [type, JSON.stringify(externalFilters)]);

  // Sync initialData on first load when no type filter is active
  useEffect(() => {
    if (!type) setData(initialData);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialData]);

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
          <MonthLineChart data={data} />
        )}
      </CardContent>
    </Card>
  );
}
