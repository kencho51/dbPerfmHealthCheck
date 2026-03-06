"use client";

import { useEffect, useState } from "react";
import { api, SummaryRow, HostRow, MonthRow, DbRow, PatternCoverage } from "@/lib/api";
import { Card, CardHeader, CardTitle, CardValue, CardContent } from "@/components/ui/card";
import {
  SummaryBarChart,
  EnvPieChart,
  CoverageDonut,
  HostBarChart,
} from "@/components/charts";
import { MonthlyTrendCard } from "@/components/MonthlyTrendCard";

function fmt(n: number) {
  return n.toLocaleString();
}

interface DashboardData {
  totalQueries: number;
  distinctHosts: number;
  monthsCount: number;
  patternCount: number;
  summary: SummaryRow[];
  hosts: HostRow[];
  months: MonthRow[];
  dbs: DbRow[];
  coverage: PatternCoverage;
}

const DEFAULT_COVERAGE: PatternCoverage = { total: 0, tagged: 0, untagged: 0, coverage_pct: 0 };

export default function DashboardPage() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  async function fetchAll() {
    setLoading(true);
    setError(null);
    try {
      const [
        queryCount,
        distinctValues,
        summary,
        hosts,
        months,
        dbs,
        coverage,
        patCount,
      ] = await Promise.all([
        api.queries.count(),
        api.queries.distinct(),
        api.analytics.summary().catch(() => [] as SummaryRow[]),
        api.analytics.byHost(100).catch(() => [] as HostRow[]),
        api.analytics.byMonth().catch(() => [] as MonthRow[]),
        api.analytics.byDb(10).catch(() => [] as DbRow[]),
        api.analytics.patternCoverage().catch(() => DEFAULT_COVERAGE),
        api.queries.count({ has_pattern: "true" }).catch(() => ({ count: 0 })),
      ]);

      setData({
        totalQueries: queryCount.count,
        distinctHosts: distinctValues.hosts.length,
        monthsCount: months.length,
        patternCount: patCount.count,
        summary,
        hosts,
        months,
        dbs,
        coverage,
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load dashboard data");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    fetchAll();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  if (loading) {
    return (
      <div className="space-y-8">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Dashboard</h1>
          <p className="text-sm text-slate-500 mt-1">Loading</p>
        </div>
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          {[...Array(4)].map((_, i) => (
            <Card key={i}>
              <CardHeader>
                <div className="h-4 w-24 bg-slate-100 rounded animate-pulse" />
                <div className="h-8 w-16 bg-slate-100 rounded animate-pulse mt-2" />
              </CardHeader>
            </Card>
          ))}
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="space-y-4">
        <h1 className="text-2xl font-bold text-slate-900">Dashboard</h1>
        <p className="text-sm text-red-500">{error ?? "No data"}</p>
        <button
          onClick={fetchAll}
          className="text-sm text-indigo-600 underline"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Dashboard</h1>
          <p className="text-sm text-slate-500 mt-1">Database performance overview</p>
        </div>
        <button
          onClick={fetchAll}
          className="text-xs text-slate-400 hover:text-slate-700 border border-slate-200 rounded px-2 py-1 transition-colors"
        >
           Refresh
        </button>
      </div>

      {/* KPI row */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <Card>
          <CardHeader>
            <CardTitle>Total Queries</CardTitle>
            <CardValue>{fmt(data.totalQueries)}</CardValue>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Distinct Hosts</CardTitle>
            <CardValue>{data.distinctHosts}</CardValue>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Months Covered</CardTitle>
            <CardValue>{data.monthsCount}</CardValue>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Patterns Curated</CardTitle>
            <CardValue>{data.patternCount}</CardValue>
          </CardHeader>
        </Card>
      </div>

      {/* Chart row 1 */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Queries by Type</CardTitle>
          </CardHeader>
          <CardContent>
            <SummaryBarChart data={data.summary} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>By Environment</CardTitle>
          </CardHeader>
          <CardContent>
            <EnvPieChart data={data.summary} />
          </CardContent>
        </Card>
      </div>

      {/* Chart row 2 */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <MonthlyTrendCard initialData={data.months} />
        <Card>
          <CardHeader>
            <CardTitle>Top Hosts (by occurrences)</CardTitle>
          </CardHeader>
          <CardContent>
            <HostBarChart data={data.hosts} />
          </CardContent>
        </Card>
      </div>

      {/* Bottom row */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        {/* Top DBs table */}
        <Card>
          <CardHeader>
            <CardTitle>Top Databases</CardTitle>
          </CardHeader>
          <CardContent>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100 text-left text-xs text-slate-400">
                  <th className="pb-2">Database</th>
                  <th className="pb-2 text-right">Rows</th>
                  <th className="pb-2 text-right">Occurrences</th>
                </tr>
              </thead>
              <tbody>
                {data.dbs.map((d) => (
                  <tr key={d.db_name} className="border-b border-slate-50 hover:bg-slate-50">
                    <td className="py-1.5 font-mono text-xs">{d.db_name ?? ""}</td>
                    <td className="py-1.5 text-right">{fmt(d.row_count)}</td>
                    <td className="py-1.5 text-right">{fmt(d.total_occurrences)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>

        {/* Pattern coverage */}
        <Card>
          <CardHeader>
            <CardTitle>Pattern Coverage</CardTitle>
          </CardHeader>
          <CardContent>
            <CoverageDonut data={data.coverage} />
            <p className="text-center text-sm text-slate-500 mt-2">
              {data.coverage.coverage_pct.toFixed(1)}% of queries tagged with a pattern
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
