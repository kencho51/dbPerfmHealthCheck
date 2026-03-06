"use client";

import { useEffect, useState } from "react";
import { api, SummaryRow, HostRow, MonthRow, DbRow, CurationCoverage, AnalyticsFilters } from "@/lib/api";
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
  curatedCount: number;
  summary: SummaryRow[];
  hosts: HostRow[];
  months: MonthRow[];
  dbs: DbRow[];
  coverage: CurationCoverage;
}

const DEFAULT_COVERAGE: CurationCoverage = { total_rows: 0, curated_rows: 0, uncurated_rows: 0, coverage_pct: 0 };

export default function DashboardPage() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // ── Filter state ──────────────────────────────────────────────────────────
  const [filterHost, setFilterHost] = useState("");
  const [filterDb,   setFilterDb]   = useState("");
  const [hostOptions, setHostOptions] = useState<string[]>([]);
  const [dbOptions,   setDbOptions]   = useState<string[]>([]);

  async function fetchAll(filters: AnalyticsFilters = {}) {
    setLoading(true);
    setError(null);
    try {
      const countParams: Record<string, string> = {};
      if (filters.host)    countParams.host    = filters.host;
      if (filters.db_name) countParams.db_name = filters.db_name;

      const [
        queryCount,
        distinctValues,
        summary,
        hosts,
        months,
        dbs,
        coverage,
      ] = await Promise.all([
        api.queries.count(Object.keys(countParams).length ? countParams : undefined),
        api.queries.distinct(),
        api.analytics.summary(filters).catch(() => [] as SummaryRow[]),
        api.analytics.byHost(100, filters).catch(() => [] as HostRow[]),
        api.analytics.byMonth(filters).catch(() => [] as MonthRow[]),
        api.analytics.byDb(10, filters).catch(() => [] as DbRow[]),
        api.analytics.curationCoverage(filters).catch(() => DEFAULT_COVERAGE),
      ]);

      // Populate dropdown options from the full distinct list (unfiltered)
      setHostOptions(distinctValues.hosts);
      setDbOptions(distinctValues.db_names);

      const uniqueHosts = new Set(hosts.map((h) => h.host)).size;

      setData({
        totalQueries:  queryCount.count,
        distinctHosts: uniqueHosts,
        monthsCount:   months.length,
        curatedCount:  coverage.curated_rows,
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

  // Fetch whenever filters change (also covers initial mount)
  useEffect(() => {
    const filters: AnalyticsFilters = {};
    if (filterHost) filters.host    = filterHost;
    if (filterDb)   filters.db_name = filterDb;
    fetchAll(filters);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterHost, filterDb]);

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
          onClick={() => fetchAll({
            ...(filterHost ? { host: filterHost } : {}),
            ...(filterDb   ? { db_name: filterDb } : {}),
          })}
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
          onClick={() => fetchAll({
            ...(filterHost ? { host: filterHost } : {}),
            ...(filterDb   ? { db_name: filterDb } : {}),
          })}
          className="text-xs text-slate-400 hover:text-slate-700 border border-slate-200 rounded px-2 py-1 transition-colors"
        >
           Refresh
        </button>
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-3 bg-slate-50 border border-slate-200 rounded-lg px-4 py-3">
        <span className="text-xs font-medium text-slate-500 uppercase tracking-wider">Filter</span>

        {/* Host */}
        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-500">Host</label>
          <select
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 bg-white focus:outline-none focus:border-indigo-400 min-w-[160px]"
            value={filterHost}
            onChange={(e) => { setFilterHost(e.target.value); setFilterDb(""); }}
          >
            <option value="">All hosts</option>
            {hostOptions.map((h) => (
              <option key={h} value={h}>{h}</option>
            ))}
          </select>
        </div>

        {/* Database */}
        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-500">Database</label>
          <select
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 bg-white focus:outline-none focus:border-indigo-400 min-w-[180px]"
            value={filterDb}
            onChange={(e) => { setFilterDb(e.target.value); setFilterHost(""); }}
          >
            <option value="">All databases</option>
            {dbOptions.map((d) => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>
        </div>

        {/* Active filter badge + clear */}
        {(filterHost || filterDb) && (
          <div className="flex items-center gap-2 ml-auto">
            <span className="text-xs bg-indigo-100 text-indigo-700 border border-indigo-200 rounded px-2 py-0.5">
              {filterHost ? `Host: ${filterHost}` : `DB: ${filterDb}`}
            </span>
            <button
              className="text-xs text-slate-400 hover:text-red-500 transition-colors"
              onClick={() => { setFilterHost(""); setFilterDb(""); }}
            >
              ✕ Clear
            </button>
          </div>
        )}
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
            <CardTitle>Queries Curated</CardTitle>
            <CardValue>{data.curatedCount}</CardValue>
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
        <MonthlyTrendCard
          initialData={data.months}
          filters={{
            ...(filterHost ? { host: filterHost } : {}),
            ...(filterDb   ? { db_name: filterDb }   : {}),
          }}
        />
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

        {/* Curation coverage */}
        <Card>
          <CardHeader>
            <CardTitle>Curation Coverage</CardTitle>
          </CardHeader>
          <CardContent>
            <CoverageDonut data={data.coverage} />
            <p className="text-center text-sm text-slate-500 mt-2">
              {data.coverage.coverage_pct.toFixed(1)}% of queries curated
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
