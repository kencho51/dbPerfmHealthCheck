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
import { HourHeatmap } from "@/components/HourHeatmap";
import { TopFingerprintsTable } from "@/components/TopFingerprintsTable";
import { HostStatsTable } from "@/components/HostStatsTable";
import { CoOccurrenceTable } from "@/components/CoOccurrenceTable";

function fmt(n: number) {
  return n.toLocaleString();
}

/** Format a coverage_pct value that now has 4 decimal places of precision. */
function fmtPct(pct: number, curated: number): string {
  if (pct === 0 && curated > 0) return "< 0.01%";
  if (pct >= 1) return `${pct.toFixed(1)}%`;
  if (pct >= 0.1) return `${pct.toFixed(2)}%`;
  return `${pct.toFixed(3)}%`;
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

// Static infrastructure system list (from ITWS_DB_Hosts.csv)
const SYSTEMS = [
  "AP", "BCS-AA", "BCS-BA", "CMGC", "FO",
  "IDA", "PMU.COL", "PMU.ODPS", "PTRM", "TRD.QFM", "WCR",
];

export default function DashboardPage() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // ── Filter state ──────────────────────────────────────────────────────────
  const [filterHost,        setFilterHost]        = useState("");
  const [filterDb,          setFilterDb]          = useState("");
  const [filterEnvironment, setFilterEnvironment] = useState("");
  const [filterMonth,       setFilterMonth]       = useState("");
  const [filterSystem,      setFilterSystem]      = useState("");

  // ── Dropdown option lists (always unfiltered) ─────────────────────────────
  const [hostOptions,  setHostOptions]  = useState<string[]>([]);
  const [dbOptions,    setDbOptions]    = useState<string[]>([]);
  const [monthOptions, setMonthOptions] = useState<string[]>([]);

  // Fetch dropdown options once at mount (unfiltered)
  useEffect(() => {
    Promise.all([api.queries.distinct(), api.analytics.byMonth()])
      .then(([dist, months]) => {
        setHostOptions(dist.hosts);
        setDbOptions(dist.db_names);
        setMonthOptions(
          months
            .map((m) => m.month_year)
            .filter(Boolean)
            .sort()
            .reverse()
        );
      })
      .catch(console.error);
  }, []);

  // ── Active filters helper ─────────────────────────────────────────────────
  function activeFilters(): AnalyticsFilters {
    const f: AnalyticsFilters = {};
    if (filterHost)        f.host        = filterHost;
    if (filterDb)          f.db_name     = filterDb;
    if (filterEnvironment) f.environment = filterEnvironment;
    if (filterMonth)       f.month_year  = filterMonth;
    if (filterSystem)      f.system      = filterSystem;
    return f;
  }

  async function fetchAll(filters: AnalyticsFilters = {}) {
    setLoading(true);
    setError(null);
    try {
      const countParams: Record<string, string> = {};
      if (filters.host)        countParams.host        = filters.host;
      if (filters.db_name)     countParams.db_name     = filters.db_name;
      if (filters.environment) countParams.environment = filters.environment;
      if (filters.month_year)  countParams.month_year  = filters.month_year;
      if (filters.system)      countParams.system      = filters.system;

      const [
        queryCount,
        summary,
        hosts,
        months,
        dbs,
        coverage,
      ] = await Promise.all([
        api.queries.count(Object.keys(countParams).length ? countParams : undefined),
        api.analytics.summary(filters).catch(() => [] as SummaryRow[]),
        api.analytics.byHost(100, filters).catch(() => [] as HostRow[]),
        api.analytics.byMonth(filters).catch(() => [] as MonthRow[]),
        api.analytics.byDb(10, filters).catch(() => [] as DbRow[]),
        api.analytics.curationCoverage(filters).catch(() => DEFAULT_COVERAGE),
      ]);

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

  // Re-fetch whenever any filter changes (also covers initial mount)
  useEffect(() => {
    fetchAll(activeFilters());
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterHost, filterDb, filterEnvironment, filterMonth, filterSystem]);

  const hasActiveFilter = !!(filterHost || filterDb || filterEnvironment || filterMonth || filterSystem);

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
          onClick={() => fetchAll(activeFilters())}
          className="text-sm text-indigo-600 underline"
        >
          Retry
        </button>
      </div>
    );
  }

  const filters = activeFilters();

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Dashboard</h1>
          <p className="text-sm text-slate-500 mt-1">Database performance overview</p>
        </div>
        <button
          onClick={() => fetchAll(activeFilters())}
          className="text-xs text-slate-400 hover:text-slate-700 border border-slate-200 rounded px-2 py-1 transition-colors"
        >
           Refresh
        </button>
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-3 bg-slate-50 border border-slate-200 rounded-lg px-4 py-3">
        <span className="text-xs font-medium text-slate-500 uppercase tracking-wider">Filter</span>

        {/* System */}
        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-500">System</label>
          <select
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 bg-white focus:outline-none focus:border-indigo-400"
            value={filterSystem}
            onChange={(e) => { setFilterSystem(e.target.value); setFilterHost(""); }}
          >
            <option value="">All systems</option>
            {SYSTEMS.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>

        {/* Host */}
        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-500">Host</label>
          <select
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 bg-white focus:outline-none focus:border-indigo-400 min-w-[160px]"
            value={filterHost}
            onChange={(e) => { setFilterHost(e.target.value); setFilterDb(""); setFilterSystem(""); }}
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

        {/* Environment */}
        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-500">Environment</label>
          <select
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 bg-white focus:outline-none focus:border-indigo-400"
            value={filterEnvironment}
            onChange={(e) => setFilterEnvironment(e.target.value)}
          >
            <option value="">All envs</option>
            <option value="prod">Prod</option>
            <option value="sat">SAT</option>
          </select>
        </div>

        {/* Month */}
        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-500">Month</label>
          <select
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 bg-white focus:outline-none focus:border-indigo-400"
            value={filterMonth}
            onChange={(e) => setFilterMonth(e.target.value)}
          >
            <option value="">All months</option>
            {monthOptions.map((m) => (
              <option key={m} value={m}>{m}</option>
            ))}
          </select>
        </div>

        {/* Active filter badges + clear */}
        {hasActiveFilter && (
          <div className="flex flex-wrap items-center gap-2 ml-auto">
            {filterHost && (
              <span className="inline-flex items-center gap-1 text-xs bg-indigo-100 text-indigo-700 border border-indigo-200 rounded px-2 py-0.5">
                Host: {filterHost}
                <button onClick={() => setFilterHost("")} className="ml-0.5 hover:text-red-500 transition-colors leading-none">✕</button>
              </span>
            )}
            {filterDb && (
              <span className="inline-flex items-center gap-1 text-xs bg-indigo-100 text-indigo-700 border border-indigo-200 rounded px-2 py-0.5">
                DB: {filterDb}
                <button onClick={() => setFilterDb("")} className="ml-0.5 hover:text-red-500 transition-colors leading-none">✕</button>
              </span>
            )}
            {filterEnvironment && (
              <span className="inline-flex items-center gap-1 text-xs bg-violet-100 text-violet-700 border border-violet-200 rounded px-2 py-0.5">
                Env: {filterEnvironment}
                <button onClick={() => setFilterEnvironment("")} className="ml-0.5 hover:text-red-500 transition-colors leading-none">✕</button>
              </span>
            )}
            {filterMonth && (
              <span className="inline-flex items-center gap-1 text-xs bg-sky-100 text-sky-700 border border-sky-200 rounded px-2 py-0.5">
                Month: {filterMonth}
                <button onClick={() => setFilterMonth("")} className="ml-0.5 hover:text-red-500 transition-colors leading-none">✕</button>
              </span>
            )}
            {filterSystem && (
              <span className="inline-flex items-center gap-1 text-xs bg-teal-100 text-teal-700 border border-teal-200 rounded px-2 py-0.5">
                System: {filterSystem}
                <button onClick={() => setFilterSystem("")} className="ml-0.5 hover:text-red-500 transition-colors leading-none">✕</button>
              </span>
            )}
            <button
              className="text-xs text-slate-400 hover:text-red-500 transition-colors"
              onClick={() => { setFilterHost(""); setFilterDb(""); setFilterEnvironment(""); setFilterMonth(""); setFilterSystem(""); }}
            >
              ✕ Clear all
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
            <p className="text-xs text-slate-400 mt-1">Distinct query rows ingested (after deduplication)</p>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Distinct Hosts</CardTitle>
            <CardValue>{data.distinctHosts}</CardValue>
            <p className="text-xs text-slate-400 mt-1">Unique DB server hosts with recorded activity</p>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Months Covered</CardTitle>
            <CardValue>{data.monthsCount}</CardValue>
            <p className="text-xs text-slate-400 mt-1">Number of calendar months with ingested data</p>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Queries Curated</CardTitle>
            <CardValue>{data.curatedCount}</CardValue>
            <p className="text-xs text-slate-400 mt-1">Queries manually reviewed and labelled so far</p>
          </CardHeader>
        </Card>
      </div>

      {/* Chart row 1 */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Queries by Type</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">Total occurrences per query type — slow SQL, slow MongoDB, blockers, deadlocks</p>
          </CardHeader>
          <CardContent>
            <SummaryBarChart data={data.summary} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>By Environment</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">Share of activity from Production vs SAT (pre-production) environments</p>
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
          filters={filters}
        />
        <Card>
          <CardHeader>
            <CardTitle>Top Hosts (by occurrences)</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">Hosts ranked by total occurrence count — highlights the busiest DB servers</p>
          </CardHeader>
          <CardContent>
            <HostBarChart data={data.hosts} />
          </CardContent>
        </Card>
      </div>

      {/* Heatmap row — full width */}
      <HourHeatmap filters={filters} />

      {/* Fingerprints table — full width */}
      <TopFingerprintsTable filters={filters} />

      {/* Phase 8C + 8D: hidden until value is confirmed
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <HostStatsTable filters={filters} />
        <CoOccurrenceTable filters={filters} />
      </div>
      */}

      {/* Bottom row */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        {/* Top DBs table */}
        <Card>
          <CardHeader>
            <CardTitle>Top Databases</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">Databases with the most query activity — rows ingested and total occurrence count</p>
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
            <p className="text-xs text-slate-400 mt-0.5">How many ingested queries have been reviewed and labelled by the team</p>
          </CardHeader>
          <CardContent>
            <CoverageDonut data={data.coverage} />
            <p className="text-center text-sm font-medium text-slate-700 mt-2">
              {fmtPct(data.coverage.coverage_pct, data.coverage.curated_rows)} of queries curated
            </p>
            <p className="text-center text-xs text-slate-400 mt-0.5">
              {fmt(data.coverage.curated_rows)} curated / {fmt(data.coverage.total_rows)} total
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

