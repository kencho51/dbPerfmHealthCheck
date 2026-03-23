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

function fmt(n: number) {
  return n.toLocaleString();
}

function fmtPct(pct: number, curated: number): string {
  if (pct === 0 && curated > 0) return "< 0.01%";
  if (pct >= 1) return `${pct.toFixed(1)}%`;
  if (pct >= 0.1) return `${pct.toFixed(2)}%`;
  return `${pct.toFixed(3)}%`;
}

// ---------------------------------------------------------------------------
// Section-level skeleton helpers
// ---------------------------------------------------------------------------

function KpiSkeleton() {
  return (
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
  );
}

function ChartSkeleton({ height = 200 }: { height?: number }) {
  return <div className="bg-slate-100 rounded animate-pulse" style={{ height }} />;
}

function TableSkeleton({ rows = 5 }: { rows?: number }) {
  return (
    <div className="space-y-2 pt-2">
      {[...Array(rows)].map((_, i) => (
        <div key={i} className="h-5 bg-slate-100 rounded animate-pulse" />
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_COVERAGE: CurationCoverage = {
  total_rows: 0, curated_rows: 0, uncurated_rows: 0, coverage_pct: 0,
};

const SYSTEMS = [
  "AP", "BCS-AA", "BCS-BA", "CMGC", "FO",
  "IDA", "PMU.COL", "PMU.ODPS", "PTRM", "TRD.QFM", "WCR",
];

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function DashboardPage() {
  // Filter state
  const [filterHost,        setFilterHost]        = useState("");
  const [filterDb,          setFilterDb]          = useState("");
  const [filterEnvironment, setFilterEnvironment] = useState("");
  const [filterMonth,       setFilterMonth]       = useState("");
  const [filterSystem,      setFilterSystem]      = useState("");

  // Incrementing this forces a full data re-fetch even when filters haven't changed
  const [refreshKey, setRefreshKey] = useState(0);

  // ?�?� Dropdown option lists (unfiltered, loaded once) ?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�
  const [hostOptions,  setHostOptions]  = useState<string[]>([]);
  const [dbOptions,    setDbOptions]    = useState<string[]>([]);
  const [monthOptions, setMonthOptions] = useState<string[]>([]);

  // ?�?� Section data + per-section loading flags ?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�
  const [kpi, setKpi] = useState<{
    totalQueries: number;
    coverage: CurationCoverage;
  } | null>(null);
  const [kpiLoading, setKpiLoading] = useState(true);

  const [summary, setSummary] = useState<SummaryRow[] | null>(null);
  const [summaryLoading, setSummaryLoading] = useState(true);

  const [trends, setTrends] = useState<{ months: MonthRow[]; hosts: HostRow[] } | null>(null);
  const [trendsLoading, setTrendsLoading] = useState(true);

  const [dbs, setDbs] = useState<DbRow[] | null>(null);
  const [dbsLoading, setDbsLoading] = useState(true);

  // ?�?� Active filters helper ?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�
  function activeFilters(): AnalyticsFilters {
    const f: AnalyticsFilters = {};
    if (filterHost)        f.host        = filterHost;
    if (filterDb)          f.db_name     = filterDb;
    if (filterEnvironment) f.environment = filterEnvironment;
    if (filterMonth)       f.month_year  = filterMonth;
    if (filterSystem)      f.system      = filterSystem;
    return f;
  }

  // ?�?� Load dropdown options once (unaffected by filters) ?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�?�
  useEffect(() => {
    Promise.all([api.queries.distinct(), api.analytics.byMonth()])
      .then(([dist, months]) => {
        setHostOptions(dist.hosts);
        setDbOptions(dist.db_names);
        setMonthOptions(
          months.map((m) => m.month_year).filter(Boolean).sort().reverse()
        );
      })
      .catch(console.error);
  }, []);

  // ?�?� Independent per-section fetches (re-run on filter change) ?�?�?�?�?�?�?�?�?�?�?�?�
  //
  // Four parallel groups ordered by expected speed (fastest first so KPIs
  // appear immediately). Each group manages its own loading flag ??a slow
  // group never blocks a fast one from rendering.
  useEffect(() => {
    const filters = activeFilters();

    const countParams: Record<string, string> = {};
    if (filters.host)        countParams.host        = filters.host;
    if (filters.db_name)     countParams.db_name     = filters.db_name;
    if (filters.environment) countParams.environment = filters.environment;
    if (filters.month_year)  countParams.month_year  = filters.month_year;
    if (filters.system)      countParams.system      = filters.system;
    const hasCountFilter = Object.keys(countParams).length > 0;

    // Group 1 ??KPI cards (count + coverage)
    setKpiLoading(true);
    Promise.all([
      api.queries.count(hasCountFilter ? countParams : undefined),
      api.analytics.curationCoverage(filters).catch(() => DEFAULT_COVERAGE),
    ])
      .then(([queryCount, coverage]) => {
        setKpi({ totalQueries: queryCount.count, coverage });
      })
      .catch(console.error)
      .finally(() => setKpiLoading(false));

    // Group 2 ??Summary bar + env pie
    setSummaryLoading(true);
    api.analytics
      .summary(filters)
      .then(setSummary)
      .catch(() => setSummary([]))
      .finally(() => setSummaryLoading(false));

    // Group 3 ??Monthly trend + host bar (shared fetch)
    setTrendsLoading(true);
    Promise.all([
      api.analytics.byMonth(filters).catch(() => [] as MonthRow[]),
      api.analytics.byHost(100, filters).catch(() => [] as HostRow[]),
    ])
      .then(([months, hosts]) => setTrends({ months, hosts }))
      .catch(console.error)
      .finally(() => setTrendsLoading(false));

    // Group 4 ??Top databases table
    setDbsLoading(true);
    api.analytics
      .byDb(10, filters)
      .then(setDbs)
      .catch(() => setDbs([]))
      .finally(() => setDbsLoading(false));

  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterHost, filterDb, filterEnvironment, filterMonth, filterSystem, refreshKey]);

  const filters = activeFilters();
  const hasActiveFilter = !!(filterHost || filterDb || filterEnvironment || filterMonth || filterSystem);

  // Derived KPI values available once trend data loads
  const distinctHosts = trends ? new Set(trends.hosts.map((h) => h.host)).size : null;
  const monthsCount   = trends ? trends.months.length : null;

  // Render page shell is always visible; sections fill in independently ?
  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Dashboard</h1>
          <p className="text-sm text-slate-500 mt-1">Database performance overview</p>
        </div>
        <button
          onClick={() => {
            setFilterHost(""); setFilterDb(""); setFilterEnvironment("");
            setFilterMonth(""); setFilterSystem("");
            setRefreshKey((k) => k + 1); // always triggers a real API re-fetch
          }}
          className="text-xs text-slate-400 hover:text-slate-700 border border-slate-200 rounded px-2 py-1 transition-colors"
          title="Reload all dashboard data"
        >
          Refresh
        </button>
      </div>

      {/* Filter bar ??rendered immediately, no loading gate */}
      <div className="flex flex-wrap items-center gap-3 bg-slate-50 border border-slate-200 rounded-lg px-4 py-3">
        <span className="text-xs font-medium text-slate-500 uppercase tracking-wider">Filter</span>

        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-500">System</label>
          <select
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 bg-white focus:outline-none focus:border-indigo-400"
            value={filterSystem}
            onChange={(e) => { setFilterSystem(e.target.value); setFilterHost(""); }}
          >
            <option value="">All systems</option>
            {SYSTEMS.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>

        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-500">Host</label>
          <select
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 bg-white focus:outline-none focus:border-indigo-400 min-w-[160px]"
            value={filterHost}
            onChange={(e) => { setFilterHost(e.target.value); setFilterDb(""); setFilterSystem(""); }}
          >
            <option value="">All hosts</option>
            {hostOptions.map((h) => <option key={h} value={h}>{h}</option>)}
          </select>
        </div>

        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-500">Database</label>
          <select
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 bg-white focus:outline-none focus:border-indigo-400 min-w-[180px]"
            value={filterDb}
            onChange={(e) => { setFilterDb(e.target.value); setFilterHost(""); }}
          >
            <option value="">All databases</option>
            {dbOptions.map((d) => <option key={d} value={d}>{d}</option>)}
          </select>
        </div>

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

        <div className="flex items-center gap-1.5">
          <label className="text-xs text-slate-500">Month</label>
          <select
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 bg-white focus:outline-none focus:border-indigo-400"
            value={filterMonth}
            onChange={(e) => setFilterMonth(e.target.value)}
          >
            <option value="">All months</option>
            {monthOptions.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
        </div>

        {hasActiveFilter && (
          <div className="flex flex-wrap items-center gap-2 ml-auto">
            {filterHost && (
              <span className="inline-flex items-center gap-1 text-xs bg-indigo-100 text-indigo-700 border border-indigo-200 rounded px-2 py-0.5">
                Host: {filterHost}
                <button onClick={() => setFilterHost("")} className="ml-0.5 hover:text-red-500 transition-colors leading-none">x</button>
              </span>
            )}
            {filterDb && (
              <span className="inline-flex items-center gap-1 text-xs bg-indigo-100 text-indigo-700 border border-indigo-200 rounded px-2 py-0.5">
                DB: {filterDb}
                <button onClick={() => setFilterDb("")} className="ml-0.5 hover:text-red-500 transition-colors leading-none">x</button>
              </span>
            )}
            {filterEnvironment && (
              <span className="inline-flex items-center gap-1 text-xs bg-violet-100 text-violet-700 border border-violet-200 rounded px-2 py-0.5">
                Env: {filterEnvironment}
                <button onClick={() => setFilterEnvironment("")} className="ml-0.5 hover:text-red-500 transition-colors leading-none">x</button>
              </span>
            )}
            {filterMonth && (
              <span className="inline-flex items-center gap-1 text-xs bg-sky-100 text-sky-700 border border-sky-200 rounded px-2 py-0.5">
                Month: {filterMonth}
                <button onClick={() => setFilterMonth("")} className="ml-0.5 hover:text-red-500 transition-colors leading-none">x</button>
              </span>
            )}
            {filterSystem && (
              <span className="inline-flex items-center gap-1 text-xs bg-teal-100 text-teal-700 border border-teal-200 rounded px-2 py-0.5">
                System: {filterSystem}
                <button onClick={() => setFilterSystem("")} className="ml-0.5 hover:text-red-500 transition-colors leading-none">x</button>
              </span>
            )}
            <button
              className="text-xs text-slate-400 hover:text-red-500 transition-colors"
              onClick={() => { setFilterHost(""); setFilterDb(""); setFilterEnvironment(""); setFilterMonth(""); setFilterSystem(""); }}
            >
              Clear all
            </button>
          </div>
        )}
      </div>

      {/* KPI row ??independent loading */}
      {kpiLoading ? (
        <KpiSkeleton />
      ) : (
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <Card>
            <CardHeader>
              <CardTitle>Total Queries</CardTitle>
              <CardValue>{fmt(kpi?.totalQueries ?? 0)}</CardValue>
              <p className="text-xs text-slate-400 mt-1">Distinct query rows ingested (after deduplication)</p>
            </CardHeader>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Distinct Hosts</CardTitle>
              <CardValue>{trendsLoading ? "-" : (distinctHosts ?? 0)}</CardValue>
              <p className="text-xs text-slate-400 mt-1">Unique DB server hosts with recorded activity</p>
            </CardHeader>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Months Covered</CardTitle>
              <CardValue>{trendsLoading ? "-" : (monthsCount ?? 0)}</CardValue>
              <p className="text-xs text-slate-400 mt-1">Number of calendar months with ingested data</p>
            </CardHeader>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Queries Curated</CardTitle>
              <CardValue>{fmt(kpi?.coverage.curated_rows ?? 0)}</CardValue>
              <p className="text-xs text-slate-400 mt-1">Queries manually reviewed and labelled so far</p>
            </CardHeader>
          </Card>
        </div>
      )}

      {/* Chart row 1 ??independent loading */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Queries by Type</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">Total occurrences per query type</p>
          </CardHeader>
          <CardContent>
            {summaryLoading ? <ChartSkeleton /> : <SummaryBarChart data={summary ?? []} />}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>By Environment</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">Share of activity from Production vs SAT environments</p>
          </CardHeader>
          <CardContent>
            {summaryLoading ? <ChartSkeleton /> : <EnvPieChart data={summary ?? []} />}
          </CardContent>
        </Card>
      </div>

      {/* Chart row 2 ??independent loading */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        {trendsLoading ? (
          <Card>
            <CardHeader><CardTitle>Monthly Trend</CardTitle></CardHeader>
            <CardContent><ChartSkeleton height={240} /></CardContent>
          </Card>
        ) : (
          <MonthlyTrendCard initialData={trends?.months ?? []} filters={filters} />
        )}
        <Card>
          <CardHeader>
            <CardTitle>Top Hosts (by occurrences)</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">Hosts ranked by total occurrence count</p>
          </CardHeader>
          <CardContent>
            {trendsLoading ? <ChartSkeleton /> : <HostBarChart data={trends?.hosts ?? []} />}
          </CardContent>
        </Card>
      </div>

      {/* Heatmap ??self-contained, fetches its own data */}
      <HourHeatmap filters={filters} />

      {/* Fingerprints ??self-contained, fetches its own data */}
      <TopFingerprintsTable filters={filters} />

      {/* Bottom row ??independent loading */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Top Databases</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">Databases with the most query activity</p>
          </CardHeader>
          <CardContent>
            {dbsLoading ? (
              <TableSkeleton />
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-100 text-left text-xs text-slate-400">
                    <th className="pb-2">Database</th>
                    <th className="pb-2 text-right">Rows</th>
                    <th className="pb-2 text-right">Occurrences</th>
                  </tr>
                </thead>
                <tbody>
                  {(dbs ?? []).map((d) => (
                    <tr key={d.db_name} className="border-b border-slate-50 hover:bg-slate-50">
                      <td className="py-1.5 font-mono text-xs">{d.db_name ?? ""}</td>
                      <td className="py-1.5 text-right">{fmt(d.row_count)}</td>
                      <td className="py-1.5 text-right">{fmt(d.total_occurrences)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Curation Coverage</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">How many ingested queries have been reviewed and labelled</p>
          </CardHeader>
          <CardContent>
            {kpiLoading ? (
              <ChartSkeleton height={160} />
            ) : (
              <>
                <CoverageDonut data={kpi?.coverage ?? DEFAULT_COVERAGE} />
                <p className="text-center text-sm font-medium text-slate-700 mt-2">
                  {fmtPct(kpi?.coverage.coverage_pct ?? 0, kpi?.coverage.curated_rows ?? 0)} of queries curated
                </p>
                <p className="text-center text-xs text-slate-400 mt-0.5">
                  {fmt(kpi?.coverage.curated_rows ?? 0)} curated / {fmt(kpi?.coverage.total_rows ?? 0)} total
                </p>
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

