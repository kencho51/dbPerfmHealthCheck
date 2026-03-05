import { api } from "@/lib/api";
import { Card, CardHeader, CardTitle, CardValue, CardContent } from "@/components/ui/card";
import {
  SummaryBarChart,
  EnvPieChart,
  CoverageDonut,
  HostBarChart,
} from "@/components/charts";
import { MonthlyTrendCard } from "@/components/MonthlyTrendCard";

export const dynamic = "force-dynamic";

function fmt(n: number) {
  return n.toLocaleString();
}

export default async function DashboardPage() {
  const [summary, hosts, months, dbs, coverage] = await Promise.all([
    api.analytics.summary().catch(() => []),
    api.analytics.byHost(10).catch(() => []),
    api.analytics.byMonth().catch(() => []),
    api.analytics.byDb(10).catch(() => []),
    api.analytics.patternCoverage().catch(() => ({ total: 0, tagged: 0, untagged: 0, coverage_pct: 0 })),
  ]);

  const totalRows = summary.reduce((s, r) => s + r.row_count, 0);
  const distinctHosts = new Set(hosts.map((h) => h.host)).size;
  const monthsCount = months.length;
  const [patCount] = await Promise.all([api.patterns.count().catch(() => ({ count: 0 }))]);

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Dashboard</h1>
        <p className="text-sm text-slate-500 mt-1">Database performance overview — Jan 2026</p>
      </div>

      {/* KPI row */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <Card>
          <CardHeader>
            <CardTitle>Total Queries</CardTitle>
            <CardValue>{fmt(totalRows)}</CardValue>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Distinct Hosts</CardTitle>
            <CardValue>{distinctHosts}</CardValue>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Months Covered</CardTitle>
            <CardValue>{monthsCount}</CardValue>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Patterns Curated</CardTitle>
            <CardValue>{patCount.count}</CardValue>
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
            <SummaryBarChart data={summary} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>By Environment</CardTitle>
          </CardHeader>
          <CardContent>
            <EnvPieChart data={summary} />
          </CardContent>
        </Card>
      </div>

      {/* Chart row 2 */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <MonthlyTrendCard initialData={months} />
        <Card>
          <CardHeader>
            <CardTitle>Top Hosts (by occurrences)</CardTitle>
          </CardHeader>
          <CardContent>
            <HostBarChart data={hosts} />
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
                {dbs.map((d) => (
                  <tr key={d.db_name} className="border-b border-slate-50 hover:bg-slate-50">
                    <td className="py-1.5 font-mono text-xs">{d.db_name ?? "—"}</td>
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
            <CoverageDonut data={coverage} />
            <p className="text-center text-sm text-slate-500 mt-2">
              {coverage.coverage_pct.toFixed(1)}% of queries tagged with a pattern
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
