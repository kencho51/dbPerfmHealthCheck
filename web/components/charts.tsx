"use client";

import {
  Bar,
  BarChart,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { SummaryRow, HostRow, MonthRow, DbRow, CurationCoverage } from "@/lib/api";

const COLORS = ["#6366f1", "#14b8a6", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899"];
const TYPE_COLORS: Record<string, string> = {
  slow_query: "#6366f1",
  blocker: "#f59e0b",
  deadlock: "#ef4444",
  slow_query_mongo: "#14b8a6",
};
const ENV_COLORS: Record<string, string> = {
  prod: "#6366f1",
  sat: "#14b8a6",
};

// ---- Summary bar chart (by type) -----------------------------------------
export function SummaryBarChart({ data }: { data: SummaryRow[] }) {
  // Aggregate by type across environments
  const byType: Record<string, number> = {};
  for (const row of data) {
    const key = row.type as string;
    byType[key] = (byType[key] ?? 0) + row.row_count;
  }
  const chartData = Object.entries(byType).map(([type, count]) => ({ type, count }));

  return (
    <ResponsiveContainer width="100%" height={220}>
      <BarChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
        <XAxis dataKey="type" tick={{ fontSize: 11 }} />
        <YAxis tick={{ fontSize: 11 }} />
        <Tooltip />
        <Bar dataKey="count" radius={4}>
          {chartData.map((entry) => (
            <Cell key={entry.type} fill={TYPE_COLORS[entry.type] ?? "#94a3b8"} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}

// ---- Environment pie chart -----------------------------------------------
export function EnvPieChart({ data }: { data: SummaryRow[] }) {
  const byEnv: Record<string, number> = {};
  for (const row of data) {
    const key = row.environment as string;
    byEnv[key] = (byEnv[key] ?? 0) + row.row_count;
  }
  const chartData = Object.entries(byEnv).map(([env, value]) => ({ env, value }));

  return (
    <ResponsiveContainer width="100%" height={220}>
      <PieChart>
        <Pie
          data={chartData}
          dataKey="value"
          nameKey="env"
          cx="50%"
          cy="50%"
          outerRadius={80}
          label={({ name, percent }) => `${name} ${((percent ?? 0) * 100).toFixed(0)}%`}
        >
          {chartData.map((entry) => (
            <Cell key={entry.env} fill={ENV_COLORS[entry.env] ?? "#94a3b8"} />
          ))}
        </Pie>
        <Tooltip />
      </PieChart>
    </ResponsiveContainer>
  );
}

// ---- Month trend line chart -----------------------------------------------
export function MonthLineChart({ data }: { data: MonthRow[] }) {
  return (
    <ResponsiveContainer width="100%" height={220}>
      <LineChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
        <XAxis dataKey="month_year" tick={{ fontSize: 11 }} />
        <YAxis tick={{ fontSize: 11 }} />
        <Tooltip />
        <Line
          type="monotone"
          dataKey="row_count"
          name="Rows"
          stroke="#6366f1"
          strokeWidth={2}
          dot={{ r: 4 }}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}

// ---- Curation coverage donut ----------------------------------------------
export function CoverageDonut({ data }: { data: CurationCoverage }) {
  const chartData = [
    { name: "Curated", value: data.curated_rows },
    { name: "Uncurated", value: data.uncurated_rows },
  ];
  return (
    <ResponsiveContainer width="100%" height={180}>
      <PieChart>
        <Pie
          data={chartData}
          dataKey="value"
          nameKey="name"
          cx="50%"
          cy="50%"
          innerRadius={50}
          outerRadius={75}
          label={({ name, percent }) => `${name} ${((percent ?? 0) * 100).toFixed(1)}%`}
        >
          <Cell fill="#6366f1" />
          <Cell fill="#e2e8f0" />
        </Pie>
        <Tooltip />
        <Legend />
      </PieChart>
    </ResponsiveContainer>
  );
}

// ---- Top hosts bar (horizontal) ------------------------------------------
export function HostBarChart({ data }: { data: HostRow[] }) {
  const chartData = data.slice(0, 8).map((r) => ({
    host: r.host?.split("\\").pop() ?? r.host,
    occ: r.total_occurrences,
  }));
  return (
    <ResponsiveContainer width="100%" height={220}>
      <BarChart layout="vertical" data={chartData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
        <XAxis type="number" tick={{ fontSize: 11 }} />
        <YAxis dataKey="host" type="category" tick={{ fontSize: 10 }} width={110} />
        <Tooltip />
        <Bar dataKey="occ" name="Occurrences" fill="#6366f1" radius={3} />
      </BarChart>
    </ResponsiveContainer>
  );
}
