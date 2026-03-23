"use client";

import React, { useEffect, useState } from "react";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { api, type FingerprintRow, type QueryType, type AnalyticsFilters } from "@/lib/api";

// ── Constants ────────────────────────────────────────────────────────────────

const TYPE_OPTIONS: { value: string; label: string }[] = [
  { value: "",                 label: "All types" },
  { value: "slow_query",       label: "Slow query (SQL)" },
  { value: "slow_query_mongo", label: "Slow query (Mongo)" },
  { value: "blocker",          label: "Blocker" },
  { value: "deadlock",         label: "Deadlock" },
];

const TOP_N_OPTIONS = [10, 20, 50] as const;

const TYPE_COLORS: Record<string, string> = {
  slow_query:       "bg-indigo-100 text-indigo-700",
  slow_query_mongo: "bg-violet-100 text-violet-700",
  blocker:          "bg-amber-100  text-amber-700",
  deadlock:         "bg-red-100    text-red-700",
  unknown:          "bg-slate-100  text-slate-500",
};

const TYPE_LABEL: Record<string, string> = {
  slow_query:       "SQL",
  slow_query_mongo: "Mongo",
  blocker:          "Blocker",
  deadlock:         "Deadlock",
  unknown:          "?",
};

function typeBadge(typeName: string) {
  const cls = TYPE_COLORS[typeName] ?? TYPE_COLORS.unknown;
  const lbl = TYPE_LABEL[typeName] ?? typeName;
  return (
    <span
      key={typeName}
      className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium leading-none ${cls}`}
    >
      {lbl}
    </span>
  );
}

/**
 * sp_executesql stores a parameter-type declaration block before the actual SQL:
 *   "(@p? nvarchar(?),@p? nvarchar(?))select * from ..."
 *
 * This function strips that prefix so we display the meaningful SQL body.
 * It also returns the number of parameters stripped so we can show a hint.
 */
function readableFingerprint(fp: string): { sql: string; paramCount: number } {
  if (!fp.startsWith("(@p?")) return { sql: fp, paramCount: 0 };

  // Count @p? occurrences in the param block
  const paramBlock = fp.match(/^\((@p\?[^)]*(?:\([^)]*\)[^)]*)*)\)/);
  const paramCount = paramBlock ? (paramBlock[0].match(/@p\?/g) ?? []).length : 0;

  // Find where the actual SQL starts — the `)` followed by a SQL keyword
  const bodyMatch = fp.match(/\)\s*(?=select|insert|update|delete|exec|with|begin|declare|create|merge)/);
  if (bodyMatch?.index !== undefined) {
    return { sql: fp.slice(bodyMatch.index + 1).trim(), paramCount };
  }
  return { sql: fp, paramCount };
}

// ── Component ────────────────────────────────────────────────────────────────

export function TopFingerprintsTable({
  filters: externalFilters = {},
}: {
  filters?: AnalyticsFilters;
}) {
  const [selectedType, setSelectedType] = useState("");
  const [topN, setTopN]                 = useState<number>(20);
  const [data, setData]                 = useState<FingerprintRow[]>([]);
  const [loading, setLoading]           = useState(true);
  const [expandedRow, setExpandedRow]   = useState<number | null>(null);

  useEffect(() => {
    setLoading(true);
    const merged: AnalyticsFilters = {
      ...externalFilters,
      ...(selectedType ? { type: selectedType as QueryType } : {}),
    };
    api.analytics
      .topFingerprints(topN, Object.keys(merged).length ? merged : undefined)
      .then(setData)
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedType, topN, JSON.stringify(externalFilters)]);

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <CardTitle>Top Query Fingerprints</CardTitle>
          <p className="text-xs text-slate-400 mt-0.5">
            Ranked by total occurrence count (sum across all matching rows)
          </p>

          <div className="flex items-center gap-2">
            {/* Top-N picker */}
            <select
              className="h-7 rounded border border-slate-200 bg-white px-2 text-xs text-slate-700 focus:outline-none focus:border-indigo-400"
              value={topN}
              onChange={(e) => setTopN(Number(e.target.value))}
            >
              {TOP_N_OPTIONS.map((n) => (
                <option key={n} value={n}>
                  Top {n}
                </option>
              ))}
            </select>

            {/* Type filter */}
            <select
              className="h-7 rounded border border-slate-200 bg-white px-2 text-xs text-slate-700 focus:outline-none focus:border-indigo-400"
              value={selectedType}
              onChange={(e) => setSelectedType(e.target.value)}
            >
              {TYPE_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>
        </div>
      </CardHeader>

      <CardContent>
        {loading ? (
          <div className="space-y-2">
            {[...Array(5)].map((_, i) => (
              <div key={i} className="h-8 rounded bg-slate-100 animate-pulse" />
            ))}
          </div>
        ) : data.length === 0 ? (
          <p className="text-center text-sm text-slate-400 py-6">No data</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100 text-left text-xs text-slate-400">
                  <th className="pb-2 pr-3 w-8">#</th>
                  <th className="pb-2 pr-3">Fingerprint</th>
                  <th className="pb-2 pr-3 text-center">Types</th>
                  <th className="pb-2 pr-3 text-right">Occurrences</th>
                  <th className="pb-2 pr-3 text-right hidden sm:table-cell">Rows</th>
                  <th className="pb-2 pr-3 hidden lg:table-cell">Months</th>
                  <th className="pb-2 pr-3 hidden md:table-cell">Host</th>
                  <th className="pb-2 hidden md:table-cell">Database</th>
                </tr>
              </thead>
              <tbody>
                {data.map((row, idx) => {
                  const isExpanded = expandedRow === idx;
                  return (
                    <React.Fragment key={idx}>
                      <tr
                        key={`${idx}-row`}
                        className="border-b border-slate-50 hover:bg-slate-50 cursor-pointer"
                        onClick={() => setExpandedRow(isExpanded ? null : idx)}
                      >
                        {/* Rank */}
                        <td className="py-2 pr-3 text-xs text-slate-400 align-top">
                          {idx + 1}
                        </td>

                        {/* Fingerprint — truncated mono, click to expand */}
                        <td className="py-2 pr-3 align-top max-w-xs">
                          {(() => {
                            const { sql, paramCount } = readableFingerprint(row.fingerprint);
                            return (
                              <>
                                <span
                                  className={`font-mono text-xs text-slate-700 break-all ${
                                    isExpanded ? "" : "line-clamp-2"
                                  }`}
                                >
                                  {sql || <span className="italic text-slate-400">(empty)</span>}
                                </span>
                                {paramCount > 0 && (
                                  <span className="inline-block mt-0.5 rounded bg-slate-100 px-1 py-0.5 text-[10px] text-slate-400">
                                    sp_executesql · {paramCount} param{paramCount !== 1 ? "s" : ""}
                                  </span>
                                )}
                                {!isExpanded && sql.length >= 120 && (
                                  <span className="text-[10px] text-indigo-400 ml-1">▾ expand</span>
                                )}
                              </>
                            );
                          })()}
                        </td>

                        {/* Type badges */}
                        <td className="py-2 pr-3 align-top">
                          <div className="flex flex-wrap gap-1 justify-center">
                            {Object.entries(row.by_type)
                              .sort(([, a], [, b]) => b - a)
                              .map(([t]) => typeBadge(t))}
                          </div>
                        </td>

                        {/* Occurrences */}
                        <td className="py-2 pr-3 text-right align-top font-medium tabular-nums">
                          {row.count.toLocaleString()}
                        </td>

                        {/* Rows */}
                        <td className="py-2 pr-3 text-right align-top text-slate-400 tabular-nums hidden sm:table-cell">
                          {row.row_count.toLocaleString()}
                        </td>

                        {/* Months */}
                        <td className="py-2 pr-3 align-top hidden lg:table-cell">
                          <div className="flex flex-wrap gap-0.5">
                            {row.months.length === 0 ? (
                              <span className="text-slate-300">—</span>
                            ) : (
                              row.months.map((m) => (
                                <span key={m} className="inline-block rounded bg-slate-100 px-1 py-0.5 text-[10px] font-mono text-slate-600">
                                  {m}
                                </span>
                              ))
                            )}
                          </div>
                        </td>

                        {/* Host */}
                        <td className="py-2 pr-3 align-top text-xs text-slate-500 hidden md:table-cell">
                          {row.example_host || <span className="text-slate-300">—</span>}
                        </td>

                        {/* DB */}
                        <td className="py-2 align-top text-xs text-slate-500 hidden md:table-cell">
                          {row.example_db || <span className="text-slate-300">—</span>}
                        </td>
                      </tr>

                      {/* Expanded detail row */}
                      {isExpanded && (
                        <tr key={`${idx}-detail`} className="bg-slate-50">
                          <td />
                          <td colSpan={7} className="pb-3 pr-3">
                            <div className="space-y-2">
                              {/* Full fingerprint */}
                              <pre className="whitespace-pre-wrap break-all text-xs font-mono text-slate-700 bg-white border border-slate-100 rounded p-2">
                                {(() => {
                                  const { sql, paramCount } = readableFingerprint(row.fingerprint);
                                  return (
                                    <>
                                      {sql}
                                      {paramCount > 0 && (
                                        <details className="mt-2">
                                          <summary className="cursor-pointer text-[10px] text-slate-400 hover:text-slate-600">
                                            sp_executesql parameter declaration ({paramCount} params) ▸
                                          </summary>
                                          <span className="text-slate-400">{row.fingerprint.slice(0, row.fingerprint.indexOf(")") + 1)}</span>
                                        </details>
                                      )}
                                    </>
                                  );
                                })()}
                              </pre>

                              {/* Meta row: months · environments · source */}
                              <div className="flex flex-wrap items-center gap-4 text-xs text-slate-500">
                                {row.months.length > 0 && (
                                  <span>
                                    <span className="font-medium text-slate-600">Months: </span>
                                    {row.months.join(", ")}
                                  </span>
                                )}
                                {row.environments.length > 0 && (
                                  <span>
                                    <span className="font-medium text-slate-600">Env: </span>
                                    {row.environments.join(", ")}
                                  </span>
                                )}
                                {row.example_source && (
                                  <span>
                                    <span className="font-medium text-slate-600">Source: </span>
                                    {row.example_source}
                                  </span>
                                )}
                              </div>

                              {/* Type breakdown */}
                              {Object.keys(row.by_type).length > 0 && (
                                <div className="flex flex-wrap gap-2">
                                  {Object.entries(row.by_type)
                                    .sort(([, a], [, b]) => b - a)
                                    .map(([t, n]) => (
                                      <span key={t} className="text-xs text-slate-500">
                                        {typeBadge(t)}{" "}
                                        <span className="ml-0.5 tabular-nums">
                                          {n.toLocaleString()}
                                        </span>
                                      </span>
                                    ))}
                                </div>
                              )}
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
