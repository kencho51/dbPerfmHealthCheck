"use client";

import { useEffect, useState } from "react";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { api, type CoOccurrenceRow, type AnalyticsFilters } from "@/lib/api";

// ── Helpers ───────────────────────────────────────────────────────────────────

function fmt(n: number) {
  return n.toLocaleString();
}

/**
 * Row severity — a host with BOTH blockers AND deadlocks in the same month
 * is the most critical signal (resource contention → lock cycles).
 */
function rowSeverity(row: CoOccurrenceRow): "both" | "blocker" | "deadlock" {
  if (row.blocker_count > 0 && row.deadlock_count > 0) return "both";
  if (row.blocker_count > 0) return "blocker";
  return "deadlock";
}

const SEVERITY_ROW: Record<string, string> = {
  both:     "bg-red-50    hover:bg-red-100",
  blocker:  "bg-amber-50  hover:bg-amber-100",
  deadlock: "bg-orange-50 hover:bg-orange-100",
};

const SEVERITY_BADGE: Record<string, string> = {
  both: "bg-red-100 text-red-700 border border-red-200",
  blocker: "bg-amber-100 text-amber-700 border border-amber-200",
  deadlock: "bg-orange-100 text-orange-700 border border-orange-200",
};

// ── Component ─────────────────────────────────────────────────────────────────

export function CoOccurrenceTable({
  filters: externalFilters = {},
}: {
  filters?: AnalyticsFilters;
}) {
  const [data, setData]         = useState<CoOccurrenceRow[]>([]);
  const [loading, setLoading]   = useState(true);
  const [showAll, setShowAll]   = useState(false);

  useEffect(() => {
    setLoading(true);
    api.analytics
      .coOccurrence(Object.keys(externalFilters).length ? externalFilters : undefined)
      .then(setData)
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(externalFilters)]);

  // Show only hosts with BOTH types by default; toggle to see all
  const bothCount = data.filter((r) => r.blocker_count > 0 && r.deadlock_count > 0).length;
  const displayed = showAll ? data : data.filter((r) => r.blocker_count > 0 && r.deadlock_count > 0);

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <CardTitle>Blocker + Deadlock Co-occurrence</CardTitle>
            <p className="text-xs text-slate-400 mt-0.5">
              Hosts where blocking and deadlocking events overlap in the same month — critical contention signal
            </p>
          </div>

          {data.length > 0 && (
            <button
              onClick={() => setShowAll((v) => !v)}
              className="text-xs text-indigo-600 hover:text-indigo-800 border border-indigo-200 rounded px-2 py-1 transition-colors"
            >
              {showAll ? `Show critical only (${bothCount})` : `Show all (${data.length})`}
            </button>
          )}
        </div>
      </CardHeader>

      <CardContent>
        {loading ? (
          <div className="space-y-2">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-7 rounded bg-slate-100 animate-pulse" />
            ))}
          </div>
        ) : data.length === 0 ? (
          <div className="text-center py-6">
            <p className="text-sm text-emerald-600 font-medium">✓ No blocker/deadlock activity found</p>
            <p className="text-xs text-slate-400 mt-1">No hosts had blocker or deadlock events in the selected period</p>
          </div>
        ) : displayed.length === 0 ? (
          <div className="text-center py-6">
            <p className="text-sm text-emerald-600 font-medium">✓ No co-occurring events found</p>
            <p className="text-xs text-slate-400 mt-1">
              {data.length} host{data.length !== 1 ? "s" : ""} had single-type events.{" "}
              <button onClick={() => setShowAll(true)} className="underline text-indigo-500">Show all</button>
            </p>
          </div>
        ) : (
          <>
            {/* Legend */}
            <div className="flex items-center gap-3 mb-3 text-[10px]">
              <span className="font-medium text-slate-500 uppercase tracking-wider">Legend:</span>
              <span className={`inline-block rounded px-1.5 py-0.5 ${SEVERITY_BADGE.both}`}>Both — critical</span>
              <span className={`inline-block rounded px-1.5 py-0.5 ${SEVERITY_BADGE.blocker}`}>Blocker only</span>
              <span className={`inline-block rounded px-1.5 py-0.5 ${SEVERITY_BADGE.deadlock}`}>Deadlock only</span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-100 text-left text-xs text-slate-400">
                    <th className="pb-2 pr-3">Host</th>
                    <th className="pb-2 pr-3">Month</th>
                    <th className="pb-2 pr-3 text-right">Blockers</th>
                    <th className="pb-2 pr-3 text-right">Deadlocks</th>
                    <th className="pb-2 text-right">Score</th>
                  </tr>
                </thead>
                <tbody>
                  {displayed.map((row, idx) => {
                    const sev = rowSeverity(row);
                    return (
                      <tr key={`${row.host}-${row.month_year}-${idx}`} className={`border-b border-white/60 ${SEVERITY_ROW[sev]}`}>
                        <td className="py-1.5 pr-3 font-mono text-xs text-slate-700 max-w-[180px] truncate">
                          {row.host}
                        </td>
                        <td className="py-1.5 pr-3 text-xs text-slate-500 font-mono">
                          {row.month_year}
                        </td>
                        <td className="py-1.5 pr-3 text-right tabular-nums">
                          {row.blocker_count > 0 ? (
                            <span className={`inline-block rounded px-1.5 py-0.5 text-xs ${SEVERITY_BADGE.blocker}`}>
                              {fmt(row.blocker_count)}
                            </span>
                          ) : (
                            <span className="text-slate-300">—</span>
                          )}
                        </td>
                        <td className="py-1.5 pr-3 text-right tabular-nums">
                          {row.deadlock_count > 0 ? (
                            <span className={`inline-block rounded px-1.5 py-0.5 text-xs ${SEVERITY_BADGE.deadlock}`}>
                              {fmt(row.deadlock_count)}
                            </span>
                          ) : (
                            <span className="text-slate-300">—</span>
                          )}
                        </td>
                        <td className="py-1.5 text-right tabular-nums font-medium text-slate-700">
                          {fmt(row.combined_score)}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}
