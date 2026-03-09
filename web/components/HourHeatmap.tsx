"use client";

/**
 * HourHeatmap — 24 × 7 event-density heatmap (hour-of-day × day-of-week).
 *
 * Data comes from GET /api/analytics/by-hour.  The endpoint performs all
 * datetime parsing server-side with Polars so mixed Splunk time formats
 * (ISO+TZ, US AM/PM, bare ISO) are all handled uniformly.
 *
 * Color scale: slate-100 (zero) → indigo-800 (maximum cell count).
 * Cells are keyed by {hour, weekday}; missing cells in the response = 0.
 * Hover a cell to see a rich floating tooltip with type breakdown, top hosts, and top DBs.
 */

import React, { useEffect, useState, useCallback } from "react";
import { api, type HourCell, type QueryType, type AnalyticsFilters } from "@/lib/api";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";

// ---------------------------------------------------------------------------
// Type filter options — mirrors MonthlyTrendCard
// ---------------------------------------------------------------------------

const TYPE_OPTIONS: { value: string; label: string }[] = [
  { value: "",                 label: "All types"           },
  { value: "slow_query",       label: "Slow query (SQL)"    },
  { value: "slow_query_mongo", label: "Slow query (Mongo)"  },
  { value: "blocker",          label: "Blocker"             },
  { value: "deadlock",         label: "Deadlock"            },
];

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

/** Hex color ramp — index 0 = zero events, index 7 = maximum density */
const COLOR_SCALE = [
  "#f1f5f9", // slate-100  (zero)
  "#e0e7ff", // indigo-100
  "#c7d2fe", // indigo-200
  "#a5b4fc", // indigo-300
  "#818cf8", // indigo-400
  "#6366f1", // indigo-500
  "#4f46e5", // indigo-600
  "#3730a3", // indigo-800  (max)
];

const CELL_H = 20; // px height per row

/** Colour per query type — used for tooltip breakdown bars */
const TYPE_COLORS: Record<string, string> = {
  slow_query:        "#6366f1", // indigo
  slow_query_mongo:  "#8b5cf6", // violet
  blocker:           "#f59e0b", // amber
  deadlock:          "#ef4444", // red
  unknown:           "#94a3b8", // slate
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function colorFor(count: number, max: number): string {
  if (count === 0 || max === 0) return COLOR_SCALE[0];
  const level = Math.min(7, Math.ceil((count / max) * 7));
  return COLOR_SCALE[level];
}

function formatHour(h: number): string {
  if (h === 0) return "12am";
  if (h === 12) return "12pm";
  return h < 12 ? `${h}am` : `${h - 12}pm`;
}

function fmt(n: number) {
  return n.toLocaleString();
}

// ---------------------------------------------------------------------------
// Floating tooltip component
// ---------------------------------------------------------------------------

interface TooltipProps {
  cell: HourCell;
  x: number;
  y: number;
}

function HoverTooltip({ cell, x, y }: TooltipProps) {
  const displayTotal = cell.count;
  const types = Object.entries(cell.by_type).sort(([, a], [, b]) => b - a);

  const style: React.CSSProperties = {
    position: "fixed",
    top: y + 16,
    left: x + 16,
    zIndex: 9999,
    pointerEvents: "none",
    minWidth: 220,
    maxWidth: 300,
  };

  return (
    <div
      style={style}
      className="rounded-lg border border-slate-200 bg-white shadow-xl text-xs text-slate-700 overflow-hidden"
    >
      {/* Header */}
      <div className="bg-indigo-600 text-white px-3 py-2 font-semibold leading-snug">
        {DAYS[cell.weekday]} · {formatHour(cell.hour)}–{formatHour((cell.hour + 1) % 24)}
        <span className="ml-2 font-normal opacity-80">{fmt(displayTotal)} event{displayTotal !== 1 ? "s" : ""}</span>
      </div>

      <div className="px-3 py-2 space-y-3">
        {/* Query type breakdown */}
        {types.length > 0 && (
          <div>
            <p className="font-semibold text-slate-400 uppercase tracking-wider text-[10px] mb-1.5">
              By type
            </p>
            <div className="space-y-1.5">
              {types.map(([type, count]) => {
                const pct = displayTotal > 0 ? (count / displayTotal) * 100 : 0;
                const color = TYPE_COLORS[type] ?? TYPE_COLORS.unknown;
                return (
                  <div key={type}>
                    <div className="flex justify-between mb-0.5 text-slate-600">
                      <span className="capitalize">{type.replace(/_/g, " ")}</span>
                      <span className="font-mono text-slate-500">
                        {fmt(count)}{" "}
                        <span className="text-slate-400">({pct.toFixed(1)}%)</span>
                      </span>
                    </div>
                    <div className="h-1.5 w-full rounded-full bg-slate-100 overflow-hidden">
                      <div
                        className="h-full rounded-full"
                        style={{ width: `${pct}%`, backgroundColor: color }}
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Top hosts */}
        {cell.top_hosts.length > 0 && (
          <div>
            <p className="font-semibold text-slate-400 uppercase tracking-wider text-[10px] mb-1">
              Top hosts
            </p>
            <table className="w-full">
              <tbody>
                {cell.top_hosts.map(({ host, count }) => (
                  <tr key={host} className="border-b border-slate-50 last:border-0">
                    <td className="py-0.5 font-mono truncate max-w-[160px] text-slate-600" title={host}>{host}</td>
                    <td className="py-0.5 text-right font-mono text-slate-400">{fmt(count)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Top databases */}
        {cell.top_dbs.length > 0 && (
          <div>
            <p className="font-semibold text-slate-400 uppercase tracking-wider text-[10px] mb-1">
              Top databases
            </p>
            <table className="w-full">
              <tbody>
                {cell.top_dbs.map(({ db_name, count }) => (
                  <tr key={db_name} className="border-b border-slate-50 last:border-0">
                    <td className="py-0.5 font-mono truncate max-w-[160px] text-slate-600" title={db_name}>{db_name}</td>
                    <td className="py-0.5 text-right font-mono text-slate-400">{fmt(count)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

interface Props {
  filters?: AnalyticsFilters;
}

export function HourHeatmap({ filters }: Props) {
  const [cells, setCells]     = useState<HourCell[]>([]);
  const [loading, setLoading] = useState(true);
  const [hovered, setHovered] = useState<HourCell | null>(null);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });
  // Local type filter — merged with externalFilters before the API call,
  // matching the same pattern used by MonthlyTrendCard.
  const [selectedType, setSelectedType] = useState("");

  // Destructure so useEffect has stable primitive deps
  const { environment, host, db_name, month_year, source } = filters ?? {};

  useEffect(() => {
    setLoading(true);
    setHovered(null);
    const merged: AnalyticsFilters = {
      ...(environment  ? { environment }  : {}),
      ...(host        ? { host }        : {}),
      ...(db_name     ? { db_name }     : {}),
      ...(month_year  ? { month_year }  : {}),
      ...(source      ? { source }      : {}),
      ...(selectedType ? { type: selectedType as QueryType } : {}),
    };
    api.analytics.byHour(Object.keys(merged).length ? merged : undefined)
      .then(setCells)
      .catch(console.error)
      .finally(() => setLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [environment, host, db_name, month_year, source, selectedType]);

  // Build 24×7 lookup map (full HourCell — backend has already applied all filters)
  const cellMap = new Map<string, HourCell>();
  let maxCount = 0;
  for (const c of cells) {
    if (c.hour >= 0 && c.hour < 24 && c.weekday >= 0 && c.weekday < 7) {
      cellMap.set(`${c.hour}-${c.weekday}`, c);
      if (c.count > maxCount) maxCount = c.count;
    }
  }

  const totalEvents = cells.reduce((s, c) => s + c.count, 0);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    setMousePos({ x: e.clientX, y: e.clientY });
  }, []);

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-3">
          <CardTitle>Peak Hour Heatmap</CardTitle>
          <select
            className="h-7 rounded border border-slate-200 bg-white px-2 text-xs text-slate-700 focus:outline-none focus:border-indigo-400"
            value={selectedType}
            onChange={(e) => { setSelectedType(e.target.value); setHovered(null); }}
          >
            {TYPE_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
        </div>
        <p className="text-xs text-slate-500 mt-1 leading-relaxed">
          Each cell shows how many slow query / blocker / deadlock <strong>events</strong> occurred
          at that hour of the day (rows, 0–23h) on that day of the week (columns, Mon–Sun).
          Density is the <strong>sum of <code>occurrence_count</code></strong> for all ingested
          rows whose timestamp falls in that bucket — so a single Splunk row that fired 50 times
          contributes 50 to the cell, not 1.
          Darker indigo = more events. Hover a cell to see the full breakdown by type, host, and database.
        </p>
        {!loading && totalEvents > 0 && (
          <p className="text-xs text-slate-400 mt-0.5">
            {fmt(totalEvents)} total occurrences · local host time
          </p>
        )}
      </CardHeader>

      <CardContent>
        {loading ? (
          <div className="h-52 flex items-center justify-center text-sm text-slate-400">
            Loading…
          </div>
        ) : cells.length === 0 ? (
          <div className="h-52 flex items-center justify-center text-sm text-slate-400">
            No data with parseable timestamps
          </div>
        ) : (
          <>
            {/* ---- Grid -------------------------------------------------- */}
            <div
              className="relative overflow-x-auto"
              onMouseMove={handleMouseMove}
              onMouseLeave={() => setHovered(null)}
            >
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "3rem repeat(7, minmax(0, 1fr))",
                  gap: "2px",
                }}
              >
                {/* Header row */}
                <div /> {/* top-left corner */}
                {DAYS.map((d) => (
                  <div
                    key={d}
                    className="text-center text-[10px] font-semibold text-slate-500 pb-1 tracking-wide"
                  >
                    {d}
                  </div>
                ))}

                {/* 24 hour rows */}
                {Array.from({ length: 24 }, (_, h) => (
                  <React.Fragment key={h}>
                    {/* Hour label — show 12am, 6am, 12pm, 6pm, 11pm */}
                    <div
                      className="flex items-center justify-end pr-1.5 text-[10px] text-slate-400 leading-none select-none"
                      style={{ height: CELL_H }}
                    >
                      {(h % 6 === 0 || h === 23) ? formatHour(h) : ""}
                    </div>

                    {/* 7 day cells */}
                    {Array.from({ length: 7 }, (_, d) => {
                      const key = `${h}-${d}`;
                      const cell = cellMap.get(key);
                      const count = cell?.count ?? 0;
                      const isHovered = hovered?.hour === h && hovered?.weekday === d;
                      return (
                        <div
                          key={key}
                          style={{
                            backgroundColor: colorFor(count, maxCount),
                            height: CELL_H,
                            borderRadius: 3,
                            outline: isHovered ? "2px solid #4f46e5" : "1px solid transparent",
                            transition: "outline 0.05s",
                            cursor: count > 0 ? "crosshair" : "default",
                          }}
                          onMouseEnter={() => setHovered(cell ?? null)}
                        />
                      );
                    })}
                  </React.Fragment>
                ))}
              </div>
            </div>

            {/* ---- Density legend ---------------------------------------- */}
            <div className="flex items-center gap-2 mt-3 justify-end">
              <span className="text-[10px] text-slate-400">Low</span>
              <div className="flex gap-0.5">
                {COLOR_SCALE.map((c, i) => (
                  <div
                    key={i}
                    style={{ width: 16, height: 10, backgroundColor: c, borderRadius: 2 }}
                  />
                ))}
              </div>
              <span className="text-[10px] text-slate-400">
                High ({fmt(maxCount)})
              </span>
            </div>

            {/* ---- Type colour legend ------------------------------------ */}
            {!selectedType && (
              <div className="flex flex-wrap gap-x-4 gap-y-1 mt-2">
                {Object.entries(TYPE_COLORS)
                  .filter(([t]) => t !== "unknown")
                  .map(([type, color]) => (
                    <div key={type} className="flex items-center gap-1">
                      <div style={{ width: 8, height: 8, borderRadius: "50%", backgroundColor: color }} />
                      <span className="text-[10px] text-slate-500 capitalize">
                        {type.replace(/_/g, " ")}
                      </span>
                    </div>
                  ))}
              </div>
            )}
          </>
        )}
      </CardContent>

      {/* Floating tooltip — rendered at viewport level, follows cursor */}
      {hovered && <HoverTooltip cell={hovered} x={mousePos.x} y={mousePos.y} />}
    </Card>
  );
}
