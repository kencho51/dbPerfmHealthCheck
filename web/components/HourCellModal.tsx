"use client";

/**
 * HourCellModal — wide centered modal for drilling into the raw query rows
 * that belong to a specific Peak Hour Heatmap cell (hour × weekday bucket).
 *
 * Row click → inline detail panel showing full query text + type-specific
 * metrics from the raw_query_* typed tables (not the truncated raw_query row).
 *
 * Optional "Curate this query →" button opens the existing QueryDetailDrawer.
 */

import React, { useEffect, useRef, useState, useCallback } from "react";
import { X, ChevronLeft, ExternalLink } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import {
  api,
  type HourCell,
  type HourQueryRow,
  type RawQuery,
  type AnalyticsFilters,
  type QueryType,
  type EnvironmentType,
  type TypedQueryDetail,
  type SlowSqlDetail,
  type BlockerDetail,
  type DeadlockDetail,
  type SlowMongoDetail,
} from "@/lib/api";
import { QueryDetailDrawer } from "@/components/QueryDetailDrawer";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

const TYPE_COLORS: Record<string, string> = {
  slow_query:        "bg-indigo-100 text-indigo-700",
  slow_query_mongo:  "bg-violet-100 text-violet-700",
  blocker:           "bg-amber-100  text-amber-700",
  deadlock:          "bg-red-100    text-red-700",
  unknown:           "bg-slate-100  text-slate-500",
};

function formatHour(h: number): string {
  if (h === 0)  return "12am";
  if (h === 12) return "12pm";
  return h < 12 ? `${h}am` : `${h - 12}pm`;
}

function fmt(n: number) { return n.toLocaleString(); }

function TypeBadge({ type }: { type: string }) {
  const cls = TYPE_COLORS[type] ?? TYPE_COLORS.unknown;
  return (
    <span className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium leading-none whitespace-nowrap ${cls}`}>
      {type.replace(/_/g, " ")}
    </span>
  );
}

function EnvBadge({ env }: { env: EnvironmentType }) {
  return <Badge variant={env === "prod" ? "prod" : "sat"}>{env}</Badge>;
}

// ---------------------------------------------------------------------------
// Type-specific detail panels
// ---------------------------------------------------------------------------

function MetaGrid({ items }: { items: { label: string; value: React.ReactNode }[] }) {
  return (
    <div className="grid grid-cols-2 gap-x-8 gap-y-1.5 text-xs">
      {items.map(({ label, value }) => (
        <div key={label} className="flex gap-2">
          <span className="w-36 shrink-0 text-slate-400">{label}</span>
          <span className="text-slate-800 font-mono break-all">{value ?? "—"}</span>
        </div>
      ))}
    </div>
  );
}

function FullQueryBlock({ label, code }: { label: string; code: string | null }) {
  return (
    <div>
      <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1">{label}</p>
      <pre className="rounded-lg border border-slate-100 bg-slate-950 text-green-400 text-[11px] font-mono p-3 whitespace-pre-wrap break-all leading-relaxed max-h-72 overflow-y-auto">
        {code ?? "(no text available)"}
      </pre>
    </div>
  );
}

function SlowSqlPanel({ d }: { d: SlowSqlDetail }) {
  return (
    <div className="space-y-4">
      <MetaGrid items={[
        { label: "Execution count",    value: d.execution_count?.toLocaleString() },
        { label: "Max elapsed (s)",    value: d.max_elapsed_time_s?.toFixed(3) },
        { label: "Avg elapsed (s)",    value: d.avg_elapsed_time_s?.toFixed(3) },
        { label: "Total elapsed (s)",  value: d.total_elapsed_time_s?.toFixed(3) },
        { label: "Total worker (s)",   value: d.total_worker_time_s?.toFixed(3) },
        { label: "Avg I/O",            value: d.avg_io?.toFixed(2) },
        { label: "Avg logical reads",  value: d.avg_logical_reads?.toFixed(2) },
        { label: "Avg logical writes", value: d.avg_logical_writes?.toFixed(2) },
        { label: "Creation time",      value: d.creation_time },
        { label: "Last execution",     value: d.last_execution_time },
      ]} />
      <FullQueryBlock label="Full SQL (query_final)" code={d.query_final} />
    </div>
  );
}

function BlockerPanel({ d }: { d: BlockerDetail }) {
  return (
    <div className="space-y-4">
      <MetaGrid items={[
        { label: "Database",   value: d.currentdbname },
        { label: "Lock modes", value: d.lock_modes },
        { label: "Count",      value: d.count?.toLocaleString() },
        { label: "Latest",     value: d.latest },
        { label: "Earliest",   value: d.earliest },
        { label: "Victims",    value: d.victims },
        { label: "Resources",  value: d.resources },
      ]} />
      <FullQueryBlock label="Full SQL (all_query)" code={d.all_query} />
    </div>
  );
}

function DeadlockPanel({ d }: { d: DeadlockDetail }) {
  return (
    <div className="space-y-4">
      <MetaGrid items={[
        { label: "Event time",       value: d.event_time },
        { label: "Deadlock ID",      value: d.deadlock_id },
        { label: "Is victim",        value: d.is_victim === 1 ? "Yes" : d.is_victim === 0 ? "No" : null },
        { label: "Lock mode",        value: d.lock_mode },
        { label: "Wait resource",    value: d.wait_resource },
        { label: "Wait time (ms)",   value: d.wait_time_ms?.toLocaleString() },
        { label: "Transaction name", value: d.transaction_name },
        { label: "App host",         value: d.app_host },
      ]} />
      <FullQueryBlock label="Full SQL (sql_text)" code={d.sql_text} />
      {d.raw_xml && (
        <details className="text-xs">
          <summary className="cursor-pointer text-slate-400 hover:text-slate-600 select-none">
            Show raw XML
          </summary>
          <pre className="mt-1 rounded border border-slate-200 bg-slate-50 p-2 text-[10px] font-mono whitespace-pre-wrap break-all max-h-48 overflow-y-auto text-slate-600">
            {d.raw_xml}
          </pre>
        </details>
      )}
    </div>
  );
}

function SlowMongoPanel({ d }: { d: SlowMongoDetail }) {
  return (
    <div className="space-y-4">
      <MetaGrid items={[
        { label: "Event time",    value: d.event_time },
        { label: "Duration (ms)", value: d.duration_ms?.toLocaleString() },
        { label: "Collection",    value: d.collection },
        { label: "Operation",     value: d.op_type },
        { label: "Plan summary",  value: d.plan_summary },
        { label: "Remote client", value: d.remote_client },
      ]} />
      <FullQueryBlock label="Full command (command_json)" code={d.command_json} />
    </div>
  );
}

function TypedDetailSection({
  detail,
  row,
}: {
  detail: TypedQueryDetail;
  row:    HourQueryRow;
}) {
  if (!detail.data) {
    // All 4 fallbacks failed — genuinely no typed row covers this query.
    // For mongo, query_details is the full command JSON; show it.
    if (detail.type === "slow_query_mongo") {
      return (
        <div className="space-y-3">
          <p className="text-[10px] text-amber-600 bg-amber-50 border border-amber-200 rounded px-2 py-1">
            No matching metrics row found — showing raw command JSON.
          </p>
          <FullQueryBlock label="Full command (query_details)" code={row.query_details} />
        </div>
      );
    }
    // SQL/blocker/deadlock: query_details may be truncated — be honest.
    return (
      <div className="space-y-2">
        <p className="text-xs text-slate-400 italic">
          No typed detail available — this row predates the typed ingestion tables.
          Re-upload the source CSV file to populate full detail.
        </p>
        {row.query_details && (
          <>
            <p className="text-[10px] text-amber-600 bg-amber-50 border border-amber-200 rounded px-2 py-1">
              The text below may be truncated. Full text requires a re-upload.
            </p>
            <FullQueryBlock label="Query text (possibly truncated)" code={row.query_details} />
          </>
        )}
      </div>
    );
  }
  switch (detail.type) {
    case "slow_query":       return <SlowSqlPanel   d={detail.data as SlowSqlDetail}   />;
    case "blocker":          return <BlockerPanel   d={detail.data as BlockerDetail}   />;
    case "deadlock":         return <DeadlockPanel  d={detail.data as DeadlockDetail}  />;
    case "slow_query_mongo": return <SlowMongoPanel d={detail.data as SlowMongoDetail} />;
    default:                 return <p className="text-xs text-slate-400 italic">Unknown query type.</p>;
  }
}

// ---------------------------------------------------------------------------
// Inline detail view (replaces table body when a row is clicked)
// ---------------------------------------------------------------------------

interface DetailState {
  row:     HourQueryRow;
  typed:   TypedQueryDetail | null;
  loading: boolean;
}

function InlineDetailView({
  detail,
  onBack,
  onCurate,
}: {
  detail:   DetailState;
  onBack:   () => void;
  onCurate: () => void;
}) {
  const r = detail.row;
  return (
    <div className="flex flex-col h-full">
      {/* Sub-header */}
      <div className="flex items-center justify-between px-5 py-2.5 border-b border-slate-200 bg-slate-50 shrink-0">
        <button
          className="flex items-center gap-1.5 text-xs text-slate-500 hover:text-slate-800 transition-colors"
          onClick={onBack}
        >
          <ChevronLeft className="h-3 w-3" />
          Back to results
        </button>
        <button
          className="flex items-center gap-1.5 text-xs rounded border border-indigo-300 bg-white text-indigo-600 px-2.5 py-1 hover:bg-indigo-50 transition-colors"
          onClick={onCurate}
        >
          <ExternalLink className="h-3 w-3" />
          Curate this query
        </button>
      </div>

      {/* Scrollable detail body */}
      <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
        {/* Basic metadata */}
        <div>
          <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-2">
            Query metadata — row #{r.id}
          </p>
          <div className="rounded-lg border border-slate-100 bg-slate-50 p-3 space-y-2">
            <div className="flex gap-2 flex-wrap">
              <TypeBadge type={r.type} />
              <EnvBadge env={r.environment} />
            </div>
            <MetaGrid items={[
              { label: "Host",        value: r.host },
              { label: "Database",    value: r.db_name },
              { label: "Month",       value: r.month_year },
              { label: "Occurrences", value: r.occurrence_count.toLocaleString() },
              { label: "Time",        value: r.time },
            ]} />
          </div>
        </div>

        {/* Typed detail */}
        <div>
          <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-2">
            Type-specific detail{detail.loading && <span className="ml-2 text-indigo-400 normal-case font-normal">Loading…</span>}
          </p>
          {detail.loading ? (
            <div className="h-24 flex items-center justify-center text-sm text-slate-400">
              Fetching typed detail…
            </div>
          ) : detail.typed ? (
            <TypedDetailSection detail={detail.typed} row={detail.row} />
          ) : (
            <p className="text-xs text-slate-400 italic">Could not load typed detail.</p>
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

interface Props {
  cell:         HourCell;
  filters:      AnalyticsFilters;
  selectedType: string;
  weekLabel?:   string;
  onClose:      () => void;
}

export function HourCellModal({ cell, filters, selectedType, weekLabel, onClose }: Props) {
  const panelRef = useRef<HTMLDivElement>(null);
  const [rows, setRows]               = useState<HourQueryRow[]>([]);
  const [total, setTotal]             = useState(0);
  const [loading, setLoading]         = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);

  const [detailView, setDetailView]     = useState<DetailState | null>(null);
  const [curateQuery, setCurateQuery]   = useState<RawQuery | null>(null);
  const [loadingCurate, setLoadingCurate] = useState(false);

  const mergedFilters: AnalyticsFilters = {
    ...filters,
    ...(selectedType ? { type: selectedType as QueryType } : {}),
  };

  // Initial fetch
  useEffect(() => {
    setLoading(true);
    api.analytics
      .byHourQueries(cell.hour, cell.weekday, mergedFilters, 50, 0)
      .then((res) => { setRows(res.rows); setTotal(res.total); })
      .catch(console.error)
      .finally(() => setLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleLoadMore = useCallback(() => {
    setLoadingMore(true);
    api.analytics
      .byHourQueries(cell.hour, cell.weekday, mergedFilters, 50, rows.length)
      .then((res) => { setRows((p) => [...p, ...res.rows]); setTotal(res.total); })
      .catch(console.error)
      .finally(() => setLoadingMore(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rows.length]);

  // Row click: show typed detail inline
  const handleRowClick = useCallback((row: HourQueryRow) => {
    setDetailView({ row, typed: null, loading: true });
    api.queries
      .typedDetail(row.id)
      .then((typed) => setDetailView({ row, typed, loading: false }))
      .catch(()    => setDetailView({ row, typed: null, loading: false }));
  }, []);

  // Curate button inside detail view
  const handleCurate = useCallback(async () => {
    if (!detailView) return;
    setLoadingCurate(true);
    try {
      const q = await api.queries.get(detailView.row.id);
      setCurateQuery(q);
    } catch (e) {
      console.error(e);
    } finally {
      setLoadingCurate(false);
    }
  }, [detailView]);

  // Escape: close innermost layer first
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      if (curateQuery) { setCurateQuery(null); return; }
      if (detailView)  { setDetailView(null);  return; }
      onClose();
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [onClose, detailView, curateQuery]);

  // Backdrop click closes modal (not when curation drawer is open)
  const onBackdrop = (e: React.MouseEvent) => {
    if (curateQuery) return;
    if (panelRef.current && !panelRef.current.contains(e.target as Node)) onClose();
  };

  const headerLabel = `${DAYS[cell.weekday]} · ${formatHour(cell.hour)}–${formatHour((cell.hour + 1) % 24)}`;

  return (
    <>
      <div
        className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-[1px] p-4"
        onClick={onBackdrop}
      >
        <div
          ref={panelRef}
          className="w-[90vw] max-w-7xl max-h-[85vh] bg-white rounded-xl shadow-2xl flex flex-col overflow-hidden"
        >
          {/* ── Header ──────────────────────────────────────────────────── */}
          <div className="bg-indigo-600 text-white px-5 py-3 flex items-center justify-between shrink-0">
            <div className="flex items-center gap-3 min-w-0">
              <span className="font-semibold text-sm shrink-0">{headerLabel}</span>
              <span className="text-indigo-200 text-xs shrink-0">
                {fmt(cell.count)} event{cell.count !== 1 ? "s" : ""}
              </span>
              {weekLabel && <span className="text-indigo-300 text-xs shrink-0">· {weekLabel}</span>}
              {detailView && (
                <span className="text-indigo-300 text-xs truncate">
                  · Row #{detailView.row.id} · {detailView.row.type.replace(/_/g, " ")}
                </span>
              )}
            </div>
            <button
              onClick={onClose}
              className="text-indigo-200 hover:text-white transition-colors ml-4 shrink-0"
              aria-label="Close"
            >
              <X className="h-4 w-4" />
            </button>
          </div>

          {/* ── Body: detail view OR table ─────────────────────────────── */}
          {detailView ? (
            <InlineDetailView
              detail={detailView}
              onBack={() => setDetailView(null)}
              onCurate={handleCurate}
            />
          ) : (
            <>
              <div className="flex-1 overflow-y-auto">
                {loading ? (
                  <div className="h-48 flex items-center justify-center text-sm text-slate-400">
                    Loading queries…
                  </div>
                ) : rows.length === 0 ? (
                  <div className="h-48 flex items-center justify-center text-sm text-slate-400">
                    No rows found for this cell.
                  </div>
                ) : (
                  <table className="w-full text-xs border-collapse">
                    <thead className="sticky top-0 bg-slate-50 z-10">
                      <tr>
                        <th className="px-3 py-2 text-left font-semibold text-slate-500 border-b border-slate-200 w-14">#</th>
                        <th className="px-3 py-2 text-left font-semibold text-slate-500 border-b border-slate-200 w-36">Type</th>
                        <th className="px-3 py-2 text-left font-semibold text-slate-500 border-b border-slate-200">Host</th>
                        <th className="px-3 py-2 text-left font-semibold text-slate-500 border-b border-slate-200">Database</th>
                        <th className="px-3 py-2 text-left font-semibold text-slate-500 border-b border-slate-200 w-20">Env</th>
                        <th className="px-3 py-2 text-left font-semibold text-slate-500 border-b border-slate-200 w-20">Month</th>
                        <th className="px-3 py-2 text-left font-semibold text-slate-500 border-b border-slate-200 w-36">Time</th>
                        <th className="px-3 py-2 text-right font-semibold text-slate-500 border-b border-slate-200 w-20">Occ.</th>
                        <th className="px-3 py-2 text-left font-semibold text-slate-500 border-b border-slate-200">Query preview</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((row) => {
                        const preview = row.query_details
                          ? row.query_details.length > 120
                            ? row.query_details.slice(0, 120) + "…"
                            : row.query_details
                          : "";
                        return (
                          <tr
                            key={row.id}
                            className="border-b border-slate-100 last:border-0 hover:bg-indigo-50 cursor-pointer transition-colors"
                            onClick={() => handleRowClick(row)}
                            title="Click for full typed detail"
                          >
                            <td className="px-3 py-2 text-slate-400 font-mono">{row.id}</td>
                            <td className="px-3 py-2"><TypeBadge type={row.type} /></td>
                            <td className="px-3 py-2 font-mono text-slate-700 max-w-[160px] truncate" title={row.host ?? ""}>{row.host ?? <span className="text-slate-300">—</span>}</td>
                            <td className="px-3 py-2 font-mono text-slate-700 max-w-[160px] truncate" title={row.db_name ?? ""}>{row.db_name ?? <span className="text-slate-300">—</span>}</td>
                            <td className="px-3 py-2"><EnvBadge env={row.environment} /></td>
                            <td className="px-3 py-2 text-slate-600">{row.month_year ?? "—"}</td>
                            <td className="px-3 py-2 font-mono text-slate-500 text-[11px] whitespace-nowrap">{row.time ?? "—"}</td>
                            <td className="px-3 py-2 text-right font-mono text-slate-700">{fmt(row.occurrence_count)}</td>
                            <td className="px-3 py-2 font-mono text-slate-500 text-[11px] max-w-xs" title={row.query_details ?? ""}>
                              {preview || <span className="text-slate-300">—</span>}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                )}
              </div>

              {/* Footer */}
              {!loading && (
                <div className="shrink-0 border-t border-slate-200 bg-slate-50 px-5 py-2.5 flex items-center justify-between gap-4">
                  <span className="text-xs text-slate-500">
                    Showing <span className="font-semibold text-slate-700">{fmt(rows.length)}</span> of{" "}
                    <span className="font-semibold text-slate-700">{fmt(total)}</span> queries
                    {total > 0 && <span className="ml-2 text-slate-400">· Click a row for full typed detail</span>}
                  </span>
                  {rows.length < total && (
                    <button
                      className="text-xs rounded border border-indigo-300 bg-white text-indigo-600 px-3 py-1 hover:bg-indigo-50 disabled:opacity-50 transition-colors"
                      onClick={handleLoadMore}
                      disabled={loadingMore}
                    >
                      {loadingMore ? "Loading…" : `Load more (${fmt(total - rows.length)} remaining)`}
                    </button>
                  )}
                </div>
              )}
            </>
          )}
        </div>
      </div>

      {/* Curation drawer — stacked at z-60, opened from the detail view */}
      {curateQuery && (
        <div className="fixed inset-0 z-[60]">
          <QueryDetailDrawer
            query={curateQuery}
            onClose={() => setCurateQuery(null)}
            onPatternChange={(updated) => setCurateQuery(updated)}
          />
        </div>
      )}
      {loadingCurate && (
        <div className="fixed inset-0 z-[60] flex items-center justify-center bg-black/20">
          <div className="bg-white rounded-lg px-5 py-3 text-sm text-slate-600 shadow-lg">
            Opening query…
          </div>
        </div>
      )}
    </>
  );
}
