"use client";

import React, { useCallback, useEffect, useState } from "react";
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  type ColumnDef,
} from "@tanstack/react-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { api, type CuratedQuery, type RawQuery, type EnvironmentType, type QueryType, type SeverityType } from "@/lib/api";
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, X } from "lucide-react";
import { QueryDetailDrawer } from "@/components/QueryDetailDrawer";

const PAGE_SIZE = 50;

const SEV_COLORS: Record<SeverityType, string> = {
  critical: "text-red-600 bg-red-50 border-red-200",
  warning: "text-amber-600 bg-amber-50 border-amber-200",
  info: "text-indigo-600 bg-indigo-50 border-indigo-200",
};

function envBadge(env: EnvironmentType) {
  return <Badge variant={env === "prod" ? "prod" : "sat"}>{env}</Badge>;
}
function typeBadge(t: QueryType) {
  const v =
    t === "slow_query" ? "default"
    : t === "blocker" ? "warning"
    : t === "deadlock" ? "critical"
    : "mongo" as const;
  return <Badge variant={v as "default"}>{t.replaceAll("_", " ")}</Badge>;
}

/** Convert a flat CuratedQuery into a RawQuery for the drawer. */
function curatedToRaw(cq: CuratedQuery): RawQuery {
  return {
    id: cq.raw_query_id,
    query_hash: cq.query_hash,
    time: cq.time,
    source: cq.source,
    host: cq.host,
    db_name: cq.db_name,
    environment: cq.environment,
    type: cq.type,
    query_details: cq.query_details,
    month_year: cq.month_year,
    occurrence_count: cq.occurrence_count,
    first_seen: cq.first_seen,
    last_seen: cq.last_seen,
    created_at: cq.created_at,
    updated_at: cq.updated_at,
    curated_id: cq.id,
  };
}

const columns: ColumnDef<CuratedQuery>[] = [
  { accessorKey: "raw_query_id", header: "ID",    size: 52 },
  {
    accessorKey: "label",
    header: "Label",
    size: 140,
    cell: (i) => {
      const l = i.getValue<CuratedQuery["label"]>();
      if (!l) return <span className="text-slate-300 text-[11px] italic">none</span>;
      return (
        <span className={`inline-block text-[10px] font-medium rounded border px-1.5 py-0.5 ${SEV_COLORS[l.severity] ?? ""}`}>
          {l.name}
        </span>
      );
    },
  },
  { accessorKey: "environment", header: "Env",  size: 64,  cell: (i) => envBadge(i.getValue<EnvironmentType>()) },
  { accessorKey: "type",        header: "Type",            cell: (i) => typeBadge(i.getValue<QueryType>()) },
  {
    accessorKey: "source",
    header: "Src",
    size: 60,
    cell: (i) => {
      const v = i.getValue<string>();
      return <Badge variant={v === "sql" ? "sql" : "mongo"}>{v === "mongodb" ? "mongo" : v}</Badge>;
    },
  },
  { accessorKey: "host",             header: "Host",     size: 160, cell: (i) => <span className="font-mono truncate block max-w-[148px]" title={i.getValue<string>() ?? ""}>{i.getValue<string>() ?? ""}</span> },
  { accessorKey: "db_name",          header: "Database", size: 160, cell: (i) => <span className="font-mono truncate block max-w-[148px]" title={i.getValue<string>() ?? ""}>{i.getValue<string>() ?? ""}</span> },
  { accessorKey: "occurrence_count", header: "Occ",      size: 48 },
  { accessorKey: "month_year",       header: "Month",    size: 76,  cell: (i) => <span>{i.getValue<string | null>() ?? ""}</span> },
  {
    accessorKey: "query_details",
    header: "Query Details",
    cell: (i) => {
      const v = i.getValue<string | null>();
      return v
        ? <span className="font-mono text-slate-600 truncate block max-w-[340px]" title={v}>{v}</span>
        : <span className="text-slate-300"></span>;
    },
  },
];

function FInput({ value, onChange, placeholder }: { value: string; onChange: (v: string) => void; placeholder?: string }) {
  const [draft, setDraft] = React.useState(value);
  React.useEffect(() => { setDraft(value); }, [value]);
  const commit = (d: string) => { if (d !== value) onChange(d); };
  return (
    <input
      className="h-6 w-full rounded border border-slate-200 bg-white px-1.5 text-[11px] text-slate-700 placeholder:text-slate-300 focus:outline-none focus:border-indigo-400"
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={(e) => commit(e.target.value)}
      onKeyDown={(e) => { if (e.key === "Enter") { commit((e.target as HTMLInputElement).value); (e.target as HTMLInputElement).blur(); } }}
      placeholder={placeholder ?? " to filter"}
    />
  );
}
function SearchInput({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const [draft, setDraft] = React.useState(value);
  React.useEffect(() => { setDraft(value); }, [value]);
  const commit = (d: string) => { if (d !== value) onChange(d); };
  return (
    <input
      className="h-7 w-56 rounded border border-slate-200 bg-white px-2 text-xs text-slate-700 placeholder:text-slate-300 focus:outline-none focus:border-indigo-400"
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={(e) => commit(e.target.value)}
      onKeyDown={(e) => { if (e.key === "Enter") { commit((e.target as HTMLInputElement).value); (e.target as HTMLInputElement).blur(); } }}
      placeholder="Search query text (Enter)"
    />
  );
}
function FSelect({ value, onChange, children }: { value: string; onChange: (v: string) => void; children: React.ReactNode }) {
  return (
    <select
      className="h-6 w-full rounded border border-slate-200 bg-white px-1 text-[11px] text-slate-700 focus:outline-none focus:border-indigo-400"
      value={value}
      onChange={(e) => onChange(e.target.value)}
    >
      {children}
    </select>
  );
}

export default function PatternsPage() {
  const [page, setPage] = useState(0);
  const [total, setTotal] = useState(0);
  const [data, setData] = useState<CuratedQuery[]>([]);
  const [loading, setLoading] = useState(true);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  // Selected query for the drawer  stored as RawQuery for drawer compatibility
  const [selectedQuery, setSelectedQuery] = useState<RawQuery | null>(null);

  // Refresh mechanism
  const [refreshKey, setRefreshKey] = useState(0);
  const refresh = () => setRefreshKey((k) => k + 1);
  useEffect(() => {
    const onVisible = () => { if (document.visibilityState === "visible") refresh(); };
    document.addEventListener("visibilitychange", onVisible);
    return () => document.removeEventListener("visibilitychange", onVisible);
  }, []);

  const [environment, setEnvironment] = useState("");
  const [type, setType] = useState("");
  const [source, setSource] = useState("");

  const [hostOpts, setHostOpts] = useState<string[]>([]);
  const [dbOpts, setDbOpts] = useState<string[]>([]);
  useEffect(() => {
    api.queries.distinct().then((r) => { setHostOpts(r.hosts); setDbOpts(r.db_names); }).catch(() => {});
  }, []);

  const [host, setHost] = useState("");
  const [dbName, setDbName] = useState("");
  const [month, setMonth] = useState("");
  const [search, setSearch] = useState("");

  useEffect(() => { setPage(0); }, [environment, type, source, host, dbName, month, search, sortDir]);

  const buildParams = useCallback(() => {
    const p: Record<string, string | number> = {
      limit: PAGE_SIZE,
      offset: page * PAGE_SIZE,
      sort_by: "id",
      sort_dir: sortDir,
    };
    if (environment) p.environment = environment;
    if (type)        p.type        = type;
    if (source)      p.source      = source;
    if (host)        p.host        = host;
    if (dbName)      p.db_name     = dbName;
    if (month)       p.month_year  = month;
    if (search)      p.search      = search;
    return p;
  }, [page, environment, type, source, host, dbName, month, search, sortDir]);

  useEffect(() => {
    setLoading(true);
    const params = buildParams();
    const countParams = Object.fromEntries(
      Object.entries(params).filter(([k]) => !["limit", "offset", "sort_by", "sort_dir"].includes(k))
    ) as Record<string, string>;
    Promise.all([api.curated.list(params), api.curated.count(countParams)])
      .then(([rows, cnt]) => { setData(rows); setTotal(cnt.count); })
      .catch(console.error)
      .finally(() => setLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [buildParams, refreshKey]);

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
    manualPagination: true,
    pageCount: Math.ceil(total / PAGE_SIZE),
  });

  const pageCount = Math.ceil(total / PAGE_SIZE);

  const resetAll = () => {
    setEnvironment(""); setType(""); setSource("");
    setHost(""); setDbName(""); setMonth(""); setSearch("");
  };

  const handleCurationChange = (updated: RawQuery) => {
    if (updated.curated_id === null) {
      // Unassigned  remove from table
      setData((prev) => prev.filter((r) => r.raw_query_id !== updated.id));
      setTotal((t) => t - 1);
      setSelectedQuery(null);
    } else {
      // Updated  refresh the row
      setSelectedQuery(updated);
      refresh();
    }
  };

  return (
    <div className="space-y-3">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-slate-900">Curated Queries</h1>
          <p className="text-xs text-slate-500 mt-0.5">{total.toLocaleString()} rows in curation list</p>
        </div>
        <div className="flex items-center gap-2">
          <SearchInput value={search} onChange={setSearch} />
          <Button variant="outline" size="sm" onClick={refresh} className="h-7 text-xs gap-1 text-slate-400 hover:text-slate-700"></Button>
          <Button variant="outline" size="sm" onClick={resetAll} className="h-7 text-xs gap-1">
            <X className="h-3 w-3" />Reset
          </Button>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-auto rounded-lg border border-slate-200 bg-white">
        {loading ? (
          <div className="flex items-center justify-center h-48"><Spinner /></div>
        ) : (
          <table className="w-full text-xs">
            <thead>
              {table.getHeaderGroups().map((hg) => (
                <tr key={hg.id} className="border-b border-slate-200 bg-slate-100">
                  {hg.headers.map((h) => (
                    <th
                      key={h.id}
                      className="px-2 py-1.5 text-left text-[11px] font-semibold text-slate-600 whitespace-nowrap"
                      style={{ width: h.getSize() }}
                    >
                      {h.column.id === "raw_query_id" ? (
                        <button
                          onClick={() => setSortDir((d) => (d === "desc" ? "asc" : "desc"))}
                          className="flex items-center gap-0.5 hover:text-indigo-600 cursor-pointer select-none"
                          title="Sort by ID"
                        >
                          ID <span className="text-[10px]">{sortDir === "desc" ? "" : ""}</span>
                        </button>
                      ) : flexRender(h.column.columnDef.header, h.getContext())}
                    </th>
                  ))}
                </tr>
              ))}
              {/* Filter row */}
              <tr className="border-b border-slate-200 bg-slate-50">
                <td className="px-2 py-1" />
                <td className="px-2 py-1" />
                <td className="px-2 py-1 w-16">
                  <FSelect value={environment} onChange={setEnvironment}>
                    <option value="">all</option>
                    <option value="prod">prod</option>
                    <option value="sat">sat</option>
                  </FSelect>
                </td>
                <td className="px-2 py-1">
                  <FSelect value={type} onChange={setType}>
                    <option value="">all</option>
                    <option value="slow_query">slow query</option>
                    <option value="blocker">blocker</option>
                    <option value="deadlock">deadlock</option>
                  </FSelect>
                </td>
                <td className="px-2 py-1 w-16">
                  <FSelect value={source} onChange={setSource}>
                    <option value="">all</option>
                    <option value="sql">sql</option>
                    <option value="mongodb">mongo</option>
                  </FSelect>
                </td>
                <td className="px-2 py-1">
                  <FSelect value={host} onChange={setHost}>
                    <option value="">all</option>
                    {hostOpts.map((h) => <option key={h} value={h}>{h}</option>)}
                  </FSelect>
                </td>
                <td className="px-2 py-1">
                  <FSelect value={dbName} onChange={setDbName}>
                    <option value="">all</option>
                    {dbOpts.map((d) => <option key={d} value={d}>{d}</option>)}
                  </FSelect>
                </td>
                <td className="px-2 py-1" />
                <td className="px-2 py-1">
                  <FInput value={month} onChange={setMonth} placeholder="YYYY-MM" />
                </td>
                <td className="px-2 py-1" />
              </tr>
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row) => (
                <tr
                  key={row.id}
                  className="border-b border-slate-50 hover:bg-indigo-50/50 cursor-pointer"
                  onClick={() => setSelectedQuery(curatedToRaw(row.original))}
                >
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="px-2 py-0.5 align-middle">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              ))}
              {data.length === 0 && (
                <tr>
                  <td colSpan={columns.length} className="py-12 text-center text-xs text-slate-400">
                    No curated queries found
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        )}
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between text-xs text-slate-500">
        <span>Page {page + 1} of {pageCount || 1} &bull; {total.toLocaleString()} rows</span>
        <div className="flex gap-1">
          <Button variant="outline" size="icon" onClick={() => setPage(0)} disabled={page === 0}>
            <ChevronsLeft className="h-3.5 w-3.5" />
          </Button>
          <Button variant="outline" size="icon" onClick={() => setPage((p) => p - 1)} disabled={page === 0}>
            <ChevronLeft className="h-3.5 w-3.5" />
          </Button>
          <Button variant="outline" size="icon" onClick={() => setPage((p) => p + 1)} disabled={page >= pageCount - 1}>
            <ChevronRight className="h-3.5 w-3.5" />
          </Button>
          <Button variant="outline" size="icon" onClick={() => setPage(pageCount - 1)} disabled={page >= pageCount - 1}>
            <ChevronsRight className="h-3.5 w-3.5" />
          </Button>
        </div>
      </div>

      <QueryDetailDrawer
        query={selectedQuery}
        onClose={() => setSelectedQuery(null)}
        onPatternChange={handleCurationChange}
      />
    </div>
  );
}
