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
import { api, type RawQuery, type EnvironmentType, type QueryType } from "@/lib/api";
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, X } from "lucide-react";

const PAGE_SIZE = 50;

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

const columns: ColumnDef<RawQuery>[] = [
  { accessorKey: "id",               header: "ID",       size: 52 },
  { accessorKey: "environment",      header: "Env",      size: 64,  cell: (i) => envBadge(i.getValue<EnvironmentType>()) },
  { accessorKey: "type",             header: "Type",               cell: (i) => typeBadge(i.getValue<QueryType>()) },
  { accessorKey: "source",           header: "Src",      size: 60,  cell: (i) => { const v = i.getValue<string>(); return <Badge variant={v === "sql" ? "sql" : "mongo"}>{v === "mongodb" ? "mongo" : v}</Badge>; } },
  { accessorKey: "host",             header: "Host",     size: 160, cell: (i) => <span className="font-mono truncate block max-w-[148px]" title={i.getValue<string>() ?? ""}>{i.getValue<string>() ?? "—"}</span> },
  { accessorKey: "db_name",          header: "Database", size: 160, cell: (i) => <span className="font-mono truncate block max-w-[148px]" title={i.getValue<string>() ?? ""}>{i.getValue<string>() ?? "—"}</span> },
  { accessorKey: "occurrence_count", header: "Occ",      size: 48 },
  { accessorKey: "month_year",       header: "Month",    size: 76,  cell: (i) => <span>{i.getValue<string | null>() ?? "—"}</span> },
  {
    accessorKey: "query_details",
    header: "Query Details",
    cell: (i) => {
      const v = i.getValue<string | null>();
      return v
        ? <span className="font-mono text-slate-600 truncate block max-w-[340px]" title={v}>{v}</span>
        : <span className="text-slate-300">—</span>;
    },
  },
];

// Compact filter controls -----------------------------------------------
function FInput({ value, onChange, placeholder }: { value: string; onChange: (v: string) => void; placeholder?: string }) {
  return (
    <input
      className="h-6 w-full rounded border border-slate-200 bg-white px-1.5 text-[11px] text-slate-700 placeholder:text-slate-300 focus:outline-none focus:border-indigo-400"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder ?? "filter…"}
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

export default function QueriesPage() {
  const [page, setPage] = useState(0);
  const [total, setTotal] = useState(0);
  const [data, setData] = useState<RawQuery[]>([]);
  const [loading, setLoading] = useState(true);

  // Instant filters (selects)
  const [environment, setEnvironment] = useState("");
  const [type, setType] = useState("");
  const [source, setSource] = useState("");

  // Text inputs (UI state)
  const [hostInput,   setHostInput]   = useState("");
  const [dbInput,     setDbInput]     = useState("");
  const [monthInput,  setMonthInput]  = useState("");
  const [searchInput, setSearchInput] = useState("");

  // Debounced values (used in API call)
  const [host,   setHost]   = useState("");
  const [dbName, setDbName] = useState("");
  const [month,  setMonth]  = useState("");
  const [search, setSearch] = useState("");

  useEffect(() => { const t = setTimeout(() => setHost(hostInput),   300); return () => clearTimeout(t); }, [hostInput]);
  useEffect(() => { const t = setTimeout(() => setDbName(dbInput),   300); return () => clearTimeout(t); }, [dbInput]);
  useEffect(() => { const t = setTimeout(() => setMonth(monthInput), 300); return () => clearTimeout(t); }, [monthInput]);
  useEffect(() => { const t = setTimeout(() => setSearch(searchInput), 300); return () => clearTimeout(t); }, [searchInput]);

  // Reset page on any filter change
  useEffect(() => { setPage(0); }, [environment, type, source, host, dbName, month, search]);

  const buildParams = useCallback(() => {
    const p: Record<string, string | number> = { limit: PAGE_SIZE, offset: page * PAGE_SIZE };
    if (environment) p.environment = environment;
    if (type)        p.type        = type;
    if (source)      p.source      = source;
    if (host)        p.host        = host;
    if (dbName)      p.db_name     = dbName;
    if (month)       p.month_year  = month;
    if (search)      p.search      = search;
    return p;
  }, [page, environment, type, source, host, dbName, month, search]);

  useEffect(() => {
    setLoading(true);
    const params = buildParams();
    const countParams = Object.fromEntries(
      Object.entries(params).filter(([k]) => !["limit", "offset"].includes(k))
    ) as Record<string, string>;
    Promise.all([api.queries.list(params), api.queries.count(countParams)])
      .then(([rows, cnt]) => { setData(rows); setTotal(cnt.count); })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [buildParams]);

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
    setHostInput(""); setDbInput(""); setMonthInput(""); setSearchInput("");
    setHost(""); setDbName(""); setMonth(""); setSearch("");
  };

  return (
    <div className="space-y-3">
      {/* Header bar */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-slate-900">Raw Queries</h1>
          <p className="text-xs text-slate-500 mt-0.5">{total.toLocaleString()} rows</p>
        </div>
        <div className="flex items-center gap-2">
          <input
            className="h-7 w-56 rounded border border-slate-200 bg-white px-2 text-xs text-slate-700 placeholder:text-slate-300 focus:outline-none focus:border-indigo-400"
            placeholder="Search query text…"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
          />
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
              {/* Column labels */}
              {table.getHeaderGroups().map((hg) => (
                <tr key={hg.id} className="border-b border-slate-200 bg-slate-100">
                  {hg.headers.map((h) => (
                    <th
                      key={h.id}
                      className="px-2 py-1.5 text-left text-[11px] font-semibold text-slate-600 whitespace-nowrap"
                      style={{ width: h.getSize() }}
                    >
                      {flexRender(h.column.columnDef.header, h.getContext())}
                    </th>
                  ))}
                </tr>
              ))}
              {/* Per-column filter row */}
              <tr className="border-b border-slate-200 bg-slate-50">
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
                    <option value="slow_query_mongo">mongo slow</option>
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
                  <FInput value={hostInput} onChange={setHostInput} placeholder="host…" />
                </td>
                <td className="px-2 py-1">
                  <FInput value={dbInput} onChange={setDbInput} placeholder="db…" />
                </td>
                <td className="px-2 py-1" />             {/* Occ */}
                <td className="px-2 py-1">
                  <FInput value={monthInput} onChange={setMonthInput} placeholder="YYYY-MM" />
                </td>
                <td className="px-2 py-1" />             {/* Query Details — search is in header */}
              </tr>
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row) => (
                <tr key={row.id} className="border-b border-slate-50 hover:bg-indigo-50/30">
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
                    No rows found
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
    </div>
  );
}
