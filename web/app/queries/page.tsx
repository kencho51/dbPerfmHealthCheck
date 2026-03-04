"use client";

import { useCallback, useEffect, useState } from "react";
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  type ColumnDef,
} from "@tanstack/react-table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Spinner } from "@/components/ui/spinner";
import { api, type RawQuery, type EnvironmentType, type QueryType } from "@/lib/api";
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight } from "lucide-react";

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
  return <Badge variant={v as "default"}>{t.replace("_", " ")}</Badge>;
}

const columns: ColumnDef<RawQuery>[] = [
  { accessorKey: "id", header: "ID", size: 60 },
  { accessorKey: "environment", header: "Env", cell: (i) => envBadge(i.getValue<EnvironmentType>()) },
  { accessorKey: "type", header: "Type", cell: (i) => typeBadge(i.getValue<QueryType>()) },
  { accessorKey: "source", header: "Src", size: 50 },
  { accessorKey: "host", header: "Host", cell: (i) => <span className="font-mono text-xs">{i.getValue<string>() ?? "—"}</span> },
  { accessorKey: "db_name", header: "Database", cell: (i) => <span className="font-mono text-xs">{i.getValue<string>() ?? "—"}</span> },
  { accessorKey: "occurrence_count", header: "Occ", size: 60 },
  { accessorKey: "month_year", header: "Month", size: 80 },
  {
    accessorKey: "pattern_id",
    header: "Pattern",
    cell: (i) => {
      const v = i.getValue<number | null>();
      return v ? <Badge variant="info">#{v}</Badge> : <span className="text-slate-300">—</span>;
    },
  },
];

export default function QueriesPage() {
  const [page, setPage] = useState(0);
  const [total, setTotal] = useState(0);
  const [data, setData] = useState<RawQuery[]>([]);
  const [loading, setLoading] = useState(true);

  // Filters
  const [environment, setEnvironment] = useState("");
  const [type, setType] = useState("");
  const [host, setHost] = useState("");
  const [search, setSearch] = useState("");
  const [searchInput, setSearchInput] = useState(""); // debounce source

  // Expanded row
  const [expanded, setExpanded] = useState<number | null>(null);

  const buildParams = useCallback(() => {
    const p: Record<string, string | number> = { limit: PAGE_SIZE, offset: page * PAGE_SIZE };
    if (environment) p.environment = environment;
    if (type) p.type = type;
    if (host) p.host = host;
    if (search) p.search = search;
    return p;
  }, [page, environment, type, host, search]);

  useEffect(() => {
    const handler = setTimeout(() => setSearch(searchInput), 300);
    return () => clearTimeout(handler);
  }, [searchInput]);

  useEffect(() => {
    setPage(0);
  }, [environment, type, host, search]);

  useEffect(() => {
    setLoading(true);
    const params = buildParams();
    Promise.all([
      api.queries.list(params),
      api.queries.count(
        Object.fromEntries(
          Object.entries(params).filter(([k]) => !["limit", "offset"].includes(k))
        ) as Record<string, string>
      ),
    ])
      .then(([rows, cnt]) => {
        setData(rows);
        setTotal(cnt.count);
      })
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

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Raw Queries</h1>
        <p className="text-sm text-slate-500 mt-1">
          {total.toLocaleString()} rows — click a row to expand query details
        </p>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3">
        <Select
          className="w-32"
          value={environment}
          onChange={(e) => setEnvironment(e.target.value)}
        >
          <option value="">All envs</option>
          <option value="prod">prod</option>
          <option value="sat">sat</option>
        </Select>
        <Select
          className="w-40"
          value={type}
          onChange={(e) => setType(e.target.value)}
        >
          <option value="">All types</option>
          <option value="slow_query">slow query</option>
          <option value="blocker">blocker</option>
          <option value="deadlock">deadlock</option>
          <option value="slow_query_mongo">mongo slow</option>
        </Select>
        <Input
          className="w-48"
          placeholder="Host contains…"
          value={host}
          onChange={(e) => setHost(e.target.value)}
        />
        <Input
          className="w-56"
          placeholder="Search query text…"
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
        />
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            setEnvironment(""); setType(""); setHost(""); setSearchInput(""); setSearch("");
          }}
        >
          Reset
        </Button>
      </div>

      {/* Table */}
      <div className="overflow-auto rounded-lg border border-slate-200 bg-white">
        {loading ? (
          <div className="flex items-center justify-center h-48">
            <Spinner />
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              {table.getHeaderGroups().map((hg) => (
                <tr key={hg.id} className="border-b border-slate-100 bg-slate-50">
                  {hg.headers.map((h) => (
                    <th
                      key={h.id}
                      className="px-3 py-2 text-left text-xs font-medium text-slate-500"
                      style={{ width: h.getSize() }}
                    >
                      {flexRender(h.column.columnDef.header, h.getContext())}
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row) => (
                <>
                  <tr
                    key={row.id}
                    className="border-b border-slate-50 hover:bg-slate-50 cursor-pointer"
                    onClick={() => setExpanded(expanded === row.original.id ? null : row.original.id)}
                  >
                    {row.getVisibleCells().map((cell) => (
                      <td key={cell.id} className="px-3 py-2">
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </td>
                    ))}
                  </tr>
                  {expanded === row.original.id && (
                    <tr key={`${row.id}-expanded`} className="bg-slate-50">
                      <td colSpan={columns.length} className="px-4 py-3">
                        <pre className="whitespace-pre-wrap text-xs font-mono text-slate-700 max-h-48 overflow-auto">
                          {row.original.query_details ?? "—"}
                        </pre>
                      </td>
                    </tr>
                  )}
                </>
              ))}
              {data.length === 0 && (
                <tr>
                  <td colSpan={columns.length} className="py-12 text-center text-sm text-slate-400">
                    No rows found
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        )}
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between text-sm">
        <span className="text-slate-500">
          Page {page + 1} of {pageCount || 1} &bull; {total.toLocaleString()} rows
        </span>
        <div className="flex gap-1">
          <Button variant="outline" size="icon" onClick={() => setPage(0)} disabled={page === 0}>
            <ChevronsLeft className="h-4 w-4" />
          </Button>
          <Button variant="outline" size="icon" onClick={() => setPage((p) => p - 1)} disabled={page === 0}>
            <ChevronLeft className="h-4 w-4" />
          </Button>
          <Button variant="outline" size="icon" onClick={() => setPage((p) => p + 1)} disabled={page >= pageCount - 1}>
            <ChevronRight className="h-4 w-4" />
          </Button>
          <Button variant="outline" size="icon" onClick={() => setPage(pageCount - 1)} disabled={page >= pageCount - 1}>
            <ChevronsRight className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </div>
  );
}
