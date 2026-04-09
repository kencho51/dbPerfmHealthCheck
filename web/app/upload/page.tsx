"use client";

import { useState, useRef, useEffect, useCallback } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { Spinner } from "@/components/ui/spinner";
import { api, type ValidationResult, type UploadResult, type MonthTypeRow } from "@/lib/api";
import { CheckCircle, XCircle, Upload, AlertTriangle, X } from "lucide-react";

type EntryStatus = "validating" | "ready" | "uploading" | "done" | "error";

// ---------------------------------------------------------------------------
// Client-side validation — instant, no network call.
// Reads only the first 4 KB of the file (just the header + a few rows).
// ---------------------------------------------------------------------------
const _REQUIRED_COLS: Record<string, string[]> = {
  slow_query_sql:   ["host", "db_name", "query_final"],
  blocker:          ["host", "database_name", "query_text"],
  deadlock:         [],
  slow_query_mongo: ["host", "_raw"],
};

function _detectFileType(name: string): string {
  const n = name.toLowerCase();
  if (n.includes("maxelapsed")) return "slow_query_sql";
  if (n.includes("blocker")) return "blocker";
  if (n.includes("deadlock")) return "deadlock";
  if (n.includes("mongodb") && n.includes("slow")) return "slow_query_mongo";
  return "unknown";
}

function _detectEnvironment(name: string): string {
  const n = name.toLowerCase();
  if (n.includes("prod")) return "prod";
  if (n.includes("sat")) return "sat";
  return "unknown";
}

/** Minimal CSV line parser that handles double-quoted fields. */
function _parseCSVLine(line: string): string[] {
  const vals: string[] = [];
  let cur = "", inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { if (inQ && line[i + 1] === '"') { cur += '"'; i++; } else inQ = !inQ; }
    else if (c === "," && !inQ) { vals.push(cur); cur = ""; }
    else cur += c;
  }
  vals.push(cur);
  return vals;
}

async function validateFileLocally(file: File): Promise<ValidationResult> {
  // Read the FULL file as a byte buffer once.
  // - Exact row count: count LF bytes (no string allocation for the whole file).
  // - Preview: decode only the first 4 KB for header detection + sample rows.
  const fullBuf   = await file.arrayBuffer();
  const fullBytes = new Uint8Array(fullBuf);

  // --- Exact row count ---------------------------------------------------
  // Count total LF characters. Subtract 1 for the trailing LF (if present)
  // to get data-only rows. The header's LF is naturally cancelled out:
  //   - trailing LF present : lfCount = (1 header + N data) → N after subtract
  //   - no trailing LF      : lfCount = N (header LF + N-1 data LFs) → N
  let lfCount = 0;
  for (let i = 0; i < fullBytes.length; i++) {
    if (fullBytes[i] === 0x0a) lfCount++;
  }
  const hasTrailingLF = fullBytes.length > 0 && fullBytes[fullBytes.length - 1] === 0x0a;
  const rowCount = Math.max(0, lfCount - (hasTrailingLF ? 1 : 0));

  // --- Header + sample rows from the first 4 KB only --------------------
  const CHUNK     = Math.min(fullBytes.length, 4096);
  const sampleText = new TextDecoder("utf-8").decode(fullBytes.subarray(0, CHUNK));
  const allLines  = sampleText.split("\n");
  const headerLine = allLines[0] ?? "";
  const headers   = _parseCSVLine(headerLine).map((h) => h.replace(/^"|"$/g, "").trim());

  const fileType    = _detectFileType(file.name);
  const environment = _detectEnvironment(file.name);
  const errors: string[]   = [];
  const warnings: string[] = [];

  if (fileType === "unknown") {
    errors.push(`Unrecognised filename '${file.name}'. Expected: maxElapsed*, blockers*, deadlocks*, mongodbSlowQueries*.`);
  } else {
    const missing = (_REQUIRED_COLS[fileType] ?? []).filter((col) => !headers.includes(col));
    if (missing.length) errors.push(`Missing required columns: ${JSON.stringify(missing)}`);
  }
  if (environment === "unknown")
    warnings.push("Could not detect environment (prod/sat) from filename.");

  const dataLines = allLines.slice(1).filter((l) => l.trim());
  const sampleRows = dataLines.slice(0, 5).map((line) => {
    const vals = _parseCSVLine(line);
    return Object.fromEntries(
      headers.map((h, i) => [
        h,
        (vals[i] ?? "").replace(/^"|"$/g, "") || null,
      ])
    );
  });

  return {
    is_valid:    errors.length === 0,
    file_type:   fileType,
    environment,
    row_count:   rowCount,
    warnings,
    errors,
    null_rates:  {},
    sample_rows: sampleRows,
  } as ValidationResult;
}

interface FileEntry {
  id: string;
  file: File;
  status: EntryStatus;
  validation: ValidationResult | null;
  result: UploadResult | null;
  error: string | null;
}

export default function UploadPage() {
  const [entries, setEntries] = useState<FileEntry[]>([]);
  const [activeTab, setActiveTab] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const [monthStats, setMonthStats] = useState<MonthTypeRow[]>([]);

  const refreshStats = useCallback(async () => {
    try {
      const data = await api.analytics.byMonthType();
      setMonthStats(data);
    } catch { /* silent — table stays stale on network failure */ }
  }, []);

  useEffect(() => { void refreshStats(); }, [refreshStats]);

  function updateEntry(id: string, patch: Partial<FileEntry>) {
    setEntries((prev) => prev.map((e) => (e.id === id ? { ...e, ...patch } : e)));
  }

  async function addFiles(fileList: FileList) {
    const newEntries: FileEntry[] = Array.from(fileList).map((file) => ({
      id: `${file.name}-${Date.now()}-${Math.random()}`,
      file,
      status: "validating" as EntryStatus,
      validation: null,
      result: null,
      error: null,
    }));

    setEntries((prev) => [...prev, ...newEntries]);
    setActiveTab((prev) => prev ?? newEntries[0]?.id ?? null);

    // Validate all files in parallel — purely client-side, no network call.
    await Promise.all(
      newEntries.map(async (entry) => {
        try {
          const v = await validateFileLocally(entry.file);
          updateEntry(entry.id, { status: v.is_valid ? "ready" : "error", validation: v,
            error: v.errors.length ? v.errors.join("; ") : null });
        } catch (err) {
          updateEntry(entry.id, { status: "error", error: String(err) });
        }
      })
    );
  }

  function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    if (!e.target.files?.length) return;
    addFiles(e.target.files);
    e.target.value = "";
  }

  function closeTab(id: string) {
    setEntries((prev) => {
      const next = prev.filter((e) => e.id !== id);
      setActiveTab((cur) => {
        if (cur !== id) return cur;
        if (next.length === 0) return null;
        const idx = prev.findIndex((e) => e.id === id);
        return next[Math.min(idx, next.length - 1)].id;
      });
      return next;
    });
  }

  async function handleIngestAll() {
    const toIngest = entries.filter((e) => e.status === "ready" && e.validation?.is_valid);
    for (const entry of toIngest) {
      updateEntry(entry.id, { status: "uploading" });
      try {
        const r = await api.upload(entry.file);
        updateEntry(entry.id, { status: "done", result: r });
      } catch (err) {
        updateEntry(entry.id, { status: "error", error: String(err) });
      }
    }
    void refreshStats();
  }

  const validReadyCount = entries.filter((e) => e.status === "ready" && e.validation?.is_valid).length;
  const anyBusy = entries.some((e) => e.status === "validating" || e.status === "uploading");
  const activeEntry = entries.find((e) => e.id === activeTab) ?? null;

  return (
    <div className="max-w-7xl space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Upload CSV</h1>
        <p className="text-sm text-slate-500 mt-1">
          Select one or more Splunk CSV files. Review each tab, close any you don&apos;t want, then ingest.
        </p>
      </div>

      {/* Drop zone + monthly stats — side by side */}
      <div className="flex flex-col lg:flex-row gap-6 items-start">
        {/* Drop zone */}
        <Card className="w-full lg:flex-1">
          <CardContent className="pt-5">
            <div
              className="flex flex-col items-center justify-center gap-3 rounded-lg border-2 border-dashed border-slate-300 p-10 text-center cursor-pointer hover:border-indigo-400 hover:bg-indigo-50/30 transition-colors"
              onClick={() => inputRef.current?.click()}
              onDragOver={(e) => e.preventDefault()}
              onDrop={(e) => { e.preventDefault(); if (e.dataTransfer.files.length) addFiles(e.dataTransfer.files); }}
            >
              <Upload className="h-8 w-8 text-slate-400" />
              <p className="text-sm text-slate-500">
                Click or drag CSV files here&nbsp;
                <span className="text-slate-400">(multiple allowed)</span>
              </p>
              <input
                ref={inputRef}
                type="file"
                accept=".csv"
                multiple
                className="hidden"
                onChange={handleFileChange}
              />
            </div>
          </CardContent>
        </Card>

        {/* Monthly stats table */}
        <Card className="w-full lg:w-[560px] shrink-0">
          <CardContent className="pt-4 pb-2 px-3">
            <h2 className="text-sm font-semibold text-slate-700 mb-2 flex items-center justify-between">
              Monthly Upload Stats
              <span className="text-xs text-slate-400 font-normal">(all time)</span>
            </h2>

            {/* Column legend */}
            <div className="mb-3 rounded bg-slate-50 border border-slate-100 px-3 py-2 text-xs text-slate-500 space-y-0.5">
              <p><span className="font-medium text-indigo-600">Blocker / Deadlock / Slow SQL / Slow Mongo</span> — CSV rows uploaded per query type for that month. These four columns always add up to <span className="font-medium">File Rows</span>.</p>
              <p><span className="font-medium text-slate-600">File Rows</span> — total rows across all uploaded CSV files (latest upload per file only, so re-uploads are not double-counted).</p>
              <p><span className="font-medium text-teal-600">SQL Patterns</span> — unique normalised SQL entries stored in the database. Higher than File Rows because each deadlock / blocker event is expanded into multiple individual SQL statements during ingestion.</p>
            </div>

            {monthStats.length === 0 ? (
              <p className="text-xs text-slate-400 py-6 text-center">No data yet</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full text-xs">
                  <thead>
                    <tr className="text-slate-500 border-b border-slate-100">
                      <th className="text-left py-1 pr-3 font-medium whitespace-nowrap">Month</th>
                      <th className="text-right py-1 px-2 font-medium">Blocker</th>
                      <th className="text-right py-1 px-2 font-medium">Deadlock</th>
                      <th className="text-right py-1 px-2 font-medium whitespace-nowrap">Slow SQL</th>
                      <th className="text-right py-1 px-2 font-medium whitespace-nowrap">Slow Mongo</th>
                      <th className="text-right py-1 px-2 font-medium whitespace-nowrap">File Rows</th>
                    </tr>
                  </thead>
                  <tbody>
                    {monthStats.map((row) => (
                      <tr key={row.month_year} className="border-b border-slate-50 hover:bg-slate-50 transition-colors">
                        <td className="py-1 pr-3 font-mono text-slate-600 whitespace-nowrap">{row.month_year}</td>
                        <td className="text-right py-1 px-2 tabular-nums text-indigo-600">{row.blocker.toLocaleString()}</td>
                        <td className="text-right py-1 px-2 tabular-nums text-orange-600">{row.deadlock.toLocaleString()}</td>
                        <td className="text-right py-1 px-2 tabular-nums text-blue-600">{row.slow_query.toLocaleString()}</td>
                        <td className="text-right py-1 px-2 tabular-nums text-purple-600">{row.slow_query_mongo.toLocaleString()}</td>
                        <td className="text-right py-1 px-2 tabular-nums text-slate-500">{row.total_file_rows != null ? row.total_file_rows.toLocaleString() : <span className="text-slate-300">—</span>}</td>
                      </tr>
                    ))}
                  </tbody>
                  <tfoot>
                    <tr className="border-t-2 border-slate-200 bg-slate-50 font-semibold">
                      <td className="py-1 pr-3 text-slate-500">Total</td>
                      <td className="text-right py-1 px-2 tabular-nums text-indigo-600">{monthStats.reduce((s, r) => s + r.blocker, 0).toLocaleString()}</td>
                      <td className="text-right py-1 px-2 tabular-nums text-orange-600">{monthStats.reduce((s, r) => s + r.deadlock, 0).toLocaleString()}</td>
                      <td className="text-right py-1 px-2 tabular-nums text-blue-600">{monthStats.reduce((s, r) => s + r.slow_query, 0).toLocaleString()}</td>
                      <td className="text-right py-1 px-2 tabular-nums text-purple-600">{monthStats.reduce((s, r) => s + r.slow_query_mongo, 0).toLocaleString()}</td>
                      <td className="text-right py-1 px-2 tabular-nums text-slate-500">{monthStats.some(r => r.total_file_rows != null) ? monthStats.reduce((s, r) => s + (r.total_file_rows ?? 0), 0).toLocaleString() : <span className="text-slate-300">—</span>}</td>
                    </tr>
                  </tfoot>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Tab panel — shown when any files are queued */}
      {entries.length > 0 && (
        <div>
          {/* Tab strip */}
          <div className="flex items-end gap-0.5 border-b border-slate-200 overflow-x-auto">
            {entries.map((entry) => {
              const isActive = entry.id === activeTab;
              const isInvalid =
                entry.status === "ready" && entry.validation && !entry.validation.is_valid;
              const icon =
                entry.status === "validating" || entry.status === "uploading" ? (
                  <Spinner className="h-3 w-3 shrink-0" />
                ) : entry.status === "done" ? (
                  <CheckCircle className="h-3 w-3 shrink-0 text-teal-500" />
                ) : entry.status === "error" || isInvalid ? (
                  <XCircle className="h-3 w-3 shrink-0 text-red-400" />
                ) : (
                  <CheckCircle className="h-3 w-3 shrink-0 text-green-500" />
                );
              return (
                <div
                  key={entry.id}
                  className={`flex items-center gap-1.5 px-3 py-2 text-xs font-medium rounded-t border border-b-0 cursor-pointer whitespace-nowrap select-none transition-colors ${
                    isActive
                      ? "bg-white border-slate-200 text-slate-800 shadow-sm -mb-px z-10"
                      : "bg-slate-50 border-transparent text-slate-500 hover:bg-slate-100"
                  }`}
                  onClick={() => setActiveTab(entry.id)}
                >
                  {icon}
                  <span className="max-w-[160px] truncate" title={entry.file.name}>
                    {entry.file.name}
                  </span>
                  <button
                    className="ml-1 rounded p-0.5 opacity-40 hover:opacity-100 hover:bg-slate-200 transition-opacity"
                    title="Remove this file"
                    onClick={(e) => { e.stopPropagation(); closeTab(entry.id); }}
                  >
                    <X className="h-3 w-3" />
                  </button>
                </div>
              );
            })}
          </div>

          {/* Active tab content */}
          {activeEntry && (
            <div className="border border-t-0 border-slate-200 rounded-b bg-white p-4">
              {/* Validating / Uploading spinner */}
              {(activeEntry.status === "validating" || activeEntry.status === "uploading") && (
                <div className="flex items-center justify-center gap-2 py-10 text-slate-400 text-sm">
                  <Spinner />
                  {activeEntry.status === "validating" ? "Validating…" : "Ingesting…"}
                </div>
              )}

              {/* Error */}
              {activeEntry.status === "error" && (
                <p className="text-sm text-red-600 py-4">{activeEntry.error}</p>
              )}

              {/* Done */}
              {activeEntry.status === "done" && activeEntry.result && (
                <div>
                  <div className="flex items-center gap-2 mb-4">
                    <CheckCircle className="h-4 w-4 text-green-500" />
                    <span className="text-sm font-medium text-green-700">Ingest complete</span>
                    <span className="text-xs text-slate-400 font-mono ml-1">{activeEntry.result.filename}</span>
                  </div>
                  {/* Row count pill */}
                  <p className="text-xs text-slate-500 mb-3">
                    <span className="font-semibold text-slate-700">{activeEntry.result.row_count.toLocaleString()}</span> rows found in file
                  </p>
                  {/* raw_query stats */}
                  <p className="text-xs font-medium text-slate-400 uppercase tracking-wide mb-1">raw_query</p>
                  <dl className="grid grid-cols-3 gap-4 text-center text-sm mb-4">
                    <div>
                      <dt className="text-slate-400 text-xs">Inserted</dt>
                      <dd className="text-2xl font-bold text-indigo-600">
                        {activeEntry.result.inserted.toLocaleString()}
                      </dd>
                    </div>
                    <div>
                      <dt className="text-slate-400 text-xs">Updated</dt>
                      <dd className="text-2xl font-bold text-teal-600">
                        {activeEntry.result.updated.toLocaleString()}
                      </dd>
                    </div>
                    <div>
                      <dt className="text-slate-400 text-xs">Skipped</dt>
                      <dd className={`text-2xl font-bold ${
                        activeEntry.result.skipped > 0 ? "text-red-500" : "text-slate-400"
                      }`}>
                        {activeEntry.result.skipped.toLocaleString()}
                      </dd>
                    </div>
                  </dl>
                  {/* typed table stats */}
                  <p className="text-xs font-medium text-slate-400 uppercase tracking-wide mb-1">typed table</p>
                  <dl className="grid grid-cols-2 gap-4 text-center text-sm mb-4">
                    <div>
                      <dt className="text-slate-400 text-xs">Inserted</dt>
                      <dd className="text-2xl font-bold text-indigo-400">
                        {(activeEntry.result.typed_inserted ?? 0).toLocaleString()}
                      </dd>
                    </div>
                    <div>
                      <dt className="text-slate-400 text-xs">Updated</dt>
                      <dd className="text-2xl font-bold text-teal-400">
                        {(activeEntry.result.typed_updated ?? 0).toLocaleString()}
                      </dd>
                    </div>
                  </dl>
                  {activeEntry.result.warnings?.length > 0 && (
                    <ul className="mt-3 space-y-1">
                      {activeEntry.result.warnings.map((w, i) => (
                        <li key={i} className="text-xs text-amber-700 bg-amber-50 rounded px-2 py-1">{w}</li>
                      ))}
                    </ul>
                  )}
                  {activeEntry.result.errors?.length > 0 && (
                    <div className="mt-3">
                      <p className="text-xs font-medium text-red-600 flex items-center gap-1 mb-1">
                        <XCircle className="h-3 w-3" /> {activeEntry.result.errors.length} row{activeEntry.result.errors.length !== 1 ? "s" : ""} skipped — reasons:
                      </p>
                      <ul className="space-y-1 max-h-48 overflow-y-auto">
                        {activeEntry.result.errors.map((e, i) => (
                          <li key={i} className="text-xs text-red-700 bg-red-50 rounded px-2 py-1 font-mono break-all">{e}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              )}

              {/* Ready — validation details + data preview */}
              {activeEntry.status === "ready" && activeEntry.validation && (() => {
                const v = activeEntry.validation;
                return (
                  <>
                    <dl className="grid grid-cols-3 gap-x-6 gap-y-2 text-sm mb-4">
                      <div>
                        <dt className="text-slate-400 text-xs">Type</dt>
                        <dd className="font-medium">{v.file_type}</dd>
                      </div>
                      <div>
                        <dt className="text-slate-400 text-xs">Environment</dt>
                        <dd>
                          <Badge variant={v.environment === "prod" ? "prod" : "sat"}>
                            {v.environment}
                          </Badge>
                        </dd>
                      </div>
                      <div>
                        <dt className="text-slate-400 text-xs">Rows</dt>
                        <dd className="font-medium">{v.row_count.toLocaleString()}</dd>
                      </div>
                    </dl>

                    {!v.is_valid && (
                      <div className="mb-3 flex items-center gap-2 text-xs font-medium text-red-600">
                        <XCircle className="h-4 w-4" />
                        This file has errors and cannot be ingested.
                      </div>
                    )}

                    {v.warnings.length > 0 && (
                      <div className="mb-3">
                        <p className="text-xs font-medium text-amber-600 flex items-center gap-1 mb-1">
                          <AlertTriangle className="h-3 w-3" /> Warnings
                        </p>
                        <ul className="space-y-1">
                          {v.warnings.map((w, i) => (
                            <li key={i} className="text-xs text-amber-700 bg-amber-50 rounded px-2 py-1">{w}</li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {v.errors.length > 0 && (
                      <div className="mb-3">
                        <p className="text-xs font-medium text-red-600 flex items-center gap-1 mb-1">
                          <XCircle className="h-3 w-3" /> Errors
                        </p>
                        <ul className="space-y-1">
                          {v.errors.map((e, i) => (
                            <li key={i} className="text-xs text-red-700 bg-red-50 rounded px-2 py-1">{e}</li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {Object.keys(v.null_rates).length > 0 && (
                      <div className="mb-4">
                        <p className="text-xs font-medium text-slate-500 mb-1">Null rates (required columns)</p>
                        <div className="grid grid-cols-2 gap-1">
                          {Object.entries(v.null_rates).map(([col, rate]) => (
                            <div key={col} className="flex justify-between text-xs">
                              <span className="text-slate-500">{col}</span>
                              <span className={rate > 0.1 ? "text-red-600" : "text-slate-700"}>
                                {(rate * 100).toFixed(1)}%
                              </span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {v.sample_rows && v.sample_rows.length > 0 && (
                      <div>
                        <p className="text-xs font-medium text-slate-500 mb-1">
                          Data preview (first {v.sample_rows.length} rows)
                        </p>
                        <div className="overflow-x-auto rounded border border-slate-200 max-h-72 overflow-y-auto">
                          <table className="min-w-full text-xs font-mono">
                            <thead className="bg-slate-100 sticky top-0 z-10">
                              <tr>
                                {Object.keys(v.sample_rows[0]).map((col) => (
                                  <th
                                    key={col}
                                    className="px-2 py-1 text-left text-slate-600 font-semibold whitespace-nowrap border-r border-slate-200 last:border-r-0"
                                  >
                                    {col}
                                  </th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {v.sample_rows.map((row, ri) => (
                                <tr key={ri} className={ri % 2 === 0 ? "bg-white" : "bg-slate-50"}>
                                  {Object.values(row).map((val, ci) => (
                                    <td
                                      key={ci}
                                      className="px-2 py-0.5 text-slate-700 whitespace-nowrap border-r border-slate-100 last:border-r-0 max-w-[220px] truncate"
                                      title={val != null ? String(val) : ""}
                                    >
                                      {val != null ? String(val) : (
                                        <span className="text-slate-300">null</span>
                                      )}
                                    </td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </>
                );
              })()}
            </div>
          )}

          {/* Summary across all done files */}
          {entries.some((e) => e.status === "done" && e.result) && (() => {
            const doneEntries = entries.filter((e) => e.status === "done" && e.result);
            const totals = {
              files:          doneEntries.length,
              rows:           doneEntries.reduce((s, e) => s + (e.result?.row_count      ?? 0), 0),
              inserted:       doneEntries.reduce((s, e) => s + (e.result?.inserted       ?? 0), 0),
              updated:        doneEntries.reduce((s, e) => s + (e.result?.updated        ?? 0), 0),
              typedInserted:  doneEntries.reduce((s, e) => s + (e.result?.typed_inserted ?? 0), 0),
              typedUpdated:   doneEntries.reduce((s, e) => s + (e.result?.typed_updated  ?? 0), 0),
            };
            return (
              <div className="mt-3 rounded-lg border border-indigo-100 bg-indigo-50/60 px-4 py-3">
                <p className="text-xs font-semibold text-indigo-700 mb-2">
                  Upload Summary — {totals.files} file{totals.files !== 1 ? "s" : ""} · {totals.rows.toLocaleString()} rows total
                </p>
                <div className="grid grid-cols-4 gap-3 text-center text-xs">
                  <div>
                    <p className="text-slate-400">Rows Found</p>
                    <p className="font-bold text-slate-700 text-base">{totals.rows.toLocaleString()}</p>
                  </div>
                  <div>
                    <p className="text-slate-400">Inserted</p>
                    <p className="font-bold text-indigo-600 text-base">{totals.inserted.toLocaleString()}</p>
                  </div>
                  <div>
                    <p className="text-slate-400">Updated</p>
                    <p className="font-bold text-teal-600 text-base">{totals.updated.toLocaleString()}</p>
                  </div>
                  <div>
                    <p className="text-slate-400">Typed (ins/upd)</p>
                    <p className="font-bold text-slate-600 text-base">
                      {totals.typedInserted.toLocaleString()} / {totals.typedUpdated.toLocaleString()}
                    </p>
                  </div>
                </div>
              </div>
            );
          })()}

          {/* Action bar */}
          <div className="flex items-center justify-between mt-4">
            <p className="text-xs text-slate-500">
              {validReadyCount > 0
                ? `${validReadyCount} valid file${validReadyCount !== 1 ? "s" : ""} ready to ingest`
                : entries.every((e) => e.status === "done")
                ? "All files ingested."
                : "No valid files ready."}
            </p>
            <div className="flex gap-3">
              <Button
                variant="outline"
                onClick={() => { setEntries([]); setActiveTab(null); }}
                disabled={anyBusy}
              >
                Clear all
              </Button>
              <Button
                onClick={handleIngestAll}
                disabled={validReadyCount === 0 || anyBusy}
              >
                {anyBusy ? (
                  <span className="flex items-center gap-2">
                    <Spinner className="h-4 w-4" /> Working…
                  </span>
                ) : (
                  `Ingest ${validReadyCount > 1 ? `all ${validReadyCount} files` : "file"}`
                )}
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
