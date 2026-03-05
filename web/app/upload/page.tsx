"use client";

import { useState, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { Spinner } from "@/components/ui/spinner";
import { api, type ValidationResult, type UploadResult } from "@/lib/api";
import { CheckCircle, XCircle, Upload, AlertTriangle, X } from "lucide-react";

type EntryStatus = "validating" | "ready" | "uploading" | "done" | "error";

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

    await Promise.all(
      newEntries.map(async (entry) => {
        try {
          const v = await api.validate(entry.file);
          updateEntry(entry.id, { status: "ready", validation: v });
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
  }

  const validReadyCount = entries.filter((e) => e.status === "ready" && e.validation?.is_valid).length;
  const anyBusy = entries.some((e) => e.status === "validating" || e.status === "uploading");
  const activeEntry = entries.find((e) => e.id === activeTab) ?? null;

  return (
    <div className="max-w-5xl space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Upload CSV</h1>
        <p className="text-sm text-slate-500 mt-1">
          Select one or more Splunk CSV files. Review each tab, close any you don&apos;t want, then ingest.
        </p>
      </div>

      {/* Drop zone — always visible */}
      <Card>
        <CardContent className="pt-5">
          <div
            className="flex flex-col items-center justify-center gap-3 rounded-lg border-2 border-dashed border-slate-300 p-10 text-center cursor-pointer hover:border-indigo-400 hover:bg-indigo-50/30 transition-colors"
            onClick={() => inputRef.current?.click()}
          >
            <Upload className="h-8 w-8 text-slate-400" />
            <p className="text-sm text-slate-500">
              Click to select CSV files&nbsp;
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
                  <dl className="grid grid-cols-3 gap-4 text-center text-sm">
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
                      <dd className="text-2xl font-bold text-slate-400">
                        {activeEntry.result.skipped.toLocaleString()}
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
