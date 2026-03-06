"use client";

import { useEffect, useRef, useState } from "react";
import { Plus, X, ChevronUp, ChevronDown, ChevronsUpDown, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { api, type PatternLabel, type SeverityType, type LabelSourceType } from "@/lib/api";

// ---- constants --------------------------------------------------------------

const SEV_OPTIONS: SeverityType[] = ["critical", "warning", "info"];

const SEV_COLORS: Record<SeverityType, string> = {
  critical: "text-red-600 bg-red-50 border-red-200",
  warning:  "text-amber-600 bg-amber-50 border-amber-200",
  info:     "text-indigo-600 bg-indigo-50 border-indigo-200",
};

const SEV_RANK: Record<SeverityType, number> = { critical: 0, warning: 1, info: 2 };

const SRC_OPTIONS: LabelSourceType[] = ["sql", "mongodb", "both"];

const SRC_COLORS: Record<LabelSourceType, string> = {
  sql:     "text-sky-700 bg-sky-50 border-sky-200",
  mongodb: "text-emerald-700 bg-emerald-50 border-emerald-200",
  both:    "text-slate-600 bg-slate-50 border-slate-200",
};

const SRC_RANK: Record<LabelSourceType, number> = { sql: 0, mongodb: 1, both: 2 };

type SortKey = "name" | "severity" | "source" | "description";
type SortDir = "asc" | "desc";

function sortLabels(labels: PatternLabel[], key: SortKey, dir: SortDir): PatternLabel[] {
  return [...labels].sort((a, b) => {
    let cmp = 0;
    if (key === "severity") {
      cmp = SEV_RANK[a.severity] - SEV_RANK[b.severity];
    } else if (key === "source") {
      cmp = SRC_RANK[a.source] - SRC_RANK[b.source];
    } else if (key === "description") {
      cmp = (a.description ?? "").localeCompare(b.description ?? "");
    } else {
      cmp = a.name.localeCompare(b.name);
    }
    return dir === "asc" ? cmp : -cmp;
  });
}

function SortHeader({
  label, sortKey, active, dir, onSort,
}: {
  label: string;
  sortKey: SortKey;
  active: boolean;
  dir: SortDir;
  onSort: (k: SortKey) => void;
}) {
  const Icon = active ? (dir === "asc" ? ChevronUp : ChevronDown) : ChevronsUpDown;
  return (
    <button
      className="inline-flex items-center gap-1 hover:text-slate-800 transition-colors"
      onClick={() => onSort(sortKey)}
    >
      {label}
      <Icon className={`h-3 w-3 ${active ? "text-indigo-500" : "text-slate-300"}`} />
    </button>
  );
}

// ---- right-side panel -------------------------------------------------------

function LabelPanel({
  label,
  onSave,
  onDelete,
  onClose,
}: {
  label:    PatternLabel | null;
  onSave:   (data: { name: string; severity: SeverityType; source: LabelSourceType; description: string | null }) => Promise<void>;
  onDelete: ((id: number) => Promise<void>) | null;
  onClose:  () => void;
}) {
  const panelRef = useRef<HTMLDivElement>(null);

  const [name,        setName]        = useState(label?.name ?? "");
  const [severity,    setSeverity]    = useState<SeverityType>(label?.severity ?? "warning");
  const [source,      setSource]      = useState<LabelSourceType>(label?.source ?? "both");
  const [description, setDescription] = useState(label?.description ?? "");
  const [saving,      setSaving]      = useState(false);
  const [deleting,    setDeleting]    = useState(false);
  const [confirmDel,  setConfirmDel]  = useState(false);
  const [error,       setError]       = useState("");

  // Reset fields when switching to a different label
  useEffect(() => {
    setName(label?.name ?? "");
    setSeverity(label?.severity ?? "warning");
    setSource(label?.source ?? "both");
    setDescription(label?.description ?? "");
    setError("");
    setConfirmDel(false);
  }, [label?.id]); // eslint-disable-line react-hooks/exhaustive-deps

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [onClose]);

  // Close on backdrop click
  const onBackdrop = (e: React.MouseEvent) => {
    if (panelRef.current && !panelRef.current.contains(e.target as Node)) onClose();
  };

  const handleSave = async () => {
    if (!name.trim()) { setError("Name is required"); return; }
    setSaving(true);
    setError("");
    try {
      await onSave({ name: name.trim(), severity, source, description: description.trim() || null });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!label || !onDelete) return;
    setDeleting(true);
    setError("");
    try {
      await onDelete(label.id);
      onClose();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg.includes("409") ? "Cannot delete  label is still used by curated queries" : msg);
    } finally {
      setDeleting(false);
      setConfirmDel(false);
    }
  };

  const isNew = label === null;

  return (
    <div
      className="fixed inset-0 z-50 flex justify-end bg-black/20 backdrop-blur-[1px]"
      onClick={onBackdrop}
    >
      <div
        ref={panelRef}
        className="relative h-full w-full max-w-sm bg-white shadow-2xl flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-200 px-5 py-4 shrink-0">
          <h2 className="text-sm font-semibold text-slate-800">
            {isNew ? "New Label" : "Edit Label"}
          </h2>
          <button onClick={onClose} className="text-slate-400 hover:text-slate-700">
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-5 py-5 space-y-4">
          {/* Name */}
          <div className="space-y-1.5">
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">
              Name <span className="text-red-400">*</span>
            </label>
            <input
              autoFocus
              className="w-full rounded border border-slate-300 px-3 py-2 text-sm focus:outline-none focus:border-indigo-400"
              placeholder="e.g. Full Table Scan"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </div>

          {/* Severity */}
          <div className="space-y-1.5">
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">Severity</label>
            <div className="flex gap-2">
              {SEV_OPTIONS.map((s) => (
                <button
                  key={s}
                  onClick={() => setSeverity(s)}
                  className={`flex-1 rounded border px-3 py-1.5 text-xs font-medium transition-colors ${
                    severity === s
                      ? SEV_COLORS[s]
                      : "border-slate-200 text-slate-400 hover:border-slate-300 hover:text-slate-600"
                  }`}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>

          {/* Source */}
          <div className="space-y-1.5">
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">Source</label>
            <div className="flex gap-2">
              {SRC_OPTIONS.map((s) => (
                <button
                  key={s}
                  onClick={() => setSource(s)}
                  className={`flex-1 rounded border px-3 py-1.5 text-xs font-medium transition-colors ${
                    source === s
                      ? SRC_COLORS[s]
                      : "border-slate-200 text-slate-400 hover:border-slate-300 hover:text-slate-600"
                  }`}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>

          {/* Description */}
          <div className="space-y-1.5">
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">Description</label>
            <textarea
              className="w-full rounded border border-slate-300 px-3 py-2 text-sm focus:outline-none focus:border-indigo-400 resize-y leading-relaxed"
              rows={6}
              placeholder="Describe symptoms, root cause, and fix"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
            />
          </div>

          {error && (
            <p className="text-xs text-red-500 rounded bg-red-50 border border-red-200 px-3 py-2">
              {error}
            </p>
          )}
        </div>

        {/* Footer */}
        <div className="border-t border-slate-100 px-5 py-4 shrink-0 space-y-3">
          <div className="flex gap-2">
            <Button className="flex-1" onClick={handleSave} disabled={saving || deleting}>
              {saving ? <Spinner /> : null}
              {isNew ? "Create Label" : "Save Changes"}
            </Button>
            <Button variant="outline" onClick={onClose} disabled={saving || deleting}>
              Cancel
            </Button>
          </div>

          {/* Delete  only for existing labels */}
          {!isNew && onDelete && (
            confirmDel ? (
              <div className="flex items-center gap-2 justify-end">
                <span className="text-xs text-slate-500">Remove this label?</span>
                <Button
                  size="sm"
                  className="h-7 text-xs bg-red-500 hover:bg-red-600 text-white border-0"
                  onClick={handleDelete}
                  disabled={deleting}
                >
                  {deleting ? <Spinner /> : "Yes, delete"}
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  className="h-7 text-xs"
                  onClick={() => setConfirmDel(false)}
                  disabled={deleting}
                >
                  Cancel
                </Button>
              </div>
            ) : (
              <button
                className="flex items-center gap-1.5 text-xs text-red-500 hover:text-red-700 w-full justify-end"
                onClick={() => setConfirmDel(true)}
              >
                <Trash2 className="h-3 w-3" />
                Delete label
              </button>
            )
          )}
        </div>
      </div>
    </div>
  );
}

// ---- main page --------------------------------------------------------------

export default function LabelsPage() {
  const [labels,    setLabels]    = useState<PatternLabel[]>([]);
  const [loading,   setLoading]   = useState(true);
  const [loadError, setLoadError] = useState("");
  const [selected,  setSelected]  = useState<PatternLabel | "new" | null>(null);
  const [sortKey,   setSortKey]   = useState<SortKey>("name");
  const [sortDir,   setSortDir]   = useState<SortDir>("asc");

  const handleSort = (key: SortKey) => {
    if (key === sortKey) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortKey(key); setSortDir("asc"); }
  };

  const sortedLabels = sortLabels(labels, sortKey, sortDir);

  const load = async () => {
    setLoading(true);
    try { setLabels(await api.labels.list()); }
    catch { setLoadError("Failed to load labels"); }
    finally { setLoading(false); }
  };

  useEffect(() => { load(); }, []);

  // ---- panel handlers -------------------------------------------------------

  const handleSave = async (data: { name: string; severity: SeverityType; source: LabelSourceType; description: string | null }) => {
    if (selected === "new") {
      const created = await api.labels.create(data);
      setLabels((prev) => [...prev, created].sort((a, b) => a.name.localeCompare(b.name)));
      setSelected(null);
    } else if (selected) {
      const updated = await api.labels.patch(selected.id, data);
      setLabels((prev) => prev.map((l) => (l.id === updated.id ? updated : l)));
      setSelected(updated);
    }
  };

  const handleDelete = async (id: number) => {
    await api.labels.delete(id);
    setLabels((prev) => prev.filter((l) => l.id !== id));
  };

  // ---- render ---------------------------------------------------------------

  const panelLabel = selected === "new" ? null : selected;
  const panelOpen  = selected !== null;

  return (
    <div className="max-w-6xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Labels</h1>
          <p className="text-sm text-slate-500 mt-1">
            Click a row to edit  {labels.length} label{labels.length !== 1 ? "s" : ""}
          </p>
        </div>
        <Button onClick={() => setSelected("new")}>
          <Plus className="h-4 w-4 mr-1.5" />
          New Label
        </Button>
      </div>

      {/* Table */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-slate-400"><Spinner /> Loading labels</div>
      ) : loadError ? (
        <p className="text-sm text-red-500">{loadError}</p>
      ) : (
        <div className="rounded-lg border border-slate-200 bg-white overflow-hidden shadow-sm">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-100 bg-slate-50 text-xs text-slate-500 uppercase tracking-wider">
                <th className="px-4 py-3 text-left font-medium w-52">
                  <SortHeader label="Name" sortKey="name" active={sortKey === "name"} dir={sortDir} onSort={handleSort} />
                </th>
                <th className="px-4 py-3 text-left font-medium w-28">
                  <SortHeader label="Severity" sortKey="severity" active={sortKey === "severity"} dir={sortDir} onSort={handleSort} />
                </th>
                <th className="px-4 py-3 text-left font-medium w-28">
                  <SortHeader label="Source" sortKey="source" active={sortKey === "source"} dir={sortDir} onSort={handleSort} />
                </th>
                <th className="px-4 py-3 text-left font-medium">
                  <SortHeader label="Description" sortKey="description" active={sortKey === "description"} dir={sortDir} onSort={handleSort} />
                </th>
              </tr>
            </thead>

            <tbody className="divide-y divide-slate-100">
              {labels.length === 0 ? (
                <tr>
                  <td colSpan={4} className="px-4 py-10 text-center text-sm text-slate-400">
                    No labels yet.{" "}
                    <button className="text-indigo-500 hover:underline" onClick={() => setSelected("new")}>
                      Create the first one
                    </button>.
                  </td>
                </tr>
              ) : (
                sortedLabels.map((label) => {
                  const isActive =
                    typeof selected === "object" && selected !== null && selected.id === label.id;
                  return (
                    <tr
                      key={label.id}
                      className={`cursor-pointer transition-colors ${
                        isActive
                          ? "bg-indigo-50 border-l-2 border-indigo-400"
                          : "hover:bg-slate-50"
                      }`}
                      onClick={() => setSelected(label)}
                    >
                      <td className="px-4 py-3 font-medium text-slate-800">{label.name}</td>

                      <td className="px-4 py-3">
                        <span className={`inline-block text-xs font-medium rounded border px-1.5 py-0.5 ${SEV_COLORS[label.severity] ?? ""}`}>
                          {label.severity}
                        </span>
                      </td>

                      <td className="px-4 py-3">
                        <span className={`inline-block text-xs font-medium rounded border px-1.5 py-0.5 ${SRC_COLORS[label.source] ?? ""}`}>
                          {label.source}
                        </span>
                      </td>

                      <td className="px-4 py-3 text-slate-500 whitespace-normal break-words leading-relaxed">
                        {label.description ?? <span className="italic text-slate-300"></span>}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Right panel */}
      {panelOpen && (
        <LabelPanel
          label={panelLabel}
          onSave={handleSave}
          onDelete={panelLabel ? handleDelete : null}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  );
}
