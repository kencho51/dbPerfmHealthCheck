"use client";

import { useEffect, useState } from "react";
import { Plus, Pencil, Trash2, Check, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { api, type PatternLabel, type SeverityType } from "@/lib/api";

// ---- constants --------------------------------------------------------------

const SEV_OPTIONS: SeverityType[] = ["critical", "warning", "info"];

const SEV_COLORS: Record<SeverityType, string> = {
  critical: "text-red-600 bg-red-50 border-red-200",
  warning:  "text-amber-600 bg-amber-50 border-amber-200",
  info:     "text-indigo-600 bg-indigo-50 border-indigo-200",
};

// ---- inline edit / create row -----------------------------------------------

interface EditRowProps {
  label?:    PatternLabel; // undefined = new row
  onSave:    (data: { name: string; severity: SeverityType; description: string | null }) => Promise<void>;
  onCancel:  () => void;
}

function EditRow({ label, onSave, onCancel }: EditRowProps) {
  const [name,        setName]        = useState(label?.name ?? "");
  const [severity,    setSeverity]    = useState<SeverityType>(label?.severity ?? "warning");
  const [description, setDescription] = useState(label?.description ?? "");
  const [saving,      setSaving]      = useState(false);
  const [error,       setError]       = useState("");

  const handleSave = async () => {
    if (!name.trim()) { setError("Name is required"); return; }
    setSaving(true);
    setError("");
    try {
      await onSave({ name: name.trim(), severity, description: description.trim() || null });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter")  handleSave();
    if (e.key === "Escape") onCancel();
  };

  return (
    <tr className="bg-indigo-50/60">
      <td className="px-4 py-2">
        <input
          autoFocus
          className="w-full rounded border border-slate-300 px-2 py-1 text-sm focus:outline-none focus:border-indigo-400"
          placeholder="Label name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          onKeyDown={onKeyDown}
        />
        {error && <p className="mt-1 text-xs text-red-500">{error}</p>}
      </td>

      <td className="px-4 py-2">
        <select
          className="rounded border border-slate-300 px-2 py-1 text-sm focus:outline-none focus:border-indigo-400"
          value={severity}
          onChange={(e) => setSeverity(e.target.value as SeverityType)}
        >
          {SEV_OPTIONS.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
      </td>

      <td className="px-4 py-2">
        <input
          className="w-full rounded border border-slate-300 px-2 py-1 text-sm focus:outline-none focus:border-indigo-400"
          placeholder="Description (optional)"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          onKeyDown={onKeyDown}
        />
      </td>

      <td className="px-4 py-2">
        <span className="inline-flex gap-1 justify-end w-full">
          <Button size="sm" className="h-7 text-xs" onClick={handleSave} disabled={saving}>
            {saving ? <Spinner /> : <Check className="h-3 w-3" />}
          </Button>
          <Button variant="outline" size="sm" className="h-7 text-xs" onClick={onCancel} disabled={saving}>
            <X className="h-3 w-3" />
          </Button>
        </span>
      </td>
    </tr>
  );
}

// ---- main page --------------------------------------------------------------

export default function LabelsPage() {
  const [labels,     setLabels]     = useState<PatternLabel[]>([]);
  const [loading,    setLoading]    = useState(true);
  const [loadError,  setLoadError]  = useState("");
  const [editingId,  setEditingId]  = useState<number | null>(null);
  const [showNewRow, setShowNewRow] = useState(false);
  const [deletingId, setDeletingId] = useState<number | null>(null);
  const [deleteErrors, setDeleteErrors] = useState<Record<number, string>>({});

  const load = async () => {
    setLoading(true);
    try {
      setLabels(await api.labels.list());
    } catch {
      setLoadError("Failed to load labels");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  // ---- handlers -------------------------------------------------------------

  const handleCreate = async (data: { name: string; severity: SeverityType; description: string | null }) => {
    const created = await api.labels.create(data);
    setLabels((prev) => [...prev, created].sort((a, b) => a.name.localeCompare(b.name)));
    setShowNewRow(false);
  };

  const handleUpdate = async (id: number, data: { name: string; severity: SeverityType; description: string | null }) => {
    const updated = await api.labels.patch(id, data);
    setLabels((prev) => prev.map((l) => (l.id === id ? updated : l)));
    setEditingId(null);
  };

  const handleDelete = async (id: number) => {
    setDeletingId(id);
    setDeleteErrors((prev) => ({ ...prev, [id]: "" }));
    try {
      await api.labels.delete(id);
      setLabels((prev) => prev.filter((l) => l.id !== id));
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      const friendly = msg.includes("409")
        ? "Cannot delete — label is still used by curated queries"
        : msg;
      setDeleteErrors((prev) => ({ ...prev, [id]: friendly }));
    } finally {
      setDeletingId(null);
    }
  };

  // ---- render ---------------------------------------------------------------

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Labels</h1>
          <p className="text-sm text-slate-500 mt-1">
            Manage labels used to categorise curated queries
          </p>
        </div>
        <Button
          onClick={() => { setShowNewRow(true); setEditingId(null); }}
          disabled={showNewRow}
        >
          <Plus className="h-4 w-4 mr-1.5" />
          New Label
        </Button>
      </div>

      {/* Table */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-slate-400">
          <Spinner /> Loading labels…
        </div>
      ) : loadError ? (
        <p className="text-sm text-red-500">{loadError}</p>
      ) : (
        <div className="rounded-lg border border-slate-200 bg-white overflow-hidden shadow-sm">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-100 bg-slate-50 text-xs text-slate-500 uppercase tracking-wider">
                <th className="px-4 py-3 text-left font-medium w-48">Name</th>
                <th className="px-4 py-3 text-left font-medium w-32">Severity</th>
                <th className="px-4 py-3 text-left font-medium">Description</th>
                <th className="px-4 py-3 text-right font-medium w-32">Actions</th>
              </tr>
            </thead>

            <tbody className="divide-y divide-slate-100">
              {/* New label row — always at top */}
              {showNewRow && (
                <EditRow
                  onSave={handleCreate}
                  onCancel={() => setShowNewRow(false)}
                />
              )}

              {/* Empty state */}
              {labels.length === 0 && !showNewRow && (
                <tr>
                  <td colSpan={4} className="px-4 py-10 text-center text-sm text-slate-400">
                    No labels yet.{" "}
                    <button
                      className="text-indigo-500 hover:underline"
                      onClick={() => setShowNewRow(true)}
                    >
                      Create the first one
                    </button>
                    .
                  </td>
                </tr>
              )}

              {/* Label rows */}
              {labels.map((label) =>
                editingId === label.id ? (
                  <EditRow
                    key={label.id}
                    label={label}
                    onSave={(data) => handleUpdate(label.id, data)}
                    onCancel={() => setEditingId(null)}
                  />
                ) : (
                  <tr key={label.id} className="hover:bg-slate-50/70 group">
                    <td className="px-4 py-3 font-medium text-slate-800">
                      {label.name}
                    </td>

                    <td className="px-4 py-3">
                      <span
                        className={`inline-block text-xs font-medium rounded border px-1.5 py-0.5 ${
                          SEV_COLORS[label.severity] ?? ""
                        }`}
                      >
                        {label.severity}
                      </span>
                    </td>

                    <td className="px-4 py-3 text-slate-500 max-w-xs truncate">
                      {label.description ?? (
                        <span className="italic text-slate-300">—</span>
                      )}
                    </td>

                    <td className="px-4 py-3">
                      <div className="flex justify-end gap-1">
                        <span className="inline-flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                          <Button
                            variant="outline"
                            size="sm"
                            className="h-7 text-xs"
                            onClick={() => {
                              setEditingId(label.id);
                              setShowNewRow(false);
                              setDeleteErrors((p) => ({ ...p, [label.id]: "" }));
                            }}
                          >
                            <Pencil className="h-3 w-3 mr-1" />
                            Edit
                          </Button>

                          <Button
                            variant="outline"
                            size="sm"
                            className="h-7 text-xs text-red-600 hover:bg-red-50 hover:border-red-200"
                            onClick={() => handleDelete(label.id)}
                            disabled={deletingId === label.id}
                          >
                            {deletingId === label.id ? (
                              <Spinner />
                            ) : (
                              <Trash2 className="h-3 w-3" />
                            )}
                          </Button>
                        </span>
                      </div>
                      {deleteErrors[label.id] && (
                        <p className="mt-1 text-xs text-red-500 text-right">
                          {deleteErrors[label.id]}
                        </p>
                      )}
                    </td>
                  </tr>
                )
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
