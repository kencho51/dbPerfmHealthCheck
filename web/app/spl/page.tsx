"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Check, ChevronDown, Clipboard, ClipboardCheck, Code2, Pencil, Plus, Trash2, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { api, type SplQueryEntry, type SplQueryCreate } from "@/lib/api";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ENV_OPTIONS = ["both", "prod", "sat"] as const;
type EnvOption = typeof ENV_OPTIONS[number];

const ENV_COLORS: Record<EnvOption, string> = {
  both: "text-slate-600 bg-slate-50 border-slate-200",
  prod: "text-emerald-700 bg-emerald-50 border-emerald-200",
  sat:  "text-amber-700 bg-amber-50 border-amber-200",
};

// ---------------------------------------------------------------------------
// CopyButton — clipboard mini-component
// ---------------------------------------------------------------------------

function CopyButton({ text, className = "" }: { text: string; className?: string }) {
  const [copied, setCopied] = useState(false);

  const copy = (e: React.MouseEvent) => {
    e.stopPropagation();
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <button
      onClick={copy}
      title={copied ? "Copied!" : "Copy SPL"}
      className={`inline-flex items-center gap-1 rounded px-2 py-1 text-xs font-medium transition-colors
        ${copied
          ? "bg-emerald-50 text-emerald-700 border border-emerald-200"
          : "bg-slate-50 text-slate-500 border border-slate-200 hover:bg-slate-100 hover:text-slate-700"
        } ${className}`}
    >
      {copied ? <ClipboardCheck className="h-3 w-3" /> : <Clipboard className="h-3 w-3" />}
      {copied ? "Copied" : "Copy SPL"}
    </button>
  );
}

// ---------------------------------------------------------------------------
// TypeCombobox — free-form input with dropdown of existing types
// ---------------------------------------------------------------------------

function TypeCombobox({
  value,
  onChange,
  suggestions,
}: {
  value: string;
  onChange: (v: string) => void;
  suggestions: string[];
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const filtered = suggestions.filter((s) =>
    s.toLowerCase().includes(value.toLowerCase())
  );

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  return (
    <div ref={ref} className="relative">
      <div className="relative">
        <input
          className="w-full rounded border border-slate-300 px-3 py-2 text-sm focus:outline-none focus:border-indigo-400 pr-8"
          placeholder="e.g. slow_query  (or type a new category)"
          value={value}
          onChange={(e) => { onChange(e.target.value); setOpen(true); }}
          onFocus={() => setOpen(true)}
        />
        <ChevronDown
          className="pointer-events-none absolute right-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-slate-400"
        />
      </div>

      {open && filtered.length > 0 && (
        <ul className="absolute z-50 mt-1 w-full rounded border border-slate-200 bg-white shadow-md max-h-48 overflow-y-auto text-sm">
          {filtered.map((s) => (
            <li
              key={s}
              onMouseDown={(e) => { e.preventDefault(); onChange(s); setOpen(false); }}
              className={`flex items-center justify-between px-3 py-2 cursor-pointer hover:bg-indigo-50 hover:text-indigo-700 ${
                s === value ? "text-indigo-700 font-medium" : "text-slate-700"
              }`}
            >
              {s}
              {s === value && <Check className="h-3.5 w-3.5 text-indigo-500" />}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// SplPanel — right-side create / edit drawer
// ---------------------------------------------------------------------------

function SplPanel({
  entry,
  allTypes,
  onSave,
  onDelete,
  onClose,
}: {
  entry:    SplQueryEntry | null;   // null = create mode
  allTypes: string[];
  onSave:   (data: SplQueryCreate) => Promise<void>;
  onDelete: ((id: number) => Promise<void>) | null;
  onClose:  () => void;
}) {
  const panelRef = useRef<HTMLDivElement>(null);

  const [name,        setName]        = useState(entry?.name ?? "");
  const [queryType,   setQueryType]   = useState(entry?.query_type ?? "");
  const [environment, setEnvironment] = useState<EnvOption>((entry?.environment as EnvOption) ?? "both");
  const [description, setDescription] = useState(entry?.description ?? "");
  const [spl,         setSpl]         = useState(entry?.spl ?? "");
  const [saving,      setSaving]      = useState(false);
  const [deleting,    setDeleting]    = useState(false);
  const [confirmDel,  setConfirmDel]  = useState(false);
  const [error,       setError]       = useState("");

  useEffect(() => {
    setName(entry?.name ?? "");
    setQueryType(entry?.query_type ?? "");
    setEnvironment((entry?.environment as EnvOption) ?? "both");
    setDescription(entry?.description ?? "");
    setSpl(entry?.spl ?? "");
    setError("");
    setConfirmDel(false);
  }, [entry?.id]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    const h = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", h);
    return () => document.removeEventListener("keydown", h);
  }, [onClose]);

  const onBackdrop = (e: React.MouseEvent) => {
    if (panelRef.current && !panelRef.current.contains(e.target as Node)) onClose();
  };

  const handleSave = async () => {
    if (!name.trim())      { setError("Name is required");       return; }
    if (!queryType.trim()) { setError("Query type is required"); return; }
    if (!spl.trim())       { setError("SPL is required");        return; }
    setSaving(true);
    setError("");
    try {
      await onSave({
        name:        name.trim(),
        query_type:  queryType.trim(),
        environment,
        description: description.trim() || null,
        spl:         spl.trim(),
      });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!entry || !onDelete) return;
    setDeleting(true);
    setError("");
    try {
      await onDelete(entry.id);
      onClose();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setDeleting(false);
      setConfirmDel(false);
    }
  };

  const isNew = entry === null;

  return (
    <div
      className="fixed inset-0 z-50 flex justify-end bg-black/20 backdrop-blur-[1px]"
      onClick={onBackdrop}
    >
      <div
        ref={panelRef}
        className="relative h-full w-full max-w-lg bg-white shadow-2xl flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-200 px-5 py-4 shrink-0">
          <h2 className="text-sm font-semibold text-slate-800">
            {isNew ? "New SPL Query" : "Edit SPL Query"}
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
              placeholder="e.g. SQL Slow Queries – Prod"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </div>

          {/* Query type combobox */}
          <div className="space-y-1.5">
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">
              Query Type <span className="text-red-400">*</span>
            </label>
            <TypeCombobox value={queryType} onChange={setQueryType} suggestions={allTypes} />
            <p className="text-[11px] text-slate-400">
              Pick an existing type or type a new one to create a new category.
            </p>
          </div>

          {/* Environment */}
          <div className="space-y-1.5">
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">Environment</label>
            <div className="flex gap-2">
              {ENV_OPTIONS.map((e) => (
                <button
                  key={e}
                  onClick={() => setEnvironment(e)}
                  className={`flex-1 rounded border px-3 py-1.5 text-xs font-medium transition-colors ${
                    environment === e
                      ? ENV_COLORS[e]
                      : "border-slate-200 text-slate-400 hover:border-slate-300 hover:text-slate-600"
                  }`}
                >
                  {e}
                </button>
              ))}
            </div>
          </div>

          {/* Description */}
          <div className="space-y-1.5">
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">Description</label>
            <textarea
              className="w-full rounded border border-slate-300 px-3 py-2 text-sm focus:outline-none focus:border-indigo-400 resize-y leading-relaxed"
              rows={3}
              placeholder="What does this SPL extract? Add notes, tips, or caveats."
              value={description}
              onChange={(e) => setDescription(e.target.value)}
            />
          </div>

          {/* SPL */}
          <div className="space-y-1.5">
            <div className="flex items-center justify-between">
              <label className="text-xs font-medium text-slate-500 uppercase tracking-wider">
                SPL <span className="text-red-400">*</span>
              </label>
              {spl.trim() && <CopyButton text={spl.trim()} />}
            </div>
            <textarea
              className="w-full rounded border border-slate-300 px-3 py-2 text-xs font-mono focus:outline-none focus:border-indigo-400 resize-y leading-relaxed bg-slate-50"
              rows={14}
              spellCheck={false}
              placeholder={`index=db_perf sourcetype=mssql_slow_query\n| eval elapsed=tonumber(elapsed_ms)/1000\n| where elapsed > 5\n| stats count by host, db_name\n| sort -count`}
              value={spl}
              onChange={(e) => setSpl(e.target.value)}
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
              {isNew ? "Create SPL" : "Save Changes"}
            </Button>
            <Button variant="outline" onClick={onClose} disabled={saving || deleting}>
              Cancel
            </Button>
          </div>

          {!isNew && onDelete && (
            confirmDel ? (
              <div className="flex items-center gap-2 justify-end">
                <span className="text-xs text-slate-500">Delete this SPL?</span>
                <Button
                  size="sm"
                  className="h-7 text-xs bg-red-500 hover:bg-red-600 text-white border-0"
                  onClick={handleDelete}
                  disabled={deleting}
                >
                  {deleting ? <Spinner /> : "Yes, delete"}
                </Button>
                <Button variant="outline" size="sm" className="h-7 text-xs"
                  onClick={() => setConfirmDel(false)} disabled={deleting}>
                  Cancel
                </Button>
              </div>
            ) : (
              <button
                className="flex items-center gap-1.5 text-xs text-red-500 hover:text-red-700 w-full justify-end"
                onClick={() => setConfirmDel(true)}
              >
                <Trash2 className="h-3 w-3" />
                Delete SPL
              </button>
            )
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// SplCard — single entry card
// ---------------------------------------------------------------------------

function SplCard({
  entry,
  onEdit,
}: {
  entry:  SplQueryEntry;
  onEdit: (e: SplQueryEntry) => void;
}) {
  const envCol = ENV_COLORS[(entry.environment as EnvOption)] ?? ENV_COLORS.both;

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4 hover:border-indigo-200 hover:shadow-sm transition-all group">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2 flex-wrap mb-1.5">
            <span className="text-sm font-semibold text-slate-800 truncate">{entry.name}</span>
            <span className={`text-[10px] font-medium rounded border px-1.5 py-0.5 shrink-0 ${envCol}`}>
              {entry.environment}
            </span>
          </div>
          {entry.description && (
            <p className="text-xs text-slate-500 leading-relaxed mb-2">
              {entry.description}
            </p>
          )}
          {/* SPL preview */}
          <pre className="text-[11px] font-mono text-slate-600 bg-slate-50 rounded border border-slate-100 px-3 py-2 overflow-x-auto whitespace-pre-wrap break-all">
            {entry.spl}
          </pre>
        </div>
      </div>

      {/* Actions */}
      <div className="flex items-center gap-2 mt-3 pt-3 border-t border-slate-100">
        <CopyButton text={entry.spl} />
        <button
          onClick={() => onEdit(entry)}
          className="inline-flex items-center gap-1 rounded px-2 py-1 text-xs font-medium text-slate-500 border border-slate-200 bg-slate-50 hover:bg-slate-100 hover:text-slate-700 transition-colors"
        >
          <Pencil className="h-3 w-3" />
          Edit
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function SplPage() {
  const [entries,   setEntries]   = useState<SplQueryEntry[]>([]);
  const [allTypes,  setAllTypes]  = useState<string[]>([]);
  const [loading,   setLoading]   = useState(true);
  const [loadError, setLoadError] = useState("");
  const [selected,  setSelected]  = useState<SplQueryEntry | "new" | null>(null);
  const [activeTab, setActiveTab] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [rows, types] = await Promise.all([api.spl.list(), api.spl.types()]);
      setEntries(rows);
      setAllTypes(types);
      // Set initial tab to first type that has entries, else first type overall
      setActiveTab((prev) => {
        if (prev) return prev;
        const withEntries = types.find((t) => rows.some((r) => r.query_type === t));
        return withEntries ?? types[0] ?? null;
      });
    } catch {
      setLoadError("Failed to load SPL library");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  // Group entries by query_type
  const grouped: Record<string, SplQueryEntry[]> = {};
  for (const e of entries) {
    if (!grouped[e.query_type]) grouped[e.query_type] = [];
    grouped[e.query_type].push(e);
  }
  // Tab list: all known types (from API) that have at least one entry, plus any extras
  const tabKeys = Array.from(
    new Set([...allTypes.filter((t) => grouped[t]), ...Object.keys(grouped)])
  );
  // Active tab falls back to first available
  const currentTab = activeTab && tabKeys.includes(activeTab) ? activeTab : (tabKeys[0] ?? null);
  const tabItems = currentTab ? (grouped[currentTab] ?? []) : [];

  // ---- panel handlers -------------------------------------------------------

  const handleCreate = async (data: SplQueryCreate) => {
    const created = await api.spl.create(data);
    setEntries((prev) => [...prev, created]);
    const types = await api.spl.types();
    setAllTypes(types);
    setActiveTab(data.query_type);
    setSelected(null);
  };

  const handleSave = async (data: SplQueryCreate) => {
    if (selected === "new") {
      await handleCreate(data);
    } else if (selected) {
      const updated = await api.spl.update(selected.id, data);
      setEntries((prev) => prev.map((e) => (e.id === updated.id ? updated : e)));
      const types = await api.spl.types();
      setAllTypes(types);
      // Switch to updated type's tab in case query_type changed
      setActiveTab(data.query_type);
      setSelected(updated);
    }
  };

  const handleDelete = async (id: number) => {
    await api.spl.delete(id);
    setEntries((prev) => prev.filter((e) => e.id !== id));
    const types = await api.spl.types();
    setAllTypes(types);
  };

  const panelEntry = selected === "new" ? null : selected;
  const panelOpen  = selected !== null;

  // ---- render ---------------------------------------------------------------

  return (
    <div className="flex flex-col h-full space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between shrink-0">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
            <Code2 className="h-6 w-6 text-indigo-600" />
            SPL Library
          </h1>
          <p className="text-sm text-slate-500 mt-1">
            Splunk Processing Language queries used to export CSV data.
            {entries.length > 0 && ` ${entries.length} entr${entries.length !== 1 ? "ies" : "y"}.`}
          </p>
        </div>
        <Button onClick={() => setSelected("new")}>
          <Plus className="h-4 w-4 mr-1.5" />
          New SPL
        </Button>
      </div>

      {/* Content */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-slate-400"><Spinner /> Loading…</div>
      ) : loadError ? (
        <p className="text-sm text-red-500">{loadError}</p>
      ) : entries.length === 0 ? (
        <div className="rounded-lg border border-dashed border-slate-300 bg-white px-6 py-16 text-center">
          <Code2 className="h-10 w-10 text-slate-300 mx-auto mb-3" />
          <p className="text-sm font-medium text-slate-500">No SPL queries yet</p>
          <p className="text-xs text-slate-400 mt-1 mb-4">
            Add your first Splunk query to start building the library.
          </p>
          <Button size="sm" onClick={() => setSelected("new")}>
            <Plus className="h-4 w-4 mr-1.5" />
            New SPL
          </Button>
        </div>
      ) : (
        <div className="flex flex-col flex-1 min-h-0">
          {/* Tab bar */}
          <div className="flex gap-0 border-b border-slate-200 shrink-0 overflow-x-auto">
            {tabKeys.map((type) => {
              const count = (grouped[type] ?? []).length;
              const isActive = type === currentTab;
              return (
                <button
                  key={type}
                  onClick={() => setActiveTab(type)}
                  className={`relative flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium whitespace-nowrap transition-colors border-b-2 -mb-px ${
                    isActive
                      ? "border-indigo-500 text-indigo-700"
                      : "border-transparent text-slate-500 hover:text-slate-800 hover:border-slate-300"
                  }`}
                >
                  <span className="font-mono">{type}</span>
                  <span className={`text-[10px] rounded-full px-1.5 py-0.5 font-sans ${
                    isActive ? "bg-indigo-100 text-indigo-600" : "bg-slate-100 text-slate-400"
                  }`}>
                    {count}
                  </span>
                </button>
              );
            })}
          </div>

          {/* Tab content — full width, 2-col grid */}
          <div className="flex-1 overflow-y-auto pt-5">
            {currentTab && tabItems.length === 0 ? (
              <p className="text-sm text-slate-400 text-center py-10">No entries for this type yet.</p>
            ) : (
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
                {tabItems.map((entry) => (
                  <SplCard
                    key={entry.id}
                    entry={entry}
                    onEdit={setSelected}
                  />
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Right panel */}
      {panelOpen && (
        <SplPanel
          entry={panelEntry}
          allTypes={allTypes}
          onSave={handleSave}
          onDelete={panelEntry ? handleDelete : null}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  );
}
