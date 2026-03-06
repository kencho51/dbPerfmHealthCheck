"use client";

import { useEffect, useRef, useState } from "react";
import { X, Link2, Link2Off, Plus, ChevronDown, ChevronUp } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { api, type RawQuery, type Pattern, type EnvironmentType, type QueryType } from "@/lib/api";

// ---- helpers ----------------------------------------------------------------

function envBadge(env: EnvironmentType) {
  return <Badge variant={env === "prod" ? "prod" : "sat"}>{env}</Badge>;
}
function typeBadge(t: QueryType) {
  const v =
    t === "slow_query" ? "default"
    : t === "blocker" ? "warning"
    : t === "deadlock" ? "critical"
    : ("mongo" as const);
  return <Badge variant={v as "default"}>{t.replaceAll("_", " ")}</Badge>;
}
function srcBadge(v: string) {
  return <Badge variant={v === "sql" ? "sql" : "mongo"}>{v === "mongodb" ? "mongo" : v}</Badge>;
}

const SEV_COLORS: Record<string, string> = {
  critical: "text-red-600 bg-red-50 border-red-200",
  warning: "text-amber-600 bg-amber-50 border-amber-200",
  info: "text-indigo-600 bg-indigo-50 border-indigo-200",
};

const DEFAULT_PATTERNS: Pattern[] = [
  { id: -1, name: "Blocker",            pattern_tag: "blocker",         severity: "warning",  description: null, example_query_hash: null, source: null,      environment: null, type: "blocker",         first_seen: null, last_seen: null, total_occurrences: 0, notes: null, created_at: "", updated_at: "" },
  { id: -2, name: "Deadlock",           pattern_tag: "deadlock",        severity: "critical", description: null, example_query_hash: null, source: null,      environment: null, type: "deadlock",        first_seen: null, last_seen: null, total_occurrences: 0, notes: null, created_at: "", updated_at: "" },
  { id: -3, name: "Slow Query (SQL)",   pattern_tag: "slow_query",      severity: "warning",  description: null, example_query_hash: null, source: "sql",     environment: null, type: "slow_query",      first_seen: null, last_seen: null, total_occurrences: 0, notes: null, created_at: "", updated_at: "" },
  { id: -4, name: "Slow Query (Mongo)", pattern_tag: "slow_query_mongo", severity: "warning", description: null, example_query_hash: null, source: "mongodb", environment: null, type: "slow_query_mongo", first_seen: null, last_seen: null, total_occurrences: 0, notes: null, created_at: "", updated_at: "" },
];

function MetaRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex gap-3 py-1.5 border-b border-slate-100 last:border-0 text-xs">
      <span className="w-28 shrink-0 text-slate-400">{label}</span>
      <span className="text-slate-800 break-all">{children}</span>
    </div>
  );
}

// ---- Pattern assignment panel -----------------------------------------------

function PatternPanel({
  query,
  onAssigned,
}: {
  query: RawQuery;
  onAssigned: (updated: RawQuery) => void;
}) {
  const [patterns, setPatterns] = useState<Pattern[]>([]);
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<number | "">("");
  const [assigning, setAssigning] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDesc, setNewDesc] = useState("");
  const [newTag, setNewTag]   = useState("");
  const [newSev, setNewSev] = useState<"info" | "warning" | "critical">("info");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [confirmUnassign, setConfirmUnassign] = useState(false);

  // Current assigned pattern
  const [currentPattern, setCurrentPattern] = useState<Pattern | null>(null);
  const [loadingCurrent, setLoadingCurrent] = useState(false);

  // Reset confirm state when switching to a different query
  useEffect(() => { setConfirmUnassign(false); }, [query.id]);

  // Fetch current pattern when query.pattern_id changes
  useEffect(() => {
    if (!query.pattern_id) { setCurrentPattern(null); return; }
    setLoadingCurrent(true);
    api.patterns.get(query.pattern_id)
      .then(setCurrentPattern)
      .catch(() => setCurrentPattern(null))
      .finally(() => setLoadingCurrent(false));
  }, [query.pattern_id]);

  // Fetch all patterns for the picker — seed with defaults when search is empty
  useEffect(() => {
    api.patterns.list(search ? { search } : undefined)
      .then((rows) => setPatterns(search ? rows : [...DEFAULT_PATTERNS, ...rows]))
      .catch(() => setPatterns(DEFAULT_PATTERNS));
  }, [search]);

  const handleAssign = async () => {
    if (selected === "") return;
    setAssigning(true);
    setError("");
    try {
      let patternId = selected as number;
      // Negative IDs are defaults that don't exist yet — create them first
      if (patternId < 0) {
        const def = DEFAULT_PATTERNS.find((p) => p.id === patternId);
        if (def) {
          const created = await api.patterns.create({
            name: def.name,
            pattern_tag: def.pattern_tag ?? undefined,
            severity: def.severity,
            example_query_hash: query.query_hash ?? undefined,
          });
          patternId = created.id;
        }
      }
      const updated = await api.queries.patch(query.id, { pattern_id: patternId });
      onAssigned(updated);
    } catch (e) {
      setError(String(e));
    } finally {
      setAssigning(false);
    }
  };

  const handleUnassign = async () => {
    setConfirmUnassign(false);
    setAssigning(true);
    setError("");
    try {
      const updated = await api.queries.patch(query.id, { pattern_id: null });
      onAssigned(updated);
    } catch (e) {
      setError(String(e));
    } finally {
      setAssigning(false);
    }
  };

  const handleCreate = async () => {
    if (!newName.trim()) return;
    setSaving(true);
    setError("");
    try {
      const pat = await api.patterns.create({
        name: newName.trim(),
        description: newDesc.trim() || undefined,
        pattern_tag: newTag.trim() || undefined,
        severity: newSev,
        example_query_hash: query.query_hash ?? undefined,
      });
      const updated = await api.queries.patch(query.id, { pattern_id: pat.id });
      onAssigned(updated);
      setShowCreate(false);
      setNewName(""); setNewDesc(""); setNewTag(""); setNewSev("info");
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="space-y-3">
      <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Pattern</h3>

      {/* Current assignment */}
      {loadingCurrent ? (
        <div className="flex items-center gap-2 text-xs text-slate-400"><Spinner /> Loading…</div>
      ) : currentPattern ? (
        <div className="flex items-start justify-between rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
          <div className="space-y-0.5">
            <div className="text-xs font-medium text-slate-800">{currentPattern.name}</div>
            {currentPattern.description && (
              <div className="text-[11px] text-slate-500">{currentPattern.description}</div>
            )}
            <span className={`inline-block text-[10px] font-medium rounded border px-1.5 py-0.5 ${SEV_COLORS[currentPattern.severity] ?? ""}`}>
              {currentPattern.severity}
            </span>
          </div>
          {confirmUnassign ? (
            <div className="flex items-center gap-1.5 shrink-0 ml-3">
              <span className="text-[11px] text-slate-500">Unassign?</span>
              <Button
                size="sm"
                className="h-7 text-xs bg-red-500 hover:bg-red-600 text-white border-0"
                onClick={handleUnassign}
                disabled={assigning}
              >
                {assigning ? <Spinner /> : "Yes"}
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="h-7 text-xs"
                onClick={() => setConfirmUnassign(false)}
                disabled={assigning}
              >
                Cancel
              </Button>
            </div>
          ) : (
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-xs gap-1 shrink-0 ml-3"
              onClick={() => setConfirmUnassign(true)}
              disabled={assigning}
            >
              <Link2Off className="h-3 w-3" />
              Unassign
            </Button>
          )}
        </div>
      ) : (
        <p className="text-xs text-slate-400 italic">No pattern assigned.</p>
      )}

      {/* Assign picker */}
      <div className="space-y-2">
        <div className="flex gap-2">
          <input
            className="h-7 flex-1 rounded border border-slate-200 bg-white px-2 text-xs placeholder:text-slate-300 focus:outline-none focus:border-indigo-400"
            placeholder="Search patterns…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
        <div className="flex gap-2">
          <select
            className="h-7 flex-1 rounded border border-slate-200 bg-white px-2 text-xs text-slate-700 focus:outline-none focus:border-indigo-400"
            value={selected}
            onChange={(e) => setSelected(e.target.value === "" ? "" : Number(e.target.value))}
          >
            <option value="">— pick a pattern —</option>
            <optgroup label="Quick defaults (created on assign)">
              {patterns.filter((p) => p.id < 0).map((p) => (
                <option key={p.id} value={p.id}>{p.name}</option>
              ))}
            </optgroup>
            {patterns.some((p) => p.id > 0) && (
              <optgroup label="Saved patterns">
                {patterns.filter((p) => p.id > 0).map((p) => (
                  <option key={p.id} value={p.id}>[{p.severity}] {p.name}</option>
                ))}
              </optgroup>
            )}
          </select>
          <Button
            size="sm"
            className="h-7 text-xs gap-1 shrink-0"
            onClick={handleAssign}
            disabled={selected === "" || assigning}
          >
            {assigning ? <Spinner /> : <Link2 className="h-3 w-3" />}
            Assign
          </Button>
        </div>
      </div>

      {/* Create new pattern */}
      <div>
        <button
          className="flex items-center gap-1 text-xs text-indigo-600 hover:text-indigo-800"
          onClick={() => setShowCreate((v) => !v)}
        >
          {showCreate ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
          <Plus className="h-3 w-3" />
          Create new pattern & assign
        </button>
        {showCreate && (
          <div className="mt-2 space-y-2 rounded-lg border border-slate-200 bg-slate-50 p-3">
            <input
              className="h-7 w-full rounded border border-slate-200 bg-white px-2 text-xs placeholder:text-slate-300 focus:outline-none focus:border-indigo-400"
              placeholder="Pattern name *"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
            />
            <input
              className="h-7 w-full rounded border border-slate-200 bg-white px-2 text-xs placeholder:text-slate-300 focus:outline-none focus:border-indigo-400"
              placeholder="Description (optional)"
              value={newDesc}
              onChange={(e) => setNewDesc(e.target.value)}
            />
            <input
              className="h-7 w-full rounded border border-slate-200 bg-white px-2 text-xs placeholder:text-slate-300 focus:outline-none focus:border-indigo-400"
              placeholder="Tag (optional, e.g. slow_query)"
              value={newTag}
              onChange={(e) => setNewTag(e.target.value)}
            />
            <div className="flex gap-2">
              <select
                className="h-7 flex-1 rounded border border-slate-200 bg-white px-2 text-xs text-slate-700 focus:outline-none focus:border-indigo-400"
                value={newSev}
                onChange={(e) => setNewSev(e.target.value as "info" | "warning" | "critical")}
              >
                <option value="info">info</option>
                <option value="warning">warning</option>
                <option value="critical">critical</option>
              </select>
              <Button
                size="sm"
                className="h-7 text-xs gap-1 shrink-0"
                onClick={handleCreate}
                disabled={!newName.trim() || saving}
              >
                {saving ? <Spinner /> : <Plus className="h-3 w-3" />}
                Create & Assign
              </Button>
            </div>
          </div>
        )}
      </div>

      {error && <p className="text-xs text-red-500">{error}</p>}
    </div>
  );
}

// ---- Main drawer ------------------------------------------------------------

export function QueryDetailDrawer({
  query,
  onClose,
  onPatternChange,
}: {
  query: RawQuery | null;
  onClose: () => void;
  onPatternChange: (updated: RawQuery) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [onClose]);

  // Close on backdrop click
  const onBackdrop = (e: React.MouseEvent) => {
    if (ref.current && !ref.current.contains(e.target as Node)) onClose();
  };

  if (!query) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex justify-end bg-black/30 backdrop-blur-[1px]"
      onClick={onBackdrop}
    >
      <div
        ref={ref}
        className="relative h-full w-full max-w-xl bg-white shadow-2xl flex flex-col overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-200 px-5 py-3 shrink-0">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-slate-800">Query #{query.id}</span>
            {envBadge(query.environment)}
            {typeBadge(query.type)}
            {srcBadge(query.source)}
          </div>
          <button onClick={onClose} className="text-slate-400 hover:text-slate-700">
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
          {/* Metadata */}
          <div>
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">Metadata</h3>
            <div className="rounded-lg border border-slate-100 bg-slate-50 px-3">
              <MetaRow label="Host">{query.host ?? "—"}</MetaRow>
              <MetaRow label="Database">{query.db_name ?? "—"}</MetaRow>
              <MetaRow label="Month">{query.month_year ?? "—"}</MetaRow>
              <MetaRow label="Occurrences">{query.occurrence_count.toLocaleString()}</MetaRow>
              <MetaRow label="First seen">{query.first_seen ? new Date(query.first_seen).toLocaleString() : "—"}</MetaRow>
              <MetaRow label="Last seen">{query.last_seen ? new Date(query.last_seen).toLocaleString() : "—"}</MetaRow>
              <MetaRow label="Time">{query.time ?? "—"}</MetaRow>
              <MetaRow label="Query hash"><span className="font-mono text-[10px]">{query.query_hash}</span></MetaRow>
            </div>
          </div>

          {/* Query details */}
          <div>
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">Query Details</h3>
            <pre className="rounded-lg border border-slate-100 bg-slate-950 text-green-400 text-[11px] font-mono p-3 whitespace-pre-wrap break-all leading-relaxed max-h-64 overflow-y-auto">
              {query.query_details ?? "—"}
            </pre>
          </div>

          {/* Pattern assignment */}
          <div className="rounded-lg border border-slate-200 p-3">
            <PatternPanel query={query} onAssigned={onPatternChange} />
          </div>
        </div>
      </div>
    </div>
  );
}
