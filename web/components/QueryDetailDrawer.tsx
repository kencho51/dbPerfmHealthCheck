"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { X, BookmarkPlus, BookmarkX } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { api, type RawQuery, type PatternLabel, type CuratedQuery, type EnvironmentType, type QueryType, type TypedQueryDetail } from "@/lib/api";

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

function MetaRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex gap-3 py-1.5 border-b border-slate-100 last:border-0 text-xs">
      <span className="w-28 shrink-0 text-slate-400">{label}</span>
      <span className="text-slate-800 break-all">{children}</span>
    </div>
  );
}

// ---- Curation panel ---------------------------------------------------------

function CurationPanel({
  query,
  onCurationChange,
}: {
  query: RawQuery;
  onCurationChange: (updated: RawQuery) => void;
}) {
  const [labels, setLabels] = useState<PatternLabel[]>([]);
  const [curatedEntry, setCuratedEntry] = useState<CuratedQuery | null>(null);
  const [loadingEntry, setLoadingEntry] = useState(false);

  // Assign form state
  const [selectedLabel, setSelectedLabel] = useState<number | "">("");
  const [notes, setNotes] = useState("");
  const [assigning, setAssigning] = useState(false);

  // Edit notes
  const [editingNotes, setEditingNotes] = useState(false);
  const [editNotesDraft, setEditNotesDraft] = useState("");
  const [savingNotes, setSavingNotes] = useState(false);

  // Unassign confirm
  const [confirmUnassign, setConfirmUnassign] = useState(false);
  const [unassigning, setUnassigning] = useState(false);

  // Label edit
  const [editingLabel, setEditingLabel] = useState(false);
  const [selectedEditLabel, setSelectedEditLabel] = useState<number | "">("");
  const [savingLabel, setSavingLabel] = useState(false);



  const [error, setError] = useState("");

  // Reset state on query change
  useEffect(() => {
    setConfirmUnassign(false);
    setEditingNotes(false);
    setEditingLabel(false);
    setSelectedLabel("");
    setNotes("");
    setError("");
  }, [query.id]);

  // Load labels
  useEffect(() => {
    api.labels.list().then(setLabels).catch(() => setLabels([]));
  }, []);

  // Load curated entry when curated_id is set
  useEffect(() => {
    if (!query.curated_id) { setCuratedEntry(null); return; }
    setLoadingEntry(true);
    api.curated.get(query.curated_id)
      .then(setCuratedEntry)
      .catch(() => setCuratedEntry(null))
      .finally(() => setLoadingEntry(false));
  }, [query.curated_id]);

  // ---- Assign -----------------------------------------------------------------
  const handleAssign = async () => {
    setAssigning(true);
    setError("");
    try {
      const created = await api.curated.create({
        raw_query_id: query.id,
        label_id: selectedLabel === "" ? null : (selectedLabel as number),
        notes: notes.trim() || null,
      });
      setCuratedEntry(created);
      onCurationChange({ ...query, curated_id: created.id });
    } catch (e) {
      setError(String(e));
    } finally {
      setAssigning(false);
    }
  };

  // ---- Unassign ---------------------------------------------------------------
  const handleUnassign = async () => {
    if (!query.curated_id) return;
    setUnassigning(true);
    setError("");
    try {
      await api.curated.delete(query.curated_id);
      setCuratedEntry(null);
      setConfirmUnassign(false);
      onCurationChange({ ...query, curated_id: null });
    } catch (e) {
      setError(String(e));
    } finally {
      setUnassigning(false);
    }
  };

  // ---- Save notes edit --------------------------------------------------------
  const handleSaveNotes = async () => {
    if (!curatedEntry) return;
    setSavingNotes(true);
    setError("");
    try {
      const updated = await api.curated.patch(curatedEntry.id, { notes: editNotesDraft.trim() || null });
      setCuratedEntry(updated);
      setEditingNotes(false);
    } catch (e) {
      setError(String(e));
    } finally {
      setSavingNotes(false);
    }
  };

  // ---- Save label edit --------------------------------------------------------
  const handleSaveLabel = async () => {
    if (!curatedEntry) return;
    setSavingLabel(true);
    setError("");
    try {
      const updated = await api.curated.patch(curatedEntry.id, {
        label_id: selectedEditLabel === "" ? null : (selectedEditLabel as number),
      });
      setCuratedEntry(updated);
      setEditingLabel(false);
    } catch (e) {
      setError(String(e));
    } finally {
      setSavingLabel(false);
    }
  };



  // ---- Render -----------------------------------------------------------------

  const isAssigned = !!query.curated_id;

  return (
    <div className="space-y-3">
      <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Curation</h3>

      {isAssigned ? (
        /* ---- Assigned view -------------------------------------------------- */
        <>
          {loadingEntry ? (
            <div className="flex items-center gap-2 text-xs text-slate-400"><Spinner /> Loading</div>
          ) : curatedEntry ? (
            <div className="rounded-lg border border-indigo-200 bg-indigo-50 px-3 py-2 space-y-2">
              {/* Label row */}
              <div className="flex items-center justify-between gap-2">
                <div className="space-y-0.5 flex-1 min-w-0">
                  <div className="text-[10px] text-slate-400 uppercase tracking-wider">Label</div>
                  {editingLabel ? (
                    <div className="flex gap-1.5">
                      <select
                        className="h-7 flex-1 rounded border border-slate-200 bg-white px-2 text-xs text-slate-700 focus:outline-none focus:border-indigo-400"
                        value={selectedEditLabel}
                        onChange={(e) => setSelectedEditLabel(e.target.value === "" ? "" : Number(e.target.value))}
                      >
                        <option value=""> no label </option>
                        {labels.map((l) => (
                          <option key={l.id} value={l.id}>[{l.severity}] {l.name}</option>
                        ))}
                      </select>
                      <Button size="sm" className="h-7 text-xs" onClick={handleSaveLabel} disabled={savingLabel}>
                        {savingLabel ? <Spinner /> : "Save"}
                      </Button>
                      <Button variant="outline" size="sm" className="h-7 text-xs" onClick={() => setEditingLabel(false)}></Button>
                    </div>
                  ) : (
                    <div className="flex items-center gap-2">
                      {curatedEntry.label ? (
                        <>
                          <span className="text-xs font-medium text-slate-800">{curatedEntry.label.name}</span>
                          <span className={`text-[10px] font-medium rounded border px-1.5 py-0.5 ${SEV_COLORS[curatedEntry.label.severity] ?? ""}`}>
                            {curatedEntry.label.severity}
                          </span>
                        </>
                      ) : (
                        <span className="text-xs text-slate-400 italic">No label</span>
                      )}
                      <button
                        className="text-[10px] text-indigo-500 hover:text-indigo-700 ml-1"
                        onClick={() => { setSelectedEditLabel(curatedEntry.label_id ?? ""); setEditingLabel(true); }}
                      >Edit</button>
                    </div>
                  )}
                </div>
              </div>

              {/* Notes row */}
              <div>
                <div className="text-[10px] text-slate-400 uppercase tracking-wider mb-0.5">Notes</div>
                {editingNotes ? (
                  <div className="space-y-1.5">
                    <textarea
                      className="w-full rounded border border-slate-200 bg-white px-2 py-1 text-xs text-slate-700 focus:outline-none focus:border-indigo-400 resize-none"
                      rows={3}
                      value={editNotesDraft}
                      onChange={(e) => setEditNotesDraft(e.target.value)}
                    />
                    <div className="flex gap-1.5">
                      <Button size="sm" className="h-7 text-xs" onClick={handleSaveNotes} disabled={savingNotes}>
                        {savingNotes ? <Spinner /> : "Save"}
                      </Button>
                      <Button variant="outline" size="sm" className="h-7 text-xs" onClick={() => setEditingNotes(false)}>Cancel</Button>
                    </div>
                  </div>
                ) : (
                  <div className="flex items-start gap-2">
                    <span className="text-xs text-slate-700 flex-1">{curatedEntry.notes || <span className="italic text-slate-400">No notes</span>}</span>
                    <button
                      className="text-[10px] text-indigo-500 hover:text-indigo-700 shrink-0"
                      onClick={() => { setEditNotesDraft(curatedEntry.notes ?? ""); setEditingNotes(true); }}
                    >Edit</button>
                  </div>
                )}
              </div>

              {/* Unassign */}
              <div className="pt-1 border-t border-indigo-100">
                {confirmUnassign ? (
                  <div className="flex items-center gap-2">
                    <span className="text-[11px] text-slate-500">Remove from curation?</span>
                    <Button size="sm" className="h-6 text-[11px] bg-red-500 hover:bg-red-600 text-white border-0 px-2" onClick={handleUnassign} disabled={unassigning}>
                      {unassigning ? <Spinner /> : "Yes, remove"}
                    </Button>
                    <Button variant="outline" size="sm" className="h-6 text-[11px] px-2" onClick={() => setConfirmUnassign(false)} disabled={unassigning}>Cancel</Button>
                  </div>
                ) : (
                  <Button variant="outline" size="sm" className="h-7 text-xs gap-1" onClick={() => setConfirmUnassign(true)}>
                    <BookmarkX className="h-3 w-3" />
                    Remove from curation
                  </Button>
                )}
              </div>
            </div>
          ) : null}
        </>
      ) : (
        /* ---- Not assigned view ---------------------------------------------- */
        <div className="space-y-2">
          <p className="text-xs text-slate-400 italic">Not in curation list.</p>

          {/* Label picker */}
          <div className="flex gap-2">
            <select
              className="h-7 flex-1 rounded border border-slate-200 bg-white px-2 text-xs text-slate-700 focus:outline-none focus:border-indigo-400"
              value={selectedLabel}
              onChange={(e) => setSelectedLabel(e.target.value === "" ? "" : Number(e.target.value))}
            >
              <option value=""> pick a label (optional) </option>
              {labels.map((l) => (
                <option key={l.id} value={l.id}>[{l.severity}] {l.name}</option>
              ))}
            </select>
          </div>

          {/* Notes input */}
          <textarea
            className="w-full rounded border border-slate-200 bg-white px-2 py-1 text-xs text-slate-700 placeholder:text-slate-300 focus:outline-none focus:border-indigo-400 resize-none"
            rows={2}
            placeholder="Notes (optional)"
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
          />

          <div className="flex gap-2 items-center flex-wrap">
            <Button size="sm" className="h-7 text-xs gap-1" onClick={handleAssign} disabled={assigning}>
              {assigning ? <Spinner /> : <BookmarkPlus className="h-3 w-3" />}
              Add to Curation
            </Button>
            <Link
              href="/labels"
              className="text-xs text-indigo-500 hover:text-indigo-700 hover:underline"
              onClick={(e) => e.stopPropagation()}
            >
              Manage labels →
            </Link>
          </div>
        </div>
      )}

      {error && <p className="text-xs text-red-500">{error}</p>}
    </div>
  );
}

// ---- Main drawer ------------------------------------------------------------

// Fields to hide in typed-detail section (already shown in metadata or internal bookkeeping)
const _TYPED_SKIP = new Set([
  "id", "raw_query_id", "query_hash", "occurrence_count",
  "first_seen", "last_seen", "created_at", "updated_at",
  "host", "db_name", "environment", "month_year",
]);

function TypedDetailPanel({ detail }: { detail: TypedQueryDetail }) {
  if (!detail.data) {
    return (
      <p className="text-xs text-slate-400 italic">
        No typed detail available yet — the linking background task may still be running,
        or re-upload the source CSV to populate it.
      </p>
    );
  }
  const entries = Object.entries(detail.data).filter(
    ([k, v]) => !_TYPED_SKIP.has(k) && v !== null && v !== ""
  );
  if (entries.length === 0) {
    return <p className="text-xs text-slate-400 italic">No additional columns found.</p>;
  }
  return (
    <div className="rounded-lg border border-slate-100 bg-slate-50 px-3">
      {entries.map(([k, v]) => (
        <MetaRow key={k} label={k.replace(/_/g, " ")}>
          {typeof v === "number" ? (
            v.toLocaleString()
          ) : String(v).length > 120 ? (
            <span className="font-mono text-[10px] break-all">{String(v)}</span>
          ) : (
            String(v)
          )}
        </MetaRow>
      ))}
    </div>
  );
}

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
  const [typedDetail, setTypedDetail] = useState<TypedQueryDetail | null>(null);
  const [loadingTyped, setLoadingTyped] = useState(false);

  // Fetch typed-detail whenever the selected query changes
  useEffect(() => {
    let cancelled = false;

    if (!query) {
      setTypedDetail(null);
      setLoadingTyped(false);
      return () => {
        cancelled = true;
      };
    }

    setLoadingTyped(true);
    setTypedDetail(null);
    api.queries.typedDetail(query.id)
      .then((detail) => {
        if (!cancelled) setTypedDetail(detail);
      })
      .catch(() => {
        if (!cancelled) setTypedDetail(null);
      })
      .finally(() => {
        if (!cancelled) setLoadingTyped(false);
      });

    return () => {
      cancelled = true;
    };
  }, [query?.id]);

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
              <MetaRow label="Row ID"><span className="font-mono">{query.id}</span></MetaRow>
              <MetaRow label="Host">{query.host ?? ""}</MetaRow>
              <MetaRow label="Database">{query.db_name ?? ""}</MetaRow>
              <MetaRow label="Month">{query.month_year ?? ""}</MetaRow>
              <MetaRow label="Occurrences">{query.occurrence_count.toLocaleString()}</MetaRow>
              <MetaRow label="First seen">{query.first_seen ? new Date(query.first_seen).toLocaleString() : ""}</MetaRow>
              <MetaRow label="Last seen">{query.last_seen ? new Date(query.last_seen).toLocaleString() : ""}</MetaRow>
              <MetaRow label="Time">{query.time ?? ""}</MetaRow>
              <MetaRow label="Query hash"><span className="font-mono text-[10px]">{query.query_hash}</span></MetaRow>
            </div>
          </div>

          {/* Query details */}
          <div>
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">Query Details</h3>
            <pre className="rounded-lg border border-slate-100 bg-slate-950 text-green-400 text-[11px] font-mono p-3 whitespace-pre-wrap break-all leading-relaxed max-h-64 overflow-y-auto">
              {query.query_details ?? ""}
            </pre>
          </div>

          {/* Raw CSV data from typed table */}
          <div>
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">Raw CSV Data</h3>
            {loadingTyped ? (
              <div className="flex items-center gap-2 text-xs text-slate-400">
                <Spinner /> Loading…
              </div>
            ) : typedDetail ? (
              <TypedDetailPanel detail={typedDetail} />
            ) : (
              <p className="text-xs text-slate-400 italic">—</p>
            )}
          </div>

          {/* Curation panel */}
          <div className="rounded-lg border border-slate-200 p-3">
            <CurationPanel query={query} onCurationChange={onPatternChange} />
          </div>
        </div>
      </div>
    </div>
  );
}
