"use client";

import { useCallback, useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Spinner } from "@/components/ui/spinner";
import { api, type Pattern, type SeverityType } from "@/lib/api";
import { Plus, X, Save } from "lucide-react";

function severityBadge(s: SeverityType) {
  const v = s === "critical" ? "critical" : s === "warning" ? "warning" : "info";
  return <Badge variant={v}>{s}</Badge>;
}

const EMPTY_PATTERN: Partial<Pattern> = {
  name: "",
  description: "",
  pattern_tag: "",
  severity: "warning",
  notes: "",
};

export default function PatternsPage() {
  const [patterns, setPatterns] = useState<Pattern[]>([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<Pattern | null>(null);
  const [editing, setEditing] = useState<Partial<Pattern>>(EMPTY_PATTERN);
  const [isNew, setIsNew] = useState(false);
  const [saving, setSaving] = useState(false);
  const [filterSeverity, setFilterSeverity] = useState("");

  const load = useCallback(() => {
    setLoading(true);
    api.patterns.list(filterSeverity ? { severity: filterSeverity } : undefined)
      .then(setPatterns)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [filterSeverity]);

  useEffect(() => { load(); }, [load]);

  function openEdit(p: Pattern) {
    setSelected(p);
    setEditing({ ...p });
    setIsNew(false);
  }

  function openNew() {
    setSelected(null);
    setEditing({ ...EMPTY_PATTERN });
    setIsNew(true);
  }

  function closePanel() {
    setSelected(null);
    setIsNew(false);
  }

  async function handleSave() {
    setSaving(true);
    try {
      if (isNew) {
        await api.patterns.create(editing);
      } else if (selected) {
        await api.patterns.patch(selected.id, editing);
      }
      load();
      closePanel();
    } catch (err) {
      alert(String(err));
    } finally {
      setSaving(false);
    }
  }

  const showPanel = selected !== null || isNew;

  return (
    <div className="flex gap-6 h-full">
      {/* Left — pattern list */}
      <div className="flex-1 space-y-4 min-w-0">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-slate-900">Patterns</h1>
            <p className="text-sm text-slate-500 mt-1">{patterns.length} curated patterns</p>
          </div>
          <Button size="sm" onClick={openNew}>
            <Plus className="h-4 w-4" /> New Pattern
          </Button>
        </div>

        {/* Filter */}
        <Select
          className="w-40"
          value={filterSeverity}
          onChange={(e) => setFilterSeverity(e.target.value)}
        >
          <option value="">All severities</option>
          <option value="critical">critical</option>
          <option value="warning">warning</option>
          <option value="info">info</option>
        </Select>

        {loading ? (
          <div className="flex items-center justify-center h-48">
            <Spinner />
          </div>
        ) : patterns.length === 0 ? (
          <Card>
            <CardContent className="py-12">
              <p className="text-center text-sm text-slate-400">
                No patterns yet. Promote a raw query or create one manually.
              </p>
            </CardContent>
          </Card>
        ) : (
          <div className="space-y-2">
            {patterns.map((p) => (
              <Card
                key={p.id}
                className={`cursor-pointer transition-shadow hover:shadow-md ${selected?.id === p.id ? "ring-2 ring-indigo-500" : ""}`}
                onClick={() => openEdit(p)}
              >
                <CardHeader>
                  <div className="flex items-start justify-between gap-3">
                    <div className="space-y-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        {severityBadge(p.severity)}
                        {p.pattern_tag && (
                          <Badge variant="outline">{p.pattern_tag}</Badge>
                        )}
                        {p.environment && (
                          <Badge variant={p.environment === "prod" ? "prod" : "sat"}>
                            {p.environment}
                          </Badge>
                        )}
                      </div>
                      <p className="font-semibold text-slate-900 text-sm">{p.name}</p>
                      {p.description && (
                        <p className="text-xs text-slate-500 line-clamp-2">{p.description}</p>
                      )}
                    </div>
                    <div className="text-right shrink-0">
                      <p className="text-xl font-bold text-slate-700">{p.total_occurrences.toLocaleString()}</p>
                      <p className="text-xs text-slate-400">occurrences</p>
                    </div>
                  </div>
                </CardHeader>
              </Card>
            ))}
          </div>
        )}
      </div>

      {/* Right — edit panel */}
      {showPanel && (
        <div className="w-80 shrink-0">
          <Card className="sticky top-0">
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>{isNew ? "New Pattern" : "Edit Pattern"}</CardTitle>
                <button onClick={closePanel} className="text-slate-400 hover:text-slate-600">
                  <X className="h-4 w-4" />
                </button>
              </div>
            </CardHeader>
            <CardContent>
              <form
                className="space-y-3"
                onSubmit={(e) => { e.preventDefault(); handleSave(); }}
              >
                <div>
                  <label className="text-xs text-slate-500 mb-1 block">Name *</label>
                  <Input
                    required
                    placeholder="COLLSCAN on audit_log"
                    value={editing.name ?? ""}
                    onChange={(e) => setEditing((p) => ({ ...p, name: e.target.value }))}
                  />
                </div>
                <div>
                  <label className="text-xs text-slate-500 mb-1 block">Pattern Tag</label>
                  <Input
                    placeholder="missing_index"
                    value={editing.pattern_tag ?? ""}
                    onChange={(e) => setEditing((p) => ({ ...p, pattern_tag: e.target.value }))}
                  />
                </div>
                <div>
                  <label className="text-xs text-slate-500 mb-1 block">Severity *</label>
                  <Select
                    value={editing.severity ?? "warning"}
                    onChange={(e) =>
                      setEditing((p) => ({ ...p, severity: e.target.value as SeverityType }))
                    }
                  >
                    <option value="critical">critical</option>
                    <option value="warning">warning</option>
                    <option value="info">info</option>
                  </Select>
                </div>
                <div>
                  <label className="text-xs text-slate-500 mb-1 block">Description</label>
                  <textarea
                    rows={3}
                    className="w-full rounded-md border border-slate-300 px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-slate-900"
                    placeholder="Collection scan detected…"
                    value={editing.description ?? ""}
                    onChange={(e) => setEditing((p) => ({ ...p, description: e.target.value }))}
                  />
                </div>
                <div>
                  <label className="text-xs text-slate-500 mb-1 block">Notes</label>
                  <textarea
                    rows={2}
                    className="w-full rounded-md border border-slate-300 px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-slate-900"
                    placeholder="Recommendation…"
                    value={editing.notes ?? ""}
                    onChange={(e) => setEditing((p) => ({ ...p, notes: e.target.value }))}
                  />
                </div>
                <Button type="submit" className="w-full" disabled={saving}>
                  {saving ? <Spinner className="h-4 w-4" /> : <Save className="h-4 w-4" />}
                  {saving ? "Saving…" : isNew ? "Create" : "Save Changes"}
                </Button>
              </form>

              {/* Linked queries count */}
              {selected && (
                <div className="mt-4 pt-4 border-t border-slate-100">
                  <LinkedQueriesCount patternId={selected.id} />
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}

function LinkedQueriesCount({ patternId }: { patternId: number }) {
  const [count, setCount] = useState<number | null>(null);
  useEffect(() => {
    api.patterns.queries(patternId)
      .then((rows) => setCount(rows.length))
      .catch(() => setCount(null));
  }, [patternId]);

  if (count === null) return null;
  return (
    <p className="text-xs text-slate-500">
      <span className="font-semibold text-slate-700">{count.toLocaleString()}</span> linked raw queries
    </p>
  );
}
