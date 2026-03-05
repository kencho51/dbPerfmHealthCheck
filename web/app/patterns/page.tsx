"use client";

import { useCallback, useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Spinner } from "@/components/ui/spinner";
import { api, type Pattern, type RawQuery, type SeverityType, type QueryType, type EnvironmentType } from "@/lib/api";
import { Plus, X, Save, List } from "lucide-react";

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
  const [panelTab, setPanelTab] = useState<"edit" | "queries">("edit");

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
    setPanelTab("edit");
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
      <div className={panelTab === "queries" && showPanel && !isNew ? "w-72 shrink-0 space-y-4 overflow-y-auto" : "flex-1 space-y-4 min-w-0"}>
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
        <div className={panelTab === "queries" && !isNew ? "flex-1 min-w-0" : "w-[720px] shrink-0"}>
          <Card className={panelTab === "queries" && !isNew ? "" : "sticky top-0"}>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>{isNew ? "New Pattern" : "Edit Pattern"}</CardTitle>
                <div className="flex items-center gap-2">
                  {!isNew && selected && (
                    <>
                      <button
                        onClick={() => setPanelTab("edit")}
                        className={`text-xs px-2 py-1 rounded ${panelTab === "edit" ? "bg-indigo-100 text-indigo-700 font-medium" : "text-slate-500 hover:text-slate-700"}`}
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => setPanelTab("queries")}
                        className={`flex items-center gap-1 text-xs px-2 py-1 rounded ${panelTab === "queries" ? "bg-indigo-100 text-indigo-700 font-medium" : "text-slate-500 hover:text-slate-700"}`}
                      >
                        <List className="h-3 w-3" /> Queries
                      </button>
                    </>
                  )}
                  <button onClick={closePanel} className="text-slate-400 hover:text-slate-600">
                    <X className="h-4 w-4" />
                  </button>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              {panelTab === "queries" && selected ? (
                <LinkedQueriesList patternId={selected.id} />
              ) : (
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
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}

function qTypeBadge(t: QueryType) {
  const v = t === "slow_query" ? "default" : t === "blocker" ? "warning" : t === "deadlock" ? "critical" : "mongo" as const;
  return <Badge variant={v as "default"}>{t.replaceAll("_", " ")}</Badge>;
}

function LinkedQueriesList({ patternId }: { patternId: number }) {
  const [rows, setRows] = useState<RawQuery[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    api.patterns.queries(patternId)
      .then(setRows)
      .catch(() => setRows([]))
      .finally(() => setLoading(false));
  }, [patternId]);

  if (loading) return <div className="flex justify-center py-8"><Spinner /></div>;
  if (rows.length === 0) return <p className="text-xs text-slate-400 py-4 text-center">No queries linked to this pattern yet.</p>;

  return (
    <>
      <p className="text-xs text-slate-500 mb-2 font-medium">{rows.length.toLocaleString()} linked quer{rows.length === 1 ? "y" : "ies"}</p>
      <div className="overflow-auto rounded-lg border border-slate-200 bg-white">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-slate-200 bg-slate-100">
              <th className="px-2 py-1.5 text-left text-[11px] font-semibold text-slate-600 whitespace-nowrap">ID</th>
              <th className="px-2 py-1.5 text-left text-[11px] font-semibold text-slate-600 whitespace-nowrap">Env</th>
              <th className="px-2 py-1.5 text-left text-[11px] font-semibold text-slate-600 whitespace-nowrap">Type</th>
              <th className="px-2 py-1.5 text-left text-[11px] font-semibold text-slate-600 whitespace-nowrap">Src</th>
              <th className="px-2 py-1.5 text-left text-[11px] font-semibold text-slate-600 whitespace-nowrap">Host</th>
              <th className="px-2 py-1.5 text-left text-[11px] font-semibold text-slate-600 whitespace-nowrap">Database</th>
              <th className="px-2 py-1.5 text-left text-[11px] font-semibold text-slate-600 whitespace-nowrap">Occ</th>
              <th className="px-2 py-1.5 text-left text-[11px] font-semibold text-slate-600 whitespace-nowrap">Month</th>
              <th className="px-2 py-1.5 text-left text-[11px] font-semibold text-slate-600 whitespace-nowrap">Query Details</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id} className="border-b border-slate-50 hover:bg-indigo-50/50">
                <td className="px-2 py-0.5 align-middle">{r.id}</td>
                <td className="px-2 py-0.5 align-middle">
                  <Badge variant={r.environment === "prod" ? "prod" : "sat"}>{r.environment}</Badge>
                </td>
                <td className="px-2 py-0.5 align-middle">{qTypeBadge(r.type)}</td>
                <td className="px-2 py-0.5 align-middle">
                  <Badge variant={r.source === "sql" ? "sql" : "mongo"}>{r.source === "mongodb" ? "mongo" : r.source}</Badge>
                </td>
                <td className="px-2 py-0.5 align-middle">
                  <span className="font-mono truncate block max-w-[148px]" title={r.host ?? ""}>{r.host ?? "—"}</span>
                </td>
                <td className="px-2 py-0.5 align-middle">
                  <span className="font-mono truncate block max-w-[148px]" title={r.db_name ?? ""}>{r.db_name ?? "—"}</span>
                </td>
                <td className="px-2 py-0.5 align-middle">{r.occurrence_count}</td>
                <td className="px-2 py-0.5 align-middle">{r.month_year ?? "—"}</td>
                <td className="px-2 py-0.5 align-middle">
                  {r.query_details
                    ? <span className="font-mono text-slate-600 truncate block max-w-[340px]" title={r.query_details}>{r.query_details}</span>
                    : <span className="text-slate-300">—</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
