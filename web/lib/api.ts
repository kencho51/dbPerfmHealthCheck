/**
 * Typed fetch helpers that talk to the FastAPI backend.
 *
 * - Browser (Client Components): relative `/api/*` — Next.js rewrite proxies to the backend.
 * - Server (RSC / Server Actions): absolute URL is required; rewrites don't run server-side.
 *   Use NEXT_PUBLIC_API_BASE (defaults to http://localhost:8000).
 */

const SERVER_BASE =
  (process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000") + "/api";
const CLIENT_BASE = "/api";
// Always send file uploads directly to the backend — bypasses the Next.js
// dev-server proxy which has a 10 MB body-size cap on rewrites.
const UPLOAD_BASE = SERVER_BASE;

// Pick the right base at call-time so this module is importable in both contexts.
function base(): string {
  return typeof window === "undefined" ? SERVER_BASE : CLIENT_BASE;
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${base()}${path}`, {
    headers: { "Content-Type": "application/json" },
    cache: "no-store",
    ...init,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

// ---- Types ----------------------------------------------------------------

export type QueryType = "slow_query" | "blocker" | "deadlock" | "slow_query_mongo";
export type EnvironmentType = "prod" | "sat";
export type SourceType = "sql" | "mongodb";
export type SeverityType = "critical" | "warning" | "info";
export type LabelSourceType = "sql" | "mongodb" | "both";

export interface RawQuery {
  id: number;
  query_hash: string;
  time: string | null;
  source: SourceType;
  host: string | null;
  db_name: string | null;
  environment: EnvironmentType;
  type: QueryType;
  month_year: string | null;
  occurrence_count: number;
  first_seen: string;
  last_seen: string;
  query_details: string | null;
  curated_id: number | null;
  created_at: string;
  updated_at: string;
}

export interface PatternLabel {
  id: number;
  name: string;
  severity: SeverityType;
  source: LabelSourceType;
  description: string | null;
  created_at: string;
  updated_at: string;
}

export interface CuratedQuery {
  // curated_query fields
  id: number;
  raw_query_id: number;
  label_id: number | null;
  label: PatternLabel | null;
  notes: string | null;
  created_at: string;
  updated_at: string;
  // raw_query fields (denormalised)
  query_hash: string;
  time: string | null;
  source: SourceType;
  host: string | null;
  db_name: string | null;
  environment: EnvironmentType;
  type: QueryType;
  query_details: string | null;
  month_year: string | null;
  occurrence_count: number;
  first_seen: string;
  last_seen: string;
}

export interface ValidationResult {
  is_valid: boolean;
  file_type: string;
  environment: string;
  row_count: number;
  warnings: string[];
  errors: string[];
  null_rates: Record<string, number>;
  sample_rows: Record<string, unknown>[];
}

export interface UploadResult {
  filename: string;
  file_type: string;
  environment: string;
  row_count: number;
  inserted: number;
  updated: number;
  skipped: number;
  warnings: string[];
  errors: string[];
}

// ---- Analytics ------------------------------------------------------------

export interface SummaryRow {
  environment: EnvironmentType;
  type: QueryType;
  source: SourceType;
  row_count: number;
  total_occurrences: number;
}

export interface HostRow {
  host: string;
  environment: EnvironmentType;
  row_count: number;
  total_occurrences: number;
}

export interface MonthRow {
  month_year: string;
  row_count: number;
  total_occurrences: number;
}

export interface DbRow {
  db_name: string;
  environment: EnvironmentType;
  row_count: number;
  total_occurrences: number;
}

export interface CurationCoverage {
  total_rows: number;
  curated_rows: number;
  uncurated_rows: number;
  coverage_pct: number;
}

// ---- API calls ------------------------------------------------------------

export const api = {
  analytics: {
    summary: () => apiFetch<SummaryRow[]>("/analytics/summary"),
    byHost: (topN = 10) => apiFetch<HostRow[]>(`/analytics/by-host?top_n=${topN}`),
    byMonth: (params?: { type?: string; environment?: string; source?: string }) => {
      const qs = params ? "?" + new URLSearchParams(Object.fromEntries(Object.entries(params).filter(([, v]) => v))).toString() : "";
      return apiFetch<MonthRow[]>(`/analytics/by-month${qs}`);
    },
    byDb: (topN = 10) => apiFetch<DbRow[]>(`/analytics/by-db?top_n=${topN}`),
    curationCoverage: () => apiFetch<CurationCoverage>("/analytics/curation-coverage"),
  },

  queries: {
    list: (params?: Record<string, string | number>) => {
      const qs = params ? "?" + new URLSearchParams(params as Record<string, string>).toString() : "";
      return apiFetch<RawQuery[]>(`/queries${qs}`);
    },
    count: (params?: Record<string, string>) => {
      const qs = params ? "?" + new URLSearchParams(params).toString() : "";
      return apiFetch<{ count: number }>(`/queries/count${qs}`);
    },
    distinct: () => apiFetch<{ hosts: string[]; db_names: string[] }>("/queries/distinct"),
    get: (id: number) => apiFetch<RawQuery>(`/queries/${id}`),
  },

  labels: {
    list: () => apiFetch<PatternLabel[]>("/labels"),
    create: (body: { name: string; severity?: SeverityType; source?: LabelSourceType; description?: string | null }) =>
      apiFetch<PatternLabel>("/labels", { method: "POST", body: JSON.stringify(body) }),
    patch: (id: number, body: { name?: string; severity?: SeverityType; source?: LabelSourceType; description?: string | null }) =>
      apiFetch<PatternLabel>(`/labels/${id}`, { method: "PATCH", body: JSON.stringify(body) }),
    delete: (id: number) =>
      fetch(`${base()}/labels/${id}`, { method: "DELETE" }).then(async (r) => {
        if (r.status === 204 || r.ok) return;
        const text = await r.text().catch(() => r.statusText);
        throw new Error(`API error ${r.status}: ${text}`);
      }),
  },

  curated: {
    list: (params?: Record<string, string | number>) => {
      const qs = params ? "?" + new URLSearchParams(params as Record<string, string>).toString() : "";
      return apiFetch<CuratedQuery[]>(`/curated${qs}`);
    },
    count: (params?: Record<string, string>) => {
      const qs = params ? "?" + new URLSearchParams(params).toString() : "";
      return apiFetch<{ count: number }>(`/curated/count${qs}`);
    },
    get: (id: number) => apiFetch<CuratedQuery>(`/curated/${id}`),
    create: (body: { raw_query_id: number; label_id?: number | null; notes?: string | null }) =>
      apiFetch<CuratedQuery>("/curated", { method: "POST", body: JSON.stringify(body) }),
    patch: (id: number, body: { label_id?: number | null; notes?: string | null }) =>
      apiFetch<CuratedQuery>(`/curated/${id}`, { method: "PATCH", body: JSON.stringify(body) }),
    delete: (id: number) =>
      fetch(`${base()}/curated/${id}`, { method: "DELETE" }).then((r) => {
        if (!r.ok && r.status !== 204) throw new Error(`Unassign failed: ${r.status}`);
      }),
  },

  upload: (file: File) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${UPLOAD_BASE}/upload`, { method: "POST", body: fd })
      .then(async (r) => {
        if (!r.ok) {
          const text = await r.text().catch(() => r.statusText);
          throw new Error(`Upload failed (${r.status}): ${text}`);
        }
        return r.json() as Promise<UploadResult>;
      });
  },

  validate: (file: File) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${UPLOAD_BASE}/validate`, { method: "POST", body: fd })
      .then(async (r) => {
        if (!r.ok) {
          const text = await r.text().catch(() => r.statusText);
          throw new Error(`Validation failed (${r.status}): ${text}`);
        }
        return r.json() as Promise<ValidationResult>;
      });
  },
};
