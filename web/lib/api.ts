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
  pattern_id: number | null;
  created_at: string;
  updated_at: string;
}

export interface Pattern {
  id: number;
  name: string;
  description: string | null;
  pattern_tag: string | null;
  severity: SeverityType;
  example_query_hash: string | null;
  source: SourceType | null;
  environment: EnvironmentType | null;
  type: QueryType | null;
  first_seen: string | null;
  last_seen: string | null;
  total_occurrences: number;
  notes: string | null;
  created_at: string;
  updated_at: string;
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

export interface PatternCoverage {
  total: number;
  tagged: number;
  untagged: number;
  coverage_pct: number;
}

// ---- API calls ------------------------------------------------------------

export const api = {
  analytics: {
    summary: () => apiFetch<SummaryRow[]>("/analytics/summary"),
    byHost: (topN = 10) => apiFetch<HostRow[]>(`/analytics/by-host?top_n=${topN}`),
    byMonth: () => apiFetch<MonthRow[]>("/analytics/by-month"),
    byDb: (topN = 10) => apiFetch<DbRow[]>(`/analytics/by-db?top_n=${topN}`),
    patternCoverage: () => apiFetch<PatternCoverage>("/analytics/pattern-coverage"),
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
    get: (id: number) => apiFetch<RawQuery>(`/queries/${id}`),
    patch: (id: number, body: { pattern_id: number | null }) =>
      apiFetch<RawQuery>(`/queries/${id}`, { method: "PATCH", body: JSON.stringify(body) }),
  },

  patterns: {
    list: (params?: Record<string, string>) => {
      const qs = params ? "?" + new URLSearchParams(params).toString() : "";
      return apiFetch<Pattern[]>(`/patterns${qs}`);
    },
    count: () => apiFetch<{ count: number }>("/patterns/count"),
    get: (id: number) => apiFetch<Pattern>(`/patterns/${id}`),
    create: (body: Partial<Pattern>) =>
      apiFetch<Pattern>("/patterns", { method: "POST", body: JSON.stringify(body) }),
    patch: (id: number, body: Partial<Pattern>) =>
      apiFetch<Pattern>(`/patterns/${id}`, { method: "PATCH", body: JSON.stringify(body) }),
    queries: (id: number) => apiFetch<RawQuery[]>(`/patterns/${id}/queries`),
  },

  upload: (file: File) => {
    const fd = new FormData();
    fd.append("file", file);
    return fetch(`${base()}/upload`, { method: "POST", body: fd })
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
    return fetch(`${base()}/validate`, { method: "POST", body: fd })
      .then(async (r) => {
        if (!r.ok) {
          const text = await r.text().catch(() => r.statusText);
          throw new Error(`Validation failed (${r.status}): ${text}`);
        }
        return r.json() as Promise<ValidationResult>;
      });
  },
};
