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
    // Try to parse as JSON first (our exception handler returns {"detail": "..."}).
    // Fall back to raw text, then statusText if the body is unreadable.
    let message: string;
    try {
      const json = await res.json();
      message = json?.detail ?? JSON.stringify(json);
    } catch {
      message = await res.text().catch(() => res.statusText);
    }
    throw new Error(`API error ${res.status}: ${message}`);
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
  filename:       string;
  file_type:      string;
  environment:    string;
  row_count:      number;
  inserted:       number;
  updated:        number;
  skipped:        number;
  typed_inserted: number;
  typed_updated:  number;
  warnings:       string[];
  errors:         string[];
}

// ---- Analytics ------------------------------------------------------------

export interface AnalyticsFilters {
  [key: string]: string | number | undefined;  // index signature — allows passing directly to buildQS
  host?:        string;
  db_name?:     string;
  environment?: string;
  source?:      string;
  type?:        string;
  system?:      string;
  month_year?:  string;
  top_n?:       number;
  /** Start date of the selected calendar week, ISO format e.g. '2026-01-05' */
  week_start?:  string;
  /** End date of the selected calendar week, ISO format e.g. '2026-01-11' */
  week_end?:    string;
}

function buildQS(params?: Record<string, string | number | undefined>): string {
  if (!params) return "";
  const entries = Object.entries(params).filter(([, v]) => v !== undefined && v !== "");
  if (!entries.length) return "";
  return "?" + new URLSearchParams(entries.map(([k, v]) => [k, String(v)])).toString();
}

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
  row_delta:         number | null;  // null for the first month (no prior period)
  occ_delta:         number | null;
  prod_count?:       number;
  sat_count?:        number;
}

export interface MonthTypeRow {
  month_year:       string;
  blocker:          number;  // CSV rows of type blocker uploaded for this month
  deadlock:         number;  // CSV rows of type deadlock uploaded
  slow_query:       number;  // CSV rows of type slow_query_sql uploaded
  slow_query_mongo: number;  // CSV rows of type slow_query_mongo uploaded
  total_file_rows:  number | null;  // NULL for months uploaded before log tracking
  total_patterns:   number;  // COUNT(*) distinct normalised SQL entries in raw_query
  blocker_prod?:    number;
  deadlock_prod?:   number;
  slow_query_prod?: number;
  slow_query_mongo_prod?: number;
  blocker_sat?:     number;
  deadlock_sat?:    number;
  slow_query_sat?:  number;
  slow_query_mongo_sat?: number;
}

export interface HostStatsRow {
  host:              string;
  p50:               number;
  p95:               number;
  p99:               number;
  max_occ:           number;
  total_occurrences: number;
  row_count:         number;
}

export interface CoOccurrenceRow {
  host:           string;
  month_year:     string;
  blocker_count:  number;
  deadlock_count: number;
  combined_score: number;
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

export interface HourCell {
  hour: number;     // 0–23
  weekday: number;  // 0=Monday … 6=Sunday
  count: number;
  by_type:   Record<string, number>;            // e.g. {slow_query: 800, blocker: 300}
  top_hosts: Array<{ host: string; count: number }>;
  top_dbs:   Array<{ db_name: string; count: number }>;
}

export interface FingerprintRow {
  fingerprint:     string;
  count:           number;   // sum of occurrence_count — ranking criterion
  row_count:       number;   // distinct raw_query rows sharing this fingerprint
  by_type:         Record<string, number>;
  example_host:    string;
  example_db:      string;
  months:          string[];  // sorted distinct month_year values (e.g. ["2025-11", "2025-12"])
  environments:    string[];  // distinct environments (e.g. ["prod"] or ["prod", "sat"])
  example_source:  string;    // most-common source ("mssql" / "mongodb")
}

/** A single raw_query row returned by the by-hour drill-down endpoint. */
export interface HourQueryRow {
  id:               number;
  type:             QueryType;
  host:             string | null;
  db_name:          string | null;
  environment:      EnvironmentType;
  source:           SourceType;
  time:             string | null;
  month_year:       string | null;
  occurrence_count: number;
  query_details:    string | null;
}

export interface HourQueriesResult {
  rows:  HourQueryRow[];
  total: number;
}

// ---- Typed query detail interfaces (from raw_query_* tables) --------------

export interface SlowSqlDetail {
  creation_time:         string | null;
  last_execution_time:   string | null;
  max_elapsed_time_s:    number | null;
  avg_elapsed_time_s:    number | null;
  total_elapsed_time_s:  number | null;
  total_worker_time_s:   number | null;
  avg_io:                number | null;
  avg_logical_reads:     number | null;
  avg_logical_writes:    number | null;
  execution_count:       number | null;
  query_final:           string | null;  // full untruncated SQL
}

export interface BlockerDetail {
  currentdbname: string | null;
  victims:       string | null;
  resources:     string | null;
  lock_modes:    string | null;
  count:         number | null;
  latest:        string | null;
  earliest:      string | null;
  all_query:     string | null;  // full untruncated SQL
}

export interface DeadlockDetail {
  event_time:       string | null;
  deadlock_id:      string | null;
  is_victim:        number | null;  // 0 | 1
  lock_mode:        string | null;
  wait_resource:    string | null;
  wait_time_ms:     number | null;
  transaction_name: string | null;
  app_host:         string | null;
  sql_text:         string | null;  // full untruncated SQL
  raw_xml:          string | null;
}

export interface SlowMongoDetail {
  collection:    string | null;
  event_time:    string | null;
  duration_ms:   number | null;
  plan_summary:  string | null;
  op_type:       string | null;
  remote_client: string | null;
  command_json:  string | null;  // full untruncated Mongo command
}

export type TypedQueryDetail =
  | { type: "slow_query";       data: SlowSqlDetail   | null }
  | { type: "blocker";          data: BlockerDetail   | null }
  | { type: "deadlock";         data: DeadlockDetail  | null }
  | { type: "slow_query_mongo"; data: SlowMongoDetail | null }
  | { type: string;             data: null };

// ---- SPL Library ----------------------------------------------------------

export interface SplQueryEntry {
  id: number;
  name: string;
  query_type: string;
  environment: string;
  description: string | null;
  spl: string;
  created_at: string;
  updated_at: string;
}

export interface SplQueryCreate {
  name: string;
  query_type: string;
  environment: string;
  description?: string | null;
  spl: string;
}

// ---- API calls ------------------------------------------------------------

export const api = {
  analytics: {
    summary: (filters?: AnalyticsFilters) => {
      const qs = buildQS(filters);
      return apiFetch<SummaryRow[]>(`/analytics/summary${qs}`);
    },
    byHost: (topN = 10, filters?: AnalyticsFilters) => {
      const qs = buildQS({ top_n: topN, ...filters });
      return apiFetch<HostRow[]>(`/analytics/by-host${qs}`);
    },
    byMonth: (filters?: AnalyticsFilters) => {
      const qs = buildQS(filters);
      return apiFetch<MonthRow[]>(`/analytics/by-month${qs}`);
    },
    byDb: (topN = 10, filters?: AnalyticsFilters) => {
      const qs = buildQS({ top_n: topN, ...filters });
      return apiFetch<DbRow[]>(`/analytics/by-db${qs}`);
    },
    curationCoverage: (filters?: AnalyticsFilters) => {
      const qs = buildQS(filters);
      return apiFetch<CurationCoverage>(`/analytics/curation-coverage${qs}`);
    },
    byHour: (filters?: AnalyticsFilters) => {
      const qs = buildQS(filters);
      return apiFetch<HourCell[]>(`/analytics/by-hour${qs}`);
    },
    byHourQueries: (
      hour: number,
      weekday: number,
      filters?: AnalyticsFilters,
      limit = 50,
      offset = 0,
    ) => {
      const qs = buildQS({ hour, weekday, limit, offset, ...filters });
      return apiFetch<HourQueriesResult>(`/analytics/by-hour-queries${qs}`);
    },
    topFingerprints: (topN = 20, filters?: AnalyticsFilters) => {
      const qs = buildQS({ top_n: topN, ...filters });
      return apiFetch<FingerprintRow[]>(`/analytics/top-fingerprints${qs}`);
    },
    hostStats: (topN = 30, filters?: AnalyticsFilters) => {
      const qs = buildQS({ top_n: topN, ...filters });
      return apiFetch<HostStatsRow[]>(`/analytics/host-stats${qs}`);
    },
    coOccurrence: (filters?: AnalyticsFilters) => {
      // co-occurrence doesn't support type/source filters (handled internally)
      const { type: _t, source: _s, ...rest } = filters ?? {};
      const qs = buildQS(Object.keys(rest).length ? rest : undefined);
      return apiFetch<CoOccurrenceRow[]>(`/analytics/co-occurrence${qs}`);
    },
    byMonthType: (filters?: AnalyticsFilters) => {
      const { environment } = filters ?? {};
      const qs = buildQS(environment ? { environment } : undefined);
      return apiFetch<MonthTypeRow[]>(`/analytics/by-month-type${qs}`);
    },
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
    typedDetail: (id: number) => apiFetch<TypedQueryDetail>(`/queries/${id}/typed-detail`),
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

  spl: {
    list:  (queryType?: string) => {
      const qs = queryType ? `?query_type=${encodeURIComponent(queryType)}` : "";
      return apiFetch<SplQueryEntry[]>(`/spl${qs}`);
    },
    types: () => apiFetch<string[]>("/spl/types"),
    create: (body: SplQueryCreate) =>
      apiFetch<SplQueryEntry>("/spl", { method: "POST", body: JSON.stringify(body) }),
    update: (id: number, body: Partial<SplQueryCreate>) =>
      apiFetch<SplQueryEntry>(`/spl/${id}`, { method: "PUT", body: JSON.stringify(body) }),
    delete: (id: number) =>
      fetch(`${base()}/spl/${id}`, { method: "DELETE" }).then((r) => {
        if (!r.ok && r.status !== 204) throw new Error(`Delete failed: ${r.status}`);
      }),
  },

  upload: (file: File) => {
    const fd = new FormData();
    fd.append("file", file);
    // 10-minute timeout: MongoDB slow-query CSVs with large _raw JSON fields
    // can take several minutes to extract on the server. Without a timeout
    // the browser fetch never rejects and the spinner hangs forever.
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 10 * 60 * 1000);
    return fetch(`${UPLOAD_BASE}/upload`, { method: "POST", body: fd, signal: controller.signal })
      .then(async (r) => {
        clearTimeout(timer);
        if (!r.ok) {
          const text = await r.text().catch(() => r.statusText);
          throw new Error(`Upload failed (${r.status}): ${text}`);
        }
        return r.json() as Promise<UploadResult>;
      })
      .catch((err: unknown) => {
        clearTimeout(timer);
        if (err instanceof Error && err.name === "AbortError") {
          throw new Error("Upload timed out after 10 minutes. Try uploading fewer files at once.");
        }
        throw err;
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

// ---- Auth -----------------------------------------------------------------

export type UserRole = "admin" | "viewer";

export interface AuthUser {
  id: number;
  username: string;
  email: string;
  role: UserRole;
  is_active: boolean;
  created_at: string;
  last_login: string | null;
}

export interface LoginResponse {
  access_token: string;
  token_type: string;
  user: AuthUser;
}

export interface UpdateUserRequest {
  role?: UserRole;
  is_active?: boolean;
}

export interface AdminCreateUserRequest {
  username: string;
  email: string;
  password: string;
  role: UserRole;
}

function authHeaders(token: string) {
  return { "Content-Type": "application/json", Authorization: `Bearer ${token}` };
}

export const authApi = {
  login: (username: string, password: string): Promise<LoginResponse> =>
    apiFetch("/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),

  me: (token: string): Promise<AuthUser> =>
    apiFetch("/auth/me", { headers: authHeaders(token) }),

  listUsers: (token: string): Promise<AuthUser[]> =>
    apiFetch("/auth/users", { headers: authHeaders(token) }),

  createUser: (token: string, body: AdminCreateUserRequest): Promise<AuthUser> =>
    apiFetch("/auth/register/admin", {
      method: "POST",
      headers: authHeaders(token),
      body: JSON.stringify(body),
    }),

  updateUser: (token: string, userId: number, body: UpdateUserRequest): Promise<AuthUser> =>
    apiFetch(`/auth/users/${userId}`, {
      method: "PATCH",
      headers: authHeaders(token),
      body: JSON.stringify(body),
    }),

  deleteUser: (token: string, userId: number): Promise<void> =>
    fetch(`${base()}/auth/users/${userId}`, {
      method: "DELETE",
      headers: authHeaders(token),
    }).then((r) => {
      if (!r.ok && r.status !== 204) throw new Error(`Delete failed: ${r.status}`);
    }),

  updateProfile: (
    token: string,
    body: { email?: string; current_password?: string; new_password?: string },
  ): Promise<AuthUser> =>
    apiFetch("/auth/me", {
      method: "PATCH",
      headers: authHeaders(token),
      body: JSON.stringify(body),
    }),
};
