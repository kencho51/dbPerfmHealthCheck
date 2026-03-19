/**
 * Client-side auth utilities.
 *
 * All auth state is stored in:
 *   - localStorage  ("auth_user" JSON, "auth_token" JWT string) — for client components
 *   - Cookie "auth_token" — for Next.js middleware route protection (set via document.cookie)
 */

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

const TOKEN_KEY = "auth_token";
const USER_KEY = "auth_user";
const COOKIE_MAX_AGE = 60 * 60 * 24; // 24 hours

/** Persist token + user after a successful login. */
export function saveAuth(token: string, user: AuthUser): void {
  localStorage.setItem(TOKEN_KEY, token);
  localStorage.setItem(USER_KEY, JSON.stringify(user));
  // Set cookie so Next.js middleware can protect routes
  document.cookie = `${TOKEN_KEY}=${token}; path=/; max-age=${COOKIE_MAX_AGE}; SameSite=Lax`;
}

/** Clear all auth state (call on logout). */
export function clearAuth(): void {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USER_KEY);
  // Expire the cookie immediately
  document.cookie = `${TOKEN_KEY}=; path=/; max-age=0; SameSite=Lax`;
}

/** Get the stored JWT (null if not logged in). */
export function getToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(TOKEN_KEY);
}

/** Get the stored user object (null if not logged in). */
export function getUser(): AuthUser | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = localStorage.getItem(USER_KEY);
    return raw ? (JSON.parse(raw) as AuthUser) : null;
  } catch {
    return null;
  }
}

/** Decode the `exp` claim from a JWT and return it as a ms timestamp (0 on failure). */
function getJwtExpiry(token: string): number {
  try {
    const payload = JSON.parse(atob(token.split(".")[1]));
    return (payload.exp ?? 0) * 1000; // JWT exp is in seconds → convert to ms
  } catch {
    return 0;
  }
}

/** True if a non-expired token is present. Clears stale auth automatically. */
export function isLoggedIn(): boolean {
  const token = getToken();
  if (!token) return false;
  if (Date.now() > getJwtExpiry(token)) {
    clearAuth(); // wipe stale token + cookie
    return false;
  }
  return true;
}

/** Redirect to login (and clear auth) if not logged in. */
export function requireAuth(): void {
  if (!isLoggedIn()) {
    clearAuth();
    window.location.href = "/login";
  }
}
