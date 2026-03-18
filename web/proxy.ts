/**
 * Next.js Proxy (formerly middleware) — protect all routes except /login.
 *
 * The auth flow uses a plain cookie "auth_token" (JWT value).
 * Login page sets it; logout clears it.  Proxy reads it here.
 *
 * Renamed from middleware.ts → proxy.ts (Next.js 15.2+ convention).
 * See: https://nextjs.org/docs/messages/middleware-to-proxy
 */
import { type NextRequest, NextResponse } from "next/server";

const PUBLIC_PATHS = ["/login"];

export function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Allow public paths and Next.js internal paths
  if (
    PUBLIC_PATHS.some((p) => pathname.startsWith(p)) ||
    pathname.startsWith("/_next") ||
    pathname.startsWith("/favicon")
  ) {
    return NextResponse.next();
  }

  // Check for auth token cookie
  const token = request.cookies.get("auth_token")?.value;
  if (!token) {
    const loginUrl = new URL("/login", request.url);
    loginUrl.searchParams.set("from", pathname);
    return NextResponse.redirect(loginUrl);
  }

  return NextResponse.next();
}

export const config = {
  // Run on all page routes; skip static assets and API routes
  matcher: ["/((?!api|_next/static|_next/image|favicon.ico).*)"],
};
