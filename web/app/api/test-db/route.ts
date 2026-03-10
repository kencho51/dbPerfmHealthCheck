/**
 * GET /api/test-db
 *
 * Test route to verify the Neon PostgreSQL connection from Next.js.
 * Uses the `postgres` npm package with individual PG* env vars (Neon docs pattern).
 *
 * Usage:
 *   curl http://localhost:3000/api/test-db
 *   or visit http://localhost:3000/api/test-db in the browser
 *
 * Remove this route once Phase 7 migration is confirmed working.
 */
import postgres from "postgres";
import { NextResponse } from "next/server";

// Force dynamic rendering — this route must never be statically cached
export const dynamic = "force-dynamic";

export async function GET() {
  const { PGHOST, PGDATABASE, PGUSER, PGPASSWORD } = process.env;

  if (!PGHOST || !PGDATABASE || !PGUSER || !PGPASSWORD) {
    return NextResponse.json(
      {
        ok: false,
        error: "Missing PG* environment variables. Check web/.env.local",
        missing: { PGHOST: !PGHOST, PGDATABASE: !PGDATABASE, PGUSER: !PGUSER, PGPASSWORD: !PGPASSWORD },
      },
      { status: 500 }
    );
  }

  const conn = postgres({
    host: PGHOST,
    database: PGDATABASE,
    username: PGUSER,
    password: PGPASSWORD,
    port: 5432,
    ssl: "require",
    max: 1,           // single connection for this test
    idle_timeout: 5,
  });

  try {
    const [versionRow] = await conn`SELECT version() AS version, current_database() AS db`;
    await conn.end();

    return NextResponse.json({
      ok: true,
      db: versionRow.db,
      version: versionRow.version,
      host: PGHOST,
    });
  } catch (err) {
    await conn.end().catch(() => {});
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
