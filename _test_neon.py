"""
Neon connectivity test using the REST API (HTTPS port 443).
This bypasses the corporate proxy's block on raw PostgreSQL port 5432.

Run with:  uv run python _test_neon.py

Requires NEON_API_KEY in api/.env.
Get one at: https://console.neon.tech/app/settings/api-keys
"""
import os
import sys
import urllib.request
import urllib.error
import urllib.parse
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "api" / ".env")

API_KEY = os.environ.get("NEON_API_KEY", "").strip()
ORG_ID = os.environ.get("NEON_ORG_ID", "").strip()
API_BASE = "https://console.neon.tech/api/v2"


def neon_get(path: str, params: dict | None = None) -> dict:
    """GET request to the Neon REST API over HTTPS."""
    url = f"{API_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def main():
    if not API_KEY:
        print("ERROR: NEON_API_KEY is not set in api/.env")
        print("Create one at: https://console.neon.tech/app/settings/api-keys")
        sys.exit(1)
    if not ORG_ID:
        print("ERROR: NEON_ORG_ID is not set in api/.env")
        sys.exit(1)

    print(f"Testing Neon REST API (HTTPS) ...")
    print(f"  Endpoint: {API_BASE}/projects")
    print(f"  Org ID:   {ORG_ID}")
    print()

    # 1. List projects (org_id required for org-scoped API keys)
    try:
        data = neon_get("/projects", params={"org_id": ORG_ID})
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"HTTP {e.code} {e.reason}: {body}")
        sys.exit(1)
    except Exception as e:
        print(f"Connection failed: {type(e).__name__}: {e}")
        sys.exit(1)

    projects = data.get("projects", [])
    print(f"[OK] API reachable. Found {len(projects)} project(s):")
    for p in projects:
        print(f"  - {p['name']}  id={p['id']}  region={p['region_id']}")

    # 2. Find hkjc-db-perfm and list its branches
    target = next((p for p in projects if "perfm" in p["name"].lower()), projects[0] if projects else None)
    if not target:
        print("No projects found.")
        return

    print()
    print(f"[OK] Inspecting project: {target['name']} ({target['id']})")
    branches_data = neon_get(f"/projects/{target['id']}/branches")
    branches = branches_data.get("branches", [])
    print(f"     Branches ({len(branches)}):")
    for b in branches:
        print(f"       - {b['name']}  state={b.get('current_state', '?')}  id={b.get('id', '')}")

    # 3. List databases (scoped to the primary/production branch)
    print()
    primary = next((b for b in branches if b["name"] in ("production", "main")), branches[0])
    dbs_data = neon_get(f"/projects/{target['id']}/branches/{primary['id']}/databases")
    dbs = dbs_data.get("databases", [])
    print(f"[OK] Databases on branch '{primary['name']}' ({len(dbs)}):")
    for db in dbs:
        print(f"       - {db['name']}  owner={db.get('owner_name', '?')}")

    print()
    print("All checks passed. HTTPS API connection works.")
    print()
    print("Next step: Apply Alembic migration via Neon Console SQL Editor.")
    print("  1. Run:  uv run alembic upgrade head --sql > migration.sql")
    print("  2. Open: https://console.neon.tech")
    print("  3. Navigate to your project -> SQL Editor")
    print("  4. Paste and run migration.sql")


main()
