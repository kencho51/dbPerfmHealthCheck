"""
UI smoke tests using Playwright — Next.js frontend (http://localhost:3000).

These tests require:
  1. Next.js dev server running:  cd web && npm run dev
  2. FastAPI server running:      uv run fastapi dev api/main.py --port 8000
  3. Playwright installed:        uv run playwright install chromium

Skip automatically when the frontend is not reachable.

Run:
    uv run pytest tests/test_ui_smoke.py -v

Install Playwright browsers (first time only):
    uv run playwright install chromium
"""
from __future__ import annotations

import re
import socket

import pytest

FRONTEND_URL = "http://localhost:3000"
API_URL = "http://localhost:8000"


# ---------------------------------------------------------------------------
# Skip guard — skip all tests if the servers aren't running
# ---------------------------------------------------------------------------

def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _frontend_up() -> bool:
    return _port_open("localhost", 3000)


def _api_up() -> bool:
    return _port_open("localhost", 8000)


pytestmark = pytest.mark.skipif(
    not (_frontend_up() and _api_up()),
    reason="UI smoke tests require Next.js (port 3000) and FastAPI (port 8000) running",
)

try:
    from playwright.sync_api import Page, expect, sync_playwright
    _playwright_available = True
except ImportError:
    _playwright_available = False

if not _playwright_available:
    pytestmark = pytest.mark.skip(reason="playwright not installed — run: uv run playwright install")


# ---------------------------------------------------------------------------
# Ensure testadmin exists in the live server (runs once per module)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def _ensure_test_user():
    """Register testadmin in the live FastAPI server if it doesn't exist yet."""
    if not _api_up():
        return
    import json
    import urllib.error
    import urllib.request

    payload = json.dumps({
        "username": "testadmin",
        "email": "testadmin@test.local",
        "password": "AdminPass123!",
    }).encode()
    try:
        req = urllib.request.Request(
            f"{API_URL}/api/auth/register",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # already exists or other error — proceed


# ---------------------------------------------------------------------------
# Playwright fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def browser():
    with sync_playwright() as pw:
        try:
            b = pw.chromium.launch(headless=True)
        except Exception as exc:
            pytest.skip(f"Chromium binary not installed — run: uv run playwright install chromium ({exc})")
        yield b
        b.close()


@pytest.fixture
def page(browser):
    ctx = browser.new_context(base_url=FRONTEND_URL)
    p = ctx.new_page()
    yield p
    ctx.close()


# ---------------------------------------------------------------------------
# Login page
# ---------------------------------------------------------------------------

class TestLoginPage:
    def test_login_page_loads(self, page: "Page"):
        page.goto("/login")
        expect(page).to_have_title(re.compile(r".+"))

    def test_login_form_present(self, page: "Page"):
        page.goto("/login")
        expect(page.get_by_label("Username", exact=False)).to_be_visible()
        expect(page.get_by_label("Password", exact=False)).to_be_visible()
        expect(page.get_by_role("button", name="Sign in", exact=False)).to_be_visible()

    def test_invalid_login_shows_error(self, page: "Page"):
        page.goto("/login")
        page.get_by_label("Username", exact=False).fill("nobody")
        page.get_by_label("Password", exact=False).fill("wrongpass")
        page.get_by_role("button", name="Sign in", exact=False).click()
        # Should show an error message
        page.wait_for_timeout(1000)
        body = page.content()
        assert any(word in body.lower() for word in ("invalid", "error", "incorrect", "failed"))

    def test_login_page_performance(self, page: "Page"):
        """Login page must load within 3 seconds."""
        import time
        t0 = time.perf_counter()
        page.goto("/login", wait_until="networkidle")
        elapsed = time.perf_counter() - t0
        assert elapsed < 3.0, f"Login page took {elapsed:.1f}s to load"


# ---------------------------------------------------------------------------
# Successful login flow
# ---------------------------------------------------------------------------

class TestAuthFlow:
    def test_valid_login_redirects(self, page: "Page"):
        page.goto("/login")
        page.get_by_label("Username", exact=False).fill("testadmin")
        page.get_by_label("Password", exact=False).fill("AdminPass123!")
        page.get_by_role("button", name="Sign in", exact=False).click()
        # Allow time for login API call + Next.js router redirect
        page.wait_for_timeout(5000)
        assert "/login" not in page.url, f"Still on login page after 5s: {page.url}"


# ---------------------------------------------------------------------------
# Dashboard / main views (authenticated)
# ---------------------------------------------------------------------------

class TestDashboard:
    @pytest.fixture(autouse=True)
    def _login(self, page: "Page"):
        """Inject auth token directly via API — bypasses form to avoid timing fragility."""
        import json
        import urllib.request

        # Get JWT from the live FastAPI server directly
        try:
            payload = json.dumps(
                {"username": "testadmin", "password": "AdminPass123!"}
            ).encode()
            req = urllib.request.Request(
                f"{API_URL}/api/auth/login",
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            token = data["access_token"]
        except Exception as exc:
            pytest.skip(f"testadmin login failed — skipping dashboard tests: {exc}")
            return

        # Land on the origin first so localStorage/cookie writes are valid
        page.goto("/login")
        # Inject token exactly as saveAuth() in auth-client.ts does
        page.evaluate(
            """(token) => {
                localStorage.setItem('auth_token', token);
                document.cookie = 'auth_token=' + token + '; path=/; max-age=86400; SameSite=Lax';
            }""",
            token,
        )
        # Navigate directly to dashboard (middleware reads the cookie)
        page.goto("/dashboard")
        page.wait_for_load_state("networkidle")

    def test_dashboard_loads(self, page: "Page"):
        page.goto("/")
        page.wait_for_load_state("networkidle")
        assert page.title() != ""

    def test_no_console_errors(self, page: "Page"):
        errors: list[str] = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        page.goto("/")
        page.wait_for_load_state("networkidle")
        # Tolerate known non-critical errors; fail on unexpected ones
        critical = [e for e in errors if "TypeError" in e or "ReferenceError" in e]
        assert critical == [], f"Console JS errors: {critical}"

    def test_page_load_under_3s(self, page: "Page"):
        import time
        t0 = time.perf_counter()
        page.goto("/", wait_until="networkidle")
        elapsed = time.perf_counter() - t0
        assert elapsed < 3.0, f"Dashboard took {elapsed:.1f}s"
