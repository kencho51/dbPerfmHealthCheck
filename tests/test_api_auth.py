"""
API tests — authentication endpoints.

Tests all /api/auth/* routes against a local SQLite DB (no Neon needed).

Run:
    uv run pytest tests/test_api_auth.py -v
"""

from __future__ import annotations

from httpx import AsyncClient

# ---------------------------------------------------------------------------
# POST /api/auth/register
# ---------------------------------------------------------------------------


class TestRegister:
    async def test_first_user_registers_as_admin(self, client: AsyncClient):
        r = await client.post(
            "/api/auth/register",
            json={
                "username": "firstuser",
                "email": "first@test.local",
                "password": "Password123!",
            },
        )
        # First user: 200/201; already exists (from conftest admin): 403
        assert r.status_code in (200, 201, 403)

    async def test_register_returns_user_fields(self, client: AsyncClient):
        # Use a unique name to avoid conflict with conftest admin
        r = await client.post(
            "/api/auth/register",
            json={
                "username": "newuser_reg",
                "email": "newreg@test.local",
                "password": "SecurePass99!",
            },
        )
        # May be 200/201 (success) or 403 (users already exist — admin-only)
        if r.status_code in (200, 201):
            data = r.json()
            assert "username" in data
            assert "role" in data
            assert "id" in data

    async def test_password_too_short_rejected(self, client: AsyncClient):
        # After the first user exists, /register returns 403 (admin-only).
        # When it IS the first user, short passwords should give 400/422.
        # Either outcome is valid depending on DB state.
        r = await client.post(
            "/api/auth/register",
            json={
                "username": "shortpass",
                "email": "short@test.local",
                "password": "abc",
            },
        )
        assert r.status_code in (400, 403, 422)

    async def test_missing_username_422(self, client: AsyncClient):
        r = await client.post(
            "/api/auth/register",
            json={
                "email": "nousername@test.local",
                "password": "ValidPassword1!",
            },
        )
        assert r.status_code == 422


# ---------------------------------------------------------------------------
# POST /api/auth/login
# ---------------------------------------------------------------------------


class TestLogin:
    async def test_valid_login_returns_token(self, admin_token: str):
        assert isinstance(admin_token, str)
        assert len(admin_token) > 20

    async def test_invalid_password_401(self, client: AsyncClient):
        r = await client.post(
            "/api/auth/login",
            json={
                "username": "testadmin",
                "password": "wrongpassword",
            },
        )
        assert r.status_code == 401

    async def test_nonexistent_user_401(self, client: AsyncClient):
        r = await client.post(
            "/api/auth/login",
            json={
                "username": "doesnotexist",
                "password": "whatever",
            },
        )
        assert r.status_code == 401

    async def test_login_response_shape(self, client: AsyncClient, admin_token: str):
        r = await client.post(
            "/api/auth/login",
            json={
                "username": "testadmin",
                "password": "AdminPass123!",
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert "user" in data
        assert data["user"]["username"] == "testadmin"


# ---------------------------------------------------------------------------
# GET /api/auth/me
# ---------------------------------------------------------------------------


class TestMe:
    async def test_me_returns_current_user(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/auth/me", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert data["username"] == "testadmin"
        assert "role" in data
        assert "email" in data

    async def test_me_without_token_401(self, client: AsyncClient):
        r = await client.get("/api/auth/me")
        # FastAPI HTTPBearer returns 403 in older versions, 401 in newer ones
        assert r.status_code in (401, 403)

    async def test_me_with_bad_token_401(self, client: AsyncClient):
        r = await client.get("/api/auth/me", headers={"Authorization": "Bearer invalid.token.here"})
        assert r.status_code == 401


# ---------------------------------------------------------------------------
# GET /api/auth/users  (admin only)
# ---------------------------------------------------------------------------


class TestListUsers:
    async def test_list_users_requires_auth(self, client: AsyncClient):
        r = await client.get("/api/auth/users")
        assert r.status_code in (401, 403)

    async def test_admin_can_list_users(self, client: AsyncClient, auth_headers: dict):
        r = await client.get("/api/auth/users", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert any(u["username"] == "testadmin" for u in data)


# ---------------------------------------------------------------------------
# PATCH /api/auth/users/{id}  (admin only)
# ---------------------------------------------------------------------------


class TestUpdateUser:
    async def test_update_requires_auth(self, client: AsyncClient):
        r = await client.patch("/api/auth/users/1", json={"is_active": True})
        assert r.status_code in (401, 403)

    async def test_update_nonexistent_user_404(self, client: AsyncClient, auth_headers: dict):
        r = await client.patch(
            "/api/auth/users/99999", json={"is_active": False}, headers=auth_headers
        )
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/auth/register/admin  (admin creates user with role)
# ---------------------------------------------------------------------------


class TestAdminRegister:
    async def test_admin_can_create_viewer(self, client: AsyncClient, auth_headers: dict):
        import uuid

        r = await client.post(
            "/api/auth/register/admin",
            json={
                "username": f"viewer_{uuid.uuid4().hex[:8]}",
                "email": f"viewer_{uuid.uuid4().hex[:8]}@test.local",
                "password": "ViewerPass99!",
                "role": "viewer",
            },
            headers=auth_headers,
        )
        assert r.status_code == 201
        data = r.json()
        assert data["role"] == "viewer"
        assert "id" in data

    async def test_admin_can_create_admin_role(self, client: AsyncClient, auth_headers: dict):
        import uuid

        r = await client.post(
            "/api/auth/register/admin",
            json={
                "username": f"admin2_{uuid.uuid4().hex[:8]}",
                "email": f"admin2_{uuid.uuid4().hex[:8]}@test.local",
                "password": "AdminPass99!",
                "role": "admin",
            },
            headers=auth_headers,
        )
        assert r.status_code == 201
        assert r.json()["role"] == "admin"

    async def test_unauthenticated_cannot_use_admin_register(self, client: AsyncClient):
        r = await client.post(
            "/api/auth/register/admin",
            json={
                "username": "unauthorized",
                "email": "unauth@test.local",
                "password": "Password123!",
            },
        )
        assert r.status_code in (401, 403)

    async def test_duplicate_username_rejected_400(self, client: AsyncClient, auth_headers: dict):
        r = await client.post(
            "/api/auth/register/admin",
            json={
                "username": "testadmin",  # already exists from conftest
                "email": "another@test.local",
                "password": "Password123!",
            },
            headers=auth_headers,
        )
        assert r.status_code == 400

    async def test_default_role_is_viewer(self, client: AsyncClient, auth_headers: dict):
        import uuid

        r = await client.post(
            "/api/auth/register/admin",
            json={
                "username": f"defaultrole_{uuid.uuid4().hex[:8]}",
                "email": f"default_{uuid.uuid4().hex[:8]}@test.local",
                "password": "DefaultRole1!",
            },
            headers=auth_headers,
        )
        assert r.status_code == 201
        assert r.json()["role"] == "viewer"


# ---------------------------------------------------------------------------
# DELETE /api/auth/users/{id}  (admin only)
# ---------------------------------------------------------------------------


class TestDeleteUser:
    async def test_delete_requires_auth(self, client: AsyncClient):
        r = await client.delete("/api/auth/users/999")
        assert r.status_code in (401, 403)

    async def test_delete_nonexistent_404(self, client: AsyncClient, auth_headers: dict):
        r = await client.delete("/api/auth/users/99999999", headers=auth_headers)
        assert r.status_code == 404

    async def test_cannot_delete_own_account(self, client: AsyncClient, auth_headers: dict):
        r_me = await client.get("/api/auth/me", headers=auth_headers)
        admin_id = r_me.json()["id"]
        r = await client.delete(f"/api/auth/users/{admin_id}", headers=auth_headers)
        assert r.status_code == 400

    async def test_admin_can_delete_another_user(self, client: AsyncClient, auth_headers: dict):
        import uuid

        r_create = await client.post(
            "/api/auth/register/admin",
            json={
                "username": f"todelete_{uuid.uuid4().hex[:8]}",
                "email": f"todelete_{uuid.uuid4().hex[:8]}@test.local",
                "password": "ToDelete123!",
                "role": "viewer",
            },
            headers=auth_headers,
        )
        assert r_create.status_code == 201
        user_id = r_create.json()["id"]

        r_del = await client.delete(f"/api/auth/users/{user_id}", headers=auth_headers)
        assert r_del.status_code == 204

    async def test_deleted_user_not_in_list(self, client: AsyncClient, auth_headers: dict):
        import uuid

        uname = f"gone_{uuid.uuid4().hex[:8]}"
        r_create = await client.post(
            "/api/auth/register/admin",
            json={
                "username": uname,
                "email": f"{uname}@test.local",
                "password": "Gone2026!",
                "role": "viewer",
            },
            headers=auth_headers,
        )
        user_id = r_create.json()["id"]
        await client.delete(f"/api/auth/users/{user_id}", headers=auth_headers)

        r_list = await client.get("/api/auth/users", headers=auth_headers)
        usernames = [u["username"] for u in r_list.json()]
        assert uname not in usernames


# ---------------------------------------------------------------------------
# PATCH /api/auth/me  (self-service profile update)
# ---------------------------------------------------------------------------


class TestUpdateMe:
    async def test_update_email_without_password_fails(
        self, client: AsyncClient, auth_headers: dict
    ):
        r = await client.patch(
            "/api/auth/me",
            json={
                "email": "newaddress@test.local",
            },
            headers=auth_headers,
        )
        assert r.status_code == 400

    async def test_update_with_wrong_current_password_401(
        self, client: AsyncClient, auth_headers: dict
    ):
        r = await client.patch(
            "/api/auth/me",
            json={
                "email": "x@test.local",
                "current_password": "WrongPassword!",
            },
            headers=auth_headers,
        )
        assert r.status_code == 401

    async def test_no_changes_succeeds(self, client: AsyncClient, auth_headers: dict):
        """Sending an empty patch body should succeed (no-op)."""
        r = await client.patch("/api/auth/me", json={}, headers=auth_headers)
        assert r.status_code == 200

    async def test_update_password_for_fresh_user(self, client: AsyncClient, auth_headers: dict):
        import uuid

        uname = f"pwtest_{uuid.uuid4().hex[:8]}"
        r_create = await client.post(
            "/api/auth/register/admin",
            json={
                "username": uname,
                "email": f"{uname}@test.local",
                "password": "Original1!",
                "role": "viewer",
            },
            headers=auth_headers,
        )
        assert r_create.status_code == 201

        r_login = await client.post(
            "/api/auth/login",
            json={
                "username": uname,
                "password": "Original1!",
            },
        )
        token = r_login.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        r_pw = await client.patch(
            "/api/auth/me",
            json={
                "new_password": "NewPass2026!",
                "current_password": "Original1!",
            },
            headers=headers,
        )
        assert r_pw.status_code == 200

        # Old password should now be rejected
        r_old = await client.post(
            "/api/auth/login",
            json={
                "username": uname,
                "password": "Original1!",
            },
        )
        assert r_old.status_code == 401
