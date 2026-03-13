"""
Authentication router.

Endpoints
---------
POST  /api/auth/register          — create a new user (admin only once 1+ users exist, or open for first user)
POST  /api/auth/login             — get a JWT access token
GET   /api/auth/me                — current user info (requires token)
GET   /api/auth/users             — list all users (admin only)
PATCH /api/auth/users/{user_id}   — update user role / active status (admin only)
DELETE /api/auth/users/{user_id}  — delete a user (admin only)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.database import get_session
from api.models import User, UserRole
from api.services.auth_service import (
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)
from pydantic import BaseModel

router = APIRouter()
_bearer = HTTPBearer()


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str


class AdminRegisterRequest(BaseModel):
    username: str
    email: str
    password: str
    role: UserRole = UserRole.viewer


class LoginRequest(BaseModel):
    username: str
    password: str


class UserPublic(BaseModel):
    id: int
    username: str
    email: str
    role: UserRole
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime]


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserPublic


class UpdateUserRequest(BaseModel):
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None


class UpdateProfileRequest(BaseModel):
    email: Optional[str] = None
    current_password: Optional[str] = None
    new_password: Optional[str] = None


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

async def _current_user(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
    session: AsyncSession = Depends(get_session),
) -> User:
    """Decode JWT, load User row. Raise 401 on any failure."""
    try:
        payload = decode_access_token(creds.credentials)
        user_id: int = int(payload["sub"])
    except (JWTError, KeyError, ValueError):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = await session.get(User, user_id)
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")
    return user


async def _require_admin(user: User = Depends(_current_user)) -> User:
    if user.role != UserRole.admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user


def _to_public(u: User) -> UserPublic:
    return UserPublic(
        id=u.id,
        username=u.username,
        email=u.email,
        role=u.role,
        is_active=u.is_active,
        created_at=u.created_at,
        last_login=u.last_login,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/register", response_model=UserPublic, status_code=status.HTTP_201_CREATED)
async def register(
    body: RegisterRequest,
    session: AsyncSession = Depends(get_session),
):
    """
    Register a new user.
    - If NO users exist yet → open registration (creates the first admin).
    - Otherwise → requires an existing admin token.
    """
    # Check if any users exist
    existing_count_result = await session.exec(select(User))
    all_users = existing_count_result.all()
    is_first_user = len(all_users) == 0

    if not is_first_user:
        # Require admin auth for subsequent registrations
        # (We can't use Depends here because we need conditional auth, so we check the header manually)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Registration is closed. Ask an admin to create your account.",
        )

    # Check username / email uniqueness
    result = await session.exec(select(User).where(User.username == body.username))
    if result.first():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username already taken")

    result2 = await session.exec(select(User).where(User.email == body.email))
    if result2.first():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")

    user = User(
        username=body.username,
        email=body.email,
        hashed_password=hash_password(body.password),
        # First user is always admin
        role=UserRole.admin if is_first_user else UserRole.viewer,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return _to_public(user)


@router.post("/register/admin", response_model=UserPublic, status_code=status.HTTP_201_CREATED)
async def admin_create_user(
    body: AdminRegisterRequest,
    _: User = Depends(_require_admin),
    session: AsyncSession = Depends(get_session),
):
    """Admin-only endpoint to create additional users with a specified role."""
    result = await session.exec(select(User).where(User.username == body.username))
    if result.first():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username already taken")

    result2 = await session.exec(select(User).where(User.email == body.email))
    if result2.first():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")

    user = User(
        username=body.username,
        email=body.email,
        hashed_password=hash_password(body.password),
        role=body.role,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return _to_public(user)


@router.post("/login", response_model=LoginResponse)
async def login(body: LoginRequest, session: AsyncSession = Depends(get_session)):
    """Verify credentials, return a JWT access token + user info."""
    result = await session.exec(select(User).where(User.username == body.username))
    user = result.first()

    if not user or not verify_password(body.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account deactivated")

    # Update last_login
    user.last_login = datetime.now(tz=timezone.utc)
    session.add(user)
    await session.commit()
    await session.refresh(user)

    token = create_access_token({"sub": str(user.id), "role": user.role.value})
    return LoginResponse(access_token=token, user=_to_public(user))


@router.get("/me", response_model=UserPublic)
async def me(current: User = Depends(_current_user)):
    """Return the currently authenticated user's info."""
    return _to_public(current)


@router.patch("/me", response_model=UserPublic)
async def update_me(
    body: UpdateProfileRequest,
    current: User = Depends(_current_user),
    session: AsyncSession = Depends(get_session),
):
    """
    Let the logged-in user update their own email and/or password.
    Changing either requires the current password for verification.
    """
    changing_email = body.email is not None and body.email != current.email
    changing_password = body.new_password is not None

    if changing_email or changing_password:
        if not body.current_password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Current password is required to make changes",
            )
        if not verify_password(body.current_password, current.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Current password is incorrect",
            )

    if changing_email:
        result = await session.exec(select(User).where(User.email == body.email))
        if result.first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already in use",
            )
        current.email = body.email  # type: ignore[assignment]

    if changing_password:
        if len(body.new_password) < 8:  # type: ignore[arg-type]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="New password must be at least 8 characters",
            )
        current.hashed_password = hash_password(body.new_password)  # type: ignore[arg-type]

    session.add(current)
    await session.commit()
    await session.refresh(current)
    return _to_public(current)


@router.get("/users", response_model=list[UserPublic])
async def list_users(
    _: User = Depends(_require_admin),
    session: AsyncSession = Depends(get_session),
):
    """List all users. Admin only."""
    result = await session.exec(select(User).order_by(User.created_at))
    return [_to_public(u) for u in result.all()]


@router.patch("/users/{user_id}", response_model=UserPublic)
async def update_user(
    user_id: int,
    body: UpdateUserRequest,
    admin: User = Depends(_require_admin),
    session: AsyncSession = Depends(get_session),
):
    """Update a user's role or active status. Admin only."""
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    if body.role is not None:
        user.role = body.role
    if body.is_active is not None:
        user.is_active = body.is_active

    session.add(user)
    await session.commit()
    await session.refresh(user)
    return _to_public(user)


@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: int,
    admin: User = Depends(_require_admin),
    session: AsyncSession = Depends(get_session),
):
    """Delete a user. Admin only. Cannot delete yourself."""
    if admin.id == user_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete your own account")

    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    await session.delete(user)
    await session.commit()
