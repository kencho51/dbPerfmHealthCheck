"""
Authentication utilities: password hashing (bcrypt) and JWT creation/verification.

Environment variables required:
    JWT_SECRET   — random secret for signing tokens (min 32 chars)
    JWT_EXPIRE_MINUTES — optional, default 60 * 24 (24 hours)
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import bcrypt
from jose import jwt

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

JWT_SECRET: str = os.getenv("JWT_SECRET", "change-me-in-production-please-use-a-long-random-string")
JWT_ALGORITHM: str = "HS256"
JWT_EXPIRE_MINUTES: int = int(os.getenv("JWT_EXPIRE_MINUTES", str(60 * 24)))  # 24 h default


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------


def hash_password(plain: str) -> str:
    """Return bcrypt hash of *plain* password."""
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    """Return True if *plain* matches the stored *hashed* bcrypt digest."""
    return bcrypt.checkpw(plain.encode(), hashed.encode())


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------


def create_access_token(payload: dict) -> str:
    """
    Sign a JWT containing *payload* plus an exp claim.

    The caller should include at minimum: ``{"sub": str(user_id), "role": role_value}``.
    """
    to_encode = payload.copy()
    expire = datetime.now(tz=UTC) + timedelta(minutes=JWT_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    """
    Decode and verify a JWT.  Raises ``jose.JWTError`` on invalid/expired tokens.
    """
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
