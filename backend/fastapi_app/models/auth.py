"""
Pydantic models for authentication endpoints.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, EmailStr, Field

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class UserRole(str, Enum):
    admin = "admin"
    client = "client"


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class SignUpRequest(BaseModel):
    """Email + password sign-up with optional profile fields."""

    email: EmailStr
    password: str = Field(..., min_length=8)
    full_name: str = Field(default="")


class SignInRequest(BaseModel):
    """Email + password sign-in."""

    email: EmailStr
    password: str = Field(..., min_length=1)


class RefreshRequest(BaseModel):
    """Exchange a refresh token for a new session (Supabase rotates the refresh token)."""

    refresh_token: str = Field(..., min_length=10)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class UserProfile(BaseModel):
    """Public-facing user profile returned by auth endpoints."""

    id: str
    email: str
    full_name: str | None = None
    avatar_url: str | None = None
    role: UserRole = UserRole.client


class AuthResponse(BaseModel):
    """Wrapper returned after sign-up or sign-in."""

    access_token: str
    refresh_token: str
    user: UserProfile
