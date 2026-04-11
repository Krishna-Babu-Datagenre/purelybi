"""
Reusable FastAPI dependencies for authentication.

All protected routes must derive the tenant identity from the validated JWT
(``UserProfile.id``). Never trust a ``user_id`` (or similar) sent in the request
body or query string for authorization — only use it as opaque resource IDs
scoped by ``get_current_user_dep``.
"""

from __future__ import annotations

from fastapi import Header, HTTPException, status

from fastapi_app.models.auth import UserProfile
from fastapi_app.services.auth_service import get_current_user


def parse_bearer_token(authorization: str) -> str:
    """Extract the JWT from ``Authorization: Bearer <token>``."""
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Authorization header. Expected 'Bearer <token>'.",
        )
    return token


async def get_bearer_token_dep(authorization: str = Header(...)) -> str:
    """Dependency: raw access token (e.g. for Supabase user-scoped PostgREST)."""
    return parse_bearer_token(authorization)


async def get_current_user_dep(
    authorization: str = Header(...),
) -> UserProfile:
    """Parse ``Authorization: Bearer <token>`` and return the authenticated user."""
    return get_current_user(access_token=parse_bearer_token(authorization))
