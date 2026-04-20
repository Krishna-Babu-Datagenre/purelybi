"""
API routes for authentication.

Endpoints
---------
POST /api/auth/signup          – register with email + password
POST /api/auth/signin          – login with email + password
POST /api/auth/refresh         – rotate access token using refresh token
GET  /api/auth/google          – get Google OAuth redirect URL
GET  /api/auth/me              – get current user profile from token
DELETE /api/auth/account       – permanently delete the current user (auth + profile)
"""

from __future__ import annotations

import logging
import threading

from fastapi import APIRouter, Header, Query, status
from fastapi.responses import JSONResponse

from fastapi_app.models.auth import (
    AuthResponse,
    RefreshRequest,
    SignInRequest,
    SignUpRequest,
    UserProfile,
)
from fastapi_app.services.auth_service import (
    EmailConfirmationRequired,
    delete_user_account,
    get_current_user,
    get_google_oauth_url,
    refresh_with_refresh_token,
    sign_in_with_email,
    sign_up_with_email,
)
from fastapi_app.utils.auth_dep import parse_bearer_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["auth"])


def _prewarm_tenant_sandbox(user_id: str) -> None:
    """Fire-and-forget: materialise the user's DuckDB sandbox in the background.

    Called in a daemon thread immediately after sign-in so that the first
    dashboard load hits the warm pool instead of triggering an 80s cold start.
    """
    try:
        from ai.agents.sql.duckdb_sandbox import get_tenant_sandbox
        get_tenant_sandbox(user_id)
        logger.info("Pre-warm completed for tenant %s", user_id)
    except Exception:
        # Non-fatal — user will still get their dashboard, just slightly slower.
        logger.debug("Pre-warm failed for tenant %s (non-fatal)", user_id, exc_info=True)


@router.post(
    "/signup", response_model=AuthResponse, status_code=status.HTTP_201_CREATED
)
async def signup(body: SignUpRequest):
    """Register a new user with email, password, and optional name."""
    try:
        return sign_up_with_email(
            email=body.email,
            password=body.password,
            full_name=body.full_name,
        )
    except EmailConfirmationRequired as exc:
        # Not an error – user was created but must confirm their email first.
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": exc.message, "requires_confirmation": True},
        )


@router.post("/signin", response_model=AuthResponse)
async def signin(body: SignInRequest):
    """Sign in with email + password and receive tokens."""
    result = sign_in_with_email(email=body.email, password=body.password)
    # Kick off DuckDB materialization in background so the first dashboard load
    # hits the warm pool instead of a cold 80s download.
    threading.Thread(
        target=_prewarm_tenant_sandbox,
        args=(result.user.id,),
        daemon=True,
    ).start()
    return result


@router.post("/refresh", response_model=AuthResponse)
async def refresh_session(body: RefreshRequest):
    """Rotate access token using the Supabase refresh token (client should persist both)."""
    return refresh_with_refresh_token(body.refresh_token)


@router.get("/google")
async def google_oauth(redirect_to: str = Query(default=None)):
    """Return the Google OAuth URL for the frontend to redirect to."""
    url = get_google_oauth_url(redirect_to=redirect_to)
    return {"url": url}


@router.get("/me", response_model=UserProfile)
async def me(authorization: str = Header(...)):
    """Return the profile for the currently authenticated user.

    Expects ``Authorization: Bearer <access_token>`` header.
    """
    return get_current_user(access_token=parse_bearer_token(authorization))


@router.delete("/account", status_code=status.HTTP_204_NO_CONTENT)
async def delete_account(authorization: str = Header(...)):
    """Permanently delete the current user from Supabase Auth and related rows."""
    delete_user_account(access_token=parse_bearer_token(authorization))
