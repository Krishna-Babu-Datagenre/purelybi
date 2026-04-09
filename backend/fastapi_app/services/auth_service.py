"""
Authentication service – wraps Supabase Auth operations.

All password handling is delegated to Supabase Auth (bcrypt-hashed internally
in auth.users). The profiles table only stores app-level metadata (name,
avatar, role) — never passwords.
"""

from __future__ import annotations

from fastapi import HTTPException, status
from supabase_auth.errors import AuthApiError

from fastapi_app.models.auth import AuthResponse, UserProfile, UserRole
from fastapi_app.settings import AUTH_SIGNUP_EMAIL_REDIRECT_TO
from fastapi_app.utils.supabase_client import (
    get_supabase_admin_client,
    get_supabase_client,
)


class EmailConfirmationRequired(Exception):
    """Raised when a new user must confirm their email before signing in."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


def _build_user_profile(
    user: dict, profile_row: dict | None = None
) -> UserProfile:
    """Build a UserProfile from Supabase auth user + optional profiles row."""
    meta = user.get("user_metadata") or {}
    return UserProfile(
        id=user["id"],
        email=user.get("email", ""),
        full_name=(profile_row or {}).get("full_name")
        or meta.get("full_name", ""),
        avatar_url=(profile_row or {}).get("avatar_url")
        or meta.get("avatar_url"),
        role=(profile_row or {}).get("role", UserRole.client),
    )


def _build_auth_response(
    session: dict, user: dict, profile_row: dict | None = None
) -> AuthResponse:
    """Construct an AuthResponse from Supabase session + user objects."""
    return AuthResponse(
        access_token=session["access_token"],
        refresh_token=session.get("refresh_token", ""),
        user=_build_user_profile(user, profile_row),
    )


# ---------------------------------------------------------------------------
# Sign-up (email + password)
# ---------------------------------------------------------------------------


def sign_up_with_email(
    email: str, password: str, full_name: str = ""
) -> AuthResponse:
    """Register a new user via email + password.

    Supabase Auth creates the auth.users row; the on_auth_user_created
    trigger automatically inserts a profiles row.
    """
    supabase = get_supabase_client()
    signup_options: dict[str, object] = {"data": {"full_name": full_name}}
    if AUTH_SIGNUP_EMAIL_REDIRECT_TO:
        signup_options["email_redirect_to"] = AUTH_SIGNUP_EMAIL_REDIRECT_TO
    try:
        res = supabase.auth.sign_up(
            {
                "email": email,
                "password": password,
                "options": signup_options,
            }
        )
    except AuthApiError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    if not res.session:
        # Email confirmation is enabled — user exists but can't sign in yet.
        # Raise a domain-level exception (not HTTPException) so the router
        # can return a proper 200 success response, not an error.
        raise EmailConfirmationRequired(
            "Sign-up successful. Please check your email to confirm your account."
        )

    user = res.user
    session = res.session

    # Fetch the freshly-created profile row (trigger should have created it)
    profile_rows = (
        supabase.table("profiles")
        .select("full_name, avatar_url, role")
        .eq("id", user.id)
        .limit(1)
        .execute()
    ).data
    profile_row = profile_rows[0] if profile_rows else None

    return _build_auth_response(
        session=session.__dict__
        if hasattr(session, "__dict__")
        else dict(session),
        user=user.__dict__ if hasattr(user, "__dict__") else dict(user),
        profile_row=profile_row,
    )


# ---------------------------------------------------------------------------
# Sign-in (email + password)
# ---------------------------------------------------------------------------


def sign_in_with_email(email: str, password: str) -> AuthResponse:
    """Authenticate an existing user via email + password."""
    supabase = get_supabase_client()
    try:
        res = supabase.auth.sign_in_with_password(
            {"email": email, "password": password}
        )
    except AuthApiError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
        )

    user = res.user
    session = res.session

    profile_rows = (
        supabase.table("profiles")
        .select("full_name, avatar_url, role")
        .eq("id", user.id)
        .limit(1)
        .execute()
    ).data
    profile_row = profile_rows[0] if profile_rows else None

    return _build_auth_response(
        session=session.__dict__
        if hasattr(session, "__dict__")
        else dict(session),
        user=user.__dict__ if hasattr(user, "__dict__") else dict(user),
        profile_row=profile_row,
    )


def refresh_with_refresh_token(refresh_token: str) -> AuthResponse:
    """Issue new access + refresh tokens using a valid Supabase refresh token."""
    supabase = get_supabase_client()
    try:
        res = supabase.auth.refresh_session(refresh_token)
    except AuthApiError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
        )
    if not res.session or not res.user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh failed",
        )
    session = res.session
    user = res.user
    session_dict = (
        session.model_dump()
        if hasattr(session, "model_dump")
        else session.__dict__
        if hasattr(session, "__dict__")
        else dict(session)
    )
    user_dict = (
        user.model_dump()
        if hasattr(user, "model_dump")
        else user.__dict__
        if hasattr(user, "__dict__")
        else dict(user)
    )

    profile_rows = (
        supabase.table("profiles")
        .select("full_name, avatar_url, role")
        .eq("id", user_dict["id"])
        .limit(1)
        .execute()
    ).data
    profile_row = profile_rows[0] if profile_rows else None

    return _build_auth_response(
        session=session_dict,
        user=user_dict,
        profile_row=profile_row,
    )


# ---------------------------------------------------------------------------
# Google OAuth – return the redirect URL
# ---------------------------------------------------------------------------


def get_google_oauth_url(redirect_to: str | None = None) -> str:
    """Return the Supabase-generated Google OAuth URL.

    The frontend should redirect the user to this URL. After Google
    authenticates the user, Supabase redirects back to `redirect_to`
    with access/refresh tokens in the URL fragment.
    """
    supabase = get_supabase_client()
    try:
        res = supabase.auth.sign_in_with_oauth(
            {
                "provider": "google",
                "options": {"redirect_to": redirect_to} if redirect_to else {},
            }
        )
    except AuthApiError as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"OAuth provider error: {e}",
        )
    return res.url


# ---------------------------------------------------------------------------
# Get current user from access token
# ---------------------------------------------------------------------------


def get_current_user(access_token: str) -> UserProfile:
    """Validate an access token and return the user profile."""
    supabase = get_supabase_client()
    try:
        res = supabase.auth.get_user(access_token)
    except AuthApiError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
        )

    user = res.user

    # Use the admin client for the profile query because the anon client
    # has no active session here (get_user doesn't set one) and RLS would
    # block the read.
    admin = get_supabase_admin_client()
    profile_rows = (
        admin.table("profiles")
        .select("full_name, avatar_url, role")
        .eq("id", user.id)
        .limit(1)
        .execute()
    ).data
    profile_row = profile_rows[0] if profile_rows else None

    return _build_user_profile(
        user=user.__dict__ if hasattr(user, "__dict__") else dict(user),
        profile_row=profile_row,
    )
