"""
Authentication service – wraps Supabase Auth operations.

All password handling is delegated to Supabase Auth (bcrypt-hashed internally
in auth.users). The profiles table only stores app-level metadata (name,
avatar, role) — never passwords.
"""

from __future__ import annotations

from datetime import datetime, timezone
from fastapi import HTTPException, status
from supabase_auth.errors import AuthApiError

from fastapi_app.models.auth import AuthResponse, ProfileUpdateRequest, SubscriptionPlan, UserProfile, UserRole
from fastapi_app.settings import AUTH_SIGNUP_EMAIL_REDIRECT_TO
from fastapi_app.utils.supabase_client import (
    get_supabase_admin_client,
    get_supabase_client,
)

# ---------------------------------------------------------------------------
# In-process profile cache — profile data (name, avatar, role) is stable and
# doesn't need a Supabase round-trip on every authenticated request.
# Cache entries expire after 60 seconds.
# ---------------------------------------------------------------------------
_PROFILE_CACHE_TTL_SECONDS = 60
_profile_cache: dict[str, tuple[dict, datetime]] = {}


class EmailConfirmationRequired(Exception):
    """Raised when a new user must confirm their email before signing in."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


def _build_user_profile(
    user: dict, profile_row: dict | None = None, dashboard_count: int = 0, active_connector_count: int = 0
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
        subscription_tier=(profile_row or {}).get("subscription_tier", None),
        ai_credits_balance=(profile_row or {}).get("ai_credits_balance", 0),
        trial_ends_at=(profile_row or {}).get("trial_ends_at", None),
        dashboard_count=dashboard_count,
        active_connector_count=active_connector_count,
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
        .select("full_name, avatar_url, role, ai_credits_balance, trial_ends_at, subscription_tier(*)")
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
        .select("full_name, avatar_url, role, ai_credits_balance, trial_ends_at, subscription_tier(*)")
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
        .select("full_name, avatar_url, role, ai_credits_balance, trial_ends_at, subscription_tier(*)")
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
    user_id = str(user.id)

    # Check in-process profile cache to avoid a Supabase round-trip on every request.
    now = datetime.now(timezone.utc)
    cached = _profile_cache.get(user_id)
    if cached is not None:
        profile_row, cached_at = cached
        if (now - cached_at).total_seconds() < _PROFILE_CACHE_TTL_SECONDS:
            return _build_user_profile(
                user=user.__dict__ if hasattr(user, "__dict__") else dict(user),
                profile_row=profile_row,
            )

    # Use the admin client for the profile query because the anon client
    # has no active session here (get_user doesn't set one) and RLS would
    # block the read.
    admin = get_supabase_admin_client()
    profile_rows = (
        admin.table("profiles")
        .select("full_name, avatar_url, role, ai_credits_balance, trial_ends_at, subscription_tier(*)")
        .eq("id", user_id)
        .limit(1)
        .execute()
    )
    profile_row = profile_rows.data[0] if profile_rows.data else None

    # Fetch resource counts to hydrate the frontend limits UI
    db_res = admin.table("dashboards").select("id", count="exact").eq("user_id", user_id).execute()
    dashboard_count = db_res.count if db_res.count is not None else 0
    
    conn_res = admin.table("user_connector_configs").select("id", count="exact").eq("user_id", user_id).eq("is_active", True).execute()
    active_connector_count = conn_res.count if conn_res.count is not None else 0

    _profile_cache[user_id] = (profile_row or {}, now)

    return _build_user_profile(
        user=user.__dict__ if hasattr(user, "__dict__") else dict(user),
        profile_row=profile_row,
        dashboard_count=dashboard_count,
        active_connector_count=active_connector_count,
    )


# ---------------------------------------------------------------------------
# Get AI credits balance (lightweight, cache-free)
# ---------------------------------------------------------------------------


def get_user_credits(access_token: str) -> int:
    """Return the current AI credits balance for the authenticated user.

    Unlike ``get_current_user`` this deliberately **bypasses** the profile
    cache so the frontend always receives the freshest value after an agent
    turn deducts credits.
    """
    supabase = get_supabase_client()
    try:
        res = supabase.auth.get_user(access_token)
    except AuthApiError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
        )

    user_id = str(res.user.id)
    admin = get_supabase_admin_client()
    row = (
        admin.table("profiles")
        .select("ai_credits_balance")
        .eq("id", user_id)
        .limit(1)
        .execute()
    )
    if row.data:
        return int(row.data[0].get("ai_credits_balance", 0))
    return 0


# ---------------------------------------------------------------------------
# Delete account (auth user + cascaded public data)
# ---------------------------------------------------------------------------


def delete_user_account(access_token: str) -> None:
    """Permanently delete the authenticated user from Supabase Auth.

    The ``profiles`` row and other ``public`` tables referencing ``auth.users``
    with ``ON DELETE CASCADE`` are removed automatically by PostgreSQL.
    """
    supabase = get_supabase_client()
    try:
        res = supabase.auth.get_user(access_token)
    except AuthApiError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
        )

    user = res.user
    user_id = getattr(user, "id", None)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid session",
        )

    admin = get_supabase_admin_client()
    try:
        admin.auth.admin.delete_user(str(user_id), should_soft_delete=False)
    except AuthApiError as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Could not delete account: {e}",
        )


# ---------------------------------------------------------------------------
# Update profile (partial patch)
# ---------------------------------------------------------------------------


def update_user_profile(access_token: str, body: ProfileUpdateRequest) -> UserProfile:
    """Patch the authenticated user's profile row and return the updated profile.

    Only non-``None`` fields from ``body`` are written. The in-process profile
    cache is invalidated so the next ``get_current_user`` call fetches fresh data.
    """
    supabase = get_supabase_client()
    try:
        res = supabase.auth.get_user(access_token)
    except AuthApiError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
        )

    user_id = str(res.user.id)

    # Build the update payload from non-None fields
    update_data: dict[str, object] = {}
    if body.full_name is not None:
        update_data["full_name"] = body.full_name

    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update.",
        )

    admin = get_supabase_admin_client()
    admin.table("profiles").update(update_data).eq("id", user_id).execute()

    # Invalidate the profile cache so subsequent reads are fresh
    _profile_cache.pop(user_id, None)

    # Return the full refreshed profile
    return get_current_user(access_token)


# ---------------------------------------------------------------------------
# List subscription plans
# ---------------------------------------------------------------------------


def list_subscription_plans() -> list[SubscriptionPlan]:
    """Return all subscription plans ordered by credit tier (ascending)."""
    admin = get_supabase_admin_client()
    rows = (
        admin.table("subscription_plans")
        .select("id, tier_name, max_data_sources, max_storage_mb, max_dashboards, included_ai_credits, min_sync_frequency_minutes")
        .order("included_ai_credits", desc=False)
        .execute()
    )
    return [SubscriptionPlan(**row) for row in rows.data]

