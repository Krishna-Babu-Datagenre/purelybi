"""
Supabase client factories.

**Which client to use**

- ``get_supabase_client()`` — anon (public) key. Use for **auth flows** only
  (sign-up, sign-in, OAuth URL) where the client acts without a logged-in session.

- ``get_supabase_user_client(access_token)`` — anon key + the user’s JWT in
  headers. PostgREST applies **RLS** as that user. Use when the backend should
  mirror direct client access under RLS (rare today; prefer admin + explicit
  ``user_id`` checks for server routes).

- ``get_supabase_admin_client()`` — **service_role** key. Bypasses RLS. Use only
  in trusted server code **after** the caller identity is established from a
  validated JWT (e.g. ``get_current_user_dep``). Always scope queries with
  ``.eq("user_id", user.id)`` (or equivalent) — never trust client-supplied user
  ids for authorization.
"""

from __future__ import annotations

import os
import threading

from supabase import Client, ClientOptions, create_client
from supabase.lib.client_options import DEFAULT_HEADERS

# ---------------------------------------------------------------------------
# Module-level singletons for Supabase clients.
# Neither the anon client nor the admin client carries per-user state — they
# are safe to share across all requests for the process lifetime.  Creating a
# new client on every call costs ~2-4 s (HTTP client init inside the SDK)
# which dominated every API endpoint's latency.
# ---------------------------------------------------------------------------
_anon_client: "Client | None" = None
_anon_client_lock = threading.Lock()
_admin_client: "Client | None" = None
_admin_client_lock = threading.Lock()


def get_supabase_client() -> Client:
    """Return a **singleton** Supabase client configured with the anon (public) key.

    The anon client carries no user-specific session state — it is safe to
    share across requests. Thread-safe via double-checked locking.
    """
    global _anon_client
    if _anon_client is not None:
        return _anon_client
    with _anon_client_lock:
        if _anon_client is not None:
            return _anon_client
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_KEY environment variables must be set."
            )
        _anon_client = create_client(url, key)
        return _anon_client


def get_supabase_user_client(access_token: str) -> Client:
    """Return a Supabase client that sends the user JWT to PostgREST (RLS applies)."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_KEY environment variables must be set."
        )
    headers = dict(DEFAULT_HEADERS)
    headers["Authorization"] = f"Bearer {access_token}"
    options = ClientOptions(headers=headers)
    return create_client(url, key, options)


def get_supabase_admin_client() -> Client:
    """Return a **singleton** Supabase client using the service_role key (bypasses RLS).

    The client is created once and reused for the process lifetime — initialising
    a new client per-request added ~4 s of overhead to every API call.
    Thread-safe via double-checked locking.
    """
    global _admin_client
    # Fast path: already initialised.
    if _admin_client is not None:
        return _admin_client
    with _admin_client_lock:
        # Re-check after acquiring lock (another thread may have initialised it).
        if _admin_client is not None:
            return _admin_client
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        if not url or not key:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables must be set."
            )
        _admin_client = create_client(url, key)
        return _admin_client
