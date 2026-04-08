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

from supabase import Client, ClientOptions, create_client
from supabase.lib.client_options import DEFAULT_HEADERS


def get_supabase_client() -> Client:
    """Return a Supabase client configured with the anon (public) key."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_KEY environment variables must be set."
        )
    return create_client(url, key)


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
    """Return a Supabase client using the service_role key (bypasses RLS)."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables must be set."
        )
    return create_client(url, key)
