"""
Seed script – creates initial admin and client users in Supabase.

Usage:
    python -m scripts.seed_users

Requires these environment variables:
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY   (service role key, NOT the anon key)

The service-role key is needed to create users via the Admin API
(bypasses email confirmation and RLS).
"""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

load_dotenv()

from supabase import create_client  # noqa: E402


def _get_admin_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env")
        sys.exit(1)
    return create_client(url, key)


SEED_USERS = [
    {
        "email": "admin@biagent.app",
        "password": "Admin@12345",
        "full_name": "BI Agent Admin",
        "role": "admin",
    },
    {
        "email": "dravya@biagent.app",
        "password": "Dravya@12345",
        "full_name": "Dravya Store",
        "role": "client",
    },
]


def seed():
    supabase = _get_admin_client()

    for user_data in SEED_USERS:
        email = user_data["email"]
        print(f"Creating user: {email} ...", end=" ")

        # Create auth user (idempotent — will error if already exists)
        try:
            res = supabase.auth.admin.create_user(
                {
                    "email": email,
                    "password": user_data["password"],
                    "email_confirm": True,  # auto-confirm so they can sign in immediately
                    "user_metadata": {
                        "full_name": user_data["full_name"],
                    },
                }
            )
            user_id = res.user.id
            print(f"created (id={user_id}).")
        except Exception as e:
            err = str(e)
            if "already been registered" in err or "already exists" in err:
                print("already exists, skipping auth creation.")
                # Look up existing user to get ID
                users = supabase.auth.admin.list_users()
                user_id = None
                for u in users:
                    if u.email == email:
                        user_id = u.id
                        break
                if not user_id:
                    print(f"  WARNING: could not find existing user {email}")
                    continue
            else:
                print(f"ERROR: {e}")
                continue

        # Update the profile role (trigger auto-creates the row, but role defaults to 'client')
        if user_data["role"] != "client":
            print(f"  Setting role to '{user_data['role']}' ...")
            supabase.table("profiles").update({"role": user_data["role"]}).eq(
                "id", str(user_id)
            ).execute()

        print(f"  Done: {email} ({user_data['role']})")

    print("\nSeed complete.")


if __name__ == "__main__":
    seed()
