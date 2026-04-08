# Supabase schema bootstrap

## Migration order

1. Create a Supabase project (dev/staging first).
2. In **SQL Editor**, run `1-create-tables.sql` as a single script on an empty `public` schema (new project or after dropping prior objects). The script creates enums, tables, triggers, and RLS policies.
3. Load seed data for dashboard templates if needed (see `template_dashboard_scripts/` and any project-specific seed steps).
4. Run `python -m scripts.seed_users` from `backend/` if you need local test accounts (requires `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` in `.env`).

## Rollback

There is no automated down migration. For a throwaway dev project, recreate the project or drop objects in reverse dependency order (children before parents), e.g. `widgets` → `dashboards` → … → `profiles`, then types. Dropping `public` schema contents is destructive; prefer a new Supabase project for a clean baseline.

## Refreshing `query_results/public_schema.json`

1. Open `queries/public_schema.sql` in the SQL Editor and run it against your project.
2. Export the result as JSON (Supabase SQL Editor: use the result export, or copy rows into a JSON array) and save as `backend/supabase/query_results/public_schema.json` to match the checklist reference dump.

## Environment variables (backend)

| Variable | Purpose |
|----------|---------|
| `SUPABASE_URL` | Project API URL (`https://<project-ref>.supabase.co`). |
| `SUPABASE_KEY` | Anon/public key (browser-safe; used by `get_supabase_client()`). |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key (server only; bypasses RLS; used by admin client and seed script). |

Copy `backend/.env-example` to `backend/.env` and fill in values from **Project Settings → API**.

## Secrets in connector tables

`user_connector_configs.config` and `oauth_meta` may hold credentials. Before production, choose and document one of: Supabase Vault, application-level encryption, or column encryption—see the comment block at the end of `1-create-tables.sql`.
