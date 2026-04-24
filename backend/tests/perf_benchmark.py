"""
perf_benchmark.py — End-to-end load-time measurement for Purely BI backend.

Measures:
  - Cold-start vs cached timings for each page-level endpoint
  - Per-endpoint breakdown showing which component dominates
  - Direct Supabase latency (raw PostgREST query, no app server)
  - Direct Azure Blob Storage latency (container list, no app server)

Usage
-----
  # From the backend directory with the venv active:
  uv run python tests/perf_benchmark.py

  # Optional overrides (all also available as env vars or in .env):
  uv run python tests/perf_benchmark.py \
      --base-url http://127.0.0.1:8000 \
      --email you@example.com \
      --password yourpassword \
      --passes 3

Output is a colour-coded table printed to stdout.
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import textwrap
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# Optional direct-client imports — gracefully degraded if env vars are missing
# ──────────────────────────────────────────────────────────────────────────────
try:
    from supabase import create_client as _create_supabase_client  # type: ignore[import]
except ImportError:
    _create_supabase_client = None  # type: ignore[assignment]

try:
    from azure.storage.blob import BlobServiceClient as _BlobServiceClient  # type: ignore[import]
except ImportError:
    _BlobServiceClient = None  # type: ignore[assignment]


# ──────────────────────────────────────────────────────────────────────────────
# Colour helpers (ANSI; disabled on non-TTY / Windows without ANSI)
# ──────────────────────────────────────────────────────────────────────────────
_USE_COLOUR = sys.stdout.isatty()

def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOUR else text

def _green(t: str) -> str:  return _c("32", t)
def _yellow(t: str) -> str: return _c("33", t)
def _red(t: str) -> str:    return _c("31", t)
def _bold(t: str) -> str:   return _c("1",  t)
def _dim(t: str) -> str:    return _c("2",  t)


def _fmt_ms(ms: float) -> str:
    """Colour-coded milliseconds: green <500, yellow <2000, red ≥2000."""
    s = f"{ms:>8.0f} ms"
    if ms < 500:
        return _green(s)
    if ms < 2000:
        return _yellow(s)
    return _red(s)


# ──────────────────────────────────────────────────────────────────────────────
# Result container
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class BenchResult:
    label: str
    samples_ms: list[float] = field(default_factory=list)
    error: str | None = None
    status_codes: list[int] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def first_ms(self) -> float | None:
        return self.samples_ms[0] if self.samples_ms else None

    @property
    def median_ms(self) -> float | None:
        return statistics.median(self.samples_ms) if len(self.samples_ms) > 1 else None

    @property
    def min_ms(self) -> float | None:
        return min(self.samples_ms) if self.samples_ms else None


# ──────────────────────────────────────────────────────────────────────────────
# HTTP helper
# ──────────────────────────────────────────────────────────────────────────────

def _timed_get(
    client: httpx.Client,
    url: str,
    token: str,
    *,
    label: str,
    passes: int = 2,
    params: dict[str, str] | None = None,
) -> BenchResult:
    result = BenchResult(label=label)
    headers = {"Authorization": f"Bearer {token}"}
    for i in range(passes):
        t0 = time.perf_counter()
        try:
            resp = client.get(url, headers=headers, params=params, timeout=120.0)
            elapsed = (time.perf_counter() - t0) * 1000
            result.samples_ms.append(elapsed)
            result.status_codes.append(resp.status_code)
            if resp.status_code >= 400:
                result.notes.append(f"pass {i+1}: HTTP {resp.status_code}")
        except Exception as exc:
            elapsed = (time.perf_counter() - t0) * 1000
            result.samples_ms.append(elapsed)
            result.error = str(exc)
            result.notes.append(f"pass {i+1}: {type(exc).__name__}")
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Sign-in
# ──────────────────────────────────────────────────────────────────────────────

def _sign_in(base_url: str, email: str, password: str) -> str:
    """POST /api/auth/signin and return access_token."""
    t0 = time.perf_counter()
    resp = httpx.post(
        f"{base_url}/api/auth/signin",
        json={"email": email, "password": password},
        timeout=30.0,
    )
    elapsed = (time.perf_counter() - t0) * 1000
    if resp.status_code != 200:
        raise SystemExit(
            f"Sign-in failed (HTTP {resp.status_code}): {resp.text[:200]}"
        )
    data = resp.json()
    token: str = data.get("access_token") or data.get("session", {}).get("access_token", "")
    if not token:
        raise SystemExit(f"No access_token in sign-in response: {list(data.keys())}")
    print(f"  {_dim('sign-in')}  {_fmt_ms(elapsed)}")
    return token


# ──────────────────────────────────────────────────────────────────────────────
# Direct Supabase latency
# ──────────────────────────────────────────────────────────────────────────────

def _bench_supabase(passes: int) -> list[BenchResult]:
    results: list[BenchResult] = []
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY", "")

    if not url or not key:
        r = BenchResult(label="Supabase – REST ping (direct)")
        r.error = "SUPABASE_URL / SUPABASE_KEY not set"
        return [r]

    # Plain HTTP POST to REST endpoint to avoid SDK overhead
    rest_url = f"{url.rstrip('/')}/rest/v1/connector_schemas"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "count=exact",
        "Range": "0-0",  # single row only
    }

    r = BenchResult(label="Supabase – REST query (direct)")
    with httpx.Client() as client:
        for _ in range(passes):
            t0 = time.perf_counter()
            try:
                resp = client.get(rest_url, headers=headers, timeout=30.0)
                elapsed = (time.perf_counter() - t0) * 1000
                r.samples_ms.append(elapsed)
                r.status_codes.append(resp.status_code)
            except Exception as exc:
                elapsed = (time.perf_counter() - t0) * 1000
                r.samples_ms.append(elapsed)
                r.error = str(exc)
    results.append(r)
    return results


# ──────────────────────────────────────────────────────────────────────────────
# Direct Azure Blob latency
# ──────────────────────────────────────────────────────────────────────────────

def _bench_azure(passes: int) -> list[BenchResult]:
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
    container_name = os.environ.get("AZURE_STORAGE_CONTAINER") or os.environ.get("BLOB_CONTAINER_NAME", "raw")
    results: list[BenchResult] = []

    if not conn_str:
        r = BenchResult(label="Azure Blob – list blobs (direct)")
        r.error = "AZURE_STORAGE_CONNECTION_STRING not set"
        return [r]

    if _BlobServiceClient is None:
        r = BenchResult(label="Azure Blob – list blobs (direct)")
        r.error = "azure-storage-blob not installed"
        return [r]

    # Measure: initial client creation (connection pool init)
    r_create = BenchResult(label="Azure Blob – client init")
    t0 = time.perf_counter()
    try:
        service = _BlobServiceClient.from_connection_string(conn_str)
        container = service.get_container_client(container_name)
        r_create.samples_ms.append((time.perf_counter() - t0) * 1000)
        r_create.status_codes.append(0)
    except Exception as exc:
        r_create.samples_ms.append((time.perf_counter() - t0) * 1000)
        r_create.error = str(exc)
        results.append(r_create)
        return results
    results.append(r_create)

    # Measure: container.list_blobs() (first page, limit 5)
    r_list = BenchResult(label="Azure Blob – list blobs (first 5)")
    for _ in range(passes):
        t0 = time.perf_counter()
        try:
            count = 0
            for _blob in container.list_blobs():
                count += 1
                if count >= 5:
                    break
            r_list.samples_ms.append((time.perf_counter() - t0) * 1000)
            r_list.notes.append(f"{count} blob(s)")
        except Exception as exc:
            r_list.samples_ms.append((time.perf_counter() - t0) * 1000)
            r_list.error = str(exc)
    results.append(r_list)
    return results


# ──────────────────────────────────────────────────────────────────────────────
# Dashboard discovery
# ──────────────────────────────────────────────────────────────────────────────

def _find_dashboard_by_name(base_url: str, token: str, name: str) -> str | None:
    """Return the dashboard_id whose name matches *name* (case-insensitive)."""
    try:
        resp = httpx.get(
            f"{base_url}/api/dashboards",
            headers={"Authorization": f"Bearer {token}"},
            timeout=60.0,
        )
        if resp.status_code == 200:
            items = resp.json()
            if not isinstance(items, list):
                items = items.get("dashboards", [])
            target = name.lower()
            for item in items:
                if (item.get("name") or "").lower() == target:
                    return item.get("id") or item.get("dashboard_id")
            # Fall back: partial match
            for item in items:
                if target in (item.get("name") or "").lower():
                    return item.get("id") or item.get("dashboard_id")
    except Exception:
        pass
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Markdown report
# ──────────────────────────────────────────────────────────────────────────────

def _write_markdown(
    groups: list[tuple[str, list[BenchResult]]],
    passes: int,
    path: str,
    dashboard_name: str,
    base_url: str,
) -> None:
    from datetime import datetime as _dt

    lines: list[str] = []
    lines.append("# Purely BI — Load-Time Benchmark Results")
    lines.append("")
    lines.append(f"**Date:** {_dt.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**API base URL:** `{base_url}`")
    lines.append(f"**Dashboard under test:** {dashboard_name}")
    lines.append(f"**Passes per endpoint:** {passes} (Pass 1 = cold, Passes 2+ = warm)")
    lines.append("")
    lines.append(
        "> **Colour guide** — thresholds used in terminal output:\n"
        "> - 🟢 < 500 ms  · 🟡 500–1 999 ms  · 🔴 ≥ 2 000 ms"
    )

    header_cols = ["Measurement", "Pass 1 (ms)"] + [
        f"Pass {i} (ms)" for i in range(2, min(passes, 3) + 1)
    ] + ["Min (ms)", "Median (ms)"]

    def _cell(ms: float | None) -> str:
        if ms is None:
            return "—"
        icon = "🟢" if ms < 500 else ("🟡" if ms < 2000 else "🔴")
        return f"{icon} {ms:.0f}"

    for group_name, results in groups:
        lines.append("")
        lines.append(f"## {group_name}")
        lines.append("")
        lines.append(" | ".join(header_cols))
        lines.append(" | ".join(["---"] * len(header_cols)))
        for r in results:
            if r.error and not r.samples_ms:
                lines.append(
                    f"{r.label} | " + " | ".join([f"❌ {r.error[:60]}"] * (len(header_cols) - 1))
                )
                continue
            row = [r.label]
            for ms in r.samples_ms[:3]:
                row.append(_cell(ms))
            for _ in range(min(passes, 3) - len(r.samples_ms[:3])):
                row.append("—")
            row.append(_cell(r.min_ms))
            row.append(_cell(r.median_ms))
            lines.append(" | ".join(row))
            if r.error:
                lines.append(f"  > ⚠ {r.error[:120]}")
            for note in r.notes[:3]:
                lines.append(f"  > · {note}")

    lines.append("")
    lines.append("---")
    lines.append("*Generated by `tests/perf_benchmark.py`*")

    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"\nReport written → {path}")


# ──────────────────────────────────────────────────────────────────────────────
# Filter-engine microbenchmark (offline, no HTTP, no auth)
#
# Group C8: confirm that rewriting widget SQL with a FilterSpec adds
# negligible overhead compared to query execution. Runs entirely against an
# in-memory DuckDB seeded with ~50k rows so results are deterministic.
# ──────────────────────────────────────────────────────────────────────────────

def _bench_filter_engine(passes: int) -> list[BenchResult]:
    try:
        import duckdb  # type: ignore[import]
    except ImportError:
        r = BenchResult(label="filter_engine (duckdb not installed)")
        r.error = "duckdb not available"
        return [r]

    try:
        from fastapi_app.models.filters import (  # type: ignore[import]
            CategoricalFilter,
            ColumnRef,
            FilterSpec,
            TimeFilter,
            TimeRange,
        )
        from fastapi_app.services.filter_engine import apply_filters  # type: ignore[import]
    except ImportError as exc:
        r = BenchResult(label="filter_engine import error")
        r.error = str(exc)
        return [r]

    # ── Seed: ~50k orders across 60 days, 5 payment gateways ─────────────
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE shopify_orders AS
        SELECT
            (TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (s % 60) DAY
                + INTERVAL (s % 86400) SECOND) AS created_at,
            (CASE (s % 5)
                WHEN 0 THEN 'stripe' WHEN 1 THEN 'paypal'
                WHEN 2 THEN 'amex'   WHEN 3 THEN 'cash'
                ELSE 'other' END)                                AS payment_gateways,
            (CASE (s % 3) WHEN 0 THEN 'paid'
                           WHEN 1 THEN 'pending'
                           ELSE 'cancelled' END)                 AS financial_status,
            (10 + (s % 500) * 0.37)                              AS total_price
        FROM range(0, 50000) t(s);
        """
    )

    base_sql = (
        "SELECT cast(created_at AS date) AS day, "
        "       COUNT(*)          AS orders, "
        "       SUM(total_price)  AS revenue "
        "FROM shopify_orders "
        "GROUP BY 1 ORDER BY 1"
    )

    spec = FilterSpec(
        time=TimeFilter(
            column_ref=ColumnRef(table="shopify_orders", column="created_at"),
            range=TimeRange.model_validate({"from": "2026-01-15", "to": "2026-02-15"}),
        ),
        filters=[
            CategoricalFilter(
                column_ref=ColumnRef(
                    table="shopify_orders", column="payment_gateways"
                ),
                values=["stripe", "paypal", "amex"],
            )
        ],
    )

    results: list[BenchResult] = []

    # 1. Baseline: execute widget SQL with no filters.
    r_base = BenchResult(label="[filter_engine] baseline query (no filters)")
    for _ in range(passes):
        t0 = time.perf_counter()
        conn.execute(base_sql).fetchall()
        r_base.samples_ms.append((time.perf_counter() - t0) * 1000)
    results.append(r_base)

    # 2. Rewrite only: apply_filters() cost isolated from execution.
    r_rewrite = BenchResult(label="[filter_engine] SQL rewrite only (apply_filters)")
    rewritten_sql = base_sql
    rewritten_params: tuple = ()
    for _ in range(passes):
        t0 = time.perf_counter()
        rewritten_sql, rewritten_params, _ = apply_filters(
            base_sql, spec=spec, conn=conn, relationships=None, existing_params=()
        )
        r_rewrite.samples_ms.append((time.perf_counter() - t0) * 1000)
    results.append(r_rewrite)

    # 3. Execute the rewritten SQL (filters actually applied).
    r_filtered = BenchResult(label="[filter_engine] rewritten query execution")
    for _ in range(passes):
        t0 = time.perf_counter()
        conn.execute(rewritten_sql, list(rewritten_params)).fetchall()
        r_filtered.samples_ms.append((time.perf_counter() - t0) * 1000)
    results.append(r_filtered)

    # 4. End-to-end: rewrite + execute together (what hydrate_widget pays).
    r_e2e = BenchResult(label="[filter_engine] rewrite + execute (end-to-end)")
    for _ in range(passes):
        t0 = time.perf_counter()
        sql, params, _ = apply_filters(
            base_sql, spec=spec, conn=conn, relationships=None, existing_params=()
        )
        conn.execute(sql, list(params)).fetchall()
        r_e2e.samples_ms.append((time.perf_counter() - t0) * 1000)
    results.append(r_e2e)

    # Annotate the rewrite row with the overhead ratio for quick visual check.
    base_med = r_base.median_ms or r_base.first_ms or 1.0
    rw_med = r_rewrite.median_ms or r_rewrite.first_ms or 0.0
    if base_med > 0:
        pct = 100.0 * rw_med / base_med
        r_rewrite.notes.append(
            f"rewrite overhead ~ {pct:.1f}% of baseline query time"
        )
    return results


# ──────────────────────────────────────────────────────────────────────────────
# Result printer
# ──────────────────────────────────────────────────────────────────────────────

def _print_results(groups: list[tuple[str, list[BenchResult]]], passes: int) -> None:
    col_label = 42
    print()
    print(_bold("=" * 80))
    print(_bold("  PURELY BI — LOAD-TIME BENCHMARK"))
    print(_bold("=" * 80))
    print(
        f"  {'Measurement':<{col_label}}  "
        f"{'Pass 1':>10}  "
        + (f"{'Pass 2':>10}  " if passes >= 2 else "")
        + (f"{'Pass 3':>10}  " if passes >= 3 else "")
        + f"{'Min':>10}  {'Median':>10}"
    )
    print("  " + "-" * (col_label + 2 + 12 * min(passes, 3) + 24))

    for group_name, results in groups:
        print(f"\n  {_bold(_dim(group_name))}")
        for r in results:
            label_str = f"  {r.label:<{col_label}}"
            if r.error and not r.samples_ms:
                print(f"{label_str}  {_red('ERROR: ' + r.error[:50])}")
                continue

            row_parts = [label_str]
            for ms in r.samples_ms[:3]:
                row_parts.append(f"  {_fmt_ms(ms)}")
            # pad if fewer passes
            for _ in range(min(passes, 3) - len(r.samples_ms[:3])):
                row_parts.append(f"  {'—':>10}")

            min_v = r.min_ms
            med_v = r.median_ms
            row_parts.append(f"  {_fmt_ms(min_v) if min_v is not None else '':>10}")
            row_parts.append(f"  {_fmt_ms(med_v) if med_v is not None else '':>10}")

            print("".join(row_parts))

            if r.error:
                print(f"    {_red('  ⚠ ' + r.error[:70])}")
            for note in r.notes[:3]:
                print(f"    {_dim('  · ' + note)}")

    print()
    print(_bold("=" * 80))
    print(
        textwrap.dedent(f"""\
        Legend
          Pass 1  = cold (caches empty)         Passes 2+ = warm (caches populated)
          Colour  {_green('green < 500 ms')}  {_yellow('yellow < 2 000 ms')}  {_red('red ≥ 2 000 ms')}
        """)
    )


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

    parser = argparse.ArgumentParser(description="Purely BI end-to-end load-time benchmark.")
    parser.add_argument("--base-url", default=os.environ.get("API_PUBLIC_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--email",    default=os.environ.get("BENCH_EMAIL", ""))
    parser.add_argument("--password", default=os.environ.get("BENCH_PASSWORD", ""))
    parser.add_argument("--passes",       type=int, default=3, help="HTTP request passes per endpoint (default 3).")
    parser.add_argument("--dashboard-id",  default=os.environ.get("BENCH_DASHBOARD_ID", ""), help="Specific dashboard ID (looked up by name if omitted).")
    parser.add_argument("--dashboard-name", default=os.environ.get("BENCH_DASHBOARD_NAME", "Movies Overview"), help="Dashboard name to look up (default: 'Movies Overview').")
    parser.add_argument("--output",        default=os.environ.get("BENCH_OUTPUT", ""), help="Path for markdown report (default: docs/benchmark_results.md next to backend/).")
    parser.add_argument(
        "--filter-engine-only",
        action="store_true",
        help="Skip HTTP/auth and only run the offline filter-engine microbenchmark.",
    )
    args = parser.parse_args()

    passes = max(1, args.passes)

    # ── Filter-engine-only path: no HTTP, no Supabase, no Azure. ────────
    if args.filter_engine_only:
        print(_bold("\nFilter-engine microbenchmark (offline) …"))
        fe_results = _bench_filter_engine(passes)
        groups = [("FILTER ENGINE (offline DuckDB, no server)", fe_results)]
        _print_results(groups, passes)
        output_path = args.output or os.path.join(
            os.path.dirname(__file__), "..", "..", "docs",
            "benchmark_results", "benchmark_filter_engine.md",
        )
        output_path = os.path.normpath(output_path)
        _write_markdown(groups, passes, output_path, "(filter engine only)", "n/a")
        return

    if not args.email or not args.password:
        raise SystemExit(
            "Provide credentials via --email / --password flags "
            "or BENCH_EMAIL / BENCH_PASSWORD env vars (or in .env)."
        )

    base = args.base_url.rstrip("/")

    print(_bold("\nAuthenticating …"))
    token = _sign_in(base, args.email, args.password)

    # ── API endpoint benchmarks ──────────────────────────────────────────────
    api_results: list[BenchResult] = []
    with httpx.Client() as client:

        # ── Manage tab ───────────────────────────────────────────────────────
        # Primary call: list user connector configs (hits Supabase only)
        api_results.append(_timed_get(
            client, f"{base}/api/connectors",
            token, label="[Manage] list user connectors (Supabase)", passes=passes,
        ))
        # Secondary call: synced-tables metadata (Supabase + Azure blob list)
        api_results.append(_timed_get(
            client, f"{base}/api/connectors/synced-tables",
            token, label="[Manage] synced-tables metadata (Supabase + Azure)", passes=passes,
        ))

        # ── View raw tables tab ──────────────────────────────────────────────
        # Same endpoint but include Parquet inventory (start/end prompts Azure)
        from datetime import date, timedelta
        today = date.today()
        six_months_ago = today - timedelta(days=180)
        api_results.append(_timed_get(
            client, f"{base}/api/connectors/synced-tables",
            token,
            label="[Raw Tables] synced-tables + date inventory (Azure heavy)",
            passes=passes,
            params={
                "start_date": six_months_ago.isoformat(),
                "end_date": today.isoformat(),
            },
        ))

        # ── Dashboard list ───────────────────────────────────────────────────
        r_list = _timed_get(
            client, f"{base}/api/dashboards",
            token, label="[Dashboard] list dashboards (Supabase)", passes=passes,
        )
        api_results.append(r_list)

        # ── Single dashboard load ────────────────────────────────────────────
        dash_id = args.dashboard_id
        if not dash_id:
            print(_dim(f"  Looking up dashboard '{args.dashboard_name}' …"))
            dash_id = _find_dashboard_by_name(base, token, args.dashboard_name) or ""
            if dash_id:
                print(_dim(f"  Found: {dash_id}"))
            else:
                print(_red(f"  Dashboard '{args.dashboard_name}' not found."))

        if dash_id:
            api_results.append(_timed_get(
                client, f"{base}/api/dashboards/{dash_id}",
                token,
                label=f"[Dashboard] open '{args.dashboard_name}' (DuckDB hydration)",
                passes=passes,
            ))
            # With last_30_days preset (triggers date filter logic + DuckDB)
            api_results.append(_timed_get(
                client, f"{base}/api/dashboards/{dash_id}",
                token,
                label=f"[Dashboard] open '{args.dashboard_name}' last_30_days",
                passes=passes,
                params={"preset": "last_30_days"},
            ))
        else:
            r = BenchResult(label=f"[Dashboard] open '{args.dashboard_name}' (DuckDB hydration)")
            r.error = f"Dashboard '{args.dashboard_name}' not found"
            api_results.append(r)

        # ── Max data date (readiness check) ──────────────────────────────────
        api_results.append(_timed_get(
            client, f"{base}/api/dashboards/data/max-date",
            token, label="[Dashboard] max data date (DuckDB probe)", passes=passes,
        ))

        # ── Builder readiness ────────────────────────────────────────────────
        api_results.append(_timed_get(
            client, f"{base}/api/dashboards/builder/readiness",
            token, label="[Dashboard] builder readiness (blob discovery)", passes=passes,
        ))

    # ── Direct infrastructure benchmarks ────────────────────────────────────
    print(_dim("\nProbing Supabase directly …"))
    supabase_results = _bench_supabase(passes)

    print(_dim("Probing Azure Blob directly …"))
    azure_results = _bench_azure(passes)

    print(_dim("Running filter-engine microbenchmark …"))
    filter_engine_results = _bench_filter_engine(passes)

    # ── Print all results ────────────────────────────────────────────────────
    groups = [
        ("API ENDPOINTS (round-trip through FastAPI)", api_results),
        ("SUPABASE (direct PostgREST, no app server)", supabase_results),
        ("AZURE BLOB STORAGE (direct SDK, no app server)", azure_results),
        ("FILTER ENGINE (offline DuckDB microbenchmark)", filter_engine_results),
    ]
    _print_results(groups, passes)

    # ── Write markdown report ────────────────────────────────────────────────
    output_path = args.output or os.path.join(
        os.path.dirname(__file__), "..", "..", "docs", "benchmark_results.md"
    )
    output_path = os.path.normpath(output_path)
    _write_markdown(groups, passes, output_path, args.dashboard_name, base)


if __name__ == "__main__":
    main()
