"""Optional Docker-based Airbyte connector check/discover (local dev)."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from typing import Any

from ai.agents.onboarding.infra.azure_job_runner import (
    run_onboarding_aca_job,
    run_onboarding_docker_native_job,
)
from fastapi_app.settings import DOCKER_IMAGE_LANGUAGES, ONBOARDING_DOCKER_EXECUTION_MODE


def _use_azure_job_mode() -> bool:
    return ONBOARDING_DOCKER_EXECUTION_MODE == "azure_job"


def _lookup_connector_language(docker_image: str) -> str:
    """Look up the connector language from ``connector_schemas`` in Supabase.

    Returns the language string (e.g. ``"java"``, ``"python"``, ``"manifest-only"``)
    or ``"unknown"`` if not found.
    """
    try:
        from fastapi_app.utils.supabase_client import get_supabase_admin_client

        docker_repo = docker_image.split(":")[0]
        client = get_supabase_admin_client()
        rows = (
            client.table("connector_schemas")
            .select("language")
            .eq("docker_repository", docker_repo)
            .limit(1)
            .execute()
        ).data
        if rows:
            return rows[0].get("language", "unknown")
    except Exception:
        pass
    return "unknown"


def _should_use_docker_native(docker_image: str) -> bool:
    """Return True if this connector should use the official Docker image."""
    language = _lookup_connector_language(docker_image)
    return language in DOCKER_IMAGE_LANGUAGES


def docker_check_connection(docker_image: str, config: dict[str, Any]) -> tuple[bool, str]:
    if _use_azure_job_mode():
        if _should_use_docker_native(docker_image):
            ok, msg, _streams = run_onboarding_docker_native_job(
                action="check",
                docker_image=docker_image,
                config=config,
            )
            return ok, msg
        return run_onboarding_aca_job(
            action="check",
            docker_image=docker_image,
            config=config,
        )
    clean = {k: v for k, v in config.items() if not str(k).startswith("__")}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(clean, f)
        config_path = f.name

    try:
        cmd = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{config_path}:/tmp/config.json",
            docker_image,
            "check",
            "--config",
            "/tmp/config.json",
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120, encoding="utf-8", errors="replace"
        )
        for line in (result.stdout or "").strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                if msg.get("type") == "CONNECTION_STATUS":
                    status = msg.get("connectionStatus", {})
                    if status.get("status") == "SUCCEEDED":
                        return True, "Connection check succeeded!"
                    reason = status.get("message", "Unknown error")
                    return False, f"Connection check failed: {reason}"
            except json.JSONDecodeError:
                continue
        if result.returncode == 0:
            return True, "Connection check passed (no status message parsed)."
        return (
            False,
            f"Docker exited with code {result.returncode}:\n{(result.stderr or '')[:500]}",
        )
    except subprocess.TimeoutExpired:
        return False, "Connection check timed out (120s)."
    except FileNotFoundError:
        return False, "Docker is not installed or not on PATH."
    finally:
        os.unlink(config_path)


def docker_discover_streams(
    docker_image: str, config: dict[str, Any]
) -> tuple[bool, list[str], str]:
    if _use_azure_job_mode():
        if _should_use_docker_native(docker_image):
            ok, msg, discovered = run_onboarding_docker_native_job(
                action="discover",
                docker_image=docker_image,
                config=config,
            )
            return ok, discovered, msg
        else:
            ok, msg = run_onboarding_aca_job(
                action="discover",
                docker_image=docker_image,
                config=config,
            )
            return ok, [], msg
    clean = {k: v for k, v in config.items() if not str(k).startswith("__")}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(clean, f)
        config_path = f.name

    try:
        cmd = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{config_path}:/tmp/config.json",
            docker_image,
            "discover",
            "--config",
            "/tmp/config.json",
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=180, encoding="utf-8", errors="replace"
        )
        streams: list[str] = []
        for line in (result.stdout or "").strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                if msg.get("type") == "CATALOG":
                    catalog = msg.get("catalog", {})
                    for stream_obj in catalog.get("streams", []):
                        name = stream_obj.get("name") or stream_obj.get("stream", {}).get(
                            "name"
                        )
                        if name:
                            streams.append(name)
            except json.JSONDecodeError:
                continue
        if streams:
            return True, sorted(streams), f"Discovered {len(streams)} streams."
        if result.returncode != 0:
            return (
                False,
                [],
                f"Discover failed (exit {result.returncode}):\n{(result.stderr or '')[:500]}",
            )
        return False, [], "No catalog found in discover output."
    except subprocess.TimeoutExpired:
        return False, [], "Discover timed out (180s)."
    except FileNotFoundError:
        return False, [], "Docker is not installed or not on PATH."
    finally:
        os.unlink(config_path)


def _clean_connector_config(config: dict[str, Any]) -> dict[str, Any]:
    """Strip internal keys (``__*``) before passing config to Docker connector CLI."""
    return {k: v for k, v in config.items() if not str(k).startswith("__")}


def docker_discover_catalog(
    docker_image: str, config: dict[str, Any]
) -> tuple[bool, dict[str, Any] | None, str]:
    if _use_azure_job_mode():
        if _should_use_docker_native(docker_image):
            ok, msg, _streams = run_onboarding_docker_native_job(
                action="discover_catalog",
                docker_image=docker_image,
                config=config,
            )
        else:
            ok, msg = run_onboarding_aca_job(
                action="discover_catalog",
                docker_image=docker_image,
                config=config,
            )
        return ok, None, msg
    """Run ``discover`` and return the Airbyte ``catalog`` dict from the first CATALOG message."""
    clean = _clean_connector_config(config)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(clean, f)
        config_path = f.name

    try:
        cmd = [
            "docker",
            "run",
            "--rm",
            "-e",
            "AIRBYTE_ENABLE_UNSAFE_CODE=true",
            "-v",
            f"{config_path}:/tmp/config.json",
            docker_image,
            "discover",
            "--config",
            "/tmp/config.json",
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=180, encoding="utf-8", errors="replace"
        )
        for line in (result.stdout or "").strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                if msg.get("type") == "CATALOG":
                    catalog = msg.get("catalog")
                    if isinstance(catalog, dict) and catalog.get("streams"):
                        return True, catalog, "Catalog discovered."
            except json.JSONDecodeError:
                continue
        if result.returncode != 0:
            return (
                False,
                None,
                f"Discover failed (exit {result.returncode}):\n{(result.stderr or '')[:800]}",
            )
        return False, None, "No CATALOG message in discover output."
    except subprocess.TimeoutExpired:
        return False, None, "Discover timed out (180s)."
    except FileNotFoundError:
        return False, None, "Docker is not installed or not on PATH."
    finally:
        os.unlink(config_path)


def _catalog_stream_names(catalog: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for stream_obj in catalog.get("streams", []) or []:
        if not isinstance(stream_obj, dict):
            continue
        name = stream_obj.get("name")
        if name:
            names.append(str(name))
    return names


def _pick_streams_for_probe(
    catalog: dict[str, Any],
    requested: list[str] | None,
    *,
    max_streams: int,
) -> list[str]:
    """Resolve which catalog streams to include in the test read (subset + cap)."""
    all_names = _catalog_stream_names(catalog)
    if not all_names:
        return []
    if not requested:
        return all_names[:max_streams]
    by_lower = {n.lower(): n for n in all_names}
    picked: list[str] = []
    for req in requested:
        r = str(req).strip()
        if not r:
            continue
        if r in all_names:
            picked.append(r)
        elif r.lower() in by_lower:
            picked.append(by_lower[r.lower()])
    if not picked:
        return all_names[:max_streams]
    return picked[:max_streams]


def _build_configured_catalog(catalog: dict[str, Any], stream_names: list[str]) -> dict[str, Any]:
    """Build a ConfiguredAirbyteCatalog JSON for ``read``."""
    name_set = set(stream_names)
    streams_out: list[dict[str, Any]] = []
    for stream_obj in catalog.get("streams", []) or []:
        if not isinstance(stream_obj, dict):
            continue
        name = stream_obj.get("name")
        if name not in name_set:
            continue
        modes = stream_obj.get("supported_sync_modes") or ["full_refresh"]
        sync_mode = "full_refresh" if "full_refresh" in modes else str(modes[0])
        cursor_field: list[str] = []
        if sync_mode != "full_refresh":
            dc = stream_obj.get("default_cursor_field")
            if isinstance(dc, list):
                cursor_field = [str(x) for x in dc]
        streams_out.append(
            {
                "stream": stream_obj,
                "sync_mode": sync_mode,
                "destination_sync_mode": "overwrite",
                "cursor_field": cursor_field,
            }
        )
    return {"streams": streams_out}


def docker_read_probe(
    docker_image: str,
    config: dict[str, Any],
    stream_names: list[str] | None,
    *,
    max_streams: int = 3,
    read_timeout: int = 300,
) -> tuple[bool, int, str, str]:
    """
    Run ``docker … read`` with a minimal configured catalog (subset of streams).

    Returns ``(success, record_count, message, stderr_tail)``.
    Success means the connector process exited 0 and no ERROR line (or LOG level ERROR) was parsed.
    """
    if _use_azure_job_mode():
        if _should_use_docker_native(docker_image):
            ok, msg, _streams = run_onboarding_docker_native_job(
                action="read_probe",
                docker_image=docker_image,
                config=config,
                streams=stream_names,
                max_streams=max_streams,
                read_timeout=read_timeout,
            )
        else:
            ok, msg = run_onboarding_aca_job(
                action="read_probe",
                docker_image=docker_image,
                config=config,
                streams=stream_names,
                max_streams=max_streams,
                read_timeout=read_timeout,
            )
        return ok, 0, msg, ""

    ok, catalog, dmsg = docker_discover_catalog(docker_image, config)
    if not ok or not catalog:
        return False, 0, dmsg, ""

    picked = _pick_streams_for_probe(catalog, stream_names, max_streams=max_streams)
    if not picked:
        return False, 0, "No streams available for test read.", ""

    configured = _build_configured_catalog(catalog, picked)
    if not configured.get("streams"):
        return False, 0, "Could not build configured catalog for selected streams.", ""

    clean = _clean_connector_config(config)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fc:
        json.dump(clean, fc)
        config_path = fc.name
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fq:
        json.dump(configured, fq)
        catalog_path = fq.name
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fs:
        fs.write("{}")
        state_path = fs.name

    def _run_cmd(with_state: bool) -> subprocess.CompletedProcess[str]:
        cmd: list[str] = [
            "docker",
            "run",
            "--rm",
            "-e",
            "AIRBYTE_ENABLE_UNSAFE_CODE=true",
            "-v",
            f"{config_path}:/tmp/config.json",
            "-v",
            f"{catalog_path}:/tmp/catalog.json",
        ]
        if with_state:
            cmd.extend(["-v", f"{state_path}:/tmp/state.json"])
        cmd.extend(
            [
                docker_image,
                "read",
                "--config",
                "/tmp/config.json",
                "--catalog",
                "/tmp/catalog.json",
            ]
        )
        if with_state:
            cmd.extend(["--state", "/tmp/state.json"])
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=read_timeout,
            encoding="utf-8",
            errors="replace",
        )

    try:
        result = _run_cmd(with_state=True)
        err = (result.stderr or "")[:2000]
        if result.returncode != 0 and (
            "unknown flag" in err.lower() or "unrecognized" in err.lower()
        ):
            result = _run_cmd(with_state=False)
            err = (result.stderr or "")[:2000]

        record_count = 0
        saw_error = False
        for line in (result.stdout or "").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            t = msg.get("type")
            if t == "RECORD":
                record_count += 1
            if t == "ERROR":
                saw_error = True
            if t == "LOG":
                log = msg.get("log") or {}
                if str(log.get("level", "")).upper() == "ERROR":
                    saw_error = True
        ok_run = result.returncode == 0 and not saw_error
        msg = (
            f"Docker read finished (exit {result.returncode}, {record_count} RECORD line(s), "
            f"streams={picked!r})."
        )
        return ok_run, record_count, msg, err
    except subprocess.TimeoutExpired:
        return False, 0, f"Docker read timed out after {read_timeout}s.", ""
    except FileNotFoundError:
        return False, 0, "Docker is not installed or not on PATH.", ""
    finally:
        for p in (config_path, catalog_path, state_path):
            try:
                os.unlink(p)
            except OSError:
                pass
