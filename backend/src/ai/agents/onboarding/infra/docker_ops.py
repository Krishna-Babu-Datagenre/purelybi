"""Docker / ACA connector operations for onboarding (Sync V2).

Two execution paths:
  - ``local``: uses ``docker run`` subprocess (dev machine only)
  - ``azure_job``: starts the official connector image directly on the single
    ACA Job via ``connector_runner`` — no language routing, no PyAirbyte.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi_app.settings import ONBOARDING_DOCKER_EXECUTION_MODE


def _slug(name: str, max_len: int = 24) -> str:
    """Normalise a connector/image name into a short, filesystem-safe slug."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:max_len]


def _use_azure_job_mode() -> bool:
    return ONBOARDING_DOCKER_EXECUTION_MODE == "azure_job"


def docker_check_connection(docker_image: str, config: dict[str, Any]) -> tuple[bool, str]:
    if _use_azure_job_mode():
        return _aca_check_connection(docker_image, config)
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


def docker_discover_streams_with_catalog(
    docker_image: str, config: dict[str, Any]
) -> tuple[bool, list[str], str, dict[str, Any] | None]:
    """Discover streams and return the full catalog in one call.

    Returns (ok, stream_names, message, catalog_dict_or_None).
    In ACA mode this is a single execution; locally it parses from docker output.
    """
    if _use_azure_job_mode():
        return _aca_discover_streams_with_catalog(docker_image, config)
    # Local: run discover once, extract both streams and catalog
    ok, streams, msg = docker_discover_streams(docker_image, config)
    if not ok:
        return False, streams, msg, None
    # Re-run discover to get the raw catalog (local mode is fast)
    _, catalog, _ = docker_discover_catalog(docker_image, config)
    return ok, streams, msg, catalog


def docker_discover_streams(
    docker_image: str, config: dict[str, Any]
) -> tuple[bool, list[str], str]:
    if _use_azure_job_mode():
        return _aca_discover_streams(docker_image, config)
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
        return _aca_discover_catalog(docker_image, config)
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
    max_records: int = 200,
    read_timeout: int = 300,
) -> tuple[bool, int, str, str]:
    """
    Run ``docker … read`` with a minimal configured catalog (subset of streams).

    Returns ``(success, record_count, message, stderr_tail)``.
    Success means the connector process exited 0 and no ERROR line (or LOG level ERROR) was parsed.
    """
    if _use_azure_job_mode():
        return _aca_read_probe(
            docker_image, config, stream_names,
            max_streams=max_streams, max_records=max_records, read_timeout=read_timeout,
        )

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

    def _build_cmd(with_state: bool) -> list[str]:
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
        return cmd

    def _run_probe(with_state: bool) -> tuple[int, int, bool, bool, str]:
        cmd = _build_cmd(with_state)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        deadline = time.monotonic() + max(1, read_timeout)
        record_count = 0
        saw_error = False
        sampled_stop = False

        try:
            while True:
                if time.monotonic() > deadline:
                    proc.kill()
                    return -1, record_count, saw_error, sampled_stop, ""

                line = proc.stdout.readline() if proc.stdout else ""
                if not line:
                    if proc.poll() is not None:
                        break
                    time.sleep(0.01)
                    continue

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
                    if max_records > 0 and record_count >= max_records:
                        sampled_stop = True
                        proc.terminate()
                        try:
                            proc.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        break
                elif t == "ERROR":
                    saw_error = True
                elif t == "LOG":
                    log = msg.get("log") or {}
                    if str(log.get("level", "")).upper() == "ERROR":
                        saw_error = True

            returncode = proc.wait(timeout=5)
            stderr_tail = ((proc.stderr.read() if proc.stderr else "") or "")[:2000]
            return returncode, record_count, saw_error, sampled_stop, stderr_tail
        finally:
            if proc.poll() is None:
                proc.kill()

    try:
        returncode, record_count, saw_error, sampled_stop, err = _run_probe(with_state=True)
        if returncode != 0 and (
            "unknown flag" in err.lower() or "unrecognized" in err.lower()
        ):
            returncode, record_count, saw_error, sampled_stop, err = _run_probe(with_state=False)

        if returncode == -1:
            return False, record_count, f"Docker read timed out after {read_timeout}s.", err

        ok_run = ((returncode == 0) or sampled_stop) and not saw_error and record_count > 0
        mode = "sampled" if sampled_stop else "full"
        msg = (
            f"Docker read probe finished ({mode}, exit {returncode}, "
            f"records={record_count}, streams={picked!r})."
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


# ── ACA-direct helpers (Sync V2) ─────────────────────────────────────
# These replace the old azure_job_runner.py — no language routing,
# no PyAirbyte, all connectors use official Docker images on one ACA Job.


def _aca_check_connection(docker_image: str, config: dict[str, Any]) -> tuple[bool, str]:
    """Test connection via the single ACA Job."""
    from ai.agents.onboarding.infra.connector_runner import (
        cleanup_fileshare,
        parse_connection_status,
        read_from_fileshare,
        start_connector_execution,
        wait_for_execution,
        write_to_fileshare,
    )

    connector = _slug(docker_image.rsplit("/", 1)[-1].split(":")[0])
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    work_id = f"onb-check-{connector}-{ts}"
    clean = {k: v for k, v in config.items() if not str(k).startswith("__")}

    try:
        write_to_fileshare(work_id, "config.json", json.dumps(clean, default=str))

        execution_name = start_connector_execution(
            docker_image=docker_image,
            airbyte_command="check",
            work_id=work_id,
        )

        success = wait_for_execution(execution_name, timeout=120)

        if success:
            jsonl = read_from_fileshare(work_id, "output.jsonl")
            ok, message = parse_connection_status(jsonl)
            return ok, message or "Connection check succeeded!"

        stderr = read_from_fileshare(work_id, "stderr.log")
        return False, f"Check failed: {stderr[:500]}" if stderr else "Check failed (no output)"
    finally:
        cleanup_fileshare(work_id)


def _aca_discover_streams(
    docker_image: str, config: dict[str, Any]
) -> tuple[bool, list[str], str]:
    """Discover streams via the single ACA Job."""
    ok, streams, msg, _catalog = _aca_discover_streams_with_catalog(docker_image, config)
    return ok, streams, msg


def _aca_discover_streams_with_catalog(
    docker_image: str, config: dict[str, Any]
) -> tuple[bool, list[str], str, dict[str, Any] | None]:
    """Discover streams + full catalog via the single ACA Job."""
    from ai.agents.onboarding.infra.connector_runner import (
        cleanup_fileshare,
        parse_catalog,
        read_from_fileshare,
        start_connector_execution,
        wait_for_execution,
        write_to_fileshare,
    )

    connector = _slug(docker_image.rsplit("/", 1)[-1].split(":")[0])
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    work_id = f"onb-discover-{connector}-{ts}"
    clean = {k: v for k, v in config.items() if not str(k).startswith("__")}

    try:
        write_to_fileshare(work_id, "config.json", json.dumps(clean, default=str))

        execution_name = start_connector_execution(
            docker_image=docker_image,
            airbyte_command="discover",
            work_id=work_id,
        )

        success = wait_for_execution(execution_name, timeout=180)

        if success:
            jsonl = read_from_fileshare(work_id, "output.jsonl")
            streams, catalog = parse_catalog(jsonl)
            return True, streams, f"Discovered {len(streams)} streams.", catalog

        stderr = read_from_fileshare(work_id, "stderr.log")
        return False, [], f"Discover failed: {stderr[:500]}" if stderr else "Discover failed", None
    finally:
        cleanup_fileshare(work_id)


def _aca_discover_catalog(
    docker_image: str, config: dict[str, Any]
) -> tuple[bool, dict[str, Any] | None, str]:
    """Discover full catalog via the single ACA Job."""
    from ai.agents.onboarding.infra.connector_runner import (
        cleanup_fileshare,
        parse_catalog,
        read_from_fileshare,
        start_connector_execution,
        wait_for_execution,
        write_to_fileshare,
    )

    connector = _slug(docker_image.rsplit("/", 1)[-1].split(":")[0])
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    work_id = f"onb-catalog-{connector}-{ts}"
    clean = {k: v for k, v in config.items() if not str(k).startswith("__")}

    try:
        write_to_fileshare(work_id, "config.json", json.dumps(clean, default=str))

        execution_name = start_connector_execution(
            docker_image=docker_image,
            airbyte_command="discover",
            work_id=work_id,
        )

        success = wait_for_execution(execution_name, timeout=180)

        if success:
            jsonl = read_from_fileshare(work_id, "output.jsonl")
            streams, catalog = parse_catalog(jsonl)
            if catalog:
                return True, catalog, f"Catalog discovered ({len(streams)} streams)."
            return False, None, "No CATALOG message in discover output."

        stderr = read_from_fileshare(work_id, "stderr.log")
        return False, None, f"Discover failed: {stderr[:500]}" if stderr else "Discover failed"
    finally:
        cleanup_fileshare(work_id)


def _aca_read_probe(
    docker_image: str,
    config: dict[str, Any],
    stream_names: list[str] | None,
    *,
    max_streams: int = 3,
    max_records: int = 200,
    read_timeout: int = 300,
) -> tuple[bool, int, str, str]:
    """Bounded read test via the single ACA Job (discover → read)."""
    from ai.agents.onboarding.infra.connector_runner import (
        build_configured_catalog,
        cleanup_fileshare,
        count_records,
        parse_catalog,
        read_from_fileshare,
        start_connector_execution,
        wait_for_execution,
        write_to_fileshare,
    )

    connector = _slug(docker_image.rsplit("/", 1)[-1].split(":")[0])
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    work_id = f"onb-probe-{connector}-{ts}"
    clean = {k: v for k, v in config.items() if not str(k).startswith("__")}

    try:
        write_to_fileshare(work_id, "config.json", json.dumps(clean, default=str))

        # Phase 1: Discover to get the real catalog
        exec1 = start_connector_execution(docker_image, "discover", work_id)
        if not wait_for_execution(exec1, timeout=180):
            stderr = read_from_fileshare(work_id, "stderr.log")
            return False, 0, f"Discover phase failed: {stderr[:500]}", ""

        jsonl = read_from_fileshare(work_id, "output.jsonl")
        discovered_names, catalog = parse_catalog(jsonl)
        if not catalog:
            return False, 0, "No catalog found in discover output", ""

        # Phase 2: Read with bounded catalog
        selected = (stream_names or discovered_names)[:max_streams]
        configured = build_configured_catalog(catalog, selected)
        if not configured.get("streams"):
            return False, 0, "No matching streams found in catalog", ""

        write_to_fileshare(work_id, "catalog.json", json.dumps(configured, default=str))

        exec2 = start_connector_execution(
            docker_image, "read", work_id,
            extra_args=f"--catalog /data/{work_id}/catalog.json",
        )
        if not wait_for_execution(exec2, timeout=read_timeout):
            stderr = read_from_fileshare(work_id, "stderr.log")
            return False, 0, f"Read phase failed: {stderr[:500]}", ""

        jsonl = read_from_fileshare(work_id, "output.jsonl")
        record_count = count_records(jsonl, max_records)
        return True, record_count, f"Read probe succeeded ({record_count} records)", ""
    finally:
        cleanup_fileshare(work_id)
