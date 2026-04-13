"""LangChain tools for guided onboarding (UI render + connector ops + Supabase persist)."""

from __future__ import annotations

import json
import logging
import secrets
from datetime import datetime, timezone
from typing import Any, Optional

from langchain_core.tools import tool
from pydantic import BaseModel, Field

from ai.agents.onboarding.infra import stores
from ai.agents.onboarding.infra.context import get_onboarding_context
from ai.agents.onboarding.infra.docker_ops import (
    docker_check_connection,
    docker_discover_streams,
    docker_read_probe,
)
from ai.agents.onboarding.infra.oauth_backend import (
    OAUTH_PROVIDERS,
    build_authorize_url,
    inject_tokens_into_config,
    supports_oauth,
)
from fastapi_app.services.connector_service import (
    get_active_user_connector_by_repository,
    upsert_user_connector_onboarding,
)
from fastapi_app.settings import (
    API_PUBLIC_BASE_URL,
    ONBOARDING_DOCKER_ENABLED,
    ONBOARDING_DOCKER_READ_RECORD_CAP,
    ONBOARDING_DOCKER_READ_STREAM_CAP,
    ONBOARDING_DOCKER_READ_TIMEOUT,
)

logger = logging.getLogger(__name__)


def _coerce_str_list(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if x is not None and str(x).strip()]
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except json.JSONDecodeError:
            pass
        s = val.strip()
        return [s] if s else []
    return []


def _sync_schedule_form_fields() -> list[dict[str, Any]]:
    return [
        {
            "key": "sync_mode",
            "label": "Sync type",
            "type": "select",
            "required": True,
            "default": "recurring",
            "options": [
                {"value": "one_off", "label": "One-off sync"},
                {"value": "recurring", "label": "Recurring sync"},
            ],
            "description": "Choose whether this connector runs once or on a repeating schedule.",
        },
        {
            "key": "interval_value",
            "label": "Sync interval value",
            "type": "number",
            "required": False,
            "default": 6,
            "description": "For recurring sync only. Example: 6 with hours = every 6 hours.",
        },
        {
            "key": "interval_unit",
            "label": "Sync interval unit",
            "type": "select",
            "required": False,
            "default": "hours",
            "options": [
                {"value": "minutes", "label": "Minutes"},
                {"value": "hours", "label": "Hours"},
                {"value": "days", "label": "Days"},
            ],
            "description": "Used only when recurring sync is selected.",
        },
        {
            "key": "start_date",
            "label": "Start date (optional)",
            "type": "date",
            "required": False,
            "description": "Optional first run date for recurring sync.",
        },
    ]


def _resolve_sync_schedule(raw: Any) -> tuple[str, int, datetime | None] | None:
    if not isinstance(raw, dict):
        return None
    mode = str(raw.get("mode") or "").strip().lower()
    if mode == "one_off":
        return ("one_off", 1, None)
    if mode != "recurring":
        return None

    freq = raw.get("frequency_minutes")
    try:
        freq_i = int(freq) if freq is not None else 0
    except (TypeError, ValueError):
        return None
    if freq_i < 1:
        return None

    start_raw = raw.get("start_date")
    start_at: datetime | None = None
    if start_raw not in (None, ""):
        try:
            d = datetime.fromisoformat(str(start_raw)).date()
            start_at = datetime(
                year=d.year,
                month=d.month,
                day=d.day,
                hour=0,
                minute=0,
                second=0,
                tzinfo=timezone.utc,
            )
        except ValueError:
            start_at = None

    return ("recurring", freq_i, start_at)


UI_TOOL_NAMES = frozenset(
    {
        "render_auth_options",
        "render_input_fields",
        "render_stream_selector",
        "render_sync_schedule_form",
        "start_oauth_flow",
    }
)


class TestConnectionArgs(BaseModel):
    docker_image: str = Field(description="Full Docker image string")
    config: dict[str, Any] = Field(description="Connector configuration dict")


class DiscoverStreamsArgs(BaseModel):
    docker_image: str
    config: dict[str, Any]


class RunSyncArgs(BaseModel):
    connector_name: str
    streams: Optional[list[str]] = Field(
        default=None,
        description="Optional stream names; omit for all.",
    )


class SaveConfigArgs(BaseModel):
    connector_name: str
    docker_image: str
    config: dict[str, Any]
    selected_streams: Optional[list[str]] = None


class RenderAuthOptionsArgs(BaseModel):
    options: list[dict[str, Any]]


class RenderInputFieldsArgs(BaseModel):
    fields: list[dict[str, Any]]


class RenderStreamSelectorArgs(BaseModel):
    streams: list[dict[str, Any]]


class StartOAuthFlowArgs(BaseModel):
    docker_repository: str
    client_id: str
    client_secret: str
    shop: Optional[str] = None


@tool
def get_connector_spec(connector_name: str) -> str:
    """Return connection specification and OAuth config for the active connector."""
    ctx = get_onboarding_context()
    if not ctx:
        return json.dumps({"error": "No onboarding context"})
    cat = ctx.catalog
    if (cat.get("name") or "").strip() != connector_name.strip():
        return json.dumps(
            {
                "error": f"Connector name mismatch (context is {cat.get('name')!r}).",
            }
        )
    return json.dumps(
        {
            "name": cat.get("name"),
            "docker_repository": cat.get("docker_repository"),
            "docker_image_tag": cat.get("docker_image_tag", "latest"),
            "documentation_url": cat.get("documentation_url", ""),
            "config_schema": cat.get("config_schema") or {},
            "oauth_config": cat.get("oauth_config"),
        },
        indent=2,
        default=str,
    )


@tool(args_schema=TestConnectionArgs)
def test_connection(docker_image: str, config: dict) -> str:
    """Validate configuration via the connector Docker image (optional; requires ONBOARDING_DOCKER_ENABLED)."""
    if not ONBOARDING_DOCKER_ENABLED:
        return json.dumps(
            {
                "success": True,
                "skipped": True,
                "message": (
                    "Docker-based connection check is disabled on this server "
                    "(set ONBOARDING_DOCKER_ENABLED=1 locally to enable). "
                    "Treat credentials as unverified until you run a check in a dev environment."
                ),
            }
        )
    ok, message = docker_check_connection(docker_image, config)
    return json.dumps({"success": ok, "message": message})


@tool(args_schema=DiscoverStreamsArgs)
def discover_streams(docker_image: str, config: dict) -> str:
    """List streams from the connector image (optional; requires ONBOARDING_DOCKER_ENABLED)."""
    if not ONBOARDING_DOCKER_ENABLED:
        return json.dumps(
            {
                "success": False,
                "streams": [],
                "message": (
                    "Discover is disabled (ONBOARDING_DOCKER_ENABLED=0). "
                    "Ask the user which streams they need or enable Docker discover locally."
                ),
            }
        )
    ok, streams, msg = docker_discover_streams(docker_image, config)
    return json.dumps({"success": ok, "streams": streams, "message": msg})


@tool(args_schema=RunSyncArgs)
def run_sync(connector_name: str, streams: list[str] | None = None) -> str:
    """Run a Docker ``read`` probe when enabled; otherwise only note that no live extraction ran."""
    ctx = get_onboarding_context()
    if not ctx:
        return json.dumps({"success": False, "error": "Missing onboarding context"})

    cat = ctx.catalog
    docker_repo = (cat.get("docker_repository") or "").strip()
    tag = cat.get("docker_image_tag", "latest")
    docker_image = f"{docker_repo}:{tag}" if docker_repo else ""

    existing = get_active_user_connector_by_repository(ctx.user_id, docker_repo)
    if not existing:
        return json.dumps(
            {
                "success": False,
                "message": (
                    "No saved profile yet. Call save_config with the working configuration first, "
                    "then call run_sync again to complete the test-sync step."
                ),
            }
        )

    raw_cfg = existing.get("config") or {}
    if isinstance(raw_cfg, str):
        cfg = json.loads(raw_cfg) if raw_cfg else {}
    else:
        cfg = dict(raw_cfg)

    stream_list: list[str] = []
    if streams:
        stream_list = [str(s).strip() for s in streams if s and str(s).strip()]
    if not stream_list:
        stream_list = _coerce_str_list(cfg.get("__selected_streams__"))
    if not stream_list:
        stream_list = _coerce_str_list(existing.get("selected_streams"))
    if stream_list:
        cfg["__selected_streams__"] = stream_list

    image = (existing.get("docker_image") or docker_image or "").strip()
    if not image:
        return json.dumps(
            {
                "success": False,
                "message": "No docker image resolved for this connector; cannot run test sync.",
            }
        )

    if ONBOARDING_DOCKER_ENABLED:
        ok_probe, records, probe_msg, err_tail = docker_read_probe(
            image,
            cfg,
            stream_list if stream_list else None,
            max_streams=ONBOARDING_DOCKER_READ_STREAM_CAP,
            max_records=ONBOARDING_DOCKER_READ_RECORD_CAP,
            read_timeout=ONBOARDING_DOCKER_READ_TIMEOUT,
        )
        if not ok_probe:
            return json.dumps(
                {
                    "success": False,
                    "message": (
                        "Docker test read failed — extraction was not verified end-to-end. "
                        "Fix credentials/streams or check Docker logs."
                    ),
                    "detail": probe_msg,
                    "docker_stderr": err_tail,
                    "sync_validated": False,
                    "docker_sync": True,
                    "records_read": records,
                    "streams_attempted": stream_list,
                }
            )
        logger.info("run_sync: Docker read OK (%s RECORD lines)", records)
    else:
        logger.warning(
            "run_sync: ONBOARDING_DOCKER_ENABLED=0 — skipping Docker read; "
            "sync_validated will remain false until a real probe is run."
        )

    try:
        upsert_user_connector_onboarding(
            ctx.user_id,
            connector_name=connector_name,
            docker_repository=docker_repo,
            docker_image=image,
            config=cfg,
            oauth_meta=existing.get("oauth_meta"),
            selected_streams=stream_list if stream_list else None,
            sync_mode=str(existing.get("sync_mode") or "recurring"),
            sync_frequency_minutes=int(existing.get("sync_frequency_minutes") or 360),
            sync_start_at=existing.get("sync_start_at"),
            sync_validated=bool(ONBOARDING_DOCKER_ENABLED),
        )
    except Exception as e:
        logger.exception("run_sync upsert failed")
        return json.dumps({"success": False, "error": str(e)})

    if ONBOARDING_DOCKER_ENABLED:
        return json.dumps(
            {
                "success": True,
                "message": (
                    f"Docker validation sample succeeded ({records} RECORD line(s) sampled). "
                    "Validation reads are capped by design (not a full extraction). "
                    "This confirms the connector can fetch data end-to-end locally. "
                    "Scheduled syncs still run in your cloud environment."
                ),
                "sync_validated": True,
                "docker_sync": True,
                "records_read": records,
                "streams": stream_list,
                "probe_detail": probe_msg,
            }
        )

    return json.dumps(
        {
            "success": True,
            "message": (
                "ONBOARDING_DOCKER_ENABLED=0: no Docker read was executed, so extraction was not "
                "verified. Set ONBOARDING_DOCKER_ENABLED=1 locally and call run_sync again to run a "
                "real connector read (you should see Docker activity). "
                "sync_validated was left false in the database."
            ),
            "sync_validated": False,
            "docker_sync": False,
            "streams": stream_list,
        }
    )


@tool(args_schema=SaveConfigArgs)
def save_config(
    connector_name: str,
    docker_image: str,
    config: dict,
    selected_streams: list[str] | None = None,
) -> str:
    """Persist configuration to Supabase ``user_connector_configs`` for the current user."""
    ctx = get_onboarding_context()
    if not ctx:
        return json.dumps({"success": False, "error": "Missing onboarding context"})

    oauth_meta = None
    if config.get("__oauth_meta__"):
        oauth_meta = config.get("__oauth_meta__")
    elif stores.get_tool_kv("oauth_meta"):
        oauth_meta = stores.get_tool_kv("oauth_meta")

    cfg = dict(config)
    if oauth_meta and "__oauth_meta__" not in cfg:
        cfg["__oauth_meta__"] = oauth_meta
    if selected_streams:
        cfg["__selected_streams__"] = selected_streams

    schedule_raw = stores.get_tool_kv("sync_schedule")
    resolved = _resolve_sync_schedule(schedule_raw)
    if resolved is None:
        stores.set_tool_kv(
            "pending_ui",
            {
                "type": "input_fields",
                "fields": _sync_schedule_form_fields(),
            },
        )
        return json.dumps(
            {
                "success": False,
                "needs_sync_schedule": True,
                "message": (
                    "Before saving, collect sync schedule settings using the rendered form "
                    "(one-off or recurring with interval and optional start date)."
                ),
            }
        )

    sync_mode, sync_frequency_minutes, sync_start_at = resolved
    cfg["__sync_schedule__"] = {
        "mode": sync_mode,
        "frequency_minutes": sync_frequency_minutes if sync_mode == "recurring" else None,
        "start_at": sync_start_at.isoformat() if sync_start_at else None,
    }

    try:
        upsert_user_connector_onboarding(
            ctx.user_id,
            connector_name=connector_name,
            docker_repository=ctx.catalog.get("docker_repository", ""),
            docker_image=docker_image,
            config=cfg,
            oauth_meta=oauth_meta,
            selected_streams=selected_streams,
            sync_mode=sync_mode,
            sync_frequency_minutes=sync_frequency_minutes,
            sync_start_at=sync_start_at,
            sync_validated=False,
        )
    except Exception as e:
        logger.exception("save_config failed")
        return json.dumps({"success": False, "error": str(e)})

    return json.dumps(
        {
            "success": True,
            "message": f"Configuration saved for {connector_name}",
        }
    )


def _normalize_auth_options(options: list[dict]) -> list[dict[str, Any]]:
    """Ensure each option has ``label`` and ``auth_type`` (LLMs often omit ``label`` or use ``title``)."""
    out: list[dict[str, Any]] = []
    for i, raw in enumerate(options):
        if not isinstance(raw, dict):
            continue
        label = (
            (raw.get("label") or raw.get("title") or raw.get("name") or raw.get("value") or "")
        )
        if not str(label).strip():
            label = raw.get("description") or ""
        label = str(label).strip() or f"Option {i + 1}"
        auth_type = str(
            raw.get("auth_type") or raw.get("type") or raw.get("key") or label,
        ).strip() or label
        desc = raw.get("description")
        description = str(desc).strip() if desc not in (None, "") else None
        if description == label:
            description = None
        out.append(
            {
                "label": label,
                "auth_type": auth_type,
                **({"description": description} if description else {}),
            }
        )
    return out


def _normalize_input_fields(fields: list[dict]) -> list[dict[str, Any]]:
    """Ensure each field has ``key`` (LLMs often emit ``name`` or ``id`` instead)."""
    out: list[dict[str, Any]] = []
    for i, raw in enumerate(fields):
        if not isinstance(raw, dict):
            continue
        key = (
            raw.get("key")
            or raw.get("name")
            or raw.get("id")
            or raw.get("field")
            or raw.get("property")
        )
        key_s = str(key).strip() if key is not None else ""
        if not key_s:
            label = str(raw.get("label") or raw.get("title") or "").strip()
            key_s = label.lower().replace(" ", "_") if label else f"field_{i + 1}"
        merged = dict(raw)
        merged["key"] = key_s
        out.append(merged)
    return out


@tool(args_schema=RenderAuthOptionsArgs)
def render_auth_options(options: list[dict]) -> str:
    """Show authentication method choices as buttons in the onboarding UI."""
    normalized = _normalize_auth_options(options)
    stores.set_tool_kv(
        "pending_ui",
        {
            "type": "auth_options",
            "options": normalized,
        },
    )
    labels = [o.get("label", "") for o in normalized]
    return (
        f"Displayed {len(normalized)} auth options: {', '.join(labels)}. "
        "Wait for the user to choose before calling another UI tool."
    )


@tool(args_schema=RenderInputFieldsArgs)
def render_input_fields(fields: list[dict]) -> str:
    """Render a dynamic credential/configuration form in the onboarding UI."""
    normalized = _normalize_input_fields(fields)
    stores.set_tool_kv(
        "pending_ui",
        {
            "type": "input_fields",
            "fields": normalized,
        },
    )
    names = [f.get("label", f.get("key", "")) for f in normalized]
    return (
        f"Displayed input form ({len(normalized)} fields): {', '.join(names)}. "
        "Wait for the user to submit."
    )


@tool(args_schema=RenderStreamSelectorArgs)
def render_stream_selector(streams: list[dict]) -> str:
    """Show a stream multi-select in the onboarding UI."""
    stores.set_tool_kv(
        "pending_ui",
        {
            "type": "stream_selector",
            "streams": streams,
        },
    )
    acc = sum(1 for s in streams if s.get("accessible"))
    return (
        f"Displayed stream selector ({acc}/{len(streams)} accessible). "
        "Wait for the user to confirm."
    )


@tool
def render_sync_schedule_form() -> str:
    """Render the required sync schedule form (one-off or recurring) and wait for submission."""
    stores.set_tool_kv(
        "pending_ui",
        {
            "type": "input_fields",
            "fields": _sync_schedule_form_fields(),
        },
    )
    return (
        "Displayed sync schedule form (required before save). "
        "Wait for the user to submit before calling save_config."
    )


@tool(args_schema=StartOAuthFlowArgs)
def start_oauth_flow(
    docker_repository: str,
    client_id: str,
    client_secret: str,
    shop: str | None = None,
) -> str:
    """Build provider authorization URL and register OAuth state for the callback handler."""
    ctx = get_onboarding_context()
    if not ctx:
        return json.dumps({"error": "No onboarding context"})
    if docker_repository not in OAUTH_PROVIDERS:
        return json.dumps({"error": f"OAuth not supported for {docker_repository}"})

    cat = ctx.catalog
    oauth_config = cat.get("oauth_config")
    if not supports_oauth(docker_repository, oauth_config):
        return json.dumps(
            {"error": "Connector does not declare OAuth 2.0 in catalog oauth_config."}
        )

    state = secrets.token_urlsafe(32)
    redirect_uri = f"{API_PUBLIC_BASE_URL.rstrip('/')}/api/onboarding/oauth/callback"

    stores.register_oauth_state(
        state,
        {
            "user_id": ctx.user_id,
            "thread_id": ctx.thread_id,
            "docker_repository": docker_repository,
            "client_id": client_id,
            "client_secret": client_secret,
            "shop": shop,
            "oauth_config": oauth_config,
        },
    )

    try:
        auth_url = build_authorize_url(
            docker_repository=docker_repository,
            client_id=client_id,
            redirect_uri=redirect_uri,
            state=state,
            shop=shop,
        )
    except ValueError as e:
        return json.dumps({"error": str(e)})

    stores.set_tool_kv("oauth_client_id", client_id)
    stores.set_tool_kv("oauth_client_secret", client_secret)
    stores.set_tool_kv("oauth_docker_repo", docker_repository)
    stores.set_tool_kv("oauth_shop", shop)
    stores.set_tool_kv(
        "pending_ui",
        {
            "type": "oauth_button",
            "url": auth_url,
            "provider": docker_repository,
            "state": state,
        },
    )

    return json.dumps(
        {
            "success": True,
            "message": "OAuth authorize URL ready. User should open the link to continue.",
        }
    )


ALL_TOOLS = [
    get_connector_spec,
    test_connection,
    discover_streams,
    run_sync,
    save_config,
    render_auth_options,
    render_input_fields,
    render_stream_selector,
    render_sync_schedule_form,
    start_oauth_flow,
]
