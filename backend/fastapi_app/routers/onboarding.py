"""
Guided “Connect a new source” onboarding agent — SSE streaming + OAuth callback.

UI protocol: same SSE events as ``/api/chat`` plus ``ui_block`` with dynamic UI payloads
(``auth_options``, ``input_fields``, ``stream_selector``, ``oauth_button``).
"""

from __future__ import annotations

import json
import logging
import time
import urllib.parse
from collections import defaultdict
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import RedirectResponse, StreamingResponse
from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field

from fastapi_app.models.auth import UserProfile
from fastapi_app.onboarding.context import OnboardingContext
from fastapi_app.onboarding.oauth_backend import (
    build_oauth_meta,
    exchange_code_for_token,
    inject_tokens_into_config,
)
from fastapi_app.onboarding import stores as onboarding_stores
from fastapi_app.onboarding.streaming import (
    get_remembered_catalog,
    remember_catalog,
    stream_onboarding,
)
from fastapi_app.services.connector_service import get_connector_catalog_detail
from fastapi_app.settings import (
    API_PUBLIC_BASE_URL,
    ONBOARDING_FRONTEND_REDIRECT,
    ONBOARDING_RATE_LIMIT_PER_MIN,
)
from fastapi_app.utils.auth_dep import get_current_user_dep

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/onboarding", tags=["onboarding"])

_rate_buckets: dict[str, list[float]] = defaultdict(list)


def _check_rate_limit(user_id: str) -> None:
    now = time.time()
    window = 60.0
    bucket = _rate_buckets[user_id]
    bucket[:] = [t for t in bucket if now - t < window]
    if len(bucket) >= ONBOARDING_RATE_LIMIT_PER_MIN:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many onboarding requests. Try again in a minute.",
        )
    bucket.append(now)


class OnboardingStatusResponse(BaseModel):
    status: str
    message: str
    sse_events: list[str] = Field(
        default_factory=lambda: [
            "start",
            "token",
            "tool_call_start",
            "tool_call_args",
            "tool_result",
            "ui_block",
            "end",
            "error",
        ]
    )


@router.get("/status", response_model=OnboardingStatusResponse)
async def onboarding_status():
    return OnboardingStatusResponse(
        status="ok",
        message="Guided onboarding agent is available. POST /api/onboarding/chat with SSE.",
    )


class FormFieldValue(BaseModel):
    key: str
    type: str = "text"
    value: object | None = None


class OnboardingChatRequest(BaseModel):
    """Send one user turn; include ``catalog_connector_id`` on the first message to start."""

    message: str = ""
    thread_id: str = Field(..., min_length=8, max_length=128)
    catalog_connector_id: str | None = Field(
        default=None,
        description="``connector_schemas.id`` — when set, the server builds the first prompt from catalog.",
    )
    form_fields: list[FormFieldValue] | None = Field(
        default=None,
        description="Structured form submit — server redacts secrets before the model sees them.",
    )
    auth_choice: dict[str, str] | None = Field(
        default=None,
        description='e.g. {"label": "...", "auth_type": "..."}',
    )
    stream_names: list[str] | None = Field(
        default=None,
        description="Confirmed stream names from the stream selector.",
    )


def _message_from_form_fields(
    rows: list[FormFieldValue],
    *,
    user_id: str,
    thread_id: str,
) -> str:
    agent_values: dict[str, object] = {}
    for row in rows:
        k = row.key
        if row.type == "password" and row.value not in (None, ""):
            agent_values[k] = onboarding_stores.store_secret(
                k,
                str(row.value),
                user_id=user_id,
                thread_id=thread_id,
            )
        else:
            agent_values[k] = row.value
    return (
        "User submitted configuration values:\n"
        f"```json\n{json.dumps(agent_values, indent=2, default=str)}\n```"
    )


def _build_init_message(cat: dict) -> str:
    schema = cat.get("config_schema") or {}
    oauth_config = cat.get("oauth_config")
    name = cat.get("name", "connector")
    docker_repo = cat.get("docker_repository", "")
    tag = cat.get("docker_image_tag", "latest")
    spec_json = json.dumps(schema, indent=2, default=str)[:8000]
    oauth_json = (
        json.dumps(oauth_config, indent=2, default=str)
        if oauth_config
        else "None"
    )
    return (
        f"The user selected the **{name}** connector.\n\n"
        f"Docker image: `{docker_repo}:{tag}`\n"
        f"Documentation: {cat.get('documentation_url', 'N/A')}\n\n"
        f"OAuth config:\n```json\n{oauth_json}\n```\n\n"
        f"Connection specification schema:\n```json\n{spec_json}\n```\n\n"
        "Begin the onboarding flow. Analyse the schema to identify auth methods "
        "and guide the user through configuration."
    )


@router.post("/chat")
async def onboarding_chat(
    body: OnboardingChatRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    _check_rate_limit(user.id)

    tid = body.thread_id.strip()
    catalog: dict[str, Any] | None = None
    if body.catalog_connector_id:
        catalog = get_connector_catalog_detail(identifier=body.catalog_connector_id.strip())
        if not catalog:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Unknown catalog_connector_id.",
            )
        remember_catalog(user.id, tid, catalog)
        text = _build_init_message(catalog)
    else:
        remembered = get_remembered_catalog(user.id, tid)
        if not remembered:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Unknown session: send catalog_connector_id on the first request for this thread_id.",
            )
        catalog = remembered
        if body.form_fields is not None:
            if not body.form_fields:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="form_fields must not be empty when provided.",
                )
            text = _message_from_form_fields(
                body.form_fields,
                user_id=user.id,
                thread_id=tid,
            )
        elif body.auth_choice is not None:
            ac = body.auth_choice
            label = (
                (ac.get("label") or ac.get("title") or ac.get("name") or "").strip()
            )
            auth_type = (
                (ac.get("auth_type") or ac.get("type") or ac.get("key") or label).strip()
            )
            if not label and not auth_type:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        "auth_choice must include label/title or auth_type/type "
                        "(empty object is not valid)."
                    ),
                )
            if not label:
                label = auth_type
            if not auth_type:
                auth_type = label
            text = f"User selected auth method: {label} (auth_type: {auth_type})"
        elif body.stream_names is not None:
            text = f"User confirmed stream selection: {json.dumps(body.stream_names)}"
        else:
            text = body.message.strip()
            if not text:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Send message, form_fields, auth_choice, or stream_names.",
                )

    fe = ONBOARDING_FRONTEND_REDIRECT.rstrip("/")
    ctx = OnboardingContext(
        user_id=user.id,
        thread_id=tid,
        oauth_redirect_uri=f"{API_PUBLIC_BASE_URL.rstrip('/')}/api/onboarding/oauth/callback",
        frontend_base=fe,
        catalog=catalog or {},
    )

    messages = [HumanMessage(content=text)]
    return StreamingResponse(
        stream_onboarding(ctx=ctx, messages=messages),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/oauth/callback")
async def onboarding_oauth_callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
):
    """OAuth redirect target — exchanges the code and stores the result for ``/oauth/result``."""
    fe = ONBOARDING_FRONTEND_REDIRECT.rstrip("/")
    if error:
        q = urllib.parse.urlencode(
            {
                "onboarding_oauth": "error",
                "error": error,
                "detail": error_description or "",
            }
        )
        return RedirectResponse(url=f"{fe}?{q}", status_code=302)

    if not code or not state:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing code or state.",
        )

    row = onboarding_stores.get_oauth_state_row(state)
    if not row:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired OAuth state.",
        )

    docker_repository = row["docker_repository"]
    redirect_uri = f"{API_PUBLIC_BASE_URL.rstrip('/')}/api/onboarding/oauth/callback"

    try:
        token_resp = exchange_code_for_token(
            docker_repository=docker_repository,
            code=code,
            client_id=row["client_id"],
            client_secret=row["client_secret"],
            redirect_uri=redirect_uri,
            shop=row.get("shop"),
        )
    except Exception as e:
        logger.exception("OAuth token exchange failed")
        q = urllib.parse.urlencode({"onboarding_oauth": "error", "error": "token_exchange", "detail": str(e)})
        return RedirectResponse(url=f"{fe}?{q}", status_code=302)

    oauth_config = row.get("oauth_config") or {}
    cfg: dict = {}
    try:
        cfg = inject_tokens_into_config(cfg, token_resp, oauth_config)
    except Exception:
        logger.exception("inject_tokens_into_config")

    oauth_meta = build_oauth_meta(
        docker_repository=docker_repository,
        token_response=token_resp,
        client_id=row["client_id"],
        client_secret=row["client_secret"],
        shop=row.get("shop"),
    )
    onboarding_stores.set_tool_kv(
        "oauth_meta",
        oauth_meta,
        user_id=str(row["user_id"]),
        thread_id=str(row["thread_id"]),
    )

    agent_message = (
        "OAuth authorization succeeded.\n\n"
        f"Token response (for your reasoning): ```json\n{json.dumps(token_resp, default=str)}\n```\n\n"
        f"Persisted oauth_meta for scheduled refresh: ```json\n{json.dumps(oauth_meta, default=str)}\n```\n\n"
        "Merge tokens into the connector config using the schema's credential paths, "
        "and include `__oauth_meta__` in the config dict when calling `save_config`."
    )
    if cfg:
        agent_message += (
            f"\n\nSuggested config fragment after token injection: ```json\n{json.dumps(cfg, indent=2, default=str)}\n```"
        )

    onboarding_stores.set_oauth_result(
        state,
        {
            "token_response": token_resp,
            "oauth_meta": oauth_meta,
            "suggested_config_fragment": cfg,
            "agent_message": agent_message,
            "display_message": "OAuth completed. Sending you back to the app…",
        },
    )

    q = urllib.parse.urlencode({"onboarding_oauth": "1", "state": state})
    return RedirectResponse(url=f"{fe}?{q}", status_code=302)


class OAuthResultResponse(BaseModel):
    agent_message: str
    display_message: str
    oauth_meta: dict | None = None
    suggested_config_fragment: dict | None = None


@router.get("/oauth/result", response_model=OAuthResultResponse)
async def onboarding_oauth_result(
    state: str = Query(..., min_length=8),
    user: UserProfile = Depends(get_current_user_dep),
):
    """Fetch OAuth exchange result after redirect (must match authenticated user)."""
    row = onboarding_stores.get_oauth_state_row(state)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown or expired state.")

    if row.get("user_id") != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="State does not belong to this user.")

    result = row.get("result")
    if not isinstance(result, dict):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="OAuth result not ready.",
        )

    onboarding_stores.delete_oauth_state(state)

    return OAuthResultResponse(
        agent_message=str(result.get("agent_message", "")),
        display_message=str(result.get("display_message", "")),
        oauth_meta=result.get("oauth_meta"),
        suggested_config_fragment=result.get("suggested_config_fragment"),
    )
