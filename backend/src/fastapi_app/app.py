"""
FastAPI application entry point.

Run with:
    uv run python -m uvicorn fastapi_app.app:app --reload --host 127.0.0.1 --port 8000
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from fastapi_app.middleware.request_id import RequestIdMiddleware
from fastapi_app.routers import agent, auth, chat, connectors, dashboards, onboarding, templates
from fastapi_app.settings import LOG_LEVEL

_LEVEL = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
logging.basicConfig(
    level=_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "-")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


def _cors_extra_origins() -> list[str]:
    """Production frontends (e.g. Azure Static Web Apps), comma-separated in env."""
    raw = os.environ.get("CORS_EXTRA_ORIGINS", "")
    return [o.strip() for o in raw.split(",") if o.strip()]


_TAGS = [
    {
        "name": "auth",
        "description": "Sign-up, sign-in, OAuth URL, and current user profile.",
    },
    {
        "name": "templates",
        "description": "Dashboard template catalog and live hydrated views.",
    },
    {
        "name": "dashboards",
        "description": "User-owned dashboards and widgets.",
    },
    {"name": "chat", "description": "Streaming agent chat (SSE)."},
    {
        "name": "connectors",
        "description": "Connector catalog and per-user connector configurations.",
    },
    {
        "name": "onboarding",
        "description": "Guided data-source onboarding (SSE agent + OAuth; see checklist Phase 5).",
    },
    {
        "name": "agent",
        "description": "SQL agent backend status and capability flags.",
    },
]

app = FastAPI(
    title="BI Agent Backend",
    description=(
        "Dashboard templates, AI analytics, and multi-tenant data connectors. "
        "Protected routes require `Authorization: Bearer <access_token>` from Supabase."
    ),
    version="0.2.0",
    lifespan=lifespan,
    openapi_tags=_TAGS,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_extra_origins(),
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestIdMiddleware)


# ---------------------------------------------------------------------------
# Global exception handlers
# ---------------------------------------------------------------------------


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
):
    """Return a structured 422 when request body / query params fail validation."""
    errors = []
    for e in exc.errors():
        loc = [str(part) for part in e["loc"] if part != "body"]
        errors.append(
            {
                "field": ".".join(loc) if loc else None,
                "message": e["msg"],
                "type": e["type"],
            }
        )
    logger.warning(
        "Validation error request_id=%s %s %s: %s",
        _request_id(request),
        request.method,
        request.url.path,
        errors,
    )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "detail": "Request validation failed",
            "errors": errors,
            "request_id": _request_id(request),
        },
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Ensure HTTP errors return a consistent JSON shape."""
    rid = _request_id(request)
    if exc.status_code >= 500:
        logger.error(
            "HTTP %d request_id=%s %s %s: %s",
            exc.status_code,
            rid,
            request.method,
            request.url.path,
            exc.detail,
        )
    else:
        logger.warning(
            "HTTP %d request_id=%s %s %s: %s",
            exc.status_code,
            rid,
            request.method,
            request.url.path,
            exc.detail,
        )
    body: dict = {"detail": exc.detail, "request_id": rid}
    return JSONResponse(
        status_code=exc.status_code,
        content=body,
        headers=getattr(exc, "headers", None),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Log tracebacks server-side; return a generic 500 to the client."""
    rid = _request_id(request)
    logger.exception(
        "Unhandled exception request_id=%s %s %s",
        rid,
        request.method,
        request.url.path,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "An unexpected server error occurred. Please try again later.",
            "request_id": rid,
        },
    )


# Register routers
app.include_router(auth.router)
app.include_router(templates.router)
app.include_router(dashboards.router)
app.include_router(chat.router)
app.include_router(connectors.router)
app.include_router(onboarding.router)
app.include_router(agent.router)


@app.get("/health")
async def health_check():
    return {"status": "ok"}
