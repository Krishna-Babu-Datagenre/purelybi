"""
API routes for the chat / streaming endpoint.

Endpoints
---------
POST /api/chat                       – stream agent response via SSE
GET  /api/chat/history/{session_id}  – retrieve full conversation history
DELETE /api/chat/history/{session_id} – clear a conversation session

All endpoints require ``Authorization: Bearer <token>`` and scope
sessions to the authenticated user so multiple users never share state.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from fastapi_app.models.auth import UserProfile
from fastapi_app.models.chat import ChatMessage, ChatRequest
from fastapi_app.services.chat_service import (
    AGENT_CLASSES,
    _sessions,
    get_conversation_history,
    stream_agent_response,
)
from ai.agents.sql.tools.charts import clear_query_result
from fastapi_app.utils.auth_dep import get_current_user_dep

router = APIRouter(prefix="/api/chat", tags=["chat"])


def _scoped_session_id(user: UserProfile, client_session_id: str) -> str:
    """Prefix the client-supplied session id with the user id for isolation."""
    return f"{user.id}:{client_session_id}"


@router.post("")
async def chat(
    request: ChatRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """
    Stream the agent's response as **Server-Sent Events** (SSE).

    The frontend should consume this with an ``EventSource`` or
    ``fetch()`` reader. Each SSE frame carries an ``event`` field
    (e.g. ``token``, ``tool_call_start``, ``tool_result``, ``chart``,
    ``end``, ``error``) and a JSON ``data`` payload.
    """
    if request.agent_type not in AGENT_CLASSES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown agent_type '{request.agent_type}'. "
                f"Valid options: {list(AGENT_CLASSES.keys())}"
            ),
        )
    if request.database.lower() != "duckdb":
        raise HTTPException(
            status_code=400,
            detail="Only DuckDB is supported for /api/chat.",
        )
    session_id = _scoped_session_id(user, request.session_id)
    return StreamingResponse(
        stream_agent_response(
            message=request.message,
            tenant_id=user.id,
            session_id=session_id,
            agent_type=request.agent_type,
            llm=request.llm,
            database=request.database,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/history/{session_id}", response_model=list[ChatMessage])
async def get_history(
    session_id: str,
    agent_type: str = "analyst",
    llm: str = "gpt-4.1",
    database: str = "DuckDB",
    user: UserProfile = Depends(get_current_user_dep),
):
    """
    Return the full conversation history for *session_id*.

    If the session has not been started yet, returns an empty list.
    """
    scoped_id = _scoped_session_id(user, session_id)
    if database.lower() != "duckdb":
        raise HTTPException(
            status_code=400,
            detail="Only DuckDB is supported for /api/chat/history.",
        )
    history = get_conversation_history(
        session_id=scoped_id,
        agent_type=agent_type,
        llm=llm,
        database=database,
    )
    return history


@router.delete("/history/{session_id}")
async def clear_history(
    session_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """
    Delete the in-memory conversation for *session_id*.

    Returns 204 on success, 404 if the session does not exist.
    """
    scoped_id = _scoped_session_id(user, session_id)
    if scoped_id not in _sessions:
        raise HTTPException(
            status_code=404, detail=f"Session '{session_id}' not found"
        )
    conn = _sessions[scoped_id].get("conn")
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    del _sessions[scoped_id]
    clear_query_result(scoped_id)
    return {"status": "deleted", "session_id": session_id}
