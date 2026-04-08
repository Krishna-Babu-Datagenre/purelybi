"""
Pydantic models for the chat / streaming API.

These schemas define the request body accepted by the chat endpoint
and the Server-Sent Event (SSE) payload shapes streamed back to the
React frontend.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Request
# ---------------------------------------------------------------------------


class ChatRequest(BaseModel):
    """Body sent by the frontend to start a chat interaction."""

    message: str = Field(
        ...,
        min_length=1,
        description="The user's natural-language question or instruction.",
    )
    session_id: str = Field(
        default="default",
        description="Unique conversation thread ID.  Reuse the same value to continue a conversation.",
    )
    agent_type: str = Field(
        default="analyst",
        description="Which agent to use.  Options: 'analyst', 'supervisor'.",
    )
    llm: str = Field(
        default="gpt-4.1",
        description="LLM model name to use for the agent.",
    )
    database: str = Field(
        default="DuckDB",
        description="Database backend for analytics agent. Options: 'DuckDB'.",
    )


# ---------------------------------------------------------------------------
# SSE event types
# ---------------------------------------------------------------------------


class SSEEventType(str, Enum):
    """Discriminator for the different SSE event kinds."""

    # Streamed text tokens from the model
    token = "token"

    # A tool call has started (includes tool name)
    tool_call_start = "tool_call_start"

    # Accumulated arguments for the in-flight tool call
    tool_call_args = "tool_call_args"

    # Tool execution finished – carries the result
    tool_result = "tool_result"

    # Chart payload (Plotly JSON) ready to render
    chart = "chart"

    # The stream has ended normally
    end = "end"

    # An error occurred
    error = "error"


# ---------------------------------------------------------------------------
# SSE data payloads
# ---------------------------------------------------------------------------


class SSEToken(BaseModel):
    """A chunk of streamed text from the model."""

    content: str


class SSEToolCallStart(BaseModel):
    """Signals that a tool invocation has begun."""

    tool_call_id: str
    tool_name: str


class SSEToolCallArgs(BaseModel):
    """Accumulated argument fragment for the current tool call."""

    tool_call_id: str
    args_chunk: str


class SSEToolResult(BaseModel):
    """Result payload returned after a tool finishes executing."""

    tool_call_id: str
    tool_name: str
    args: str
    result: str


class SSEChart(BaseModel):
    """A chart that should be rendered by the frontend."""

    tool_call_id: str
    chart_type: str
    figure_json: str  # Plotly figure serialised as JSON string


class SSEEnd(BaseModel):
    """Marks the end of the stream."""

    pass


class SSEError(BaseModel):
    """Carries an error description."""

    detail: str


# ---------------------------------------------------------------------------
# Conversation history (returned by GET /chat/history)
# ---------------------------------------------------------------------------


class MessageRole(str, Enum):
    user = "user"
    assistant = "assistant"
    tool = "tool"


class ToolCall(BaseModel):
    id: str
    name: str
    args: dict[str, Any] | str


class ChatMessage(BaseModel):
    """A single message in the conversation history."""

    role: MessageRole
    content: str | None = None
    tool_calls: list[ToolCall] | None = None
    tool_call_id: str | None = None
    tool_name: str | None = None
    chart: SSEChart | None = None
