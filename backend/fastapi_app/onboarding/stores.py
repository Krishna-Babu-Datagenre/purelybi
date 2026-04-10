"""Per-session stores for onboarding UI payloads, OAuth hand-off, and secret refs."""

from __future__ import annotations

import threading
import time
from typing import Any

from fastapi_app.onboarding.context import get_onboarding_context

_lock = threading.Lock()

# session_key (user_id:thread_id) -> tool_kv dict (pending_ui, oauth_meta, oauth client fields, …)
_session_tool_stores: dict[str, dict[str, Any]] = {}

SECRET_REF_PREFIX = "__SECRET_REF__:"
# session_key -> field_key -> secret value
_secret_store: dict[str, dict[str, str]] = {}


def _session_key(user_id: str, thread_id: str) -> str:
    return f"{user_id}:{thread_id}"


def _resolve_session(
    *,
    user_id: str | None = None,
    thread_id: str | None = None,
) -> tuple[str, str] | None:
    """Return (user_id, thread_id) from args or active onboarding context."""
    ctx = get_onboarding_context()
    uid = user_id or (ctx.user_id if ctx else None)
    tid = thread_id or (ctx.thread_id if ctx else None)
    if uid is None or tid is None:
        return None
    return (uid, tid)


def peek_pending_ui(*, user_id: str, thread_id: str) -> dict[str, Any] | None:
    sk = _session_key(user_id, thread_id)
    with _lock:
        ui = _session_tool_stores.get(sk, {}).get("pending_ui")
        return ui if isinstance(ui, dict) else None


def clear_pending_ui(*, user_id: str, thread_id: str) -> None:
    sk = _session_key(user_id, thread_id)
    with _lock:
        bucket = _session_tool_stores.get(sk)
        if bucket is not None:
            bucket["pending_ui"] = None


def store_secret(
    key: str,
    value: str,
    *,
    user_id: str | None = None,
    thread_id: str | None = None,
) -> str:
    """Store a password field value; returned token is safe to pass through the LLM."""
    resolved = _resolve_session(user_id=user_id, thread_id=thread_id)
    if resolved is None:
        raise ValueError(
            "store_secret requires user_id/thread_id or active onboarding context",
        )
    uid, tid = resolved
    sk = _session_key(uid, tid)
    with _lock:
        if sk not in _secret_store:
            _secret_store[sk] = {}
        _secret_store[sk][key] = value
    return f"{SECRET_REF_PREFIX}{key}"


def resolve_secrets(obj: Any) -> Any:
    """Replace secret ref strings using the current onboarding context session."""

    ctx = get_onboarding_context()
    sk = _session_key(ctx.user_id, ctx.thread_id) if ctx else None

    def walk(o: Any) -> Any:
        if isinstance(o, str) and o.startswith(SECRET_REF_PREFIX):
            if not sk:
                return o
            secret_key = o[len(SECRET_REF_PREFIX) :]
            with _lock:
                return _secret_store.get(sk, {}).get(secret_key, o)
        if isinstance(o, dict):
            return {k: walk(v) for k, v in o.items()}
        if isinstance(o, list):
            return [walk(item) for item in o]
        return o

    return walk(obj)


def clear_secrets_for_session(user_id: str, thread_id: str) -> None:
    """Remove secret values for one onboarding session (e.g. after save)."""
    sk = _session_key(user_id, thread_id)
    with _lock:
        _secret_store.pop(sk, None)


def set_tool_kv(
    key: str,
    value: Any,
    *,
    user_id: str | None = None,
    thread_id: str | None = None,
) -> None:
    """Set a session-scoped tool store value (OAuth client id, pending_ui, oauth_meta, …)."""
    resolved = _resolve_session(user_id=user_id, thread_id=thread_id)
    if resolved is None:
        raise ValueError(
            "set_tool_kv requires user_id/thread_id or active onboarding context",
        )
    uid, tid = resolved
    sk = _session_key(uid, tid)
    with _lock:
        if sk not in _session_tool_stores:
            _session_tool_stores[sk] = {}
        _session_tool_stores[sk][key] = value


def get_tool_kv(
    key: str,
    *,
    user_id: str | None = None,
    thread_id: str | None = None,
) -> Any:
    resolved = _resolve_session(user_id=user_id, thread_id=thread_id)
    if resolved is None:
        return None
    uid, tid = resolved
    sk = _session_key(uid, tid)
    with _lock:
        return _session_tool_stores.get(sk, {}).get(key)


# --- OAuth pending state (CSRF + token exchange hand-off) ---

_oauth_states: dict[str, dict[str, Any]] = {}
_OAUTH_TTL_SEC = 900


def register_oauth_state(state: str, payload: dict[str, Any]) -> None:
    with _lock:
        payload = dict(payload)
        payload["expires_at"] = time.time() + _OAUTH_TTL_SEC
        payload.setdefault("result", None)
        _oauth_states[state] = payload


def get_oauth_state_row(state: str) -> dict[str, Any] | None:
    with _lock:
        row = _oauth_states.get(state)
        if row is None:
            return None
        if time.time() > float(row.get("expires_at", 0)):
            _oauth_states.pop(state, None)
            return None
        return row


def delete_oauth_state(state: str) -> None:
    with _lock:
        _oauth_states.pop(state, None)


def get_oauth_result(state: str) -> dict[str, Any] | None:
    """Peek completed OAuth result payload (still valid TTL)."""
    row = get_oauth_state_row(state)
    if row is None:
        return None
    res = row.get("result")
    return res if isinstance(res, dict) else None


def set_oauth_result(state: str, result: dict[str, Any]) -> None:
    with _lock:
        if state not in _oauth_states:
            return
        _oauth_states[state]["result"] = result


def prune_expired_oauth_states() -> None:
    now = time.time()
    with _lock:
        dead = [k for k, v in _oauth_states.items() if now > float(v.get("expires_at", 0))]
        for k in dead:
            _oauth_states.pop(k, None)
