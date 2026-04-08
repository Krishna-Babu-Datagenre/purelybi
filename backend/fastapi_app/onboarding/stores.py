"""Module-level stores for UI specs and OAuth hand-off (mirrors Streamlit prototype)."""

from __future__ import annotations

import threading
import time
from typing import Any

_tool_store: dict[str, Any] = {
    "pending_ui": None,
    "oauth_client_id": None,
    "oauth_client_secret": None,
    "oauth_docker_repo": None,
    "oauth_shop": None,
    "oauth_meta": None,
    "oauth_state": None,
    "oauth_thread_id": None,
}

_lock = threading.Lock()

SECRET_REF_PREFIX = "__SECRET_REF__:"
_secret_store: dict[str, str] = {}


def drain_tool_store() -> dict[str, Any]:
    """Return a copy of stored values and reset keys to None."""
    with _lock:
        snapshot = dict(_tool_store)
        for k in _tool_store:
            _tool_store[k] = None
        return snapshot


def peek_pending_ui() -> dict[str, Any] | None:
    with _lock:
        ui = _tool_store.get("pending_ui")
        return ui if isinstance(ui, dict) else None


def clear_pending_ui() -> None:
    with _lock:
        _tool_store["pending_ui"] = None


def store_secret(key: str, value: str) -> str:
    with _lock:
        _secret_store[key] = value
    return f"{SECRET_REF_PREFIX}{key}"


def resolve_secrets(obj: Any) -> Any:
    if isinstance(obj, str) and obj.startswith(SECRET_REF_PREFIX):
        secret_key = obj[len(SECRET_REF_PREFIX) :]
        with _lock:
            return _secret_store.get(secret_key, obj)
    if isinstance(obj, dict):
        return {k: resolve_secrets(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_secrets(item) for item in obj]
    return obj


def clear_secrets() -> None:
    with _lock:
        _secret_store.clear()


def set_tool_kv(key: str, value: Any) -> None:
    with _lock:
        _tool_store[key] = value


def get_tool_kv(key: str) -> Any:
    with _lock:
        return _tool_store.get(key)


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
