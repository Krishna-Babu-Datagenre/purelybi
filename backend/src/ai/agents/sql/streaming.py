"""Server-Sent Events for the SQL chat agent.

The runnable implementation is in :mod:`fastapi_app.services.chat_service`
(``stream_agent_response``) to avoid circular imports between ``ai`` and
``fastapi_app``.
"""
