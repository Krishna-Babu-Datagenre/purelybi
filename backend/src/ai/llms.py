"""LLM configuration shared by agents (same Azure / Anthropic env vars)."""

from __future__ import annotations

import os


def get_onboarding_llm():
    from langchain_anthropic import ChatAnthropic

    return ChatAnthropic(
        anthropic_api_url=os.getenv("AZURE_LLM_ENDPOINT"),
        anthropic_api_key=os.getenv("AZURE_LLM_API_KEY"),
        model=os.getenv("AZURE_LLM_NAME"),
        streaming=True,
    )


def get_analyst_llm():
    """Default chat model for the SQL / analytics agent."""
    from langchain_anthropic import ChatAnthropic

    return ChatAnthropic(
        anthropic_api_url=os.getenv("AZURE_LLM_ENDPOINT"),
        anthropic_api_key=os.getenv("AZURE_LLM_API_KEY"),
        model=os.getenv("AZURE_LLM_NAME"),
        streaming=True,
    )


def get_user_proxy_llm():
    """Small non-streaming model for Magic Mode user-proxy decisions."""
    from langchain_anthropic import ChatAnthropic

    return ChatAnthropic(
        anthropic_api_url=os.getenv("AZURE_LLM_ENDPOINT"),
        anthropic_api_key=os.getenv("AZURE_LLM_API_KEY"),
        model=os.getenv("AZURE_LLM_NAME"),
        streaming=False,
    )
