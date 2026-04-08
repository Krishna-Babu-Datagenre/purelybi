"""LLM for the onboarding agent (same Azure env vars as analyst)."""

from __future__ import annotations

import os


# def get_onboarding_llm():
#     from langchain_openai import AzureChatOpenAI

#     return AzureChatOpenAI(
#         azure_deployment=os.getenv("AZURE_LLM_NAME"),
#         api_key=os.getenv("AZURE_LLM_API_KEY"),
#         azure_endpoint=os.getenv("AZURE_LLM_ENDPOINT"),
#         api_version=os.getenv("AZURE_LLM_API_VERSION"),
#         streaming=True,
#     )


def get_onboarding_llm():
    from langchain_anthropic import ChatAnthropic

    return ChatAnthropic(
        anthropic_api_url=os.getenv("AZURE_LLM_ENDPOINT"),
        anthropic_api_key=os.getenv("AZURE_LLM_API_KEY"),
        model=os.getenv("AZURE_LLM_NAME"),
        streaming=True,
    )
