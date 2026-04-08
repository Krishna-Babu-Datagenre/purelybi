"""OAuth 2.0 helpers for Airbyte connectors (no Streamlit; state is caller-managed)."""

from __future__ import annotations

import time
import urllib.parse
from typing import Any

import requests

OAUTH_PROVIDERS: dict[str, dict[str, Any]] = {
    "airbyte/source-shopify": {
        "authorize_url": "https://{shop}.myshopify.com/admin/oauth/authorize",
        "token_url": "https://{shop}.myshopify.com/admin/oauth/access_token",
        "scopes": (
            "read_products,read_orders,read_customers,read_inventory,"
            "read_fulfillments,read_draft_orders,read_content,"
            "read_themes,read_locations,read_discounts,"
            "read_price_rules,read_marketing_events"
        ),
        "shop_required": True,
    },
    "airbyte/source-facebook-marketing": {
        "authorize_url": "https://www.facebook.com/v19.0/dialog/oauth",
        "token_url": "https://graph.facebook.com/v19.0/oauth/access_token",
        "scopes": "ads_read,ads_management,business_management",
    },
    "airbyte/source-hubspot": {
        "authorize_url": "https://app.hubspot.com/oauth/authorize",
        "token_url": "https://api.hubapi.com/oauth/v1/token",
        "scopes": (
            "crm.objects.contacts.read,crm.objects.companies.read,"
            "crm.objects.deals.read,content"
        ),
    },
    "airbyte/source-google-ads": {
        "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_url": "https://oauth2.googleapis.com/token",
        "scopes": "https://www.googleapis.com/auth/adwords",
        "extra_params": {"access_type": "offline", "prompt": "consent"},
    },
    "airbyte/source-github": {
        "authorize_url": "https://github.com/login/oauth/authorize",
        "token_url": "https://github.com/login/oauth/access_token",
        "scopes": "repo,read:org,read:user",
    },
    "airbyte/source-google-analytics-data-api": {
        "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_url": "https://oauth2.googleapis.com/token",
        "scopes": "https://www.googleapis.com/auth/analytics.readonly",
        "extra_params": {"access_type": "offline", "prompt": "consent"},
    },
    "airbyte/source-slack": {
        "authorize_url": "https://slack.com/oauth/v2/authorize",
        "token_url": "https://slack.com/api/oauth.v2.access",
        "scopes": "channels:history,channels:read,users:read",
    },
}


def supports_oauth(docker_repository: str, oauth_config: dict | None) -> bool:
    return bool(
        docker_repository in OAUTH_PROVIDERS
        and oauth_config
        and oauth_config.get("auth_flow_type") == "oauth2.0"
    )


def build_authorize_url(
    *,
    docker_repository: str,
    client_id: str,
    redirect_uri: str,
    state: str,
    shop: str | None = None,
) -> str:
    provider = OAUTH_PROVIDERS[docker_repository]
    authorize_url = provider["authorize_url"]
    if "{shop}" in authorize_url:
        if not shop:
            raise ValueError("shop is required for this connector")
        shop_name = shop.replace(".myshopify.com", "").strip()
        authorize_url = authorize_url.replace("{shop}", shop_name)

    params: dict[str, Any] = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": provider.get("scopes", ""),
        "state": state,
        "response_type": "code",
    }
    params.update(provider.get("extra_params", {}))
    return f"{authorize_url}?{urllib.parse.urlencode(params)}"


def exchange_code_for_token(
    *,
    docker_repository: str,
    code: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    shop: str | None = None,
) -> dict[str, Any]:
    provider = OAUTH_PROVIDERS[docker_repository]
    token_url = provider["token_url"]
    if "{shop}" in token_url:
        shop_name = (shop or "").replace(".myshopify.com", "").strip()
        token_url = token_url.replace("{shop}", shop_name)

    payload: dict[str, Any] = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
    }
    shop_required = provider.get("shop_required", False)
    if not shop_required:
        payload["redirect_uri"] = redirect_uri
        payload["grant_type"] = "authorization_code"

    resp = requests.post(
        token_url,
        data=payload,
        headers={"Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def build_oauth_meta(
    *,
    docker_repository: str,
    token_response: dict[str, Any],
    client_id: str,
    client_secret: str,
    shop: str | None = None,
) -> dict[str, Any]:
    grant_type = (
        "refresh_token"
        if token_response.get("refresh_token")
        else "static"
    )
    provider = OAUTH_PROVIDERS.get(docker_repository, {})
    token_url = provider.get("token_url", "")
    if shop and "{shop}" in token_url:
        shop_name = shop.replace(".myshopify.com", "").strip()
        token_url = token_url.replace("{shop}", shop_name)

    meta: dict[str, Any] = {
        "docker_repository": docker_repository,
        "grant_type": grant_type,
        "token_url": token_url,
        "client_id": client_id,
        "client_secret": client_secret,
        "credentials_path": [],
        "obtained_at": time.time(),
    }
    if shop:
        meta["shop"] = shop
    if token_response.get("refresh_token"):
        meta["refresh_token"] = token_response["refresh_token"]
    expires_in = token_response.get("expires_in")
    if expires_in:
        meta["expires_at"] = time.time() + int(expires_in)
    else:
        meta["expires_at"] = 0
    return meta


def inject_tokens_into_config(
    config: dict[str, Any],
    token_response: dict[str, Any],
    oauth_config: dict[str, Any],
) -> dict[str, Any]:
    output_spec = (
        oauth_config.get("oauth_config_specification", {})
        .get("complete_oauth_output_specification", {})
        .get("properties", {})
    )
    for token_name, spec in output_spec.items():
        path = spec.get("path_in_connector_config", [])
        value = token_response.get(token_name)
        if value is None or not path:
            continue
        target = config
        for key in path[:-1]:
            if key not in target or not isinstance(target[key], dict):
                target[key] = {}
            target = target[key]
        target[path[-1]] = value
    return config
