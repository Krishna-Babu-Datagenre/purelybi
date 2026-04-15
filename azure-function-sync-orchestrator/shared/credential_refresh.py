"""Credential refresh for scheduled (headless) sync jobs.

This module handles automatic token refresh WITHOUT human interaction.
It supports 3 refresh patterns that cover ALL OAuth providers:

1. client_credentials — Mint a fresh access token using client_id +
   client_secret. No stored token needed. (e.g. Shopify custom apps)

2. refresh_token — Use a long-lived refresh_token (obtained during
   initial OAuth consent) to get a new short-lived access_token.
   This is the **standard OAuth2 pattern** and is the DEFAULT for any
   connector that returns a refresh_token. (e.g. Google, HubSpot,
   Salesforce, Zendesk, and most other OAuth connectors)

3. token_exchange — Exchange the current long-lived token for a new
   one before it expires. (e.g. Facebook Marketing)

4. static — Tokens don't expire or the connector uses API keys.
   No refresh needed. (e.g. GitHub, Slack, Stripe API key)

The refresh strategy is determined at runtime from the __oauth_meta__
dict persisted during onboarding — NOT from a hardcoded connector list.
Only non-standard connectors need explicit overrides in GRANT_TYPE_OVERRIDES.

Usage (in sync_worker.py or Azure Container App job):
    from src.credential_refresh import ensure_fresh_credentials

    config, updated = ensure_fresh_credentials(config, oauth_meta)
    if updated:
        # persist the new tokens back to DB/Supabase
        save_updated_config(config, oauth_meta)
"""

from __future__ import annotations

import logging
import time

import requests

logger = logging.getLogger(__name__)

# Maximum retries for transient HTTP errors (5xx, timeouts, connection errors)
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds: 2, 4, 8


def _request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    """Make an HTTP request with exponential backoff on transient failures.

    Retries on: 5xx status codes, timeouts, and connection errors.
    Does NOT retry on 4xx (those indicate invalid credentials, not transient issues).
    """
    kwargs.setdefault("timeout", 60)
    last_exc = None

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.request(method, url, **kwargs)
            # 5xx = server error, retry
            if resp.status_code >= 500 and attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                logger.warning(
                    "Token endpoint returned %d, retrying in %ds (attempt %d/%d)",
                    resp.status_code,
                    wait,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(wait)
                continue
            return resp
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                logger.warning(
                    "Token request failed (%s), retrying in %ds (attempt %d/%d)",
                    type(e).__name__,
                    wait,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(wait)
            else:
                raise TokenRefreshError(
                    f"Token endpoint unreachable after {MAX_RETRIES} attempts: {e}"
                ) from e

    # Should not reach here, but just in case
    raise TokenRefreshError(
        f"Token refresh failed after {MAX_RETRIES} attempts: {last_exc}"
    )


# ── Grant type overrides for non-standard connectors ──────────────────
# Most OAuth connectors use the standard "refresh_token" grant. Only
# connectors with non-standard flows need an explicit override here.
# The token_url, credentials_path, etc. come from oauth_meta at runtime.
GRANT_TYPE_OVERRIDES: dict[str, dict] = {
    "airbyte/source-shopify": {
        "grant_type": "client_credentials",
        "shop_required": True,
    },
    "airbyte/source-facebook-marketing": {
        "grant_type": "token_exchange",
        "token_lifetime_seconds": 5_184_000,  # ~60 days
    },
}

# Kept for backward compatibility — modules that import REFRESH_STRATEGIES
REFRESH_STRATEGIES = GRANT_TYPE_OVERRIDES


class TokenRefreshError(Exception):
    """Raised when token refresh fails (e.g. revoked refresh token)."""

    pass


class ReauthRequired(TokenRefreshError):
    """Raised when the user must re-authenticate interactively."""

    pass


def get_strategy(docker_repository: str) -> dict:
    """Look up the refresh strategy for a connector.

    Only used for the grant_type override. The actual token_url and
    credentials_path come from oauth_meta at runtime.
    """
    return GRANT_TYPE_OVERRIDES.get(docker_repository, {})


def _resolve_strategy(oauth_meta: dict) -> dict:
    """Build a full strategy dict from oauth_meta + any overrides.

    This is the main strategy resolver. It combines:
    1. grant_type overrides for non-standard connectors (GRANT_TYPE_OVERRIDES)
    2. Runtime metadata captured during onboarding (oauth_meta)
    3. Intelligent defaults (refresh_token if refresh_token present, else static)

    The result is a complete strategy dict with grant_type, token_url,
    and credentials_path — everything needed to refresh credentials.
    """
    docker_repo = oauth_meta.get("docker_repository", "")
    override = GRANT_TYPE_OVERRIDES.get(docker_repo, {})

    # Determine grant_type: explicit override > oauth_meta > auto-detect
    grant_type = (
        override.get("grant_type")
        or oauth_meta.get("grant_type")
        or _detect_grant_type(oauth_meta)
    )

    # token_url: captured during onboarding in oauth_meta
    token_url = oauth_meta.get("token_url", "")

    strategy = {
        "grant_type": grant_type,
        "token_url": token_url,
        "credentials_path": oauth_meta.get("credentials_path", []),
        **override,  # merge any override fields (shop_required, token_lifetime_seconds)
    }

    # For overrides that have their own token_url (not needed now since
    # token_url comes from oauth_meta, but kept as fallback)
    if not strategy["token_url"] and override.get("token_url"):
        strategy["token_url"] = override["token_url"]

    return strategy


def _detect_grant_type(oauth_meta: dict) -> str:
    """Auto-detect the appropriate grant type from what's in oauth_meta.

    Logic:
    - Has refresh_token → "refresh_token" (standard OAuth2)
    - Has client_id + client_secret but no refresh_token → "static"
      (API-key-style OAuth, tokens usually don't expire)
    - Nothing → "static"
    """
    if oauth_meta.get("refresh_token"):
        return "refresh_token"
    return "static"


def _get_nested(config: dict, path: list[str]) -> dict:
    """Navigate into a nested dict following a key path."""
    target = config
    for key in path:
        if not isinstance(target, dict) or key not in target:
            return {}
        target = target[key]
    return target if isinstance(target, dict) else {}


def _set_nested(config: dict, path: list[str], key: str, value) -> None:
    """Set a value deep inside a nested dict, creating intermediaries."""
    target = config
    for p in path:
        if p not in target or not isinstance(target[p], dict):
            target[p] = {}
        target = target[p]
    target[key] = value


def _is_expired(
    oauth_meta: dict, strategy: dict, buffer_seconds: int = 300
) -> bool:
    """Check if the current access token needs refreshing.

    For token_exchange (Facebook), we use a proactive threshold of 50% of the
    token's lifetime instead of a fixed 5-minute buffer. This is because Facebook
    tokens can't be refreshed after expiry — a lapsed token means full re-auth.
    Refreshing at the halfway point (e.g. day 30 of 60) gives a large safety margin.

    For refresh_token (Google, HubSpot), a 5-minute buffer is sufficient because
    the refresh_token itself is long-lived and can always mint a new access_token.
    """
    expires_at = oauth_meta.get("expires_at", 0)
    if expires_at == 0:
        # No expiry tracked — assume expired to be safe
        return True

    grant_type = strategy.get("grant_type", "")
    if grant_type == "token_exchange":
        # Proactive: refresh at 50% of lifetime to avoid catastrophic expiry
        obtained_at = oauth_meta.get("refreshed_at") or oauth_meta.get(
            "obtained_at", 0
        )
        if obtained_at:
            lifetime = expires_at - obtained_at
            halfway = obtained_at + (lifetime * 0.5)
            return time.time() > halfway
        # Fallback: use 7-day buffer for Facebook (conservative)
        return time.time() > (expires_at - 7 * 86400)

    return time.time() > (expires_at - buffer_seconds)


# ── Pattern 1: client_credentials ─────────────────────────────────────


def _refresh_client_credentials(
    config: dict, oauth_meta: dict, strategy: dict
) -> dict:
    """Mint a fresh token using client_id + client_secret (Shopify)."""
    creds_path = strategy.get("credentials_path", [])
    creds = _get_nested(config, creds_path) if creds_path else config

    client_id = creds.get("client_id") or oauth_meta.get("client_id")
    client_secret = creds.get("client_secret") or oauth_meta.get(
        "client_secret"
    )
    shop = oauth_meta.get("shop") or creds.get("shop")

    if not client_id or not client_secret:
        raise TokenRefreshError(
            "client_credentials refresh requires client_id and client_secret"
        )

    token_url = strategy.get("token_url") or oauth_meta.get("token_url", "")
    if "{shop}" in token_url:
        if not shop:
            raise TokenRefreshError(
                "Shop name required for Shopify token refresh"
            )
        shop_name = shop.replace(".myshopify.com", "").strip()
        token_url = token_url.replace("{shop}", shop_name)

    logger.info("Refreshing via client_credentials: %s", token_url)
    resp = _request_with_retry(
        "POST",
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if resp.status_code >= 400:
        raise TokenRefreshError(
            f"client_credentials refresh failed ({resp.status_code}): {resp.text[:500]}"
        )

    token_data = resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise TokenRefreshError(f"No access_token in response: {token_data}")

    # Write new token into config
    if creds_path:
        _set_nested(config, creds_path, "access_token", access_token)
    else:
        config["access_token"] = access_token

    # Update expiry in oauth_meta (Shopify tokens are long-lived but we
    # refresh every time anyway, so set a generous expiry)
    oauth_meta["expires_at"] = time.time() + 86400 * 365
    oauth_meta["refreshed_at"] = time.time()

    logger.info("client_credentials refresh succeeded")
    return config


# ── Pattern 2: refresh_token ──────────────────────────────────────────


def _refresh_via_refresh_token(
    config: dict, oauth_meta: dict, strategy: dict
) -> dict:
    """Use a stored refresh_token to get a new access_token (Google, HubSpot)."""
    creds_path = strategy.get("credentials_path", [])
    creds = _get_nested(config, creds_path) if creds_path else config

    refresh_token = creds.get("refresh_token") or oauth_meta.get(
        "refresh_token"
    )
    client_id = creds.get("client_id") or oauth_meta.get("client_id")
    client_secret = creds.get("client_secret") or oauth_meta.get(
        "client_secret"
    )

    if not refresh_token:
        raise ReauthRequired(
            "No refresh_token available. User must re-authenticate."
        )
    if not client_id or not client_secret:
        raise TokenRefreshError(
            "refresh_token flow requires client_id and client_secret"
        )

    token_url = strategy["token_url"]
    logger.info("Refreshing via refresh_token: %s", token_url)

    resp = _request_with_retry(
        "POST",
        token_url,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if resp.status_code == 400 or resp.status_code == 401:
        raise ReauthRequired(
            f"Refresh token rejected ({resp.status_code}): {resp.text[:500]}. "
            "User must re-authenticate."
        )
    if resp.status_code >= 400:
        raise TokenRefreshError(
            f"refresh_token request failed ({resp.status_code}): {resp.text[:500]}"
        )

    token_data = resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise TokenRefreshError(f"No access_token in response: {token_data}")

    # Write new access_token into config
    if creds_path:
        _set_nested(config, creds_path, "access_token", access_token)
    else:
        config["access_token"] = access_token

    # Some providers rotate the refresh token — store it if present
    new_refresh = token_data.get("refresh_token")
    if new_refresh:
        if creds_path:
            _set_nested(config, creds_path, "refresh_token", new_refresh)
        else:
            config["refresh_token"] = new_refresh
        oauth_meta["refresh_token"] = new_refresh

    # Update expiry
    expires_in = token_data.get("expires_in", 3600)
    oauth_meta["expires_at"] = time.time() + int(expires_in)
    oauth_meta["refreshed_at"] = time.time()

    logger.info(
        "refresh_token succeeded, new access_token expires in %ss",
        expires_in,
    )
    return config


# ── Pattern 3: token_exchange (Facebook) ──────────────────────────────


def _refresh_via_token_exchange(
    config: dict, oauth_meta: dict, strategy: dict
) -> dict:
    """Exchange current long-lived token for a new one (Facebook).

    Facebook long-lived tokens last ~60 days. Before expiry, you can
    exchange the current one for a fresh one. If it's already expired,
    the user must re-authorize.
    """
    creds_path = strategy.get("credentials_path", [])
    creds = _get_nested(config, creds_path) if creds_path else config

    current_token = creds.get("access_token") or oauth_meta.get("access_token")
    client_id = creds.get("client_id") or oauth_meta.get("client_id")
    client_secret = creds.get("client_secret") or oauth_meta.get(
        "client_secret"
    )

    if not current_token:
        raise ReauthRequired(
            "No access_token available for exchange. User must re-authenticate."
        )
    if not client_id or not client_secret:
        raise TokenRefreshError(
            "token_exchange requires client_id and client_secret"
        )

    token_url = strategy["token_url"]
    logger.info("Exchanging token via fb_exchange_token: %s", token_url)

    resp = _request_with_retry(
        "GET",
        token_url,
        params={
            "grant_type": "fb_exchange_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "fb_exchange_token": current_token,
        },
    )

    if resp.status_code == 400 or resp.status_code == 401:
        raise ReauthRequired(
            f"Token exchange rejected ({resp.status_code}): {resp.text[:500]}. "
            "The token has expired. User must re-authenticate."
        )
    if resp.status_code >= 400:
        raise TokenRefreshError(
            f"token_exchange failed ({resp.status_code}): {resp.text[:500]}"
        )

    token_data = resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise TokenRefreshError(f"No access_token in response: {token_data}")

    # Write new token into config
    if creds_path:
        _set_nested(config, creds_path, "access_token", access_token)
    else:
        config["access_token"] = access_token

    # Update expiry
    expires_in = token_data.get(
        "expires_in", strategy.get("token_lifetime_seconds", 5_184_000)
    )
    oauth_meta["expires_at"] = time.time() + int(expires_in)
    oauth_meta["refreshed_at"] = time.time()

    logger.info(
        "token_exchange succeeded, new token expires in %s days",
        int(expires_in) // 86400,
    )
    return config


# ── Main entry point ──────────────────────────────────────────────────


def upgrade_to_long_lived_token(
    docker_repository: str,
    short_lived_token: str,
    client_id: str,
    client_secret: str,
) -> dict | None:
    """Exchange a short-lived token for a long-lived one during onboarding.

    Currently only Facebook requires this: the initial OAuth code→token
    exchange returns a ~1hr token. This MUST be upgraded to a ~60-day
    long-lived token before saving, otherwise the first scheduled sync
    (which may be hours later) will fail.

    Args:
        docker_repository: e.g. "airbyte/source-facebook-marketing"
        short_lived_token: The short-lived access token from the OAuth flow
        client_id: OAuth app Client ID
        client_secret: OAuth app Client Secret

    Returns:
        Dict with "access_token" and "expires_in" if upgraded, or None
        if this connector doesn't need it.
    """
    strategy = GRANT_TYPE_OVERRIDES.get(docker_repository, {})
    if strategy.get("grant_type") != "token_exchange":
        return None

    # Look up the token_url from OAUTH_PROVIDERS (imported at call time
    # to avoid circular import at module level)
    from src.oauth import OAUTH_PROVIDERS

    provider = OAUTH_PROVIDERS.get(docker_repository, {})
    token_url = provider.get("token_url", "")
    if not token_url:
        logger.warning(
            "No token_url for %s, cannot upgrade token", docker_repository
        )
        return None
    logger.info("Upgrading short-lived token to long-lived: %s", token_url)

    resp = _request_with_retry(
        "GET",
        token_url,
        params={
            "grant_type": "fb_exchange_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "fb_exchange_token": short_lived_token,
        },
    )

    if resp.status_code >= 400:
        logger.error(
            "Failed to upgrade to long-lived token (%d): %s",
            resp.status_code,
            resp.text[:500],
        )
        return None

    token_data = resp.json()
    if token_data.get("access_token"):
        logger.info(
            "Upgraded to long-lived token (expires_in=%s)",
            token_data.get("expires_in", "unknown"),
        )
        return token_data

    return None


def ensure_fresh_credentials(
    config: dict,
    oauth_meta: dict,
) -> tuple[dict, bool]:
    """Ensure the config has valid, non-expired credentials.

    Call this before every sync job. It checks the token expiry and
    refreshes if needed using the appropriate strategy.

    Args:
        config: The connector configuration dict (will be mutated in place).
        oauth_meta: Metadata dict with:
            - docker_repository: e.g. "airbyte/source-google-ads"
            - client_id, client_secret: OAuth app credentials
            - refresh_token: (for refresh_token grant)
            - expires_at: Unix timestamp of token expiry
            - shop: (for Shopify)

    Returns:
        Tuple of (updated_config, was_refreshed).
        If was_refreshed is True, persist the updated config + oauth_meta.

    Raises:
        ReauthRequired: User must re-authenticate (revoked/expired token).
        TokenRefreshError: Transient error during refresh.
    """
    docker_repo = oauth_meta.get("docker_repository", "")
    strategy = _resolve_strategy(oauth_meta)
    grant_type = strategy["grant_type"]

    if grant_type == "static":
        logger.debug(
            "Static credentials for %s, no refresh needed", docker_repo
        )
        return config, False

    # Validate that we have a token_url for non-static strategies
    if not strategy.get("token_url"):
        logger.warning(
            "No token_url in oauth_meta for %s (grant_type=%s). "
            "Cannot refresh. Was this connector onboarded before the "
            "token_url capture was added?",
            docker_repo,
            grant_type,
        )
        return config, False

    # client_credentials always mints a fresh token (cheap, no stored state)
    if grant_type == "client_credentials":
        config = _refresh_client_credentials(config, oauth_meta, strategy)
        return config, True

    # For refresh_token and token_exchange, only refresh if expired
    if not _is_expired(oauth_meta, strategy):
        logger.debug("Token still valid for %s, skipping refresh", docker_repo)
        return config, False

    if grant_type == "refresh_token":
        config = _refresh_via_refresh_token(config, oauth_meta, strategy)
        return config, True

    if grant_type == "token_exchange":
        config = _refresh_via_token_exchange(config, oauth_meta, strategy)
        return config, True

    logger.warning("Unknown grant_type '%s', skipping refresh", grant_type)
    return config, False
