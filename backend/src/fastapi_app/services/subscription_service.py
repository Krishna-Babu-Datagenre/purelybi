import logging
from typing import Any, Dict, Tuple
from datetime import datetime, timezone

from fastapi import HTTPException, status
from fastapi_app.utils.supabase_client import get_supabase_admin_client

logger = logging.getLogger(__name__)

def get_user_subscription(user_id: str) -> Dict[str, Any]:
    """Fetch the user's profile and active subscription plan."""
    client = get_supabase_admin_client()
    
    # We join profiles with subscription_plans using Supabase's resource embedding
    res = (
        client.table("profiles")
        .select("*, subscription_tier(*)")
        .eq("id", user_id)
        .limit(1)
        .execute()
    )
    
    if not res.data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User profile not found."
        )
        
    profile = res.data[0]
    return profile

def get_user_credits(user_id: str) -> int:
    """Return the current AI credits balance for the user."""
    profile = get_user_subscription(user_id)
    return profile.get("ai_credits_balance", 0)

def can_create_dashboard(user_id: str) -> bool:
    """Check if the user has reached their max dashboards limit."""
    profile = get_user_subscription(user_id)
    plan = profile.get("subscription_tier")
    if not plan:
        return False
        
    max_dashboards = plan.get("max_dashboards", 0)
    
    client = get_supabase_admin_client()
    res = (
        client.table("dashboards")
        .select("id", count="exact")
        .eq("user_id", user_id)
        .execute()
    )
    
    current_count = res.count if res.count is not None else 0
    return current_count < max_dashboards

def can_add_source(user_id: str) -> bool:
    """Check if the user has reached their max data sources limit."""
    profile = get_user_subscription(user_id)
    plan = profile.get("subscription_tier")
    if not plan:
        return False
        
    max_sources = plan.get("max_data_sources", 0)
    
    client = get_supabase_admin_client()
    res = (
        client.table("user_connector_configs")
        .select("id", count="exact")
        .eq("user_id", user_id)
        .eq("is_active", True)
        .execute()
    )
    
    current_count = res.count if res.count is not None else 0
    return current_count < max_sources

def can_sync_now(user_id: str) -> Tuple[bool, int]:
    """Return whether the user can sync now, based on min_sync_frequency_minutes.
    Returns (bool, min_frequency_minutes).
    (Actual implementation checks last sync timestamp vs now for specific sources, 
     but this returns the plan limits)."""
    profile = get_user_subscription(user_id)
    plan = profile.get("subscription_tier")
    if not plan:
        return False, 1440
        
    min_freq = plan.get("min_sync_frequency_minutes", 1440)
    return True, min_freq

def check_trial_status(user_id: str) -> bool:
    """Check if the user is on a free plan and if the 7-day trial has expired.
    Returns False if the trial is expired, True otherwise."""
    profile = get_user_subscription(user_id)
    plan = profile.get("subscription_tier")
    
    if plan and plan.get("tier_name") == "Free":
        trial_ends_at_str = profile.get("trial_ends_at")
        if trial_ends_at_str:
            # Parse ISO 8601 string
            try:
                # Replace 'Z' with '+00:00' for fromisoformat if needed
                if trial_ends_at_str.endswith('Z'):
                    trial_ends_at_str = trial_ends_at_str[:-1] + '+00:00'
                trial_ends_at = datetime.fromisoformat(trial_ends_at_str)
                if datetime.now(timezone.utc) > trial_ends_at:
                    return False
            except ValueError:
                logger.error("Failed to parse trial_ends_at: %s", trial_ends_at_str)
                pass
    return True
