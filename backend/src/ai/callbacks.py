import logging
import uuid
from typing import Any, Dict, List
from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.outputs import LLMResult
from fastapi_app.utils.supabase_client import get_supabase_admin_client

logger = logging.getLogger(__name__)

# Pricing for Claude 3.5 Sonnet (acting as Sonnet 4.5 placeholder)
INPUT_TOKEN_COST = 3.0 / 1_000_000
OUTPUT_TOKEN_COST = 15.0 / 1_000_000
CREDITS_PER_USD = 100 # 1 AI Credit = $0.01

class AICreditTrackerCallback(AsyncCallbackHandler):
    """Callback to track token usage and deduct credits after LLM completion."""
    
    def __init__(self, user_id: str, session_id: str):
        self.user_id = user_id
        self.session_id = session_id
        self.total_input_tokens = 0
        self.total_output_tokens = 0

    async def on_chat_model_start(self, serialized: Dict[str, Any], messages: List[List[Any]], **kwargs: Any) -> None:
        pass

    async def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        """Called when LLM run ends."""
        try:
            if response.generations:
                # Check for usage metadata inside generations
                for generation_list in response.generations:
                    for generation in generation_list:
                        message = getattr(generation, "message", None)
                        if message and hasattr(message, "usage_metadata") and message.usage_metadata:
                            self.total_input_tokens += message.usage_metadata.get("input_tokens", 0)
                            self.total_output_tokens += message.usage_metadata.get("output_tokens", 0)
            
            # Fallback for other providers or older langchain versions
            if response.llm_output and "token_usage" in response.llm_output:
                usage = response.llm_output["token_usage"]
                self.total_input_tokens += usage.get("prompt_tokens", 0)
                self.total_output_tokens += usage.get("completion_tokens", 0)
                
        except Exception as e:
            logger.error("Error extracting token usage: %s", e)
            
    async def flush_and_deduct(self):
        """Called at the end of the streaming route to deduct credits."""
        if self.total_input_tokens == 0 and self.total_output_tokens == 0:
            return
            
        cost_usd = (self.total_input_tokens * INPUT_TOKEN_COST) + (self.total_output_tokens * OUTPUT_TOKEN_COST)
        # Deduct at least 1 credit if there was any usage
        credits_to_deduct = max(1, round(cost_usd * CREDITS_PER_USD))
        
        logger.info("Deducting %s credits for %s USD from user %s", credits_to_deduct, cost_usd, self.user_id)
        
        try:
            # We pass None for session_id to avoid foreign key constraint errors
            # since session_id from LangGraph threads doesn't map to public.chat_sessions yet.
            valid_uuid = None

            client = get_supabase_admin_client()
            client.rpc(
                "deduct_user_credits",
                {
                    "p_user_id": self.user_id,
                    "p_session_id": valid_uuid,
                    "p_input_tokens": self.total_input_tokens,
                    "p_output_tokens": self.total_output_tokens,
                    "p_cost_usd": float(cost_usd),
                    "p_credits_deducted": credits_to_deduct
                }
            ).execute()
        except Exception as e:
            logger.error("Failed to deduct user credits: %s", e)
