import asyncio
import os
import sys

# Ensure backend/src is in path
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from dotenv import load_dotenv
load_dotenv()

from langchain_core.outputs import LLMResult, Generation, ChatGeneration
from langchain_core.messages import AIMessage
from ai.callbacks import AICreditTrackerCallback
from fastapi_app.utils.supabase_client import get_supabase_admin_client

async def test_credits():
    client = get_supabase_admin_client()
    
    # 1. Get the first user
    res = client.table("profiles").select("id, email, ai_credits_balance").limit(1).execute()
    if not res.data:
        print("No users found in the database. Run seed script or create a user.")
        return
        
    user = res.data[0]
    user_id = user["id"]
    email = user["email"]
    initial_credits = user["ai_credits_balance"]
    
    print(f"Testing for user: {email} (ID: {user_id})")
    print(f"Initial AI Credits Balance: {initial_credits}")
    
    import uuid
    from ai.llms import get_user_proxy_llm
    from langchain_core.tools import tool
    from langgraph.prebuilt import create_react_agent
    
    # Setup dummy tool
    @tool
    def get_current_weather(location: str) -> str:
        """Get the current weather in a given location."""
        print(f"--> Tool called for {location}")
        # Return a large string to artificially inflate input tokens for the second turn
        return f"The weather in {location} is 72 degrees and sunny. " * 50
    
    # 2. Setup chat session for logs
    session_id = str(uuid.uuid4())
    client.table("chat_sessions").insert({"id": session_id, "user_id": user_id, "agent_type": "analyst", "title": "agent_tool_test"}).execute()

    # 3. Create the callback
    callback = AICreditTrackerCallback(user_id=user_id, session_id=session_id)
    
    # 4. Invoke a real Agent call
    print("Executing a real Agent call via create_react_agent...")
    llm = get_user_proxy_llm()
    agent = create_react_agent(llm, tools=[get_current_weather])
    
    res = await agent.ainvoke(
        {"messages": [("user", "What is the weather in San Francisco?")]},
        config={"callbacks": [callback]}
    )
    
    final_message = res["messages"][-1].content
    print(f"\nFinal Agent Response:\n{final_message}\n")
    print(f"Captured Tokens -> Input: {callback.total_input_tokens}, Output: {callback.total_output_tokens}")
    
    print("Flushing and deducting...")
    await callback.flush_and_deduct()
    
    # 5. Verify deduction
    res2 = client.table("profiles").select("ai_credits_balance").eq("id", user_id).limit(1).execute()
    new_credits = res2.data[0]["ai_credits_balance"]
    
    print(f"New AI Credits Balance: {new_credits}")
    print(f"Deducted Amount: {initial_credits - new_credits}")
    
    # 6. Check log
    log_res = client.table("ai_usage_logs").select("*").eq("user_id", user_id).order("created_at", desc=True).limit(1).execute()
    if log_res.data:
        latest_log = log_res.data[0]
        print(f"Latest Log Entry: {latest_log}")
    else:
        print("No log entry found!")

if __name__ == "__main__":
    asyncio.run(test_credits())
