import os

from langchain.agents import create_agent
from langchain.tools import tool
from langchain_openai import ChatOpenAI

from .math import MathAgent

SYSTEM_PROMPT = (
    "You are a helpful personal assistant. "
    "You can schedule calendar events and send emails. "
    "Break down user requests into appropriate tool calls and coordinate the results. "
    "When a request involves multiple actions, use multiple tools in sequence. "
    "Return your final answer as a markdown-formatted string."
)


@tool
def calculate(request: str) -> str:
    """Perform a mathematical calculation based on a natural language query.

    Use this when the user wants to perform calculations or solve math problems.

    Input: Natural language math query (e.g., 'What is 25% of 200?')
    """
    result = (
        MathAgent(llm="gpt-4.1-mini")
        .get_agent()
        .invoke({"messages": [{"role": "user", "content": request}]})
    )
    return result["messages"][-1].text


@tool
def query_database(request: str) -> str:
    """Query a SQL database using natural language.

    Use this when the user wants to retrieve information from a database.

    Input: Natural language database query (e.g., 'Show me the top 5 artists by album count.')
    """
    return (
        "Database querying is only available through the dedicated analyst chat "
        "endpoint/session, where tenant-scoped DuckDB context is attached."
    )


class SupervisorAgent:
    def __init__(self, llm="gpt-4.1-mini", checkpointer=None):
        # Initialize the base LLM
        model = ChatOpenAI(
            model=llm,
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            base_url=os.getenv("AZURE_OPENAI_ENDPOINT"),
            streaming=True,  # required for token-by-token SSE in FastAPI chat
        )

        self.agent = create_agent(
            model,
            tools=[calculate, query_database],
            system_prompt=SYSTEM_PROMPT,
            checkpointer=checkpointer,
        )

        self.starter_prompts = {
            ":blue[:material/database:] How many journeys were there in 2025?": (
                "How many journeys were there in 2025?"
            ),
            ":blue[:material/database:] YoY growth in journeys": (
                "What is the year-over-year growth in journeys between 2024 and 2025?"
            ),
        }

    def get_agent(self):
        return self.agent

    def get_starter_prompts(self):
        return self.starter_prompts
