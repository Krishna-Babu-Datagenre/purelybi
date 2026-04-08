import os

import duckdb
from langchain.agents import create_agent
from langchain.agents.middleware import SummarizationMiddleware

from streamchat.tools.duckdb_tools import build_duckdb_tools
from streamchat.tools import (
    calculate,
    create_react_chart,
    create_react_kpi,
    get_current_time,
)
# from langchain_azure_ai.chat_models import AzureAIOpenAIApiChatModel
# LLM = AzureAIOpenAIApiChatModel(
#     # The 'endpoint' is your Azure AI Foundry project or model deployment URL
#     endpoint=os.getenv("AZURE_LLM_ENDPOINT"),
#     # Use your Azure AI Foundry API Key
#     credential=os.getenv("AZURE_LLM_API_KEY"),
#     # This must match your 'Deployment Name' in the Foundry portal (e.g., "claude-35-sonnet")
#     model=os.getenv("AZURE_LLM_NAME"),
#     # Streaming is supported natively by this class
#     # streaming=True,
# )

# from langchain_openai import ChatOpenAI
# LLM = ChatOpenAI(
#     model=os.getenv("AZURE_LLM_NAME"),
#     api_key=os.getenv("AZURE_LLM_API_KEY"),
#     base_url=os.getenv("AZURE_LLM_ENDPOINT"),
#     api_version=os.getenv("AZURE_LLM_API_VERSION"),
#     streaming=True,  # required for token-by-token SSE in FastAPI chat
# )

# from langchain_openai import AzureChatOpenAI
# LLM = AzureChatOpenAI(
#     azure_deployment=os.getenv("AZURE_LLM_NAME"),
#     api_key=os.getenv("AZURE_LLM_API_KEY"),
#     azure_endpoint=os.getenv("AZURE_LLM_ENDPOINT"),
#     api_version=os.getenv("AZURE_LLM_API_VERSION"),
#     streaming=True,
# )

from langchain_anthropic import ChatAnthropic

LLM = ChatAnthropic(
    anthropic_api_url=os.getenv("AZURE_LLM_ENDPOINT"),
    anthropic_api_key=os.getenv("AZURE_LLM_API_KEY"),
    model=os.getenv("AZURE_LLM_NAME"),
    streaming=True,
)

class AnalystAgent:
    """Agent designed to interact with a SQL database for data analysis tasks."""

    def __init__(
        self,
        llm="gpt-4.1-mini",
        checkpointer=None,
        database: str = "DuckDB",
        conn: duckdb.DuckDBPyConnection | None = None,
    ):
        if database.lower() != "duckdb":
            raise ValueError("Only DuckDB is supported by AnalystAgent.")
        if conn is None:
            raise ValueError("DuckDB connection is required for AnalystAgent.")
        model = LLM if llm is None or isinstance(llm, str) else llm
        database_tools = build_duckdb_tools(conn, model)

        system_prompt = """## Role
You are a **chatbot** answering natural-language questions about user's
data using DuckDB. You write correct SQL, interpret results, and explain
findings clearly for a BI web app (React UI). Charts use ECharts configs from
your tools; KPI cards use the dashboard KPI schema from ``create_react_kpi``.

## Data
The schema is not fixed—it may grow over time. Always discover what exist (`sql_db_list_tables`, `sql_db_schema`) before querying; use only tables and columns returned there. Do not invent metrics, dimensions, or filters.

## Greetings
When the user says "hi", "hello", or any casual greeting, respond with a friendly intro: greet them back, briefly explain that you can answer questions about their data, and suggest some example questions they can ask."

## Conversation rules
- Treat each user message as standalone unless they explicitly refer to earlier context.
- If scope is unclear, prefer a broader query; narrow only when the question demands it.

## Workflow (every turn)
1. List or inspect tables (`sql_db_list_tables` / `sql_db_schema`) before writing SQL—never skip discovery.
2. Pull schema for tables you will use; align filters to actual column values.
3. Run `sql_db_query`; on error, fix the SQL and retry.

## SQL rules
- Read-only: SELECT only. No INSERT, UPDATE, DELETE, DROP, or DDL.
- Default `LIMIT {top_k}` unless the user asks for a different row count; order by a column that matches the question.
- Select only columns you need—never `SELECT *` for exploration of whole tables in the final answer path.
- Date keys use integer `YYYYMMDD` where applicable. Do not use data before 2024 unless the user asks for older history.
- Before filtering on categorical values, confirm plausible values from the data or schema; avoid guessing labels.
- For rankings, comparisons, or "top/best/worst" style questions, exclude rows where relevant dimensions are 'Unknown'. For broad aggregates (totals, averages, overall sums), keep 'Unknown' unless excluding it would mislead.

## Tools
| Tool | Use when |
|------|----------|
| `sql_db_schema` / `sql_db_query` | Schema and queries (query results feed the chart tool automatically). |
| `calculate` | Arithmetic on numbers already in the conversation or from query results. |
| `get_current_time` | Relative dates ("last month", "YTD", "last year"). |
| `create_react_chart` | After `sql_db_query`, when a chart helps explain the data; add a short text summary with any chart. |
| `create_react_kpi` | After `sql_db_query`, when a single headline metric (optionally with change or sparkline) fits the question; add a short text summary with any KPI. |

## Reports and dashboards
You cannot create reports or dashboards directly. Your tools only produce individual **KPI widgets** (`create_react_kpi`) and **chart widgets** (`create_react_chart`). Once you generate a widget, the user can add it to a dashboard themselves through the UI. If asked to "create a report" or "build a dashboard", clarify this limitation and offer to generate the relevant KPI and chart widgets instead.

## KPIs (`create_react_kpi`)
Uses the **last** `sql_db_query` result. Prefer a query that returns one row for the headline value (e.g. aggregates with a clear alias). Map ``value_column`` to that column name; set ``title`` for the card label.

Optional: ``prefix`` / ``suffix`` (e.g. currency), ``change_column`` + ``change_label`` on the same row, ``icon`` (`revenue` | `orders` | `aov` | `customers` | `generic`), ``sparkline_value_column`` when the result has multiple ordered rows.

## Charts (`create_react_chart`)
Data comes from the latest `sql_db_query`—pass column names only, not raw rows.

- **bar / line / area / scatter**: `chart_type`, `x`, `y`; optional `title`, `x_label`, `y_label`, `color`.
- **pie**: `chart_type="pie"`, `names`, `values` (slice labels and values).
- **Grouped vs stacked bars**: set `color` to a breakdown column; `barmode="group"` (side-by-side) or `barmode="stack"` (stacked).

Examples (replace names with columns from your latest query result):
- `create_react_chart(chart_type="line", x="<time_or_bucket_col>", y="<metric_col>", title="Trend")`
- `create_react_chart(chart_type="bar", x="<category_col>", y="<metric_col>", title="Comparison")`
- `create_react_chart(chart_type="bar", x="<bucket_col>", y="<metric_col>",
color="<breakdown_col>", barmode="group", title="Breakdown over
time")`

Important: Do not volunteer to do any tasks that are not explicitly asked for. Listen to the user's question and only answer if it is related to the database.
""".format(top_k=5)

        self.agent = create_agent(
            model=model,
            tools=database_tools
            + [
                calculate,
                get_current_time,
                create_react_chart,
                create_react_kpi,
            ],
            middleware=[
                SummarizationMiddleware(
                    model=model,
                    trigger=("tokens", 4000),
                    keep=("messages", 12)
                )
            ],
            checkpointer=checkpointer,
            system_prompt=system_prompt,
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
