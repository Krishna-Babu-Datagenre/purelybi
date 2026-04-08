from __future__ import annotations

import re

import duckdb
from langchain_core.language_models import BaseLanguageModel
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import tool

from streamchat.tools.charts import get_session_context, store_query_snapshot

QUERY_CHECKER_PROMPT = PromptTemplate(
    input_variables=["dialect", "query"],
    template=(
        "{query}\n"
        "Double check the {dialect} query above for common mistakes, including:\n"
        "- Using NOT IN with NULL values\n"
        "- Using UNION when UNION ALL should have been used\n"
        "- Using BETWEEN for exclusive ranges\n"
        "- Data type mismatch in predicates\n"
        "- Properly quoting identifiers\n"
        "- Using the correct number of arguments for functions\n"
        "- Casting to the correct data type\n"
        "- Using the proper columns for joins\n\n"
        "If there are any of the above mistakes, rewrite the query. "
        "If there are no mistakes, just reproduce the original query.\n\n"
        "Output the final SQL query only.\n\nSQL Query: "
    ),
)

_DISALLOWED_SQL_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|ATTACH|DETACH|COPY|EXPORT|IMPORT|INSTALL|LOAD|PRAGMA|CALL)\b",
    re.IGNORECASE,
)


def _is_readonly_sql(query: str) -> bool:
    q = query.strip().lstrip("(").upper()
    if not (q.startswith("SELECT") or q.startswith("WITH")):
        return False
    return _DISALLOWED_SQL_RE.search(query) is None


def build_duckdb_tools(
    conn: duckdb.DuckDBPyConnection, llm: BaseLanguageModel
) -> list:
    @tool("sql_db_list_tables")
    def list_tables(tool_input: str = "") -> str:
        """Input is an empty string, output is a comma-separated list of tables in the database."""
        try:
            rows = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
            return ", ".join(row[0] for row in rows)
        except Exception as e:
            return f"Error listing tables: {e}"

    @tool("sql_db_schema")
    def get_schema(table_names: str) -> str:
        """Input to this tool is a comma-separated list of tables, output is the schema and sample rows for those tables."""
        tables = [t.strip() for t in table_names.split(",") if t.strip()]
        parts: list[str] = []
        for table in tables:
            try:
                cols = conn.execute(f"DESCRIBE {table}").fetchall()
                header = f"CREATE TABLE {table} (\n"
                col_lines = [f"  {c[0]} {c[1]}" for c in cols]
                header += ",\n".join(col_lines) + "\n)"
                sample = conn.execute(f"SELECT * FROM {table} LIMIT 3").fetchdf()
                parts.append(
                    f"{header}\n\n/*\n3 rows from {table} table:\n{sample.to_string(index=False)}\n*/"
                )
            except Exception as e:
                parts.append(f"Error getting schema for {table}: {e}")
        return "\n\n".join(parts)

    @tool("sql_db_query_checker")
    def sql_checker(query: str) -> str:
        """Use this tool to double check if your query is correct before executing it."""
        prompt_value = QUERY_CHECKER_PROMPT.format(dialect="DuckDB", query=query)
        response = llm.invoke(prompt_value)
        return response.content if hasattr(response, "content") else str(response)

    @tool("sql_db_query")
    def execute_sql(query: str) -> str:
        """Execute a SQL query against the database and get back the result."""
        if not _is_readonly_sql(query):
            return "Error: Only read-only SELECT/WITH queries are allowed."
        try:
            df = conn.execute(query).fetchdf()
            session_id = get_session_context()
            if session_id and not df.empty:
                store_query_snapshot(session_id, query, df)
            return str(df.to_dict(orient="records"))
        except Exception as e:
            return f"Error: {e}"

    return [execute_sql, get_schema, list_tables, sql_checker]
