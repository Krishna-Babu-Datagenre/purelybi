# SQL Agent Chat
Natural language interface for SQL databases using LangChain and Streamlit.

## Getting started

### 1. Sync the virtual environment
```bash
uv sync
```

### 2. Create a `.env` file
Use the `.env.example` file as a template to create your own `.env` file. Make sure to set the `AZURE_OPENAI_API_KEY` variable with your Azure OpenAI API key.

### 3. Run the main app
The main entry point is `app/app.py`:
```bash
uv run streamlit run app/app.py
```

## To do
  - 
  - EDW connection via service account (not Windows Auth)
  - Snowflake connection via key pair authentication (not external browser)
  - Snowflake MCP