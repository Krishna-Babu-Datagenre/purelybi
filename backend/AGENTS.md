# Project Context
I am building a web application that enables users to interact with their data using natural language powered by Generative AI. Users can ask questions about their data, create visuals, build reports (including mixed visuals, KPI blocks, and more), rearrange visuals within a report, and export their reports seamlessly.

# Role
Act as an python expert, developing backend for React and TypeScript Frontend Architecture.

# Tech Stack
- **AI Agent Framework**: Langchain (Python)
- **Web Framework**: FastAPI (Python)

# Project Folder Structure

/data             # Data storage and management
/sql-agent        # Main backend code for SQL agent
  /app            # Sample Streamlit app for testing and demonstration
  /notebooks      # Jupyter notebooks for experimentation and prototyping
  /streamchat     # Agent framework code and related utilities
/bi-templates     # Dashboard templates and related code
/fastapi-app      # FastAPI application code for the backend
  /routers        # API route definitions
  /services       # Business logic and service layer
  /models         # Data models and schemas
  /utils          # Utility functions and helpers