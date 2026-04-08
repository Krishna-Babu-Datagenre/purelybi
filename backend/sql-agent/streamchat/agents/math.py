import os

from langchain.agents import create_agent
from langchain_openai import ChatOpenAI

from streamchat.tools import calculate

SYSTEM_PROMPT = (
    "You are a helpful mathematician. "
    "Your task is to provide accurate solutions to mathematical problems. "
    "You have a calculator that you can use. "
)


class MathAgent:
    def __init__(self, llm="gpt-4.1-mini", checkpointer=None):
        llm = ChatOpenAI(
            model=llm,
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            base_url=os.getenv("AZURE_OPENAI_ENDPOINT"),
        )

        self.agent = create_agent(
            model=llm,
            tools=[calculate],
            checkpointer=checkpointer,
            system_prompt=SYSTEM_PROMPT,
        )

        self.starter_prompts = {
            ":green[:material/calculate:] What is the cube root of 500 566 184?": (
                "What is the cube root of 500 566 184?"
            )
        }

    def get_agent(self):
        return self.agent

    def get_starter_prompts(self):
        return self.starter_prompts
