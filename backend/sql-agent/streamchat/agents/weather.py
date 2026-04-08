import os

from langchain.agents import create_agent
from langchain_openai import ChatOpenAI

from streamchat.tools import get_current_time, get_weather

SYSTEM_PROMPT = (
    "You are a helpful weather assistant. "
    "Your task is to provide accurate and concise weather information based on user queries. "
    "Use the `get_current_time` tool to determine the current date and time before providing weather information. "
    "You can search for hourly and daily weather forecasts. "
    "Your answer should include structured information such as temperature, humidity, wind speed, and weather conditions, as well as a textual summary."
)


class WeatherAgent:
    def __init__(self, llm="gpt-4.1-mini", checkpointer=None):
        llm = ChatOpenAI(
            model=llm,
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            base_url=os.getenv("AZURE_OPENAI_ENDPOINT"),
        )

        self.agent = create_agent(
            model=llm,
            tools=[get_current_time, get_weather],
            checkpointer=checkpointer,
            system_prompt=SYSTEM_PROMPT,
        )

        self.starter_prompts = {
            ":material/weather_mix: What is the weather like tomorrow in Auckland?": (
                "What is the weather like tomorrow in Auckland?"
            ),
            ":material/weather_mix: What's the weather this weekend in Wellington?": (
                "What's the weather this weekend in Wellington?"
            ),
        }

    def get_agent(self):
        return self.agent

    def get_starter_prompts(self):
        return self.starter_prompts
