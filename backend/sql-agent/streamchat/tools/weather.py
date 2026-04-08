import os
from typing import Literal
from urllib.parse import quote

import requests

BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
API_KEY = os.getenv("VISUAL_CROSSING_API_KEY")

ELEMENTS = (
    "remove:cloudcover,remove:datetimeEpoch,remove:dew,remove:feelslike,"
    "remove:feelslikemax,remove:feelslikemin,remove:icon,remove:moonphase,"
    "remove:name,remove:preciptype,remove:pressure,remove:snow,remove:snowdepth,"
    "remove:solarenergy,remove:solarradiation,remove:stations,remove:sunrise,"
    "remove:sunset,remove:visibility"
)


def get_weather(city: str, timesteps: Literal["hours", "days"], horizon: int) -> str:
    """Get weather for a given city.
    Args:
        city (str): The city to get the weather for. Can include country or state for disambiguation.
        timesteps (str): The timesteps for the weather forecast. Must be 'hours' or 'days'.
        horizon (int): The number of future days to get the forecast for.
    Returns:
        str: The weather forecast for the given city as a JSON string.
    """
    # construct URL
    city = quote(city)

    url = (
        f"{BASE_URL}/{city}/next7days/next{horizon}days"
        f"?unitGroup=metric"
        f"&elements={ELEMENTS}"
        f"&include={timesteps}"
        f"&key={API_KEY}"
        f"&contentType=json"
    )
    # make request
    session = requests.Session()
    response = session.get(url)
    response.raise_for_status()

    return response.json()
