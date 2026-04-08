import datetime

import pytz


def get_current_time(timezone: str) -> str:
    """Get the current time in a specified timezone.

    Args:
        timezone (str): The timezone to get the current time for. Use tz database name (e.g., 'Pacific/Auckland').
    Returns:
        str: The current time in the specified timezone as a formatted string.
    """
    try:
        tz = pytz.timezone(timezone)
    except pytz.UnknownTimeZoneError:
        return f"Error: Unknown timezone '{timezone}'. Please provide a valid tz database name."

    # Format: "1:25 PM Tuesday, 23 December 2025"
    now = datetime.datetime.now(tz).strftime("%#I:%M %p %A %d %B %Y")
    return now
