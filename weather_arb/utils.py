"""
weather_arb/utils.py
Helper functions for timezone conversions, logging, etc.
"""
import logging
from datetime import datetime
import pytz

logger = logging.getLogger("arb_bot.weather")

def get_et_now() -> datetime:
    """Returns current datetime in Eastern Time."""
    utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)
    return utc_now.astimezone(pytz.timezone("US/Eastern"))

def get_utc_now() -> datetime:
    """Returns current datetime in UTC."""
    return datetime.utcnow().replace(tzinfo=pytz.utc)

def is_same_day(dt1: datetime, dt2: datetime) -> bool:
    """Checks if two datetimes are the same calendar day in their respective timezones."""
    return dt1.date() == dt2.date()

def format_temp(temp: float) -> str:
    """Formats temperature nicely."""
    return f"{temp:.1f}°F"
