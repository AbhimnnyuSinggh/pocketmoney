"""
weather_arb/config.py
Configuration structures and constants for the Weather Arbitrage module.
"""
from enum import Enum
import os

class TradingMode(Enum):
    SAFE = "SAFE"
    NEUTRAL = "NEUTRAL"
    AGGRESSIVE = "AGGRESSIVE"

# Minimum edge (model_prob - market_price) required to enter a trade
MODE_THRESHOLDS = {
    "SAFE": 0.25,
    "NEUTRAL": 0.15,
    "AGGRESSIVE": 0.10,
}

# Fractional Kelly multipliers to scale risk per mode
MODE_KELLY_MULTIPLIER = {
    "SAFE": 0.25,
    "NEUTRAL": 0.50,
    "AGGRESSIVE": 0.75,
}

# API Endpoints
OPENMETEO_BASE = os.environ.get("OPENMETEO_BASE", "https://api.open-meteo.com/v1")
NWS_BASE = os.environ.get("NWS_BASE", "https://api.weather.gov")
NWS_USER_AGENT = os.environ.get("NWS_USER_AGENT", "WeatherBot/1.0 (pocketmoney@example.com)")

# Known City to Station Mappings (add more as Polymarket expands)
CITY_STATIONS = {
    "NYC": "KLGA",         # LaGuardia
    "Chicago": "KORD",      # O'Hare
    "Atlanta": "KATL",      # Hartsfield
    "London": "EGLC",       # London City
    "Miami": "KMIA",        # Miami International
    "LA": "KLAX",           # Los Angeles Intl
    "Houston": "KIAH",      # Houston Intercontinental
}

# Database constants
WEATHER_DB_PATH = os.environ.get("WEATHER_DB", "weather.db")
