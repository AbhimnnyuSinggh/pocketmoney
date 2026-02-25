"""
weather_arb/data_fetcher.py
Fetches multi-model weather forecasts and real-time NWS observations.
"""
import httpx
import logging
from weather_arb.config import OPENMETEO_BASE, NWS_BASE, NWS_USER_AGENT

logger = logging.getLogger("arb_bot.weather.fetcher")

# Approximate coordinates for the major target cities
CITY_COORDS = {
    "NYC": (40.7128, -74.0060),
    "Chicago": (41.8781, -87.6298),
    "Atlanta": (33.7490, -84.3880),
    "London": (51.5074, -0.1278),
    "Miami": (25.7617, -80.1918)
}

async def fetch_open_meteo_forecast(city: str) -> dict:
    if city not in CITY_COORDS:
        logger.error(f"Unknown city for coords: {city}")
        return {}
        
    lat, lon = CITY_COORDS[city]
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max",
        "models": "gfs_seamless,ecmwf_ifs04,icon_seamless",
        "temperature_unit": "fahrenheit",
        "timezone": "auto"
    }
    
    try:
        async with httpx.AsyncClient(http2=True) as client:
            resp = await client.get(f"{OPENMETEO_BASE}/forecast", params=params, timeout=10.0)
            resp.raise_for_status()
            data = resp.json()
            return data
    except Exception as e:
        logger.error(f"Open-Meteo fetch failed for {city}: {e}")
        return {}

async def fetch_nws_observation(station: str) -> float | None:
    """Fetch current observation (in F) for a specific NWS station (e.g., KLGA)."""
    headers = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}
    
    try:
        async with httpx.AsyncClient(http2=True) as client:
            resp = await client.get(f"{NWS_BASE}/stations/{station}/observations/latest", headers=headers, timeout=10.0)
            resp.raise_for_status()
            data = resp.json()
            
            temp_c = data.get("properties", {}).get("temperature", {}).get("value")
            if temp_c is not None:
                # Convert C to F
                temp_f = (temp_c * 9/5) + 32
                return round(temp_f, 1)
            return None
    except Exception as e:
        logger.error(f"NWS fetch failed for station {station}: {e}")
        return None
