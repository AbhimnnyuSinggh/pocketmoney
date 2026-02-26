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
    "Miami": (25.7617, -80.1918),
    "LA": (34.0522, -118.2437),
    "Houston": (29.7604, -95.3698),
}

async def fetch_open_meteo_forecast(city: str) -> dict[str, float | None]:
    """
    Fetch forecast from multiple models individually.
    Returns: {"gfs_seamless": 34.2, "ecmwf_ifs04": 35.1, "icon_seamless": 33.8}
    """
    if city not in CITY_COORDS:
        logger.error(f"Unknown city for coords: {city}")
        return {}

    lat, lon = CITY_COORDS[city]
    models = ["gfs_seamless", "ecmwf_ifs04", "icon_seamless"]
    results = {}

    async with httpx.AsyncClient(http2=True) as client:
        for model in models:
            try:
                params = {
                    "latitude": lat,
                    "longitude": lon,
                    "daily": "temperature_2m_max",
                    "models": model,
                    "temperature_unit": "fahrenheit",
                    "timezone": "auto",
                    "forecast_days": 3,
                }
                resp = await client.get(
                    f"{OPENMETEO_BASE}/forecast",
                    params=params, timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json()

                # Try model-specific key first, then generic key
                daily = data.get("daily", {})
                temps = (
                    daily.get(f"temperature_2m_max_{model}") or
                    daily.get("temperature_2m_max") or
                    []
                )
                if temps and temps[0] is not None:
                    results[model] = temps[0]
                    logger.debug(f"Open-Meteo {model} for {city}: {temps[0]}°F")

            except Exception as e:
                logger.warning(f"Open-Meteo {model} failed for {city}: {e}")
                continue

    if results:
        logger.info(f"Weather forecasts for {city}: {results}")
    else:
        logger.warning(f"All Open-Meteo models failed for {city}")

    return results

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
