"""
elite_edges/weather_forecast.py — Weather Forecast Signal Scanner

Scans Polymarket temperature markets, fetches forecasts from 5+ weather APIs,
computes source confluence, and generates signals for "Climate & Science" users.

This is a SIGNAL module — it creates Opportunity objects that flow through
distribute_signals(). It does NOT auto-trade.
"""
import re
import os
import logging
import asyncio
from datetime import datetime
from html import escape as html_escape

import httpx

logger = logging.getLogger("arb_bot.elite.weather_forecast")

# ---------------------------------------------------------------------------
# City data (shared with weather_arb but duplicated to keep this self-contained)
# ---------------------------------------------------------------------------
CITY_COORDS = {
    "NYC":     (40.7128, -74.0060),
    "Chicago": (41.8781, -87.6298),
    "Atlanta": (33.7490, -84.3880),
    "London":  (51.5074, -0.1278),
    "Miami":   (25.7617, -80.1918),
    "LA":      (34.0522, -118.2437),
    "Houston": (29.7604, -95.3698),
}

CITY_STATIONS = {
    "NYC": "KLGA", "Chicago": "KORD", "Atlanta": "KATL",
    "London": "EGLC", "Miami": "KMIA", "LA": "KLAX", "Houston": "KIAH",
}


# ===================================================================
# SECTION 1: Multi-Source Fetching
# ===================================================================
async def _fetch_open_meteo(client: httpx.AsyncClient, lat: float, lon: float,
                            model: str) -> dict | None:
    """Fetch one model from Open-Meteo. Returns {"source": "GFS", ...}."""
    source_name = {
        "gfs_seamless": "GFS", "ecmwf_ifs04": "ECMWF", "icon_seamless": "ICON"
    }.get(model, model.upper())
    try:
        url = f"https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat, "longitude": lon,
            "daily": "temperature_2m_max",
            "temperature_unit": "fahrenheit",
            "forecast_days": 1,
            "models": model,
        }
        r = await client.get(url, params=params, timeout=8)
        data = r.json()
        daily = data.get("daily", {})
        temps = daily.get("temperature_2m_max", [])
        if not temps:
            # Some models nest under model-specific keys
            for key in daily:
                if "temperature_2m_max" in key and daily[key]:
                    temps = daily[key]
                    break
        if temps and temps[0] is not None:
            return {"source": source_name, "high_f": float(temps[0])}
    except Exception as e:
        logger.debug(f"Open-Meteo {model} failed: {e}")
    return None


async def _fetch_nws(client: httpx.AsyncClient,
                     lat: float, lon: float) -> dict | None:
    """Fetch NWS gridpoint forecast. No API key needed."""
    try:
        headers = {"User-Agent": "WeatherBot/1.0 (pocketmoney@example.com)"}
        # Step 1: get gridpoint
        points_r = await client.get(
            f"https://api.weather.gov/points/{lat},{lon}",
            headers=headers, timeout=8
        )
        forecast_url = points_r.json()["properties"]["forecast"]
        # Step 2: get forecast
        fc_r = await client.get(forecast_url, headers=headers, timeout=8)
        periods = fc_r.json()["properties"]["periods"]
        # Find daytime period
        for p in periods:
            if p.get("isDaytime", False):
                return {"source": "NWS", "high_f": float(p["temperature"])}
    except Exception as e:
        logger.debug(f"NWS fetch failed: {e}")
    return None


async def _fetch_openweathermap(client: httpx.AsyncClient,
                                lat: float, lon: float,
                                api_key: str) -> dict | None:
    """Fetch from OpenWeatherMap. Needs free API key."""
    if not api_key:
        return None
    try:
        r = await client.get(
            "https://api.openweathermap.org/data/2.5/forecast",
            params={"lat": lat, "lon": lon, "appid": api_key, "units": "imperial"},
            timeout=8
        )
        data = r.json()
        # Find today's max from 3-hour forecast list
        today = datetime.utcnow().strftime("%Y-%m-%d")
        max_temp = None
        for item in data.get("list", []):
            if today in item.get("dt_txt", ""):
                t = item.get("main", {}).get("temp_max", 0)
                if max_temp is None or t > max_temp:
                    max_temp = t
        if max_temp is not None:
            return {"source": "OpenWeatherMap", "high_f": float(max_temp)}
    except Exception as e:
        logger.debug(f"OWM fetch failed: {e}")
    return None


async def _fetch_weatherapi(client: httpx.AsyncClient,
                            lat: float, lon: float,
                            api_key: str) -> dict | None:
    """Fetch from WeatherAPI.com. Needs free API key."""
    if not api_key:
        return None
    try:
        r = await client.get(
            "http://api.weatherapi.com/v1/forecast.json",
            params={"key": api_key, "q": f"{lat},{lon}", "days": 1},
            timeout=8
        )
        data = r.json()
        max_f = data["forecast"]["forecastday"][0]["day"]["maxtemp_f"]
        return {"source": "WeatherAPI", "high_f": float(max_f)}
    except Exception as e:
        logger.debug(f"WeatherAPI fetch failed: {e}")
    return None


async def _fetch_visualcrossing(client: httpx.AsyncClient,
                                lat: float, lon: float,
                                api_key: str) -> dict | None:
    """Fetch from Visual Crossing. Needs free API key."""
    if not api_key:
        return None
    try:
        r = await client.get(
            f"https://weather.visualcrossing.com/VisualCrossingWebServices"
            f"/rest/services/timeline/{lat},{lon}/today",
            params={"key": api_key, "unitGroup": "us", "include": "days"},
            timeout=8
        )
        data = r.json()
        tempmax = data["days"][0]["tempmax"]
        return {"source": "VisualCrossing", "high_f": float(tempmax)}
    except Exception as e:
        logger.debug(f"VisualCrossing fetch failed: {e}")
    return None


async def _fetch_nws_observation(client: httpx.AsyncClient,
                                 station: str) -> dict | None:
    """Fetch real-time observation from NWS station. For afternoon edge."""
    if not station:
        return None
    try:
        headers = {"User-Agent": "WeatherBot/1.0 (pocketmoney@example.com)"}
        r = await client.get(
            f"https://api.weather.gov/stations/{station}/observations/latest",
            headers=headers, timeout=8
        )
        props = r.json()["properties"]
        temp_c = props.get("temperature", {}).get("value")
        if temp_c is None:
            return None
        temp_f = temp_c * 9 / 5 + 32
        ts = props.get("timestamp", "")
        time_str = ts[11:16] if len(ts) > 16 else "now"
        return {"temp_f": round(temp_f, 1), "time": time_str}
    except Exception as e:
        logger.debug(f"NWS obs {station} failed: {e}")
    return None


async def fetch_all_forecasts(city: str, lat: float, lon: float,
                              cfg: dict) -> list[dict]:
    """Fetch forecasts from all available sources in parallel."""
    wf_cfg = cfg.get("weather_forecast", {})
    owm_key = wf_cfg.get("openweathermap_key", "") or os.environ.get("OWM_API_KEY", "")
    wapi_key = wf_cfg.get("weatherapi_key", "") or os.environ.get("WEATHERAPI_KEY", "")
    vc_key = wf_cfg.get("visualcrossing_key", "") or os.environ.get("VISUALCROSSING_KEY", "")

    results = []
    async with httpx.AsyncClient(http2=True) as client:
        tasks = [
            _fetch_open_meteo(client, lat, lon, "gfs_seamless"),
            _fetch_open_meteo(client, lat, lon, "ecmwf_ifs04"),
            _fetch_open_meteo(client, lat, lon, "icon_seamless"),
            _fetch_nws(client, lat, lon),
            _fetch_openweathermap(client, lat, lon, owm_key),
            _fetch_weatherapi(client, lat, lon, wapi_key),
            _fetch_visualcrossing(client, lat, lon, vc_key),
        ]
        fetched = await asyncio.gather(*tasks, return_exceptions=True)
        for item in fetched:
            if isinstance(item, dict) and item is not None:
                results.append(item)

    logger.info(f"Weather forecast {city}: {len(results)} sources fetched "
                f"({', '.join(r['source'] for r in results)})")
    return results


# ===================================================================
# SECTION 2: Bin Assignment
# ===================================================================
def assign_to_bin(temp_f: float, available_bins: list[str]) -> str | None:
    """Assign a temperature to the correct 2°F bin.
    33.5°F with bins ["30-31","32-33","34-35"] → "32-33"
    """
    rounded = round(temp_f)

    for bin_label in available_bins:
        # Standard range: "32-33"
        m = re.match(r'^(\d+)-(\d+)$', bin_label)
        if m:
            low, high = int(m.group(1)), int(m.group(2))
            if low <= rounded <= high:
                return bin_label

    # Edge bins: "50+" or "20-"
    for bin_label in available_bins:
        if bin_label.endswith("+"):
            nums = re.findall(r'\d+', bin_label)
            if nums and rounded >= int(nums[0]):
                return bin_label
        if bin_label.endswith("-"):
            nums = re.findall(r'\d+', bin_label)
            if nums and rounded <= int(nums[0]):
                return bin_label

    return None


# ===================================================================
# SECTION 3: Confluence Scoring
# ===================================================================
def compute_confluence(forecasts: list[dict],
                       available_bins: list[str]) -> dict:
    """Count how many sources agree on each bin."""
    for f in forecasts:
        f["bin"] = assign_to_bin(f["high_f"], available_bins)

    bin_votes: dict[str, dict] = {}
    for f in forecasts:
        b = f.get("bin")
        if not b:
            continue
        if b not in bin_votes:
            bin_votes[b] = {"count": 0, "sources": [], "temps": []}
        bin_votes[b]["count"] += 1
        bin_votes[b]["sources"].append(f["source"])
        bin_votes[b]["temps"].append(f["high_f"])

    total = sum(v["count"] for v in bin_votes.values())
    result = {}
    for b, data in bin_votes.items():
        result[b] = {
            "count": data["count"],
            "pct": (data["count"] / total * 100) if total > 0 else 0,
            "sources": data["sources"],
            "avg_temp": sum(data["temps"]) / len(data["temps"]),
        }

    return result


# ===================================================================
# SECTION 4: Signal Generation
# ===================================================================
def build_weather_signal(event_data: dict, confluence: dict,
                         bin_prices: dict, cfg: dict,
                         current_obs: dict | None = None):
    """Create an Opportunity if confluence is strong and market is mispriced."""
    from cross_platform_scanner import Opportunity

    wf_cfg = cfg.get("weather_forecast", {})
    min_conf = wf_cfg.get("min_confluence_pct", 60)
    min_edge = wf_cfg.get("min_edge_pct", 15) / 100

    if not confluence:
        return None

    # Best bin = most voted
    best_bin = max(confluence, key=lambda b: confluence[b]["count"])
    best_data = confluence[best_bin]

    if best_data["pct"] < min_conf:
        return None

    market_price = bin_prices.get(best_bin, {}).get("yes_price", 0)
    if market_price <= 0.01 or market_price >= 0.95:
        return None

    model_prob = best_data["pct"] / 100
    edge = model_prob - market_price
    if edge < min_edge:
        return None

    roi = ((1.0 / market_price) - 1) * 100
    profit_per_100 = (1.0 / market_price - 1) * 100

    # Adjacent bin
    sorted_bins = sorted(confluence.keys(),
                         key=lambda b: confluence[b]["count"], reverse=True)
    adjacent_bin = sorted_bins[1] if len(sorted_bins) > 1 else None

    # Risk level
    if best_data["pct"] >= 80:
        risk = "low"
    elif best_data["pct"] >= 60:
        risk = "medium"
    else:
        risk = "high"

    city = event_data["city"]
    station = event_data.get("station", CITY_STATIONS.get(city, ""))
    slug = bin_prices.get(best_bin, {}).get("slug", "")
    url = f"https://polymarket.com/event/{event_data.get('event_slug', '')}" if event_data.get("event_slug") else ""

    # Check if observation shows bin already reached
    obs_already_reached = False
    if current_obs:
        obs_bin = assign_to_bin(current_obs["temp_f"],
                                list(bin_prices.keys()))
        if obs_bin == best_bin:
            obs_already_reached = True
        current_obs["already_reached"] = obs_already_reached

    opp = Opportunity(
        opp_type="weather_forecast",
        title=f"{city} Daily High Temperature",
        description=(
            f"{best_data['count']}/{len(event_data['forecasts'])} weather sources agree: "
            f"{best_bin}°F range most likely.\n"
            f"Market prices this at {market_price:.0%} but sources suggest {model_prob:.0%}."
        ),
        profit_pct=round(roi, 2),
        profit_amount=round(profit_per_100, 2),
        total_cost=round(market_price, 4),
        platforms=["polymarket"],
        legs=[{"platform": "polymarket", "side": "YES", "price": market_price}],
        urls=[url],
        risk_level=risk,
        hold_time="Today",
        category="climate",
        edge_score=min(100, edge * 200),
        market_slug=slug,
    )

    # Attach rich weather data for custom formatter
    opp._weather_data = {
        "city": city,
        "date": event_data.get("date_label", "Today"),
        "station": station,
        "forecasts": event_data["forecasts"],
        "best_bin": best_bin,
        "agree_count": best_data["count"],
        "agree_pct": best_data["pct"],
        "total_sources": len(event_data["forecasts"]),
        "market_price": market_price,
        "edge": edge,
        "adjacent_bin": adjacent_bin,
        "adjacent_count": confluence[adjacent_bin]["count"] if adjacent_bin else 0,
        "adjacent_price": (bin_prices.get(adjacent_bin, {}).get("yes_price", 0)
                           if adjacent_bin else 0),
        "current_obs": current_obs,
    }

    logger.info(
        f"[WEATHER SIGNAL] {city} {best_bin}°F: "
        f"{best_data['count']}/{len(event_data['forecasts'])} agree, "
        f"mkt={market_price:.0%}, edge={edge:.0%}"
    )
    return opp


# ===================================================================
# SECTION 5: Main Scanner Function
# ===================================================================
async def scan_weather_forecasts(poly_markets: list,
                                 cfg: dict) -> list:
    """
    Main entry point. Called from run_cycle() in main_v2.py.
    Returns list of Opportunity objects for the signal pipeline.
    """
    if not cfg.get("weather_forecast", {}).get("enabled", True):
        return []

    from weather_arb.scanner import (get_active_weather_markets,
                                     group_weather_markets_by_event)

    weather_markets = get_active_weather_markets(poly_markets)
    if not weather_markets:
        return []

    events = group_weather_markets_by_event(weather_markets)
    if not events:
        return []

    opportunities = []

    for event_key, event in events.items():
        city = event["city"]
        if city == "Unknown" or city not in CITY_COORDS:
            continue

        bins = event["bins"]
        if len(bins) < 3:
            continue

        lat, lon = CITY_COORDS[city]

        # Fetch all weather sources
        forecasts = await fetch_all_forecasts(city, lat, lon, cfg)
        if len(forecasts) < 3:
            logger.info(f"Weather {city}: only {len(forecasts)} sources, need 3+")
            continue

        # Compute confluence
        available_bins = list(bins.keys())
        confluence = compute_confluence(forecasts, available_bins)
        if not confluence:
            continue

        # Afternoon observation (US cities, 12-6PM ET)
        current_obs = None
        wf_cfg = cfg.get("weather_forecast", {})
        if wf_cfg.get("include_afternoon_obs", True) and city in CITY_STATIONS:
            try:
                import pytz
                et = pytz.timezone("US/Eastern")
                now_et = datetime.now(et)
                if 12 <= now_et.hour <= 18:
                    async with httpx.AsyncClient() as client:
                        current_obs = await _fetch_nws_observation(
                            client, CITY_STATIONS[city])
            except ImportError:
                pass
            except Exception as e:
                logger.debug(f"Afternoon obs {city}: {e}")

        # Build signal
        event["forecasts"] = forecasts
        event["station"] = CITY_STATIONS.get(city, "")
        event["date_label"] = event.get("end_date", "Today")[:10] or "Today"

        opp = build_weather_signal(event, confluence, bins, cfg, current_obs)
        if opp:
            opportunities.append(opp)

    logger.info(f"[WEATHER FORECAST] Scanned {len(events)} events, "
                f"{len(opportunities)} signals generated")
    return opportunities
