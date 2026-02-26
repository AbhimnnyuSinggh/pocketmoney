"""
weather_arb/scanner.py
Filters the global Polymarket market list for daily temperature and climate events.
Groups individual sub-markets (YES/NO per bin) into unified event objects.
"""
import re
import logging
from datetime import datetime, timezone

logger = logging.getLogger("arb_bot.weather.scanner")


def extract_bin_from_title(title: str) -> str | None:
    """Extract temperature bin from sub-market title.
    'Will the highest temp be 32-33°F?' → '32-33'
    'Will the highest temp be 50°F or higher?' → '50+'
    'Will the highest temp be 20°F or lower?' → '20-'
    """
    title_clean = title.replace("–", "-")  # normalize en-dash

    range_match = re.search(r'(\d+)\s*-\s*(\d+)\s*°?F', title_clean, re.IGNORECASE)
    if range_match:
        return f"{range_match.group(1)}-{range_match.group(2)}"

    high_match = re.search(r'(\d+)\s*°?F\s+or\s+higher', title_clean, re.IGNORECASE)
    if high_match:
        return f"{high_match.group(1)}+"

    low_match = re.search(r'(\d+)\s*°?F\s+or\s+lower', title_clean, re.IGNORECASE)
    if low_match:
        return f"{low_match.group(1)}-"

    return None


def get_active_weather_markets(all_poly_markets: list) -> list:
    """
    Finds Daily High Temperature markets and long-term Climate markets.
    """
    weather_markets = []
    now = datetime.now(timezone.utc)

    for m in all_poly_markets:
        if m.get("closed") or not m.get("active"):
            continue

        title = m.get("title", "").lower()
        is_daily_temp = any(kw in title for kw in [
            "highest temperature",
            "high temperature",
            "daily high",
            "temperature high",
        ])
        is_climate = "hottest year" in title or "temperature anomaly" in title

        if is_daily_temp or is_climate:
            # Check if recently launched (< 12 hours old = new launch edge)
            created_at_str = m.get("created_at") or m.get("createdAt")
            is_new = False
            if created_at_str:
                try:
                    created_dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    age_hours = (now - created_dt).total_seconds() / 3600
                    is_new = age_hours < 12
                except Exception:
                    pass

            # Identify City Target
            city_target = "Unknown"
            if any(x in title for x in ["new york", "nyc", "laguardia", "manhattan"]):
                city_target = "NYC"
            elif "chicago" in title:
                city_target = "Chicago"
            elif "atlanta" in title:
                city_target = "Atlanta"
            elif "london" in title:
                city_target = "London"
            elif "miami" in title:
                city_target = "Miami"
            elif "los angeles" in title or "la " in title:
                city_target = "LA"
            elif "houston" in title:
                city_target = "Houston"

            # Extract YES price for grouping
            prices = m.get("outcome_prices", [])
            yes_price = 0
            if prices:
                try:
                    yes_price = float(prices[0]) if isinstance(prices[0], (int, float, str)) else 0
                except (ValueError, TypeError):
                    pass

            # Extract CLOB token IDs
            clob_ids = m.get("clob_token_ids", [])
            if not clob_ids:
                raw = m.get("clobTokenIds", "[]")
                if isinstance(raw, str):
                    try:
                        import json
                        clob_ids = json.loads(raw)
                    except Exception:
                        clob_ids = []
                elif isinstance(raw, list):
                    clob_ids = raw

            m["weather_type"] = "climate" if is_climate else "daily_high"
            m["city"] = city_target
            m["is_new_launch"] = is_new
            m["yes_price"] = yes_price
            m["clob_token_ids"] = clob_ids
            weather_markets.append(m)

    return weather_markets


def group_weather_markets_by_event(weather_markets: list) -> dict:
    """Group individual YES/NO sub-markets into full events.

    Returns: {
        "highest-temp-nyc-feb-27": {
            "city": "NYC",
            "event_slug": "highest-temp-nyc-feb-27",
            "end_date": "2026-02-28",
            "is_new_launch": False,
            "bins": {
                "32-33": {"yes_price": 0.35, "token_id": "abc123", "slug": "...", "market_id": "..."},
                "34-35": {"yes_price": 0.40, "token_id": "def456", "slug": "...", "market_id": "..."},
                ...
            }
        }
    }
    """
    grouped = {}

    for m in weather_markets:
        if m.get("weather_type") != "daily_high":
            continue

        event_key = m.get("event_slug", "") or m.get("group_item_title", "") or ""
        if not event_key:
            # Fallback: generate key from city + end_date
            city = m.get("city", "Unknown")
            end_date = m.get("end_date", "") or m.get("end_date_iso", "")
            if city != "Unknown" and end_date:
                event_key = f"weather-{city}-{end_date}"
            else:
                continue

        title = m.get("title", "")
        bin_label = extract_bin_from_title(title)
        if not bin_label:
            continue

        if event_key not in grouped:
            grouped[event_key] = {
                "city": m.get("city", "Unknown"),
                "event_slug": event_key,
                "end_date": m.get("end_date", ""),
                "is_new_launch": m.get("is_new_launch", False),
                "bins": {},
            }

        clob_ids = m.get("clob_token_ids", [])
        grouped[event_key]["bins"][bin_label] = {
            "yes_price": m.get("yes_price", 0),
            "token_id": clob_ids[0] if clob_ids else "",
            "slug": m.get("slug", ""),
            "market_id": m.get("market_id", m.get("condition_id", "")),
        }

    # Log summary
    for key, event in grouped.items():
        logger.info(f"Weather event: {key} ({event['city']}) — {len(event['bins'])} bins")

    return grouped
