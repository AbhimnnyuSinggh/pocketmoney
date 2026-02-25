"""
weather_arb/scanner.py
Filters the global Polymarket market list for daily temperature and climate events.
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger("arb_bot.weather.scanner")

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
        is_daily_temp = "highest temperature" in title and "°f" in title.lower()
        is_climate = "hottest year" in title or "temperature anomaly" in title
        
        if is_daily_temp or is_climate:
            # Check if recently launched (< 12 hours old = new launch edge)
            created_at_str = m.get("created_at") or m.get("createdAt")
            is_new = False
            if created_at_str:
                try:
                    # Simple parse
                    created_dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    age_hours = (now - created_dt).total_seconds() / 3600
                    is_new = age_hours < 12
                except Exception:
                    pass
            
            # Identify City Target
            city_target = "Unknown"
            if "new york" in title or "nyc" in title or "laguardia" in title:
                city_target = "NYC"
            elif "chicago" in title:
                city_target = "Chicago"
            elif "atlanta" in title:
                city_target = "Atlanta"
            elif "london" in title:
                city_target = "London"
            elif "miami" in title:
                city_target = "Miami"
                
            m["weather_type"] = "climate" if is_climate else "daily_high"
            m["city"] = city_target
            m["is_new_launch"] = is_new
            weather_markets.append(m)
            
    return weather_markets
