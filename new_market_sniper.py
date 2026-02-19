"""
new_market_sniper.py — Detects brand-new markets appearing on Polymarket.
New markets have the worst pricing because nobody has researched them yet.
The first few hours are when the biggest edges exist — before professional
bots and researchers calibrate the odds.
Persists a cache of known market IDs to JSON so new markets survive restarts.
First run seeds the cache without alerting (to avoid a flood).
"""
import os
import json
import logging
import requests
from datetime import datetime, timezone
from cross_platform_scanner import Opportunity
logger = logging.getLogger("arb_bot.new_market_sniper")
GAMMA_API_URL = "https://gamma-api.polymarket.com"
# =========================================================================
# Market ID Cache (persisted to disk)
# =========================================================================
def load_known_markets(cache_path: str) -> set[str]:
    """Load the set of known market IDs from disk."""
    if not os.path.exists(cache_path):
        return set()
    try:
        with open(cache_path, "r") as f:
            data = json.load(f)
            return set(data.get("market_ids", []))
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Could not load market cache: {e}")
        return set()
def save_known_markets(cache_path: str, market_ids: set[str]):
    """Save the set of known market IDs to disk."""
    try:
        with open(cache_path, "w") as f:
            json.dump({
                "market_ids": list(market_ids),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "count": len(market_ids),
            }, f, indent=2)
    except IOError as e:
        logger.error(f"Could not save market cache: {e}")
# =========================================================================
# Fetch Current Markets
# =========================================================================
def fetch_all_market_ids(cfg: dict) -> list[dict]:
    """
    Fetch all active markets from Polymarket Gamma API.
    Returns list of market dicts with id, title, prices, url, etc.
    """
    base_url = cfg.get("scanner", {}).get("gamma_api_url", GAMMA_API_URL)
    all_markets = []
    offset = 0
    limit = 100
    max_markets = cfg.get("scanner", {}).get("max_markets", 2000)
    while offset < max_markets:
        params = {
            "limit": limit,
            "offset": offset,
            "closed": "false",
            "active": "true",
            "order": "startDate",
            "ascending": "false",  # Most recent first
        }
        try:
            resp = requests.get(f"{base_url}/markets", params=params, timeout=15)
            resp.raise_for_status()
            markets = resp.json()
        except requests.RequestException as e:
            logger.error(f"Gamma API error during new-market scan: {e}")
            break
        if not markets:
            break
        for m in markets:
            try:
                prices_raw = m.get("outcomePrices", "[]")
                if isinstance(prices_raw, str):
                    prices = json.loads(prices_raw)
                else:
                    prices = prices_raw or []
                yes_price = float(prices[0]) if len(prices) > 0 else 0
                no_price = float(prices[1]) if len(prices) > 1 else 0
                # Build correct URL using event slug
                events_list = m.get("events", [])
                if events_list and isinstance(events_list, list):
                    event_slug = events_list[0].get("slug", m.get("slug", ""))
                else:
                    event_slug = m.get("slug", "")
                all_markets.append({
                    "id": str(m.get("id", "")),
                    "title": m.get("question", ""),
                    "slug": m.get("slug", ""),
                    "event_slug": event_slug,
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "volume": float(m.get("volume", 0) or 0),
                    "volume_24h": float(m.get("volume24hr", 0) or 0),
                    "liquidity": float(m.get("liquidity", 0) or 0),
                    "end_date": m.get("endDate", ""),
                    "category": m.get("category", ""),
                    "url": f"https://polymarket.com/event/{event_slug}",
                    "created_at": m.get("createdAt", ""),
                    "start_date": m.get("startDate", ""),
                })
            except (ValueError, IndexError, TypeError):
                continue
        offset += limit
        import time
        time.sleep(0.3)
    return all_markets
# =========================================================================
# Detect New Markets
# =========================================================================
def detect_new_markets(
    current_markets: list[dict],
    cfg: dict,
) -> list[Opportunity]:
    """
    Compare current markets against the known cache.
    New markets become Opportunity objects with opp_type="new_market".
    On first run (empty cache), seeds the cache and returns nothing
    to avoid a flood of false alerts.
    """
    new_markets_cfg = cfg.get("new_markets", {})
    cache_file = new_markets_cfg.get("cache_file", "known_markets.json")
    first_run_alert = False  # Whether to alert on first run
    known_ids = load_known_markets(cache_file)
    current_ids = {m["id"] for m in current_markets}
    # First run — seed the cache
    if not known_ids:
        logger.info(
            f"First run: seeding cache with {len(current_ids)} markets "
            f"(no alerts on first run)"
        )
        save_known_markets(cache_file, current_ids)
        return []
    # Find genuinely new markets
    new_ids = current_ids - known_ids
    opportunities = []
    if new_ids:
        logger.info(f"🆕 Detected {len(new_ids)} NEW markets!")
        # Build opportunities for each new market
        new_market_data = [m for m in current_markets if m["id"] in new_ids]
        for m in new_market_data:
            yes_p = m["yes_price"]
            no_p = m["no_price"]
            spread = abs(yes_p - (1.0 - no_p))
            # Estimate potential edge — new markets are often mispriced
            estimated_edge = max(spread * 100, 2.0)  # At least 2% estimated edge
            opp = Opportunity(
                opp_type="new_market",
                title=m["title"],
                description=(
                    f"🆕 Brand new market just launched!\n"
                    f"Current prices: YES={yes_p:.4f} NO={no_p:.4f}\n"
                    f"YES+NO spread: {(yes_p + no_p):.4f}\n"
                    f"Liquidity: ${m['liquidity']:,.0f}\n"
                    f"Volume: ${m['volume']:,.0f}\n"
                    f"Created: {m.get('created_at', 'Unknown')[:19]}\n"
                    f"\n⚡ New markets have the worst pricing — "
                    f"research the resolution criteria ASAP!"
                ),
                profit_pct=round(estimated_edge, 2),
                profit_amount=round(estimated_edge, 2),
                total_cost=round(min(yes_p, no_p), 4),
                platforms=["polymarket"],
                legs=[{
                    "platform": "Polymarket",
                    "side": "RESEARCH",
                    "price": yes_p,
                }],
                urls=[m["url"]],
                risk_level="medium",
                hold_time=m.get("end_date", "")[:10] if m.get("end_date") else "",
                category=m.get("category", ""),
            )
            opportunities.append(opp)
            logger.info(
                f"[NEW MARKET] {m['title'][:60]} | "
                f"YES={yes_p:.2f} NO={no_p:.2f} | "
                f"Liquidity: ${m['liquidity']:,.0f}"
            )
    # Update cache with all current IDs (also removes stale ones)
    save_known_markets(cache_file, current_ids)
    # Also log how many markets were removed (closed/resolved)
    removed = known_ids - current_ids
    if removed:
        logger.debug(f"{len(removed)} markets removed from cache (closed/resolved)")
    return opportunities
# =========================================================================
# Top-Level Entry Point
# =========================================================================
def find_new_market_opportunities(
    cfg: dict,
    existing_markets: list[dict] | None = None,
) -> list[Opportunity]:
    """
    Main entry point called from the scan cycle.
    Fetches all current markets (or uses existing), compares to cache, alerts on new ones.
    """
    new_markets_cfg = cfg.get("new_markets", {})
    if not new_markets_cfg.get("enabled", True):
        return []
    logger.info("Scanning for newly launched markets...")
    # Fetch current markets
    # Fetch current markets OR use existing
    if existing_markets:
        # Normalize existing markets to match sniper format
        current_markets = []
        for m in existing_markets:
            if m.get("platform") == "polymarket":
                # Create a copy to avoid mutating the original
                new_m = m.copy()
                # Ensure compatibility with sniper (it expects 'id' as string)
                if "market_id" in new_m:
                    new_m["id"] = str(new_m["market_id"])
                
                # Check required fields
                if "id" in new_m and "yes_price" in new_m:
                    current_markets.append(new_m)
        
        if not current_markets:
            logger.warning("Existing markets provided but none were valid Polymarket data—refetching?")
            # Fallback if normalization failed? Or simply assume we have none.
            # If cross_platform_scanner fetched markets, they should be valid.
            pass
    else:
        current_markets = fetch_all_market_ids(cfg)
    if not current_markets:
        logger.warning("No markets fetched — skipping new market detection")
        return []
    # Detect new ones
    opportunities = detect_new_markets(current_markets, cfg)
    if opportunities:
        logger.info(f"🆕 {len(opportunities)} new market alerts!")
    else:
        logger.info("No new markets since last scan")
    return opportunities
