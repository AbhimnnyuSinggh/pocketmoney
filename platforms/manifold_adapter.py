"""
manifold_adapter.py — Manifold Markets Platform Adapter

Fetches markets from Manifold Markets' free public API (no auth required)
and converts them to the standard Opportunity format for cross-platform
comparison with Polymarket and Kalshi.

API: https://docs.manifold.markets/api
Rate limit: 100 req/min (very generous)
"""
import time
import logging
import requests
from difflib import SequenceMatcher
from cross_platform_scanner import Opportunity

logger = logging.getLogger("arb_bot.manifold")

MANIFOLD_API = "https://api.manifold.markets/v0"


def fetch_manifold_markets(cfg: dict) -> list[dict]:
    """
    Fetch active binary markets from Manifold.

    Returns list of normalized market dicts.
    """
    mf_cfg = cfg.get("manifold", {})
    if not mf_cfg.get("enabled", True):
        return []

    limit = mf_cfg.get("limit", 200)

    try:
        resp = requests.get(
            f"{MANIFOLD_API}/markets",
            params={
                "limit": limit,
                "sort": "liquidity",
                "filter": "open",
            },
            timeout=15,
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        raw = resp.json()
    except requests.RequestException as e:
        logger.debug(f"Manifold API error: {e}")
        return []

    markets = []
    for m in raw:
        try:
            # Only binary markets (YES/NO)
            if m.get("outcomeType") != "BINARY":
                continue

            # Must have meaningful liquidity
            pool = m.get("pool", {})
            total_pool = sum(float(v) for v in pool.values()) if pool else 0
            if total_pool < 100:  # Skip tiny markets
                continue

            prob = float(m.get("probability", 0.5))

            markets.append({
                "id": m.get("id", ""),
                "title": m.get("question", ""),
                "description": m.get("textDescription", "")[:200],
                "yes_price": prob,
                "no_price": 1.0 - prob,
                "volume": float(m.get("volume", 0)),
                "liquidity": total_pool,
                "url": m.get("url", ""),
                "close_time": m.get("closeTime"),
                "category": _categorize_manifold(m.get("question", "")),
                "platform": "manifold",
            })
        except (ValueError, TypeError, KeyError):
            continue

    logger.info(f"Manifold: fetched {len(markets)} binary markets")
    return markets


def find_manifold_cross_platform_opps(
    poly_markets: list[dict], cfg: dict
) -> list[Opportunity]:
    """
    Compare Manifold markets against Polymarket for cross-platform arb.

    Args:
        poly_markets: List of Polymarket market dicts
        cfg: Bot configuration

    Returns:
        List of cross-platform arbitrage Opportunities
    """
    mf_cfg = cfg.get("manifold", {})
    if not mf_cfg.get("enabled", True):
        return []

    similarity_threshold = mf_cfg.get("similarity_threshold", 0.65)
    min_delta_pct = mf_cfg.get("min_delta_pct", 8.0)

    # Fetch Manifold markets
    manifold_markets = fetch_manifold_markets(cfg)
    if not manifold_markets:
        return []

    # Build lookup from Polymarket
    poly_parsed = []
    for pm in poly_markets:
        title = pm.get("question", pm.get("title", ""))
        tokens = pm.get("tokens", [])
        if not title or not tokens or len(tokens) < 2:
            continue
        yes_price = float(tokens[0].get("price", 0))
        if yes_price <= 0:
            continue
        poly_parsed.append((title.lower(), yes_price, pm))

    opportunities = []

    for mf in manifold_markets:
        mf_title = mf["title"].lower()
        mf_yes = mf["yes_price"]

        for poly_title, poly_yes, pm in poly_parsed:
            # Check title similarity
            ratio = SequenceMatcher(None, mf_title, poly_title).ratio()
            if ratio < similarity_threshold:
                continue

            # Check price delta
            delta = abs(mf_yes - poly_yes) * 100
            if delta < min_delta_pct:
                continue

            # Found a cross-platform discrepancy!
            if mf_yes > poly_yes:
                buy_platform = "Polymarket"
                buy_price = poly_yes
                sell_platform = "Manifold"
                sell_price = mf_yes
            else:
                buy_platform = "Manifold"
                buy_price = mf_yes
                sell_platform = "Polymarket"
                sell_price = poly_yes

            profit_pct = round((sell_price - buy_price) * 100, 2)
            poly_slug = pm.get("event_slug", pm.get("slug", ""))
            poly_url = f"https://polymarket.com/event/{poly_slug}" if poly_slug else ""

            opp = Opportunity(
                opp_type="cross_platform_arb",
                title=mf["title"][:120],
                description=(
                    f"🔄 CROSS-PLATFORM ARB (Manifold × Polymarket)\n"
                    f"🅰️ {buy_platform}: YES @ {buy_price:.0%}\n"
                    f"🅱️ {sell_platform}: YES @ {sell_price:.0%}\n"
                    f"📊 Delta: {delta:.1f}%\n"
                    f"💰 Buy on {buy_platform}, effectively sell on {sell_platform}\n"
                    f"⚡ Similar market detected across platforms."
                ),
                profit_pct=profit_pct,
                profit_amount=round(profit_pct * 0.5, 2),
                total_cost=round(buy_price, 4),
                platforms=["polymarket", "manifold"],
                legs=[
                    {"platform": buy_platform, "side": "YES", "price": buy_price},
                    {"platform": sell_platform, "side": "YES", "price": sell_price},
                ],
                urls=[poly_url, mf["url"]],
                risk_level="medium",
                hold_time="",
                category=mf.get("category", ""),
            )
            opportunities.append(opp)

    if opportunities:
        logger.info(f"🌐 Manifold Cross-Platform: {len(opportunities)} arbs found")

    return opportunities


def _categorize_manifold(title: str) -> str:
    """Simple keyword-based categorization for Manifold markets."""
    t = title.lower()
    if any(w in t for w in ["bitcoin", "crypto", "ethereum", "btc", "eth", "solana"]):
        return "crypto"
    if any(w in t for w in ["election", "president", "congress", "vote", "trump", "biden"]):
        return "politics"
    if any(w in t for w in ["nfl", "nba", "mlb", "sports", "soccer", "football"]):
        return "sports"
    if any(w in t for w in ["ai", "openai", "google", "apple", "tech", "startup"]):
        return "tech"
    if any(w in t for w in ["stock", "market", "fed", "inflation", "gdp"]):
        return "finance"
    return ""
