"""
elite_edges/reward_farming.py — Reward Market Picker

Finds the single best Polymarket market for LP reward farming.

Criteria:
  - Market resolves within 24 hours (maximize reward / time)
  - Volume 24h > $10K (enough fills)
  - Liquidity > $5K (book not too thin)
  - YES price between 0.20 and 0.80 (balanced = safer LP)
  - Ranked by volume/liquidity ratio (higher = more fills)

Also generates signal-type Opportunity objects for the "Reward Farming"
signal category in the regular bot flow.
"""
import time
import logging
import requests
import json
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("arb_bot.elite.rewards")


def _parse_end_date(end_date_str: str) -> datetime | None:
    """Parse ISO end date string to datetime."""
    if not end_date_str:
        return None
    try:
        if end_date_str.endswith("Z"):
            end_date_str = end_date_str[:-1] + "+00:00"
        return datetime.fromisoformat(end_date_str)
    except (ValueError, TypeError):
        return None


def _hours_until(end_date_str: str) -> float:
    """Calculate hours until a market's end date."""
    dt = _parse_end_date(end_date_str)
    if dt is None:
        return 9999
    now = datetime.now(timezone.utc)
    return (dt - now).total_seconds() / 3600


def pick_best_lp_market(cfg: dict, markets: list[dict] | None = None) -> dict | None:
    """
    Find the single best market for LP reward farming right now.

    Args:
        cfg: Full config dict
        markets: Optional pre-fetched market list. If None, fetches from API.

    Returns:
        Market dict with slug, title, condition_id, token_ids, end_date etc.
        None if no suitable market found.
    """
    if markets is None:
        markets = _fetch_lp_candidates(cfg)

    if not markets:
        logger.info("No markets available for LP farming")
        return None

    candidates = []

    for m in markets:
        # Must be Polymarket (LP rewards only on Polymarket)
        if m.get("platform", "") != "polymarket":
            continue

        # Must resolve within 24 hours
        hours_left = _hours_until(m.get("end_date", ""))
        if hours_left > 24 or hours_left < 1:
            continue

        # Volume threshold
        vol_24h = m.get("volume_24h", 0)
        if vol_24h < 10000:
            continue

        # Liquidity threshold
        liq = m.get("liquidity", 0)
        if liq < 5000:
            continue

        # Price balance check — mid-range is safer for LP
        yes_price = m.get("yes_price", 0)
        if yes_price < 0.20 or yes_price > 0.80:
            continue

        # Score = volume / liquidity (higher = more fills = more reward)
        vl_ratio = vol_24h / liq if liq > 0 else 0

        candidates.append({
            **m,
            "hours_left": hours_left,
            "vl_ratio": vl_ratio,
            "lp_score": vl_ratio * 10,  # Normalize to ~0-100
        })

    if not candidates:
        logger.info("No markets match LP farming criteria")
        return None

    # Sort by LP score (best first)
    candidates.sort(key=lambda x: x["lp_score"], reverse=True)
    best = candidates[0]

    logger.info(
        f"🏭 Best LP market: {best['title'][:50]} | "
        f"V/L={best['vl_ratio']:.1f} | {best['hours_left']:.1f}h left | "
        f"YES=${best['yes_price']:.2f}"
    )

    return best


def _fetch_lp_candidates(cfg: dict) -> list[dict]:
    """Fetch markets from Gamma API specifically for LP ranking."""
    base_url = cfg.get("scanner", {}).get(
        "gamma_api_url", "https://gamma-api.polymarket.com"
    )

    try:
        resp = requests.get(
            f"{base_url}/markets",
            params={
                "limit": 100,
                "closed": "false",
                "active": "true",
                "order": "volume24hr",
                "ascending": "false",
            },
            timeout=10,
        )
        resp.raise_for_status()
        raw = resp.json()
    except requests.RequestException as e:
        logger.error(f"LP market fetch error: {e}")
        return []

    markets = []
    for m in raw:
        try:
            prices_raw = m.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                prices = json.loads(prices_raw)
            else:
                prices = prices_raw or []

            yes_price = float(prices[0]) if prices else 0
            no_price = float(prices[1]) if len(prices) > 1 else 0

            # Extract token IDs for CLOB order placement
            clob_tokens = m.get("clobTokenIds", "[]")
            if isinstance(clob_tokens, str):
                clob_tokens = json.loads(clob_tokens)
            yes_token = clob_tokens[0] if clob_tokens else ""
            no_token = clob_tokens[1] if len(clob_tokens) > 1 else ""

            events_list = m.get("events", [])
            event_slug = (
                events_list[0].get("slug", m.get("slug", ""))
                if events_list else m.get("slug", "")
            )

            markets.append({
                "platform": "polymarket",
                "title": m.get("question", ""),
                "slug": m.get("slug", ""),
                "event_slug": event_slug,
                "market_id": m.get("id", ""),
                "condition_id": m.get("conditionId", ""),
                "yes_token_id": yes_token,
                "no_token_id": no_token,
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": float(m.get("volume", 0) or 0),
                "volume_24h": float(m.get("volume24hr", 0) or 0),
                "liquidity": float(m.get("liquidity", 0) or 0),
                "end_date": m.get("endDate", ""),
                "category": m.get("category", ""),
                "url": f"https://polymarket.com/event/{event_slug}",
            })
        except (ValueError, IndexError, TypeError):
            continue

    logger.info(f"Fetched {len(markets)} markets for LP screening")
    return markets


def find_lp_markets_display(cfg: dict, markets: list[dict] | None = None) -> str:
    """
    Format top 5 LP-able markets for /lp markets command.
    """
    if markets is None:
        markets = _fetch_lp_candidates(cfg)

    candidates = []
    for m in markets:
        if m.get("platform", "") != "polymarket":
            continue
        hours_left = _hours_until(m.get("end_date", ""))
        if hours_left > 48 or hours_left < 1:
            continue
        vol_24h = m.get("volume_24h", 0)
        liq = m.get("liquidity", 0)
        yes_price = m.get("yes_price", 0)
        if vol_24h < 5000 or liq < 2000 or yes_price < 0.15 or yes_price > 0.85:
            continue
        vl_ratio = vol_24h / liq if liq > 0 else 0
        candidates.append({**m, "hours_left": hours_left, "vl_ratio": vl_ratio})

    candidates.sort(key=lambda x: x["vl_ratio"], reverse=True)
    top5 = candidates[:5]

    if not top5:
        return (
            "🏭 <b>LP Markets</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "No suitable markets for LP farming right now.\n"
            "Need: resolves <24h, vol >$10K, balanced price."
        )

    msg = "🏭 <b>Top LP Markets</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    for i, m in enumerate(top5, 1):
        msg += (
            f"\n{i}. <b>{m['title'][:50]}</b>\n"
            f"   💰 Vol: ${m['volume_24h']:,.0f} | Liq: ${m['liquidity']:,.0f}\n"
            f"   📊 V/L: {m['vl_ratio']:.1f}x | YES: ${m['yes_price']:.2f}\n"
            f"   ⏱ {m['hours_left']:.0f}h remaining\n"
        )

    msg += "\n💡 <i>Higher V/L = more fills = more reward</i>"
    return msg
