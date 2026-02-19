"""
resolution_intel.py — Resolution Intelligence Module

Analyzes market metadata to detect resolution-related edge opportunities:
  1. Near-expiry mispricing — Markets resolving soon but price still uncertain
  2. Ambiguous resolution — Markets with unclear/disputed resolution criteria
  3. Resolution source mismatch — Market claims vs actual data source gaps

Produces `resolution_intel` type opportunities.
"""
import time
import logging
import re
from datetime import datetime, timezone, timedelta
from cross_platform_scanner import Opportunity

logger = logging.getLogger("arb_bot.resolution_intel")

# Resolution source keywords for quality assessment
STRONG_SOURCES = {
    "associated press", "reuters", "ap news", "official", "government",
    "sec", "fda", "cdc", "who", "un",
}

WEAK_SOURCES = {
    "twitter", "x.com", "reddit", "blog", "rumor", "source says",
    "reportedly", "allegedly", "unconfirmed",
}

# Time-sensitive keywords
URGENCY_KEYWORDS = {
    "today", "tonight", "tomorrow", "breaking", "just in",
    "developing", "happening now", "imminent", "deadline",
}


def find_resolution_intel_opportunities(
    markets: list[dict], cfg: dict
) -> list[Opportunity]:
    """
    Scan markets for resolution intelligence opportunities.

    Args:
        markets: List of market dicts from Polymarket API
        cfg: Bot configuration

    Returns:
        List of Opportunity objects for resolution-based edges
    """
    ri_cfg = cfg.get("resolution_intel", {})
    if not ri_cfg.get("enabled", True):
        return []

    opportunities = []
    now = time.time()

    for market in markets:
        try:
            title = market.get("question", market.get("title", ""))
            description = market.get("description", "")
            end_date_str = market.get("end_date_iso", market.get("endDate", ""))
            volume = float(market.get("volume", 0))
            liquidity = float(market.get("liquidity", 0))

            if not title:
                continue

            # Get best YES price
            outcomes = market.get("outcomes", [])
            tokens = market.get("tokens", [])
            yes_price = 0.5
            no_price = 0.5

            if tokens and len(tokens) >= 2:
                yes_price = float(tokens[0].get("price", 0.5))
                no_price = float(tokens[1].get("price", 0.5))
            elif outcomes:
                yes_price = float(outcomes[0]) if isinstance(outcomes[0], (int, float)) else 0.5

            # --- Check 1: Near-expiry mispricing ---
            if end_date_str:
                opp = _check_near_expiry(
                    title, description, end_date_str,
                    yes_price, no_price, volume, liquidity, market, now
                )
                if opp:
                    opportunities.append(opp)

            # --- Check 2: Ambiguous resolution criteria ---
            opp = _check_ambiguous_resolution(
                title, description, yes_price, volume, liquidity, market
            )
            if opp:
                opportunities.append(opp)

            # --- Check 3: Resolution source quality ---
            opp = _check_source_quality(
                title, description, yes_price, no_price, volume, market
            )
            if opp:
                opportunities.append(opp)

        except (ValueError, TypeError, KeyError) as e:
            logger.debug(f"Resolution Intel skip: {e}")
            continue

    if opportunities:
        logger.info(f"🔍 Resolution Intel: {len(opportunities)} signals")

    return opportunities


def _check_near_expiry(
    title: str, description: str, end_date_str: str,
    yes_price: float, no_price: float,
    volume: float, liquidity: float,
    market: dict, now: float,
) -> Opportunity | None:
    """
    Markets resolving within 6 hours where price is still uncertain (30-70¢).
    This often means the market hasn't priced in available information.
    """
    try:
        # Parse end date
        clean = end_date_str.strip().replace("Z", "+00:00")
        if len(clean) == 10:
            clean += "T23:59:59+00:00"
        end_dt = datetime.fromisoformat(clean)
        end_ts = end_dt.timestamp()
    except (ValueError, TypeError):
        return None

    hours_left = (end_ts - now) / 3600

    # Only interested in markets resolving in 1-12 hours
    if hours_left < 1 or hours_left > 12:
        return None

    # Price must be uncertain (neither side is confident)
    if yes_price > 0.80 or yes_price < 0.20:
        return None  # Already priced in

    # Need meaningful volume
    if volume < 1000:
        return None

    # Calculate edge: How far from 50/50
    uncertainty = abs(yes_price - 0.50)
    profit_pct = round(max(yes_price, 1 - yes_price) * 100 - 50, 2)

    event_slug = market.get("event_slug", market.get("slug", ""))
    url = f"https://polymarket.com/event/{event_slug}" if event_slug else ""

    return Opportunity(
        opp_type="resolution_intel",
        title=title[:120],
        description=(
            f"🔍 NEAR-EXPIRY INTEL\n"
            f"⏰ Resolves in {hours_left:.1f} hours\n"
            f"💰 YES: {yes_price:.0%} | NO: {no_price:.0%}\n"
            f"📊 Volume: ${volume:,.0f} | Liq: ${liquidity:,.0f}\n"
            f"⚡ Price still uncertain despite imminent resolution.\n"
            f"Research the resolution source for an edge."
        ),
        profit_pct=profit_pct,
        profit_amount=round(profit_pct * 1.0, 2),
        total_cost=round(min(yes_price, no_price), 4),
        platforms=["polymarket"],
        legs=[{
            "platform": "Polymarket",
            "side": f"YES @ {yes_price:.0%}" if yes_price > 0.5 else f"NO @ {no_price:.0%}",
            "price": yes_price if yes_price > 0.5 else no_price,
        }],
        urls=[url],
        risk_level="medium",
        hold_time=end_date_str[:10] if end_date_str else "",
        category=market.get("category", ""),
    )


def _check_ambiguous_resolution(
    title: str, description: str,
    yes_price: float, volume: float, liquidity: float,
    market: dict,
) -> Opportunity | None:
    """
    Markets with ambiguous or disputed resolution criteria.
    These are risky but can offer edges to those who understand the rules.
    """
    desc_lower = (description or "").lower()
    title_lower = title.lower()

    # Ambiguity signals
    ambiguity_score = 0
    reasons = []

    # Vague resolution language
    vague_phrases = [
        "at the discretion", "may be resolved", "subject to interpretation",
        "as determined by", "in the opinion of", "reasonable interpretation",
        "unclear", "disputed", "controversial",
    ]
    for phrase in vague_phrases:
        if phrase in desc_lower:
            ambiguity_score += 2
            reasons.append(f"'{phrase}' in description")

    # Multiple resolution conditions
    if desc_lower.count("or ") > 3:
        ambiguity_score += 1
        reasons.append("Multiple OR conditions")

    # Very long description (often = complex rules)
    if len(description or "") > 2000:
        ambiguity_score += 1
        reasons.append("Very long resolution description")

    # Price near 50/50 with high volume = genuine disagreement
    if 0.40 < yes_price < 0.60 and volume > 10000:
        ambiguity_score += 1
        reasons.append("50/50 split with high volume")

    if ambiguity_score < 3:
        return None

    if volume < 2000:
        return None

    event_slug = market.get("event_slug", market.get("slug", ""))
    url = f"https://polymarket.com/event/{event_slug}" if event_slug else ""

    return Opportunity(
        opp_type="resolution_intel",
        title=title[:120],
        description=(
            f"⚠️ AMBIGUOUS RESOLUTION\n"
            f"🔍 Ambiguity score: {ambiguity_score}/10\n"
            f"💰 YES: {yes_price:.0%} | Vol: ${volume:,.0f}\n"
            f"📋 Flags: {'; '.join(reasons[:3])}\n"
            f"⚡ Read the resolution source carefully — edge for those who do."
        ),
        profit_pct=round(abs(yes_price - 0.5) * 100, 2),
        profit_amount=round(abs(yes_price - 0.5) * 100, 2),
        total_cost=round(min(yes_price, 1 - yes_price), 4),
        platforms=["polymarket"],
        legs=[{
            "platform": "Polymarket",
            "side": "Research required",
            "price": yes_price,
        }],
        urls=[url],
        risk_level="high",
        hold_time="",
        category=market.get("category", ""),
    )


def _check_source_quality(
    title: str, description: str,
    yes_price: float, no_price: float,
    volume: float, market: dict,
) -> Opportunity | None:
    """
    Check if market resolution source is weak or strong.
    Flag markets relying on weak sources where a strong source tells a different story.
    """
    desc_lower = (description or "").lower()

    has_strong = any(src in desc_lower for src in STRONG_SOURCES)
    has_weak = any(src in desc_lower for src in WEAK_SOURCES)

    # Only interesting if market relies on weak source with meaningful volume
    if not has_weak or has_strong:
        return None

    if volume < 5000:
        return None

    # Price must not be settled (avoid 90%+ markets)
    if yes_price > 0.85 or yes_price < 0.15:
        return None

    event_slug = market.get("event_slug", market.get("slug", ""))
    url = f"https://polymarket.com/event/{event_slug}" if event_slug else ""

    weak_found = [src for src in WEAK_SOURCES if src in desc_lower]

    return Opportunity(
        opp_type="resolution_intel",
        title=title[:120],
        description=(
            f"🔬 WEAK RESOLUTION SOURCE\n"
            f"⚠️ Relies on: {', '.join(weak_found[:3])}\n"
            f"💰 YES: {yes_price:.0%} | Vol: ${volume:,.0f}\n"
            f"📋 No official source referenced.\n"
            f"⚡ Cross-check with official sources for an edge."
        ),
        profit_pct=round(abs(yes_price - 0.5) * 100, 2),
        profit_amount=round(abs(yes_price - 0.5) * 50, 2),
        total_cost=round(min(yes_price, no_price), 4),
        platforms=["polymarket"],
        legs=[{
            "platform": "Polymarket",
            "side": "Cross-reference needed",
            "price": yes_price,
        }],
        urls=[url],
        risk_level="high",
        hold_time="",
        category=market.get("category", ""),
    )
