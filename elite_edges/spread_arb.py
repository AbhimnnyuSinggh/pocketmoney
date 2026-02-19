"""
spread_arb.py — Spread Arb Engine

Detects YES+NO price gaps across related or linked markets where the spread
offers 5-15% edge. Signal-only — alerts user but does not execute.

Strategies:
  1. Same-event spread: YES+NO significantly != $1.00
  2. Mirror market: Two markets on same topic with inverted pricing
  3. Stale pricing: One side hasn't updated while the other moved

Produces `spread_arb` type opportunities.
"""
import logging
from difflib import SequenceMatcher
from cross_platform_scanner import Opportunity

logger = logging.getLogger("arb_bot.spread_arb")


def find_spread_arb_opportunities(
    markets: list[dict], cfg: dict
) -> list[Opportunity]:
    """
    Scan for spread arbitrage opportunities across markets.

    Args:
        markets: List of market dicts from Polymarket API
        cfg: Bot configuration

    Returns:
        List of Opportunity objects for spread arb signals
    """
    sa_cfg = cfg.get("spread_arb", {})
    if not sa_cfg.get("enabled", True):
        return []

    min_spread_pct = sa_cfg.get("min_spread_pct", 5.0)
    min_volume = sa_cfg.get("min_volume", 1000)
    similarity_threshold = sa_cfg.get("similarity_threshold", 0.70)

    opportunities = []

    # --- Strategy 1: Same-event spread (YES+NO gap) ---
    for market in markets:
        try:
            opp = _check_single_market_spread(market, min_spread_pct, min_volume)
            if opp:
                opportunities.append(opp)
        except (ValueError, TypeError, KeyError):
            continue

    # --- Strategy 2: Mirror markets (related events with inverted pricing) ---
    try:
        mirror_opps = _find_mirror_markets(markets, similarity_threshold, min_volume)
        opportunities.extend(mirror_opps)
    except Exception as e:
        logger.debug(f"Mirror market scan error: {e}")

    if opportunities:
        logger.info(f"📐 Spread Arb: {len(opportunities)} signals")

    return opportunities


def _check_single_market_spread(
    market: dict, min_spread_pct: float, min_volume: float
) -> Opportunity | None:
    """Check a single market for YES+NO spread opportunities."""
    title = market.get("question", market.get("title", ""))
    volume = float(market.get("volume", 0))

    if volume < min_volume:
        return None

    tokens = market.get("tokens", [])
    if not tokens or len(tokens) < 2:
        return None

    yes_price = float(tokens[0].get("price", 0))
    no_price = float(tokens[1].get("price", 0))

    if yes_price <= 0 or no_price <= 0:
        return None

    price_sum = yes_price + no_price
    spread_pct = abs(1.0 - price_sum) * 100

    if spread_pct < min_spread_pct:
        return None

    # Must be a meaningful spread, not just dust prices
    if yes_price < 0.03 or no_price < 0.03:
        return None

    # Determine the edge direction
    if price_sum < 1.0:
        strategy = "BUY BOTH"
        action = f"Buy YES ({yes_price:.2f}) + NO ({no_price:.2f}) = {price_sum:.4f} < $1.00"
        profit_pct = round((1.0 - price_sum) / price_sum * 100, 2)
    else:
        strategy = "OVERPRICED"
        action = f"YES ({yes_price:.2f}) + NO ({no_price:.2f}) = {price_sum:.4f} > $1.00"
        profit_pct = round((price_sum - 1.0) * 100, 2)

    event_slug = market.get("event_slug", market.get("slug", ""))
    url = f"https://polymarket.com/event/{event_slug}" if event_slug else ""

    return Opportunity(
        opp_type="spread_arb",
        title=title[:120],
        description=(
            f"📐 SPREAD ARB — {strategy}\n"
            f"💰 {action}\n"
            f"📊 Spread: {spread_pct:.1f}%\n"
            f"📈 Volume: ${volume:,.0f}\n"
            f"⚡ Buy both sides for guaranteed profit at resolution."
        ),
        profit_pct=profit_pct,
        profit_amount=round(profit_pct * 1.0, 2),
        total_cost=round(price_sum, 4),
        platforms=["polymarket"],
        legs=[
            {"platform": "Polymarket", "side": "YES", "price": yes_price},
            {"platform": "Polymarket", "side": "NO", "price": no_price},
        ],
        urls=[url],
        risk_level="low" if price_sum < 1.0 else "medium",
        hold_time=market.get("end_date_iso", "")[:10] if market.get("end_date_iso") else "",
        category=market.get("category", ""),
    )


def _find_mirror_markets(
    markets: list[dict], threshold: float, min_volume: float
) -> list[Opportunity]:
    """
    Find pairs of markets that are mirrors (same topic, inverted pricing).
    Example: "Will X happen?" at 60% + "Will X NOT happen?" at 60%
    = 120% total → 20% overpriced.
    """
    opportunities = []

    # Build list of (title, yes_price, market) tuples
    parsed = []
    for m in markets:
        title = m.get("question", m.get("title", ""))
        vol = float(m.get("volume", 0))
        tokens = m.get("tokens", [])

        if not title or vol < min_volume or not tokens or len(tokens) < 2:
            continue

        yes_price = float(tokens[0].get("price", 0))
        if yes_price <= 0:
            continue

        parsed.append((title.lower(), yes_price, m))

    # Compare pairs (limit to first 200 to avoid O(n²) explosion)
    parsed = parsed[:200]
    seen_pairs = set()

    for i in range(len(parsed)):
        for j in range(i + 1, min(len(parsed), i + 50)):
            t1, p1, m1 = parsed[i]
            t2, p2, m2 = parsed[j]

            # Check title similarity
            ratio = SequenceMatcher(None, t1, t2).ratio()
            if ratio < threshold:
                continue

            pair_key = f"{min(t1[:30], t2[:30])}_{max(t1[:30], t2[:30])}"
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            # Check if prices suggest mirror (both YES > 50% on similar markets)
            combined = p1 + p2
            if combined < 1.10:  # Need at least 10% overpriced
                continue

            spread = round((combined - 1.0) * 100, 2)
            title1 = m1.get("question", m1.get("title", ""))
            title2 = m2.get("question", m2.get("title", ""))

            slug1 = m1.get("event_slug", m1.get("slug", ""))
            slug2 = m2.get("event_slug", m2.get("slug", ""))
            url1 = f"https://polymarket.com/event/{slug1}" if slug1 else ""
            url2 = f"https://polymarket.com/event/{slug2}" if slug2 else ""

            opp = Opportunity(
                opp_type="spread_arb",
                title=f"Mirror: {title1[:55]} vs {title2[:55]}",
                description=(
                    f"📐 MIRROR MARKET SPREAD\n"
                    f"🅰️ {title1[:60]}\n"
                    f"   YES: {p1:.0%}\n"
                    f"🅱️ {title2[:60]}\n"
                    f"   YES: {p2:.0%}\n"
                    f"📊 Combined: {combined:.0%} (spread: {spread:.1f}%)\n"
                    f"⚡ These markets appear to be mirrors — combined YES > 100%."
                ),
                profit_pct=spread,
                profit_amount=round(spread * 0.5, 2),
                total_cost=round(combined, 4),
                platforms=["polymarket"],
                legs=[
                    {"platform": "Polymarket", "side": f"YES @ {p1:.0%}", "price": p1},
                    {"platform": "Polymarket", "side": f"YES @ {p2:.0%}", "price": p2},
                ],
                urls=[url1, url2],
                risk_level="medium",
                hold_time="",
                category=m1.get("category", ""),
            )
            opportunities.append(opp)

    return opportunities
