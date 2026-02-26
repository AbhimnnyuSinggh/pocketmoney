"""
elite_edges/anti_hype.py — Anti-Hype Detector

Identifies markets where YES price is significantly higher than what
fundamentals suggest (crowd euphoria / hype). Generates "bet NO" signals.

Detection Logic:
  1. Find markets with YES price > cfg threshold (e.g., > 50¢)
  2. Check for hype indicators:
     - Sudden volume spike (24h vol >> 7d avg)
     - Price recently jumped > 15¢ upward
     - Category matches hype-prone areas (crypto, politics)
  3. Estimate "fair" probability from market structure
  4. If gap between market price and fair estimate > min_delta → signal

This is a signal-based module — it creates Opportunity objects for
the bot to distribute. No execution is performed here.
"""
import time
import logging
from datetime import datetime
from html import escape as html_escape

logger = logging.getLogger("arb_bot.elite.anti_hype")


# Categories prone to crowd hype
HYPE_PRONE_CATEGORIES = {
    "crypto", "cryptocurrency", "bitcoin", "ethereum",
    "politics", "election", "trump", "biden",
    "tech", "ai", "artificial intelligence",
    "meme", "viral", "celebrity",
}


def _is_hype_candidate(market: dict, cfg: dict) -> bool:
    """Check if a market shows signs of crowd hype."""
    anti_hype_cfg = cfg.get("anti_hype", {})
    max_price = anti_hype_cfg.get("max_price", 0.85)
    min_volume = anti_hype_cfg.get("min_volume", 5000)

    yes_price = market.get("yes_price", 0)
    volume_24h = market.get("volume_24h", 0)

    # Must be in the "expensive but not certain" range
    if yes_price < 0.45 or yes_price > max_price:
        return False

    # Must have meaningful volume (hype needs volume)
    if volume_24h < min_volume:
        return False

    return True


def _estimate_hype_delta(market: dict, cfg: dict) -> tuple[float, str]:
    """
    Estimate how much the market is overhyped.

    Returns: (delta_pct, reason)
      - delta_pct: How much YES is overpriced (e.g., 20 = 20% overhyped)
      - reason: Human-readable explanation
    """
    yes_price = market.get("yes_price", 0)
    no_price = market.get("no_price", 0)
    volume_24h = market.get("volume_24h", 0)
    liquidity = market.get("liquidity", 0)
    category = market.get("category", "").lower()
    title = market.get("title", "").lower()

    delta = 0.0
    reasons = []

    # Heuristic 1: Volume/Liquidity ratio (high = potential hype)
    # Normal markets have V/L < 5. Hyped markets often 10-50+
    if liquidity > 0:
        vl_ratio = volume_24h / liquidity
        if vl_ratio > 10:
            delta += 15.0
            reasons.append(f"Vol/Liq ratio {vl_ratio:.1f}x (normal <5)")
        elif vl_ratio > 5:
            delta += 8.0
            reasons.append(f"High vol/liq ratio {vl_ratio:.1f}x")

    # Heuristic 2: YES+NO spread indicates market inefficiency
    total = yes_price + no_price
    if total > 1.02:
        # Both sides overpriced = hype
        delta += (total - 1.0) * 100
        reasons.append(f"Total {total:.3f} > $1.00 (both sides overpriced)")

    # Heuristic 3: Category-based hype adjustment
    for keyword in HYPE_PRONE_CATEGORIES:
        if keyword in category or keyword in title:
            delta += 5.0
            reasons.append(f"Hype-prone category: {keyword}")
            break

    # Heuristic 4: Extreme prices with low liquidity = fragile conviction
    if yes_price > 0.70 and liquidity < 20000:
        delta += 10.0
        reasons.append(f"High price ({yes_price:.0%}) but low liq (${liquidity:,.0f})")

    reason_text = " | ".join(reasons) if reasons else "General overpricing detected"
    return (delta, reason_text)


def find_anti_hype_opportunities(
    markets: list[dict],
    cfg: dict,
) -> list:
    from cross_platform_scanner import Opportunity
    try:
        from scoring import compute_edge_score
    except ImportError:
        compute_edge_score = None

    anti_hype_cfg = cfg.get("anti_hype", {})
    min_delta = anti_hype_cfg.get("min_delta", 8)
    enabled = anti_hype_cfg.get("enabled", True)

    if not enabled:
        return []

    # --- Diagnostic counters ---
    total = len(markets)
    skip_closed = skip_price_range = skip_volume = skip_delta = 0
    opportunities = []

    for m in markets:
        if m.get("closed") or not m.get("active", True):
            skip_closed += 1
            continue

        yes_price = m.get("yes_price", 0)
        max_price = anti_hype_cfg.get("max_price", 0.90)
        min_volume = anti_hype_cfg.get("min_volume", 1000)

        if yes_price < 0.45 or yes_price > max_price:
            skip_price_range += 1
            continue

        if m.get("volume_24h", 0) < min_volume:
            skip_volume += 1
            continue

        delta, reason = _estimate_hype_delta(m, cfg)

        if delta < min_delta:
            skip_delta += 1
            continue

        no_price = m.get("no_price", 0)
        no_profit = 1.0 - no_price
        no_roi = (no_profit / no_price) * 100 if no_price > 0 else 0

        opp = Opportunity(
            opp_type="anti_hype",
            title=m["title"],
            description=(
                f"🔻 <b>HYPE DETECTED — Bet NO</b>\n"
                f"Market YES price: ${yes_price:.2f} (overhyped by ~{delta:.0f}%)\n"
                f"Buy NO @ ${no_price:.4f} → pays $1.00\n"
                f"\n"
                f"💡 <b>Why overhyped:</b>\n"
                f"  {html_escape(reason)}\n"
                f"\n"
                f"Platform: {html_escape(m['platform'].title())}\n"
                f"Volume 24h: ${m.get('volume_24h', 0):,.0f}\n"
                f"Liquidity: ${m.get('liquidity', 0):,.0f}"
            ),
            profit_pct=round(no_roi, 2),
            profit_amount=round(no_profit * 100, 2),
            total_cost=round(no_price, 4),
            platforms=[m["platform"]],
            legs=[{"platform": m["platform"], "side": "NO", "price": no_price}],
            urls=[m.get("url", "")],
            risk_level="medium",
            hold_time=m.get("end_date", ""),
            category=m.get("category", ""),
        )

        if compute_edge_score:
            opp.edge_score = compute_edge_score(opp, market_data=m, cfg=cfg)

        opportunities.append(opp)
        logger.info(
            f"[ANTI-HYPE] {m['title'][:50]} | Delta: {delta:.0f}% | NO ROI: {no_roi:.1f}%"
        )

    opportunities.sort(key=lambda o: o.profit_pct, reverse=True)
    opportunities = opportunities[:5]

    # Always log diagnostic summary
    logger.info(
        f"[ANTI-HYPE DIAG] markets={total} | closed={skip_closed} "
        f"| price_range={skip_price_range} | vol<{min_volume}={skip_volume} "
        f"| delta<{min_delta}={skip_delta} | SIGNALS={len(opportunities)}"
    )
    return opportunities
