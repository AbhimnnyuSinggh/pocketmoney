"""
elite_edges/longshot_scanner.py — Asymmetric Longshot Scanner

Finds undervalued outcomes priced 5–30¢ with potential
for large payoffs (3–10x). Focuses on:
  1. Markets with NO side priced low (crowd consensus is YES)
     but the event has real uncertainty
  2. Markets with YES side priced low with catalysts
  3. Volume/price momentum suggesting informed money

Core principle: Risk $5–20 to potentially earn $50–200.
Even a 20% hit rate is profitable at these odds.
"""
import time
import logging
from datetime import datetime

logger = logging.getLogger("arb_bot.elite.longshot")


def _calculate_payoff_ratio(price: float) -> float:
    """Calculate potential payoff ratio (e.g., 5¢ → 20x)."""
    if price <= 0 or price >= 1:
        return 0
    return 1.0 / price


def _has_catalyst_signals(market: dict) -> tuple[bool, str]:
    """
    Check if a low-priced market shows signs of having a real catalyst
    (not just a dead/forgotten market).

    Returns: (has_signals, reason)
    """
    volume_24h = market.get("volume_24h", 0)
    liquidity = market.get("liquidity", 0)
    title = market.get("title", "").lower()

    signals = []

    # Volume indicates activity
    if volume_24h > 1000:
        signals.append(f"Active trading (${volume_24h:,.0f}/24h)")

    # Reasonable liquidity means market is monitored
    if liquidity > 5000:
        signals.append(f"Good liquidity (${liquidity:,.0f})")

    # Title keywords suggesting potential upside
    catalyst_keywords = [
        "announce", "launch", "release", "approve",
        "vote", "decision", "ruling", "verdict",
        "breakout", "surge", "rally", "surprise",
        "unexpected", "upset", "dark horse",
    ]
    for kw in catalyst_keywords:
        if kw in title:
            signals.append(f"Catalyst keyword: '{kw}'")
            break

    return (len(signals) >= 2, " | ".join(signals))


def find_longshot_opportunities(
    markets: list[dict],
    cfg: dict,
) -> list:
    from cross_platform_scanner import Opportunity
    try:
        from scoring import compute_edge_score
    except ImportError:
        compute_edge_score = None

    longshot_cfg = cfg.get("longshot", {})
    min_payoff = longshot_cfg.get("min_payoff", 2)
    max_price = longshot_cfg.get("max_price", 0.40)
    min_price = longshot_cfg.get("min_price", 0.02)
    min_volume = longshot_cfg.get("min_volume", 200)
    enabled = longshot_cfg.get("enabled", True)

    if not enabled:
        return []

    # --- Diagnostic counters ---
    total = len(markets)
    skip_closed = skip_volume = skip_price = skip_payoff = skip_catalyst = 0
    opportunities = []

    for m in markets:
        if m.get("closed") or not m.get("active", True):
            skip_closed += 1
            continue

        volume_24h = m.get("volume_24h", 0)
        if volume_24h < min_volume:
            skip_volume += 1
            continue

        for side, price in [("YES", m.get("yes_price", 0)), ("NO", m.get("no_price", 0))]:
            if price < min_price or price > max_price:
                skip_price += 1
                continue

            payoff = _calculate_payoff_ratio(price)
            if payoff < min_payoff:
                skip_payoff += 1
                continue

            has_catalyst, catalyst_reason = _has_catalyst_signals(m)
            if not has_catalyst:
                skip_catalyst += 1
                continue

            profit = 1.0 - price
            roi = (profit / price) * 100

            opp = Opportunity(
                opp_type="longshot",
                title=m["title"],
                description=(
                    f"🎯 <b>ASYMMETRIC LONGSHOT — {payoff:.0f}x Potential</b>\n"
                    f"Buy {side} @ ${price:.4f} → pays $1.00\n"
                    f"Risk $10 to potentially win ${10 * payoff:.0f}\n"
                    f"\n"
                    f"💡 <b>Why this longshot:</b>\n"
                    f"  {catalyst_reason}\n"
                    f"\n"
                    f"📊 Payoff: <b>{payoff:.1f}x</b> | "
                    f"ROI: <b>{roi:.0f}%</b>\n"
                    f"Platform: {m['platform'].title()}\n"
                    f"Volume 24h: ${volume_24h:,.0f}"
                ),
                profit_pct=round(roi, 2),
                profit_amount=round(profit * 100, 2),
                total_cost=round(price, 4),
                platforms=[m["platform"]],
                legs=[{"platform": m["platform"], "side": side, "price": price}],
                urls=[m.get("url", "")],
                risk_level="high",
                hold_time=m.get("end_date", ""),
                category=m.get("category", ""),
            )

            if compute_edge_score:
                opp.edge_score = compute_edge_score(opp, market_data=m, cfg=cfg)

            opportunities.append(opp)
            logger.info(
                f"[LONGSHOT] {m['title'][:50]} | "
                f"{payoff:.0f}x | {side} @ ${price:.2f}"
            )

    # Sort by payoff ratio (highest first), cap at 5
    opportunities.sort(
        key=lambda o: _calculate_payoff_ratio(o.total_cost), reverse=True
    )
    opportunities = opportunities[:5]

    # Always log diagnostic summary
    logger.info(
        f"[LONGSHOT DIAG] markets={total} | closed={skip_closed} "
        f"| vol<{min_volume}={skip_volume} | price_range={skip_price} "
        f"| payoff<{min_payoff}={skip_payoff} | no_catalyst={skip_catalyst} "
        f"| SIGNALS={len(opportunities)}"
    )
    return opportunities
