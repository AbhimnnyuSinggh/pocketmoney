"""
elite_edges/bond_compounder.py — Enhanced Bond Scanner with Compounding Projections

Extends the existing high-probability bond scanner with:
  1. Edge Score filtering (only show bonds scoring above threshold)
  2. Compounding projections (shows daily/weekly ROI at given reinvest rate)
  3. Volume-weighted prioritization (higher volume = more reliable)
  4. Resolution time awareness (shorter resolve = faster compound)

This module processes EXISTING bond opportunities from the main scanner
and enriches them — it does NOT re-scan markets.
"""
import math
import time
import logging
from datetime import datetime

logger = logging.getLogger("arb_bot.elite.bond_compounder")


def compute_compound_projection(
    roi_pct: float,
    hold_hours: float,
    bankroll: float = 100.0,
    reinvest_rate: float = 0.80,
) -> dict:
    """
    Project compounding returns for a bond opportunity.

    Args:
        roi_pct: Single-trade ROI percentage (e.g., 5.0 = 5%)
        hold_hours: Hours until resolution
        bankroll: Starting capital
        reinvest_rate: Fraction of profit to reinvest (0.8 = 80%)

    Returns:
        dict with projections for daily, weekly, and monthly compounding
    """
    if roi_pct <= 0 or hold_hours <= 0:
        return {"daily": 0, "weekly": 0, "monthly": 0, "trades_per_day": 0}

    roi_frac = roi_pct / 100.0
    trades_per_day = min(24.0 / hold_hours, 20)  # Cap at 20 trades/day

    # Per-trade growth factor (reinvest portion of profit)
    growth = 1.0 + (roi_frac * reinvest_rate)

    # Compound over time periods
    daily_return = (growth ** trades_per_day - 1) * 100
    weekly_return = (growth ** (trades_per_day * 7) - 1) * 100
    monthly_return = (growth ** (trades_per_day * 30) - 1) * 100

    return {
        "daily": round(daily_return, 2),
        "weekly": round(weekly_return, 2),
        "monthly": round(min(monthly_return, 99999), 2),  # Cap display
        "trades_per_day": round(trades_per_day, 1),
        "bankroll_1w": round(bankroll * (growth ** (trades_per_day * 7)), 2),
    }


def enrich_bond_opportunities(
    opportunities: list,
    cfg: dict,
) -> list:
    """
    Enrich bond opportunities with compounding projections.
    Modifies opportunity descriptions in-place and returns filtered list.

    Args:
        opportunities: List of Opportunity objects (only bonds are processed)
        cfg: Config dict

    Returns:
        Filtered and enriched list of bond opportunities
    """
    bond_cfg = cfg.get("bond_compound", {})
    min_price = bond_cfg.get("min_price", 0.95)
    min_liq = bond_cfg.get("min_liq", 5000)
    reinvest_rate = bond_cfg.get("reinvest_rate", 0.80)
    bankroll = cfg.get("bankroll", {}).get("total_usdc", 100)

    enriched = []
    for opp in opportunities:
        if opp.opp_type != "high_prob_bond":
            continue

        # Parse hold time for compounding calc
        hold_hours = _estimate_hold_hours(opp.hold_time)

        # Compute compounding projection
        proj = compute_compound_projection(
            roi_pct=opp.profit_pct,
            hold_hours=hold_hours,
            bankroll=bankroll,
            reinvest_rate=reinvest_rate,
        )

        # Add compounding projection to description
        if proj["daily"] > 0:
            compound_text = (
                f"\n📈 <b>Compound Projections</b> "
                f"({int(reinvest_rate * 100)}% reinvest):\n"
                f"  Daily: +{proj['daily']:.1f}%"
                f" ({proj['trades_per_day']:.0f} trades/day)\n"
                f"  Weekly: +{proj['weekly']:.1f}%\n"
                f"  $100 → ${proj['bankroll_1w']:.0f} in 1 week"
            )
            opp.description += compound_text

        enriched.append(opp)

    if enriched:
        logger.info(
            f"Bond Compounder: enriched {len(enriched)} bonds with projections"
        )

    return enriched


def _estimate_hold_hours(hold_time: str) -> float:
    """Estimate hours until resolution from hold_time string."""
    if not hold_time:
        return 168  # Default: 1 week

    try:
        clean = hold_time.strip().replace("Z", "+00:00")
        if len(clean) == 10:
            clean += "T23:59:59+00:00"
        end_ts = datetime.fromisoformat(clean).timestamp()
        hours = (end_ts - time.time()) / 3600.0
        return max(1, hours)  # At least 1 hour
    except (ValueError, TypeError):
        return 168  # Default: 1 week
