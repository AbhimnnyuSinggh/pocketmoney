"""
scoring.py — Unified Edge Score System (0–100)
Every opportunity gets a single score that determines:
  1. Whether it's shown to users (min_score filter)
  2. Priority ordering (highest score = first signal)
  3. Future: Kelly sizing allocation

Formula:
  edge_score = W_profit * profit_score
             + W_liquidity * liquidity_score
             + W_time * time_score
             + W_confidence * confidence_score

All sub-scores are normalized to 0–100. Weights are config-driven.
"""
import math
import time
import logging
from datetime import datetime

logger = logging.getLogger("arb_bot.scoring")

# =========================================================================
# Default Weights (overridden by config.yaml → edge_scoring.weights)
# =========================================================================
DEFAULT_WEIGHTS = {
    "profit": 0.40,
    "liquidity": 0.30,
    "time": 0.15,
    "confidence": 0.15,
}

# =========================================================================
# Sub-Score Functions (each returns 0–100)
# =========================================================================

def _profit_score(profit_pct: float) -> float:
    """
    Score based on expected ROI %.
    0% → 0,  1% → 20,  3% → 50,  5% → 70,  10%+ → 90,  20%+ → 100.
    Uses logarithmic scaling so small edges still score reasonably.
    """
    if profit_pct <= 0:
        return 0.0
    # Log scale: score = 25 * ln(1 + profit_pct)
    # At 1% → 17, 3% → 35, 5% → 45, 10% → 60, 20% → 76, 50% → 100
    score = 25.0 * math.log(1 + profit_pct)
    return min(100.0, max(0.0, score))


def _liquidity_score(liquidity: float, volume_24h: float = 0) -> float:
    """
    Score based on market liquidity + 24h volume.
    $0 → 0,  $1K → 20,  $10K → 50,  $100K → 75,  $1M+ → 100.
    Low liquidity = harder to enter/exit = lower score.
    """
    # Combine liquidity and volume (volume matters more for execution)
    combined = liquidity + (volume_24h * 0.5)
    if combined <= 0:
        return 0.0
    # Log scale: score = 15 * ln(combined / 100)
    score = 15.0 * math.log(max(1, combined) / 100.0)
    return min(100.0, max(0.0, score))


def _time_score(hold_time: str) -> float:
    """
    Score based on time until resolution.
    Shorter = better for compounding.
    < 24h → 100,  1-3d → 80,  3-7d → 60,  7-30d → 40,  30d+ → 20.
    No date → 50 (neutral).
    """
    if not hold_time:
        return 50.0  # Unknown = neutral

    try:
        clean = hold_time.strip().replace("Z", "+00:00")
        if len(clean) == 10:
            clean += "T23:59:59+00:00"
        end_ts = datetime.fromisoformat(clean).timestamp()
        hours_until = (end_ts - time.time()) / 3600.0

        if hours_until < 0:
            return 10.0  # Already expired
        elif hours_until < 24:
            return 100.0
        elif hours_until < 72:
            return 80.0
        elif hours_until < 168:
            return 60.0
        elif hours_until < 720:
            return 40.0
        else:
            return 20.0
    except (ValueError, TypeError):
        return 50.0  # Parse error = neutral


def _confidence_score(opp) -> float:
    """
    Score based on opportunity type reliability.
    cross_platform_arb   → 95 (nearly risk-free if matched correctly)
    intra_market_arb     → 90 (guaranteed but may have execution risk)
    high_prob_bond       → 70 (high confidence but not guaranteed)
    whale_convergence    → 60 (directional signal, less certain)
    new_market           → 50 (first-mover edge but uncertain)
    """
    type_scores = {
        "cross_platform_arb": 95.0,
        "intra_market_arb": 90.0,
        "high_prob_bond": 70.0,
        "data_arb": 65.0,
        "anti_hype": 55.0,
        "whale_convergence": 60.0,
        "longshot": 40.0,
        "new_market": 50.0,
        "resolution_intel": 60.0,
        "micro_arb": 85.0,
        "spread_arb": 80.0,
    }
    return type_scores.get(opp.opp_type, 50.0)


# =========================================================================
# Main Scoring Function
# =========================================================================

def compute_edge_score(opp, market_data: dict | None = None, cfg: dict | None = None) -> float:
    """
    Compute unified edge score (0–100) for an Opportunity.

    Args:
        opp: Opportunity dataclass instance
        market_data: Optional dict with extra market info (liquidity, volume_24h)
        cfg: Config dict (for custom weights)

    Returns:
        float: Edge score 0–100
    """
    # Load weights from config or use defaults
    weights = DEFAULT_WEIGHTS.copy()
    if cfg:
        custom = cfg.get("edge_scoring", {}).get("weights", {})
        weights.update(custom)

    # Normalize weights to sum to 1.0
    total_w = sum(weights.values())
    if total_w > 0:
        weights = {k: v / total_w for k, v in weights.items()}

    # Sub-scores
    p_score = _profit_score(opp.profit_pct)

    # Get liquidity from market_data or legs
    liq = 0.0
    vol = 0.0
    if market_data:
        liq = market_data.get("liquidity", 0)
        vol = market_data.get("volume_24h", 0)
    l_score = _liquidity_score(liq, vol)

    t_score = _time_score(opp.hold_time)
    c_score = _confidence_score(opp)

    # Weighted sum
    edge = (
        weights.get("profit", 0.4) * p_score
        + weights.get("liquidity", 0.3) * l_score
        + weights.get("time", 0.15) * t_score
        + weights.get("confidence", 0.15) * c_score
    )

    return round(min(100.0, max(0.0, edge)), 1)


def score_emoji(score: float) -> str:
    """Return emoji indicator for edge score."""
    if score >= 85:
        return "🔥"  # Fire — exceptional edge
    elif score >= 70:
        return "⚡"  # Lightning — strong edge
    elif score >= 55:
        return "✅"  # Check — decent edge
    elif score >= 40:
        return "⚠️"  # Warning — marginal
    else:
        return "❌"  # Skip — weak
