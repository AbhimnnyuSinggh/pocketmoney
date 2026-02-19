"""
elite_edges/data_arb.py — External Data Arb Integrator

Compares prediction market prices against external data sources
(news headlines, known scheduled events, date-based resolution logic)
to find significant mismatches.

Detection Logic:
  1. Scan market titles for date/event-based resolution patterns
  2. Check if resolution date has passed or outcome is knowable
  3. Compare market price to "objective" probability from title parsing
  4. If delta > threshold → signal an arb opportunity

NOTE: Without paid APIs (X, RCP, 538), this module uses:
  - Title parsing for implicit probabilities
  - Date-based analysis (markets about events that already happened)
  - Price-vs-consensus heuristics from market structure
"""
import re
import time
import logging
from datetime import datetime, timezone

logger = logging.getLogger("arb_bot.elite.data_arb")

# Patterns that suggest a market has a near-certain outcome
CERTAINTY_PATTERNS = [
    # Date-based: "Will X happen before DATE?" where DATE has passed
    (r"before\s+(\w+\s+\d{1,2},?\s+\d{4})", "date_deadline"),
    (r"by\s+(\w+\s+\d{1,2},?\s+\d{4})", "date_deadline"),
    # Numerical: "Will X reach Y?" with specific targets
    (r"reach\s+\$?([\d,.]+)", "numerical_target"),
    (r"above\s+\$?([\d,.]+)", "numerical_target"),
    (r"below\s+\$?([\d,.]+)", "numerical_target"),
    # Binary events: "Will X win?" in completed elections/events
    (r"winner\s+of", "outcome_known"),
    (r"win\s+the\s+(\d{4})", "outcome_known"),
]


def _analyze_market_for_data_edge(
    market: dict,
    now_ts: float,
) -> tuple[float, str] | None:
    """
    Analyze a single market for data-based edge.

    Returns: (edge_pct, explanation) or None if no edge found.
    """
    title = market.get("title", "")
    yes_price = market.get("yes_price", 0)
    no_price = market.get("no_price", 0)
    end_date = market.get("end_date", "")
    volume_24h = market.get("volume_24h", 0)
    liquidity = market.get("liquidity", 0)

    edge = None

    # === Strategy 1: Near-expiry mispricing ===
    # Markets about to resolve often have stale prices
    if end_date:
        try:
            clean = end_date.strip().replace("Z", "+00:00")
            if len(clean) == 10:
                clean += "T23:59:59+00:00"
            end_ts = datetime.fromisoformat(clean).timestamp()
            hours_left = (end_ts - now_ts) / 3600.0

            # Market resolving in < 6 hours but price isn't near 0 or 1
            if 0 < hours_left < 6:
                if 0.15 < yes_price < 0.85:
                    edge_pct = max(yes_price, 1 - yes_price) * 100 - 50
                    if edge_pct > 10:
                        edge = (
                            edge_pct,
                            f"Resolves in {hours_left:.1f}h but price still "
                            f"uncertain ({yes_price:.0%}) — likely stale pricing"
                        )

            # Market resolving in < 24 hours with extreme price
            if 0 < hours_left < 24:
                if yes_price > 0.90 or no_price > 0.90:
                    # Already near-certain — check if the gap is big enough
                    gap = max(yes_price, no_price) - 0.90
                    if gap > 0.02 and liquidity > 5000:
                        edge = (
                            gap * 100,
                            f"Resolves in {hours_left:.0f}h, "
                            f"near-certain ({max(yes_price, no_price):.0%}) — "
                            f"time-decay bond opportunity"
                        )
        except (ValueError, TypeError):
            pass

    # === Strategy 2: Consensus divergence ===
    # If YES+NO is significantly < 1.00, there's embedded edge
    total = yes_price + no_price
    if 0.50 < total < 0.95 and liquidity > 10000:
        gap = 1.0 - total
        gap_pct = gap * 100
        if gap_pct > 5:
            # Only if we don't have a better edge already
            if edge is None or gap_pct > edge[0]:
                edge = (
                    gap_pct,
                    f"Market sum ${total:.3f} (gap: {gap_pct:.1f}%) — "
                    f"consensus underpricing with ${liquidity:,.0f} liquidity"
                )

    # === Strategy 3: Volume spike detection ===
    # High volume relative to liquidity suggests new information
    if liquidity > 0 and volume_24h > 0:
        vl_ratio = volume_24h / liquidity
        if vl_ratio > 8 and 0.20 < yes_price < 0.80:
            # Price in uncertain range + unusual volume = someone knows something
            info_edge = min(vl_ratio * 2, 30)
            if edge is None or info_edge > edge[0]:
                edge = (
                    info_edge,
                    f"Volume spike detected (Vol/Liq: {vl_ratio:.1f}x) — "
                    f"possible informed trading at ${yes_price:.2f}"
                )

    return edge


def find_data_arb_opportunities(
    markets: list[dict],
    cfg: dict,
) -> list:
    """
    Scan markets for external data arbitrage opportunities.

    Args:
        markets: List of market dicts from all platforms
        cfg: Config dict

    Returns:
        List of Opportunity objects
    """
    from cross_platform_scanner import Opportunity
    try:
        from scoring import compute_edge_score
    except ImportError:
        compute_edge_score = None

    data_arb_cfg = cfg.get("data_arb", {})
    min_delta = data_arb_cfg.get("min_delta", 10)
    enabled = data_arb_cfg.get("enabled", True)

    if not enabled:
        return []

    now_ts = time.time()
    opportunities = []

    for m in markets:
        if m.get("closed") or not m.get("active", True):
            continue

        result = _analyze_market_for_data_edge(m, now_ts)
        if result is None:
            continue

        edge_pct, explanation = result
        if edge_pct < min_delta:
            continue

        # Determine which side to bet on
        yes_price = m["yes_price"]
        no_price = m["no_price"]

        if yes_price > no_price:
            # Likely outcome is YES — buy YES if cheap enough, NO if hype
            side = "YES" if yes_price < 0.60 else "NO"
        else:
            side = "NO" if no_price < 0.60 else "YES"

        price = yes_price if side == "YES" else no_price
        profit = 1.0 - price
        roi = (profit / price) * 100 if price > 0 else 0

        opp = Opportunity(
            opp_type="data_arb",
            title=m["title"],
            description=(
                f"📊 <b>DATA-DRIVEN EDGE</b>\n"
                f"Buy {side} @ ${price:.4f} → pays $1.00\n"
                f"Edge: ~{edge_pct:.0f}%\n"
                f"\n"
                f"💡 <b>Signal:</b>\n"
                f"  {explanation}\n"
                f"\n"
                f"Platform: {m['platform'].title()}\n"
                f"Volume 24h: ${m.get('volume_24h', 0):,.0f}\n"
                f"Liquidity: ${m.get('liquidity', 0):,.0f}"
            ),
            profit_pct=round(roi, 2),
            profit_amount=round(profit * 100, 2),
            total_cost=round(price, 4),
            platforms=[m["platform"]],
            legs=[{"platform": m["platform"], "side": side, "price": price}],
            urls=[m.get("url", "")],
            risk_level="medium",
            hold_time=m.get("end_date", ""),
            category=m.get("category", ""),
        )

        if compute_edge_score:
            opp.edge_score = compute_edge_score(opp, market_data=m, cfg=cfg)

        opportunities.append(opp)
        logger.info(
            f"[DATA-ARB] {m['title'][:50]} | "
            f"Edge: {edge_pct:.0f}% | Side: {side} @ ${price:.2f}"
        )

    opportunities.sort(key=lambda o: o.profit_pct, reverse=True)
    opportunities = opportunities[:8]

    if opportunities:
        logger.info(f"Data Arb: found {len(opportunities)} data-driven opportunities")

    return opportunities
