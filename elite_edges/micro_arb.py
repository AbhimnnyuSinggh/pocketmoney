"""
micro_arb.py — Microstructure Arb Scanner

Detects short-duration markets with thin order books where price inefficiencies
are more common. Signal-only — alerts user but does not execute.

Targets:
  - Markets resolving within 24 hours
  - Low liquidity (thin book = bigger price gaps)
  - Recent volume spikes (sudden interest = potential mispricing)
  - YES+NO sum significantly ≠ $1.00

Produces `micro_arb` type opportunities.
"""
import time
import logging
from datetime import datetime, timezone
from cross_platform_scanner import Opportunity

logger = logging.getLogger("arb_bot.micro_arb")


def find_micro_arb_opportunities(
    markets: list[dict], cfg: dict
) -> list[Opportunity]:
    """
    Scan for short-duration microstructure arbitrage opportunities.

    Args:
        markets: List of market dicts from Polymarket API
        cfg: Bot configuration

    Returns:
        List of Opportunity objects for micro arb signals
    """
    ma_cfg = cfg.get("micro_arb", {})
    if not ma_cfg.get("enabled", True):
        return []

    max_hours = ma_cfg.get("max_hours", 24)
    min_gap_pct = ma_cfg.get("min_gap_pct", 3.0)  # YES+NO gap from $1
    max_liquidity = ma_cfg.get("max_liquidity", 15000)  # Thin books only
    min_volume = ma_cfg.get("min_volume", 500)

    opportunities = []
    now = time.time()

    for market in markets:
        try:
            title = market.get("question", market.get("title", ""))
            end_date_str = market.get("end_date_iso", market.get("endDate", ""))
            volume = float(market.get("volume", 0))
            liquidity = float(market.get("liquidity", 0))

            if not title or not end_date_str:
                continue

            # Must be short-duration
            try:
                clean = end_date_str.strip().replace("Z", "+00:00")
                if len(clean) == 10:
                    clean += "T23:59:59+00:00"
                end_ts = datetime.fromisoformat(clean).timestamp()
            except (ValueError, TypeError):
                continue

            hours_left = (end_ts - now) / 3600
            if hours_left <= 0 or hours_left > max_hours:
                continue

            # Get prices
            tokens = market.get("tokens", [])
            if not tokens or len(tokens) < 2:
                continue

            yes_price = float(tokens[0].get("price", 0))
            no_price = float(tokens[1].get("price", 0))

            if yes_price <= 0 or no_price <= 0:
                continue

            # Check for microstructure gap
            price_sum = yes_price + no_price
            gap_pct = abs(1.0 - price_sum) * 100

            if gap_pct < min_gap_pct:
                continue

            # Prefer thin books (lower liquidity = more micro arb potential)
            if liquidity > max_liquidity:
                continue

            if volume < min_volume:
                continue

            # Calculate potential profit
            if price_sum < 1.0:
                # Buy both sides for less than $1 → guaranteed profit
                profit_pct = round((1.0 - price_sum) * 100, 2)
                strategy = f"Buy YES ({yes_price:.2f}) + NO ({no_price:.2f}) = {price_sum:.4f}"
            else:
                # Sell both sides for more than $1 → profit if you hold shares
                profit_pct = round((price_sum - 1.0) * 100, 2)
                strategy = f"Sum {price_sum:.4f} > $1.00 — sell pressure expected"

            event_slug = market.get("event_slug", market.get("slug", ""))
            url = f"https://polymarket.com/event/{event_slug}" if event_slug else ""

            opp = Opportunity(
                opp_type="micro_arb",
                title=title[:120],
                description=(
                    f"⚡ MICROSTRUCTURE ARB\n"
                    f"⏰ Resolves in {hours_left:.1f} hours\n"
                    f"💰 YES: {yes_price:.2f} | NO: {no_price:.2f}\n"
                    f"📊 Sum: {price_sum:.4f} (gap: {gap_pct:.1f}%)\n"
                    f"🔬 {strategy}\n"
                    f"💧 Liquidity: ${liquidity:,.0f} (thin book)\n"
                    f"📈 Volume: ${volume:,.0f}"
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
                risk_level="medium" if hours_left > 6 else "high",
                hold_time=end_date_str[:10] if end_date_str else "",
                category=market.get("category", ""),
            )
            opportunities.append(opp)

        except (ValueError, TypeError, KeyError) as e:
            logger.debug(f"Micro arb skip: {e}")
            continue

    if opportunities:
        logger.info(f"⚡ Micro Arb: {len(opportunities)} signals")

    return opportunities
