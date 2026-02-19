"""
whale_tracker.py — Monitors large trades on Polymarket via the PUBLIC Data API
and detects "whale convergence" — multiple large wallets buying the same side
of a market within a short time window.

Signal: When 3+ big traders all buy the same side within 1 hour,
it's a strong directional signal worth following.

Data Source: https://data-api.polymarket.com/trades (no auth required)
"""
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from cross_platform_scanner import Opportunity

logger = logging.getLogger("arb_bot.whale_tracker")

# =========================================================================
# Data API — Fetch Recent Trades (PUBLIC, no auth required)
# =========================================================================

DATA_API_TRADES_URL = "https://data-api.polymarket.com/trades"


def fetch_recent_large_trades(cfg: dict) -> list[dict]:
    """
    Fetch recent trades from Polymarket's public Data API.
    Filters for trades above the minimum size threshold.
    Returns list of trade dicts with: market, side, size, maker, timestamp, etc.
    """
    whale_cfg = cfg.get("whales", {})
    min_size = whale_cfg.get("min_trade_size", 1000)
    lookback_min = whale_cfg.get("lookback_minutes", 120)

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_min)
    cutoff_ts = int(cutoff.timestamp())
    large_trades = []

    try:
        # The Data API returns trades in reverse chronological order
        # We fetch in batches and filter by size and timestamp
        params = {
            "limit": 1000,
        }
        resp = requests.get(
            DATA_API_TRADES_URL,
            params=params,
            timeout=20,
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        trades = resp.json()

        if not isinstance(trades, list):
            trades = trades.get("data", trades.get("trades", []))

        for trade in trades:
            try:
                size = float(trade.get("size", 0))
                price = float(trade.get("price", 0))
                trade_value = size * price  # Total USDC value

                if trade_value < min_size:
                    continue

                # Parse timestamp (unix seconds)
                ts = trade.get("timestamp", 0)
                if isinstance(ts, (int, float)):
                    trade_time = datetime.fromtimestamp(ts, tz=timezone.utc)
                else:
                    continue

                if trade_time < cutoff:
                    continue

                # Determine side
                side = trade.get("side", "BUY").upper()

                # Get wallet and market info directly from the Data API response
                wallet = trade.get("proxyWallet", "unknown")
                title = trade.get("title", "Unknown Market")
                event_slug = trade.get("eventSlug", trade.get("slug", ""))
                condition_id = trade.get("conditionId", "")
                outcome = trade.get("outcome", side)
                pseudonym = trade.get("pseudonym", "")

                large_trades.append({
                    "market_id": condition_id,
                    "title": title,
                    "event_slug": event_slug,
                    "url": f"https://polymarket.com/event/{event_slug}" if event_slug else "",
                    "side": side,
                    "outcome": outcome,
                    "size": size,
                    "price": price,
                    "value": trade_value,
                    "maker": wallet,
                    "pseudonym": pseudonym,
                    "timestamp": trade_time,
                })

            except (ValueError, TypeError, KeyError) as e:
                logger.debug(f"Skipping trade: {e}")
                continue

        logger.info(
            f"Fetched {len(large_trades)} large trades "
            f"(≥${min_size}) in last {lookback_min} min"
        )

    except requests.RequestException as e:
        logger.warning(f"Data API trades error: {e}")
    except Exception as e:
        logger.error(f"Whale tracker fetch error: {e}", exc_info=True)

    return large_trades


# =========================================================================
# Whale Convergence Detection
# =========================================================================

def detect_whale_convergence(
    trades: list[dict],
    cfg: dict,
) -> list[Opportunity]:
    """
    Detect "whale convergence": multiple distinct wallets buying the same
    side of the same market within a short time window.

    If N+ unique wallets all BUY (or all SELL) on the same market within
    the convergence window → strong directional signal.
    """
    whale_cfg = cfg.get("whales", {})
    convergence_count = whale_cfg.get("convergence_count", 3)
    window_min = whale_cfg.get("convergence_window_min", 60)

    if not trades:
        return []

    opportunities = []

    # Group trades by (market_id/conditionId, side)
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for t in trades:
        key = (t["market_id"], t["side"])
        grouped[key].append(t)

    for (market_id, side), market_trades in grouped.items():
        # Get unique wallets
        unique_wallets = set()
        total_value = 0
        avg_price = 0
        latest_time = None
        title = market_trades[0]["title"]
        url = market_trades[0]["url"]
        pseudonyms = []

        for t in market_trades:
            wallet = t["maker"]
            if wallet and wallet != "unknown":
                unique_wallets.add(wallet)
            total_value += t["value"]
            avg_price += t["price"]
            if latest_time is None or t["timestamp"] > latest_time:
                latest_time = t["timestamp"]
            if t.get("pseudonym"):
                pseudonyms.append(t["pseudonym"])

        avg_price = avg_price / len(market_trades) if market_trades else 0

        # Check time window — all trades within convergence_window_min
        if len(market_trades) >= 2:
            timestamps = [t["timestamp"] for t in market_trades]
            time_span = (max(timestamps) - min(timestamps)).total_seconds() / 60
            if time_span > window_min:
                continue

        # Check convergence threshold
        if len(unique_wallets) < convergence_count:
            continue

        # Build the outcome description
        outcome = market_trades[0].get("outcome", side)
        whale_names = ", ".join(pseudonyms[:5]) if pseudonyms else "Anonymous whales"

        opp = Opportunity(
            opp_type="whale_convergence",
            title=title,
            description=(
                f"🐋 {len(unique_wallets)} whales all {side} '{outcome}' "
                f"within {window_min} min!\n"
                f"Total whale volume: ${total_value:,.0f}\n"
                f"Avg price: ${avg_price:.4f}\n"
                f"Unique wallets: {len(unique_wallets)}\n"
                f"Trades: {len(market_trades)}\n"
                f"Traders: {whale_names}"
            ),
            profit_pct=round(
                (1.0 - avg_price) * 100 if side == "BUY" else avg_price * 100, 2
            ),
            profit_amount=round(total_value * 0.05, 2),  # Estimated 5% edge
            total_cost=round(avg_price, 4),
            platforms=["polymarket"],
            legs=[{
                "platform": "Polymarket",
                "side": f"Follow whales → {side} {outcome}",
                "price": avg_price,
            }],
            urls=[url],
            risk_level="medium",
            hold_time=(
                f"Signal at {latest_time.strftime('%H:%M UTC')}"
                if latest_time else ""
            ),
        )
        opportunities.append(opp)

        logger.info(
            f"[WHALE CONVERGENCE] {title[:50]} | "
            f"{len(unique_wallets)} whales {side} | "
            f"${total_value:,.0f} total"
        )

    # Sort by total whale volume (highest conviction first)
    opportunities.sort(key=lambda o: o.profit_amount, reverse=True)
    return opportunities


# =========================================================================
# Top-Level Entry Point
# =========================================================================

def find_whale_opportunities(cfg: dict) -> list[Opportunity]:
    """
    Main entry point called from the scan cycle.
    Fetches recent large trades and checks for whale convergence.
    """
    whale_cfg = cfg.get("whales", {})
    if not whale_cfg.get("enabled", True):
        return []

    logger.info("Scanning for whale convergence signals...")

    # Fetch large trades from public Data API
    trades = fetch_recent_large_trades(cfg)

    if not trades:
        logger.info("No large trades found in lookback window")
        return []

    # Detect convergence
    opportunities = detect_whale_convergence(trades, cfg)

    if opportunities:
        logger.info(f"Found {len(opportunities)} whale convergence signals!")
    else:
        logger.info("No whale convergence detected this cycle")

    return opportunities
