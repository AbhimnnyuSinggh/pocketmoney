"""
lp_orderbook.py — Order Book Reader for LP Market Making

Reads live order books via py-clob-client and calculates:
  - Midpoint price, spread, best bid/ask
  - Recommended placement price (2nd/3rd level, inside reward zone)
  - Whether current spread qualifies for reward farming

Designed for the LP Engine to call every 30s during active farming.
"""
import logging
from dataclasses import dataclass

logger = logging.getLogger("arb_bot.lp.orderbook")


@dataclass
class BookSnapshot:
    """Snapshot of an order book at a point in time."""
    midpoint: float
    spread: float
    best_bid: float
    best_ask: float
    recommended_buy_price: float   # Where to place BUY limit (2nd-3rd best bid)
    recommended_sell_price: float  # Where to place SELL limit (2nd-3rd best ask)
    within_reward_zone: bool       # spread < max_spread
    bids: list  # [{price, size}, ...]
    asks: list  # [{price, size}, ...]
    bid_depth: float  # Total USDC on bid side
    ask_depth: float  # Total USDC on ask side


def read_book(client, token_id: str, max_spread: float = 0.04) -> BookSnapshot | None:
    """
    Read the live order book for a given token and compute placement info.

    Args:
        client: py-clob-client ClobClient instance (or None for dry_run)
        token_id: The CLOB token ID for YES or NO side
        max_spread: Maximum spread for reward qualification (default ±4¢)

    Returns:
        BookSnapshot with all computed fields, or None on error
    """
    if client is None:
        # Dry run mode — return a synthetic snapshot
        return _synthetic_snapshot(max_spread)

    try:
        raw_book = client.get_order_book(token_id)
    except Exception as e:
        logger.error(f"Order book fetch error: {e}")
        return None

    # Parse bids and asks
    bids = []
    asks = []

    # py-clob-client returns book as dict with 'bids' and 'asks' lists
    raw_bids = raw_book.get("bids", []) if isinstance(raw_book, dict) else []
    raw_asks = raw_book.get("asks", []) if isinstance(raw_book, dict) else []

    for b in raw_bids:
        try:
            price = float(b.get("price", 0))
            size = float(b.get("size", 0))
            if price > 0 and size > 0:
                bids.append({"price": price, "size": size})
        except (ValueError, TypeError):
            continue

    for a in raw_asks:
        try:
            price = float(a.get("price", 0))
            size = float(a.get("size", 0))
            if price > 0 and size > 0:
                asks.append({"price": price, "size": size})
        except (ValueError, TypeError):
            continue

    # Sort: bids descending (highest first), asks ascending (lowest first)
    bids.sort(key=lambda x: x["price"], reverse=True)
    asks.sort(key=lambda x: x["price"])

    if not bids or not asks:
        logger.warning(f"Empty order book for token {token_id[:12]}...")
        return None

    best_bid = bids[0]["price"]
    best_ask = asks[0]["price"]
    midpoint = (best_bid + best_ask) / 2
    spread = best_ask - best_bid

    # Calculate depth
    bid_depth = sum(b["price"] * b["size"] for b in bids)
    ask_depth = sum(a["price"] * a["size"] for a in asks)

    # Recommended placement: 2nd or 3rd level (not 1st — avoids being first in queue)
    # For BUY: pick 2nd-3rd best bid price (or slightly above it)
    # For SELL: pick 2nd-3rd best ask price (or slightly below it)
    rec_buy = _pick_placement_price(bids, side="buy", midpoint=midpoint, max_spread=max_spread)
    rec_sell = _pick_placement_price(asks, side="sell", midpoint=midpoint, max_spread=max_spread)

    within_zone = spread <= max_spread * 2  # Total spread ≤ 2x max_spread

    snapshot = BookSnapshot(
        midpoint=round(midpoint, 4),
        spread=round(spread, 4),
        best_bid=round(best_bid, 4),
        best_ask=round(best_ask, 4),
        recommended_buy_price=round(rec_buy, 4),
        recommended_sell_price=round(rec_sell, 4),
        within_reward_zone=within_zone,
        bids=bids[:10],  # Keep top 10 levels
        asks=asks[:10],
        bid_depth=round(bid_depth, 2),
        ask_depth=round(ask_depth, 2),
    )

    logger.debug(
        f"Book: mid={snapshot.midpoint} spread={snapshot.spread} "
        f"bid_depth=${snapshot.bid_depth} ask_depth=${snapshot.ask_depth} "
        f"reward_zone={'✅' if within_zone else '❌'}"
    )

    return snapshot


def _pick_placement_price(
    levels: list[dict],
    side: str,
    midpoint: float,
    max_spread: float,
) -> float:
    """
    Pick optimal placement price from order book levels.

    Strategy: Place at 2nd or 3rd level, but always within the reward zone.
    - BUY: place just above the 2nd-3rd best bid (get filled before others)
    - SELL: place just below the 2nd-3rd best ask

    Ensures placement is within [midpoint - max_spread, midpoint + max_spread].
    """
    if not levels:
        return midpoint

    # Target: 2nd or 3rd level (index 1 or 2)
    target_idx = min(2, len(levels) - 1)  # 3rd level if available, else 2nd, else 1st
    target_price = levels[target_idx]["price"]

    if side == "buy":
        # Place slightly above target bid (win priority without being #1)
        price = target_price + 0.001
        # Clamp to reward zone: never above midpoint
        zone_max = midpoint - 0.005  # Stay below mid
        zone_min = midpoint - max_spread
        price = max(zone_min, min(zone_max, price))
    else:
        # Sell: place slightly below target ask
        price = target_price - 0.001
        zone_min = midpoint + 0.005
        zone_max = midpoint + max_spread
        price = max(zone_min, min(zone_max, price))

    return price


def _synthetic_snapshot(max_spread: float = 0.04) -> BookSnapshot:
    """Generate a synthetic snapshot for dry_run mode testing."""
    midpoint = 0.50
    half_spread = 0.01
    return BookSnapshot(
        midpoint=midpoint,
        spread=half_spread * 2,
        best_bid=midpoint - half_spread,
        best_ask=midpoint + half_spread,
        recommended_buy_price=midpoint - 0.015,
        recommended_sell_price=midpoint + 0.015,
        within_reward_zone=True,
        bids=[
            {"price": 0.49, "size": 500},
            {"price": 0.485, "size": 300},
            {"price": 0.48, "size": 200},
        ],
        asks=[
            {"price": 0.51, "size": 500},
            {"price": 0.515, "size": 300},
            {"price": 0.52, "size": 200},
        ],
        bid_depth=500.0,
        ask_depth=500.0,
    )


def format_book_display(snapshot: BookSnapshot) -> str:
    """Format order book snapshot for Telegram display."""
    if snapshot is None:
        return "📖 Order book unavailable"

    zone = "✅ In reward zone" if snapshot.within_reward_zone else "❌ Outside reward zone"
    msg = (
        f"📖 <b>Order Book</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Best Bid: ${snapshot.best_bid:.4f}\n"
        f"Best Ask: ${snapshot.best_ask:.4f}\n"
        f"Midpoint: ${snapshot.midpoint:.4f}\n"
        f"Spread: ${snapshot.spread:.4f} ({snapshot.spread*100:.1f}¢)\n"
        f"\n"
        f"📊 Depth: ${snapshot.bid_depth:,.0f} bid / ${snapshot.ask_depth:,.0f} ask\n"
        f"🎯 Rec. BUY @ ${snapshot.recommended_buy_price:.4f}\n"
        f"🎯 Rec. SELL @ ${snapshot.recommended_sell_price:.4f}\n"
        f"{zone}"
    )
    return msg
