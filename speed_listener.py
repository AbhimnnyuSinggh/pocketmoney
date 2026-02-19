"""
speed_listener.py — Polygon Fast Polling (10s interval)

Uses Alchemy's eth_getLogs or Polymarket's Data API at 10-second intervals
to detect large trades and market events faster than the main 60s scan cycle.

This runs as a background thread and feeds "fast alerts" into the main pipeline.

NOTE: Uses Polymarket's public Data API (no Alchemy key required for basic mode).
If ALCHEMY_URL is configured, uses on-chain event logs for sub-10s detection.
"""
import os
import time
import logging
import threading
import requests
from datetime import datetime, timezone, timedelta
from collections import deque

logger = logging.getLogger("arb_bot.speed_listener")


class SpeedListener:
    """
    Fast-polling background listener for rapid trade detection.

    Two modes:
      1. Data API mode (default): Polls Polymarket's public trade API every 10s
      2. Alchemy mode: Polls eth_getLogs for on-chain CLOB events (if key provided)
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        sl_cfg = cfg.get("speed_listener", {})
        self.enabled = sl_cfg.get("enabled", True)
        self.poll_interval = sl_cfg.get("poll_interval", 10)  # seconds
        self.min_trade_value = sl_cfg.get("min_trade_value", 500)  # USD
        self.alchemy_url = os.environ.get("ALCHEMY_URL", sl_cfg.get("alchemy_url", ""))

        # Fast alerts queue — main loop reads from here (large trades)
        self.fast_alerts: deque = deque(maxlen=50)
        self._seen_trades: set = set()  # Dedup by trade hash
        self._seen_ttl: dict = {}       # trade_hash → timestamp

        # Fast NEW MARKET queue — separate from trades
        self.fast_new_markets: deque = deque(maxlen=100)
        self._new_market_poll_interval = sl_cfg.get("new_market_poll_interval", 8)  # seconds
        self._new_market_thread: threading.Thread | None = None

        self._running = False
        self._thread: threading.Thread | None = None
        self._last_poll_ts = 0

    # ------------------------------------------------------------------
    # Thread Management
    # ------------------------------------------------------------------
    def start(self):
        """Start the fast-polling background thread."""
        if not self.enabled:
            logger.info("Speed Listener disabled in config")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="speed-listener"
        )
        self._thread.start()

        # Start dedicated new-market fast-poll thread
        self._new_market_thread = threading.Thread(
            target=self._new_market_loop, daemon=True, name="fast-new-market"
        )
        self._new_market_thread.start()

        logger.info(
            f"⚡ Speed Listener started (trade interval={self.poll_interval}s, "
            f"new-market interval={self._new_market_poll_interval}s)"
        )

    def stop(self):
        """Stop the background polling thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        if self._new_market_thread:
            self._new_market_thread.join(timeout=5)
        logger.info("Speed Listener stopped")

    def get_fast_alerts(self) -> list[dict]:
        """Drain the fast trade alerts queue. Called from main scan cycle."""
        alerts = list(self.fast_alerts)
        self.fast_alerts.clear()
        return alerts

    def get_fast_new_markets(self):
        """Drain the fast new-market opps queue. Called from main loop."""
        opps = list(self.fast_new_markets)
        self.fast_new_markets.clear()
        return opps

    # ------------------------------------------------------------------
    # Main Poll Loop
    # ------------------------------------------------------------------
    def _poll_loop(self):
        """Background polling loop."""
        while self._running:
            try:
                self._poll_once()
            except Exception as e:
                logger.debug(f"Speed Listener poll error: {e}")
            time.sleep(self.poll_interval)

    def _poll_once(self):
        """Single poll iteration."""
        now = time.time()

        # Clean expired seen-trade entries (older than 10 minutes)
        cutoff = now - 600
        expired = [h for h, ts in self._seen_ttl.items() if ts < cutoff]
        for h in expired:
            self._seen_trades.discard(h)
            del self._seen_ttl[h]

        # Poll based on mode
        if self.alchemy_url:
            trades = self._poll_alchemy()
        else:
            trades = self._poll_data_api()

        if not trades:
            return

        new_count = 0
        for trade in trades:
            trade_hash = self._trade_hash(trade)
            if trade_hash in self._seen_trades:
                continue

            self._seen_trades.add(trade_hash)
            self._seen_ttl[trade_hash] = now

            # Add to fast alerts
            self.fast_alerts.append({
                "type": "fast_trade",
                "ts": now,
                "title": trade.get("title", "Unknown"),
                "side": trade.get("side", "BUY"),
                "value": trade.get("value", 0),
                "price": trade.get("price", 0),
                "maker": trade.get("maker", ""),
                "event_slug": trade.get("event_slug", ""),
            })
            new_count += 1

        if new_count > 0:
            logger.info(f"⚡ Fast: {new_count} new large trades detected")

        self._last_poll_ts = now

    # ------------------------------------------------------------------
    # Fast New Market Loop (8-second dedicated thread)
    # ------------------------------------------------------------------
    def _new_market_loop(self):
        """Dedicated background loop for fast new-market detection."""
        # Brief startup delay so the main scan seeds the cache first
        time.sleep(15)
        while self._running:
            try:
                self._poll_new_markets()
            except Exception as e:
                logger.debug(f"Fast new-market poll error: {e}")
            time.sleep(self._new_market_poll_interval)

    def _poll_new_markets(self):
        """
        Lightweight poll: fetch only the 50 NEWEST markets (sorted by startDate desc).
        Compares against the known_markets.json cache.
        If new markets found, creates Opportunity objects and queues them immediately.

        This achieves sub-10s new market detection vs the 60-90s from full pagination.
        """
        try:
            from new_market_sniper import (
                load_known_markets, save_known_markets, detect_new_markets
            )
        except ImportError:
            return

        new_markets_cfg = self.cfg.get("new_markets", {})
        if not new_markets_cfg.get("enabled", True):
            return

        cache_file = new_markets_cfg.get("cache_file", "known_markets.json")
        base_url = self.cfg.get("scanner", {}).get(
            "gamma_api_url", "https://gamma-api.polymarket.com"
        )
        import json as _json

        try:
            resp = requests.get(
                f"{base_url}/markets",
                params={
                    "limit": 50,          # Only 50 markets — tiny call
                    "closed": "false",
                    "active": "true",
                    "order": "startDate",
                    "ascending": "false",  # NEWEST FIRST
                },
                timeout=8,
            )
            resp.raise_for_status()
            raw = resp.json()
        except requests.RequestException as e:
            logger.debug(f"Fast new-market API error: {e}")
            return

        if not raw or not isinstance(raw, list):
            return

        # Normalise into sniper format
        current_markets = []
        for m in raw:
            try:
                prices_raw = m.get("outcomePrices", "[]")
                if isinstance(prices_raw, str):
                    prices = _json.loads(prices_raw)
                else:
                    prices = prices_raw or []
                yes_price = float(prices[0]) if prices else 0
                no_price = float(prices[1]) if len(prices) > 1 else 0
                events_list = m.get("events", [])
                event_slug = (
                    events_list[0].get("slug", m.get("slug", ""))
                    if events_list else m.get("slug", "")
                )
                current_markets.append({
                    "id": str(m.get("id", "")),
                    "title": m.get("question", ""),
                    "slug": m.get("slug", ""),
                    "event_slug": event_slug,
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "volume": float(m.get("volume", 0) or 0),
                    "volume_24h": float(m.get("volume24hr", 0) or 0),
                    "liquidity": float(m.get("liquidity", 0) or 0),
                    "end_date": m.get("endDate", ""),
                    "category": m.get("category", ""),
                    "url": f"https://polymarket.com/event/{event_slug}",
                    "created_at": m.get("createdAt", ""),
                    "start_date": m.get("startDate", ""),
                })
            except (ValueError, TypeError, IndexError):
                continue

        if not current_markets:
            return

        # Known IDs from cache
        known_ids = load_known_markets(cache_file)
        if not known_ids:
            # Cache not seeded yet — wait for the full scan cycle to do it
            logger.debug("Fast new-market: cache empty, waiting for full seed")
            return

        current_ids = {m["id"] for m in current_markets}
        new_ids = current_ids - known_ids

        if not new_ids:
            return

        # Build Opportunity objects for each new market
        new_market_data = [m for m in current_markets if m["id"] in new_ids]
        opps = detect_new_markets(new_market_data + [
            # Pass a marker so detect_new_markets doesn't re-save cache
            # We update the cache ourselves below
        ], self.cfg) if False else self._build_new_market_opps(new_market_data)

        # Update cache to include new IDs
        save_known_markets(cache_file, known_ids | current_ids)

        for opp in opps:
            self.fast_new_markets.append(opp)

        logger.info(
            f"🔥 FAST NEW MARKET: {len(opps)} new market(s) detected "
            f"in {self._new_market_poll_interval}s poll!"
        )

    @staticmethod
    def _build_new_market_opps(new_markets: list[dict]):
        """Convert raw market dicts to Opportunity objects."""
        from cross_platform_scanner import Opportunity
        opps = []
        for m in new_markets:
            yes_p = m["yes_price"]
            no_p = m["no_price"]
            spread = abs(yes_p - (1.0 - no_p))
            estimated_edge = max(spread * 100, 2.0)
            opps.append(Opportunity(
                opp_type="new_market",
                title=m["title"],
                description=(
                    f"🔥 BRAND NEW MARKET (fast-detected in <10s)!\n"
                    f"YES: {yes_p:.4f} | NO: {no_p:.4f}\n"
                    f"Liquidity: ${m['liquidity']:,.0f} | "
                    f"Volume: ${m['volume']:,.0f}\n"
                    f"Created: {m.get('created_at', '')[:19]}\n"
                    f"\n⚡ New markets are often mispriced — research fast!"
                ),
                profit_pct=round(estimated_edge, 2),
                profit_amount=round(estimated_edge, 2),
                total_cost=round(min(yes_p, no_p) if yes_p and no_p else 0.5, 4),
                platforms=["polymarket"],
                legs=[{"platform": "Polymarket", "side": "RESEARCH", "price": yes_p}],
                urls=[m["url"]],
                risk_level="medium",
                hold_time=m.get("end_date", "")[:10] if m.get("end_date") else "",
                category=m.get("category", ""),
            ))
        return opps

    # ------------------------------------------------------------------
    # Data API Polling (default — no key required)
    # ------------------------------------------------------------------
    def _poll_data_api(self) -> list[dict]:
        """Poll Polymarket's public Data API for recent large trades."""
        try:
            resp = requests.get(
                "https://data-api.polymarket.com/trades",
                params={"limit": 100},
                timeout=8,
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            raw_trades = resp.json()

            if not isinstance(raw_trades, list):
                raw_trades = raw_trades.get("data", raw_trades.get("trades", []))

            large_trades = []
            cutoff = time.time() - 30  # Only trades from last 30 seconds

            for t in raw_trades:
                try:
                    size = float(t.get("size", 0))
                    price = float(t.get("price", 0))
                    value = size * price

                    if value < self.min_trade_value:
                        continue

                    # Check timestamp freshness
                    ts = t.get("timestamp", 0)
                    if isinstance(ts, (int, float)) and ts > 0:
                        if ts < cutoff:
                            continue  # Old trade

                    large_trades.append({
                        "title": t.get("title", "Unknown Market"),
                        "side": t.get("side", "BUY").upper(),
                        "value": value,
                        "size": size,
                        "price": price,
                        "maker": t.get("proxyWallet", ""),
                        "event_slug": t.get("eventSlug", t.get("slug", "")),
                    })
                except (ValueError, TypeError):
                    continue

            return large_trades

        except requests.RequestException:
            return []

    # ------------------------------------------------------------------
    # Alchemy On-Chain Polling (advanced — requires ALCHEMY_URL)
    # ------------------------------------------------------------------
    def _poll_alchemy(self) -> list[dict]:
        """
        Poll Alchemy's eth_getLogs for Polymarket CLOB contract events.
        This detects trades at the on-chain level (sub-10s latency).
        Requires ALCHEMY_URL environment variable.
        """
        try:
            # Polymarket CTF Exchange on Polygon
            ctf_exchange = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"

            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getLogs",
                "params": [{
                    "fromBlock": "latest",
                    "address": ctf_exchange,
                    "topics": [
                        # OrderFilled event signature
                        "0x6869791f0a34781b29882982cc39e882768cf2c96995c2a110c577c53bc932d5"
                    ],
                }],
            }

            resp = requests.post(
                self.alchemy_url,
                json=payload,
                timeout=5,
            )
            resp.raise_for_status()
            result = resp.json()

            logs = result.get("result", [])
            if not logs:
                return []

            # Parse logs into trade-like dicts
            trades = []
            for log in logs:
                try:
                    # Extract trade value from log data
                    data = log.get("data", "0x")
                    if len(data) < 130:
                        continue

                    # Parse amount from log data (simplified)
                    amount_hex = data[2:66]
                    amount = int(amount_hex, 16) / 1e6  # USDC has 6 decimals

                    if amount < self.min_trade_value:
                        continue

                    trades.append({
                        "title": f"On-chain trade (block {log.get('blockNumber', '?')})",
                        "side": "BUY",
                        "value": amount,
                        "price": 0,
                        "size": amount,
                        "maker": log.get("topics", ["", ""])[1] if len(log.get("topics", [])) > 1 else "",
                        "event_slug": "",
                        "tx_hash": log.get("transactionHash", ""),
                    })
                except (ValueError, IndexError):
                    continue

            return trades

        except requests.RequestException:
            return []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _trade_hash(trade: dict) -> str:
        """Create a dedup hash for a trade."""
        return f"{trade.get('maker', '')[:10]}_{trade.get('title', '')[:20]}_{trade.get('value', 0):.0f}"

    def get_status(self) -> dict:
        """Get status info for /status command."""
        return {
            "running": self._running,
            "mode": "alchemy" if self.alchemy_url else "data_api",
            "interval": self.poll_interval,
            "alerts_queued": len(self.fast_alerts),
            "trades_seen": len(self._seen_trades),
        }
