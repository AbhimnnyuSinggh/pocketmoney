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

        # Fast alerts queue — main loop reads from here
        self.fast_alerts: deque = deque(maxlen=50)
        self._seen_trades: set = set()  # Dedup by trade hash
        self._seen_ttl: dict = {}       # trade_hash → timestamp
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
        logger.info(
            f"⚡ Speed Listener started (interval={self.poll_interval}s, "
            f"min_value=${self.min_trade_value})"
        )

    def stop(self):
        """Stop the background polling thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Speed Listener stopped")

    def get_fast_alerts(self) -> list[dict]:
        """Drain the fast alerts queue. Called from main scan cycle."""
        alerts = list(self.fast_alerts)
        self.fast_alerts.clear()
        return alerts

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
