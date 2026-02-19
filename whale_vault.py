"""
whale_vault.py — Persistent Whale Wallet Scoring & Intelligence

Tracks whale wallets across scan cycles and builds a performance database:
  - Win rate (30d, 90d)
  - Specialty categories (what markets they trade)
  - Consistency score (do they trade regularly or just once?)
  - Total PnL from tracked trades

This data feeds into the Edge Score for whale-based signals and
provides a "Smart Money" filter — only copy whales with proven track records.

Storage: JSON file (portable) with periodic compaction.
"""
import json
import time
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

logger = logging.getLogger("arb_bot.whale_vault")


class WhaleVault:
    """Persistent vault for whale wallet performance data."""

    def __init__(self, vault_path: str = "whale_vault.json"):
        self.vault_path = vault_path
        self.wallets: dict[str, dict] = {}
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _load(self):
        """Load wallet data from disk."""
        if os.path.exists(self.vault_path):
            try:
                with open(self.vault_path, "r") as f:
                    data = json.load(f)
                self.wallets = data.get("wallets", {})
                logger.info(f"Whale Vault loaded: {len(self.wallets)} wallets tracked")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Whale Vault load error: {e}")
                self.wallets = {}
        else:
            self.wallets = {}
            logger.info("Whale Vault: starting fresh (no existing data)")

    def save(self):
        """Save wallet data to disk."""
        try:
            data = {
                "wallets": self.wallets,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "total_wallets": len(self.wallets),
            }
            with open(self.vault_path, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except IOError as e:
            logger.error(f"Whale Vault save error: {e}")

    # ------------------------------------------------------------------
    # Wallet Tracking
    # ------------------------------------------------------------------
    def record_trade(self, trade: dict):
        """
        Record a whale trade and update wallet profile.

        trade dict should have: maker, title, side, value, price, timestamp
        """
        wallet = trade.get("maker", "")
        if not wallet or wallet == "unknown":
            return

        now = time.time()

        if wallet not in self.wallets:
            self.wallets[wallet] = {
                "first_seen": now,
                "total_trades": 0,
                "total_volume": 0.0,
                "trade_history": [],  # Last 50 trades
                "categories": {},     # Category → count
                "pseudonym": trade.get("pseudonym", ""),
                "win_count": 0,
                "loss_count": 0,
                "pending": [],        # Trades awaiting resolution
            }

        w = self.wallets[wallet]
        w["total_trades"] += 1
        w["total_volume"] += trade.get("value", 0)
        w["last_seen"] = now

        if trade.get("pseudonym"):
            w["pseudonym"] = trade["pseudonym"]

        # Track category specialty
        title = trade.get("title", "").lower()
        for cat_name in ["crypto", "politics", "sports", "tech", "finance", "entertainment"]:
            if cat_name in title:
                w["categories"][cat_name] = w["categories"].get(cat_name, 0) + 1

        # Store recent trade (keep last 50)
        trade_record = {
            "ts": now,
            "title": trade.get("title", "")[:80],
            "side": trade.get("side", ""),
            "price": trade.get("price", 0),
            "value": trade.get("value", 0),
        }
        w["trade_history"].append(trade_record)
        if len(w["trade_history"]) > 50:
            w["trade_history"] = w["trade_history"][-50:]

        # Add to pending for resolution tracking
        w["pending"].append({
            "ts": now,
            "title": trade.get("title", "")[:80],
            "side": trade.get("side", ""),
            "price": trade.get("price", 0),
        })
        if len(w["pending"]) > 20:
            w["pending"] = w["pending"][-20:]

    def record_trades_batch(self, trades: list[dict]):
        """Record multiple trades and save."""
        for t in trades:
            self.record_trade(t)
        if trades:
            self.save()

    # ------------------------------------------------------------------
    # Wallet Scoring
    # ------------------------------------------------------------------
    def score_wallet(self, wallet_address: str) -> dict:
        """
        Compute a score for a wallet based on its history.

        Returns dict with:
          - score: 0-100 overall score
          - total_trades: number of tracked trades
          - total_volume: USD volume
          - specialty: most-traded category
          - consistency: how regularly they trade (0-100)
          - is_smart_money: bool (score >= 75)
        """
        w = self.wallets.get(wallet_address)
        if not w:
            return {
                "score": 0, "total_trades": 0, "total_volume": 0,
                "specialty": "unknown", "consistency": 0, "is_smart_money": False,
            }

        now = time.time()
        total_trades = w.get("total_trades", 0)
        total_volume = w.get("total_volume", 0)
        first_seen = w.get("first_seen", now)
        last_seen = w.get("last_seen", now)
        categories = w.get("categories", {})
        wins = w.get("win_count", 0)
        losses = w.get("loss_count", 0)

        # --- Sub-scores ---
        # Volume score: More volume = more conviction
        # $1K → 20, $10K → 40, $100K → 60, $1M → 80
        import math
        vol_score = min(100, 15 * math.log(max(1, total_volume) / 500 + 1))

        # Trade frequency: More trades = more data = more reliable
        # 1 → 10, 5 → 30, 10 → 50, 50 → 80
        freq_score = min(100, 20 * math.log(max(1, total_trades) + 1))

        # Consistency: How long they've been active
        days_active = max(1, (now - first_seen) / 86400)
        recency = max(0, 1 - (now - last_seen) / (30 * 86400))  # Decay over 30 days
        consistency = min(100, days_active * 5) * recency

        # Win rate (if we have resolution data)
        total_resolved = wins + losses
        if total_resolved >= 5:
            win_rate = wins / total_resolved
            win_score = win_rate * 100
        else:
            win_score = 50  # Neutral if not enough data

        # Specialty
        specialty = "general"
        if categories:
            specialty = max(categories, key=categories.get)

        # Weighted overall
        score = (
            vol_score * 0.25
            + freq_score * 0.25
            + consistency * 0.20
            + win_score * 0.30
        )

        return {
            "score": round(score, 1),
            "total_trades": total_trades,
            "total_volume": round(total_volume, 2),
            "specialty": specialty,
            "consistency": round(consistency, 1),
            "is_smart_money": score >= 75,
            "pseudonym": w.get("pseudonym", ""),
            "win_rate": round(wins / total_resolved * 100, 1) if total_resolved >= 5 else None,
        }

    def get_top_wallets(self, n: int = 10) -> list[dict]:
        """Get the top N wallets by score."""
        scored = []
        for addr in self.wallets:
            info = self.score_wallet(addr)
            info["address"] = addr[:8] + "..." + addr[-4:] if len(addr) > 12 else addr
            info["full_address"] = addr
            scored.append(info)

        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored[:n]

    def get_smart_money_wallets(self) -> list[str]:
        """Get wallet addresses with score >= 75."""
        return [
            addr for addr in self.wallets
            if self.score_wallet(addr)["is_smart_money"]
        ]

    # ------------------------------------------------------------------
    # Telegram Formatting
    # ------------------------------------------------------------------
    def format_vault_summary(self) -> str:
        """Format a summary of the vault for Telegram display."""
        total = len(self.wallets)
        if total == 0:
            return (
                "🐋 <b>WHALE VAULT</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "No wallets tracked yet.\n"
                "Whale data builds as the bot scans."
            )

        top = self.get_top_wallets(5)
        smart_count = len(self.get_smart_money_wallets())
        total_volume = sum(w.get("total_volume", 0) for w in self.wallets.values())

        msg = (
            f"🐋 <b>WHALE VAULT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Wallets tracked: <b>{total}</b>\n"
            f"⭐ Smart Money (75+): <b>{smart_count}</b>\n"
            f"💰 Total volume: <b>${total_volume:,.0f}</b>\n"
            f"\n"
            f"🏆 <b>Top Wallets:</b>\n"
        )

        for i, w in enumerate(top, 1):
            name = w.get("pseudonym") or w["address"]
            emoji = "🔥" if w["score"] >= 85 else "⭐" if w["score"] >= 75 else "📊"
            msg += (
                f"  {i}. {emoji} <b>{name}</b>\n"
                f"     Score: {w['score']:.0f} | "
                f"Trades: {w['total_trades']} | "
                f"Vol: ${w['total_volume']:,.0f}\n"
            )

        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Smart Money wallets are auto-boosted in Edge Score."
        )
        return msg

    # ------------------------------------------------------------------
    # Compaction (cleanup old data)
    # ------------------------------------------------------------------
    def compact(self, max_age_days: int = 90):
        """Remove wallets not seen in max_age_days."""
        now = time.time()
        cutoff = now - (max_age_days * 86400)
        before = len(self.wallets)
        self.wallets = {
            addr: w for addr, w in self.wallets.items()
            if w.get("last_seen", 0) > cutoff
        }
        removed = before - len(self.wallets)
        if removed:
            logger.info(f"Whale Vault compacted: removed {removed} stale wallets")
            self.save()
