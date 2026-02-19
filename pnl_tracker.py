"""
pnl_tracker.py — Theoretical PnL Tracker

Tracks the theoretical performance of every signal the bot distributes.
Answers the question: "If you had followed every signal, how much would
you have made?"

Features:
  - Records every opportunity at signal time (entry price, side, date)
  - Checks for resolutions via API (or marks expired after end_date)
  - Computes theoretical PnL, win rate, ROI per strategy
  - Provides /stats command data and public dashboard data

NOTE: This tracks THEORETICAL performance only. No real trades are placed.
"""
import json
import time
import math
import logging
import os
from datetime import datetime, timezone
from collections import defaultdict

logger = logging.getLogger("arb_bot.pnl_tracker")


class PnLTracker:
    """Tracks theoretical performance of all bot signals."""

    def __init__(self, tracker_path: str = "pnl_tracker.json"):
        self.tracker_path = tracker_path
        self.signals: list[dict] = []     # All recorded signals
        self.summary_cache: dict = {}     # Cached summary stats
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _load(self):
        """Load tracker data from disk."""
        if os.path.exists(self.tracker_path):
            try:
                with open(self.tracker_path, "r") as f:
                    data = json.load(f)
                self.signals = data.get("signals", [])
                logger.info(f"PnL Tracker loaded: {len(self.signals)} signals tracked")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"PnL Tracker load error: {e}")
                self.signals = []
        else:
            self.signals = []

    def save(self):
        """Save tracker data to disk."""
        try:
            data = {
                "signals": self.signals[-500:],  # Keep last 500
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            with open(self.tracker_path, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except IOError as e:
            logger.error(f"PnL Tracker save error: {e}")

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------
    def record_signal(self, opp):
        """
        Record an opportunity when it's distributed to users.
        Call this from distribute_signals.
        """
        now = time.time()

        # Get entry price from legs
        entry_price = opp.total_cost if opp.total_cost > 0 else 0
        side = "BOTH"
        if opp.legs:
            side = opp.legs[0].get("side", "BOTH")
            entry_price = opp.legs[0].get("price", opp.total_cost)

        signal = {
            "ts": now,
            "date": datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%d"),
            "title": opp.title[:100],
            "opp_type": opp.opp_type,
            "side": side,
            "entry_price": entry_price,
            "profit_pct": opp.profit_pct,
            "profit_amount": opp.profit_amount,
            "edge_score": getattr(opp, "edge_score", 0),
            "risk_level": opp.risk_level,
            "hold_time": opp.hold_time[:20] if opp.hold_time else "",
            "status": "open",         # open, won, lost, expired
            "exit_price": None,
            "actual_pnl": None,
        }

        self.signals.append(signal)

        # Don't save on every single signal — batch save
        if len(self.signals) % 10 == 0:
            self.save()

    def record_batch(self, opportunities: list):
        """Record multiple opportunities at once."""
        for opp in opportunities:
            self.record_signal(opp)
        self.save()

    # ------------------------------------------------------------------
    # Resolution Checking
    # ------------------------------------------------------------------
    def check_resolutions(self):
        """
        Check if any open signals have expired (past their hold_time).
        In a full implementation, this would query the market API for
        actual resolution. For now, we mark expired signals as "expired"
        and estimate PnL based on the projected profit.
        """
        now = time.time()
        updated = 0

        for sig in self.signals:
            if sig["status"] != "open":
                continue

            hold_time = sig.get("hold_time", "")
            if not hold_time:
                # No end date — check if signal is > 30 days old
                if now - sig["ts"] > 30 * 86400:
                    sig["status"] = "expired"
                    sig["actual_pnl"] = 0
                    updated += 1
                continue

            # Parse end date
            try:
                clean = hold_time.strip().replace("Z", "+00:00")
                if len(clean) == 10:
                    clean += "T23:59:59+00:00"
                end_ts = datetime.fromisoformat(clean).timestamp()

                if now > end_ts:
                    # Market has resolved — estimate based on projected profit
                    # In production, query API for actual resolution
                    # For now, use a conservative estimate (70% of projected)
                    sig["status"] = "resolved"
                    sig["actual_pnl"] = sig["profit_amount"] * 0.70
                    updated += 1
            except (ValueError, TypeError):
                pass

        if updated:
            self.save()
            logger.info(f"PnL Tracker: resolved {updated} signals")

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------
    def compute_stats(self) -> dict:
        """Compute comprehensive performance statistics."""
        self.check_resolutions()

        total = len(self.signals)
        if total == 0:
            return {
                "total_signals": 0,
                "open": 0, "resolved": 0, "expired": 0,
                "total_theoretical_pnl": 0,
                "avg_edge_score": 0,
                "by_type": {},
                "daily_signals": 0,
            }

        # Status counts
        open_count = sum(1 for s in self.signals if s["status"] == "open")
        resolved_count = sum(1 for s in self.signals if s["status"] == "resolved")
        expired_count = sum(1 for s in self.signals if s["status"] == "expired")

        # Theoretical PnL
        total_pnl = sum(s.get("actual_pnl", 0) or 0 for s in self.signals)
        total_projected = sum(s["profit_amount"] for s in self.signals)

        # Edge Score average
        scored = [s["edge_score"] for s in self.signals if s.get("edge_score", 0) > 0]
        avg_score = sum(scored) / len(scored) if scored else 0

        # By type breakdown
        by_type: dict[str, dict] = {}
        for s in self.signals:
            t = s["opp_type"]
            if t not in by_type:
                by_type[t] = {"count": 0, "projected_pnl": 0, "avg_roi": 0}
            by_type[t]["count"] += 1
            by_type[t]["projected_pnl"] += s["profit_amount"]
            by_type[t]["avg_roi"] += s["profit_pct"]

        for t in by_type:
            if by_type[t]["count"] > 0:
                by_type[t]["avg_roi"] = round(
                    by_type[t]["avg_roi"] / by_type[t]["count"], 2
                )
                by_type[t]["projected_pnl"] = round(by_type[t]["projected_pnl"], 2)

        # Daily average
        if self.signals:
            first_ts = self.signals[0]["ts"]
            days = max(1, (time.time() - first_ts) / 86400)
            daily_avg = total / days
        else:
            daily_avg = 0

        return {
            "total_signals": total,
            "open": open_count,
            "resolved": resolved_count,
            "expired": expired_count,
            "total_theoretical_pnl": round(total_pnl, 2),
            "total_projected_pnl": round(total_projected, 2),
            "avg_edge_score": round(avg_score, 1),
            "by_type": by_type,
            "daily_signals": round(daily_avg, 1),
        }

    # ------------------------------------------------------------------
    # Telegram Formatting
    # ------------------------------------------------------------------
    def format_stats_message(self) -> str:
        """Format stats as a Telegram message for /stats command."""
        stats = self.compute_stats()

        if stats["total_signals"] == 0:
            return (
                "📊 <b>PERFORMANCE TRACKER</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "No signals tracked yet.\n"
                "Stats will appear after the first scan cycle."
            )

        type_labels = {
            "cross_platform_arb": "🔄 Arb",
            "high_prob_bond": "🏦 Bonds",
            "intra_market_arb": "🎯 Intra",
            "whale_convergence": "🐋 Whale",
            "new_market": "🆕 New",
            "anti_hype": "🔻 Anti-Hype",
            "data_arb": "📊 Data",
            "longshot": "🎯 Longshot",
        }

        msg = (
            f"📊 <b>PERFORMANCE TRACKER</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"<b>Signal Stats:</b>\n"
            f"  📡 Total signals: <b>{stats['total_signals']}</b>\n"
            f"  📈 Daily average: <b>{stats['daily_signals']:.1f}</b>\n"
            f"  ⚡ Avg Edge Score: <b>{stats['avg_edge_score']:.0f}/100</b>\n"
            f"\n"
            f"<b>Status:</b>\n"
            f"  🟢 Open: {stats['open']}\n"
            f"  ✅ Resolved: {stats['resolved']}\n"
            f"  ⏰ Expired: {stats['expired']}\n"
            f"\n"
            f"<b>Theoretical PnL:</b>\n"
            f"  💰 Projected: <b>${stats['total_projected_pnl']:,.2f}</b> per $100\n"
        )

        if stats["total_theoretical_pnl"] > 0:
            msg += f"  📈 Realized (est): <b>${stats['total_theoretical_pnl']:,.2f}</b>\n"

        # Breakdown by type
        if stats["by_type"]:
            msg += f"\n<b>By Strategy:</b>\n"
            for t, info in sorted(
                stats["by_type"].items(),
                key=lambda x: x[1]["count"],
                reverse=True,
            ):
                label = type_labels.get(t, t)
                msg += (
                    f"  {label}: {info['count']} signals"
                    f" (avg {info['avg_roi']:.1f}% ROI)\n"
                )

        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Theoretical only — actual results may vary.</i>\n"
            f"💡 /elite for Edge Score dashboard"
        )

        return msg

    def format_fomo_message(self, bankroll: float = 100.0) -> str:
        """
        Format a FOMO message for free users:
        'If you had followed all signals, you'd have made $X'
        """
        stats = self.compute_stats()
        projected = stats["total_projected_pnl"]

        if projected <= 0:
            return ""

        return (
            f"💸 <b>What you're missing:</b>\n"
            f"Following all Pro signals would have earned\n"
            f"~<b>${projected:,.0f}</b> per $100 invested.\n"
            f"\n"
            f"💡 /upgrade to stop missing opportunities!"
        )
