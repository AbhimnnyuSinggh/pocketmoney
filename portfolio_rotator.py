"""
portfolio_rotator.py — Portfolio Allocation Suggestions

Analyzes current signal portfolio and recommends allocation percentages
based on Edge Scores, diversification, and risk management.

Does NOT auto-execute — provides suggestions via /portfolio command.

Features:
  - Signal-type diversification analysis
  - Category concentration warnings
  - Risk-adjusted allocation recommendations
  - Historical performance weighting
"""
import time
import logging
from collections import defaultdict
from cross_platform_scanner import Opportunity

logger = logging.getLogger("arb_bot.portfolio")


class PortfolioRotator:
    """Analyzes signals and recommends allocation strategy."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        pr_cfg = cfg.get("portfolio", {})
        self.total_bankroll = cfg.get("bankroll", {}).get("total_usdc", 1000)
        self.max_per_position = cfg.get("bankroll", {}).get("max_per_opportunity", 50)
        self.max_positions = cfg.get("bankroll", {}).get("max_open_positions", 10)

        # Risk weights by opp_type
        self.risk_weights = {
            "cross_platform_arb": 0.95,   # Lowest risk — guaranteed spread
            "intra_market_arb": 0.90,     # Low risk — price error
            "high_prob_bond": 0.85,       # Low risk — near-certain
            "spread_arb": 0.80,           # Medium-low
            "micro_arb": 0.70,            # Medium — short timeframe
            "data_arb": 0.65,             # Medium — requires analysis
            "resolution_intel": 0.60,     # Medium — research dependent
            "whale_convergence": 0.55,    # Medium — social signal
            "anti_hype": 0.50,            # Medium-high — contrarian
            "longshot": 0.30,             # High risk — asymmetric
            "new_market": 0.40,           # High risk — unproven
        }

    def analyze(self, opportunities: list[Opportunity]) -> dict:
        """
        Analyze a set of opportunities and return allocation recommendations.

        Returns dict with:
          - allocations: list of (opp, pct, amount) tuples
          - diversification: diversity score 0-100
          - warnings: list of warning strings
          - summary: formatted text
        """
        if not opportunities:
            return {
                "allocations": [],
                "diversification": 0,
                "warnings": ["No opportunities to analyze"],
                "summary": "No signals available for portfolio analysis.",
            }

        # Score and rank opportunities
        scored = []
        for opp in opportunities:
            try:
                from scoring import compute_edge_score
                edge = compute_edge_score(opp, self.cfg)
            except (ImportError, Exception):
                edge = 50.0

            risk_w = self.risk_weights.get(opp.opp_type, 0.50)
            # Combined score: edge * risk_weight
            combined = edge * risk_w
            scored.append((opp, edge, risk_w, combined))

        # Sort by combined score (highest first)
        scored.sort(key=lambda x: x[3], reverse=True)

        # Allocate based on scores (Kelly-lite: proportional to score)
        total_score = sum(s[3] for s in scored)
        if total_score == 0:
            total_score = 1

        allocations = []
        warnings = []
        remaining = self.total_bankroll

        # Cap at max_positions
        top_n = scored[:self.max_positions]

        for opp, edge, risk_w, combined in top_n:
            # Proportional allocation
            pct = (combined / total_score) * 100
            amount = min(
                round(self.total_bankroll * (pct / 100), 2),
                self.max_per_position,
                remaining,
            )
            if amount <= 0:
                continue
            remaining -= amount
            allocations.append({
                "opp": opp,
                "pct": round(pct, 1),
                "amount": amount,
                "edge": round(edge, 1),
                "risk": round(risk_w * 100),
            })

        # Diversification analysis
        type_counts = defaultdict(int)
        cat_counts = defaultdict(int)
        for a in allocations:
            type_counts[a["opp"].opp_type] += 1
            cat_counts[a["opp"].category or "unknown"] += 1

        # Diversity score: more types + more categories = higher score
        unique_types = len(type_counts)
        unique_cats = len(cat_counts)
        max_concentration = max(type_counts.values()) if type_counts else 0
        diversity = min(100, unique_types * 15 + unique_cats * 10 - max_concentration * 5)

        # Warnings
        if max_concentration > len(allocations) * 0.5:
            dominant = max(type_counts, key=type_counts.get)
            warnings.append(f"⚠️ Over-concentrated in {dominant} ({max_concentration}/{len(allocations)})")

        if unique_cats <= 1 and len(allocations) > 3:
            warnings.append("⚠️ All signals in same category — consider diversifying")

        longshot_pct = sum(a["pct"] for a in allocations if a["opp"].opp_type == "longshot")
        if longshot_pct > 25:
            warnings.append(f"⚠️ {longshot_pct:.0f}% in longshots — high risk exposure")

        return {
            "allocations": allocations,
            "diversification": max(0, diversity),
            "warnings": warnings,
            "summary": self._format_summary(allocations, diversity, warnings),
        }

    def _format_summary(self, allocations: list, diversity: int, warnings: list) -> str:
        """Format portfolio analysis for Telegram."""
        if not allocations:
            return (
                "📊 <b>PORTFOLIO ROTATOR</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "No signals to analyze yet."
            )

        total_invested = sum(a["amount"] for a in allocations)

        msg = (
            f"📊 <b>PORTFOLIO ROTATOR</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Bankroll: <b>${self.total_bankroll:,.0f}</b>\n"
            f"📈 Recommended positions: <b>{len(allocations)}</b>\n"
            f"🎯 Diversity: <b>{diversity}/100</b>\n\n"
            f"<b>Recommended Allocations:</b>\n"
        )

        type_emojis = {
            "cross_platform_arb": "🔄", "high_prob_bond": "🏦",
            "intra_market_arb": "🎯", "spread_arb": "📐",
            "micro_arb": "⚡", "data_arb": "📊",
            "resolution_intel": "🔍", "whale_convergence": "🐋",
            "anti_hype": "🔻", "longshot": "🎯", "new_market": "🆕",
        }

        for i, a in enumerate(allocations[:8], 1):
            emoji = type_emojis.get(a["opp"].opp_type, "📌")
            msg += (
                f"\n{i}. {emoji} <b>{a['opp'].title[:45]}</b>\n"
                f"   💵 ${a['amount']:.0f} ({a['pct']:.0f}%) "
                f"| Edge: {a['edge']:.0f} | Risk: {a['risk']}%\n"
            )

        if warnings:
            msg += "\n"
            for w in warnings:
                msg += f"{w}\n"

        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Suggestions only — not financial advice.</i>"
        )

        return msg
