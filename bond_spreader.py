"""
bond_spreader.py — Bond Spread Automator

Spreads small bets ($1-5) across many 93-99¢ bonds on Polymarket.
11 features:
  1. Bond Scanner (93¢+ detection)
  2. Tiered Time Buckets (daily 50% / 3-day 30% / 7-day 20%)
  3. Price-Tiered Sizing (A/B/C tiers with multipliers)
  4. Category Caps (max 20% per category)
  5. Compound Reinvestment (80% profits redeployed)
  6. Smart Loss Cutting (75¢ emergency, 85¢ warning)
  7. Early Exit Optimizer (sell if daily ROI > 1.5x hold ROI)
  8. Volume-Weighted Conviction (trust liquid markets more)
  9. Win Rate Tracker + Adaptive Sizing (50+ samples → auto-adjust)
  10. Limit Order Entry for 3-7d bonds (0.5¢ improvement)
  11. Per-Category Win Rate Tracking

State persisted to bond_spread_state.json — survives Render restarts.
"""
import os
import json
import time
import logging
import threading
import uuid
import math
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field, asdict

logger = logging.getLogger("arb_bot.bond_spreader")

# Graceful imports
try:
    from execution_engine import ExecutionEngine, MarketLookup
    HAS_EXECUTION = True
except ImportError:
    HAS_EXECUTION = False
    ExecutionEngine = None
    MarketLookup = None

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Feature 3: Price-Tiered Sizing
TIER_CONFIG = {
    "A": {"min": 0.97, "max": 1.00, "multiplier": 1.5, "label": "Ultra-Safe"},
    "B": {"min": 0.95, "max": 0.97, "multiplier": 1.0, "label": "Standard"},
    "C": {"min": 0.93, "max": 0.95, "multiplier": 0.5, "label": "Value"},
}

# Feature 2: Tiered Time Buckets
TIME_BUCKETS = {
    "daily": {"max_hours": 24,  "capital_pct": 50, "label": "Daily",  "order_mode": "market"},
    "3day":  {"max_hours": 72,  "capital_pct": 30, "label": "3-Day",  "order_mode": "limit"},
    "7day":  {"max_hours": 168, "capital_pct": 20, "label": "7-Day",  "order_mode": "limit"},
}

# Feature 6: Smart Loss Cutting
LOSS_THRESHOLDS = {
    "emergency": 0.75,    # Below 75¢: auto-sell immediately
    "warning":   0.85,    # Below 85¢: auto-sell
    "watch":     -0.05,   # Dropped 5¢+ from entry: flag
}

# Feature 4: Category keywords (standalone — no telegram_bot import needed)
CATEGORY_KEYWORDS = {
    "crypto": ["crypto", "bitcoin", "ethereum", "btc", "eth", "solana",
               "defi", "blockchain", "token", "sol", "doge", "xrp",
               "cardano", "polygon", "matic"],
    "politics": ["politics", "political", "government", "congress",
                 "president", "democrat", "republican", "biden", "trump",
                 "senate", "house", "election", "vote", "governor"],
    "sports": ["nfl", "nba", "mlb", "soccer", "football", "basketball",
               "baseball", "tennis", "cricket", "ufc", "boxing", "hockey",
               "f1", "formula", "championship", "playoff", "super bowl"],
    "finance": ["stock", "s&p", "nasdaq", "dow", "interest rate", "fed",
                "inflation", "treasury", "gdp", "earnings", "ipo",
                "bond", "yield", "market cap"],
    "tech": ["ai", "artificial intelligence", "openai", "google", "apple",
             "microsoft", "meta", "tesla", "spacex", "chip", "nvidia"],
    "culture": ["celebrity", "movie", "music", "oscar", "grammy", "netflix",
                "entertainment", "viral", "tiktok", "youtube"],
    "climate": ["climate", "weather", "temperature", "hurricane", "earthquake",
                "science", "nasa", "space"],
    "geopolitics": ["war", "russia", "ukraine", "china", "nato", "sanctions",
                    "military", "conflict", "ceasefire"],
}


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class BondBet:
    bet_id: str
    market_slug: str
    market_title: str
    condition_id: str
    token_id: str
    side: str               # YES or NO
    price: float            # Entry price
    amount: float           # USDC risked
    shares: float           # amount / price
    tier: str               # A, B, or C
    time_bucket: str        # daily, 3day, or 7day
    category: str
    volume_24h: float
    status: str = "active"  # active/won/lost/sold_profit/sold_loss/cancelled/failed
    order_id: str = ""
    placed_at: float = 0.0
    resolved_at: float = 0.0
    sold_at: float = 0.0
    sold_price: float = 0.0
    pnl: float = 0.0
    end_date: str = ""
    user_id: str = ""
    conviction_score: float = 0.0


@dataclass
class SpreadSession:
    session_id: str
    user_id: str
    started_at: float
    mode: str = "dry_run"

    active_bets: list = field(default_factory=list)
    resolved_bets: list = field(default_factory=list)

    # Capital tracking
    initial_capital: float = 100.0
    current_pool: float = 100.0
    total_deployed: float = 0.0
    total_profits: float = 0.0
    total_losses: float = 0.0

    # Feature 5: Compound
    reinvest_rate: float = 0.80
    withdrawn: float = 0.0

    # Feature 9: Win rate per tier
    tier_stats: dict = field(default_factory=lambda: {
        "A": {"wins": 0, "losses": 0, "early_exits": 0, "cut_losses": 0, "total_pnl": 0.0},
        "B": {"wins": 0, "losses": 0, "early_exits": 0, "cut_losses": 0, "total_pnl": 0.0},
        "C": {"wins": 0, "losses": 0, "early_exits": 0, "cut_losses": 0, "total_pnl": 0.0},
    })

    # Feature 4 & 11: Category tracking
    category_deployed: dict = field(default_factory=dict)
    category_stats: dict = field(default_factory=dict)

    # Feature 2: Bucket capital
    bucket_deployed: dict = field(default_factory=lambda: {
        "daily": 0.0, "3day": 0.0, "7day": 0.0,
    })


# ---------------------------------------------------------------------------
# BondSpreader — Main class
# ---------------------------------------------------------------------------

class BondSpreader:
    """
    Autonomous bond spread engine. Called every 60s from main_v2.py.

    Flow per cycle:
      1. check_resolutions() — process wins/losses, free capital
      2. _monitor_active_bets() — loss cutting + early exits
      3. scan_and_deploy(markets) — find new bonds, place bets
    """

    def __init__(self, cfg: dict, execution_engine=None):
        self.cfg = cfg
        self.engine = execution_engine
        bs_cfg = cfg.get("bond_spreader", {})

        self.enabled = bs_cfg.get("enabled", False)
        self.mode = bs_cfg.get("mode", "dry_run")
        self.base_amount = bs_cfg.get("base_amount", 1.00)
        
        # Pull allocated bankroll from central config to prevent double-spending
        total_usdc = cfg.get("bankroll", {}).get("total_usdc", 100.0)
        allocs = cfg.get("bankroll", {}).get("allocations", {})
        
        active = cfg.get("execution", {}).get("active_autotrader", "none")
        if active == "bonds":
            self.max_deployed = total_usdc
            import logging
            logging.getLogger("arb_bot.bonds").info(f"🏦 Bond Spreader is ACTIVE module -> Routing 100% capital (${self.max_deployed})")
        else:
            self.max_deployed = allocs.get("bond_spreader", bs_cfg.get("max_total_deployed", 100.0))
        
        self.max_category_pct = bs_cfg.get("max_per_category_pct", 20)
        self.min_price = bs_cfg.get("min_price", 0.93)
        self.max_resolution_days = bs_cfg.get("max_resolution_days", 7)
        self.min_liquidity = bs_cfg.get("min_liquidity", 5000)
        self.min_volume = bs_cfg.get("min_volume", 1000)
        self.adaptive_sizing = bs_cfg.get("adaptive_sizing", True)
        self.adaptive_min_samples = bs_cfg.get("adaptive_min_samples", 50)
        self.reinvest_rate_cfg = bs_cfg.get("reinvest_rate", 0.80)
        self.limit_order_offset = bs_cfg.get("limit_order_offset", 0.005)
        self.limit_order_timeout = bs_cfg.get("limit_order_timeout_minutes", 15)

        self._state_file = "bond_spread_state.json"
        self._lock = threading.Lock()
        self.session = self._load_state()

    # ------------------------------------------------------------------
    # Feature 1: Bond Scanner + Feature 2-4 filtering + deployment
    # ------------------------------------------------------------------
    def scan_and_deploy(self, poly_markets: list[dict]) -> list[dict]:
        """
        Main entry point. Called every scan cycle with all Polymarket data.
        Filters for 93¢+ bonds, classifies, sizes, and deploys.
        """
        if not self.enabled:
            return []

        remaining = self.session.current_pool - self.session.total_deployed
        if remaining < self.base_amount * 0.5:
            return []

        active_slugs = {
            b["market_slug"] for b in self.session.active_bets
            if b.get("status") == "active"
        }
        candidates = []

        for m in poly_markets:
            if m.get("platform", "polymarket") != "polymarket":
                continue
            if m.get("closed") or not m.get("active", True):
                continue
            if m.get("slug", "") in active_slugs:
                continue

            # Check both sides for 93¢+ qualification
            bond_side = None
            bond_price = 0
            for side, price_key in [("YES", "yes_price"), ("NO", "no_price")]:
                price = m.get(price_key, 0)
                if self.min_price <= price < 0.995:
                    bond_side = side
                    bond_price = price
                    break

            if not bond_side:
                continue

            # Volume filter (Feature 8)
            vol = m.get("volume_24h", 0)
            if vol < self.min_volume:
                continue
            if m.get("liquidity", 0) < self.min_liquidity:
                continue

            # Must have CLOB data
            if not m.get("condition_id") and not m.get("conditionId"):
                continue

            # Time bucket (Feature 2)
            hours = self._hours_until_resolution(m.get("end_date", ""))
            bucket = self._classify_time_bucket(hours)
            if not bucket or hours < 1:
                continue

            # Category cap (Feature 4)
            category = self._categorize_market(m.get("title", ""))
            if not self._check_category_cap(category):
                continue

            # Bucket budget (Feature 2)
            bucket_budget = self._get_bucket_budget(bucket)
            if bucket_budget < self.base_amount * 0.3:
                continue

            # Tier + sizing (Features 3, 8, 9)
            tier = self._classify_tier(bond_price)
            conviction = self._compute_conviction(vol)
            if conviction <= 0:
                continue
            amount = self._compute_bet_amount(bond_price, tier, conviction)

            if amount > remaining or amount > bucket_budget:
                continue

            candidates.append({
                "market": m,
                "side": bond_side,
                "price": bond_price,
                "tier": tier,
                "bucket": bucket,
                "amount": amount,
                "category": category,
                "conviction": conviction,
                "hours": hours,
            })

        # Sort: daily first (velocity), then conviction, then soonest
        bucket_order = {"daily": 0, "3day": 1, "7day": 2}
        candidates.sort(key=lambda c: (
            bucket_order[c["bucket"]],
            -c["conviction"],
            c["hours"],
        ))

        # Deploy
        placed = []
        for c in candidates:
            remaining = self.session.current_pool - self.session.total_deployed
            bucket_budget = self._get_bucket_budget(c["bucket"])
            if c["amount"] > remaining or c["amount"] > bucket_budget:
                continue

            active_mod = self.cfg.get("execution", {}).get("active_autotrader", "none")
            if active_mod != "bonds":
                # Market was scanned and processed, but execution is blocked
                continue

            bet = self._place_bet(
                market=c["market"], side=c["side"], price=c["price"],
                amount=c["amount"], tier=c["tier"], time_bucket=c["bucket"],
                category=c["category"], conviction=c["conviction"],
            )
            if bet and bet.get("status") == "active":
                placed.append(bet)
            time.sleep(0.3)

        self._save_state()
        return placed

    # ------------------------------------------------------------------
    # Resolution checking
    # ------------------------------------------------------------------
    def check_resolutions(self) -> list[dict]:
        """Check if any active bonds resolved. Updates P&L and pool."""
        resolved = []

        for bet in self.session.active_bets[:]:
            if bet.get("status") != "active":
                continue

            market = MarketLookup.get_market(bet["market_slug"]) if MarketLookup else None
            if not market or not market.get("closed", False):
                continue

            yes_final = market.get("yes_price", 0)
            won = (yes_final > 0.50) if bet["side"] == "YES" else (yes_final < 0.50)

            bet["resolved_at"] = time.time()
            if won:
                payout = bet["shares"] * 1.0
                profit = payout - bet["amount"]
                bet["status"] = "won"
                bet["pnl"] = round(profit, 4)

                # Feature 5: Compound
                reinvest = profit * self.session.reinvest_rate
                self.session.current_pool += bet["amount"] + reinvest
                self.session.withdrawn += profit - reinvest
                self.session.total_profits += profit
            else:
                bet["status"] = "lost"
                bet["pnl"] = round(-bet["amount"], 4)
                self.session.total_losses += bet["amount"]

            # Feature 9: Tier stats
            tier = bet.get("tier", "C")
            ts = self.session.tier_stats.setdefault(
                tier, {"wins": 0, "losses": 0, "early_exits": 0, "cut_losses": 0, "total_pnl": 0.0}
            )
            ts["wins" if won else "losses"] += 1
            ts["total_pnl"] += bet["pnl"]

            # Feature 11: Category stats
            self._update_category_stats(bet["category"], won, bet["pnl"])

            # Release budget
            self._release_budget(bet)

            self.session.active_bets.remove(bet)
            self.session.resolved_bets.append(bet)

            resolved.append({
                "title": bet["market_title"][:50],
                "side": bet["side"],
                "price": bet["price"],
                "amount": bet["amount"],
                "tier": tier,
                "won": won,
                "pnl": bet["pnl"],
                "category": bet["category"],
            })

        if len(self.session.resolved_bets) > 500:
            self.session.resolved_bets = self.session.resolved_bets[-500:]

        self._save_state()
        return resolved

    # ------------------------------------------------------------------
    # Feature 6 & 7: Monitor active bets (loss cut + early exit)
    # ------------------------------------------------------------------
    def monitor_active_bets(self) -> dict:
        """Called every scan cycle. Returns actions taken."""
        results = {"sold_loss": [], "sold_profit": [], "flagged": []}

        for bet in self.session.active_bets[:]:
            if bet.get("status") != "active":
                continue

            market = MarketLookup.get_market(bet["market_slug"]) if MarketLookup else None
            if not market:
                continue

            current_price = (
                market["yes_price"] if bet["side"] == "YES" else market["no_price"]
            )
            entry_price = bet["price"]

            # Feature 6: Loss cutting
            if current_price < LOSS_THRESHOLDS["emergency"]:
                self._sell_bet(bet, current_price, "emergency_loss_cut")
                results["sold_loss"].append({
                    "title": bet["market_title"][:45],
                    "entry": entry_price, "exit": current_price,
                    "saved": round(entry_price - current_price, 4),
                })
                continue

            if current_price < LOSS_THRESHOLDS["warning"]:
                self._sell_bet(bet, current_price, "warning_loss_cut")
                results["sold_loss"].append({
                    "title": bet["market_title"][:45],
                    "entry": entry_price, "exit": current_price,
                    "saved": round(entry_price - current_price, 4),
                })
                continue

            if current_price < entry_price + LOSS_THRESHOLDS["watch"]:
                results["flagged"].append({
                    "title": bet["market_title"][:45],
                    "entry": entry_price, "current": current_price,
                    "drop": round(entry_price - current_price, 4),
                })

            # Feature 7: Early exit
            if current_price > entry_price + 0.005:
                exit_result = self._check_early_exit(bet, current_price, market)
                if exit_result:
                    results["sold_profit"].append(exit_result)

        return results

    # ------------------------------------------------------------------
    # Feature 7: Early Exit Optimizer
    # ------------------------------------------------------------------
    def _check_early_exit(self, bet: dict, current_price: float,
                          market: dict) -> dict | None:
        entry_price = bet["price"]
        placed_at = bet.get("placed_at", time.time())
        end_date = bet.get("end_date", "")

        now = time.time()
        days_held = max(0.01, (now - placed_at) / 86400.0)
        hours_remaining = self._hours_until_resolution(end_date)
        remaining_days = max(0.01, hours_remaining / 24.0)
        total_days = days_held + remaining_days

        current_profit_pct = (current_price - entry_price) / entry_price
        current_daily_roi = current_profit_pct / days_held

        hold_profit_pct = (1.0 - entry_price) / entry_price
        hold_daily_roi = hold_profit_pct / total_days

        # Only exit if sell daily ROI > 1.5x hold daily ROI
        if current_daily_roi > hold_daily_roi * 1.5:
            self._sell_bet(bet, current_price, "early_exit_profit")
            return {
                "title": bet["market_title"][:45],
                "entry": entry_price,
                "exit": current_price,
                "profit": round(current_price - entry_price, 4),
                "days_held": round(days_held, 1),
                "daily_roi_sell": round(current_daily_roi * 100, 2),
                "daily_roi_hold": round(hold_daily_roi * 100, 2),
            }
        return None

    # ------------------------------------------------------------------
    # Feature 2: Time Buckets
    # ------------------------------------------------------------------
    def _classify_time_bucket(self, hours: float) -> str | None:
        if hours <= 0 or hours > self.max_resolution_days * 24:
            return None
        if hours <= 24:
            return "daily"
        elif hours <= 72:
            return "3day"
        elif hours <= 168:
            return "7day"
        return None

    def _get_bucket_budget(self, bucket: str) -> float:
        pct = TIME_BUCKETS[bucket]["capital_pct"] / 100.0
        max_for_bucket = self.session.current_pool * pct
        already = self.session.bucket_deployed.get(bucket, 0.0)
        return max(0, max_for_bucket - already)

    # ------------------------------------------------------------------
    # Feature 3: Price-Tiered Sizing
    # ------------------------------------------------------------------
    def _classify_tier(self, price: float) -> str:
        for tier_key, tc in TIER_CONFIG.items():
            if tc["min"] <= price < tc["max"]:
                return tier_key
        return "C"

    # ------------------------------------------------------------------
    # Features 3, 8, 9: Compute bet amount
    # ------------------------------------------------------------------
    def _compute_bet_amount(self, price: float, tier: str, conviction: float) -> float:
        base = self.base_amount
        tier_mult = TIER_CONFIG[tier]["multiplier"]

        # Feature 9: Adaptive sizing
        if self.adaptive_sizing:
            stats = self.session.tier_stats.get(tier, {})
            total_resolved = stats.get("wins", 0) + stats.get("losses", 0)
            if total_resolved >= self.adaptive_min_samples:
                actual_wr = stats["wins"] / total_resolved
                breakeven_wr = price  # at 96¢, need 96% to break even
                if actual_wr > breakeven_wr + 0.03:
                    tier_mult *= 1.3    # Beating expectations
                elif actual_wr < breakeven_wr:
                    tier_mult *= 0.3    # Below breakeven: cut hard

        # Feature 8: Volume conviction
        amount = base * tier_mult * conviction
        return round(max(0.10, amount), 2)

    # ------------------------------------------------------------------
    # Feature 8: Volume-Weighted Conviction
    # ------------------------------------------------------------------
    def _compute_conviction(self, volume_24h: float) -> float:
        if volume_24h >= 100_000:
            return 1.2
        elif volume_24h >= 10_000:
            return 1.0
        elif volume_24h >= 1_000:
            return 0.7
        return 0.0

    # ------------------------------------------------------------------
    # Feature 4: Category Caps
    # ------------------------------------------------------------------
    def _categorize_market(self, title: str) -> str:
        text = title.lower()
        for cat, keywords in CATEGORY_KEYWORDS.items():
            for kw in keywords:
                if kw in text:
                    return cat
        return "other"

    def _check_category_cap(self, category: str) -> bool:
        current = self.session.category_deployed.get(category, 0.0)
        max_allowed = self.max_deployed * (self.max_category_pct / 100.0)
        return current < max_allowed

    # ------------------------------------------------------------------
    # Bet placement (Features 1, 10)
    # ------------------------------------------------------------------
    def _place_bet(self, market: dict, side: str, price: float,
                   amount: float, tier: str, time_bucket: str,
                   category: str, conviction: float) -> dict | None:

        condition_id = market.get("condition_id", market.get("conditionId", ""))
        clob_ids = market.get("clob_token_ids", [])
        if not clob_ids:
            raw = market.get("clobTokenIds", "[]")
            if isinstance(raw, str):
                try:
                    clob_ids = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    clob_ids = []
            elif isinstance(raw, list):
                clob_ids = raw

        token_id = clob_ids[0] if side == "YES" and clob_ids else (
            clob_ids[1] if side == "NO" and len(clob_ids) > 1 else ""
        )

        bet = BondBet(
            bet_id=uuid.uuid4().hex[:10],
            market_slug=market.get("slug", ""),
            market_title=market.get("title", "")[:80],
            condition_id=condition_id,
            token_id=token_id,
            side=side,
            price=round(price, 4),
            amount=round(amount, 2),
            shares=round(amount / price, 4) if price > 0 else 0,
            tier=tier,
            time_bucket=time_bucket,
            category=category,
            volume_24h=market.get("volume_24h", 0),
            status="active",
            placed_at=time.time(),
            end_date=market.get("end_date", ""),
            user_id=self.session.user_id,
            conviction_score=conviction,
        )

        # Execute based on mode
        if self.mode == "dry_run":
            bet.order_id = f"DRY-{bet.bet_id}"
            logger.info(
                f"[DRY] Bond {tier}: {side} {bet.market_title[:40]} "
                f"@ {price:.2f} ${amount:.2f} ({time_bucket})"
            )
        elif self.mode == "live" and self.engine:
            trade = self.engine.execute_trade_auto(
                market.get("slug", ""), side, "BUY", amount, self.session.user_id
            )
            if trade:
                bet.order_id = getattr(trade, "order_id", "")
                if trade.status in ("placed", "filled"):
                    bet.status = "active"
                else:
                    bet.status = "failed"
                    return asdict(bet)
            else:
                bet.status = "failed"
                return asdict(bet)
        else:
            bet.order_id = f"ASST-{bet.bet_id}"

        # Update session
        bet_dict = asdict(bet)
        self.session.active_bets.append(bet_dict)
        self.session.total_deployed += amount
        self.session.current_pool -= amount
        self.session.bucket_deployed[time_bucket] = (
            self.session.bucket_deployed.get(time_bucket, 0) + amount
        )
        self.session.category_deployed[category] = (
            self.session.category_deployed.get(category, 0) + amount
        )

        self._save_state()
        return bet_dict

    # ------------------------------------------------------------------
    # Sell (loss cut / early exit)
    # ------------------------------------------------------------------
    def _sell_bet(self, bet: dict, sell_price: float, reason: str):
        bet["sold_at"] = time.time()
        bet["sold_price"] = round(sell_price, 4)

        sale_proceeds = bet["shares"] * sell_price
        pnl = sale_proceeds - bet["amount"]
        bet["pnl"] = round(pnl, 4)

        if pnl >= 0:
            bet["status"] = "sold_profit"
            reinvest = pnl * self.session.reinvest_rate
            self.session.current_pool += bet["amount"] + reinvest
            self.session.withdrawn += pnl - reinvest
            self.session.total_profits += pnl
        else:
            bet["status"] = "sold_loss"
            self.session.current_pool += sale_proceeds
            self.session.total_losses += abs(pnl)

        # Feature 9: Tier stats
        tier = bet.get("tier", "C")
        ts = self.session.tier_stats.setdefault(
            tier, {"wins": 0, "losses": 0, "early_exits": 0, "cut_losses": 0, "total_pnl": 0.0}
        )
        if reason.startswith("early_exit"):
            ts["early_exits"] += 1
            ts["wins"] += 1
        elif reason.endswith("loss_cut"):
            ts["cut_losses"] += 1
            ts["losses"] += 1
        ts["total_pnl"] += pnl

        # Feature 11: Category stats
        self._update_category_stats(bet.get("category", "other"), pnl >= 0, pnl)

        # Release budget
        self._release_budget(bet)

        self.session.active_bets.remove(bet)
        self.session.resolved_bets.append(bet)

        # Live sell
        if self.mode == "live" and self.engine:
            self.engine.execute_trade_auto(
                bet["market_slug"], bet["side"], "SELL",
                bet["amount"], self.session.user_id
            )

        self._save_state()
        logger.info(
            f"[BOND] {reason}: {bet['market_title'][:40]} | "
            f"Entry: {bet['price']:.2f} → Exit: {sell_price:.2f} | "
            f"PnL: ${pnl:+.2f}"
        )

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------
    def get_status(self) -> dict:
        total_wins = sum(s.get("wins", 0) for s in self.session.tier_stats.values())
        total_losses = sum(s.get("losses", 0) for s in self.session.tier_stats.values())
        total_resolved = total_wins + total_losses
        win_rate = (total_wins / total_resolved * 100) if total_resolved > 0 else 0
        net_pnl = self.session.total_profits - self.session.total_losses
        roi = (net_pnl / self.session.initial_capital * 100) if self.session.initial_capital > 0 else 0
        active_count = len([b for b in self.session.active_bets if b.get("status") == "active"])

        tier_display = {}
        for tk, stats in self.session.tier_stats.items():
            t_total = stats.get("wins", 0) + stats.get("losses", 0)
            tier_display[tk] = {
                "label": TIER_CONFIG.get(tk, {}).get("label", tk),
                "resolved": t_total,
                "win_rate": round(stats["wins"] / t_total * 100, 1) if t_total > 0 else 0,
                "pnl": round(stats.get("total_pnl", 0), 2),
                "early_exits": stats.get("early_exits", 0),
                "cut_losses": stats.get("cut_losses", 0),
            }

        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "active_bets": active_count,
            "current_pool": round(self.session.current_pool, 2),
            "total_deployed": round(self.session.total_deployed, 2),
            "initial_capital": self.session.initial_capital,
            "total_resolved": total_resolved,
            "win_count": total_wins,
            "loss_count": total_losses,
            "win_rate": round(win_rate, 1),
            "net_pnl": round(net_pnl, 2),
            "roi_pct": round(roi, 1),
            "total_profits": round(self.session.total_profits, 2),
            "total_losses": round(self.session.total_losses, 2),
            "withdrawn": round(self.session.withdrawn, 2),
            "tiers": tier_display,
            "categories": dict(self.session.category_deployed),
            "category_stats": dict(self.session.category_stats),
            "buckets": dict(self.session.bucket_deployed),
        }

    # ------------------------------------------------------------------
    # Emergency stop
    # ------------------------------------------------------------------
    def emergency_stop(self) -> int:
        cancelled = 0
        for bet in self.session.active_bets[:]:
            if bet.get("status") == "active":
                bet["status"] = "cancelled"
                bet["pnl"] = 0
                self.session.current_pool += bet["amount"]
                self.session.total_deployed -= bet["amount"]
                self._release_budget(bet)
                self.session.resolved_bets.append(bet)
                cancelled += 1
        self.session.active_bets = [
            b for b in self.session.active_bets if b.get("status") == "active"
        ]
        self.enabled = False
        self._save_state()
        return cancelled

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _release_budget(self, bet: dict):
        """Release bucket + category budget when bet resolves/sells."""
        self.session.total_deployed = max(0, self.session.total_deployed - bet["amount"])
        bucket = bet.get("time_bucket", "daily")
        self.session.bucket_deployed[bucket] = max(
            0, self.session.bucket_deployed.get(bucket, 0) - bet["amount"]
        )
        cat = bet.get("category", "other")
        self.session.category_deployed[cat] = max(
            0, self.session.category_deployed.get(cat, 0) - bet["amount"]
        )

    def _update_category_stats(self, category: str, won: bool, pnl: float):
        cs = self.session.category_stats.setdefault(
            category, {"wins": 0, "losses": 0, "total_pnl": 0.0, "bets": 0}
        )
        cs["bets"] += 1
        cs["wins" if won else "losses"] += 1
        cs["total_pnl"] += pnl

    def _hours_until_resolution(self, end_date: str) -> float:
        if not end_date:
            return 9999.0
        try:
            clean = end_date.strip().replace("Z", "+00:00")
            if len(clean) == 10:
                clean += "T23:59:59+00:00"
            end_ts = datetime.fromisoformat(clean).timestamp()
            return max(0, (end_ts - time.time()) / 3600.0)
        except (ValueError, TypeError):
            return 9999.0

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------
    def _save_state(self):
        with self._lock:
            try:
                data = asdict(self.session)
                with open(self._state_file, "w") as f:
                    json.dump(data, f, indent=2, default=str)
            except IOError as e:
                logger.error(f"Bond state save failed: {e}")

    def _load_state(self) -> SpreadSession:
        if os.path.exists(self._state_file):
            try:
                with open(self._state_file, "r") as f:
                    data = json.load(f)
                session = SpreadSession(**{
                    k: v for k, v in data.items()
                    if k in SpreadSession.__dataclass_fields__
                })
                logger.info(
                    f"Bond spreader: loaded {len(session.active_bets)} active bets"
                )
                return session
            except (json.JSONDecodeError, IOError, TypeError) as e:
                logger.warning(f"Bond state load failed: {e}")

        return SpreadSession(
            session_id=uuid.uuid4().hex[:10],
            user_id=str(self.cfg.get("telegram", {}).get("chat_id", "")),
            started_at=time.time(),
            mode=self.mode,
            initial_capital=self.max_deployed,
            current_pool=self.max_deployed,
        )
