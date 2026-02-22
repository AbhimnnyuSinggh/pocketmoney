"""
telegram_bot.py — Interactive Telegram Bot with Signal Type + Category Selection + Monetization
Users choose signal type AND category via inline keyboards.
Only matching signals are delivered. Users can switch anytime.
TIERS:
  🆓 Free     — 5 signals/day, 1st instant + rest 5-min delay
  ⭐ Pro      — Unlimited real-time signals ($9.99/mo via Telegram Stars)
  💎 Whale    — Everything + priority alerts ($29.99/mo via Telegram Stars)
Commands:
  /start    — Welcome screen + signal picker
  /menu     — Change signal type
  /category — Change category filter
  /help     — Show help
  /status   — Current selection + stats
  /upgrade  — View plans and subscribe via Telegram Stars
  /reset    — Reset all preferences
"""
import os
import json
import time
import logging
import threading
from datetime import datetime, timezone
from collections import defaultdict, deque
import requests as http_requests
from cloud_storage import get_cloud_storage
from cross_platform_scanner import Opportunity
from telegram_alerts_v2 import format_opportunity
logger = logging.getLogger("arb_bot.interactive")
# =========================================================================
# Signal Type Definitions
# =========================================================================
SIGNAL_TYPES = {
    "all": {
        "emoji": "📡",
        "label": "All Signals",
        "desc": "Every signal type combined",
        "opp_types": None,  # None = match all
    },
    "arb": {
        "emoji": "🔄",
        "label": "Arb Trading",
        "desc": "Cross-platform price discrepancies",
        "opp_types": ["cross_platform_arb"],
    },
    "bonds": {
        "emoji": "🏦",
        "label": "High-Prob Bonds",
        "desc": "93¢+ near-certain outcomes → $1.00",
        "opp_types": ["high_prob_bond"],
    },
    "intra": {
        "emoji": "🎯",
        "label": "Intra-Market",
        "desc": "YES + NO < $1.00 mispricing",
        "opp_types": ["intra_market_arb"],
    },
    "whale": {
        "emoji": "🐋",
        "label": "Whale Tracker",
        "desc": "3+ whales buying same side",
        "opp_types": ["whale_convergence"],
    },
    "sniper": {
        "emoji": "🆕",
        "label": "New Markets",
        "desc": "Brand new market launches",
        "opp_types": ["new_market"],
    },
}
# =========================================================================
# Category Definitions
# =========================================================================
CATEGORIES = {
    "all_cat": {
        "emoji": "🌐",
        "label": "All Categories",
        "keywords": None,  # None = match all
    },
    "politics": {
        "emoji": "🏛",
        "label": "Politics",
        "keywords": ["politics", "political", "government", "congress",
                     "president", "democrat", "republican", "biden", "trump",
                     "senate", "election", "vote", "governor", "legislation"],
    },
    "sports": {
        "emoji": "⚽",
        "label": "Sports",
        "keywords": ["sports", "nfl", "nba", "mlb", "soccer", "football",
                     "basketball", "baseball", "tennis", "cricket", "f1",
                     "ufc", "boxing", "golf", "hockey", "nhl", "premier league",
                     "champions league", "world cup", "super bowl", "lakers",
                     "warriors", "yankees", "cowboys"],
    },
    "crypto": {
        "emoji": "🪙",
        "label": "Crypto",
        "keywords": ["crypto", "bitcoin", "ethereum", "btc", "eth", "solana",
                     "defi", "blockchain", "web3", "token", "sol", "doge",
                     "xrp", "altcoin", "memecoin", "nft"],
    },
    "finance": {
        "emoji": "💰",
        "label": "Finance",
        "keywords": ["finance", "stock", "market", "s&p", "nasdaq", "dow",
                     "interest rate", "fed", "inflation", "treasury", "gdp",
                     "recession", "ipo", "earnings", "wall street"],
    },
    "geopolitics": {
        "emoji": "🌍",
        "label": "Geopolitics",
        "keywords": ["geopolitics", "war", "conflict", "nato", "sanctions",
                     "china", "russia", "ukraine", "taiwan", "iran", "north korea",
                     "un", "trade war", "diplomatic"],
    },
    "tech": {
        "emoji": "💻",
        "label": "Tech",
        "keywords": ["tech", "technology", "ai", "artificial intelligence",
                     "openai", "google", "apple", "microsoft", "meta", "tesla",
                     "spacex", "startup", "silicon valley", "chatgpt"],
    },
    "culture": {
        "emoji": "🎭",
        "label": "Culture",
        "keywords": ["culture", "celebrity", "movie", "music", "oscar",
                     "grammy", "netflix", "entertainment", "viral", "tiktok",
                     "youtube", "twitter", "social media", "influencer"],
    },
    "climate": {
        "emoji": "🔬",
        "label": "Climate & Science",
        "keywords": ["climate", "weather", "temperature", "hurricane",
                     "earthquake", "science", "nasa", "space", "pandemic",
                     "vaccine", "health", "environment", "carbon"],
    },
    "elections": {
        "emoji": "🗳",
        "label": "Elections",
        "keywords": ["election", "vote", "ballot", "primary", "nominee",
                     "candidate", "swing state", "electoral", "polling",
                     "midterm", "2026", "2028"],
    },
}
# =========================================================================
# Duration Filters — how soon the market resolves
# =========================================================================
DURATIONS = {
    "all_dur":  {"emoji": "🕐", "label": "All Durations",  "max_hours": None,  "min_hours": None},
    "24h":      {"emoji": "⚡", "label": "< 24 Hours",     "max_hours": 24,    "min_hours": None},
    "3d":       {"emoji": "🔥", "label": "< 3 Days",       "max_hours": 72,    "min_hours": None},
    "7d":       {"emoji": "📅", "label": "< 7 Days",       "max_hours": 168,   "min_hours": None},
    "30d":      {"emoji": "📆", "label": "< 30 Days",      "max_hours": 720,   "min_hours": None},
    "30d_plus": {"emoji": "🏦", "label": "> 30 Days",      "max_hours": None,  "min_hours": 720},
}
# =========================================================================
# Subscription Tier Definitions
# =========================================================================
TIERS = {
    "free": {
        "emoji": "🆓",
        "label": "Free",
        "daily_limit": 5,
        "delay_seconds": 300,
        "price_stars": 0,
        "price_usd": 0,
    },
    "pro": {
        "emoji": "⭐",
        "label": "Pro",
        "daily_limit": 999999,
        "delay_seconds": 0,
        "price_stars": 300,       # monthly
        "price_usd": 6.00,        # monthly
        "price_stars_yr": 3000,   # yearly (~17% savings)
        "price_usd_yr": 60.00,    # yearly
    },
    "whale_tier": {
        "emoji": "💎",
        "label": "Whale",
        "daily_limit": 999999,
        "delay_seconds": 0,
        "price_stars": 750,       # monthly
        "price_usd": 15.00,       # monthly
        "price_stars_yr": 7500,   # yearly (~17% savings)
        "price_usd_yr": 150.00,   # yearly
    },
}
# Subscription durations
SUB_MONTHLY = 30 * 24 * 3600     # 30 days
SUB_YEARLY  = 365 * 24 * 3600    # 365 days
SUBSCRIPTION_DURATION = SUB_MONTHLY  # default
# =========================================================================
# Multi-Chain USDC Wallet Addresses
# =========================================================================
# One wallet can work on ALL EVM chains (Polygon, Arbitrum, ETH, BSC, Base)
# but Solana requires a separate wallet.
USDC_CHAINS = {
    "polygon": {
        "emoji": "🟣",
        "label": "Polygon (Recommended)",
        "short": "Polygon",
        "addr": os.environ.get("USDC_ADDR_POLYGON", ""),
        "note": "Fastest & cheapest. Same chain as Polymarket.",
    },
    "arbitrum": {
        "emoji": "🔵",
        "label": "Arbitrum",
        "short": "Arbitrum",
        "addr": os.environ.get("USDC_ADDR_ARBITRUM", ""),
        "note": "Fast & cheap L2.",
    },
    "base": {
        "emoji": "🟦",
        "label": "Base",
        "short": "Base",
        "addr": os.environ.get("USDC_ADDR_BASE", ""),
        "note": "Coinbase L2, low fees.",
    },
    "bsc": {
        "emoji": "🟡",
        "label": "BNB Smart Chain",
        "short": "BSC",
        "addr": os.environ.get("USDC_ADDR_BSC", ""),
        "note": "Binance chain, low fees.",
    },
    "ethereum": {
        "emoji": "⚪",
        "label": "Ethereum (Mainnet)",
        "short": "ETH",
        "addr": os.environ.get("USDC_ADDR_ETH", ""),
        "note": "⚠️ Higher gas fees.",
    },
    "solana": {
        "emoji": "🟪",
        "label": "Solana",
        "short": "SOL",
        "addr": os.environ.get("USDC_ADDR_SOLANA", ""),
        "note": "Different wallet address.",
    },
}
# =========================================================================
# Payment Configuration
# =========================================================================
PAYMENT_CONFIG = {
    # --- Stripe Payment Links (Secondary — cards, Apple/Google Pay) ---
    "stripe_link_pro": os.environ.get("STRIPE_LINK_PRO", ""),
    "stripe_link_whale": os.environ.get("STRIPE_LINK_WHALE", ""),
    "stripe_link_pro_yr": os.environ.get("STRIPE_LINK_PRO_YR", ""),
    "stripe_link_whale_yr": os.environ.get("STRIPE_LINK_WHALE_YR", ""),
}
def _usdc_available() -> bool:
    """Return True if at least one USDC chain has an address configured."""
    return any(c["addr"] for c in USDC_CHAINS.values())
# =========================================================================
# Interactive Bot Handler
# =========================================================================
class TelegramBotHandler:
    """
    Interactive Telegram bot that lets users choose signal types + categories.
    Runs a polling thread alongside the main scan loop.
    Includes Telegram Stars payment for Pro/Whale tiers.
    """
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.token = str(cfg["telegram"].get("bot_token", ""))
        self.default_chat_id = str(cfg["telegram"].get("chat_id", ""))
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.enabled = bool(
            cfg["telegram"].get("enabled") and self.token and self.default_chat_id
        )
        self.bot_name = cfg["telegram"].get("bot_name", "PocketMoney")
        # Preferences file — stores {chat_id: {"signal": key, "category": key}}
        self.prefs_file = cfg.get("interactive", {}).get(
            "prefs_file", "user_prefs.json"
        )
        self.user_prefs: dict[str, dict] = self._load_prefs()
        # Subscriptions file — stores tier + expiry + daily counts
        self.subs_file = cfg.get("interactive", {}).get(
            "subs_file", "user_subs.json"
        )
        self.user_subs: dict[str, dict] = self._load_subs()
        # Signal history
        self.history_file = cfg.get("interactive", {}).get(
            "history_file", "signal_history.json"
        )
        self.history: dict[str, deque] = self._load_history()
        # Stats
        self.signals_sent = 0
        self.start_time = time.time()
        # Per-user dedup: {chat_id: {opp_key: last_sent_time}}
        self._user_seen: dict[str, dict[str, float]] = {}
        self.dedup_cooldown = cfg.get("interactive", {}).get(
            "dedup_cooldown_seconds", 1800  # 30 min default
        )
        # Ghost-alert cooldown: don't spam "you missed" every cycle
        self._ghost_last_sent: dict[str, float] = {}
        self._ghost_cooldown = 300  # max one ghost alert every 5 min
        # Pending external payments awaiting admin approval
        # {chat_id: {"tier": ..., "method": ..., "ts": ..., "ref": ..., "dur": ...}}
        self._pending_payments: dict[str, dict] = {}
        # Banned users set (persisted in user_subs with "banned" flag)
        self.banned_users: set[str] = self._load_banned()
        # Delayed messages queue for free tier: [(chat_id, message, release_time)]
        self.delayed_queue: list[tuple[str, str, float]] = []
        
        # Bond results tracker for marketing
        self.bond_tracker_file = cfg.get("interactive", {}).get(
            "bond_tracker_file", "bond_tracker.json"
        )
        self.bond_tracker: list[dict] = self._load_bond_tracker()

        # v3.0: Whale Vault + PnL Tracker
        try:
            from whale_vault import WhaleVault
            self.whale_vault = WhaleVault()
        except ImportError:
            self.whale_vault = None
        try:
            from pnl_tracker import PnLTracker
            self.pnl_tracker = PnLTracker()
        except ImportError:
            self.pnl_tracker = None

        # Start background thread for delayed messages
        self._delay_thread = threading.Thread(target=self._process_delayed_messages, daemon=True)
        self._delay_thread.start()
        # Start daily digest thread (sends admin summary at midnight UTC)
        self._start_daily_digest_thread()
        # Polling internals
        self._last_update_id = 0
        self._lock = threading.Lock()
        self._running = False
        logger.info(
            f"Bot handler init: default_chat_id={self.default_chat_id!r}, "
            f"users={len(self.user_prefs)}, enabled={self.enabled}, "
            f"subs={len(self.user_subs)}"
        )
    # -----------------------------------------------------------------
    # Persistence — Preferences (dict format: {"signal": ..., "category": ...})
    # -----------------------------------------------------------------
    def _load_prefs(self) -> dict:
        # Try local file first
        local_data = {}
        if os.path.exists(self.prefs_file):
            try:
                with open(self.prefs_file, "r") as f:
                    data = json.load(f)
                raw = data.get("users", data) if isinstance(data, dict) else {}
                for k, v in raw.items():
                    k = str(k)
                    if isinstance(v, str):
                        local_data[k] = {"signal": v, "category": "all_cat"}
                    elif isinstance(v, dict):
                        local_data[k] = v
                    else:
                        local_data[k] = {"signal": "all", "category": "all_cat"}
            except (json.JSONDecodeError, IOError):
                pass
        if local_data:
            return local_data
        # Local file missing (Render restart) — restore from cloud
        cloud = get_cloud_storage()
        cloud_data = cloud.load("user_prefs.json")
        if cloud_data:
            raw = cloud_data.get("users", cloud_data) if isinstance(cloud_data, dict) else {}
            restored = {}
            for k, v in raw.items():
                k = str(k)
                if isinstance(v, str):
                    restored[k] = {"signal": v, "category": "all_cat"}
                elif isinstance(v, dict):
                    restored[k] = v
                else:
                    restored[k] = {"signal": "all", "category": "all_cat"}
            if restored:
                logger.info(
                    f"☁️ Restored {len(restored)} user preference(s) from cloud backup"
                )
                try:
                    with open(self.prefs_file, "w") as f:
                        json.dump({"users": restored, "updated": time.time()}, f, indent=2)
                except IOError:
                    pass
                return restored
        return {}
    def _save_prefs(self):
        data = {"users": self.user_prefs, "updated": time.time()}
        # Save locally
        try:
            with open(self.prefs_file, "w") as f:
                json.dump(data, f, indent=2)
        except IOError as e:
            logger.error(f"Failed to save prefs locally: {e}")
        # Sync to cloud (batched, non-blocking)
        get_cloud_storage().save("user_prefs.json", data)
    def _get_signal(self, chat_id: str) -> str:
        """Get user's selected signal type key."""
        pref = self.user_prefs.get(chat_id, {})
        if isinstance(pref, str):
            return pref  # backward compat
        return pref.get("signal", "all")
    def _get_category(self, chat_id: str) -> str:
        """Get user's selected category key."""
        pref = self.user_prefs.get(chat_id, {})
        if isinstance(pref, str):
            return "all_cat"
        return pref.get("category", "all_cat")
    def _set_signal(self, chat_id: str, sig_key: str):
        """Set user's signal type. Resets dedup so they get fresh signals."""
        pref = self.user_prefs.get(chat_id, {})
        if isinstance(pref, str):
            pref = {"signal": pref, "category": "all_cat"}
        pref["signal"] = sig_key
        self.user_prefs[chat_id] = pref
        self._user_seen.pop(chat_id, None)  # Reset dedup on combo change
    def _set_category(self, chat_id: str, cat_key: str):
        """Set user's category filter. Resets dedup so they get fresh signals."""
        pref = self.user_prefs.get(chat_id, {})
        if isinstance(pref, str):
            pref = {"signal": pref, "category": "all_cat"}
        pref["category"] = cat_key
        self.user_prefs[chat_id] = pref
        self._user_seen.pop(chat_id, None)  # Reset dedup on combo change
    def _get_duration(self, chat_id: str) -> str:
        """Get user's selected duration filter key."""
        pref = self.user_prefs.get(chat_id, {})
        if isinstance(pref, str):
            return "all_dur"
        return pref.get("duration", "all_dur")
    def _set_duration(self, chat_id: str, dur_key: str):
        """Set user's duration filter. Resets dedup so they get fresh signals."""
        pref = self.user_prefs.get(chat_id, {})
        if isinstance(pref, str):
            pref = {"signal": pref, "category": "all_cat"}
        pref["duration"] = dur_key
        self.user_prefs[chat_id] = pref
        self._user_seen.pop(chat_id, None)  # Reset dedup on combo change
    # -----------------------------------------------------------------
    # Persistence — Subscriptions
    # -----------------------------------------------------------------
    def _load_subs(self) -> dict:
        # Try local file first
        local_data = {}
        if os.path.exists(self.subs_file):
            try:
                with open(self.subs_file, "r") as f:
                    data = json.load(f)
                raw = data.get("subs", data) if isinstance(data, dict) else {}
                local_data = {str(k): v for k, v in raw.items()}
            except (json.JSONDecodeError, IOError):
                pass
        if local_data:
            return local_data
        # Local file missing/empty (Render restart) — restore from cloud
        cloud = get_cloud_storage()
        cloud_data = cloud.load("user_subs.json")
        if cloud_data:
            raw = cloud_data.get("subs", cloud_data) if isinstance(cloud_data, dict) else {}
            restored = {str(k): v for k, v in raw.items()}
            if restored:
                logger.info(
                    f"☁️ Restored {len(restored)} subscription(s) from cloud backup"
                )
                # Write to local file so subsequent loads are fast
                try:
                    with open(self.subs_file, "w") as f:
                        json.dump({"subs": restored, "updated": time.time()}, f, indent=2)
                except IOError:
                    pass
                return restored
        return {}
    def _save_subs(self):
        data = {"subs": self.user_subs, "updated": time.time()}
        # Save locally
        try:
            with open(self.subs_file, "w") as f:
                json.dump(data, f, indent=2)
        except IOError as e:
            logger.error(f"Failed to save subs locally: {e}")
        # Sync to cloud (batched, non-blocking)
        get_cloud_storage().save("user_subs.json", data)
    def _get_user_sub(self, chat_id: str) -> dict:
        """Get subscription info for a user. Creates default if missing."""
        if chat_id not in self.user_subs:
            self.user_subs[chat_id] = {
                "tier": "free",
                "expires_at": 0,
                "daily_count": 0,
                "daily_reset": self._today_str(),
                "total_signals": 0,
                "subscribed_at": 0,
            }
        sub = self.user_subs[chat_id]
        # Check expiry — revert to free if expired
        if sub["tier"] != "free" and sub.get("expires_at", 0) > 0:
            if time.time() > sub["expires_at"]:
                logger.info(f"Subscription expired for {chat_id}")
                sub["tier"] = "free"
                self._save_subs()
        # Reset daily count if new day
        today = self._today_str()
        if sub.get("daily_reset") != today:
            sub["daily_count"] = 0
            sub["daily_reset"] = today
            sub["limit_hit_today"] = False  # Allow daily-limit prompt again
        return sub
    def _get_tier(self, chat_id: str) -> str:
        """Get effective tier for a user."""
        # Owner always gets whale tier
        if chat_id == self.default_chat_id:
            return "whale_tier"
        return self._get_user_sub(chat_id).get("tier", "free")
    @staticmethod
    def _today_str() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    def _load_banned(self) -> set:
        """Load banned users from user_subs (flag-based, no extra file)."""
        banned = set()
        for uid, sub in self.user_subs.items():
            if isinstance(sub, dict) and sub.get("banned"):
                banned.add(str(uid))
        return banned
    def _ban_user(self, chat_id: str):
        """Ban a user. They won't receive signals or be able to use commands."""
        with self._lock:
            sub = self._get_user_sub(chat_id)
            sub["banned"] = True
            self.user_subs[chat_id] = sub
            self.banned_users.add(chat_id)
            self._save_subs()
    def _unban_user(self, chat_id: str):
        """Unban a user."""
        with self._lock:
            sub = self._get_user_sub(chat_id)
            sub.pop("banned", None)
            self.user_subs[chat_id] = sub
            self.banned_users.discard(chat_id)
            self._save_subs()
    # -----------------------------------------------------------------
    # Persistence — Signal History
    # -----------------------------------------------------------------
    def _load_history(self) -> dict[str, deque]:
        history: dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        if not os.path.exists(self.history_file):
            return history
        try:
            with open(self.history_file, "r") as f:
                data = json.load(f)
            for opp_type, entries in data.get("signals", {}).items():
                valid = []
                for entry in entries:
                    if isinstance(entry, dict) and "msg" in entry:
                        valid.append(entry)
                    elif isinstance(entry, str):
                        valid.append({"ts": time.time(), "msg": entry})
                history[opp_type] = deque(valid, maxlen=50)
            logger.info(
                f"Loaded signal history: "
                f"{sum(len(v) for v in history.values())} signals across "
                f"{len(history)} types"
            )
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not load signal history: {e}")
        return history
    def _save_history(self):
        try:
            data = {
                "signals": {k: list(v) for k, v in self.history.items()},
                "updated": time.time(),
            }
            with open(self.history_file, "w") as f:
                json.dump(data, f, indent=2)
        except IOError as e:
            logger.error(f"Failed to save signal history: {e}")
    # -----------------------------------------------------------------
    # Bond Results Tracker (for marketing)
    # -----------------------------------------------------------------
    def _load_bond_tracker(self) -> list:
        if os.path.exists(self.bond_tracker_file):
            try:
                with open(self.bond_tracker_file, "r") as f:
                    data = json.load(f)
                bonds = data.get("bonds", [])
                if bonds:
                    return bonds
            except (json.JSONDecodeError, IOError):
                pass
        # Try cloud restore
        try:
            cloud = get_cloud_storage()
            cloud_data = cloud.load("bond_tracker.json")
            if cloud_data and cloud_data.get("bonds"):
                logger.info(
                    f"☁️ Restored {len(cloud_data['bonds'])} bond(s) from cloud"
                )
                return cloud_data["bonds"]
        except Exception:
            pass
        return []
    def _save_bond_tracker(self):
        try:
            data = {"bonds": self.bond_tracker, "updated": time.time()}
            with open(self.bond_tracker_file, "w") as f:
                json.dump(data, f, indent=2)
            get_cloud_storage().save("bond_tracker.json", data)
        except IOError as e:
            logger.error(f"Failed to save bond tracker: {e}")
    def _track_bond(self, opp, ts: float):
        """Log a bond signal for the marketing results tracker."""
        # Extract side and price from legs
        side = "?"
        price = opp.total_cost
        platform = opp.platforms[0] if opp.platforms else "unknown"
        if opp.legs:
            side = opp.legs[0].get("side", "?")
            price = opp.legs[0].get("price", opp.total_cost)
        entry = {
            "ts": ts,
            "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%b %d"),
            "title": opp.title[:80],
            "side": side,
            "buy_price": round(price, 4),
            "roi_pct": opp.profit_pct,
            "profit_per_100": opp.profit_amount,
            "platform": platform,
            "category": opp.category,
            "hold_time": opp.hold_time,
            "resolved": False,
            "result": None,
        }
        self.bond_tracker.append(entry)
        # Keep last 500 bonds max
        if len(self.bond_tracker) > 500:
            self.bond_tracker = self.bond_tracker[-500:]
        self._save_bond_tracker()
    def _get_results_summary(self) -> str:
        """Generate marketing-ready results summary."""
        if not self.bond_tracker:
            return "No bond signals tracked yet. Results will appear after the first scan cycle."
        total = len(self.bond_tracker)
        now = time.time()
        # Time ranges
        today_bonds = [b for b in self.bond_tracker if now - b["ts"] < 86400]
        week_bonds = [b for b in self.bond_tracker if now - b["ts"] < 7 * 86400]
        # Average ROI
        avg_roi = sum(b["roi_pct"] for b in self.bond_tracker) / total if total else 0
        # Potential profit (if user bought $100 of every signal)
        total_profit_100 = sum(b["profit_per_100"] for b in self.bond_tracker)
        week_profit_100 = sum(b["profit_per_100"] for b in week_bonds)
        # Platform breakdown
        poly_count = sum(1 for b in self.bond_tracker if b["platform"] == "polymarket")
        kalshi_count = sum(1 for b in self.bond_tracker if b["platform"] == "kalshi")
        # Price range
        prices = [b["buy_price"] for b in self.bond_tracker]
        min_price = min(prices) if prices else 0
        max_price = max(prices) if prices else 0
        # Best signal
        best = max(self.bond_tracker, key=lambda b: b["roi_pct"]) if self.bond_tracker else None
        # Category breakdown (top 3)
        cat_counts: dict[str, int] = {}
        for b in self.bond_tracker:
            cat = b.get("category", "unknown") or "unknown"
            cat_counts[cat] = cat_counts.get(cat, 0) + 1
        top_cats = sorted(cat_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        msg = (
            f"📊 <b>POLYQUICK BOND TRACKER</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"📈 <b>All Time</b>\n"
            f"Signals tracked: <b>{total}</b>\n"
            f"Average ROI: <b>{avg_roi:.2f}%</b>\n"
            f"Price range: ${min_price:.2f} – ${max_price:.2f}\n"
            f"If $100/signal: <b>${total_profit_100:,.2f}</b> total profit\n"
            f"\n"
            f"📅 <b>This Week</b>\n"
            f"Signals: <b>{len(week_bonds)}</b>\n"
            f"Potential profit ($100/signal): <b>${week_profit_100:,.2f}</b>\n"
            f"\n"
            f"📅 <b>Today</b>\n"
            f"Signals: <b>{len(today_bonds)}</b>\n"
            f"\n"
            f"🏆 <b>Best Signal</b>\n"
        )
        if best:
            msg += (
                f"{best['title'][:50]}\n"
                f"{best['side']} @ ${best['buy_price']:.2f} → "
                f"ROI: {best['roi_pct']:.2f}%\n"
            )
        msg += (
            f"\n"
            f"🌐 <b>Platforms</b>\n"
            f"Polymarket: {poly_count} | Kalshi: {kalshi_count}\n"
            f"\n"
            f"📂 <b>Top Categories</b>\n"
        )
        for cat, count in top_cats:
            msg += f"  {cat}: {count} signals\n"
        msg += (
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Updated: {datetime.now(timezone.utc).strftime('%b %d, %H:%M UTC')}</i>"
        )
        return msg
    # -----------------------------------------------------------------
    # User metadata enrichment (lightweight — dict writes only)
    # -----------------------------------------------------------------
    def _enrich_user(self, chat_id: str, msg: dict):
        """Capture user metadata from Telegram message. Fast dict write only."""
        user = msg.get("from", msg.get("chat", {}))
        pref = self.user_prefs.get(chat_id, {})
        changed = False
        # Capture name/username on first contact or if missing
        fname = user.get("first_name", "")
        uname = user.get("username", "")
        if fname and pref.get("first_name") != fname:
            pref["first_name"] = fname
            changed = True
        if uname and pref.get("username") != uname:
            pref["username"] = uname
            changed = True
        if "joined_at" not in pref:
            pref["joined_at"] = time.time()
            changed = True
        # Update last_active (only if >60s since last update to avoid spam writes)
        now = time.time()
        if now - pref.get("last_active", 0) > 60:
            pref["last_active"] = now
            changed = True
        # Increment command counter
        pref["commands_used"] = pref.get("commands_used", 0) + 1
        if changed:
            self.user_prefs[chat_id] = pref
            # Don't save to disk on every command — save_prefs is called
            # by _cmd_start and other commands that already persist
    # -----------------------------------------------------------------
    # Category matching
    # -----------------------------------------------------------------
    @staticmethod
    def _matches_category(opp: Opportunity, cat_key: str) -> bool:
        """Check if an opportunity matches the selected category."""
        if cat_key == "all_cat":
            return True
        cat_info = CATEGORIES.get(cat_key)
        if not cat_info or cat_info["keywords"] is None:
            return True
        # Whale signals don't have category data — always include them
        if opp.opp_type == "whale_convergence" and not opp.category:
            return True
        # Match by Polymarket category field (exact)
        opp_cat = opp.category.lower().strip()
        if opp_cat and opp_cat == cat_key:
            return True
        # Match by keywords in title or category
        text = f"{opp.title} {opp.category}".lower()
        for keyword in cat_info["keywords"]:
            if keyword in text:
                return True
        return False
    @staticmethod
    def _matches_duration(opp, dur_key: str, now_ts: float) -> bool:
        """Check if opportunity resolves within the selected duration window."""
        if dur_key == "all_dur":
            return True  # No filter — instant return
        dur_info = DURATIONS.get(dur_key)
        if not dur_info:
            return True
        # Get end date string from opportunity (handle both obj and dict)
        if isinstance(opp, dict):
            end_str = opp.get("hold_time", "") or ""
        else:
            end_str = getattr(opp, "hold_time", "") or ""
        if not end_str:
            # If user wants SHORT duration (e.g. <24h) and we have NO date,
            # we should probably SKIP it to be safe.
            # But if user wants LONG duration, maybe include?
            # For now: strictly exclude if unknown date when filter is active.
            return False
        # Parse ISO date to timestamp
        try:
            # Handle various ISO formats
            clean = end_str.strip().replace("Z", "+00:00")
            # Handle milliseconds .000+00:00
            if "." in clean and "+" in clean:
                # 2025-01-01T12:00:00.000+00:00 -> remove millis if needed or let fromisoformat handle
                pass 
            
            # Simple fix for "YYYY-MM-DD"
            if len(clean) == 10:
                clean += "T23:59:59+00:00" # Assume end of day for pure dates
                
            end_ts = datetime.fromisoformat(clean).timestamp()
        except (ValueError, TypeError):
            # Failed to parse — safer to EXCLUDE than to leak 3-year bets into <24h
            return False
        hours_until = (end_ts - now_ts) / 3600.0
        if hours_until < -24: # Allow some grace period for recently expired
            return False  
        max_h = dur_info["max_hours"]
        min_h = dur_info["min_hours"]
        if max_h is not None and hours_until >= max_h:
            return False
        if min_h is not None and hours_until < min_h:
            return False
        return True
    # -----------------------------------------------------------------
    # Telegram API helpers
    # -----------------------------------------------------------------
    def _send(
        self,
        chat_id: str,
        text: str,
        keyboard: dict | None = None,
    ) -> bool:
        if not self.enabled:
            return False
        payload: dict = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if keyboard:
            payload["reply_markup"] = json.dumps(keyboard)
        try:
            r = http_requests.post(
                f"{self.base_url}/sendMessage", json=payload, timeout=10,
            )
            if r.status_code != 200:
                logger.error(f"Telegram send failed ({r.status_code}): {r.text} | Payload: {payload}")
                if "parse_mode" in payload:
                    logger.info("Attempting plaintext fallback...")
                    del payload["parse_mode"]
                    r_fallback = http_requests.post(f"{self.base_url}/sendMessage", json=payload, timeout=10)
                    return r_fallback.status_code == 200
                return False
            return True
        except Exception as e:
            logger.error(f"Telegram send error: {e}")
            return False
    def _answer_callback(self, cb_id: str, text: str = ""):
        try:
            http_requests.post(
                f"{self.base_url}/answerCallbackQuery",
                json={"callback_query_id": cb_id, "text": text or "✅"},
                timeout=5,
            )
        except Exception:
            pass
    def _send_admin(self, text: str):
        """Fire-and-forget admin notification. Runs in background thread.
        NEVER blocks the calling thread. Safe to call from anywhere."""
        if not self.default_chat_id:
            return
        def _bg():
            try:
                self._send(str(self.default_chat_id), text)
            except Exception:
                pass  # Admin alerts are best-effort, never crash
        threading.Thread(target=_bg, daemon=True).start()
    def _send_invoice(self, chat_id: str, tier_key: str,
                      dur: str = "mo") -> bool:
        """Send a Telegram Stars payment invoice."""
        tier = TIERS.get(tier_key)
        if not tier or tier["price_stars"] == 0:
            return False
        if dur == "yr":
            stars = tier.get("price_stars_yr", tier["price_stars"])
            desc_suffix = "12 months"
            duration_secs = SUB_YEARLY
        else:
            stars = tier["price_stars"]
            desc_suffix = "30 days"
            duration_secs = SUB_MONTHLY
        payload = {
            "chat_id": chat_id,
            "title": f"{self.bot_name} {tier['label']} — {desc_suffix}",
            "description": self._tier_invoice_desc(tier_key),
            "payload": f"sub:{tier_key}:{dur}:{chat_id}:{int(time.time())}",
            "currency": "XTR",  # Telegram Stars
            "prices": [
                {"label": f"{tier['label']} Plan ({desc_suffix})", "amount": stars}
            ],
            "provider_token": "",  # Empty for Telegram Stars
        }
        try:
            r = http_requests.post(
                f"{self.base_url}/sendInvoice", json=payload, timeout=10,
            )
            if r.status_code == 200:
                logger.info(f"Invoice sent to {chat_id} for {tier_key}")
                return True
            else:
                logger.warning(f"Invoice failed: {r.status_code} {r.text[:200]}")
                return False
        except Exception as e:
            logger.error(f"Invoice send error: {e}")
            return False
    @staticmethod
    def _tier_invoice_desc(tier_key: str) -> str:
        if tier_key == "pro":
            return (
                "⭐ Unlimited real-time signals\n"
                "⭐ Zero delay on all alerts\n"
                "⭐ All 6 signal types\n"
                "⭐ 30-day access"
            )
        elif tier_key == "whale_tier":
            return (
                "💎 Everything in Pro, PLUS:\n"
                "💎 Priority alert delivery\n"
                "💎 Whale convergence details\n"
                "💎 Daily performance summary\n"
                "💎 30-day access"
            )
        return ""
    def _answer_pre_checkout(self, query_id: str, ok: bool = True, error: str = ""):
        """Respond to pre_checkout_query (required within 10 seconds)."""
        payload: dict = {
            "pre_checkout_query_id": query_id,
            "ok": ok,
        }
        if not ok and error:
            payload["error_message"] = error
        try:
            http_requests.post(
                f"{self.base_url}/answerPreCheckoutQuery",
                json=payload,
                timeout=10,
            )
        except Exception as e:
            logger.error(f"Pre-checkout answer error: {e}")
    # -----------------------------------------------------------------
    # Keyboards
    # -----------------------------------------------------------------
    def _signal_keyboard(self, current: str | None = None) -> dict:
        buttons = []
        row: list[dict] = []
        for key, info in SIGNAL_TYPES.items():
            check = "▸ " if current == key else ""
            label = f"{check}{info['emoji']} {info['label']}"
            row.append({"text": label, "callback_data": f"sig:{key}"})
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        return {"inline_keyboard": buttons}
    def _category_keyboard(self, current: str | None = None) -> dict:
        buttons = []
        row: list[dict] = []
        for key, info in CATEGORIES.items():
            check = "▸ " if current == key else ""
            label = f"{check}{info['emoji']} {info['label']}"
            row.append({"text": label, "callback_data": f"cat:{key}"})
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        return {"inline_keyboard": buttons}
    def _duration_keyboard(self, current: str | None = None) -> dict:
        buttons = []
        row: list[dict] = []
        for key, info in DURATIONS.items():
            check = "▸ " if current == key else ""
            label = f"{check}{info['emoji']} {info['label']}"
            row.append({"text": label, "callback_data": f"dur:{key}"})
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        return {"inline_keyboard": buttons}
    @staticmethod
    def _menu_button() -> dict:
        return {
            "inline_keyboard": [
                [
                    {"text": "📋 Menu", "callback_data": "cmd:menu"},
                    {"text": "🏷 Category", "callback_data": "cmd:category"},
                ],
                [
                    {"text": "ℹ️ Help", "callback_data": "cmd:help"},
                    {"text": "🚀 Upgrade", "callback_data": "cmd:upgrade"},
                ],
            ]
        }
    def _upgrade_keyboard(self, chat_id: str = "",
                          tier_key: str = "pro",
                          yearly: bool = False) -> dict:
        """Build payment keyboard — USDC primary, Stripe secondary, Stars tertiary."""
        tier = TIERS.get(tier_key, TIERS["pro"])
        if yearly:
            p_usd = tier.get("price_usd_yr", tier["price_usd"])
            stars = tier.get("price_stars_yr", tier["price_stars"])
            period = "year"
            dur_tag = "yr"
        else:
            p_usd = tier["price_usd"]
            stars = tier["price_stars"]
            period = "month"
            dur_tag = "mo"
        buttons: list[list[dict]] = []
        # Row 1 — USDC (PRIMARY — native to Polymarket)
        if _usdc_available():
            buttons.append([
                {"text": f"🟢 Pay ${p_usd} USDC (Recommended)",
                 "callback_data": f"pay_crypto:{tier_key}:{dur_tag}"},
            ])
        # Row 2 — Stripe (cards, Apple Pay, Google Pay, PayPal)
        s_suffix = '_yr' if yearly else ''
        s_key_map = {'pro': f'stripe_link_pro{s_suffix}', 'whale_tier': f'stripe_link_whale{s_suffix}'}
        stripe_key = s_key_map.get(tier_key, f'stripe_link_pro{s_suffix}')
        if PAYMENT_CONFIG.get(stripe_key):
            buttons.append([
                {"text": f"💳 Card / Apple Pay / G-Pay — ${p_usd}",
                 "callback_data": f"pay_card:{tier_key}:{dur_tag}"},
            ])
        # Row 3 — Telegram Stars (always available, built-in)
        buttons.append([
            {"text": f"⭐ Telegram Stars — {stars} Stars",
             "callback_data": f"buy:{tier_key}:{dur_tag}"},
        ])
        # Duration toggle: Monthly ↔ Yearly
        if yearly:
            buttons.append([
                {"text": f"📅 Switch to Monthly (${tier['price_usd']}/{period[0]}o)",
                 "callback_data": f"dur_mo:{tier_key}"},
            ])
        else:
            yr_price = tier.get('price_usd_yr', tier['price_usd'] * 10)
            savings = round(tier['price_usd'] * 12 - yr_price, 2)
            buttons.append([
                {"text": f"🌟 Yearly — ${yr_price}/yr (save ${savings}!)",
                 "callback_data": f"dur_yr:{tier_key}"},
            ])
        # Switcher: Pro ↔ Whale
        if tier_key == "pro":
            buttons.append([
                {"text": "💎 View Whale Plan",
                 "callback_data": f"show_tier:whale_tier:{'yr' if yearly else 'mo'}"},
            ])
        else:
            buttons.append([
                {"text": "⭐ View Pro Plan",
                 "callback_data": f"show_tier:pro:{'yr' if yearly else 'mo'}"},
            ])
        buttons.append([
            {"text": "📋 Back to Menu", "callback_data": "cmd:menu"},
        ])
        return {"inline_keyboard": buttons}
    # -----------------------------------------------------------------
    # Polling loop (runs in background thread)
    # -----------------------------------------------------------------
    def start_polling(self):
        if not self.enabled:
            logger.warning("Telegram not configured — interactive bot disabled")
            return
        self._running = True
        t = threading.Thread(target=self._poll_loop, daemon=True, name="tg-poll")
        t.start()
        logger.info("Interactive Telegram bot polling started")
    def stop_polling(self):
        self._running = False
    def _poll_loop(self):
        while self._running:
            try:
                r = http_requests.get(
                    f"{self.base_url}/getUpdates",
                    params={
                        "offset": self._last_update_id + 1,
                        "timeout": 30,
                        "allowed_updates": json.dumps(
                            ["message", "callback_query", "pre_checkout_query"]
                        ),
                    },
                    timeout=35,
                )
                if r.status_code != 200:
                    time.sleep(5)
                    continue
                for upd in r.json().get("result", []):
                    self._last_update_id = upd["update_id"]
                    try:
                        self._handle_update(upd)
                    except Exception as e:
                        logger.error(f"Update handler error: {e}")
                        import traceback
                        tb = traceback.format_exc()
                        if "message" in upd:
                            cid = str(upd["message"].get("chat", {}).get("id", ""))
                            if self._is_admin(cid):
                                self._send(cid, f"❌ <b>Crash in handler:</b>\n<pre>{tb[-1000:]}</pre>", parse_mode="HTML")
            except http_requests.Timeout:
                continue
            except Exception as e:
                logger.error(f"Polling error: {e}", exc_info=True)
                time.sleep(5)
    # -----------------------------------------------------------------
    # Update routing
    # -----------------------------------------------------------------
    def _handle_update(self, upd: dict):
        # --- Payment: Pre-checkout query (must respond within 10s) ---
        if "pre_checkout_query" in upd:
            pcq = upd["pre_checkout_query"]
            self._answer_pre_checkout(pcq["id"], ok=True)
            logger.info(
                f"Pre-checkout approved for {pcq['from']['id']}: "
                f"{pcq.get('invoice_payload', '')}"
            )
            return
        # --- Payment: Successful payment ---
        if "message" in upd and "successful_payment" in upd["message"]:
            self._handle_successful_payment(upd["message"])
            return
        # --- Callback queries (button taps) ---
        if "callback_query" in upd:
            self._on_callback(upd["callback_query"])
            return
        # --- Text commands ---
        if "message" in upd:
            msg = upd["message"]
            text = (msg.get("text") or "").strip()
            chat_id = str(msg["chat"]["id"])
            # Lightweight metadata capture (dict write only, no I/O)
            self._enrich_user(chat_id, msg)
            # Block banned users silently (except admin)
            if chat_id in self.banned_users and chat_id != str(self.default_chat_id):
                return  # Complete silence — user gets no indication
            if text.startswith("/start"):
                self._cmd_start(chat_id)
            elif text in ("/menu", "/switch"):
                self._cmd_menu(chat_id)
            elif text == "/category":
                self._cmd_category(chat_id)
            elif text == "/help":
                self._cmd_help(chat_id)
            elif text == "/status":
                self._cmd_status(chat_id)
            elif text == "/reset":
                self._cmd_reset(chat_id)
            elif text == "/upgrade":
                self._cmd_upgrade(chat_id)
            elif text == "/results":
                self._cmd_results(chat_id)
            elif text == "/elite":
                self._cmd_elite(chat_id)
            elif text == "/duration":
                self._cmd_duration(chat_id)
            elif text == "/stats":
                self._cmd_stats(chat_id)
            elif text == "/whales":
                self._cmd_whales(chat_id)
            elif text.startswith("/approve "):
                self._cmd_approve(chat_id, text)
            # --- Admin-only commands ---
            elif text.startswith("/ban "):
                self._cmd_ban(chat_id, text)
            elif text.startswith("/unban "):
                self._cmd_unban(chat_id, text)
            elif text == "/banned":
                self._cmd_banned_list(chat_id)
            elif text == "/admin":
                self._cmd_admin(chat_id)
            elif text == "/users":
                self._cmd_users(chat_id)
            elif text.startswith("/user "):
                self._cmd_user_lookup(chat_id, text)
            elif text.startswith("/broadcast "):
                self._cmd_broadcast(chat_id, text)
            elif text == "/feedback":
                self._cmd_feedback(chat_id)
            elif text == "/portfolio":
                self._cmd_portfolio(chat_id)
            elif text in ("/topwhales", "/topwhales 30d"):
                self._cmd_top_whales(chat_id, "30d")
            elif text == "/topwhales 7d":
                self._cmd_top_whales(chat_id, "7d")
            elif text == "/topwhales all":
                self._cmd_top_whales(chat_id, "all")
            # --- LP Commands (admin only) ---
            elif text in ("/lp", "/lp status"):
                self._cmd_lp_status(chat_id)
            elif text == "/lp start":
                self._cmd_lp_start(chat_id)
            elif text == "/lp stop":
                self._cmd_lp_stop(chat_id)
            elif text == "/lp markets":
                self._cmd_lp_markets(chat_id)
            # --- Manual Trading Commands (admin only) ---
            elif text.startswith("/buy "):
                self._cmd_manual_buy(chat_id, text)
            elif text.startswith("/sell "):
                self._cmd_manual_sell(chat_id, text)
            elif text == "/orders":
                self._cmd_show_orders(chat_id)
            elif text.startswith("/cancel"):
                self._cmd_cancel_order(chat_id, text)
            elif text == "/positions":
                self._cmd_positions(chat_id)
            # --- Bond Spreader Commands ---
            elif text.startswith("/bonds"):
                self._cmd_bonds(chat_id, text)
            elif text.startswith("/wallet"):
                self._cmd_wallet(chat_id, text)
    def _on_callback(self, cb: dict):
        cb_id = cb["id"]
        data = cb.get("data", "")
        chat_id = str(cb["message"]["chat"]["id"])
        # Capture region from callback too — not needed anymore
        if data.startswith("sig:"):
            sig_key = data[4:]
            if sig_key in SIGNAL_TYPES:
                self._select_signal(chat_id, sig_key)
                self._answer_callback(cb_id, f"✅ {SIGNAL_TYPES[sig_key]['label']}")
            else:
                self._answer_callback(cb_id)
        elif data.startswith("cat:"):
            cat_key = data[4:]
            if cat_key in CATEGORIES:
                self._select_category(chat_id, cat_key)
                self._answer_callback(cb_id, f"✅ {CATEGORIES[cat_key]['label']}")
            else:
                self._answer_callback(cb_id)
        elif data.startswith("pay_crypto:"):
            parts = data.split(":")
            tier_key = parts[1] if len(parts) > 1 else "pro"
            dur = parts[2] if len(parts) > 2 else "mo"
            self._show_chain_selector(chat_id, tier_key, dur)
            self._answer_callback(cb_id, "🟢 Choose your chain")
        elif data.startswith("chain:"):
            # chain:polygon:pro:mo
            parts = data.split(":")
            chain = parts[1] if len(parts) > 1 else "polygon"
            tier_key = parts[2] if len(parts) > 2 else "pro"
            dur = parts[3] if len(parts) > 3 else "mo"
            self._handle_crypto_payment(chat_id, tier_key, chain, dur)
            self._answer_callback(cb_id, f"🟢 {chain.title()} details sent")
        elif data.startswith("pay_card:"):
            parts = data.split(":")
            tier_key = parts[1] if len(parts) > 1 else "pro"
            dur = parts[2] if len(parts) > 2 else "mo"
            self._handle_card_payment(chat_id, tier_key, dur)
            self._answer_callback(cb_id, "💳 Payment link sent")
        elif data.startswith("show_tier:"):
            parts = data.split(":")
            tier_key = parts[1] if len(parts) > 1 else "pro"
            dur = parts[2] if len(parts) > 2 else "mo"
            self._show_tier_details(chat_id, tier_key, dur == "yr")
            self._answer_callback(cb_id)
        elif data.startswith("dur_yr:"):
            tier_key = data[7:]
            self._show_tier_details(chat_id, tier_key, yearly=True)
            self._answer_callback(cb_id, "🌟 Yearly plan")
        elif data.startswith("dur_mo:"):
            tier_key = data[7:]
            self._show_tier_details(chat_id, tier_key, yearly=False)
            self._answer_callback(cb_id, "📅 Monthly plan")
        elif data.startswith("buy:"):
            parts = data.split(":")
            tier_key = parts[1] if len(parts) > 1 else "pro"
            dur = parts[2] if len(parts) > 2 else "mo"
            if tier_key in TIERS and TIERS[tier_key]["price_stars"] > 0:
                self._send_invoice(chat_id, tier_key, dur)
                self._answer_callback(cb_id, "⭐ Opening Stars payment...")
            else:
                self._answer_callback(cb_id)
        elif data.startswith("paid:"):
            parts = data.split(":")
            tier_key = parts[1] if len(parts) > 1 else "pro"
            dur = parts[2] if len(parts) > 2 else "mo"
            self._handle_paid_confirmation(chat_id, tier_key, dur)
            self._answer_callback(cb_id, "✅ Sent for verification")
        elif data == "cmd:menu":
            self._cmd_menu(chat_id)
            self._answer_callback(cb_id)
        elif data == "cmd:category":
            self._cmd_category(chat_id)
            self._answer_callback(cb_id)
        elif data == "cmd:duration":
            self._cmd_duration(chat_id)
            self._answer_callback(cb_id)
        elif data == "cmd:help":
            self._cmd_help(chat_id)
            self._answer_callback(cb_id)
        elif data == "cmd:upgrade":
            self._cmd_upgrade(chat_id)
            self._answer_callback(cb_id)
        elif data.startswith("dur:"):
            dur_key = data[4:]
            if dur_key in DURATIONS or dur_key == "all_dur":
                self._select_duration(chat_id, dur_key)
                label = DURATIONS.get(dur_key, {}).get('label', dur_key)
                self._answer_callback(cb_id, f"✅ {label}")
            else:
                self._answer_callback(cb_id)
        elif data.startswith("vote:"):
            parts = data.split(":", 2)
            vote = parts[1] if len(parts) > 1 else "up"
            sig_hash = parts[2] if len(parts) > 2 else ""
            self._handle_vote(chat_id, vote, sig_hash)
            emoji = "👍" if vote == "up" else "👎"
            self._answer_callback(cb_id, f"{emoji} Vote recorded!")
        elif data.startswith("lp_start:"):
            # LP farming start from signal inline button
            market_slug = data[9:]
            self._answer_callback(cb_id, "🏭 Starting LP...")
            self._cmd_lp_start_market(chat_id, market_slug)
        elif data.startswith("trade_buy:"):
            # Quick buy from signal
            parts = data.split(":", 3)  # trade_buy:slug:side:price
            self._answer_callback(cb_id, "💰 Processing...")
            if len(parts) >= 4:
                self._cmd_quick_trade(chat_id, "BUY", parts[1], parts[2], parts[3])
        elif data.startswith("trade_sell:"):
            parts = data.split(":", 3)
            self._answer_callback(cb_id, "💰 Processing...")
            if len(parts) >= 4:
                self._cmd_quick_trade(chat_id, "SELL", parts[1], parts[2], parts[3])
        else:
            self._answer_callback(cb_id)
    # -----------------------------------------------------------------
    # Payment handler
    # -----------------------------------------------------------------
    def _handle_successful_payment(self, msg: dict):
        """Process a successful Telegram Stars payment."""
        payment = msg["successful_payment"]
        chat_id = str(msg["chat"]["id"])
        payload = payment.get("invoice_payload", "")
        amount = payment.get("total_amount", 0)
        currency = payment.get("currency", "")
        logger.info(
            f"💰 PAYMENT RECEIVED: chat_id={chat_id}, "
            f"amount={amount} {currency}, payload={payload}"
        )
        # Parse payload: "sub:pro:chat_id:timestamp"
        parts = payload.split(":")
        tier_key = parts[1] if len(parts) >= 2 and parts[0] == "sub" else "pro"
        dur = parts[2] if len(parts) >= 3 else "mo" # 'mo' or 'yr'
        if tier_key not in TIERS:
            tier_key = "pro"
        # Determine subscription duration
        if dur == "yr":
            subscription_duration = SUB_YEARLY
        else:
            subscription_duration = SUB_MONTHLY
        # Activate subscription
        now = time.time()
        with self._lock:
            sub = self._get_user_sub(chat_id)
            # If already subscribed, extend from current expiry
            if sub["tier"] != "free" and sub.get("expires_at", 0) > now:
                expires = sub["expires_at"] + subscription_duration
            else:
                expires = now + subscription_duration
            sub["tier"] = tier_key
            sub["expires_at"] = expires
            sub["subscribed_at"] = now
            sub["daily_count"] = 0  # Reset limit on upgrade
            sub["expiry_reminded"] = 0  # Reset renewal reminders
            self.user_subs[chat_id] = sub
            self._save_subs()
        expiry_str = datetime.fromtimestamp(
            expires, tz=timezone.utc
        ).strftime("%B %d, %Y")
        tier = TIERS[tier_key]
        confirm_msg = (
            f"🎉 <b>PAYMENT SUCCESSFUL!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Plan: <b>{tier['emoji']} {tier['label']}</b>\n"
            f"Amount: <b>{amount} Stars</b>\n"
            f"Expires: <b>{expiry_str}</b>\n"
            f"\n"
            f"✅ Unlimited real-time signals activated!\n"
            f"✅ Zero delay on all alerts\n"
            f"✅ All 6 signal types unlocked\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Thank you for supporting {self.bot_name}! 🚀"
        )
        self._send(chat_id, confirm_msg)
        logger.info(f"✅ Subscription activated: {chat_id} → {tier_key} until {expiry_str}")
        # Alert admin about new payment (fire-and-forget)
        if chat_id != str(self.default_chat_id):
            pref = self.user_prefs.get(chat_id, {})
            uname = pref.get("username", "")
            name_str = f"@{uname}" if uname else pref.get("first_name", chat_id)
            pro_count = sum(1 for s in self.user_subs.values()
                           if isinstance(s, dict) and s.get("tier") == "pro"
                           and s.get("expires_at", 0) > time.time())
            whale_count = sum(1 for s in self.user_subs.values()
                             if isinstance(s, dict) and s.get("tier") == "whale_tier"
                             and s.get("expires_at", 0) > time.time())
            mrr = (pro_count * 6) + (whale_count * 15)
            self._send_admin(
                f"💰 <b>NEW PAYMENT</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"User: <b>{name_str}</b> (<code>{chat_id}</code>)\n"
                f"Plan: <b>{tier['emoji']} {tier['label']}</b>\n"
                f"Amount: <b>{amount} Stars</b>\n"
                f"Expires: <b>{expiry_str}</b>\n\n"
                f"⭐ Pro: {pro_count} | 💎 Whale: {whale_count} | "
                f"MRR: <b>${mrr}/mo</b>"
            )
    # -----------------------------------------------------------------
    # Commands
    # -----------------------------------------------------------------
    def _cmd_start(self, chat_id: str):
        current_sig = self._get_signal(chat_id)
        current_cat = self._get_category(chat_id)
        current_label = ""
        if current_sig and current_sig in SIGNAL_TYPES:
            s = SIGNAL_TYPES[current_sig]
            c = CATEGORIES.get(current_cat, CATEGORIES["all_cat"])
            current_label = (
                f"\n📌 Signal: <b>{s['emoji']} {s['label']}</b>\n"
                f"🏷 Category: <b>{c['emoji']} {c['label']}</b>\n"
            )
        tier = self._get_tier(chat_id)
        tier_info = TIERS.get(tier, TIERS["free"])
        tier_label = f"{tier_info['emoji']} {tier_info['label']}"
        msg = (
            f"🤖 <b>{self.bot_name} — Prediction Market Signals</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"I scan 4,000+ markets on Polymarket &\n"
            f"Kalshi every 60 seconds for edge.\n"
            f"{current_label}\n"
            f"Plan: <b>{tier_label}</b>"
        )
        if tier == "free":
            msg += f" (5 signals/day)\n"
            msg += f"💡 <i>/upgrade for unlimited real-time alerts</i>\n"
        else:
            msg += f" (unlimited)\n"
        msg += (
            f"\n<b>Step 1: Choose your signal type:</b>\n"
            f"📡 All • 🔄 Arb • 🏦 Bonds • 🎯 Intra • 🐋 Whales\n"
            f"\n<b>Step 2: Choose your category:</b>\n"
            f"⚽ Sports • 🏛 Politics • 🪙 Crypto • 💰 Finance\n"
            f"\n<b>Step 3: Choose duration:</b>\n"
            f"⚡ &lt;24h • 🔥 &lt;3d • 📅 &lt;7d\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Tap to configure:</b>"
        )
        # Send 3 keyboards in sequence
        self._send(chat_id, msg, self._signal_keyboard(current_sig))
        self._send(chat_id, "<b>Category:</b>", self._category_keyboard(current_cat))
        self._send(chat_id, "<b>Duration:</b>", self._duration_keyboard(self._get_duration(chat_id)))
        # Auto-register with defaults
        if chat_id not in self.user_prefs:
            self.user_prefs[chat_id] = {"signal": "all", "category": "all_cat",
                                         "joined_at": time.time()}
            self._save_prefs()
            # Alert admin about new user (fire-and-forget, non-blocking)
            if chat_id != str(self.default_chat_id):
                total = len(self.user_prefs)
                today_joins = sum(
                    1 for p in self.user_prefs.values()
                    if isinstance(p, dict)
                    and time.time() - p.get("joined_at", 0) < 86400
                )
                uname = self.user_prefs[chat_id].get("username", "")
                fname = self.user_prefs[chat_id].get("first_name", "")
                name_str = f"@{uname}" if uname else fname or chat_id
                self._send_admin(
                    f"🆕 <b>NEW USER JOINED</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"Name: <b>{name_str}</b>\n"
                    f"ID: <code>{chat_id}</code>\n"
                    f"Tier: 🆓 Free\n\n"
                    f"👥 Total: <b>{total}</b> (+{today_joins} today)"
                )
    def _cmd_menu(self, chat_id: str):
        current_sig = self._get_signal(chat_id)
        current_cat = self._get_category(chat_id)
        s = SIGNAL_TYPES.get(current_sig, SIGNAL_TYPES["all"])
        c = CATEGORIES.get(current_cat, CATEGORIES["all_cat"])
        dur_key = self._get_duration(chat_id)
        d = DURATIONS.get(dur_key, DURATIONS["all_dur"])
        
        msg = (
            f"📋 <b>SIGNAL MENU</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Signal: <b>{s['emoji']} {s['label']}</b>\n"
            f"Category: <b>{c['emoji']} {c['label']}</b>\n"
            f"Duration: <b>{d['emoji']} {d['label']}</b>\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Tap to switch:</b>"
        )
        self._send(chat_id, msg, self._signal_keyboard(current_sig))
        self._send(chat_id, "<b>Category:</b>", self._category_keyboard(current_cat))
        self._send(chat_id, "<b>Duration:</b>", self._duration_keyboard(dur_key))
    def _cmd_category(self, chat_id: str):
        """Show category picker."""
        current_cat = self._get_category(chat_id)
        current_sig = self._get_signal(chat_id)
        s = SIGNAL_TYPES.get(current_sig, SIGNAL_TYPES["all"])
        c = CATEGORIES.get(current_cat, CATEGORIES["all_cat"])
        msg = (
            f"🏷 <b>CATEGORY FILTER</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Signal: <b>{s['emoji']} {s['label']}</b>\n"
            f"Category: <b>{c['emoji']} {c['label']}</b>\n"
            f"\n"
            f"Filter signals by your area of expertise.\n"
            f"Pick 🌐 All to see everything.\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Tap to select category:</b>"
        )
        self._send(chat_id, msg, self._category_keyboard(current_cat))
    def _cmd_help(self, chat_id: str):
        tier = self._get_tier(chat_id)
        msg = (
            f"ℹ️ <b>BOT HELP</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"<b>Commands:</b>\n"
            f"/start    — Welcome + pick signal type\n"
            f"/menu     — Switch signal type\n"
            f"/category — Filter by category\n"
            f"/duration — Filter by time horizon\n"
            f"/elite    — Edge score dashboard\n"
            f"/stats    — Performance tracker\n"
            f"/whales   — Whale vault\n"
            f"/status   — View current stats\n"
            f"/results  — Bond performance tracker\n"
            f"/bonds    — Bond spread automator\n"
            f"/wallet   — Wallet & trading setup\n"
            f"/upgrade  — View plans & subscribe\n"
            f"/reset    — Reset preferences\n"
            f"/help     — This message\n"
            f"\n"
            f"<b>How it works:</b>\n"
            f"1️⃣ Pick a signal type (bonds, arb, etc.)\n"
            f"2️⃣ Pick a category (sports, crypto, etc.)\n"
            f"3️⃣ Receive matching signals\n"
            f"4️⃣ Switch anytime with /menu or /category\n"
            f"\n"
            f"<b>Signal types:</b>\n"
            f"📡 All — Every signal\n"
            f"🔄 Arb — Buy on platform A, sell on B\n"
            f"🏦 Bonds — 93¢+ → $1.00 safe returns\n"
            f"🎯 Intra — YES+NO price errors\n"
            f"🐋 Whales — Big traders converging\n"
            f"🆕 New — Brand new markets\n"
            f"\n"
            f"<b>Plans:</b>\n"
            f"🆓 Free — 5 signals/day, 30-min delay\n"
            f"⭐ Pro — Unlimited, real-time (300 Stars)\n"
            f"💎 Whale — Everything + priority (750 Stars)\n"
            f"\n"
            f"Tap /upgrade to go Pro!"
        )
        self._send(chat_id, msg)
    def _cmd_status(self, chat_id: str):
        current_sig = self._get_signal(chat_id)
        current_cat = self._get_category(chat_id)
        s = SIGNAL_TYPES.get(current_sig, SIGNAL_TYPES["all"])
        c = CATEGORIES.get(current_cat, CATEGORIES["all_cat"])
        tier = self._get_tier(chat_id)
        tier_info = TIERS.get(tier, TIERS["free"])
        sub = self._get_user_sub(chat_id)
        uptime = int(time.time() - self.start_time)
        hours, rem = divmod(uptime, 3600)
        mins = rem // 60
        # Count history
        hist_counts = {k: len(v) for k, v in self.history.items() if v}
        hist_text = ""
        for opp_type, count in hist_counts.items():
            for sk, sv in SIGNAL_TYPES.items():
                if sv["opp_types"] and opp_type in sv["opp_types"]:
                    hist_text += f"  {sv['emoji']} {sv['label']}: <b>{count}</b>\n"
                    break
        msg = (
            f"📊 <b>BOT STATUS</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Signal: <b>{s['emoji']} {s['label']}</b>\n"
            f"Category: <b>{c['emoji']} {c['label']}</b>\n"
            f"Plan: <b>{tier_info['emoji']} {tier_info['label']}</b>\n"
        )
        if tier == "free":
            daily_used = sub.get("daily_count", 0)
            msg += f"Today: <b>{daily_used}/5</b> signals used\n"
        else:
            expiry = sub.get("expires_at", 0)
            if str(chat_id) == self.default_chat_id:
                msg += f"Expires: <b>Lifetime (Admin)</b>\n"
            elif expiry > 0:
                exp_str = datetime.fromtimestamp(
                    expiry, tz=timezone.utc
                ).strftime("%b %d, %Y")
                days_left = max(0, int((expiry - time.time()) / 86400))
                msg += f"Expires: <b>{exp_str}</b> ({days_left}d left)\n"
        msg += (
            f"\nUptime: {hours}h {mins}m\n"
            f"Total signals sent: <b>{self.signals_sent}</b>\n"
        )
        if hist_text:
            msg += f"\n📦 <b>Cached signals:</b>\n{hist_text}\n"
        msg += (
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Use /menu or /category to change."
        )
        if tier == "free":
            msg += "\n💡 <i>/upgrade for unlimited alerts</i>"
        self._send(chat_id, msg)
    def _cmd_results(self, chat_id: str):
        """Show the bond results tracker — marketing-ready stats."""
        msg = self._get_results_summary()
        self._send(chat_id, msg)

    def _cmd_stats(self, chat_id: str):
        """Show PnL tracker stats — /stats command."""
        if self.pnl_tracker:
            msg = self.pnl_tracker.format_stats_message()
        else:
            msg = (
                "📊 <b>PERFORMANCE TRACKER</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "PnL tracker not available.\n"
                "It will activate on the next scan cycle."
            )
        self._send(chat_id, msg)

    def _cmd_whales(self, chat_id: str):
        """Show whale vault summary — /whales command."""
        if self.whale_vault:
            msg = self.whale_vault.format_vault_summary()
        else:
            msg = (
                "🐋 <b>WHALE VAULT</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "Whale vault not available.\n"
                "It will activate on the next scan cycle."
            )
        self._send(chat_id, msg)

    def _cmd_elite(self, chat_id: str):
        """v3.0 Elite Edge Dashboard — shows scoring stats and filter summary."""
        current_sig = self._get_signal(chat_id)
        current_cat = self._get_category(chat_id)
        current_dur = self._get_duration(chat_id)
        s = SIGNAL_TYPES.get(current_sig, SIGNAL_TYPES["all"])
        c = CATEGORIES.get(current_cat, CATEGORIES["all_cat"])
        d = DURATIONS.get(current_dur, DURATIONS["all_dur"])
        tier = self._get_tier(chat_id)
        tier_info = TIERS.get(tier, TIERS["free"])

        # Compute stats from recent history
        total_signals = sum(len(v) for v in self.history.values())
        scored_signals = 0
        avg_score = 0.0
        top_score = 0.0
        fire_count = 0  # Score >= 85
        for entries in self.history.values():
            for e in entries:
                if isinstance(e, dict) and e.get("edge_score", 0) > 0:
                    scored_signals += 1
                    score = e["edge_score"]
                    avg_score += score
                    if score > top_score:
                        top_score = score
                    if score >= 85:
                        fire_count += 1

        if scored_signals > 0:
            avg_score = avg_score / scored_signals
        
        msg = (
            f"⚡ <b>ELITE EDGE DASHBOARD</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"<b>Your Filters:</b>\n"
            f"  Signal: {s['emoji']} {s['label']}\n"
            f"  Category: {c['emoji']} {c['label']}\n"
            f"  Duration: {d['emoji']} {d['label']}\n"
            f"  Plan: {tier_info['emoji']} {tier_info['label']}\n"
            f"\n"
            f"<b>Edge Score Stats:</b>\n"
            f"  📊 Total signals cached: <b>{total_signals}</b>\n"
            f"  ⚡ Scored signals: <b>{scored_signals}</b>\n"
        )

        if scored_signals > 0:
            msg += (
                f"  📈 Average score: <b>{avg_score:.1f}/100</b>\n"
                f"  🏆 Top score: <b>{top_score:.0f}/100</b>\n"
                f"  🔥 Fire signals (85+): <b>{fire_count}</b>\n"
            )

        msg += (
            f"\n<b>v3.0 Features Active:</b>\n"
            f"  ✅ Unified Edge Scoring\n"
            f"  ✅ Smart Dedup (30-min cooldown)\n"
            f"  ✅ Duration Filter\n"
            f"  ✅ Category Filter\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Quick Actions:</b>"
        )
        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "📋 Menu", "callback_data": "cmd:menu"},
                    {"text": "🏷 Category", "callback_data": "cmd:category"},
                ],
                [
                    {"text": "🕐 Duration", "callback_data": "cmd:duration"},
                    {"text": "🚀 Upgrade", "callback_data": "cmd:upgrade"},
                ],
            ]
        }
        self._send(chat_id, msg, keyboard)

    def _cmd_duration(self, chat_id: str):
        """Show duration filter picker."""
        current_dur = self._get_duration(chat_id)
        current_sig = self._get_signal(chat_id)
        s = SIGNAL_TYPES.get(current_sig, SIGNAL_TYPES["all"])
        d = DURATIONS.get(current_dur, DURATIONS["all_dur"])
        msg = (
            f"🕐 <b>DURATION FILTER</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Signal: <b>{s['emoji']} {s['label']}</b>\n"
            f"Current Duration: <b>{d['emoji']} {d['label']}</b>\n"
            f"\n"
            f"Filter signals by when the market resolves.\n"
            f"Pick ⏳ All to see everything.\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Tap to select duration:</b>"
        )
        self._send(chat_id, msg, self._duration_keyboard(current_dur))

    def _select_duration(self, chat_id: str, dur_key: str):
        """User selected a duration. Confirm + replay filtered history."""
        # Save duration preference
        with self._lock:
            self._set_duration(chat_id, dur_key)
            self._save_prefs()
            logger.info(f"User {chat_id} selected duration: {dur_key}")

        current_sig = self._get_signal(chat_id)
        s = SIGNAL_TYPES.get(current_sig, SIGNAL_TYPES["all"])
        current_cat = self._get_category(chat_id)
        c = CATEGORIES.get(current_cat, CATEGORIES["all_cat"])
        dur_info = DURATIONS.get(dur_key, DURATIONS["all_dur"])
        tier = self._get_tier(chat_id)
        tier_info = TIERS.get(tier, TIERS["free"])

        past = self._get_history_for(current_sig, current_cat)

        confirm = (
            f"✅ <b>Duration updated!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Signal: <b>{s['emoji']} {s['label']}</b>\n"
            f"Category: <b>{c['emoji']} {c['label']}</b>\n"
            f"Duration: <b>{dur_info['emoji']} {dur_info['label']}</b>\n"
            f"Plan: {tier_info['emoji']} {tier_info['label']}\n"
            f"\n"
        )

        if past:
            confirm += (
                f"📜 Sending <b>{len(past)}</b> recent matching signal"
                f"{'s' if len(past) != 1 else ''}...\n"
                f"<i>(Filtered by your duration setting)</i>\n"
            )
        else:
            confirm += (
                f"📡 <b>Scanning active...</b>\n"
                f"No recent signals match this duration.\n"
                f"I'll alert you the moment one appears!\n"
            )

        confirm += (
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Use /menu to switch filters anytime."
        )
        self._send(chat_id, confirm)

        # Replay past signals (Filtered)
        if past:
            time.sleep(0.5)
            for entry in past[-5:]:
                msg = entry["msg"] if isinstance(entry, dict) else entry
                self._send(chat_id, msg)
                time.sleep(0.5)
    # -----------------------------------------------------------------
    # Admin-only commands (gated by default_chat_id check)
    # -----------------------------------------------------------------
    def _is_admin(self, chat_id: str) -> bool:
        return str(chat_id) == str(self.default_chat_id)
    def _cmd_admin(self, chat_id: str):
        """Live dashboard — admin only."""
        if not self._is_admin(chat_id):
            return
        now = time.time()
        total_users = len(self.user_prefs)
        # Time-based user counts
        today_joins = 0
        week_joins = 0
        active_24h = 0
        active_7d = 0
        for p in self.user_prefs.values():
            if not isinstance(p, dict):
                continue
            joined = p.get("joined_at", 0)
            last = p.get("last_active", 0)
            if now - joined < 86400:
                today_joins += 1
            if now - joined < 7 * 86400:
                week_joins += 1
            if now - last < 86400:
                active_24h += 1
            if now - last < 7 * 86400:
                active_7d += 1
        # Subscription counts
        pro_count = 0
        whale_count = 0
        for s in self.user_subs.values():
            if not isinstance(s, dict):
                continue
            if s.get("expires_at", 0) <= now:
                continue
            t = s.get("tier", "free")
            if t == "pro":
                pro_count += 1
            elif t == "whale_tier":
                whale_count += 1
        free_count = total_users - pro_count - whale_count
        mrr = (pro_count * 6) + (whale_count * 15)
        # Signal counts from history
        total_signals = sum(len(v) for v in self.history.values())
        bond_count = len(self.history.get("high_prob_bond", []))
        arb_count = len(self.history.get("cross_platform_arb", []))
        whale_sig = len(self.history.get("whale_convergence", []))
        new_mkt = len(self.history.get("new_market", []))
        # Category breakdown (from prefs)
        cat_counts: dict[str, int] = {}
        for p in self.user_prefs.values():
            if isinstance(p, dict):
                c = p.get("category", "all_cat")
                cat_counts[c] = cat_counts.get(c, 0) + 1
        top_cats = sorted(cat_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        # Signal type breakdown
        sig_counts: dict[str, int] = {}
        for p in self.user_prefs.values():
            if isinstance(p, dict):
                s = p.get("signal", "all")
                sig_counts[s] = sig_counts.get(s, 0) + 1
        # Uptime
        uptime = int(now - self.start_time)
        uh, ur = divmod(uptime, 3600)
        um = ur // 60
        msg = (
            f"📊 <b>POLYQUICK ADMIN</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 <b>USERS</b>\n"
            f"Total: <b>{total_users}</b>\n"
            f"Today: +{today_joins} | Week: +{week_joins}\n"
            f"Active 24h: {active_24h} | Active 7d: {active_7d}\n\n"
            f"💰 <b>REVENUE</b>\n"
            f"⭐ Pro: {pro_count} (${pro_count * 6}/mo)\n"
            f"💎 Whale: {whale_count} (${whale_count * 15}/mo)\n"
            f"MRR: <b>${mrr}/month</b>\n\n"
            f"📈 <b>SIGNALS (recent cache)</b>\n"
            f"Total: {total_signals}\n"
            f"🏦 {bond_count} | 🔄 {arb_count} | "
            f"🐋 {whale_sig} | 🆕 {new_mkt}\n\n"
            f"📱 <b>TIERS</b>\n"
            f"🆓 Free: {free_count} | ⭐ Pro: {pro_count} | "
            f"💎 Whale: {whale_count}\n\n"
            f"🏷 <b>TOP CATEGORIES</b>\n"
        )
        for cat_key, count in top_cats:
            ci = CATEGORIES.get(cat_key, {})
            label = ci.get("label", cat_key) if isinstance(ci, dict) else cat_key
            msg += f"  {label}: {count}\n"
        msg += (
            f"\n📡 <b>SIGNAL PREFS</b>\n"
        )
        for sk, cnt in sorted(sig_counts.items(), key=lambda x: x[1], reverse=True)[:4]:
            si = SIGNAL_TYPES.get(sk, {})
            sl = si.get("label", sk) if isinstance(si, dict) else sk
            msg += f"  {sl}: {cnt}\n"
        msg += (
            f"\n⏱ Uptime: {uh}h {um}m\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"/users — user list\n"
            f"/user &lt;id&gt; — lookup user\n"
            f"/broadcast &lt;msg&gt; — send to all"
        )
        self._send(chat_id, msg)
    def _cmd_users(self, chat_id: str):
        """List all users — admin only."""
        if not self._is_admin(chat_id):
            return
        now = time.time()
        # Build user list sorted by most recent activity
        user_list = []
        for uid, pref in self.user_prefs.items():
            if not isinstance(pref, dict):
                continue
            uname = pref.get("username", "")
            fname = pref.get("first_name", "")
            name = f"@{uname}" if uname else fname or uid
            last = pref.get("last_active", 0)
            joined = pref.get("joined_at", 0)
            # Get tier
            sub = self.user_subs.get(uid, {})
            tier = sub.get("tier", "free") if isinstance(sub, dict) else "free"
            if isinstance(sub, dict) and sub.get("expires_at", 0) <= now and tier != "free":
                tier = "free"
            tier_emoji = TIERS.get(tier, TIERS["free"])["emoji"]
            # Active ago
            if last > 0:
                ago = int(now - last)
                if ago < 3600:
                    ago_str = f"{ago // 60}m ago"
                elif ago < 86400:
                    ago_str = f"{ago // 3600}h ago"
                else:
                    ago_str = f"{ago // 86400}d ago"
            else:
                ago_str = "never"
            user_list.append({
                "uid": uid, "name": name, "tier_emoji": tier_emoji,
                "last": last, "ago_str": ago_str,
                "joined": joined,
            })
        # Sort by last active (most recent first)
        user_list.sort(key=lambda x: x["last"], reverse=True)
        # Show first 25 (Telegram message limit)
        msg = f"👥 <b>ALL USERS</b> ({len(user_list)} total)\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, u in enumerate(user_list[:25], 1):
            msg += (
                f"{i}. {u['tier_emoji']} <b>{u['name']}</b> — "
                f"active {u['ago_str']}\n"
                f"   ID: <code>{u['uid']}</code>\n"
            )
        if len(user_list) > 25:
            msg += f"\n<i>... and {len(user_list) - 25} more</i>"
        msg += f"\n\n💡 /user &lt;id&gt; for details"
        self._send(chat_id, msg)
    def _cmd_user_lookup(self, chat_id: str, text: str):
        """Look up a specific user — admin only."""
        if not self._is_admin(chat_id):
            return
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            self._send(chat_id, "Usage: /user &lt;user_id&gt;")
            return
        target_id = parts[1].strip()
        # Allow lookup by @username
        if target_id.startswith("@"):
            found = None
            for uid, pref in self.user_prefs.items():
                if isinstance(pref, dict) and pref.get("username", "").lower() == target_id[1:].lower():
                    found = uid
                    break
            if not found:
                self._send(chat_id, f"❌ User {target_id} not found")
                return
            target_id = found
        pref = self.user_prefs.get(target_id)
        if not pref or not isinstance(pref, dict):
            self._send(chat_id, f"❌ User <code>{target_id}</code> not found")
            return
        now = time.time()
        uname = pref.get("username", "")
        fname = pref.get("first_name", "")
        name = f"@{uname}" if uname else fname or target_id
        joined = pref.get("joined_at", 0)
        last_active = pref.get("last_active", 0)
        cmds = pref.get("commands_used", 0)
        sig_pref = pref.get("signal", "all")
        cat_pref = pref.get("category", "all_cat")
        # Sub info
        sub = self.user_subs.get(target_id, {})
        if not isinstance(sub, dict):
            sub = {}
        tier = sub.get("tier", "free")
        expires = sub.get("expires_at", 0)
        total_signals = sub.get("total_signals", 0)
        subscribed_at = sub.get("subscribed_at", 0)
        # Check if expired
        if tier != "free" and expires > 0 and expires <= now:
            tier = "free (expired)"
        tier_info = TIERS.get(tier.split(" ")[0] if " " in tier else tier, TIERS["free"])
        # Format dates
        joined_str = datetime.fromtimestamp(joined, tz=timezone.utc).strftime("%b %d, %Y") if joined else "Unknown"
        last_str = ""
        if last_active > 0:
            ago = int(now - last_active)
            if ago < 3600:
                last_str = f"{ago // 60} min ago"
            elif ago < 86400:
                last_str = f"{ago // 3600} hours ago"
            else:
                last_str = f"{ago // 86400} days ago"
        else:
            last_str = "Never"
        expires_str = ""
        if expires > now:
            days_left = int((expires - now) / 86400)
            expires_str = f"{datetime.fromtimestamp(expires, tz=timezone.utc).strftime('%b %d')} ({days_left}d left)"
        elif tier != "free":
            expires_str = "EXPIRED"
        s = SIGNAL_TYPES.get(sig_pref, SIGNAL_TYPES["all"])
        c = CATEGORIES.get(cat_pref, CATEGORIES["all_cat"])
        msg = (
            f"🔍 <b>USER DETAILS</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Name: <b>{name}</b>\n"
            f"ID: <code>{target_id}</code>\n"
            f"Joined: {joined_str}\n"
            f"Last active: {last_str}\n"
            f"Commands used: {cmds}\n\n"
            f"<b>Subscription</b>\n"
            f"Tier: {tier_info['emoji']} {tier}\n"
        )
        if expires_str:
            msg += f"Expires: {expires_str}\n"
        msg += (
            f"Signals received: {total_signals}\n\n"
            f"<b>Preferences</b>\n"
            f"Signal: {s['emoji']} {s['label']}\n"
            f"Category: {c['emoji']} {c['label']}"
        )
        self._send(chat_id, msg)
    def _cmd_broadcast(self, chat_id: str, text: str):
        """Broadcast message to all users — admin only."""
        if not self._is_admin(chat_id):
            return
        parts = text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            self._send(chat_id, "Usage: /broadcast &lt;your message&gt;")
            return
        broadcast_text = parts[1].strip()
        broadcast_msg = (
            f"📢 <b>ANNOUNCEMENT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{broadcast_text}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>— {self.bot_name} Team</i>"
        )
        # Send in background thread to not block admin
        def _bg_broadcast():
            sent = 0
            failed = 0
            for uid in list(self.user_prefs.keys()):
                try:
                    ok = self._send(uid, broadcast_msg)
                    if ok:
                        sent += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1
                time.sleep(0.05)  # Rate limit: 20 msgs/sec max
            self._send(
                chat_id,
                f"📢 Broadcast complete: <b>{sent}</b> sent, {failed} failed"
            )
        threading.Thread(target=_bg_broadcast, daemon=True).start()
        self._send(chat_id, f"📢 Broadcasting to {len(self.user_prefs)} users...")
    # -----------------------------------------------------------------
    # Daily digest (background, lightweight)
    # -----------------------------------------------------------------
    def _start_daily_digest_thread(self):
        """Start background thread that sends daily digest at midnight UTC."""
        def _digest_loop():
            last_digest_date = ""
            while True:
                try:
                    now_utc = datetime.now(timezone.utc)
                    today_str = now_utc.strftime("%Y-%m-%d")
                    # Send digest once per day after midnight UTC
                    if now_utc.hour == 0 and now_utc.minute < 10 and today_str != last_digest_date:
                        self._send_daily_digest()
                        last_digest_date = today_str
                    time.sleep(300)  # Check every 5 minutes
                except Exception as e:
                    logger.error(f"Digest thread error: {e}")
                    time.sleep(300)
        t = threading.Thread(target=_digest_loop, daemon=True)
        t.start()
    def _send_daily_digest(self):
        """Send daily summary to admin."""
        if not self.default_chat_id:
            return
        now = time.time()
        total = len(self.user_prefs)
        # Today's joins
        today_joins = []
        for uid, p in self.user_prefs.items():
            if isinstance(p, dict) and now - p.get("joined_at", 0) < 86400:
                uname = p.get("username", "")
                fname = p.get("first_name", "")
                name = f"@{uname}" if uname else fname or uid
                today_joins.append(name)
        # Active today
        active_today = sum(
            1 for p in self.user_prefs.values()
            if isinstance(p, dict) and now - p.get("last_active", 0) < 86400
        )
        # Revenue
        pro_count = sum(1 for s in self.user_subs.values()
                        if isinstance(s, dict) and s.get("tier") == "pro"
                        and s.get("expires_at", 0) > now)
        whale_count = sum(1 for s in self.user_subs.values()
                          if isinstance(s, dict) and s.get("tier") == "whale_tier"
                          and s.get("expires_at", 0) > now)
        mrr = (pro_count * 6) + (whale_count * 15)
        # Expiring soon (next 3 days)
        expiring_soon = []
        for uid, s in self.user_subs.items():
            if not isinstance(s, dict):
                continue
            exp = s.get("expires_at", 0)
            if 0 < exp - now < 3 * 86400 and s.get("tier", "free") != "free":
                pref = self.user_prefs.get(uid, {})
                name = pref.get("username", uid) if isinstance(pref, dict) else uid
                expiring_soon.append(name)
        # Signals today
        total_signals = sum(len(v) for v in self.history.values())
        # Bond tracker stats
        today_bonds = sum(
            1 for b in self.bond_tracker if now - b.get("ts", 0) < 86400
        )
        date_str = datetime.now(timezone.utc).strftime("%b %d, %Y")
        msg = (
            f"📋 <b>DAILY DIGEST — {date_str}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 Users: <b>{total}</b> (+{len(today_joins)} today)\n"
            f"📱 Active today: {active_today}/{total}\n"
            f"💰 MRR: <b>${mrr}/mo</b> "
            f"(⭐{pro_count} + 💎{whale_count})\n"
            f"📈 Bond signals today: {today_bonds}\n"
            f"📊 Signal cache: {total_signals}\n"
        )
        if today_joins:
            names = ", ".join(today_joins[:10])
            msg += f"\n🆕 <b>New users:</b> {names}"
            if len(today_joins) > 10:
                msg += f" +{len(today_joins) - 10} more"
            msg += "\n"
        if expiring_soon:
            msg += f"\n⚠️ <b>Expiring in 3 days:</b> {', '.join(expiring_soon[:5])}\n"
        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 /admin for live dashboard"
        )
        self._send_admin(msg)
    def _cmd_reset(self, chat_id: str):
        with self._lock:
            self.user_prefs.pop(chat_id, None)
            self._user_seen.pop(chat_id, None)
            self._save_prefs()
        self._cmd_start(chat_id)
    def _cmd_upgrade(self, chat_id: str):
        """Show upgrade plans with payment options."""
        tier = self._get_tier(chat_id)
        if tier != "free":
            tier_info = TIERS.get(tier, TIERS["free"])
            sub = self._get_user_sub(chat_id)
            expiry = sub.get("expires_at", 0)
            
            if str(chat_id) == self.default_chat_id:
                exp_str = "Lifetime (Admin)"
                days_left = "∞"
            else:
                days_left = max(0, int((expiry - time.time()) / 86400))
                exp_str = datetime.fromtimestamp(
                    expiry, tz=timezone.utc
                ).strftime("%b %d, %Y")
            msg = (
                f"✅ <b>YOU'RE ON {tier_info['label'].upper()}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"\n"
                f"Plan: <b>{tier_info['emoji']} {tier_info['label']}</b>\n"
                f"Expires: <b>{exp_str}</b> ({days_left} days left)\n"
                f"\n"
                f"Want to extend or upgrade?\n"
                f"Tap below to add 30 more days:"
            )
            self._send(chat_id, msg, self._upgrade_keyboard(chat_id, tier))
            return
        msg = (
            f"🚀 <b>UPGRADE YOUR PLAN</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"You're on <b>🆓 Free</b> (5 signals/day)\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"⭐ <b>PRO — $6/month</b>\n"
            f"  ✓ Unlimited signals\n"
            f"  ✓ Real-time alerts (zero delay)\n"
            f"  ✓ All 6 signal types\n"
            f"\n"
            f"💎 <b>WHALE — $15/month</b>\n"
            f"  ✓ Everything in Pro\n"
            f"  ✓ Priority alert delivery\n"
            f"  ✓ Whale trade details\n"
            f"  ✓ Daily summary digest\n"
            f"\n"
            f"🌟 <i>Save ~17% with yearly plans!</i>\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🟢 <i>Pay with USDC — same crypto\n"
            f"you use on Polymarket!</i>\n"
            f"\n"
            f"👇 <b>Choose your payment method:</b>"
        )
        self._send(chat_id, msg, self._upgrade_keyboard(chat_id, "pro"))
    def _show_tier_details(self, chat_id: str, tier_key: str,
                           yearly: bool = False):
        """Show payment options for a specific tier."""
        tier = TIERS.get(tier_key, TIERS["pro"])
        if yearly:
            price = f"${tier.get('price_usd_yr', tier['price_usd'])}/year"
        else:
            price = f"${tier['price_usd']}/month"
        if tier_key == "whale_tier":
            features = (
                f"💎 <b>WHALE PLAN — {price}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"  ✓ Everything in Pro\n"
                f"  ✓ Priority alert delivery\n"
                f"  ✓ Whale convergence details\n"
                f"  ✓ Daily performance summary\n"
                f"  ✓ 30-day access"
            )
        else:
            features = (
                f"⭐ <b>PRO PLAN — {price}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"  ✓ Unlimited signals\n"
                f"  ✓ Real-time (zero delay)\n"
                f"  ✓ All 6 signal types\n"
                f"  ✓ 30-day access"
            )
        msg = f"{features}\n\n👇 <b>Choose payment method:</b>"
        self._send(chat_id, msg, self._upgrade_keyboard(chat_id, tier_key, yearly))
    # -----------------------------------------------------------------
    # External Payment Handlers
    # -----------------------------------------------------------------
    def _show_chain_selector(self, chat_id: str, tier_key: str,
                             dur: str = "mo"):
        """Show available USDC chains to the user."""
        tier = TIERS.get(tier_key, TIERS["pro"])
        if dur == "yr":
            price = tier.get("price_usd_yr", tier["price_usd"])
        else:
            price = tier["price_usd"]
        available = [(k, v) for k, v in USDC_CHAINS.items() if v["addr"]]
        if not available:
            self._send(chat_id, "❌ No USDC wallets configured yet. Please use Telegram Stars.")
            return
        buttons = []
        for chain_key, chain in available:
            buttons.append([{
                "text": f"{chain['emoji']} {chain['label']}",
                "callback_data": f"chain:{chain_key}:{tier_key}:{dur}",
            }])
        buttons.append([{"text": "📋 Back", "callback_data": "cmd:upgrade"}])
        msg = (
            f"🟢 <b>SELECT CHAIN FOR USDC</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Amount: <b>${price} USDC</b>\n"
            f"\n"
            f"Choose which blockchain to send on:\n"
            f"\n"
        )
        for chain_key, chain in available:
            msg += f"{chain['emoji']} <b>{chain['short']}</b> — {chain['note']}\n"
        msg += f"\n👇 <b>Tap your preferred chain:</b>"
        self._send(chat_id, msg, {"inline_keyboard": buttons})
    def _handle_crypto_payment(self, chat_id: str, tier_key: str,
                               chain: str = "polygon", dur: str = "mo"):
        """Show USDC wallet address for a specific chain."""
        chain_info = USDC_CHAINS.get(chain)
        if not chain_info or not chain_info["addr"]:
            self._send(chat_id, f"❌ {chain.title()} not configured. Pick another chain.")
            return
        tier = TIERS.get(tier_key, TIERS["pro"])
        if dur == "yr":
            price = tier.get("price_usd_yr", tier["price_usd"])
            period = "year"
        else:
            price = tier["price_usd"]
            period = "month"
        addr = chain_info["addr"]
        network_label = chain_info["short"]
        ref = f"PM{chat_id[-6:]}{int(time.time()) % 100000}"
        self._pending_payments[chat_id] = {
            "tier": tier_key, "method": f"USDC ({network_label})",
            "ts": time.time(), "ref": ref, "amount": f"${price} USDC",
            "dur": dur,
        }
        confirm_kb = {"inline_keyboard": [
            [{"text": "✅ I've Paid", "callback_data": f"paid:{tier_key}:{dur}"}],
            [{"text": "📋 Back", "callback_data": "cmd:upgrade"}],
        ]}
        msg = (
            f"🟢 <b>PAY WITH USDC</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Amount: <b>${price} USDC</b>\n"
            f"Chain: <b>{chain_info['emoji']} {network_label}</b>\n"
            f"Duration: <b>1 {period}</b>\n"
            f"\n"
            f"Send to this address:\n"
            f"<code>{addr}</code>\n"
            f"\n"
            f"⚠️ Send <b>exactly ${price} USDC</b> on\n"
            f"<b>{network_label}</b> only!\n"
            f"Ref: <code>{ref}</code>\n"
            f"\n"
            f"After sending, tap <b>'I've Paid'</b> ✅"
        )
        self._send(chat_id, msg, confirm_kb)
    def _handle_card_payment(self, chat_id: str, tier_key: str,
                             dur: str = "mo"):
        """Send Stripe payment link — works globally."""
        ref = f"PM{chat_id[-6:]}{int(time.time()) % 100000}"
        tier = TIERS.get(tier_key, TIERS["pro"])
        if dur == "yr":
            price_usd = tier.get("price_usd_yr", tier["price_usd"])
            s_suffix = "_yr"
        else:
            price_usd = tier["price_usd"]
            s_suffix = ""
        link_key = f"stripe_link_{'pro' if tier_key == 'pro' else 'whale'}{s_suffix}"
        link = PAYMENT_CONFIG.get(link_key, "")
        if not link:
            # Fallback: suggest Stars instead
            self._send(
                chat_id,
                f"💳 Card payments coming soon!\n\n"
                f"For now, use <b>⭐ Telegram Stars</b> — it's instant "
                f"and works with Apple Pay, Google Pay, or card.\n\n"
                f"Tap /upgrade to pay with Stars."
            )
            return
        # Append chat_id as reference
        sep = "&" if "?" in link else "?"
        full_link = f"{link}{sep}client_reference_id={chat_id}&ref={ref}"
        price = f"${price_usd}"
        self._pending_payments[chat_id] = {
            "tier": tier_key, "method": "Stripe",
            "ts": time.time(), "ref": ref, "amount": price,
        }
        confirm_kb = {"inline_keyboard": [
            [{"text": f"💳 Pay {price}", "url": full_link}],
            [{"text": "✅ I've Paid", "callback_data": f"paid:{tier_key}"}],
            [{"text": "📋 Back", "callback_data": "cmd:upgrade"}],
        ]}
        msg = (
            f"💳 <b>PAY WITH CARD</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Amount: <b>{price}</b>\n"
            f"Provider: Stripe (secure)\n"
            f"Ref: <code>{ref}</code>\n"
            f"\n"
            f"Accepts: Visa, Mastercard, Apple Pay,\n"
            f"Google Pay, PayPal, and more.\n"
            f"\n"
            f"Tap the button below to pay securely.\n"
            f"After payment, tap <b>'I've Paid'</b> ✅"
        )
        self._send(chat_id, msg, confirm_kb)
    def _handle_paid_confirmation(self, chat_id: str, tier_key: str,
                                   dur: str = "mo"):
        """User clicked 'I've Paid' — notify admin for verification."""
        pending = self._pending_payments.pop(chat_id, None)
        method = pending["method"] if pending else "Unknown"
        ref = pending["ref"] if pending else "N/A"
        amount = pending["amount"] if pending else "?"
        dur = pending.get("dur", dur) if pending else dur
        period = "yearly" if dur == "yr" else "monthly"
        # Notify the bot owner (admin)
        admin_id = self.default_chat_id
        admin_msg = (
            f"🔔 <b>PAYMENT VERIFICATION NEEDED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"User: <code>{chat_id}</code>\n"
            f"Plan: <b>{TIERS.get(tier_key, TIERS['pro'])['label']}</b>\n"
            f"Duration: <b>{period}</b>\n"
            f"Method: <b>{method}</b>\n"
            f"Amount: <b>{amount}</b>\n"
            f"Ref: <code>{ref}</code>\n"
            f"\n"
            f"To activate, send:\n"
            f"<code>/approve {chat_id} {tier_key} {dur}</code>"
        )
        self._send(admin_id, admin_msg)
        # Confirm to user
        user_msg = (
            f"✅ <b>Payment submitted!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Method: {method}\n"
            f"Ref: <code>{ref}</code>\n"
            f"\n"
            f"We're verifying your payment now.\n"
            f"Your plan will activate within minutes!\n"
            f"\n"
            f"⏳ Usually takes less than 5 minutes."
        )
        self._send(chat_id, user_msg)
        logger.info(f"Payment verification requested: {chat_id} → {tier_key} via {method} (ref: {ref})")
    def _cmd_approve(self, chat_id: str, text: str):
        """Admin command: /approve <user_id> <tier_key> [mo|yr]"""
        if chat_id != self.default_chat_id:
            return  # Only admin can approve
        parts = text.split()
        if len(parts) < 3:
            self._send(chat_id, "Usage: /approve <user_id> <tier_key> [mo|yr]")
            return
        user_id = parts[1]
        tier_key = parts[2]
        dur = parts[3] if len(parts) > 3 else "mo"
        if tier_key not in TIERS:
            self._send(chat_id, f"❌ Unknown tier: {tier_key}. Use: pro, whale_tier")
            return
        # Activate subscription
        now = time.time()
        duration = SUB_YEARLY if dur == "yr" else SUB_MONTHLY
        expiry = now + duration
        sub = self._get_user_sub(user_id)
        sub["tier"] = tier_key
        sub["expires_at"] = expiry
        sub["subscribed_at"] = now
        sub["expiry_reminded"] = 0  # Reset renewal reminders
        self._save_subs()
        tier_info = TIERS[tier_key]
        exp_str = datetime.fromtimestamp(
            expiry, tz=timezone.utc
        ).strftime("%b %d, %Y")
        period = "1 year" if dur == "yr" else "30 days"
        # Notify admin
        self._send(chat_id, f"✅ Activated {tier_info['label']} ({period}) for user {user_id} until {exp_str}")
        # Notify user
        user_msg = (
            f"🎉 <b>PAYMENT VERIFIED!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Plan: <b>{tier_info['emoji']} {tier_info['label']}</b>\n"
            f"Duration: <b>{period}</b>\n"
            f"Expires: <b>{exp_str}</b>\n"
            f"\n"
            f"✅ Unlimited real-time signals activated!\n"
            f"✅ Zero delay on all alerts\n"
            f"✅ All 6 signal types unlocked\n"
            f"\n"
            f"Thank you for supporting {self.bot_name}! 🚀"
        )
        self._send(user_id, user_msg)
        logger.info(f"Admin approved payment: {user_id} → {tier_key} ({period}) until {exp_str}")
    # -----------------------------------------------------------------
    # Admin — Ban / Unban / Revoke Access
    # -----------------------------------------------------------------
    def _cmd_ban(self, chat_id: str, text: str):
        """Admin command: /ban <user_id> [reason]"""
        if chat_id != self.default_chat_id:
            return
        parts = text.split(maxsplit=2)
        if len(parts) < 2:
            self._send(chat_id, "Usage: /ban &lt;user_id&gt; [reason]")
            return
        target_id = parts[1].strip()
        reason = parts[2].strip() if len(parts) > 2 else "No reason given"
        if target_id == str(self.default_chat_id):
            self._send(chat_id, "❌ Cannot ban yourself.")
            return
        self._ban_user(target_id)
        # Silent ban — user is NOT notified (they just stop receiving signals)
        # Count active bans
        ban_count = len(self.banned_users)
        pref = self.user_prefs.get(target_id, {})
        name = ""
        if isinstance(pref, dict):
            uname = pref.get("username", "")
            name = f"@{uname}" if uname else pref.get("first_name", target_id)
        else:
            name = target_id
        self._send(chat_id,
            f"⛔ <b>BANNED:</b> {name} (<code>{target_id}</code>)\n"
            f"Reason: {reason}\n"
            f"Total banned: {ban_count}\n\n"
            f"Use /unban {target_id} to restore access."
        )
        logger.info(f"Admin banned user {target_id}: {reason}")
    def _cmd_unban(self, chat_id: str, text: str):
        """Admin command: /unban <user_id>"""
        if chat_id != self.default_chat_id:
            return
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            self._send(chat_id, "Usage: /unban &lt;user_id&gt;")
            return
        target_id = parts[1].strip()
        if target_id not in self.banned_users:
            self._send(chat_id, f"ℹ️ User <code>{target_id}</code> is not banned.")
            return
        self._unban_user(target_id)
        # Silent unban — user is NOT notified (they can use /start naturally)
        self._send(chat_id,
            f"✅ Unbanned <code>{target_id}</code>. "
            f"Remaining banned: {len(self.banned_users)}"
        )
        logger.info(f"Admin unbanned user {target_id}")
    def _cmd_banned_list(self, chat_id: str):
        """Admin command: /banned — show all banned users."""
        if chat_id != self.default_chat_id:
            return
        if not self.banned_users:
            self._send(chat_id, "✅ No banned users.")
            return
        msg = f"⛔ <b>BANNED USERS</b> ({len(self.banned_users)})\n━━━━━━━━━━━━━━━━━━\n\n"
        for uid in self.banned_users:
            pref = self.user_prefs.get(uid, {})
            if isinstance(pref, dict):
                uname = pref.get("username", "")
                name = f"@{uname}" if uname else pref.get("first_name", uid)
            else:
                name = uid
            msg += f"⛔ {name} — <code>{uid}</code>\n"
        msg += f"\nUse /unban &lt;id&gt; to restore access."
        self._send(chat_id, msg)
    # -----------------------------------------------------------------
    # Welcome signal — instant proof the bot works
    # -----------------------------------------------------------------
    def _send_welcome_signal(self, chat_id: str, sig_key: str, cat_key: str):
        """
        Send one recent signal immediately when user selects a signal type.
        This is the HOOK — proves the bot works before they leave.
        Pulls from in-memory history (if any signals exist).
        """
        past = self._get_history_for(sig_key, cat_key)
        if not past:
            # No history yet — tell user when to expect signals
            self._send(
                chat_id,
                f"📡 <i>Bot is scanning markets right now.\n"
                f"Your first {SIGNAL_TYPES.get(sig_key, SIGNAL_TYPES['all'])['emoji']} "
                f"signal will arrive within ~60 seconds!</i>"
            )
            return
        # Send the most recent matching signal as a preview
        latest = past[-1]
        msg = latest["msg"] if isinstance(latest, dict) else latest
        preview = (
            f"⚡ <b>LATEST SIGNAL — just for you:</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"{msg}\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 <i>More signals coming every 60 seconds.\n"
            f"You'll get up to 5/day free — /upgrade for unlimited.</i>"
        )
        self._send(chat_id, preview)
        logger.info(f"Sent welcome signal to {chat_id} ({sig_key})")
    # -----------------------------------------------------------------
    # Signal type selection + history replay
    # -----------------------------------------------------------------
    def _select_signal(self, chat_id: str, sig_key: str):
        """User selected a signal type. Confirm + show category picker."""
        if sig_key not in SIGNAL_TYPES:
            return
        s = SIGNAL_TYPES[sig_key]
        # Save signal preference
        with self._lock:
            self._set_signal(chat_id, sig_key)
            self._save_prefs()
            logger.info(f"User {chat_id} selected signal type: {sig_key}")
        current_cat = self._get_category(chat_id)
        c = CATEGORIES.get(current_cat, CATEGORIES["all_cat"])
        tier = self._get_tier(chat_id)
        tier_info = TIERS.get(tier, TIERS["free"])
        # Confirmation + prompt for category
        confirm = (
            f"✅ <b>Signal type updated!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Signal: <b>{s['emoji']} {s['label']}</b>\n"
            f"<i>{s['desc']}</i>\n"
            f"Category: <b>{c['emoji']} {c['label']}</b>\n"
            f"Plan: {tier_info['emoji']} {tier_info['label']}\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏷 <b>Step 2 (optional):</b> Want to filter by category?\n"
            f"Tap below or skip — you'll get all categories by default."
        )
        self._send(chat_id, confirm, self._category_keyboard(current_cat))
        # Welcome signal: send one recent signal immediately to prove bot works
        self._send_welcome_signal(chat_id, sig_key, current_cat)
    def _select_category(self, chat_id: str, cat_key: str):
        """User selected a category. Confirm + replay matching history."""
        if cat_key not in CATEGORIES:
            return
        c = CATEGORIES[cat_key]
        # Save category preference
        with self._lock:
            self._set_category(chat_id, cat_key)
            self._save_prefs()
            logger.info(f"User {chat_id} selected category: {cat_key}")
        current_sig = self._get_signal(chat_id)
        s = SIGNAL_TYPES.get(current_sig, SIGNAL_TYPES["all"])
        tier = self._get_tier(chat_id)
        tier_info = TIERS.get(tier, TIERS["free"])
        past = self._get_history_for(current_sig, cat_key)
        confirm = (
            f"✅ <b>Category updated!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Signal: <b>{s['emoji']} {s['label']}</b>\n"
            f"Category: <b>{c['emoji']} {c['label']}</b>\n"
            f"Plan: {tier_info['emoji']} {tier_info['label']}\n"
            f"\n"
        )
        if past:
            confirm += (
                f"📜 Sending <b>{len(past)}</b> recent matching signal"
                f"{'s' if len(past) != 1 else ''}...\n"
            )
        else:
            confirm += (
                f"📡 Bot is actively scanning — your first signal\n"
                f"will arrive within ~60 seconds!\n"
            )
        confirm += (
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Use /menu or /category to switch anytime."
        )
        self._send(chat_id, confirm)
        # Replay past signals
        if past:
            time.sleep(0.5)
            header = (
                f"📜 <b>RECENT {s['label'].upper()}"
                f"{' — ' + c['label'].upper() if cat_key != 'all_cat' else ''}"
                f" SIGNALS</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Showing last {len(past)} signal"
                f"{'s' if len(past) != 1 else ''}:"
            )
            self._send(chat_id, header)
            for entry in past[-10:]:
                msg = entry["msg"] if isinstance(entry, dict) else entry
                self._send(chat_id, msg)
                time.sleep(0.5)
    def _get_history_for(self, sig_key: str, cat_key: str = "all_cat") -> list:
        """Get the last 10 past signals matching signal type + category."""
        s = SIGNAL_TYPES.get(sig_key, SIGNAL_TYPES["all"])
        # Collect entries matching signal type
        if s["opp_types"] is None:
            all_entries = []
            for entries in self.history.values():
                all_entries.extend(entries)
        else:
            all_entries = []
            for opp_type in s["opp_types"]:
                all_entries.extend(self.history.get(opp_type, []))
        # Sort by timestamp
        all_entries.sort(
            key=lambda e: e.get("ts", 0) if isinstance(e, dict) else 0
        )
        # Filter by category (search category + title + msg text for keywords)
        if cat_key != "all_cat":
            cat_info = CATEGORIES.get(cat_key, {})
            keywords = cat_info.get("keywords", [])
            if keywords:
                filtered = []
                for entry in all_entries:
                    entry_cat = ""
                    entry_title = ""
                    entry_msg = ""
                    if isinstance(entry, dict):
                        entry_cat = entry.get("category", "").lower()
                        entry_title = entry.get("title", "").lower()
                        entry_msg = entry.get("msg", "").lower()
                    # Search category + title + message text for keywords
                    text = f"{entry_title} {entry_cat} {entry_msg}"
                    if entry_cat == cat_key:
                        filtered.append(entry)
                    elif any(kw in text for kw in keywords):
                        filtered.append(entry)
                all_entries = filtered
        return all_entries[-10:]
    # -----------------------------------------------------------------
    # Free-tier limit check
    # -----------------------------------------------------------------
    def _check_signal_limit(self, chat_id: str) -> tuple[bool, int]:
        """Check if a user can receive more signals today."""
        tier = self._get_tier(chat_id)
        tier_info = TIERS.get(tier, TIERS["free"])
        limit = tier_info["daily_limit"]
        if limit >= 999999:
            return True, 999999
        sub = self._get_user_sub(chat_id)
        used = sub.get("daily_count", 0)
        remaining = max(0, limit - used)
        return remaining > 0, remaining
    def _increment_signal_count(self, chat_id: str, count: int = 1):
        """Increment daily signal count for a user."""
        with self._lock:
            sub = self._get_user_sub(chat_id)
            sub["daily_count"] = sub.get("daily_count", 0) + count
            sub["total_signals"] = sub.get("total_signals", 0) + count
            self.user_subs[chat_id] = sub
            self._save_subs()
    def _send_limit_reached(self, chat_id: str):
        """First-time daily limit hit — one-time upgrade prompt."""
        sub = self._get_user_sub(chat_id)
        if sub.get("limit_hit_today"):
            return  # Already sent the big prompt today
        sub["limit_hit_today"] = True
        self._save_subs()
        msg = (
            f"🔒 <b>Daily Limit Reached</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"You've used all <b>5 free signals</b> today.\n"
            f"\n"
            f"Upgrade to <b>⭐ Pro</b> for:\n"
            f"  ✓ Unlimited signals\n"
            f"  ✓ Real-time delivery (no delay)\n"
            f"  ✓ Just 300 Stars/month\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Tap to upgrade now:</b>"
        )
        self._send(chat_id, msg, self._upgrade_keyboard(chat_id))
    # -----------------------------------------------------------------
    # Subscription expiry reminders
    # -----------------------------------------------------------------
    def _maybe_send_expiry_reminder(self, chat_id: str):
        """
        Send renewal reminders at 3 days and 1 day before expiry.
        Uses 'expiry_reminded' field to avoid spamming.
        """
        sub = self._get_user_sub(chat_id)
        expires_at = sub.get("expires_at", 0)
        if expires_at <= 0:
            return
        now = time.time()
        days_left = (expires_at - now) / 86400
        # Already reminded at this threshold?
        # 0=none sent, 3=3-day sent, 1=1-day sent
        reminded = sub.get("expiry_reminded", 0)
        tier = sub.get("tier", "free")
        tier_info = TIERS.get(tier, TIERS["free"])
        if days_left <= 1 and reminded != 1:
            # URGENT: expires tomorrow (fires even if 3-day was already sent)
            msg = (
                f"⚠️ <b>Your {tier_info['emoji']} {tier_info['label']} plan "
                f"expires TOMORROW!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"\n"
                f"After expiry you'll be back on Free tier:\n"
                f"  • 5 signals/day limit\n"
                f"  • 5-min delay on signals #2+\n"
                f"  • No whale or new market signals\n"
                f"\n"
                f"Renew now to keep unlimited real-time access.\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💡 /upgrade to renew"
            )
            self._send(chat_id, msg, self._upgrade_keyboard(chat_id))
            sub["expiry_reminded"] = 1
            self._save_subs()
            logger.info(f"Sent 1-day expiry reminder to {chat_id}")
        elif days_left <= 3 and reminded == 0:
            # WARNING: 3 days left (only if no reminder sent yet)
            days_int = max(1, int(days_left))
            msg = (
                f"📢 <b>Heads up — your {tier_info['emoji']} {tier_info['label']} "
                f"plan expires in {days_int} days</b>\n"
                f"\n"
                f"Don't lose access to unlimited real-time signals.\n"
                f"Renew early to avoid any gap.\n"
                f"\n"
                f"💡 /upgrade to renew"
            )
            self._send(chat_id, msg, self._upgrade_keyboard(chat_id))
            sub["expiry_reminded"] = 3
            self._save_subs()
            logger.info(f"Sent 3-day expiry reminder to {chat_id}")
    # -----------------------------------------------------------------
    # HOOK 1: Ghost alerts
    # -----------------------------------------------------------------
    def _send_ghost_alert(self, chat_id: str, missed_count: int,
                          missed_profit: float):
        """
        HOOK 1: Ghost alerts — show free users what they're MISSING.
        Sent every scan cycle (rate-limited) after daily limit is hit.
        """
        now = time.time()
        if now - self._ghost_last_sent.get(chat_id, 0) < self._ghost_cooldown:
            return  # Don't spam
        self._ghost_last_sent[chat_id] = now
        # Track cumulative missed profit in sub data
        sub = self._get_user_sub(chat_id)
        sub["missed_profit_week"] = sub.get("missed_profit_week", 0) + missed_profit
        sub["missed_count_week"] = sub.get("missed_count_week", 0) + missed_count
        self._save_subs()
        msg = (
            f"🔒 <b>{missed_count} new signal"
            f"{'s' if missed_count != 1 else ''} just found</b>\n"
            f"\n"
            f"Pro users received them. You didn't.\n"
            f"Estimated return: <b>${missed_profit:.2f}</b>\n"
            f"\n"
            f"💡 <i>/upgrade to never miss a signal</i>"
        )
        self._send(chat_id, msg)
    def _maybe_send_weekly_summary(self, chat_id: str):
        """
        HOOK 3: Missed-profit counter — weekly loss-aversion nudge.
        Checks if 7 days since last summary, sends if so.
        """
        sub = self._get_user_sub(chat_id)
        last_summary = sub.get("last_missed_summary", 0)
        if time.time() - last_summary < 7 * 86400:
            return  # Not time yet
        missed_profit = sub.get("missed_profit_week", 0)
        missed_count = sub.get("missed_count_week", 0)
        if missed_count == 0:
            return  # Nothing to report
        msg = (
            f"📊 <b>YOUR WEEKLY SIGNAL REPORT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Signals you missed: <b>{missed_count}</b>\n"
            f"Potential profit missed: <b>${missed_profit:.2f}</b>\n"
            f"\n"
            f"That's real money left on the table.\n"
            f"Pro users captured every one of these.\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 <b>Upgrade for just 300 Stars/month</b>\n"
            f"and never miss another signal."
        )
        self._send(chat_id, msg, self._upgrade_keyboard(chat_id))
        # Reset weekly counters
        sub["missed_profit_week"] = 0
        sub["missed_count_week"] = 0
        sub["last_missed_summary"] = time.time()
        self._save_subs()
    def _maybe_send_free_preview(self, chat_id: str,
                                  opp: Opportunity) -> bool:
        """
        HOOK 2: Weekly free preview — one premium signal per week.
        Returns True if a preview was sent (caller should skip normal limit).
        """
        sub = self._get_user_sub(chat_id)
        last_preview = sub.get("last_free_preview", 0)
        if time.time() - last_preview < 7 * 86400:
            return False  # Already gave a preview this week
        # Send the preview signal
        msg_text = format_opportunity(opp)
        preview_msg = (
            f"⭐ <b>FREE PREVIEW — THIS WEEK ONLY</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"You've hit your daily limit, but here's\n"
            f"a taste of what Pro users get:\n"
            f"\n"
            f"{msg_text}\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⭐ <i>Pro users get unlimited signals like this.\n"
            f"/upgrade to unlock everything.</i>"
        )
        self._send(chat_id, preview_msg, self._upgrade_keyboard(chat_id))
        sub["last_free_preview"] = time.time()
        self._save_subs()
        logger.info(f"  Sent weekly free preview to {chat_id}")
        return True
    # -----------------------------------------------------------------
    # Signal Distribution
    # -----------------------------------------------------------------
    def _process_delayed_messages(self):
        """Background thread to process delayed messages for free users."""
        while True:
            time.sleep(10)  # Check every 10 seconds
            now = time.time()
            remaining_queue = []
            
            # Identify messages ready to send
            ready_to_send = []
            with self._lock:
                for item in self.delayed_queue:
                    chat_id, msg, release_time = item
                    if now >= release_time:
                        ready_to_send.append(item)
                    else:
                        remaining_queue.append(item)
                self.delayed_queue = remaining_queue
            # Send ready messages
            for chat_id, msg, _ in ready_to_send:
                # Re-check limit just in case they spammed recently
                can_send, _ = self._check_signal_limit(chat_id)
                if can_send:
                    self._send(chat_id, msg)
                    self._increment_signal_count(chat_id, 1)
                else:
                    self._send_limit_reached(chat_id)
    def distribute_signals(self, opportunities: list[Opportunity], cfg: dict):
        """
        Send opportunities to all registered users, filtered by
        signal type + category + tier limits.
        Per-user dedup ensures same signal isn't repeated within cooldown.
        THIS IS THE ONLY PATH FOR SENDING SIGNALS.
        """
        if not self.enabled or not opportunities:
            return
        min_pct = cfg["telegram"].get("min_alert_profit_pct", 0.5)
        filtered = [o for o in opportunities if o.profit_pct >= min_pct]
        if not filtered:
            return
        # Store in history (with full structured data for results tracking)
        now = time.time()
        for opp in filtered:
            msg = format_opportunity(opp)

            # v3.0: Record signal in PnL tracker
            if self.pnl_tracker:
                try:
                    self.pnl_tracker.record_signal(opp)
                except Exception:
                    pass

            entry = {
                "ts": now,
                "msg": msg,
                "category": opp.category,
                "title": opp.title,
                "opp_type": opp.opp_type,
                "profit_pct": opp.profit_pct,
                "profit_amount": opp.profit_amount,
                "total_cost": opp.total_cost,
                "platforms": opp.platforms,
                "risk_level": opp.risk_level,
                "hold_time": opp.hold_time,
                "legs": opp.legs,
                "edge_score": getattr(opp, 'edge_score', 0),
            }
            with self._lock:
                self.history[opp.opp_type].append(entry)
            # Track bonds separately for marketing results
            if opp.opp_type == "high_prob_bond":
                self._track_bond(opp, now)
        # Persist history to disk
        with self._lock:
            self._save_history()
        # Build user list — ALWAYS use string keys
        with self._lock:
            users = dict(self.user_prefs)
        # Add default chat_id ONLY if not already registered
        default_id = str(self.default_chat_id)
        if default_id and default_id not in users:
            users[default_id] = {"signal": "all", "category": "all_cat"}
        logger.info(
            f"Distributing {len(filtered)} signals to {len(users)} user(s)"
        )
        # For each user, filter by signal type + category + dedup + tier
        for chat_id, pref in users.items():
            # Handle both old and new pref format
            if isinstance(pref, str):
                sig_key = pref
                cat_key = "all_cat"
            else:
                sig_key = pref.get("signal", "all")
                cat_key = pref.get("category", "all_cat")
                dur_key = pref.get("duration", "all_dur")
            s = SIGNAL_TYPES.get(sig_key, SIGNAL_TYPES["all"])
            c = CATEGORIES.get(cat_key, CATEGORIES["all_cat"])
            want_types = s["opp_types"]  # None means all
            # --- TIER GATING ---
            tier = self._get_tier(chat_id)
            can_send, remaining = self._check_signal_limit(chat_id)
            if not can_send:
                # Count what they WOULD have received
                missed_opps = []
                for opp in filtered:
                    if want_types is not None and opp.opp_type not in want_types:
                        continue
                    if not self._matches_category(opp, cat_key):
                        continue
                    if not self._matches_duration(opp, dur_key, now):
                        continue
                    # Bug #2 Fix: Don't count whale/sniper as valid missed if user can't see them anyway
                    if opp.opp_type in ["whale_convergence", "new_market"] and tier == "free":
                        continue
                    missed_opps.append(opp)
                if missed_opps:
                    # HOOK 2: Weekly free preview (one taste per week)
                    self._maybe_send_free_preview(chat_id, missed_opps[0])
                    # HOOK 1: Ghost alerts ("you missed X signals")
                    missed_profit = sum(o.profit_amount for o in missed_opps)
                    self._send_ghost_alert(
                        chat_id, len(missed_opps), missed_profit
                    )
                else:
                    self._send_limit_reached(chat_id)
                # HOOK 3: Weekly missed-profit summary
                self._maybe_send_weekly_summary(chat_id)
                logger.info(
                    f"  User {chat_id}: limit reached — "
                    f"ghost alert ({len(missed_opps)} missed)"
                )
                continue
            tier_info = TIERS.get(tier, TIERS["free"])
            # Check if paid subscription is about to expire
            if tier != "free":
                self._maybe_send_expiry_reminder(chat_id)
            # Filter by signal type + category + per-user dedup
            user_seen = self._user_seen.setdefault(chat_id, {})
            user_opps = []
            for opp in filtered:
                # Bug #1 Fix: Allow Pro users to see whale/sniper (only block free)
                if opp.opp_type in ["whale_convergence", "new_market"] and tier == "free":
                    continue
                if want_types is not None and opp.opp_type not in want_types:
                    continue
                # Filter by category
                if not self._matches_category(opp, cat_key):
                    continue
                # Filter by duration (instant return for all_dur default)
                if not self._matches_duration(opp, dur_key, now):
                    continue
                # Per-user dedup: hash includes type + title + price + end_date
                # so price changes generate new alerts but same opp doesn't repeat
                dedup_key = (
                    f"{opp.opp_type}:{opp.title[:50]}:"
                    f"{round(opp.profit_pct, 1)}:"
                    f"{opp.hold_time[:10] if opp.hold_time else ''}"
                )
                # Configurable cooldown from config (default 30 min)
                cooldown = cfg.get('dedup', {}).get('cooldown_seconds', self.dedup_cooldown)
                if now - user_seen.get(dedup_key, 0) < cooldown:
                    continue
                user_seen[dedup_key] = now
                user_opps.append(opp)
            
            if not user_opps:
                logger.info(
                    f"  User {chat_id} ({sig_key}+{cat_key}+{dur_key}): "
                    f"0 new matching — skipping"
                )
                continue
            # Cap signals for free users (preview check)
            # We don't increment yet for free users because they are queued
            if tier == "free":
                user_opps = user_opps[:remaining]
            logger.info(
                f"  User {chat_id} ({sig_key}+{cat_key}, {tier}): "
                f"processing {len(user_opps)} signals"
            )
            # Send summary header
            type_counts: dict[str, int] = {}
            for o in user_opps:
                type_counts[o.opp_type] = type_counts.get(o.opp_type, 0) + 1
            cat_label = (
                f" — {c['emoji']} {c['label']}"
                if cat_key != "all_cat" else ""
            )
            live_tag = "🟢 LIVE" if tier != "free" else "🕐 DELAYED"
            summary = (
                f"📡 <b>{s['emoji']} {s['label'].upper()}"
                f"{cat_label} — {live_tag}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Found <b>{len(user_opps)}</b> signal"
                f"{'s' if len(user_opps) != 1 else ''}:\n"
            )
            
            # Bug #3 Fix: Warn free users about delay immediately
            if tier == "free":
                summary += f"\n🟢 Signal #1 arrives instantly!"
                summary += f"\n🕐 Remaining signals delayed 5 min"
                summary += f"\n💡 /upgrade for all signals in real-time\n"
            type_labels = {
                "cross_platform_arb": "🔄 Arb",
                "high_prob_bond": "🏦 Bonds",
                "intra_market_arb": "🎯 Intra-market",
                "whale_convergence": "🐋 Whales",
                "new_market": "🆕 New markets",
                "anti_hype": "🔻 Anti-Hype",
                "data_arb": "📊 Data Arb",
                "longshot": "🎯 Longshots",
                "resolution_intel": "🔍 Resolution",
                "micro_arb": "⚡ Micro Arb",
                "spread_arb": "📐 Spread Arb",
            }
            for t, count in type_counts.items():
                lbl = type_labels.get(t, t)
                summary += f"  {lbl}: <b>{count}</b>\n"
            if tier == "free":
                used_after = self._get_user_sub(chat_id).get("daily_count", 0) + len(user_opps)
                summary += f"\n📊 {used_after}/5 daily signals used"
                if used_after >= 5:
                    summary += "\n💡 /upgrade for unlimited"
            self._send(chat_id, summary)
            # Send individual signals (max 10 per user per cycle)
            sent_count = 0
            for i, opp in enumerate(user_opps[:10]):
                msg = format_opportunity(opp)
                
                if tier == "free":
                    if i == 0:
                        # FIRST signal: send IMMEDIATELY (the hook)
                        msg += (
                            f"\n\n🟢 <i>Signal #1 delivered instantly!</i>"
                            f"\n💡 <i>/upgrade for all signals in real-time</i>"
                        )
                        fb_kb = self._feedback_keyboard(opp)
                        self._send(chat_id, msg, fb_kb)
                        self._increment_signal_count(chat_id, 1)
                        self.signals_sent += 1
                        sent_count += 1
                        time.sleep(0.5)
                    else:
                        # Remaining signals: 5-min delay (not 30)
                        delay = 300  # 5 minutes
                        release_time = time.time() + delay
                        msg += (
                            f"\n\n🕐 <i>Delayed 5 min — Pro users got this instantly</i>"
                            f"\n💡 <i>/upgrade for real-time</i>"
                        )
                        with self._lock:
                            self.delayed_queue.append((chat_id, msg, release_time))
                else:
                    # Paid users: send IMMEDIATELY with feedback buttons
                    fb_kb = self._feedback_keyboard(opp)
                    self._send(chat_id, msg, fb_kb)
                    self.signals_sent += 1
                    sent_count += 1
                    time.sleep(0.5)
            # Track signal count for paid users (free first signal tracked above)
            if tier != "free":
                self._increment_signal_count(chat_id, sent_count)
            # Periodic menu reminder
            if self.signals_sent > 0 and self.signals_sent % 5 == 0:
                self._send(
                    chat_id,
                    f"💡 <i>/menu to switch signals · /category to filter · /upgrade for Pro</i>",
                    self._menu_button(),
                )

    # -----------------------------------------------------------------
    # Feedback System (Phase B)
    # -----------------------------------------------------------------
    def _feedback_keyboard(self, opp) -> dict:
        """Build inline keyboard with 👍/👎 vote buttons for a signal."""
        if not self.cfg.get("feedback", {}).get("enabled", True):
            return {}
        sig_hash = f"{opp.opp_type}:{opp.title[:30]}"[:50]
        return {
            "inline_keyboard": [[
                {"text": "👍 Good Signal", "callback_data": f"vote:up:{sig_hash}"},
                {"text": "👎 Bad Signal", "callback_data": f"vote:dn:{sig_hash}"},
            ]]
        }

    def _handle_vote(self, chat_id: str, vote: str, sig_hash: str):
        """Record a user's vote on a signal."""
        feedback_file = self.cfg.get("feedback", {}).get("file", "feedback.json")
        import json as _json
        import os
        try:
            if os.path.exists(feedback_file):
                with open(feedback_file, "r") as f:
                    data = _json.load(f)
            else:
                data = {"votes": [], "stats": {}}
        except (_json.JSONDecodeError, IOError):
            data = {"votes": [], "stats": {}}

        data["votes"].append({
            "ts": time.time(),
            "user": chat_id,
            "vote": vote,
            "signal": sig_hash,
        })

        # Keep last 1000 votes
        if len(data["votes"]) > 1000:
            data["votes"] = data["votes"][-1000:]

        # Update stats
        stats = data.setdefault("stats", {})
        # Extract opp_type from sig_hash
        opp_type = sig_hash.split(":")[0] if ":" in sig_hash else "unknown"
        type_stats = stats.setdefault(opp_type, {"up": 0, "dn": 0})
        type_stats[vote] = type_stats.get(vote, 0) + 1

        try:
            with open(feedback_file, "w") as f:
                _json.dump(data, f, indent=2)
        except IOError:
            pass

    def _cmd_feedback(self, chat_id: str):
        """Show feedback vote summary."""
        feedback_file = self.cfg.get("feedback", {}).get("file", "feedback.json")
        import json as _json
        import os
        try:
            with open(feedback_file, "r") as f:
                data = _json.load(f)
        except (FileNotFoundError, _json.JSONDecodeError, IOError):
            self._send(chat_id, "📊 No feedback data yet. Vote on signals with 👍/👎!")
            return

        stats = data.get("stats", {})
        total_votes = len(data.get("votes", []))

        if not stats:
            self._send(chat_id, "📊 No feedback data yet. Vote on signals with 👍/👎!")
            return

        msg = (
            f"📊 <b>SIGNAL FEEDBACK</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Total votes: <b>{total_votes}</b>\n\n"
        )

        for opp_type, counts in sorted(stats.items()):
            up = counts.get("up", 0)
            dn = counts.get("dn", 0)
            total = up + dn
            pct = round(up / total * 100) if total > 0 else 0
            bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
            msg += f"  {opp_type}: {bar} {pct}% 👍 ({up}/{total})\n"

        msg += f"\n<i>Vote on signals to improve quality!</i>"
        self._send(chat_id, msg)

    def _cmd_portfolio(self, chat_id: str):
        """Show portfolio allocation recommendations."""
        try:
            from portfolio_rotator import PortfolioRotator
        except ImportError:
            self._send(chat_id, "📊 Portfolio Rotator not available.")
            return

        # Get recent signals from history
        from cross_platform_scanner import Opportunity
        recent_opps = []
        for opp_type, entries in self.history.items():
            for entry in list(entries)[-5:]:
                # Create a minimal Opportunity for analysis
                try:
                    opp = Opportunity(
                        opp_type=opp_type,
                        title=entry.get("msg", "")[:80] if isinstance(entry, dict) else str(entry)[:80],
                        description="",
                        profit_pct=0,
                        profit_amount=0,
                        total_cost=0,
                        platforms=["polymarket"],
                        legs=[],
                        urls=[],
                        risk_level="medium",
                        hold_time="",
                        category="",
                    )
                    recent_opps.append(opp)
                except Exception:
                    continue

        if not recent_opps:
            self._send(chat_id, "📊 No recent signals to analyze. Wait for the next scan cycle.")
            return

        rotator = PortfolioRotator(self.cfg)
        result = rotator.analyze(recent_opps)
        self._send(chat_id, result["summary"])

    def _cmd_top_whales(self, chat_id: str, period: str = "30d"):
        """
        /topwhales — Show top performing whales. Non-blocking via thread.
        Usage: /topwhales | /topwhales 7d | /topwhales all
        """
        import threading

        try:
            from whale_vault import WhaleVault
        except ImportError:
            self._send(chat_id, "🐋 Whale Vault not available.")
            return

        self._send(chat_id, f"⏳ Fetching top whales ({period})...")
        vault = getattr(self, "whale_vault", None)
        if vault is None:
            vault_path = self.cfg.get("whale_vault", {}).get("vault_path", "whale_vault.json")
            vault = WhaleVault(vault_path)

        def _fetch_and_send():
            try:
                msg = vault.get_leaderboard_display(period)
                self._send(chat_id, msg, parse_mode="HTML",
                           disable_web_page_preview=True)
            except Exception as e:
                self._send(chat_id,
                           f"🐋 <b>Top Whales</b>\nCould not fetch leaderboard: {e}\n"
                           "Vault builds automatically as the bot scans for whale trades.",
                           parse_mode="HTML")

        t = threading.Thread(target=_fetch_and_send, daemon=True)
        t.start()

    # ------------------------------------------------------------------
    # LP Farming Commands (Admin Only)
    # ------------------------------------------------------------------
    def _cmd_lp_status(self, chat_id: str):
        """Show LP farming status — admin only."""
        if not self._is_admin(chat_id):
            return
        engine = self._get_lp_engine()
        if engine is None:
            self._send(chat_id, "🏭 LP Engine not available. Check dependencies.")
            return
        self._send(chat_id, engine.get_status(), parse_mode="HTML")

    def _cmd_lp_start(self, chat_id: str):
        """Start LP farming on best available market — admin only."""
        if not self._is_admin(chat_id):
            return
        engine = self._get_lp_engine()
        if engine is None:
            self._send(chat_id, "🏭 LP Engine not available.")
            return
        if engine.is_running:
            self._send(chat_id, "🏭 LP is already running. Use /lp stop first.")
            return

        # Find best market
        self._send(chat_id, "⏳ Scanning for best LP market...")
        try:
            from elite_edges.reward_farming import pick_best_lp_market
            market = pick_best_lp_market(self.cfg)
        except ImportError:
            self._send(chat_id, "❌ reward_farming module not available")
            return

        if market is None:
            self._send(chat_id,
                       "🏭 No suitable markets right now.\n"
                       "Need: resolves <24h, vol >$10K, balanced price.")
            return

        # Start engine on that market
        engine.start(market)

    def _cmd_lp_stop(self, chat_id: str):
        """Kill switch — cancel all LP orders and stop — admin only."""
        if not self._is_admin(chat_id):
            return
        engine = self._get_lp_engine()
        if engine is None:
            self._send(chat_id, "🏭 LP Engine not available.")
            return
        if not engine.is_running:
            self._send(chat_id, "🏭 LP is not running.")
            return
        engine.stop()

    def _cmd_lp_markets(self, chat_id: str):
        """Show top 5 LP-able markets — admin only."""
        if not self._is_admin(chat_id):
            return
        self._send(chat_id, "⏳ Scanning LP markets...")
        import threading
        def _fetch():
            try:
                from elite_edges.reward_farming import find_lp_markets_display
                msg = find_lp_markets_display(self.cfg)
                self._send(chat_id, msg, parse_mode="HTML")
            except ImportError:
                self._send(chat_id, "❌ reward_farming module not available")
            except Exception as e:
                self._send(chat_id, f"❌ Error: {e}")
        threading.Thread(target=_fetch, daemon=True).start()

    def _get_lp_engine(self):
        """Lazy-init LP Engine. Returns cached instance or None."""
        if hasattr(self, "_lp_engine") and self._lp_engine is not None:
            return self._lp_engine

        try:
            from lp_order_manager import LPOrderManager
            from lp_engine import LPEngine
        except ImportError:
            return None

        lp_cfg = self.cfg.get("lp_farming", {})
        if not lp_cfg.get("enabled", False):
            return None

        # Initialize CLOB client if in live mode and keys available
        clob_client = None
        if lp_cfg.get("lp_mode") == "live":
            import os
            pk = os.environ.get("POLY_PRIVATE_KEY", "")
            funder = os.environ.get("POLY_FUNDER_ADDRESS", "")
            if pk and funder:
                try:
                    from py_clob_client.client import ClobClient
                    clob_client = ClobClient(
                        host="https://clob.polymarket.com",
                        key=pk,
                        chain_id=137,
                        funder=funder,
                        signature_type=1,
                    )
                    logger.info("CLOB client initialized for live LP")
                except Exception as e:
                    logger.error(f"CLOB client init error: {e}")

        om = LPOrderManager(self.cfg, clob_client=clob_client)

        # Create notify function that sends to admin
        admin_id = self.default_chat_id
        def _notify(msg):
            self._send(admin_id, msg, parse_mode="HTML")

        # Run startup recovery (cancel dangling orders from crash, notify admin)
        om.startup_recovery(notify_fn=_notify)

        self._lp_engine = LPEngine(self.cfg, om, notify_fn=_notify)
        logger.info("LP Engine initialized")
        return self._lp_engine

    # ------------------------------------------------------------------
    # Manual Trading Commands (Admin Only)
    # ------------------------------------------------------------------
    def _cmd_manual_buy(self, chat_id: str, text: str):
        """
        /buy YES <slug> <price> <amount>
        /buy NO <slug> <price> <amount>
        Example: /buy YES will-btc-hit-100k 0.55 50
        """
        if not self._is_admin(chat_id):
            return
        parts = text.split()
        if len(parts) < 5:
            self._send(chat_id,
                       "💰 <b>Usage:</b> /buy YES|NO &lt;market-slug&gt; &lt;price&gt; &lt;amount_usd&gt;\n"
                       "Example: <code>/buy YES will-btc-hit-100k 0.55 50</code>",
                       parse_mode="HTML")
            return
        self._execute_manual_trade(chat_id, "BUY", parts[1], parts[2], parts[3], parts[4])

    def _cmd_manual_sell(self, chat_id: str, text: str):
        """
        /sell YES <slug> <price> <amount>
        /sell NO <slug> <price> <amount>
        Example: /sell YES will-btc-hit-100k 0.60 50
        """
        if not self._is_admin(chat_id):
            return
        parts = text.split()
        if len(parts) < 5:
            self._send(chat_id,
                       "💰 <b>Usage:</b> /sell YES|NO &lt;market-slug&gt; &lt;price&gt; &lt;amount_usd&gt;\n"
                       "Example: <code>/sell YES will-btc-hit-100k 0.60 50</code>",
                       parse_mode="HTML")
            return
        self._execute_manual_trade(chat_id, "SELL", parts[1], parts[2], parts[3], parts[4])

    def _execute_manual_trade(self, chat_id: str, action: str,
                              side: str, slug: str, price_str: str, amount_str: str):
        """Execute a manual buy/sell order via CLOB client."""
        side = side.upper()
        if side not in ("YES", "NO"):
            self._send(chat_id, "❌ Side must be YES or NO")
            return

        try:
            price = float(price_str)
            amount = float(amount_str)
        except ValueError:
            self._send(chat_id, "❌ Price and amount must be numbers")
            return

        if price <= 0 or price >= 1:
            self._send(chat_id, "❌ Price must be between 0 and 1")
            return
        if amount <= 0 or amount > 500:
            self._send(chat_id, "❌ Amount must be $1-$500")
            return

        size = amount / price
        lp_cfg = self.cfg.get("lp_farming", {})
        mode = lp_cfg.get("lp_mode", "dry_run")

        # Look up token ID for the market
        token_id = self._resolve_token_id(slug, side)

        if mode == "dry_run":
            self._send(chat_id,
                       f"🟡 <b>DRY RUN — {action} Order</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"Market: {slug}\n"
                       f"Side: {side} | {action}\n"
                       f"Price: ${price:.4f}\n"
                       f"Amount: ${amount:.2f} ({size:.1f} shares)\n"
                       f"Token: {token_id[:20] if token_id else 'unknown'}...\n"
                       f"\n⚠️ Set <code>lp_mode: live</code> to place real orders",
                       parse_mode="HTML")
            return

        # Live mode
        if not token_id:
            self._send(chat_id, f"❌ Could not resolve token ID for {slug} {side}")
            return

        engine = self._get_lp_engine()
        if engine is None or engine.om.client is None:
            self._send(chat_id, "❌ CLOB client not available. Check POLY_PRIVATE_KEY env var.")
            return

        order_side = "BUY" if action == "BUY" else "SELL"
        order_id = engine.om.place_order(
            token_id=token_id,
            side=order_side,
            price=price,
            size=round(size, 2),
        )

        if order_id:
            self._send(chat_id,
                       f"✅ <b>Order Placed!</b>\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"
                       f"ID: <code>{order_id}</code>\n"
                       f"{action} {size:.1f} {side} shares @ ${price:.4f}\n"
                       f"Market: {slug}\n"
                       f"Total: ${amount:.2f}",
                       parse_mode="HTML")
        else:
            self._send(chat_id, f"❌ Order rejected by risk guards. Check /lp status for limits.")

    def _cmd_show_orders(self, chat_id: str):
        """Show all open orders — admin only."""
        if not self._is_admin(chat_id):
            return
        engine = self._get_lp_engine()
        if engine is None:
            self._send(chat_id, "💰 Engine not available")
            return

        open_orders = engine.om.get_open_orders()
        if not open_orders:
            self._send(chat_id, "💰 <b>Open Orders</b>\nNo open orders.", parse_mode="HTML")
            return

        msg = "💰 <b>Open Orders</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        for i, o in enumerate(open_orders, 1):
            msg += (
                f"\n{i}. {o['side']} {o['size']:.1f} @ ${o['price']:.4f}\n"
                f"   ID: <code>{o['order_id'][:20]}</code>\n"
            )
        self._send(chat_id, msg, parse_mode="HTML")

    def _cmd_cancel_order(self, chat_id: str, text: str):
        """/cancel all  OR  /cancel <order_id>"""
        if not self._is_admin(chat_id):
            return
        engine = self._get_lp_engine()
        if engine is None:
            self._send(chat_id, "💰 Engine not available")
            return

        parts = text.split()
        if len(parts) >= 2 and parts[1] == "all":
            count = engine.om.cancel_all_orders()
            self._send(chat_id, f"🛑 Cancelled {count} orders")
        elif len(parts) >= 2:
            order_id = parts[1]
            ok = engine.om.cancel_order(order_id)
            self._send(chat_id,
                       f"✅ Cancelled {order_id[:20]}" if ok
                       else f"❌ Failed to cancel {order_id[:20]}")
        else:
            self._send(chat_id,
                       "💰 <b>Usage:</b>\n"
                       "<code>/cancel all</code> — cancel all orders\n"
                       "<code>/cancel &lt;order_id&gt;</code> — cancel specific order",
                       parse_mode="HTML")

    def _cmd_positions(self, chat_id: str):
        """Show current positions — admin only."""
        if not self._is_admin(chat_id):
            return
        engine = self._get_lp_engine()
        if engine is None:
            self._send(chat_id, "💰 Engine not available")
            return

        pos = engine.om.get_position()
        mode_label = "🔴 LIVE" if engine.om.mode == "live" else "🟡 DRY RUN"
        self._send(chat_id,
                   f"📊 <b>Positions</b> ({mode_label})\n"
                   f"━━━━━━━━━━━━━━━━━━━━\n"
                   f"YES shares: {pos.yes_shares:.1f}\n"
                   f"NO shares: {pos.no_shares:.1f}\n"
                   f"Total cost: ${pos.total_cost:.2f}\n"
                   f"Total fills: {pos.total_fills}",
                   parse_mode="HTML")

    def _cmd_quick_trade(self, chat_id: str, action: str,
                         slug: str, side: str, price_str: str):
        """Quick trade from inline button — uses default order size."""
        if not self._is_admin(chat_id):
            return
        order_size = self.cfg.get("lp_farming", {}).get("order_size", 50.0)
        self._execute_manual_trade(
            chat_id, action, side, slug, price_str, str(order_size)
        )

    def _cmd_lp_start_market(self, chat_id: str, market_slug: str):
        """
        Start LP farming on a specific market (from inline button).
        Called from lp_start:<slug> callback.
        """
        if not self._is_admin(chat_id):
            return
        engine = self._get_lp_engine()
        if engine is None:
            self._send(chat_id, "🏭 LP Engine not available.")
            return
        if engine.is_running:
            self._send(chat_id, "🏭 LP already running. /lp stop first.")
            return

        # Look up market details by slug
        self._send(chat_id, f"⏳ Starting LP on {market_slug}...")
        import threading
        def _start():
            try:
                from elite_edges.reward_farming import _fetch_lp_candidates
                markets = _fetch_lp_candidates(self.cfg)
                target = None
                for m in markets:
                    if m.get("slug", "") == market_slug:
                        target = m
                        break
                if target is None:
                    self._send(chat_id, f"❌ Market {market_slug} not found")
                    return
                engine.start(target)
            except Exception as e:
                self._send(chat_id, f"❌ LP start error: {e}")
        threading.Thread(target=_start, daemon=True).start()

    def _resolve_token_id(self, slug: str, side: str) -> str:
        """
        Resolve CLOB token ID for a market slug + side.
        Fetches from Gamma API if needed.
        """
        try:
            import requests, json
            base = self.cfg.get("scanner", {}).get(
                "gamma_api_url", "https://gamma-api.polymarket.com"
            )
            resp = requests.get(
                f"{base}/markets",
                params={"slug": slug, "limit": 1},
                timeout=5,
            )
            if resp.status_code != 200:
                return ""
            markets = resp.json()
            if not markets:
                return ""
            m = markets[0]
            clob_tokens = m.get("clobTokenIds", "[]")
            if isinstance(clob_tokens, str):
                clob_tokens = json.loads(clob_tokens)
            if side.upper() == "YES":
                return clob_tokens[0] if clob_tokens else ""
            else:
                return clob_tokens[1] if len(clob_tokens) > 1 else ""
        except Exception as e:
            logger.debug(f"Token ID resolution failed: {e}")
            return ""

    # ------------------------------------------------------------------
    # Bond Spread Automator Commands
    # ------------------------------------------------------------------
    def _cmd_bonds(self, chat_id: str, text: str):
        """Bond Spread Automator commands."""
        tier = self._get_tier(chat_id)
        is_admin = self._is_admin(chat_id)
        parts = text.strip().split()
        subcmd = parts[1].lower() if len(parts) > 1 else ""

        bs = getattr(self, '_bond_spreader', None)

        # /bonds — status
        if not subcmd or subcmd == "status":
            if tier == "free" and not is_admin:
                self._send(chat_id, (
                    "🏦 <b>Bond Spread Automator</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "Auto-spread small bets across 50-100\n"
                    "high-probability bonds for consistent returns.\n\n"
                    "🔒 Requires Pro plan to view, Whale to control.\n"
                    "💡 /upgrade to unlock"
                ), parse_mode="HTML")
                return
            if not bs:
                self._send(chat_id, (
                    "🏦 <b>Bond Spreader</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "Module not initialized yet.\n"
                    "Enable in config: <code>bond_spreader.enabled: true</code>"
                ), parse_mode="HTML")
                return

            status = bs.get_status()
            mode_emoji = "🟢 LIVE" if status["mode"] == "live" else "🔵 DRY RUN"

            dep = status["total_deployed"]
            pool = status["current_pool"]
            total = max(1, dep + pool)
            bar_len = int(dep / total * 20)
            pool_bar = "█" * bar_len + "░" * (20 - bar_len)

            tier_lines = ""
            _tier_labels = {"A": "Ultra-Safe", "B": "Standard", "C": "Value"}
            for tk in ["A", "B", "C"]:
                td = status["tiers"].get(tk, {})
                if td.get("resolved", 0) > 0:
                    tier_lines += (
                        f"  {_tier_labels.get(tk, tk)}: "
                        f"{td['win_rate']:.0f}% WR ({td['resolved']} bets) "
                        f"${td['pnl']:+.2f}\n"
                    )
                    if td.get("early_exits", 0):
                        tier_lines += f"    ↗ {td['early_exits']} early exits\n"
                    if td.get("cut_losses", 0):
                        tier_lines += f"    🛡 {td['cut_losses']} losses cut\n"

            cat_lines = ""
            for cat, amount in sorted(
                status.get("categories", {}).items(), key=lambda x: -x[1]
            ):
                if amount > 0:
                    cs = status.get("category_stats", {}).get(cat, {})
                    wr_str = ""
                    if cs.get("bets", 0) > 0:
                        wr = cs["wins"] / cs["bets"] * 100
                        wr_str = f" ({wr:.0f}% WR)"
                    cat_lines += f"  {cat.title()}: ${amount:.2f}{wr_str}\n"

            msg = (
                f"🏦 <b>BOND SPREAD AUTOMATOR</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Mode: {mode_emoji}\n"
                f"Active: <b>{status['active_bets']}</b> bets\n"
                f"Pool: ${status['current_pool']:.2f} "
                f"(started ${status['initial_capital']:.2f})\n"
                f"Deployed: ${status['total_deployed']:.2f}\n"
                f"[{pool_bar}]\n\n"
                f"📊 <b>Performance:</b>\n"
                f"  Resolved: {status['total_resolved']} bets\n"
                f"  Win Rate: <b>{status['win_rate']:.1f}%</b>\n"
                f"  Net P&L: <b>${status['net_pnl']:+.2f}</b> "
                f"({status['roi_pct']:+.1f}% ROI)\n"
                f"  Profits: ${status['total_profits']:.2f} | "
                f"Losses: ${status['total_losses']:.2f}\n"
                f"  Withdrawn: ${status['withdrawn']:.2f}\n\n"
            )
            if tier_lines:
                msg += f"📈 <b>Tier Breakdown:</b>\n{tier_lines}\n"
            if cat_lines:
                msg += f"🏷 <b>Category Allocation:</b>\n{cat_lines}\n"
            msg += (
                "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "/bonds start · /bonds stop\n"
                "/bonds live · /bonds dryrun"
            )
            self._send(chat_id, msg, parse_mode="HTML")
            return

        # Control commands: whale-only (or admin)
        if tier != "whale_tier" and not is_admin:
            self._send(chat_id, (
                "🔒 Bond Spreader control requires Whale plan ($15/mo).\n"
                "Pro users can view status with /bonds\n"
                "💡 /upgrade to unlock"
            ))
            return

        if subcmd == "start":
            if not bs:
                self._send(chat_id, "⚠️ Bond spreader module not loaded.")
                return
            bs.enabled = True
            bs._save_state()
            self._send(chat_id, (
                f"🟢 <b>Bond Spreader STARTED</b>\n"
                f"Mode: {'🟢 LIVE' if bs.mode == 'live' else '🔵 DRY RUN'}\n"
                f"Capital: ${bs.max_deployed:.0f}\n"
                f"Base bet: ${bs.base_amount:.2f}\n\n"
                f"Bot will auto-deploy on next scan cycle (60s)."
            ), parse_mode="HTML")

        elif subcmd == "stop":
            if not bs:
                self._send(chat_id, "⚠️ Bond spreader module not initialized yet.")
                return
            count = bs.emergency_stop()
            self._send(chat_id, (
                f"🔴 <b>Bond Spreader STOPPED</b>\n"
                f"Cancelled {count} active bets.\n"
                f"All capital returned to pool."
            ), parse_mode="HTML")

        elif subcmd == "live":
            if not bs:
                self._send(chat_id, "⚠️ Bond spreader module not initialized yet.")
                return
            bs.mode = "live"
            bs._save_state()
            self._send(chat_id, (
                    "🟢 <b>LIVE MODE</b>\n"
                    "⚠️ Real USDC will be used!\n"
                    "Make sure wallet is connected: /wallet status"
                ), parse_mode="HTML")

        elif subcmd == "dryrun":
            if not bs:
                self._send(chat_id, "⚠️ Bond spreader module not initialized yet.")
                return
            bs.mode = "dry_run"
            bs._save_state()
            self._send(chat_id, "🔵 Switched to DRY RUN mode.")

        elif subcmd == "set" and len(parts) >= 4:
            if not bs:
                self._send(chat_id, "⚠️ Bond spreader module not initialized yet.")
                return
            param = parts[2].lower()
            try:
                value = float(parts[3])
            except ValueError:
                self._send(chat_id, "Value must be a number.")
                return
            if param == "amount":
                bs.base_amount = max(0.10, min(100, value))
                bs._save_state()
                self._send(chat_id, f"✅ Base bet amount: ${bs.base_amount:.2f}")
            elif param == "max":
                bs.max_deployed = max(10, min(100000, value))
                bs.session.initial_capital = bs.max_deployed
                bs.session.current_pool = bs.max_deployed
                bs._save_state()
                self._send(chat_id, f"✅ Max deployed capital: ${bs.max_deployed:.0f}")
            elif param == "reinvest":
                bs.session.reinvest_rate = max(0, min(1.0, value / 100.0))
                bs._save_state()
                self._send(chat_id,
                           f"✅ Reinvest rate: {bs.session.reinvest_rate * 100:.0f}%")
            else:
                self._send(chat_id, (
                    "📝 Usage:\n"
                    "  /bonds set amount 2.00\n"
                    "  /bonds set max 500\n"
                    "  /bonds set reinvest 80"
                ))

        elif subcmd == "history":
            if not bs:
                self._send(chat_id, "⚠️ Bond spreader module not initialized yet.")
                return
            recent = bs.session.resolved_bets[-15:]
            if not recent:
                self._send(chat_id, "No resolved bets yet.")
                return
            msg = "📜 <b>RECENT BONDS</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for b in reversed(recent):
                s = b.get("status", "?")
                emoji = {"won": "✅", "lost": "❌", "sold_profit": "📈",
                         "sold_loss": "🛡", "cancelled": "⚪"}.get(s, "⚪")
                msg += (
                    f"{emoji} {b.get('market_title', '?')[:40]}\n"
                    f"   {b.get('side', '')} @ {b.get('price', 0):.2f} | "
                    f"${b.get('pnl', 0):+.2f}\n\n"
                )
            self._send(chat_id, msg, parse_mode="HTML")

        else:
            self._send(chat_id, (
                "🏦 <b>Bond Spreader Commands:</b>\n"
                "  /bonds — Status dashboard\n"
                "  /bonds start — Start auto-betting\n"
                "  /bonds stop — Emergency stop\n"
                "  /bonds live — Enable real trading\n"
                "  /bonds dryrun — Simulation mode\n"
                "  /bonds set amount 2.00 — Base bet size\n"
                "  /bonds set max 500 — Max capital\n"
                "  /bonds set reinvest 80 — Reinvest %\n"
                "  /bonds history — Recent results"
            ), parse_mode="HTML")

    # ------------------------------------------------------------------
    # Wallet Management Commands
    # ------------------------------------------------------------------
    def _cmd_wallet(self, chat_id: str, text: str):
        """Wallet management for trading execution."""
        tier = self._get_tier(chat_id)
        if tier == "free" and not self._is_admin(chat_id):
            self._send(chat_id, (
                "💳 <b>Wallet</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "Connect your Polymarket wallet to trade directly\n"
                "from signals — no website needed.\n\n"
                "🔒 Requires Pro plan.\n💡 /upgrade to unlock"
            ), parse_mode="HTML")
            return

        parts = text.strip().split()
        subcmd = parts[1].lower() if len(parts) > 1 else ""
        ee = getattr(self, 'execution_engine', None)

        if not subcmd or subcmd == "status":
            if ee:
                self._send(chat_id, ee.format_wallet_status(chat_id),
                           parse_mode="HTML")
            else:
                self._send(chat_id, "💳 Execution engine not available.")
            return

        if subcmd == "set" and len(parts) >= 4:
            if not ee:
                self._send(chat_id, "💳 Execution engine not available.")
                return
            private_key = parts[2]
            funder_address = parts[3]
            # Delete the message containing the private key immediately
            try:
                msg_id = getattr(self, '_last_msg_id', None)
                if msg_id:
                    requests.post(
                        f"https://api.telegram.org/bot{self.token}/deleteMessage",
                        json={"chat_id": chat_id, "message_id": msg_id},
                    )
            except Exception:
                pass

            ok = ee.wallet_manager.store_wallet(
                chat_id, private_key, funder_address
            )
            if ok:
                self._send(chat_id, (
                    "✅ <b>Wallet connected!</b>\n"
                    "🔐 Key encrypted and stored securely.\n"
                    "Mode: 🟡 DRY RUN (use /wallet live to enable)\n\n"
                    "⚠️ Your message with the key was deleted."
                ), parse_mode="HTML")
            else:
                self._send(chat_id, "❌ Failed to store wallet. Try again.")
            return

        if subcmd == "live":
            if ee and ee.wallet_manager.has_wallet(chat_id):
                ee.wallet_manager.set_mode(chat_id, "live")
                self._send(chat_id, (
                    "🟢 <b>LIVE MODE</b>\n"
                    "⚠️ Real USDC orders will be placed!\n"
                    "Check limits: /wallet status"
                ), parse_mode="HTML")
            else:
                self._send(chat_id, "❌ No wallet connected. Use /wallet set KEY ADDRESS")
            return

        if subcmd == "dryrun":
            if ee and ee.wallet_manager.has_wallet(chat_id):
                ee.wallet_manager.set_mode(chat_id, "dry_run")
                self._send(chat_id, "🔵 Switched to DRY RUN mode.")
            return

        if subcmd == "limit" and len(parts) >= 3:
            if ee and ee.wallet_manager.has_wallet(chat_id):
                try:
                    val = float(parts[2])
                    ee.wallet_manager.set_limits(chat_id, max_per_trade=val)
                    self._send(chat_id, f"✅ Max per trade: ${val:.0f}")
                except ValueError:
                    self._send(chat_id, "❌ Value must be a number")
            return

        if subcmd == "daily" and len(parts) >= 3:
            if ee and ee.wallet_manager.has_wallet(chat_id):
                try:
                    val = float(parts[2])
                    ee.wallet_manager.set_limits(chat_id, daily_limit=val)
                    self._send(chat_id, f"✅ Daily limit: ${val:.0f}")
                except ValueError:
                    self._send(chat_id, "❌ Value must be a number")
            return

        if subcmd == "remove":
            if ee and ee.wallet_manager.remove_wallet(chat_id):
                self._send(chat_id, "✅ Wallet removed. All keys deleted.")
            else:
                self._send(chat_id, "❌ No wallet to remove.")
            return

        self._send(chat_id, (
            "💳 <b>Wallet Commands:</b>\n"
            "  /wallet — Status\n"
            "  /wallet set KEY ADDRESS — Connect\n"
            "  /wallet live — Enable trading\n"
            "  /wallet dryrun — Simulation\n"
            "  /wallet limit 50 — Max per trade\n"
            "  /wallet daily 200 — Daily cap\n"
            "  /wallet remove — Disconnect"
        ), parse_mode="HTML")
