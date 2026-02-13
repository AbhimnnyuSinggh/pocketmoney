
telegram_bot.py


"""
telegram_bot.py — Interactive Telegram Bot with Signal Type + Category Selection + Monetization
Users choose signal type AND category via inline keyboards.
Only matching signals are delivered. Users can switch anytime.
TIERS:
  🆓 Free     — 5 signals/day, 30-min delay
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
# Subscription Tier Definitions
# =========================================================================
TIERS = {
    "free": {
        "emoji": "🆓",
        "label": "Free",
        "daily_limit": 5,
        "delay_seconds": 1800,
        "price_stars": 0,
        "price_usd": 0,
    },
    "pro": {
        "emoji": "⭐",
        "label": "Pro",
        "daily_limit": 999999,
        "delay_seconds": 0,
        "price_stars": 300,       # monthly
        "price_usd": 5.99,        # monthly
        "price_stars_yr": 3000,   # yearly (~17% savings)
        "price_usd_yr": 59.99,    # yearly
    },
    "whale_tier": {
        "emoji": "💎",
        "label": "Whale",
        "daily_limit": 999999,
        "delay_seconds": 0,
        "price_stars": 750,       # monthly
        "price_usd": 14.99,       # monthly
        "price_stars_yr": 7500,   # yearly (~17% savings)
        "price_usd_yr": 149.99,   # yearly
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
        # Delayed messages queue for free tier: [(chat_id, message, release_time)]
        self.delayed_queue: list[tuple[str, str, float]] = []
        
        # Start background thread for delayed messages
        self._delay_thread = threading.Thread(target=self._process_delayed_messages, daemon=True)
        self._delay_thread.start()
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
        if not os.path.exists(self.prefs_file):
            return {}
        try:
            with open(self.prefs_file, "r") as f:
                data = json.load(f)
            raw = data.get("users", data) if isinstance(data, dict) else {}
            result = {}
            for k, v in raw.items():
                k = str(k)
                if isinstance(v, str):
                    # Backward compat: old format was just a string
                    result[k] = {"signal": v, "category": "all_cat"}
                elif isinstance(v, dict):
                    result[k] = v
                else:
                    result[k] = {"signal": "all", "category": "all_cat"}
            return result
        except (json.JSONDecodeError, IOError):
            return {}
    def _save_prefs(self):
        try:
            with open(self.prefs_file, "w") as f:
                json.dump(
                    {"users": self.user_prefs, "updated": time.time()},
                    f, indent=2,
                )
        except IOError as e:
            logger.error(f"Failed to save prefs: {e}")
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
    # -----------------------------------------------------------------
    # Persistence — Subscriptions
    # -----------------------------------------------------------------
    def _load_subs(self) -> dict:
        if not os.path.exists(self.subs_file):
            return {}
        try:
            with open(self.subs_file, "r") as f:
                data = json.load(f)
            raw = data.get("subs", data) if isinstance(data, dict) else {}
            return {str(k): v for k, v in raw.items()}
        except (json.JSONDecodeError, IOError):
            return {}
    def _save_subs(self):
        try:
            with open(self.subs_file, "w") as f:
                json.dump(
                    {"subs": self.user_subs, "updated": time.time()},
                    f, indent=2,
                )
        except IOError as e:
            logger.error(f"Failed to save subs: {e}")
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
            return r.status_code == 200
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
            "title": f"PocketMoney {tier['label']} — {desc_suffix}",
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
            elif text.startswith("/approve "):
                self._cmd_approve(chat_id, text)
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
        elif data == "cmd:help":
            self._cmd_help(chat_id)
            self._answer_callback(cb_id)
        elif data == "cmd:upgrade":
            self._cmd_upgrade(chat_id)
            self._answer_callback(cb_id)
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
            f"Thank you for supporting PocketMoney! 🚀"
        )
        self._send(chat_id, confirm_msg)
        logger.info(f"✅ Subscription activated: {chat_id} → {tier_key} until {expiry_str}")
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
            f"🤖 <b>PocketMoney — Prediction Market Signals</b>\n"
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
            f"\n"
            f"📡 <b>All Signals</b> — Everything at once\n"
            f"🔄 <b>Arb Trading</b> — Cross-platform gaps\n"
            f"🏦 <b>Bonds</b> — 93¢+ safe returns\n"
            f"🎯 <b>Intra-Market</b> — YES+NO mispricings\n"
            f"🐋 <b>Whales</b> — Follow big money\n"
            f"🆕 <b>New Markets</b> — First-mover edge\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Tap below to select:</b>"
        )
        self._send(chat_id, msg, self._signal_keyboard(current_sig))
        # Auto-register with defaults
        if chat_id not in self.user_prefs:
            self.user_prefs[chat_id] = {"signal": "all", "category": "all_cat"}
            self._save_prefs()
    def _cmd_menu(self, chat_id: str):
        current_sig = self._get_signal(chat_id)
        current_cat = self._get_category(chat_id)
        s = SIGNAL_TYPES.get(current_sig, SIGNAL_TYPES["all"])
        c = CATEGORIES.get(current_cat, CATEGORIES["all_cat"])
        msg = (
            f"📋 <b>SIGNAL MENU</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Signal: <b>{s['emoji']} {s['label']}</b>\n"
            f"<i>{s['desc']}</i>\n"
            f"Category: <b>{c['emoji']} {c['label']}</b>\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Tap to switch signal type:</b>"
        )
        self._send(chat_id, msg, self._signal_keyboard(current_sig))
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
            f"/status   — View current stats\n"
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
            f"⭐ Pro — Unlimited, real-time (500 Stars)\n"
            f"💎 Whale — Everything + priority (1500 Stars)\n"
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
            if expiry > 0:
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
            f"⭐ <b>PRO — $5.99/month</b>\n"
            f"  ✓ Unlimited signals\n"
            f"  ✓ Real-time alerts (zero delay)\n"
            f"  ✓ All 6 signal types\n"
            f"\n"
            f"💎 <b>WHALE — $14.99/month</b>\n"
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
            f"Thank you for supporting PocketMoney! 🚀"
        )
        self._send(user_id, user_msg)
        logger.info(f"Admin approved payment: {user_id} → {tier_key} ({period}) until {exp_str}")
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
            confirm += f"📭 No past signals matching this combo yet!\n"
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
            f"  ✓ Just 500 Stars/month\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Tap to upgrade now:</b>"
        )
        self._send(chat_id, msg, self._upgrade_keyboard(chat_id))
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
            f"🚀 <b>Upgrade for just 500 Stars/month</b>\n"
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
        # Store in history (with category + title for replay filtering)
        now = time.time()
        for opp in filtered:
            msg = format_opportunity(opp)
            entry = {
                "ts": now,
                "msg": msg,
                "category": opp.category,
                "title": opp.title,
            }
            with self._lock:
                self.history[opp.opp_type].append(entry)
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
            s = SIGNAL_TYPES.get(sig_key, SIGNAL_TYPES["all"])
            c = CATEGORIES.get(cat_key, CATEGORIES["all_cat"])
            want_types = s["opp_types"]  # None means all
            # --- TIER GATING ---
            can_send, remaining = self._check_signal_limit(chat_id)
            if not can_send:
                # Count what they WOULD have received
                missed_opps = []
                for opp in filtered:
                    if want_types is not None and opp.opp_type not in want_types:
                        continue
                    if not self._matches_category(opp, cat_key):
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
            tier = self._get_tier(chat_id)
            tier_info = TIERS.get(tier, TIERS["free"])
            # Filter by signal type + category + per-user dedup
            user_seen = self._user_seen.setdefault(chat_id, {})
            user_opps = []
            for opp in filtered:
                # STRICT TIER ENFORCEMENT: Whale/Sniper features
                if opp.opp_type in ["whale_convergence", "new_market"] and tier != "whale_tier":
                    continue
                if want_types is not None and opp.opp_type not in want_types:
                    continue
                # Filter by category
                if not self._matches_category(opp, cat_key):
                    continue
                # Per-user dedup: include rounded price so price changes
                # generate new alerts
                dedup_key = (
                    f"{opp.opp_type}:{opp.title[:50]}:"
                    f"{round(opp.profit_pct, 1)}"
                )
                if now - user_seen.get(dedup_key, 0) < self.dedup_cooldown:
                    continue
                user_seen[dedup_key] = now
                user_opps.append(opp)
            
            if not user_opps:
                logger.info(
                    f"  User {chat_id} ({sig_key}+{cat_key}): "
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
            type_labels = {
                "cross_platform_arb": "🔄 Arb",
                "high_prob_bond": "🏦 Bonds",
                "intra_market_arb": "🎯 Intra-market",
                "whale_convergence": "🐋 Whales",
                "new_market": "🆕 New markets",
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
            for opp in user_opps[:10]:
                msg = format_opportunity(opp)
                
                if tier == "free":
                    # QUEUE for 30 minutes
                    delay = 1800  # 30 mins
                    release_time = time.time() + delay
                    msg += f"\n\n🕐 <i>Delayed 30 min (arriving at {datetime.fromtimestamp(release_time).strftime('%H:%M:%S')})</i>" \
                           f"\n💡 <i>/upgrade for real-time</i>"
                    
                    with self._lock:
                        self.delayed_queue.append((chat_id, msg, release_time))
                    # Do NOT increment count here; delayed thread does it
                else:
                    # Send IMMEDIATELY
                    self._send(chat_id, msg)
                    self.signals_sent += 1
                    sent_count += 1
                    time.sleep(0.5)
            # Track signal count ONLY for paid users (free users tracked when sent)
            if tier != "free":
                self._increment_signal_count(chat_id, sent_count)
            # Periodic menu reminder
            if self.signals_sent > 0 and self.signals_sent % 5 == 0:
                self._send(
                    chat_id,
                    f"💡 <i>/menu to switch signals · /category to filter · /upgrade for Pro</i>",
                    self._menu_button(),
                )
