
"""
telegram_bot.py — Interactive Telegram Bot with Signal Type + Category Selection
Users choose which signal type AND category they want via inline keyboards.
Only matching signals are delivered. Users can switch anytime.
Commands:
  /start    — Welcome screen + signal picker
  /menu     — Change signal type
  /category — Change category filter
  /help     — Show help
  /status   — Current selection + stats
Signal Types:
  📡 All Signals     — Everything at once
  🔄 Arb Trading     — Cross-platform arbitrage
  🏦 High-Prob Bonds — 93¢+ near-certain outcomes
  🎯 Intra-Market    — YES+NO mispricing
  🐋 Whale Tracker   — Whale convergence alerts
  🆕 New Markets     — Brand new market launches
Categories:
  🌐 All Categories  — No filter (default)
  🏛 Politics        — Political markets
  ⚽ Sports          — Sports betting
  🪙 Crypto          — Cryptocurrency
  💰 Finance         — Financial markets
  🌍 Geopolitics     — Global events
  💻 Tech            — Technology
  🎭 Culture         — Entertainment & pop culture
  📈 Earnings        — Company earnings
  🌡 Climate & Science
  🗳 Elections
  📰 Breaking / Trending
"""
import os
import json
import time
import logging
import threading
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
        "keywords": ["politics", "political", "government", "congress", "senate",
                      "president", "democrat", "republican", "biden", "trump"],
    },
    "sports": {
        "emoji": "⚽",
        "label": "Sports",
        "keywords": ["sports", "nfl", "nba", "mlb", "soccer", "football",
                      "basketball", "baseball", "tennis", "cricket", "f1",
                      "olympics", "ufc", "boxing", "hockey", "golf"],
    },
    "crypto": {
        "emoji": "🪙",
        "label": "Crypto",
        "keywords": ["crypto", "bitcoin", "ethereum", "btc", "eth", "solana",
                      "defi", "blockchain", "web3", "token", "nft"],
    },
    "finance": {
        "emoji": "💰",
        "label": "Finance",
        "keywords": ["finance", "stock", "market", "fed", "interest rate",
                      "inflation", "gdp", "recession", "economy", "bank",
                      "treasury", "s&p", "nasdaq", "dow"],
    },
    "geopolitics": {
        "emoji": "🌍",
        "label": "Geopolitics",
        "keywords": ["geopolitics", "war", "conflict", "nato", "china",
                      "russia", "ukraine", "sanctions", "trade war",
                      "diplomacy", "un", "military"],
    },
    "tech": {
        "emoji": "💻",
        "label": "Tech",
        "keywords": ["tech", "technology", "ai", "artificial intelligence",
                      "apple", "google", "meta", "microsoft", "openai",
                      "spacex", "tesla", "semiconductor"],
    },
    "culture": {
        "emoji": "🎭",
        "label": "Culture",
        "keywords": ["culture", "entertainment", "movie", "music", "celebrity",
                      "oscar", "grammy", "award", "tv", "streaming",
                      "tiktok", "viral", "social media"],
    },
    "earnings": {
        "emoji": "📈",
        "label": "Earnings",
        "keywords": ["earnings", "revenue", "quarterly", "q1", "q2", "q3", "q4",
                      "profit", "eps", "guidance", "ipo"],
    },
    "climate": {
        "emoji": "🌡",
        "label": "Climate & Science",
        "keywords": ["climate", "weather", "temperature", "hurricane", "science",
                      "space", "nasa", "environment", "carbon", "renewable"],
    },
    "elections": {
        "emoji": "🗳",
        "label": "Elections",
        "keywords": ["election", "vote", "ballot", "primary", "midterm",
                      "governor", "mayor", "poll", "swing state", "electoral"],
    },
}
# =========================================================================
# Interactive Bot Handler
# =========================================================================
class TelegramBotHandler:
    """
    Interactive Telegram bot that lets users choose signal types + categories.
    Runs a polling thread alongside the main scan loop.
    """
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.token = str(cfg["telegram"].get("bot_token", ""))
        # CRITICAL: Force chat_id to string so it matches Telegram callback IDs
        self.default_chat_id = str(cfg["telegram"].get("chat_id", ""))
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.enabled = bool(
            cfg["telegram"].get("enabled") and self.token and self.default_chat_id
        )
        # Preferences file — stores user signal + category prefs
        self.prefs_file = cfg.get("interactive", {}).get(
            "prefs_file", "user_prefs.json"
        )
        self.user_prefs: dict[str, dict] = self._load_prefs()
        # Signal history — persisted to disk so it survives restarts
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
        # Polling internals
        self._last_update_id = 0
        self._lock = threading.Lock()
        self._running = False
        logger.info(
            f"Bot handler init: default_chat_id={self.default_chat_id!r}, "
            f"prefs={self.user_prefs}, enabled={self.enabled}"
        )
    # -----------------------------------------------------------------
    # Persistence
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
                    # Backward compat: old format was {"chat_id": "signal_key"}
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
    def _load_history(self) -> dict[str, deque]:
        """Load signal history from disk."""
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
        """Save signal history to disk."""
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
    # Preference helpers
    # -----------------------------------------------------------------
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
    # Category matching
    # -----------------------------------------------------------------
    @staticmethod
    def _matches_category(opp: Opportunity, cat_key: str) -> bool:
        """
        Check if an opportunity matches the selected category.
        Matches by: (1) exact category field, or (2) keyword in title.
        Whale signals (no category) always match.
        """
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
        """Send an HTML message with optional inline keyboard."""
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
    # -----------------------------------------------------------------
    # Keyboards
    # -----------------------------------------------------------------
    def _signal_keyboard(self, current: str | None = None) -> dict:
        """Build the 3x2 inline keyboard for signal type selection."""
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
        """Build a 2-column inline keyboard for category selection."""
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
        """Small button row appended after signals."""
        return {
            "inline_keyboard": [
                [
                    {"text": "📋 Menu", "callback_data": "cmd:menu"},
                    {"text": "🏷 Category", "callback_data": "cmd:category"},
                    {"text": "ℹ️ Help", "callback_data": "cmd:help"},
                ]
            ]
        }
    # -----------------------------------------------------------------
    # Polling loop (runs in background thread)
    # -----------------------------------------------------------------
    def start_polling(self):
        """Start a daemon thread that long-polls for Telegram updates."""
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
                            ["message", "callback_query"]
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
                logger.error(f"Polling error: {e}")
                time.sleep(5)
    # -----------------------------------------------------------------
    # Update routing
    # -----------------------------------------------------------------
    def _handle_update(self, upd: dict):
        if "callback_query" in upd:
            self._on_callback(upd["callback_query"])
        elif "message" in upd:
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
    def _on_callback(self, cb: dict):
        cb_id = cb["id"]
        data = cb.get("data", "")
        chat_id = str(cb["message"]["chat"]["id"])
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
        elif data == "cmd:menu":
            self._cmd_menu(chat_id)
            self._answer_callback(cb_id)
        elif data == "cmd:category":
            self._cmd_category(chat_id)
            self._answer_callback(cb_id)
        elif data == "cmd:help":
            self._cmd_help(chat_id)
            self._answer_callback(cb_id)
        else:
            self._answer_callback(cb_id)
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
        msg = (
            f"🤖 <b>Polymarket Arb Bot v2.0</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"I scan Polymarket & Kalshi 24/7 for\n"
            f"profitable opportunities.\n"
            f"{current_label}\n"
            f"<b>Step 1: Choose your signal type:</b>\n"
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
            f"Category: <b>{c['emoji']} {c['label']}</b>\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Tap to change signal type:</b>"
        )
        self._send(chat_id, msg, self._signal_keyboard(current_sig))
    def _cmd_category(self, chat_id: str):
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
            f"Pick a category to only receive signals\n"
            f"from your area of expertise:\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Tap to change category:</b>"
        )
        self._send(chat_id, msg, self._category_keyboard(current_cat))
    def _cmd_help(self, chat_id: str):
        msg = (
            f"ℹ️ <b>BOT HELP</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"<b>Commands:</b>\n"
            f"/start    — Welcome + pick signal type\n"
            f"/menu     — Switch signal type\n"
            f"/category — Filter by category\n"
            f"/status   — View current stats\n"
            f"/help     — This message\n"
            f"\n"
            f"<b>How it works:</b>\n"
            f"1️⃣ Pick a signal type (Bonds, Arb, etc.)\n"
            f"2️⃣ Optionally pick a category (Sports, Crypto...)\n"
            f"3️⃣ Receive only matching signals\n"
            f"4️⃣ Use /menu or /category to switch anytime\n"
            f"\n"
            f"<b>Signal types:</b>\n"
            f"📡 All — Every signal\n"
            f"🔄 Arb — Buy on platform A, sell on B\n"
            f"🏦 Bonds — 93¢+ → $1.00 safe returns\n"
            f"🎯 Intra — YES+NO price errors\n"
            f"🐋 Whales — Big traders converging\n"
            f"🆕 New — Brand new markets\n"
            f"\n"
            f"<b>Categories:</b>\n"
            f"🌐 All — No filter (default)\n"
            f"⚽ Sports • 🪙 Crypto • 🏛 Politics\n"
            f"💰 Finance • 🌍 Geopolitics • 💻 Tech\n"
            f"🎭 Culture • 📈 Earnings • 🗳 Elections\n"
            f"\n"
            f"Tap /menu or /category to change settings."
        )
        self._send(chat_id, msg)
    def _cmd_status(self, chat_id: str):
        current_sig = self._get_signal(chat_id)
        current_cat = self._get_category(chat_id)
        s = SIGNAL_TYPES.get(current_sig, SIGNAL_TYPES["all"])
        c = CATEGORIES.get(current_cat, CATEGORIES["all_cat"])
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
            f"Uptime: {hours}h {mins}m\n"
            f"Total signals sent: <b>{self.signals_sent}</b>\n"
            f"\n"
        )
        if hist_text:
            msg += (
                f"📦 <b>Cached signals:</b>\n"
                f"{hist_text}\n"
            )
        msg += (
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Use /menu or /category to change."
        )
        self._send(chat_id, msg)
    def _cmd_reset(self, chat_id: str):
        with self._lock:
            self.user_prefs.pop(chat_id, None)
            self._save_prefs()
        self._cmd_start(chat_id)
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
        # Confirmation + prompt for category
        confirm = (
            f"✅ <b>Signal type updated!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Signal: <b>{s['emoji']} {s['label']}</b>\n"
            f"<i>{s['desc']}</i>\n"
            f"\n"
            f"Category: <b>{c['emoji']} {c['label']}</b>\n"
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
        # Count available past signals
        past = self._get_history_for(current_sig, cat_key)
        confirm = (
            f"✅ <b>Category updated!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Signal: <b>{s['emoji']} {s['label']}</b>\n"
            f"Category: <b>{c['emoji']} {c['label']}</b>\n"
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
        # Filter by category (if history entries have category info)
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
    # Signal Distribution (called from scan cycle)
    # -----------------------------------------------------------------
    def distribute_signals(self, opportunities: list[Opportunity], cfg: dict):
        """
        Send opportunities to all registered users, filtered by
        signal type AND category preference.
        THIS IS THE ONLY PATH FOR SENDING SIGNALS.
        """
        if not self.enabled or not opportunities:
            return
        min_pct = cfg["telegram"].get("min_alert_profit_pct", 0.5)
        filtered = [o for o in opportunities if o.profit_pct >= min_pct]
        if not filtered:
            return
        # Store in history (with category info for replay filtering)
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
            f"Distributing {len(filtered)} signals to {len(users)} user(s): "
            f"{', '.join(f'{cid}={p}' for cid, p in users.items())}"
        )
        # For each user, filter by signal type AND category
        for chat_id, pref in users.items():
            # Handle both old and new format
            if isinstance(pref, str):
                sig_key = pref
                cat_key = "all_cat"
            else:
                sig_key = pref.get("signal", "all")
                cat_key = pref.get("category", "all_cat")
            s = SIGNAL_TYPES.get(sig_key, SIGNAL_TYPES["all"])
            c = CATEGORIES.get(cat_key, CATEGORIES["all_cat"])
            want_types = s["opp_types"]  # None means all
            # Filter by signal type + category + per-user dedup
            now = time.time()
            user_seen = self._user_seen.setdefault(chat_id, {})
            user_opps = []
            for opp in filtered:
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
            logger.info(
                f"  User {chat_id} ({sig_key}+{cat_key}): "
                f"sending {len(user_opps)} signals"
            )
            # Send summary header
            type_counts: dict[str, int] = {}
            for o in user_opps:
                type_counts[o.opp_type] = type_counts.get(o.opp_type, 0) + 1
            cat_label = (
                f" — {c['emoji']} {c['label']}"
                if cat_key != "all_cat" else ""
            )
            summary = (
                f"📡 <b>{s['emoji']} {s['label'].upper()}"
                f"{cat_label} — SCAN RESULTS</b>\n"
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
            self._send(chat_id, summary)
            # Send individual signals (max 10 per user per cycle)
            for opp in user_opps[:10]:
                msg = format_opportunity(opp)
                self._send(chat_id, msg)
                self.signals_sent += 1
                time.sleep(0.5)
            # Append a menu button every 5th batch
            if self.signals_sent % 5 == 0:
                self._send(
                    chat_id,
                    f"💡 <i>Use /menu or /category to change filters</i>",
                    self._menu_button(),
                )
