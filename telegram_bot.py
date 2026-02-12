"""
telegram_bot.py — Interactive Telegram Bot with Signal Type Selection
Users choose which signal type they want via beautiful inline keyboards.
Only matching signals are delivered. Users can switch anytime.
Commands:
  /start  — Welcome screen + signal picker
  /menu   — Change signal type
  /help   — Show help
  /status — Current selection + stats
Signal Types:
  📡 All Signals     — Everything at once
  🔄 Arb Trading     — Cross-platform arbitrage
  🏦 High-Prob Bonds — 93¢+ near-certain outcomes
  🎯 Intra-Market    — YES+NO mispricing
  🐋 Whale Tracker   — Whale convergence alerts
  🆕 New Markets     — Brand new market launches
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
# Interactive Bot Handler
# =========================================================================
class TelegramBotHandler:
    """
    Interactive Telegram bot that lets users choose signal types.
    Runs a polling thread alongside the main scan loop.
    """
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.token = cfg["telegram"].get("bot_token", "")
        self.default_chat_id = str(cfg["telegram"].get("chat_id", ""))
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.enabled = bool(
            cfg["telegram"].get("enabled") and self.token and self.default_chat_id
        )
        # Preferences file — stores {chat_id: signal_type_key}
        self.prefs_file = cfg.get("interactive", {}).get(
            "prefs_file", "user_prefs.json"
        )
        self.user_prefs: dict[str, str] = self._load_prefs()
        # Signal history — persisted to disk so it survives restarts
        self.history_file = cfg.get("interactive", {}).get(
            "history_file", "signal_history.json"
        )
        self.history: dict[str, deque] = self._load_history()
        # Stats
        self.signals_sent = 0
        self.start_time = time.time()
        # Polling internals
        self._last_update_id = 0
        self._lock = threading.Lock()
        self._running = False
    # -----------------------------------------------------------------
    # Persistence
    # -----------------------------------------------------------------
    def _load_prefs(self) -> dict:
        if not os.path.exists(self.prefs_file):
            return {}
        try:
            with open(self.prefs_file, "r") as f:
                data = json.load(f)
                return data.get("users", data) if isinstance(data, dict) else {}
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
        """Load signal history from disk. Each entry: {"ts": epoch, "msg": text}."""
        history: dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        if not os.path.exists(self.history_file):
            return history
        try:
            with open(self.history_file, "r") as f:
                data = json.load(f)
            for opp_type, entries in data.get("signals", {}).items():
                valid = []
                for entry in entries:
                    # Support both old format (plain string) and new (dict with ts)
                    if isinstance(entry, dict) and "ts" in entry and "msg" in entry:
                        valid.append(entry)
                    elif isinstance(entry, str):
                        # Old format — assign current time so it expires in 24h
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
        """Build the 3×2 inline keyboard for signal type selection."""
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
    @staticmethod
    def _menu_button() -> dict:
        """Small 'Switch' button appended after signals."""
        return {
            "inline_keyboard": [
                [
                    {"text": "📋 Menu", "callback_data": "cmd:menu"},
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
            self._select_signal(chat_id, sig_key)
            self._answer_callback(cb_id, f"✅ {SIGNAL_TYPES[sig_key]['label']}")
        elif data == "cmd:menu":
            self._cmd_menu(chat_id)
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
        current = self.user_prefs.get(chat_id)
        current_label = ""
        if current and current in SIGNAL_TYPES:
            s = SIGNAL_TYPES[current]
            current_label = (
                f"\n📌 Current: <b>{s['emoji']} {s['label']}</b>\n"
            )
        msg = (
            f"🤖 <b>Polymarket Arb Bot v2.0</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"I scan Polymarket & Kalshi 24/7 for\n"
            f"profitable opportunities.\n"
            f"{current_label}\n"
            f"<b>Choose your signal type:</b>\n"
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
        self._send(chat_id, msg, self._signal_keyboard(current))
        # Auto-register with default chat_id
        if chat_id not in self.user_prefs:
            self.user_prefs[chat_id] = "all"
            self._save_prefs()
    def _cmd_menu(self, chat_id: str):
        current = self.user_prefs.get(chat_id, "all")
        s = SIGNAL_TYPES.get(current, SIGNAL_TYPES["all"])
        msg = (
            f"📋 <b>SIGNAL MENU</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Current: <b>{s['emoji']} {s['label']}</b>\n"
            f"<i>{s['desc']}</i>\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>Tap to switch:</b>"
        )
        self._send(chat_id, msg, self._signal_keyboard(current))
    def _cmd_help(self, chat_id: str):
        msg = (
            f"ℹ️ <b>BOT HELP</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"<b>Commands:</b>\n"
            f"/start  — Welcome + pick signal type\n"
            f"/menu   — Switch signal type\n"
            f"/status — View current stats\n"
            f"/help   — This message\n"
            f"\n"
            f"<b>How it works:</b>\n"
            f"1️⃣ Pick a signal type\n"
            f"2️⃣ Receive only those signals\n"
            f"3️⃣ Use /menu to switch anytime\n"
            f"\n"
            f"<b>Signal types:</b>\n"
            f"📡 All — Every signal\n"
            f"🔄 Arb — Buy on platform A, sell on B\n"
            f"🏦 Bonds — 93¢+ → $1.00 safe returns\n"
            f"🎯 Intra — YES+NO price errors\n"
            f"🐋 Whales — Big traders converging\n"
            f"🆕 New — Brand new markets\n"
            f"\n"
            f"Tap /menu to change your signal."
        )
        self._send(chat_id, msg)
    def _cmd_status(self, chat_id: str):
        current = self.user_prefs.get(chat_id, "all")
        s = SIGNAL_TYPES.get(current, SIGNAL_TYPES["all"])
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
            f"Your signal: <b>{s['emoji']} {s['label']}</b>\n"
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
            f"Use /menu to switch signals."
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
        """User selected a signal type. Confirm + replay past signals."""
        if sig_key not in SIGNAL_TYPES:
            return
        s = SIGNAL_TYPES[sig_key]
        # Save preference
        with self._lock:
            self.user_prefs[chat_id] = sig_key
            self._save_prefs()
        # Confirmation message
        confirm = (
            f"✅ <b>Signal type updated!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Now receiving: <b>{s['emoji']} {s['label']}</b>\n"
            f"<i>{s['desc']}</i>\n"
            f"\n"
        )
        # Count available past signals
        past = self._get_history_for(sig_key)
        if past:
            confirm += (
                f"📜 Sending <b>{len(past)}</b> recent signal"
                f"{'s' if len(past) != 1 else ''}...\n"
            )
        else:
            confirm += f"📭 No past signals yet — they'll arrive soon!\n"
        confirm += (
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Use /menu to switch anytime."
        )
        self._send(chat_id, confirm)
        # Replay past signals (max 10)
        if past:
            time.sleep(0.5)
            header = (
                f"📜 <b>RECENT {s['label'].upper()} SIGNALS</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Showing last {len(past)} signal"
                f"{'s' if len(past) != 1 else ''}:"
            )
            self._send(chat_id, header)
            for entry in past[-10:]:
                msg = entry["msg"] if isinstance(entry, dict) else entry
                self._send(chat_id, msg)
                time.sleep(0.5)  # Respect rate limits
    def _get_history_for(self, sig_key: str) -> list:
        """Get the last 10 past signals matching a signal type."""
        s = SIGNAL_TYPES[sig_key]
        if s["opp_types"] is None:
            # "all" — collect from every type
            all_entries = []
            for entries in self.history.values():
                all_entries.extend(entries)
            # Sort by timestamp if available, return last 10
            all_entries.sort(key=lambda e: e.get("ts", 0) if isinstance(e, dict) else 0)
            return all_entries[-10:]
        else:
            result = []
            for opp_type in s["opp_types"]:
                result.extend(self.history.get(opp_type, []))
            result.sort(key=lambda e: e.get("ts", 0) if isinstance(e, dict) else 0)
            return result[-10:]
    # -----------------------------------------------------------------
    # Signal Distribution (called from scan cycle)
    # -----------------------------------------------------------------
    def distribute_signals(self, opportunities: list[Opportunity], cfg: dict):
        """
        Send opportunities to all registered users, filtered by preference.
        Also stores signals in history for replay.
        """
        if not self.enabled or not opportunities:
            return
        min_pct = cfg["telegram"].get("min_alert_profit_pct", 0.5)
        filtered = [o for o in opportunities if o.profit_pct >= min_pct]
        if not filtered:
            return
        # Store in history + format
        formatted: dict[str, list[str]] = defaultdict(list)
        now = time.time()
        for opp in filtered:
            msg = format_opportunity(opp)
            entry = {"ts": now, "msg": msg}
            with self._lock:
                self.history[opp.opp_type].append(entry)
            formatted[opp.opp_type].append(msg)
        # Persist history to disk
        with self._lock:
            self._save_history()
        # Get all registered users (or at least the default chat_id)
        users = dict(self.user_prefs)
        default_id = str(self.default_chat_id)
        if default_id and default_id not in users:
            users[default_id] = "all"
        # For each user, send matching signals
        for chat_id, sig_key in users.items():
            s = SIGNAL_TYPES.get(sig_key, SIGNAL_TYPES["all"])
            want_types = s["opp_types"]  # None means all
            # Filter opportunities for this user
            user_opps = []
            for opp in filtered:
                if want_types is None or opp.opp_type in want_types:
                    user_opps.append(opp)
            if not user_opps:
                continue
            # Send summary header
            type_counts: dict[str, int] = {}
            for o in user_opps:
                type_counts[o.opp_type] = type_counts.get(o.opp_type, 0) + 1
            summary = (
                f"📡 <b>{s['emoji']} {s['label'].upper()} — SCAN RESULTS</b>\n"
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
                    f"💡 <i>Use /menu to switch signal types</i>",
                    self._menu_button(),
                )
