"""
telegram_bot.py — Interactive Telegram Bot with Signal Type Selection + Monetization

Users choose which signal type they want via beautiful inline keyboards.
Only matching signals are delivered. Users can switch anytime.

TIERS:
  🆓 Free     — 5 signals/day, 30-min delay
  ⭐ Pro      — Unlimited real-time signals ($9.99/mo via Telegram Stars)
  💎 Whale    — Everything + priority alerts ($29.99/mo via Telegram Stars)

Commands:
  /start   — Welcome screen + signal picker
  /menu    — Change signal type
  /help    — Show help
  /status  — Current selection + stats
  /upgrade — View plans and subscribe via Telegram Stars
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
# Subscription Tier Definitions
# =========================================================================

TIERS = {
    "free": {
        "emoji": "🆓",
        "label": "Free",
        "daily_limit": 5,
        "delay_seconds": 1800,  # 30-min delay
        "price_stars": 0,
    },
    "pro": {
        "emoji": "⭐",
        "label": "Pro",
        "daily_limit": 999999,  # Unlimited
        "delay_seconds": 0,     # Real-time
        "price_stars": 500,     # ~$9.99
    },
    "whale": {
        "emoji": "💎",
        "label": "Whale",
        "daily_limit": 999999,  # Unlimited
        "delay_seconds": 0,     # Real-time
        "price_stars": 1500,    # ~$29.99
    },
}

# Duration: 30 days in seconds
SUBSCRIPTION_DURATION = 30 * 24 * 3600


# =========================================================================
# Interactive Bot Handler
# =========================================================================

class TelegramBotHandler:
    """
    Interactive Telegram bot that lets users choose signal types.
    Runs a polling thread alongside the main scan loop.
    Now includes Telegram Stars payment for Pro/Whale tiers.
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.token = str(cfg["telegram"].get("bot_token", ""))
        self.default_chat_id = str(cfg["telegram"].get("chat_id", ""))
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.enabled = bool(
            cfg["telegram"].get("enabled") and self.token and self.default_chat_id
        )

        # Preferences file — stores {chat_id_str: signal_type_key}
        self.prefs_file = cfg.get("interactive", {}).get(
            "prefs_file", "user_prefs.json"
        )
        self.user_prefs: dict[str, str] = self._load_prefs()

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

        # Polling internals
        self._last_update_id = 0
        self._lock = threading.Lock()
        self._running = False

        logger.info(
            f"Bot handler init: default_chat_id={self.default_chat_id!r}, "
            f"prefs={self.user_prefs}, enabled={self.enabled}, "
            f"subs={len(self.user_subs)} subscribers"
        )

    # -----------------------------------------------------------------
    # Persistence — Preferences
    # -----------------------------------------------------------------

    def _load_prefs(self) -> dict:
        if not os.path.exists(self.prefs_file):
            return {}
        try:
            with open(self.prefs_file, "r") as f:
                data = json.load(f)
            raw = data.get("users", data) if isinstance(data, dict) else {}
            return {str(k): v for k, v in raw.items()}
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

        return sub

    def _get_tier(self, chat_id: str) -> str:
        """Get effective tier for a user."""
        # Owner always gets whale tier
        if chat_id == self.default_chat_id:
            return "whale"
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

    def _send_invoice(
        self,
        chat_id: str,
        tier_key: str,
    ) -> bool:
        """Send a Telegram Stars payment invoice."""
        tier = TIERS.get(tier_key)
        if not tier or tier["price_stars"] == 0:
            return False

        payload = {
            "chat_id": chat_id,
            "title": f"PocketMoney {tier['label']} — 30 Days",
            "description": self._tier_invoice_desc(tier_key),
            "payload": f"sub:{tier_key}:{chat_id}:{int(time.time())}",
            "currency": "XTR",  # Telegram Stars
            "prices": [
                {"label": f"{tier['label']} Plan (30 days)", "amount": tier["price_stars"]}
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
                logger.warning(
                    f"Invoice failed: {r.status_code} {r.text[:200]}"
                )
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
        elif tier_key == "whale":
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

    @staticmethod
    def _menu_button() -> dict:
        return {
            "inline_keyboard": [
                [
                    {"text": "📋 Menu", "callback_data": "cmd:menu"},
                    {"text": "ℹ️ Help", "callback_data": "cmd:help"},
                ]
            ]
        }

    @staticmethod
    def _upgrade_keyboard() -> dict:
        return {
            "inline_keyboard": [
                [
                    {"text": "⭐ Pro — 500 Stars", "callback_data": "buy:pro"},
                ],
                [
                    {"text": "💎 Whale — 1500 Stars", "callback_data": "buy:whale"},
                ],
                [
                    {"text": "📋 Back to Menu", "callback_data": "cmd:menu"},
                ],
            ]
        }

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
                logger.error(f"Polling error: {e}")
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

        # --- Payment: Successful payment in message ---
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
            elif text == "/help":
                self._cmd_help(chat_id)
            elif text == "/status":
                self._cmd_status(chat_id)
            elif text == "/reset":
                self._cmd_reset(chat_id)
            elif text == "/upgrade":
                self._cmd_upgrade(chat_id)

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
        elif data.startswith("buy:"):
            tier_key = data[4:]
            if tier_key in TIERS and TIERS[tier_key]["price_stars"] > 0:
                self._send_invoice(chat_id, tier_key)
                self._answer_callback(cb_id, f"💳 Opening payment...")
            else:
                self._answer_callback(cb_id)
        elif data == "cmd:menu":
            self._cmd_menu(chat_id)
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
        if len(parts) >= 2 and parts[0] == "sub":
            tier_key = parts[1]
        else:
            tier_key = "pro"  # Fallback

        if tier_key not in TIERS:
            tier_key = "pro"

        # Activate subscription
        now = time.time()
        with self._lock:
            sub = self._get_user_sub(chat_id)

            # If already subscribed, extend from current expiry
            if sub["tier"] != "free" and sub.get("expires_at", 0) > now:
                expires = sub["expires_at"] + SUBSCRIPTION_DURATION
            else:
                expires = now + SUBSCRIPTION_DURATION

            sub["tier"] = tier_key
            sub["expires_at"] = expires
            sub["subscribed_at"] = now
            sub["daily_count"] = 0  # Reset limit on upgrade
            self.user_subs[chat_id] = sub
            self._save_subs()

        # Calculate expiry date
        expiry_str = datetime.fromtimestamp(
            expires, tz=timezone.utc
        ).strftime("%B %d, %Y")

        tier = TIERS[tier_key]

        # Send confirmation
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
        logger.info(
            f"✅ Subscription activated: {chat_id} → {tier_key} until {expiry_str}"
        )

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
            f"\n<b>Choose your signal type:</b>\n"
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

        # Auto-register
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
            f"/start   — Welcome + pick signal type\n"
            f"/menu    — Switch signal type\n"
            f"/status  — View current stats\n"
            f"/upgrade — View plans & subscribe\n"
            f"/help    — This message\n"
            f"\n"
            f"<b>How it works:</b>\n"
            f"1️⃣ Pick a signal type\n"
            f"2️⃣ Receive matching signals\n"
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
            f"<b>Plans:</b>\n"
            f"🆓 Free — 5 signals/day, 30-min delay\n"
            f"⭐ Pro — Unlimited, real-time (500 Stars)\n"
            f"💎 Whale — Everything + priority (1500 Stars)\n"
            f"\n"
            f"Tap /upgrade to go Pro!"
        )
        self._send(chat_id, msg)

    def _cmd_status(self, chat_id: str):
        current = self.user_prefs.get(chat_id, "all")
        s = SIGNAL_TYPES.get(current, SIGNAL_TYPES["all"])

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
            f"Use /menu to switch signals."
        )

        if tier == "free":
            msg += "\n💡 <i>/upgrade for unlimited alerts</i>"

        self._send(chat_id, msg)

    def _cmd_reset(self, chat_id: str):
        with self._lock:
            self.user_prefs.pop(chat_id, None)
            self._save_prefs()
        self._cmd_start(chat_id)

    def _cmd_upgrade(self, chat_id: str):
        """Show upgrade plans with payment buttons."""
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
            self._send(chat_id, msg, self._upgrade_keyboard())
            return

        msg = (
            f"🚀 <b>UPGRADE YOUR PLAN</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"You're on <b>🆓 Free</b> (5 signals/day)\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"⭐ <b>PRO — 500 Stars/month</b>\n"
            f"  ✓ Unlimited signals\n"
            f"  ✓ Real-time alerts (zero delay)\n"
            f"  ✓ All 6 signal types\n"
            f"\n"
            f"💎 <b>WHALE — 1500 Stars/month</b>\n"
            f"  ✓ Everything in Pro\n"
            f"  ✓ Priority alert delivery\n"
            f"  ✓ Whale trade details\n"
            f"  ✓ Daily summary digest\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Pay with Telegram Stars — instant,\n"
            f"no crypto wallet needed!\n"
            f"\n"
            f"👇 <b>Tap to subscribe:</b>"
        )
        self._send(chat_id, msg, self._upgrade_keyboard())

    # -----------------------------------------------------------------
    # Signal type selection + history replay
    # -----------------------------------------------------------------

    def _select_signal(self, chat_id: str, sig_key: str):
        if sig_key not in SIGNAL_TYPES:
            return

        s = SIGNAL_TYPES[sig_key]

        with self._lock:
            self.user_prefs[chat_id] = sig_key
            self._save_prefs()
            logger.info(f"User {chat_id} selected signal type: {sig_key}")

        tier = self._get_tier(chat_id)
        tier_info = TIERS.get(tier, TIERS["free"])

        confirm = (
            f"✅ <b>Signal type updated!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"Now receiving: <b>{s['emoji']} {s['label']}</b>\n"
            f"<i>{s['desc']}</i>\n"
            f"Plan: {tier_info['emoji']} {tier_info['label']}\n"
            f"\n"
        )

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
                msg_text = entry["msg"] if isinstance(entry, dict) else entry
                self._send(chat_id, msg_text)
                time.sleep(0.5)

    def _get_history_for(self, sig_key: str) -> list:
        s = SIGNAL_TYPES[sig_key]

        if s["opp_types"] is None:
            all_entries = []
            for entries in self.history.values():
                all_entries.extend(entries)
            all_entries.sort(key=lambda e: e.get("ts", 0) if isinstance(e, dict) else 0)
            return all_entries[-10:]
        else:
            result = []
            for opp_type in s["opp_types"]:
                result.extend(self.history.get(opp_type, []))
            result.sort(key=lambda e: e.get("ts", 0) if isinstance(e, dict) else 0)
            return result[-10:]

    # -----------------------------------------------------------------
    # Free-tier limit check
    # -----------------------------------------------------------------

    def _check_signal_limit(self, chat_id: str) -> tuple[bool, int]:
        """
        Check if a user can receive more signals today.
        Returns (can_send, remaining).
        """
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
        """Notify user they've hit the free limit with upgrade CTA."""
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
        self._send(chat_id, msg, self._upgrade_keyboard())

    # -----------------------------------------------------------------
    # Signal Distribution (called from scan cycle)
    # -----------------------------------------------------------------

    def distribute_signals(self, opportunities: list[Opportunity], cfg: dict):
        """
        Send opportunities to all registered users, filtered by preference
        AND tier limits.

        THIS IS THE ONLY PATH FOR SENDING SIGNALS.
        """
        if not self.enabled or not opportunities:
            return

        min_pct = cfg["telegram"].get("min_alert_profit_pct", 0.5)
        filtered = [o for o in opportunities if o.profit_pct >= min_pct]
        if not filtered:
            return

        # Store in history
        now = time.time()
        for opp in filtered:
            msg = format_opportunity(opp)
            entry = {"ts": now, "msg": msg}
            with self._lock:
                self.history[opp.opp_type].append(entry)

        with self._lock:
            self._save_history()

        # Build user list
        with self._lock:
            users = dict(self.user_prefs)

        default_id = str(self.default_chat_id)
        if default_id and default_id not in users:
            users[default_id] = "all"

        logger.info(
            f"Distributing {len(filtered)} signals to {len(users)} user(s)"
        )

        for chat_id, sig_key in users.items():
            s = SIGNAL_TYPES.get(sig_key, SIGNAL_TYPES["all"])
            want_types = s["opp_types"]

            # Filter opportunities for this user's chosen type
            user_opps = []
            for opp in filtered:
                if want_types is None or opp.opp_type in want_types:
                    user_opps.append(opp)

            if not user_opps:
                continue

            # --- TIER GATING ---
            can_send, remaining = self._check_signal_limit(chat_id)

            if not can_send:
                # Already hit limit — send upgrade prompt (max once per cycle)
                self._send_limit_reached(chat_id)
                logger.info(f"  User {chat_id}: daily limit reached — sent upgrade CTA")
                continue

            tier = self._get_tier(chat_id)
            tier_info = TIERS.get(tier, TIERS["free"])

            # Cap signals for free users
            if tier == "free":
                user_opps = user_opps[:remaining]

            logger.info(
                f"  User {chat_id} ({sig_key}, {tier}): "
                f"sending {len(user_opps)} signals"
            )

            # Send summary header
            type_counts: dict[str, int] = {}
            for o in user_opps:
                type_counts[o.opp_type] = type_counts.get(o.opp_type, 0) + 1

            live_tag = "🟢 LIVE" if tier != "free" else "🕐 DELAYED"

            summary = (
                f"📡 <b>{s['emoji']} {s['label'].upper()} — {live_tag}</b>\n"
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
                    msg += "\n\n🕐 <i>Delayed 30 min · /upgrade for real-time</i>"
                self._send(chat_id, msg)
                self.signals_sent += 1
                sent_count += 1
                time.sleep(0.5)

            # Track signal count for free users
            self._increment_signal_count(chat_id, sent_count)

            # Periodic menu reminder
            if self.signals_sent % 5 == 0:
                self._send(
                    chat_id,
                    f"💡 <i>/menu to switch signals · /upgrade for Pro</i>",
                    self._menu_button(),
                )
