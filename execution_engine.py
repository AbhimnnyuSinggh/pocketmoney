"""
execution_engine.py — Trading Infrastructure for Bond Spreader & Manual Trading

Provides:
  - MarketLookup: Cached Gamma API market data (slug → token IDs)
  - WalletManager: Encrypted per-user wallet storage
  - ExecutionEngine: Trade creation, execution (live/dry_run/assisted), tracking
  - TradeOrder: Dataclass for trade lifecycle

All crypto deps are gracefully imported — bot starts even if missing,
falling back to "assisted" mode (Polymarket deep links, no real orders).
"""
import os
import json
import time
import logging
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

import requests

logger = logging.getLogger("arb_bot.execution")

# ---------------------------------------------------------------------------
# Graceful crypto imports
# ---------------------------------------------------------------------------
try:
    from cryptography.fernet import Fernet
    HAS_FERNET = True
except ImportError:
    HAS_FERNET = False
    logger.warning("cryptography not installed — wallet encryption disabled")

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.order_builder.constants import BUY as CLOB_BUY, SELL as CLOB_SELL
    HAS_CLOB = True
except ImportError:
    HAS_CLOB = False
    ClobClient = None
    logger.info("py-clob-client not installed — live trading disabled")

# ---------------------------------------------------------------------------
# Encryption helpers
# ---------------------------------------------------------------------------
_ENC_KEY = os.environ.get("WALLET_ENCRYPTION_KEY", "")
_fernet = None

if HAS_FERNET and _ENC_KEY:
    try:
        _fernet = Fernet(_ENC_KEY.encode() if isinstance(_ENC_KEY, str) else _ENC_KEY)
    except Exception:
        logger.warning("WALLET_ENCRYPTION_KEY invalid — generating new key")
        _ENC_KEY = Fernet.generate_key().decode()
        _fernet = Fernet(_ENC_KEY.encode())
        with open(".wallet_encryption_key", "w") as f:
            f.write(_ENC_KEY)
        logger.warning(f"⚠️ Generated new encryption key — saved to .wallet_encryption_key")
elif HAS_FERNET and not _ENC_KEY:
    _ENC_KEY = Fernet.generate_key().decode()
    _fernet = Fernet(_ENC_KEY.encode())
    with open(".wallet_encryption_key", "w") as f:
        f.write(_ENC_KEY)
    logger.warning(f"No WALLET_ENCRYPTION_KEY set — auto-generated and saved to .wallet_encryption_key")

import base64

def _encrypt(plaintext: str) -> str:
    if _fernet:
        return _fernet.encrypt(plaintext.encode()).decode()
    return base64.b64encode(plaintext.encode()).decode()

def _decrypt(ciphertext: str) -> str:
    if _fernet:
        return _fernet.decrypt(ciphertext.encode()).decode()
    return base64.b64decode(ciphertext.encode()).decode()


# ---------------------------------------------------------------------------
# MarketLookup — Cached Gamma API interface
# ---------------------------------------------------------------------------
class MarketLookup:
    """Static market data lookup with 30-second cache."""

    _cache: dict = {}
    _cache_ts: dict = {}
    _TTL = 30
    _BASE = "https://gamma-api.polymarket.com"

    @classmethod
    def get_market(cls, slug: str) -> dict | None:
        """Fetch market by slug, with caching."""
        now = time.time()
        if slug in cls._cache and (now - cls._cache_ts.get(slug, 0)) < cls._TTL:
            return cls._cache[slug]

        try:
            resp = requests.get(
                f"{cls._BASE}/markets",
                params={"slug": slug, "closed": "false"},
                timeout=5,
            )
            data = resp.json() if resp.status_code == 200 else []

            if not data:
                resp = requests.get(
                    f"{cls._BASE}/markets",
                    params={"slug_contains": slug, "limit": 1},
                    timeout=5,
                )
                data = resp.json() if resp.status_code == 200 else []

            if not data:
                return None

            m = data[0] if isinstance(data, list) else data

            prices_raw = m.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                prices = json.loads(prices_raw)
            else:
                prices = prices_raw or []

            clob_raw = m.get("clobTokenIds", "[]")
            if isinstance(clob_raw, str):
                clob_ids = json.loads(clob_raw)
            else:
                clob_ids = clob_raw or []

            events = m.get("events", [])
            event_slug = events[0].get("slug", slug) if events else slug

            market = {
                "title": m.get("question", ""),
                "slug": m.get("slug", slug),
                "event_slug": event_slug,
                "condition_id": m.get("conditionId", ""),
                "yes_price": float(prices[0]) if prices else 0,
                "no_price": float(prices[1]) if len(prices) > 1 else 0,
                "yes_token_id": clob_ids[0] if clob_ids else "",
                "no_token_id": clob_ids[1] if len(clob_ids) > 1 else "",
                "clob_token_ids": clob_ids,
                "volume_24h": float(m.get("volume24hr", 0) or 0),
                "liquidity": float(m.get("liquidity", 0) or 0),
                "end_date": m.get("endDate", ""),
                "active": m.get("active", True),
                "closed": m.get("closed", False),
                "url": f"https://polymarket.com/event/{event_slug}",
            }

            cls._cache[slug] = market
            cls._cache_ts[slug] = now
            return market

        except Exception as e:
            logger.debug(f"MarketLookup error for {slug}: {e}")
            return None

    @classmethod
    def get_token_id(cls, slug: str, side: str) -> str | None:
        m = cls.get_market(slug)
        if not m:
            return None
        return m["yes_token_id"] if side.upper() == "YES" else m["no_token_id"]

    @classmethod
    def search_markets(cls, query: str, limit: int = 5) -> list[dict]:
        try:
            resp = requests.get(
                f"{cls._BASE}/markets",
                params={"_q": query, "closed": "false", "active": "true", "limit": limit},
                timeout=5,
            )
            if resp.status_code != 200:
                return []
            raw = resp.json()
            results = []
            for m in raw:
                prices_raw = m.get("outcomePrices", "[]")
                if isinstance(prices_raw, str):
                    prices = json.loads(prices_raw)
                else:
                    prices = prices_raw or []
                results.append({
                    "title": m.get("question", ""),
                    "slug": m.get("slug", ""),
                    "yes_price": float(prices[0]) if prices else 0,
                    "no_price": float(prices[1]) if len(prices) > 1 else 0,
                })
            return results
        except Exception:
            return []


# ---------------------------------------------------------------------------
# WalletManager — Encrypted per-user wallet storage
# ---------------------------------------------------------------------------
class WalletManager:
    """Manages encrypted wallet keys in user_wallets.json."""

    def __init__(self, wallets_file: str = "user_wallets.json"):
        self._file = wallets_file
        self._lock = threading.Lock()
        self._wallets = self._load()

    def _load(self) -> dict:
        if not os.path.exists(self._file):
            return {}
        try:
            with open(self._file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

    def _save(self):
        with self._lock:
            try:
                with open(self._file, "w") as f:
                    json.dump(self._wallets, f, indent=2)
            except IOError as e:
                logger.error(f"Wallet save failed: {e}")

    def has_wallet(self, chat_id: str) -> bool:
        return str(chat_id) in self._wallets

    def store_wallet(self, chat_id: str, private_key: str,
                     funder_address: str, signature_type: int = 1) -> bool:
        try:
            self._wallets[str(chat_id)] = {
                "encrypted_key": _encrypt(private_key),
                "funder_address": funder_address,
                "signature_type": signature_type,
                "stored_at": time.time(),
                "mode": "dry_run",
                "daily_limit": 200.0,
                "max_per_trade": 50.0,
            }
            self._save()
            return True
        except Exception as e:
            logger.error(f"Wallet store failed: {e}")
            return False

    def get_wallet(self, chat_id: str) -> dict | None:
        w = self._wallets.get(str(chat_id))
        if not w:
            return None
        return {k: v for k, v in w.items() if k != "encrypted_key"}

    def get_decrypted_key(self, chat_id: str) -> str | None:
        w = self._wallets.get(str(chat_id))
        if not w:
            return None
        try:
            return _decrypt(w["encrypted_key"])
        except Exception:
            return None

    def get_mode(self, chat_id: str) -> str:
        w = self._wallets.get(str(chat_id))
        if not w:
            return "assisted"
        return w.get("mode", "dry_run")

    def set_mode(self, chat_id: str, mode: str):
        if str(chat_id) in self._wallets:
            self._wallets[str(chat_id)]["mode"] = mode
            self._save()

    def set_limits(self, chat_id: str, daily_limit: float | None = None,
                   max_per_trade: float | None = None):
        w = self._wallets.get(str(chat_id))
        if not w:
            return
        if daily_limit is not None:
            w["daily_limit"] = daily_limit
        if max_per_trade is not None:
            w["max_per_trade"] = max_per_trade
        self._save()

    def remove_wallet(self, chat_id: str) -> bool:
        if str(chat_id) in self._wallets:
            del self._wallets[str(chat_id)]
            self._save()
            return True
        return False

    def create_clob_client(self, chat_id: str):
        if not HAS_CLOB:
            return None
        pk = self.get_decrypted_key(chat_id)
        w = self._wallets.get(str(chat_id))
        if not pk or not w:
            return None
        try:
            return ClobClient(
                host="https://clob.polymarket.com",
                key=pk,
                chain_id=137,
                funder=w.get("funder_address", ""),
                signature_type=w.get("signature_type", 1),
            )
        except Exception as e:
            logger.error(f"CLOB client creation failed: {e}")
            return None


# ---------------------------------------------------------------------------
# TradeOrder dataclass
# ---------------------------------------------------------------------------
@dataclass
class TradeOrder:
    trade_id: str
    market_title: str
    market_slug: str
    condition_id: str
    token_id: str
    side: str               # YES or NO
    action: str             # BUY or SELL
    price: float
    size: float             # USDC amount
    shares: float           # size / price
    status: str = "pending" # pending/placed/filled/failed/cancelled/assisted
    order_id: str = ""
    created_at: float = 0.0
    executed_at: float = 0.0
    result: str = ""
    url: str = ""
    user_id: str = ""
    mode: str = ""
    error: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "TradeOrder":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# ExecutionEngine — Multi-user trade execution
# ---------------------------------------------------------------------------
class ExecutionEngine:
    """
    Main trading engine. Routes to live (CLOB), dry_run, or assisted mode.
    Tracks trades, enforces daily limits, stores history.
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.wallet_manager = WalletManager()

        exec_cfg = cfg.get("execution", {})
        self.global_mode = exec_cfg.get("mode", "dry_run")

        # Admin wallet from env (backward compat)
        self._admin_pk = os.environ.get("POLY_PRIVATE_KEY", "")
        self._admin_funder = os.environ.get("POLY_FUNDER_ADDRESS", "")
        self._admin_chat_id = str(cfg.get("telegram", {}).get("chat_id", ""))
        self._admin_client = None

        if self._admin_pk and self._admin_funder and HAS_CLOB:
            try:
                self._admin_client = ClobClient(
                    host="https://clob.polymarket.com",
                    key=self._admin_pk,
                    chain_id=137,
                    funder=self._admin_funder,
                    signature_type=1,
                )
                logger.info("Admin CLOB client initialized")
            except Exception as e:
                logger.error(f"Admin CLOB init failed: {e}")

        # Trade tracking
        self._pending: dict[str, TradeOrder] = {}
        self._daily_spend: dict[str, dict] = {}  # {chat_id: {date, spent}}
        self._history: list[dict] = self._load_history()
        self._lock = threading.Lock()
        self._dry_counter = 0

    # ------------------------------------------------------------------
    # Trade creation & execution
    # ------------------------------------------------------------------
    def create_trade(self, market_slug: str, side: str, action: str,
                     amount: float, user_id: str = "") -> TradeOrder | None:
        user_id = user_id or self._admin_chat_id
        mode = self._get_user_mode(user_id)

        # Risk checks
        wallet = self.wallet_manager.get_wallet(user_id)
        max_trade = wallet.get("max_per_trade", 50.0) if wallet else 50.0
        daily_limit = wallet.get("daily_limit", 200.0) if wallet else 200.0

        if amount > max_trade:
            logger.warning(f"Trade rejected: ${amount} > max_per_trade ${max_trade}")
            return None

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily = self._daily_spend.get(user_id, {})
        if daily.get("date") != today:
            daily = {"date": today, "spent": 0.0}
        if daily["spent"] + amount > daily_limit:
            logger.warning(f"Trade rejected: daily limit ${daily_limit} exceeded")
            return None

        # Fetch market
        market = MarketLookup.get_market(market_slug)
        if not market:
            logger.warning(f"Market not found: {market_slug}")
            return None

        token_id = market["yes_token_id"] if side.upper() == "YES" else market["no_token_id"]
        current_price = market["yes_price"] if side.upper() == "YES" else market["no_price"]
        shares = amount / current_price if current_price > 0 else 0

        trade = TradeOrder(
            trade_id=uuid.uuid4().hex[:12],
            market_title=market["title"][:80],
            market_slug=market_slug,
            condition_id=market.get("condition_id", ""),
            token_id=token_id,
            side=side.upper(),
            action=action.upper(),
            price=round(current_price, 4),
            size=round(amount, 2),
            shares=round(shares, 4),
            status="pending",
            created_at=time.time(),
            url=market.get("url", ""),
            user_id=user_id,
            mode=mode,
        )

        self._pending[trade.trade_id] = trade
        return trade

    def execute_trade(self, trade_id: str) -> TradeOrder | None:
        trade = self._pending.pop(trade_id, None)
        if not trade:
            return None

        # Re-check price for slippage
        market = MarketLookup.get_market(trade.market_slug)
        if market and trade.action == "BUY":
            live_price = market["yes_price"] if trade.side == "YES" else market["no_price"]
            if live_price > trade.price * 1.03:
                trade.status = "failed"
                trade.error = f"Slippage: price moved from {trade.price:.4f} to {live_price:.4f}"
                return trade

        if trade.mode == "live":
            trade = self._execute_live(trade)
        elif trade.mode == "dry_run":
            trade = self._execute_dry(trade)
        else:
            trade = self._execute_assisted(trade)

        # Record daily spend
        if trade.status in ("placed", "filled"):
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            daily = self._daily_spend.setdefault(trade.user_id, {"date": today, "spent": 0.0})
            if daily["date"] != today:
                daily["date"] = today
                daily["spent"] = 0.0
            daily["spent"] += trade.size

        # Save history
        self._history.append(trade.to_dict())
        if len(self._history) > 200:
            self._history = self._history[-200:]
        self._save_history()

        trade.executed_at = time.time()
        return trade

    def execute_trade_auto(self, market_slug: str, side: str, action: str,
                           amount: float, user_id: str = "") -> TradeOrder | None:
        trade = self.create_trade(market_slug, side, action, amount, user_id)
        if not trade:
            return None
        return self.execute_trade(trade.trade_id)

    def cancel_trade(self, trade_id: str) -> bool:
        if trade_id in self._pending:
            del self._pending[trade_id]
            return True
        return False

    # ------------------------------------------------------------------
    # Execution modes
    # ------------------------------------------------------------------
    def _execute_live(self, trade: TradeOrder) -> TradeOrder:
        client = self._get_client(trade.user_id)
        if not client:
            trade.status = "failed"
            trade.error = "No CLOB client available"
            return trade

        try:
            clob_side = CLOB_BUY if trade.action == "BUY" else CLOB_SELL
            order = client.create_order(
                token_id=trade.token_id,
                price=trade.price,
                size=trade.shares,
                side=clob_side,
            )
            resp = client.post_order(order)
            trade.order_id = resp.get("orderID", resp.get("id", f"live_{int(time.time())}"))
            trade.status = "placed"
            trade.result = "Order placed on CLOB"
            logger.info(f"[LIVE] {trade.action} {trade.shares:.2f} {trade.side} @ ${trade.price:.4f}")
        except Exception as e:
            trade.status = "failed"
            trade.error = str(e)[:100]
            logger.error(f"Live trade failed: {e}")

        return trade

    def _execute_dry(self, trade: TradeOrder) -> TradeOrder:
        self._dry_counter += 1
        trade.order_id = f"DRY-{self._dry_counter}-{int(time.time())}"
        trade.status = "filled"
        trade.result = "Simulated fill (dry run)"
        logger.info(
            f"[DRY] {trade.action} {trade.shares:.2f} {trade.side} @ ${trade.price:.4f} "
            f"(${trade.size:.2f})"
        )
        return trade

    def _execute_assisted(self, trade: TradeOrder) -> TradeOrder:
        trade.status = "assisted"
        trade.result = "Deep link generated — manual execution required"
        event_slug = trade.url.split("/event/")[-1] if "/event/" in trade.url else trade.market_slug
        trade.url = f"https://polymarket.com/event/{event_slug}"
        return trade

    # ------------------------------------------------------------------
    # Positions & summary
    # ------------------------------------------------------------------
    def get_positions(self, user_id: str | None = None) -> list[dict]:
        positions: dict[str, dict] = {}
        for t in self._history:
            if user_id and t.get("user_id") != user_id:
                continue
            if t.get("status") not in ("placed", "filled"):
                continue
            key = f"{t['market_slug']}:{t['side']}"
            pos = positions.setdefault(key, {
                "title": t["market_title"],
                "slug": t["market_slug"],
                "side": t["side"],
                "shares": 0.0,
                "total_cost": 0.0,
                "url": t.get("url", ""),
            })
            if t["action"] == "BUY":
                pos["shares"] += t.get("shares", 0)
                pos["total_cost"] += t.get("size", 0)
            else:
                pos["shares"] -= t.get("shares", 0)
                pos["total_cost"] -= t.get("size", 0)

        return [p for p in positions.values() if abs(p["shares"]) > 0.01]

    def get_trade_summary(self, user_id: str | None = None) -> dict:
        total = sum(1 for t in self._history
                    if not user_id or t.get("user_id") == user_id)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily = self._daily_spend.get(user_id or self._admin_chat_id, {})
        return {
            "total_trades": total,
            "daily_spent": daily.get("spent", 0) if daily.get("date") == today else 0,
            "mode": self._get_user_mode(user_id or self._admin_chat_id),
            "clob_connected": self._admin_client is not None or HAS_CLOB,
        }

    # ------------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------------
    def format_trade_result(self, trade: TradeOrder) -> str:
        mode_label = {"live": "🔴 LIVE", "dry_run": "🟡 DRY", "assisted": "🔵 LINK"}.get(
            trade.mode, "⚪"
        )
        status_emoji = {"filled": "✅", "placed": "📝", "failed": "❌", "assisted": "🔗"}.get(
            trade.status, "⚪"
        )
        msg = (
            f"{status_emoji} <b>{trade.action} {trade.side}</b> ({mode_label})\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 {trade.market_title[:50]}\n"
            f"💰 ${trade.size:.2f} → {trade.shares:.2f} shares @ ${trade.price:.4f}\n"
        )
        if trade.order_id:
            msg += f"🔑 <code>{trade.order_id[:25]}</code>\n"
        if trade.error:
            msg += f"⚠️ {trade.error}\n"
        if trade.url and trade.mode == "assisted":
            msg += f"\n🔗 <a href=\"{trade.url}\">Open on Polymarket</a>\n"
        return msg

    def format_wallet_status(self, chat_id: str) -> str:
        w = self.wallet_manager.get_wallet(chat_id)
        if not w:
            return (
                "💳 <b>Wallet Status</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                "❌ No wallet connected.\n\n"
                "To connect:\n"
                "<code>/wallet set PRIVATE_KEY FUNDER_ADDRESS</code>\n\n"
                "⚠️ Your key is encrypted instantly and the message is deleted."
            )
        mode_label = {"live": "🔴 LIVE", "dry_run": "🟡 DRY RUN", "assisted": "🔵 ASSISTED"}.get(
            w.get("mode", "dry_run"), "⚪"
        )
        return (
            f"💳 <b>Wallet Status</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Wallet connected\n"
            f"Mode: {mode_label}\n"
            f"Funder: <code>{w.get('funder_address', '')[:10]}...{w.get('funder_address', '')[-6:]}</code>\n"
            f"Daily limit: ${w.get('daily_limit', 200):.0f}\n"
            f"Max per trade: ${w.get('max_per_trade', 50):.0f}\n"
            f"Stored: {datetime.fromtimestamp(w.get('stored_at', 0)).strftime('%Y-%m-%d')}\n"
            f"CLOB: {'✅' if HAS_CLOB else '❌'}\n"
            f"\n/wallet live · /wallet dryrun · /wallet limit · /wallet remove"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_user_mode(self, user_id: str) -> str:
        if user_id == self._admin_chat_id:
            if self._admin_client and self.global_mode == "live":
                return "live"
            return self.global_mode
        return self.wallet_manager.get_mode(user_id)

    def _get_client(self, user_id: str):
        if user_id == self._admin_chat_id and self._admin_client:
            return self._admin_client
        return self.wallet_manager.create_clob_client(user_id)

    def _load_history(self) -> list[dict]:
        try:
            if os.path.exists("trade_history.json"):
                with open("trade_history.json", "r") as f:
                    return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
        return []

    def _save_history(self):
        with self._lock:
            try:
                with open("trade_history.json", "w") as f:
                    json.dump(self._history[-200:], f, indent=2, default=str)
            except IOError as e:
                logger.error(f"Trade history save failed: {e}")
