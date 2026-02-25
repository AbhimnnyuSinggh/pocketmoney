"""
lp_order_manager.py — Order Lifecycle Manager for LP Market Making

Wraps py-clob-client with:
  - State persistence to lp_state.json (survives Render restarts)
  - Risk guards enforced INSIDE order functions (can't be bypassed)
  - Fill detection, cancel-all kill switch, position tracking
  - Dry-run mode that logs but never places real orders

Safety architecture:
  Every function that could deploy capital checks:
  1. max_lp_capital — total $ deployed across all positions
  2. max_position_one_side — max $ on YES or NO
  3. stop_flag — persistent kill switch from /lp stop
"""
import json
import os
import time
import logging
from dataclasses import dataclass, field, asdict

logger = logging.getLogger("arb_bot.lp.orders")

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class LPOrder:
    """Represents a single LP limit order."""
    order_id: str
    token_id: str
    side: str        # "BUY" or "SELL"
    price: float
    size: float      # Number of shares
    status: str      # "OPEN", "FILLED", "CANCELLED", "PARTIAL"
    placed_at: float # timestamp
    filled_at: float = 0.0
    fill_amount: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LPOrder":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class LPPosition:
    """Tracks current LP position across all orders."""
    yes_shares: float = 0.0
    no_shares: float = 0.0
    total_cost: float = 0.0
    total_fills: int = 0
    session_start: float = 0.0

    @property
    def yes_value(self) -> float:
        return self.yes_shares  # At resolution, each share = $1 or $0

    @property
    def no_value(self) -> float:
        return self.no_shares

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LPPosition":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

@dataclass
class LPState:
    """Full LP session state — persisted to disk."""
    active: bool = False
    stop_flag: bool = False
    mode: str = "dry_run"  # "dry_run" or "live"
    market_slug: str = ""
    market_title: str = ""
    condition_id: str = ""
    yes_token_id: str = ""
    no_token_id: str = ""
    end_date: str = ""
    orders: list = field(default_factory=list)      # List of LPOrder dicts
    position: dict = field(default_factory=dict)     # LPPosition dict
    last_midpoint: float = 0.0
    last_updated: float = 0.0
    session_pnl: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LPState":
        valid_fields = cls.__dataclass_fields__
        return cls(**{k: v for k, v in d.items() if k in valid_fields})


# ---------------------------------------------------------------------------
# Order Manager
# ---------------------------------------------------------------------------

class LPOrderManager:
    """
    Manages LP order lifecycle with built-in risk guards.

    Risk guards enforced here (not in engine) so they CAN'T be bypassed:
    - max_lp_capital: Rejects orders if total deployed would exceed cap
    - max_position_one_side: Rejects if YES or NO accumulation too high
    - stop_flag: Rejects ALL orders if kill switch is set
    """

    def __init__(self, cfg: dict, clob_client=None):
        self.cfg = cfg
        self.client = clob_client  # None = dry_run
        lp_cfg = cfg.get("lp_farming", {})
        
        # Inherit execution mode globally from /wallet live
        global_mode = cfg.get("execution", {}).get("mode", "dry_run")
        self.mode = global_mode
        self.state_file = lp_cfg.get("state_file", "lp_state.json")

        # Risk caps
        total_usdc = cfg.get("bankroll", {}).get("total_usdc", 100.0)
        allocs = cfg.get("bankroll", {}).get("allocations", {})
        
        active = cfg.get("execution", {}).get("active_autotrader", "none")
        if active == "lp":
            self.max_lp_capital = total_usdc
            logger.info(f"🏭 LP Engine is ACTIVE module -> Routing 100% capital (${self.max_lp_capital})")
        else:
            self.max_lp_capital = allocs.get("poly_lp", lp_cfg.get("max_lp_capital", 300.0))
        self.max_position_one_side = lp_cfg.get("max_position_one_side", 150.0)
        self.max_loss_per_session = lp_cfg.get("max_loss_per_session", 20.0)
        self.order_size = lp_cfg.get("order_size", 50.0)

        # Load persisted state
        self.state = self._load_state()
        self._dry_order_counter = 0  # For generating fake order IDs in dry_run

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------
    def _load_state(self) -> LPState:
        """Load state from disk. Returns fresh state if no file exists."""
        if not os.path.exists(self.state_file):
            return LPState(mode=self.mode)
        try:
            with open(self.state_file, "r") as f:
                data = json.load(f)
            state = LPState.from_dict(data)
            logger.info(f"LP state loaded: active={state.active} orders={len(state.orders)}")
            return state
        except Exception as e:
            logger.warning(f"Failed to load LP state: {e}")
            return LPState(mode=self.mode)

    def save_state(self):
        """Persist current state to disk."""
        self.state.last_updated = time.time()
        try:
            with open(self.state_file, "w") as f:
                json.dump(self.state.to_dict(), f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save LP state: {e}")

    # ------------------------------------------------------------------
    # Risk checks (called before EVERY order)
    # ------------------------------------------------------------------
    def _check_risk(self, side: str, price: float, size: float) -> tuple[bool, str]:
        """
        Check all risk limits before placing an order.
        Returns (ok, reason).
        """
        # Kill switch
        if self.state.stop_flag:
            return False, "Stop flag is set — /lp stop was triggered"
            
        # Autotrader switch
        active_mod = self.cfg.get("execution", {}).get("active_autotrader", "none")
        if active_mod != "lp":
            return False, f"LP disabled (active autotrader is {active_mod.upper()})"

        # Capital cap
        pos = LPPosition.from_dict(self.state.position) if self.state.position else LPPosition()
        order_cost = price * size
        total_deployed = pos.total_cost + order_cost

        if total_deployed > self.max_lp_capital:
            return False, (
                f"Would exceed max_lp_capital: "
                f"${total_deployed:.2f} > ${self.max_lp_capital:.2f}"
            )

        # One-side accumulation cap
        if side == "BUY":
            new_yes = pos.yes_shares + size
            if new_yes * price > self.max_position_one_side:
                return False, (
                    f"Would exceed max_position_one_side on YES: "
                    f"${new_yes * price:.2f} > ${self.max_position_one_side:.2f}"
                )
        else:
            new_no = pos.no_shares + size
            if new_no * price > self.max_position_one_side:
                return False, (
                    f"Would exceed max_position_one_side on NO: "
                    f"${new_no * price:.2f} > ${self.max_position_one_side:.2f}"
                )

        # Session loss cap
        if self.state.session_pnl < -self.max_loss_per_session:
            return False, (
                f"Session loss limit hit: "
                f"${self.state.session_pnl:.2f} < -${self.max_loss_per_session:.2f}"
            )

        return True, "OK"

    # ------------------------------------------------------------------
    # Order operations
    # ------------------------------------------------------------------
    def place_order(
        self, token_id: str, side: str, price: float, size: float
    ) -> str | None:
        """
        Place a GTC limit order. Returns order_id or None if rejected.
        Risk guards are enforced here — engine can't bypass them.
        """
        # Risk check
        ok, reason = self._check_risk(side, price, size)
        if not ok:
            logger.warning(f"LP order rejected: {reason}")
            return None

        if self.mode == "dry_run" or self.client is None:
            # Dry run — simulate
            self._dry_order_counter += 1
            order_id = f"dry_{self._dry_order_counter}_{int(time.time())}"
            logger.info(
                f"[DRY RUN] LP Order: {side} {size} shares @ ${price:.4f} "
                f"token={token_id[:12]}... → {order_id}"
            )
        else:
            # Live mode — use py-clob-client
            try:
                from py_clob_client.order_builder.constants import BUY, SELL
                order_side = BUY if side == "BUY" else SELL

                order = self.client.create_order(
                    token_id=token_id,
                    price=price,
                    size=size,
                    side=order_side,
                )
                resp = self.client.post_order(order)
                order_id = resp.get("orderID", resp.get("id", f"live_{int(time.time())}"))
                logger.info(
                    f"[LIVE] LP Order placed: {side} {size} @ ${price:.4f} → {order_id}"
                )
            except Exception as e:
                logger.error(f"LP order placement failed: {e}")
                return None

        # Record order in state
        lp_order = LPOrder(
            order_id=order_id,
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            status="OPEN",
            placed_at=time.time(),
        )
        self.state.orders.append(lp_order.to_dict())
        self.save_state()
        return order_id

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a single order by ID."""
        if self.mode == "live" and self.client:
            try:
                self.client.cancel(order_id)
                logger.info(f"[LIVE] Cancelled order {order_id}")
            except Exception as e:
                logger.error(f"Cancel failed for {order_id}: {e}")
                return False
        else:
            logger.info(f"[DRY RUN] Cancel order {order_id}")

        # Update state
        for order in self.state.orders:
            if order.get("order_id") == order_id:
                order["status"] = "CANCELLED"
                break
        self.save_state()
        return True

    def cancel_all_orders(self) -> int:
        """
        Cancel ALL open orders. Used by:
        - Kill switch (/lp stop)
        - Startup recovery
        - Pre-resolution exit
        """
        cancelled = 0
        if self.mode == "live" and self.client:
            try:
                self.client.cancel_all()
                logger.info("[LIVE] Cancel all orders sent to CLOB")
            except Exception as e:
                logger.error(f"Cancel all failed: {e}")
                # Still mark local state as cancelled

        for order in self.state.orders:
            if order.get("status") == "OPEN":
                order["status"] = "CANCELLED"
                cancelled += 1

        self.save_state()
        logger.info(f"Cancelled {cancelled} open orders")
        return cancelled

    def check_fills(self) -> list[dict]:
        """
        Check if any open orders have been filled.
        Returns list of filled order dicts.
        """
        fills = []

        if self.mode == "dry_run" or self.client is None:
            # In dry_run, simulate random fills (10% chance per check)
            import random
            for order in self.state.orders:
                if order.get("status") == "OPEN" and random.random() < 0.10:
                    order["status"] = "FILLED"
                    order["filled_at"] = time.time()
                    order["fill_amount"] = order["size"]
                    fills.append(order)
                    logger.info(
                        f"[DRY RUN] Simulated fill: {order['side']} "
                        f"{order['size']} @ ${order['price']:.4f}"
                    )
        else:
            # Live: check each open order status
            for order in self.state.orders:
                if order.get("status") != "OPEN":
                    continue
                try:
                    status = self.client.get_order(order["order_id"])
                    if status and status.get("status") in ("FILLED", "MATCHED"):
                        order["status"] = "FILLED"
                        order["filled_at"] = time.time()
                        order["fill_amount"] = float(
                            status.get("size_matched", order["size"])
                        )
                        fills.append(order)
                        logger.info(
                            f"[LIVE] Fill detected: {order['side']} "
                            f"{order['fill_amount']} @ ${order['price']:.4f}"
                        )
                except Exception as e:
                    logger.debug(f"Order status check failed: {e}")

        # Update position for each fill
        if fills:
            pos = LPPosition.from_dict(self.state.position) if self.state.position else LPPosition()
            for fill in fills:
                fill_size = fill.get("fill_amount", fill.get("size", 0))
                fill_price = fill.get("price", 0)
                if fill["side"] == "BUY":
                    pos.yes_shares += fill_size
                    pos.total_cost += fill_size * fill_price
                else:
                    pos.no_shares += fill_size
                    pos.total_cost += fill_size * fill_price
                pos.total_fills += 1
            self.state.position = pos.to_dict()
            self.save_state()

        return fills

    def get_position(self) -> LPPosition:
        """Get current position summary."""
        if self.state.position:
            return LPPosition.from_dict(self.state.position)
        return LPPosition()

    def get_open_orders(self) -> list[dict]:
        """Get all currently open orders."""
        return [o for o in self.state.orders if o.get("status") == "OPEN"]

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------
    def start_session(
        self, market_slug: str, market_title: str, condition_id: str,
        yes_token_id: str, no_token_id: str, end_date: str,
    ):
        """Initialize a new LP session for a specific market."""
        self.state = LPState(
            active=True,
            stop_flag=False,
            mode=self.mode,
            market_slug=market_slug,
            market_title=market_title,
            condition_id=condition_id,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            end_date=end_date,
            orders=[],
            position=LPPosition(session_start=time.time()).to_dict(),
            last_midpoint=0.0,
            last_updated=time.time(),
            session_pnl=0.0,
        )
        self.save_state()
        logger.info(f"LP session started: {market_title}")

    def end_session(self):
        """End current session — cancel all, mark inactive."""
        self.cancel_all_orders()
        self.state.active = False
        self.save_state()
        logger.info("LP session ended")

    def set_stop_flag(self):
        """Set persistent stop flag — checked every monitoring tick."""
        self.state.stop_flag = True
        self.save_state()
        logger.info("🛑 LP stop flag set")

    def startup_recovery(self, notify_fn=None):
        """
        Called on boot. Handles Render restart mid-session:
        1. Detect active session from lp_state.json
        2. Cancel all dangling orders (on-chain if live + client available)
        3. Log full position details (so admin knows what happened)
        4. Notify admin via Telegram
        5. Reset to IDLE clean state

        This ensures no orphan orders linger after a crash/restart.
        """
        if not self.state.active:
            logger.info("LP startup recovery: no active session found — clean start")
            return

        market = self.state.market_title or "Unknown Market"
        open_count = len(self.get_open_orders())
        pos = self.get_position()

        logger.warning(
            f"LP STARTUP RECOVERY: found active session for '{market}'\n"
            f"  Mode: {self.state.mode}\n"
            f"  Open orders: {open_count}\n"
            f"  Position: YES={pos.yes_shares:.1f} NO={pos.no_shares:.1f}\n"
            f"  Cost: ${pos.total_cost:.2f} | P&L: ${self.state.session_pnl:.2f}\n"
            f"  Last midpoint: ${self.state.last_midpoint:.4f}"
        )

        # Attempt on-chain cancel if live mode + client available
        if self.state.mode == "live" and self.client is not None:
            try:
                self.client.cancel_all()
                logger.info("[LIVE] Sent cancel_all to CLOB API during recovery")
            except Exception as e:
                logger.error(f"Recovery cancel_all failed: {e}")
                # Not fatal — orders may have already expired

        # Mark all local orders as cancelled
        cancelled = 0
        for order in self.state.orders:
            if order.get("status") == "OPEN":
                order["status"] = "CANCELLED"
                cancelled += 1

        # Reset to idle
        self.state.active = False
        self.state.stop_flag = False
        self.save_state()

        recovery_msg = (
            f"♻️ <b>LP Startup Recovery</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Render restarted while LP was active.\n"
            f"Market: {market[:50]}\n"
            f"\n"
            f"🛑 Cancelled {cancelled} dangling orders\n"
            f"📊 Position at crash:\n"
            f"  YES: {pos.yes_shares:.1f} | NO: {pos.no_shares:.1f}\n"
            f"  Cost: ${pos.total_cost:.2f}\n"
            f"\n"
            f"✅ State cleared. Use /lp start to resume."
        )

        # Notify admin if callback provided
        if notify_fn:
            try:
                notify_fn(recovery_msg)
            except Exception:
                pass  # Don't crash on notification failure

        logger.info(f"LP startup recovery complete — {cancelled} orders cancelled")

    def format_status(self) -> str:
        """Format current LP state for Telegram display."""
        if not self.state.active:
            return (
                "🏭 <b>LP Status</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "Status: <b>IDLE</b> — No active LP session\n"
                "\nUse /lp start to begin farming"
            )

        pos = self.get_position()
        open_orders = len(self.get_open_orders())
        mode_emoji = "🔴 LIVE" if self.mode == "live" else "🟡 DRY RUN"
        elapsed = time.time() - pos.session_start if pos.session_start else 0
        hours = elapsed / 3600

        return (
            f"🏭 <b>LP Status — {mode_emoji}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Market: {self.state.market_title[:50]}\n"
            f"⏱ Running: {hours:.1f}h\n"
            f"\n"
            f"📈 <b>Position:</b>\n"
            f"  YES shares: {pos.yes_shares:.1f}\n"
            f"  NO shares: {pos.no_shares:.1f}\n"
            f"  Total cost: ${pos.total_cost:.2f}\n"
            f"  Fills: {pos.total_fills}\n"
            f"\n"
            f"🔧 Open orders: {open_orders}\n"
            f"💰 Session P&L: ${self.state.session_pnl:.2f}\n"
            f"\n"
            f"⚙️ Caps: ${self.max_lp_capital:.0f} max | "
            f"${self.max_position_one_side:.0f}/side | "
            f"${self.max_loss_per_session:.0f} max loss"
        )
