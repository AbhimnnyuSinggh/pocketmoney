"""
lp_engine.py — LP Market Making Engine (The Brain)

Daemon thread that implements the full LP farming loop with
comprehensive risk management:

  1. Pick best market → place initial order (far back, 2nd-3rd level)
  2. Monitor midpoint every 30s → adjust if moved > 1¢
  3. Detect fills → IMMEDIATELY flip to opposite side (mandatory)
  4. Price crash protection → trailing sell, emergency close
  5. Pre-resolution exit → cancel all 2h before end
  6. Loss limit → auto-kill if session P&L < -$max_loss
  7. CLOB API failure → pause, retry with backoff, notify admin
  8. Reward rule detection → notify on major spread/volume changes

State machine:
  IDLE → SCANNING → PLACED → MONITORING → FILLED → FLIPPING → MONITORING
                                                         ↓
                                                   UNWINDING → IDLE
                                                         ↓
                                               EMERGENCY_CLOSE → IDLE

Safety: Engine NEVER places orders directly — goes through LPOrderManager
which has risk guards enforced inside each function.

Price Crash Strategy (from user):
  1. Fill at 46¢, price drops to 40¢ then 30¢
  2. Step 1: Immediately flip to SELL side, place far back inside spread
  3. Step 2: Trail the falling price — keep moving sell offer down slowly
  4. Step 3: Emergency close if drop > crash_threshold (sell at market to cut loss)
  5. Step 4: After calm, return to normal one-side far-back strategy
"""
import time
import threading
import logging
from datetime import datetime, timezone

logger = logging.getLogger("arb_bot.lp.engine")


class LPEngine:
    """
    Autonomous LP market-making loop. Runs as a daemon thread.

    Controls:
      start(market) — begin farming a specific market
      stop()        — kill switch (cancels all, stops loop)
      status()      — current state summary

    The engine pushes Telegram notifications via a callback function.
    """

    # States
    IDLE = "IDLE"
    SCANNING = "SCANNING"
    PLACED = "PLACED"
    MONITORING = "MONITORING"
    FILLED = "FILLED"
    FLIPPING = "FLIPPING"
    UNWINDING = "UNWINDING"
    EMERGENCY_CLOSE = "EMERGENCY_CLOSE"
    API_PAUSED = "API_PAUSED"

    def __init__(self, cfg: dict, order_manager, notify_fn=None):
        """
        Args:
            cfg: Full config dict
            order_manager: LPOrderManager instance
            notify_fn: Callback fn(text: str) to send Telegram messages
        """
        self.cfg = cfg
        self.om = order_manager  # LPOrderManager
        self.notify = notify_fn or (lambda msg: logger.info(f"[NOTIFY] {msg}"))

        lp_cfg = cfg.get("lp_farming", {})
        self.rebalance_interval = lp_cfg.get("rebalance_interval", 30)
        self.midpoint_threshold = lp_cfg.get("midpoint_move_threshold", 0.01)
        self.max_spread = lp_cfg.get("max_spread", 0.04)
        self.pre_exit_hours = lp_cfg.get("pre_resolution_exit_hours", 2)
        self.order_size = lp_cfg.get("order_size", 50.0)

        # Crash protection thresholds
        self.crash_threshold = lp_cfg.get("crash_threshold", 0.08)    # 8¢ drop = crash
        self.trail_step = lp_cfg.get("trail_step", 0.005)             # 0.5¢ trailing step
        self.emergency_close_pct = lp_cfg.get("emergency_close_pct", 0.15)  # 15% drop = emergency

        # API failure handling
        self.api_max_retries = lp_cfg.get("api_max_retries", 5)
        self.api_retry_backoff = lp_cfg.get("api_retry_backoff", 10)  # seconds

        self.state = self.IDLE
        self._thread = None
        self._running = False
        self._current_side = "BUY"  # Start by buying
        self._last_midpoint = 0.0
        self._fill_midpoint = 0.0   # Midpoint at time of last fill (for crash detection)
        self._current_order_id = None
        self._current_token_id = None
        self._consecutive_api_failures = 0
        self._trailing_active = False  # True when trailing a falling price after fill

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def start(self, market: dict):
        """
        Start LP farming on a specific market.

        market dict must contain:
          - slug, title, condition_id, yes_token_id, no_token_id, end_date
        """
        if self._running:
            logger.warning("LP Engine already running")
            return

        # Initialize session in order manager
        self.om.start_session(
            market_slug=market.get("slug", ""),
            market_title=market.get("title", ""),
            condition_id=market.get("condition_id", ""),
            yes_token_id=market.get("yes_token_id", ""),
            no_token_id=market.get("no_token_id", ""),
            end_date=market.get("end_date", ""),
        )

        self._running = True
        self._current_side = "BUY"
        self._consecutive_api_failures = 0
        self._trailing_active = False
        self.state = self.SCANNING

        mode_label = "🔴 LIVE" if self.om.mode == "live" else "🟡 DRY RUN"
        self.notify(
            f"🟢 <b>LP Farming Started</b> ({mode_label})\n"
            f"📊 {market.get('title', '')[:60]}\n"
            f"💰 Order size: ${self.order_size:.0f}\n"
            f"⚙️ Rebalance: every {self.rebalance_interval}s\n"
            f"🛡 Crash threshold: {self.crash_threshold*100:.0f}¢ | "
            f"Emergency: {self.emergency_close_pct*100:.0f}%"
        )

        self._thread = threading.Thread(
            target=self._main_loop,
            daemon=True,
            name="lp-engine",
        )
        self._thread.start()
        logger.info("LP Engine thread started")

    def stop(self):
        """Kill switch — cancel all orders and stop the engine."""
        self._running = False
        self.om.set_stop_flag()
        self.om.cancel_all_orders()
        self.state = self.IDLE
        self._trailing_active = False

        pos = self.om.get_position()
        self.notify(
            "🛑 <b>LP Farming Stopped</b>\n"
            "All orders cancelled. Session ended.\n"
            f"\n📊 Final: YES={pos.yes_shares:.0f} NO={pos.no_shares:.0f} "
            f"Cost=${pos.total_cost:.2f}"
        )
        logger.info("LP Engine stopped via kill switch")

    def get_status(self) -> str:
        """Get formatted status for Telegram."""
        return self.om.format_status()

    @property
    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def _main_loop(self):
        """
        The core monitoring loop. Runs every rebalance_interval seconds.

        Each tick:
        1. Check stop flag
        2. Check time to resolution → unwind if < 2h
        3. Check unrealized loss → unwind if exceeds cap
        4. Check for price crash → emergency close or trail
        5. Read order book → adjust if midpoint moved
        6. Check fills → flip to opposite side if filled
        """
        logger.info("LP Engine main loop entered")

        # Small delay to let things initialize
        time.sleep(2)

        # Place initial order
        self._place_initial_order()

        while self._running:
            try:
                self._tick()
            except Exception as e:
                logger.error(f"LP Engine tick error: {e}", exc_info=True)
                # Don't crash — but count as API failure if it looks like one
                if "request" in str(e).lower() or "connection" in str(e).lower():
                    self._handle_api_failure(str(e))

            # Sleep in 1s increments so stop is responsive
            # Use shorter interval during trailing (need faster reaction)
            interval = 5 if self._trailing_active else self.rebalance_interval
            for _ in range(interval):
                if not self._running:
                    break
                time.sleep(1)

        # Cleanup
        self.state = self.IDLE
        self.om.end_session()
        logger.info("LP Engine main loop exited")

    def _tick(self):
        """Single monitoring tick."""

        # 1. Check stop flag
        if self.om.state.stop_flag:
            logger.info("Stop flag detected — exiting")
            self._running = False
            return

        # 2. Check time to resolution
        if self._should_unwind_time():
            self._unwind("⏰ Market resolving within 2 hours")
            return

        # 3. Check session loss limit
        if self._should_unwind_loss():
            self._unwind("💸 Session loss limit exceeded")
            return

        # 4. Read order book (with API failure handling)
        snapshot = self._safe_read_book()
        if snapshot is None:
            # API failure handled inside _safe_read_book
            return

        # Reset API failure counter on success
        self._consecutive_api_failures = 0

        # 5. Check for price crash (only after a fill)
        if self._fill_midpoint > 0:
            crash_result = self._check_price_crash(snapshot)
            if crash_result == "EMERGENCY":
                return  # Emergency close triggered
            elif crash_result == "TRAILING":
                pass  # Trailing is handled below in rebalance

        # 6. Rebalance if midpoint moved
        self._check_and_rebalance(snapshot)

        # 7. Check for fills
        fills = self.om.check_fills()
        if fills:
            self._handle_fills(fills, snapshot)

    # ------------------------------------------------------------------
    # API failure handling
    # ------------------------------------------------------------------
    def _safe_read_book(self):
        """
        Read order book with retry logic.
        If CLOB API is down, pauses and retries with backoff.
        After max retries, cancels all and notifies admin.
        """
        from lp_orderbook import read_book

        token_id = self._current_token_id or self.om.state.yes_token_id
        client = self.om.client

        try:
            snapshot = read_book(client, token_id, self.max_spread)
            if snapshot is not None:
                return snapshot
        except Exception as e:
            logger.warning(f"Order book read error: {e}")

        # API failure
        self._handle_api_failure("Order book read failed")
        return None

    def _handle_api_failure(self, error_msg: str):
        """
        Handle CLOB API failure with escalating response:
          1-3 failures: Log + wait with backoff
          4: Notify admin via Telegram
          5+: Cancel all orders and pause (safe state)
        """
        self._consecutive_api_failures += 1
        failures = self._consecutive_api_failures

        if failures <= 3:
            # Just wait and retry
            wait = self.api_retry_backoff * failures
            logger.warning(
                f"API failure #{failures}: {error_msg} — "
                f"retrying in {wait}s"
            )
            time.sleep(wait)

        elif failures == 4:
            # Notify admin
            self.notify(
                f"⚠️ <b>CLOB API Issues</b>\n"
                f"4 consecutive failures.\n"
                f"Last error: {error_msg[:100]}\n"
                f"Bot is retrying. Orders are still GTC (safe)."
            )
            time.sleep(self.api_retry_backoff * 4)

        else:
            # 5+ failures — go to safe state
            self.state = self.API_PAUSED
            self.notify(
                f"🛑 <b>CLOB API DOWN — Safe Mode</b>\n"
                f"{failures} consecutive failures.\n"
                f"Cancelling all orders as precaution.\n"
                f"Your GTC orders were likely already safe on-chain.\n"
                f"Bot will retry every 60s. Use /lp stop to fully stop."
            )
            self.om.cancel_all_orders()
            time.sleep(60)  # Long pause before retry

            # After long pause, try to recover
            self._consecutive_api_failures = 0
            self.state = self.MONITORING

    # ------------------------------------------------------------------
    # Price crash detection & response
    # ------------------------------------------------------------------
    def _check_price_crash(self, snapshot) -> str:
        """
        Check if price has crashed since our last fill.
        Implements the user's 4-step crash strategy.

        Returns:
            "NORMAL" — no crash detected
            "TRAILING" — trailing sell active (price dropping but manageable)
            "EMERGENCY" — emergency close triggered (severe crash)
        """
        if self._fill_midpoint <= 0:
            return "NORMAL"

        current_mid = snapshot.midpoint
        drop = self._fill_midpoint - current_mid  # Positive = price fell

        # Emergency close: price dropped > emergency_close_pct from fill price
        # Example: filled at 49¢, now 41¢ = 16% drop > 15% threshold
        drop_pct = drop / self._fill_midpoint if self._fill_midpoint > 0 else 0

        if drop_pct >= self.emergency_close_pct:
            self._emergency_close(current_mid, drop, drop_pct)
            return "EMERGENCY"

        # Crash zone: price dropped > crash_threshold (e.g. 8¢)
        # Activate trailing sell — faster rebalance, follow price down
        if drop >= self.crash_threshold:
            if not self._trailing_active:
                self._trailing_active = True
                self.notify(
                    f"📉 <b>Price Crash Detected!</b>\n"
                    f"Fill price: ${self._fill_midpoint:.4f}\n"
                    f"Current: ${current_mid:.4f} (↓{drop*100:.1f}¢)\n"
                    f"\n🔄 <b>Trailing sell activated</b>\n"
                    f"Following price down with SELL orders.\n"
                    f"Will emergency close if drop > {self.emergency_close_pct*100:.0f}%."
                )
                logger.warning(f"Price crash: {self._fill_midpoint:.4f} → {current_mid:.4f}")
            return "TRAILING"

        # Price recovered or never crashed — deactivate trailing
        if self._trailing_active and drop < self.crash_threshold * 0.5:
            self._trailing_active = False
            self.notify(
                f"✅ <b>Price Stabilized</b>\n"
                f"Current: ${current_mid:.4f} — crash mode deactivated.\n"
                f"Returning to normal LP strategy."
            )
            logger.info("Price stabilized — trailing mode deactivated")

        return "NORMAL"

    def _emergency_close(self, current_mid: float, drop: float, drop_pct: float):
        """
        Step 3 from user's strategy: Price falling super fast.
        Cancel all orders, sell position at/near market price to stop losses.
        Accept small loss to prevent bigger loss.
        """
        self.state = self.EMERGENCY_CLOSE
        pos = self.om.get_position()

        self.notify(
            f"🚨 <b>EMERGENCY CLOSE — Severe Drop!</b>\n"
            f"Price dropped {drop_pct*100:.1f}% since fill\n"
            f"Fill: ${self._fill_midpoint:.4f} → Now: ${current_mid:.4f}\n"
            f"(↓{drop*100:.1f}¢)\n"
            f"\n🛑 Cancelling all orders...\n"
            f"📤 Selling position at near-market price to cut losses."
        )

        # Step 1: Cancel everything
        self.om.cancel_all_orders()

        # Step 2: Try to sell accumulated YES shares at near-market price
        # (accept 1-2¢ below market to ensure fill)
        if pos.yes_shares > 0:
            close_price = max(0.01, current_mid - 0.02)  # 2¢ below market
            token_id = self.om.state.yes_token_id

            order_id = self.om.place_order(
                token_id=token_id,
                side="SELL",
                price=close_price,
                size=round(pos.yes_shares, 2),
            )
            if order_id:
                logger.info(
                    f"Emergency SELL placed: {pos.yes_shares:.0f} YES "
                    f"@ ${close_price:.4f}"
                )

        # Step 3: Notify and stop the session
        self.notify(
            f"✅ <b>Emergency close complete</b>\n"
            f"Accept small loss now to avoid bigger loss.\n"
            f"Rewards still credited for today's fills.\n"
            f"\n💡 Use /lp start when market calms down."
        )

        self._running = False
        self._trailing_active = False
        self._fill_midpoint = 0
        self.om.end_session()
        self.state = self.IDLE
        logger.info("Emergency close complete")

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------
    def _place_initial_order(self):
        """Place the first order to start farming — BUY side, far back."""
        snapshot = self._safe_read_book()
        if snapshot is None:
            self.state = self.MONITORING
            return

        self._last_midpoint = snapshot.midpoint

        # Calculate shares from order_size / price
        price = snapshot.recommended_buy_price
        if price <= 0:
            price = snapshot.midpoint - 0.01
        size = self.order_size / price if price > 0 else 0

        if size <= 0:
            logger.warning("Invalid order size calculation")
            return

        token_id = self.om.state.yes_token_id
        order_id = self.om.place_order(
            token_id=token_id,
            side="BUY",
            price=price,
            size=round(size, 2),
        )

        if order_id:
            self._current_order_id = order_id
            self._current_token_id = token_id
            self._current_side = "BUY"
            self.state = self.MONITORING
            self.om.state.last_midpoint = snapshot.midpoint
            self.om.save_state()

            self.notify(
                f"📝 <b>Order Placed</b>\n"
                f"BUY {size:.0f} shares @ ${price:.4f}\n"
                f"Midpoint: ${snapshot.midpoint:.4f} | "
                f"Spread: {snapshot.spread*100:.1f}¢\n"
                f"Position: far back (2nd-3rd level)"
            )
        else:
            logger.warning("Initial order rejected by risk guards")
            self.state = self.MONITORING

    def _check_and_rebalance(self, snapshot):
        """
        Read order book and adjust order if midpoint moved significantly.
        During trailing mode: follows price down with smaller steps.
        """
        midpoint_delta = abs(snapshot.midpoint - self._last_midpoint)

        # In trailing mode, use smaller threshold for faster response
        threshold = self.trail_step if self._trailing_active else self.midpoint_threshold

        if midpoint_delta >= threshold:
            logger.info(
                f"Midpoint moved: ${self._last_midpoint:.4f} → "
                f"${snapshot.midpoint:.4f} (Δ {midpoint_delta:.4f})"
                f"{' [TRAILING]' if self._trailing_active else ''}"
            )

            # Cancel current order
            if self._current_order_id:
                self.om.cancel_order(self._current_order_id)

            # Place new order at adjusted price
            if self._current_side == "BUY":
                new_price = snapshot.recommended_buy_price
            else:
                new_price = snapshot.recommended_sell_price

            if new_price <= 0:
                return

            token_id = self._current_token_id or self.om.state.yes_token_id
            size = self.order_size / new_price if new_price > 0 else 0
            if size <= 0:
                return

            order_id = self.om.place_order(
                token_id=token_id,
                side=self._current_side,
                price=new_price,
                size=round(size, 2),
            )

            if order_id:
                self._current_order_id = order_id
                self._last_midpoint = snapshot.midpoint
                self.om.state.last_midpoint = snapshot.midpoint
                self.om.save_state()
        else:
            logger.debug(
                f"LP heartbeat: mid=${snapshot.midpoint:.4f} "
                f"Δ={midpoint_delta:.4f} (threshold={threshold})"
            )

    def _handle_fills(self, fills: list[dict], snapshot):
        """
        Handle filled orders — MANDATORY flip to opposite side.
        This is the core LP mechanic from the user's strategy:
          BUY filled → immediately place SELL far back
          SELL filled → immediately place BUY far back
        Records fill midpoint for crash detection.
        """
        self.state = self.FLIPPING

        for fill in fills:
            fill_side = fill.get("side", "")
            fill_price = fill.get("price", 0)
            fill_amount = fill.get("fill_amount", fill.get("size", 0))

            # Record fill price for crash detection
            self._fill_midpoint = snapshot.midpoint

            new_side = "SELL" if fill_side == "BUY" else "BUY"

            # Log fill with clear next-step info
            self.notify(
                f"🔄 <b>Order Filled!</b>\n"
                f"{fill_side} {fill_amount:.0f} shares @ ${fill_price:.4f}\n"
                f"Current midpoint: ${snapshot.midpoint:.4f}\n"
                f"\n➡️ Flipping to {new_side} side (far back)\n"
                f"🛡 Crash monitor active: emergency at ↓{self.emergency_close_pct*100:.0f}%"
            )

            # Flip side
            self._current_side = new_side
            if fill_side == "BUY":
                token_id = self.om.state.yes_token_id or self._current_token_id
            else:
                token_id = self.om.state.yes_token_id or self._current_token_id

            # Place opposite-side order far back
            if self._current_side == "BUY":
                new_price = snapshot.recommended_buy_price
            else:
                new_price = snapshot.recommended_sell_price

            if new_price <= 0:
                continue

            size = self.order_size / new_price if new_price > 0 else 0
            if size <= 0:
                continue

            order_id = self.om.place_order(
                token_id=token_id,
                side=self._current_side,
                price=new_price,
                size=round(size, 2),
            )

            if order_id:
                self._current_order_id = order_id
                self._current_token_id = token_id
                self._last_midpoint = snapshot.midpoint

        self.state = self.MONITORING

    # ------------------------------------------------------------------
    # Unwind logic
    # ------------------------------------------------------------------
    def _should_unwind_time(self) -> bool:
        """Check if market resolution is within pre_exit_hours."""
        end_date = self.om.state.end_date
        if not end_date:
            return False

        try:
            if end_date.endswith("Z"):
                end_date = end_date[:-1] + "+00:00"
            end_dt = datetime.fromisoformat(end_date)
            now = datetime.now(timezone.utc)
            hours_remaining = (end_dt - now).total_seconds() / 3600

            if hours_remaining < self.pre_exit_hours:
                logger.info(
                    f"Resolution in {hours_remaining:.1f}h "
                    f"(< {self.pre_exit_hours}h threshold)"
                )
                return True
        except (ValueError, TypeError):
            pass

        return False

    def _should_unwind_loss(self) -> bool:
        """Check if session loss exceeds max_loss_per_session."""
        return self.om.state.session_pnl < -self.om.max_loss_per_session

    def _unwind(self, reason: str):
        """Cancel all orders and stop — triggered by time or loss limit."""
        self.state = self.UNWINDING
        self._running = False
        self._trailing_active = False

        cancelled = self.om.cancel_all_orders()
        pos = self.om.get_position()

        self.notify(
            f"⚠️ <b>LP Unwinding</b>\n"
            f"Reason: {reason}\n"
            f"Cancelled {cancelled} orders\n"
            f"\n"
            f"📊 <b>Final Position:</b>\n"
            f"  YES: {pos.yes_shares:.0f} shares\n"
            f"  NO: {pos.no_shares:.0f} shares\n"
            f"  Total cost: ${pos.total_cost:.2f}\n"
            f"  Fills: {pos.total_fills}\n"
            f"  Session P&L: ${self.om.state.session_pnl:.2f}\n"
            f"\n💡 Rewards still credited for today's fills."
        )

        self.om.end_session()
        self.state = self.IDLE
        logger.info(f"LP unwind complete: {reason}")
