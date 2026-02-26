"""
weather_arb/trader.py
Core weather trading engine — groups sub-markets, applies consensus scoring,
executes via CLOB, tracks capital, and manages the full trade lifecycle.
"""
import re
import json
import time
import logging
import asyncio
from datetime import datetime, date, timezone
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List

from weather_arb.db import log_trade
from weather_arb.scanner import get_active_weather_markets, group_weather_markets_by_event
from weather_arb.data_fetcher import fetch_open_meteo_forecast, fetch_nws_observation
from weather_arb.edge_calculator import calculate_position
from weather_arb.config import TradingMode, CITY_STATIONS
from weather_arb.consensus_scorer import compute_bin_probs, construct_bins, parse_polymarket_bin

logger = logging.getLogger("arb_bot.weather.trader")


# ---------------------------------------------------------------------------
# WeatherSession — Capital Tracking & Compounding
# ---------------------------------------------------------------------------
@dataclass
class WeatherSession:
    initial_capital: float = 39.0
    available_capital: float = 39.0
    total_deployed: float = 0.0
    total_returned: float = 0.0
    total_profits: float = 0.0
    total_losses: float = 0.0
    trades_won: int = 0
    trades_lost: int = 0
    reinvest_rate: float = 0.80  # 80% of profits go back to pool
    withdrawn: float = 0.0
    active_positions: list = field(default_factory=list)

    def deploy(self, amount: float):
        self.available_capital -= amount
        self.total_deployed += amount

    def resolve_win(self, stake: float, payout: float):
        profit = payout - stake
        reinvest = profit * self.reinvest_rate
        self.available_capital += stake + reinvest
        self.withdrawn += profit - reinvest
        self.total_profits += profit
        self.total_deployed -= stake
        self.total_returned += payout
        self.trades_won += 1

    def resolve_loss(self, stake: float):
        self.total_losses += stake
        self.total_deployed -= stake
        self.trades_lost += 1

    @property
    def win_rate(self) -> float:
        total = self.trades_won + self.trades_lost
        return (self.trades_won / total * 100) if total > 0 else 0.0

    @property
    def net_pnl(self) -> float:
        return self.total_profits - self.total_losses

    @property
    def current_bankroll(self) -> float:
        return self.available_capital + self.total_deployed

    @property
    def phase(self) -> str:
        br = self.current_bankroll
        if br < 80:
            return "Phase 1: Micro ($39→$80)"
        elif br < 200:
            return "Phase 2: Ladder ($80→$200)"
        elif br < 500:
            return "Phase 3: Scaling ($200→$500)"
        else:
            return "Phase 4: Cruise ($500+)"

    def save(self, path="weather_session.json"):
        try:
            data = asdict(self)
            with open(path, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save weather session: {e}")

    @classmethod
    def load(cls, path="weather_session.json"):
        try:
            with open(path) as f:
                data = json.load(f)
            s = cls()
            for k, v in data.items():
                if hasattr(s, k):
                    setattr(s, k, v)
            return s
        except (FileNotFoundError, json.JSONDecodeError):
            return cls()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _bin_sort_key(bin_label: str) -> float:
    """Sort bins numerically. '32-33' → 32.0, '50+' → 50.0, '20-' → 20.0"""
    nums = re.findall(r'\d+', bin_label)
    return float(nums[0]) if nums else 0.0


# ---------------------------------------------------------------------------
# Main Trader Class
# ---------------------------------------------------------------------------
class WeatherArbitrage:
    def __init__(self, cfg: dict, exec_engine, pnl_tracker):
        self.cfg = cfg
        self.exec_engine = exec_engine
        self.pnl_tracker = pnl_tracker
        self.mode_str = cfg.get("weather_arb", {}).get("mode", "SAFE")
        try:
            self.mode = TradingMode[self.mode_str]
        except KeyError:
            self.mode = TradingMode.SAFE

        self.enabled = cfg.get("weather_arb", {}).get("enabled", False)

        # Inherit execution mode globally from /wallet live
        global_mode = cfg.get("execution", {}).get("mode", "dry_run")
        self.dry_run = (global_mode == "dry_run")

        # Capital — load persistent session
        self.session = WeatherSession.load()
        total_usdc = cfg.get("bankroll", {}).get("total_usdc", 39.0)
        allocs = cfg.get("bankroll", {}).get("allocations", {})
        active = cfg.get("execution", {}).get("active_autotrader", "none")

        if active == "weather":
            # Active autotrader gets 100% bankroll
            if self.session.initial_capital != total_usdc:
                self.session.initial_capital = total_usdc
                self.session.available_capital = total_usdc
                self.session.save()
            logger.info(f"🌤 Weather is ACTIVE → {self.session.phase}, ${self.session.available_capital:.2f} available")
        else:
            alloc = allocs.get("weather_arb", 15.0)
            if self.session.initial_capital != alloc:
                self.session.initial_capital = alloc
                self.session.available_capital = alloc
                self.session.save()

        # Trade deduplication
        self._traded_today: set = set()
        self._last_reset_date: date | None = None

    # ------------------------------------------------------------------
    # Core Trading Loop
    # ------------------------------------------------------------------
    async def scan_and_deploy(self, poly_markets: list) -> list:
        if not self.enabled:
            return []

        # Daily reset for dedup
        today = date.today()
        if self._last_reset_date != today:
            self._traded_today.clear()
            self._last_reset_date = today

        # Drawdown circuit breakers
        if self.session.current_bankroll < 20.0:
            logger.warning(f"CIRCUIT BREAKER: Bankroll ${self.session.current_bankroll:.2f} < $20. Observation only.")
            return []
        if self.session.net_pnl < -(self.session.initial_capital * 0.50):
            logger.warning(f"CIRCUIT BREAKER: Drawdown {self.session.net_pnl:.2f} exceeds 50%. Halted.")
            return []

        # Initialize DB
        if not getattr(self, "db_initialized", False):
            from weather_arb.db import init_db
            await init_db()
            self.db_initialized = True

        # Scan and group
        weather_markets = get_active_weather_markets(poly_markets)
        if not weather_markets:
            return []

        events = group_weather_markets_by_event(weather_markets)
        if not events:
            return []

        opportunities = []

        for event_key, event in events.items():
            city = event["city"]
            if city == "Unknown":
                continue

            bins = event["bins"]
            if len(bins) < 3:  # Need at least 3 bins for meaningful market
                continue

            # Fetch model forecasts
            forecasts = await fetch_open_meteo_forecast(city)
            if not forecasts:
                continue

            # Build sorted bin list and price map
            bin_labels = sorted(bins.keys(), key=_bin_sort_key)
            bin_prices = {label: bins[label]["yes_price"] for label in bin_labels}

            # Compute model probabilities for each bin
            biases = {mod: 0.0 for mod in forecasts}
            bin_probs = compute_bin_probs(forecasts, biases, bin_labels)

            logger.info(
                f"Weather {city}: {len(bin_labels)} bins, "
                f"models={list(forecasts.values())}, "
                f"top_prob={max(bin_probs.values()):.0%} on {max(bin_probs, key=bin_probs.get)}"
            )

            # === PRIMARY EDGE DETECTION ===
            for bin_label in bin_labels:
                market_price = bin_prices[bin_label]
                model_prob = bin_probs.get(bin_label, 0.0)

                # Skip if already traded today
                trade_key = f"{event_key}:{bin_label}"
                if trade_key in self._traded_today:
                    continue

                pos = calculate_position(
                    market_price, model_prob, self.mode,
                    self.session.available_capital,
                    is_new_launch=event.get("is_new_launch", False)
                )

                if pos:
                    bin_info = bins[bin_label]
                    opp = {
                        "platform": "polymarket",
                        "market_id": bin_info["market_id"],
                        "slug": bin_info["slug"],
                        "title": f"🌤 Weather: {city} {bin_label}°F",
                        "type": "WEATHER_ARB",
                        "bin": bin_label,
                        "edge": pos["edge"],
                        "size": pos["size_usdc"],
                        "expected_ev": pos.get("expected_ev", 0),
                    }
                    opportunities.append(opp)

                    if not self.dry_run and self.exec_engine:
                        pos["token_id"] = bin_info["token_id"]
                        pos["price"] = market_price
                        success = await self._execute_trade(bin_info["slug"], bin_label, pos)
                        if success:
                            self._traded_today.add(trade_key)
                            self.session.deploy(pos["size_usdc"])
                            self.session.active_positions.append({
                                "slug": bin_info["slug"],
                                "bin": bin_label,
                                "stake": pos["size_usdc"],
                                "shares": round(pos["size_usdc"] / pos["price"], 4),
                                "price": pos["price"],
                                "token_id": bin_info["token_id"],
                                "placed_at": time.time(),
                                "event_key": event_key,
                            })
                            self.session.save()

            # === LADDER STRATEGY ===
            if self.mode in (TradingMode.NEUTRAL, TradingMode.AGGRESSIVE):
                best_bin = max(bin_probs, key=bin_probs.get)
                best_prob = bin_probs[best_bin]

                if best_prob >= 0.50:
                    sorted_bins = sorted(bins.keys(), key=_bin_sort_key)
                    best_idx = sorted_bins.index(best_bin) if best_bin in sorted_bins else -1

                    if best_idx >= 0:
                        adjacent = []
                        if best_idx > 0:
                            adjacent.append(sorted_bins[best_idx - 1])
                        if best_idx < len(sorted_bins) - 1:
                            adjacent.append(sorted_bins[best_idx + 1])

                        for adj_bin in adjacent:
                            adj_price = bin_prices.get(adj_bin, 0)
                            adj_prob = bin_probs.get(adj_bin, 0)
                            trade_key = f"{event_key}:{adj_bin}"

                            if trade_key in self._traded_today:
                                continue

                            adj_edge = adj_prob - adj_price
                            if adj_edge > 0.05:
                                adj_amount = min(
                                    self.session.available_capital * 0.04,
                                    2.00
                                )
                                if adj_amount >= 0.25:
                                    adj_info = bins[adj_bin]
                                    opp = {
                                        "platform": "polymarket",
                                        "slug": adj_info["slug"],
                                        "title": f"🔀 Ladder: {city} {adj_bin}°F",
                                        "type": "WEATHER_LADDER",
                                        "bin": adj_bin,
                                        "edge": adj_edge,
                                        "size": round(adj_amount, 2),
                                    }
                                    opportunities.append(opp)

                                    if not self.dry_run and self.exec_engine:
                                        pos_adj = {
                                            "token_id": adj_info["token_id"],
                                            "price": adj_price,
                                            "size_usdc": round(adj_amount, 2),
                                            "edge": adj_edge,
                                            "mode_used": self.mode.name,
                                        }
                                        success = await self._execute_trade(adj_info["slug"], adj_bin, pos_adj)
                                        if success:
                                            self._traded_today.add(trade_key)
                                            self.session.deploy(adj_amount)
                                            self.session.save()

            # === AFTERNOON OBSERVATION EDGE ===
            try:
                import pytz
                et_tz = pytz.timezone("US/Eastern")
                now_et = datetime.now(et_tz)

                if 12 <= now_et.hour <= 18 and city in CITY_STATIONS:
                    station = CITY_STATIONS[city]
                    current_temp = await fetch_nws_observation(station)

                    if current_temp is not None:
                        logger.info(f"Afternoon obs: {city}/{station} current={current_temp}°F at {now_et.strftime('%H:%M')} ET")

                        for bin_label, bin_info in bins.items():
                            trade_key = f"{event_key}:{bin_label}:obs"
                            if trade_key in self._traded_today:
                                continue

                            bounds = parse_polymarket_bin(bin_label)
                            if not bounds:
                                continue

                            low, high = bounds
                            market_price = bin_info["yes_price"]

                            if current_temp >= low - 0.5 and market_price < 0.60:
                                hour_factor = min(1.0, (now_et.hour - 11) / 7)
                                obs_prob = 0.70 + (0.25 * hour_factor)

                                obs_pos = calculate_position(
                                    market_price, obs_prob, self.mode,
                                    self.session.available_capital,
                                    is_new_launch=False
                                )
                                if obs_pos:
                                    opp = {
                                        "platform": "polymarket",
                                        "slug": bin_info["slug"],
                                        "title": f"🌡 OBS: {city} {bin_label}°F (now: {current_temp}°F)",
                                        "type": "WEATHER_OBS",
                                        "bin": bin_label,
                                        "edge": obs_pos["edge"],
                                        "size": obs_pos["size_usdc"],
                                    }
                                    opportunities.append(opp)

                                    if not self.dry_run and self.exec_engine:
                                        obs_pos["token_id"] = bin_info["token_id"]
                                        obs_pos["price"] = market_price
                                        success = await self._execute_trade(bin_info["slug"], bin_label, obs_pos)
                                        if success:
                                            self._traded_today.add(trade_key)
                                            self.session.deploy(obs_pos["size_usdc"])
                                            self.session.save()

                                    logger.info(
                                        f"OBS EDGE: {city} current={current_temp}°F, "
                                        f"bin={bin_label} @ {market_price:.2f}, "
                                        f"obs_prob={obs_prob:.0%}, edge={obs_pos['edge']:.0%}"
                                    )
            except ImportError:
                pass  # pytz not installed

        return opportunities

    # ------------------------------------------------------------------
    # Resolution Detection
    # ------------------------------------------------------------------
    async def check_resolutions(self) -> list[dict]:
        """Check if any active positions resolved. Update session + free capital."""
        resolved = []
        still_active = []

        for pos in self.session.active_positions:
            try:
                # Check via Gamma API if market is closed
                import requests
                slug = pos.get("slug", "")
                if not slug:
                    still_active.append(pos)
                    continue

                resp = requests.get(
                    f"https://gamma-api.polymarket.com/markets?slug={slug}",
                    timeout=5
                )
                if resp.status_code != 200:
                    still_active.append(pos)
                    continue

                markets = resp.json()
                if not markets:
                    still_active.append(pos)
                    continue

                market = markets[0] if isinstance(markets, list) else markets

                if not market.get("closed", False):
                    still_active.append(pos)
                    continue

                # Market resolved — determine outcome
                final_price = float(market.get("outcomePrices", "[0,0]").strip("[]").split(",")[0])
                won = final_price > 0.50

                stake = pos["stake"]
                shares = pos.get("shares", stake / max(pos.get("price", 0.01), 0.01))

                if won:
                    payout = shares * 1.0
                    self.session.resolve_win(stake, payout)
                    profit = payout - stake
                    resolved.append({
                        "bin": pos["bin"], "slug": slug, "won": True,
                        "stake": round(stake, 2), "payout": round(payout, 2),
                        "profit": round(profit, 2),
                    })
                else:
                    self.session.resolve_loss(stake)
                    resolved.append({
                        "bin": pos["bin"], "slug": slug, "won": False,
                        "stake": round(stake, 2), "payout": 0,
                        "profit": round(-stake, 2),
                    })

                # Log to SQLite
                await log_trade(
                    market_slug=slug, outcome_bin=pos["bin"],
                    side="BUY", size=stake, price=pos.get("price", 0),
                    mode=self.mode.name, edge=0,
                )

            except Exception as e:
                logger.error(f"Resolution check failed for {pos.get('slug')}: {e}")
                still_active.append(pos)

        self.session.active_positions = still_active
        self.session.save()
        return resolved

    # ------------------------------------------------------------------
    # Trade Execution
    # ------------------------------------------------------------------
    async def _execute_trade(self, slug: str, bin_title: str, pos: dict) -> bool:
        logger.info(f"Executing weather trade on {slug} [{bin_title}] for ${pos['size_usdc']} (Edge: {pos['edge']:.2f})")

        try:
            admin_id = getattr(self.exec_engine, "_admin_chat_id", "")
            client = self.exec_engine._get_client(admin_id) if admin_id else None

            if not client:
                logger.error("No valid ClobClient found.")
                return False

            # Ensure API creds are set
            if not getattr(client, 'api_creds', None):
                client.set_api_creds(client.derive_api_key())

            from py_clob_client.order_builder.constants import BUY
            shares = pos['size_usdc'] / pos['price']

            order = client.create_order(
                token_id=pos['token_id'],
                price=round(pos['price'], 4),
                size=round(shares, 4),
                side=BUY,
            )

            resp = client.post_order(order)
            logger.info(f"Weather Order Placed: {resp}")

            await log_trade(
                market_slug=slug, outcome_bin=bin_title,
                side="BUY", size=pos['size_usdc'],
                price=pos['price'], mode=pos.get('mode_used', self.mode.name),
                edge=pos['edge']
            )
            return True

        except Exception as e:
            logger.error(f"Failed to execute weather trade: {e}", exc_info=True)
            return False

    async def update_dashboard(self):
        """Hook to trigger daily SQLite digest and PNL aggregation."""
        pass
