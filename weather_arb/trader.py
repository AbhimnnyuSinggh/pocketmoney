"""
weather_arb/trader.py
Executes trades, logs them to the dashboard, and manages active positions.
"""
import logging
import asyncio
from typing import Dict, Any, List

from weather_arb.db import log_trade
from weather_arb.scanner import get_active_weather_markets
from weather_arb.data_fetcher import fetch_open_meteo_forecast, fetch_nws_observation
from weather_arb.consensus_scorer import compute_bin_probs, construct_bins
from weather_arb.edge_calculator import calculate_position
from weather_arb.config import TradingMode, CITY_STATIONS
from weather_arb.consensus_scorer import compute_bin_probs, construct_bins, parse_polymarket_bin

logger = logging.getLogger("arb_bot.weather.trader")

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
        
        # Smart Capital Routing: 100% bankroll if active autotrader, else fractional
        total_usdc = cfg.get("bankroll", {}).get("total_usdc", 100.0)
        allocs = cfg.get("bankroll", {}).get("allocations", {})
        active = cfg.get("execution", {}).get("active_autotrader", "none")
        if active == "weather":
            self.bankroll = total_usdc
            logger.info(f"🌤 Weather Arb is the ACTIVE module -> Routing 100% capital (${self.bankroll})")
        else:
            self.bankroll = allocs.get("weather_arb", 15.0)

    async def scan_and_deploy(self, poly_markets: list) -> list:
        if not self.enabled:
            return []
            
        if not getattr(self, "db_initialized", False):
            from weather_arb.db import init_db
            await init_db()
            self.db_initialized = True
            
        weather_markets = get_active_weather_markets(poly_markets)
        if not weather_markets:
            return []

        opportunities = []
        
        for m in weather_markets:
            city = m.get("city")
            if city == "Unknown" or m.get("weather_type") != "daily_high":
                continue
                
            # Fetch forecast — returns {model_name: temp_f}
            forecasts = await fetch_open_meteo_forecast(city)
            if not forecasts:
                continue
                
            # Scrape bins from the market outcomes
            target_bins = m.get("outcomes", [])
            if not target_bins:
                continue
                
            # Compute probabilities (default 0 bias assuming fresh init)
            biases = {mod: 0.0 for mod in forecasts}
            bin_probs = compute_bin_probs(forecasts, biases, target_bins)
            
            # Find edge calculations for each outcome
            prices = m.get("outcome_prices", [])
            token_ids = m.get("clob_token_ids", [])
            for i, bin_title in enumerate(target_bins):
                if i >= len(prices) or i >= len(token_ids):
                    break
                market_price = prices[i]
                model_prob = bin_probs.get(bin_title, 0.0)
                
                pos = calculate_position(market_price, model_prob, self.mode, self.bankroll, m.get("is_new_launch", False))
                if pos:
                    slug = m.get("slug", "")
                    opp = {
                        "platform": "polymarket",
                        "market_id": m.get("market_id"),
                        "slug": slug,
                        "title": m.get("title"),
                        "type": "WEATHER_ARB",
                        "bin": bin_title,
                        "edge": pos["edge"],
                        "size": pos["size_usdc"],
                        "expected_ev": pos["expected_ev"]
                    }
                    pos["token_id"] = token_ids[i]
                    pos["price"] = market_price
                    opportunities.append(opp)
                    
                    if not self.dry_run and self.exec_engine:
                        await self._execute_trade(slug, bin_title, pos)

            # ======= AFTERNOON OBSERVATION EDGE =======
            # Between 12:00-18:00 ET, check actual current temp at the station
            import pytz
            et_tz = pytz.timezone("US/Eastern")
            now_et = datetime.now(et_tz)

            if 12 <= now_et.hour <= 18 and city in CITY_STATIONS:
                station = CITY_STATIONS[city]
                current_temp = await fetch_nws_observation(station)

                if current_temp is not None:
                    logger.info(f"Afternoon obs: {city}/{station} current high = {current_temp}°F at {now_et.strftime('%H:%M')} ET")

                    for i, bin_title in enumerate(target_bins):
                        if i >= len(prices) or i >= len(token_ids):
                            break

                        bounds = parse_polymarket_bin(bin_title)
                        if not bounds:
                            continue

                        low, high = bounds
                        market_price = prices[i]

                        # If current temp is ALREADY in this bin or above it
                        if current_temp >= low - 0.5 and market_price < 0.60:
                            hour_factor = min(1.0, (now_et.hour - 11) / 7)
                            obs_prob = 0.70 + (0.25 * hour_factor)  # 70% at noon → 95% at 6pm

                            obs_pos = calculate_position(
                                market_price, obs_prob, self.mode, self.bankroll,
                                is_new_launch=False
                            )
                            if obs_pos:
                                slug = m.get("slug", "")
                                opp = {
                                    "platform": "polymarket",
                                    "market_id": m.get("market_id"),
                                    "slug": slug,
                                    "title": f"🌡 OBS EDGE: {m.get('title', '')}",
                                    "type": "WEATHER_OBS",
                                    "bin": bin_title,
                                    "edge": obs_pos["edge"],
                                    "size": obs_pos["size_usdc"],
                                    "expected_ev": obs_pos["expected_ev"],
                                }
                                obs_pos["token_id"] = token_ids[i]
                                obs_pos["price"] = market_price
                                opportunities.append(opp)

                                if not self.dry_run and self.exec_engine:
                                    await self._execute_trade(slug, bin_title, obs_pos)

                                logger.info(
                                    f"OBS EDGE: {city} current={current_temp}°F, "
                                    f"bin={bin_title} @ {market_price:.2f}, "
                                    f"obs_prob={obs_prob:.0%}, edge={obs_pos['edge']:.0%}"
                                )

        return opportunities
        
    async def _execute_trade(self, slug: str, bin_title: str, pos: dict):
        logger.info(f"Executing weather trade on {slug} [{bin_title}] for ${pos['size_usdc']} (Edge: {pos['edge']:.2f})")
        
        try:
            admin_id = getattr(self.exec_engine, "_admin_chat_id", "")
            client = self.exec_engine._get_client(admin_id) if admin_id else None
            
            if not client:
                logger.error("No valid ClobClient found. Ensure WALLET_ADDRESS and POLY_PRIVATE_KEY are set.")
                return

            # Ensure API creds are set (in case client was cached without them)
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
            
            logger.info(f"Weather Arb Order Placed Successfully: {resp}")
            
            # Log the trade to SQLite
            await log_trade(
                market_slug=slug,
                outcome_bin=bin_title,
                side="BUY",
                size=pos['size_usdc'],
                price=pos['price'],
                mode=pos['mode_used'],
                edge=pos['edge']
            )

        except Exception as e:
            logger.error(f"Failed to execute weather trade: {e}", exc_info=True)

    async def update_dashboard(self):
        """Hook to trigger daily SQLite digest and PNL aggregation."""
        pass
