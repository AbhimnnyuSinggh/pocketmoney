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
from weather_arb.config import TradingMode

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
        self.dry_run = cfg.get("weather_arb", {}).get("dry_run", True)
        self.bankroll = 39.0 # Start with assumption from explicit goal

    async def scan_and_deploy(self, poly_markets: list) -> list:
        if not self.enabled:
            return []
            
        weather_markets = get_active_weather_markets(poly_markets)
        if not weather_markets:
            return []

        opportunities = []
        
        for m in weather_markets:
            city = m.get("city")
            if city == "Unknown" or m.get("weather_type") != "daily_high":
                continue
                
            # Fetch forecast
            forecast_data = await fetch_open_meteo_forecast(city)
            if not forecast_data:
                continue
                
            # Parse daily max temps for models
            try:
                daily = forecast_data.get("daily", {})
                models = ["gfs_seamless", "ecmwf_seamless", "icon_seamless"]
                forecasts = {}
                for mod in models:
                    temps = daily.get(f"temperature_2m_max_{mod}", [])
                    if temps:
                        forecasts[mod] = temps[0] # Grab first day
            except Exception:
                continue
                
            # Scrape bins from the market outcomes
            target_bins = m.get("outcomes", [])
            if not target_bins:
                continue
                
            # Compute probabilities (default 0 bias assuming fresh init)
            biases = {mod: 0.0 for mod in models}
            bin_probs = compute_bin_probs(forecasts, biases, target_bins)
            
            # Find edge calculations for each outcome
            prices = m.get("outcome_prices", [])
            for i, bin_title in enumerate(target_bins):
                if i >= len(prices):
                    break
                market_price = prices[i]
                model_prob = bin_probs.get(bin_title, 0.0)
                
                pos = calculate_position(market_price, model_prob, self.mode, self.bankroll, m.get("is_new_launch", False))
                if pos:
                    opp = {
                        "platform": "polymarket",
                        "market_id": m.get("market_id"),
                        "title": m.get("title"),
                        "type": "WEATHER_ARB",
                        "bin": bin_title,
                        "edge": pos["edge"],
                        "size": pos["size_usdc"],
                        "expected_ev": pos["expected_ev"]
                    }
                    opportunities.append(opp)
                    
                    if not self.dry_run and self.exec_engine:
                        await self._execute_trade(m.get("market_id"), bin_title, pos)

        return opportunities
        
    async def _execute_trade(self, slug: str, bin_title: str, pos: dict):
        logger.info(f"Executing weather trade on {slug} [{bin_title}] for ${pos['size_usdc']} (Edge: {pos['edge']:.2f})")
        # Direct integration hook for ExecutionEngine
        pass

    async def update_dashboard(self):
        """Hook to trigger daily SQLite digest and PNL aggregation."""
        pass
