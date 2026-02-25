"""
weather_arb/bias_trainer.py
Calculates and updates station bias based on resolved market outcomes.
"""
import logging
import json
from weather_arb.db import update_station_bias

logger = logging.getLogger("arb_bot.weather.bias")

async def train_biases_from_resolution(city: str, station: str, actual_high: float, model_forecasts: dict):
    """
    Given an actual resolved high (e.g. from NWS records or Polymarket resolution) 
    and the forecasts that were saved for that day, update the EWMA biases.
    """
    for model, forecast_temp in model_forecasts.items():
        if forecast_temp is None:
            continue
        
        # Error = Actual - Forecast
        # If forecast=34 and actual=36, error is +2.0
        # If forecast=34 and actual=32, error is -2.0
        error = actual_high - forecast_temp
        
        try:
            await update_station_bias(city, station, model, error)
            logger.info(f"Updated bias for {city}/{station} ({model}): error={error:+.1f}")
        except Exception as e:
            logger.error(f"Failed to update bias for {city} {model}: {e}")
