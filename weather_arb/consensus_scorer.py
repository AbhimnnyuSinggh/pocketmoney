"""
weather_arb/consensus_scorer.py
Groups individual model forecasts into discrete probabilistic bins.
"""
import logging
import math
import numpy as np

logger = logging.getLogger("arb_bot.weather.scorer")

def construct_bins(center_temp: float, num_bins_each_side: int = 4) -> list[tuple[float, float, str]]:
    """
    Construct 2°F Polymarket bins centered around the given temp.
    Returns list of (min_temp, max_temp, title).
    """
    center_base = math.floor(center_temp)
    if center_base % 2 != 0:
        center_base -= 1 # align to even
        
    bins = []
    # Add lower bins
    for i in range(num_bins_each_side, 0, -1):
        low = center_base - (2*i)
        high = low + 1
        bins.append((low, high, f"{low}-{high}"))
        
    # Center bin
    bins.append((center_base, center_base+1, f"{center_base}-{center_base+1}"))
    
    # Add higher bins
    for i in range(1, num_bins_each_side + 1):
        low = center_base + (2*i)
        high = low + 1
        bins.append((low, high, f"{low}-{high}"))
        
    return bins

def parse_polymarket_bin(bin_title: str) -> tuple[float, float] | None:
    """Parse '34-35' into (34.0, 35.0), '50+' into (50.0, 200.0), '20-' into (-50.0, 20.0)"""
    import re

    # Standard range: "34-35"
    m = re.search(r'(-?\d+)\s*-\s*(-?\d+)', bin_title)
    if m:
        return float(m.group(1)), float(m.group(2))

    # "50+" or "50 or higher"
    if bin_title.endswith("+") or "or higher" in bin_title.lower():
        num = re.search(r'(\d+)', bin_title)
        if num:
            return float(num.group(1)), 200.0  # Upper bound very high

    # "20-" or "20 or lower"
    if bin_title.endswith("-") or "or lower" in bin_title.lower():
        num = re.search(r'(\d+)', bin_title)
        if num:
            return -50.0, float(num.group(1))  # Lower bound very low

    return None

def compute_bin_probs(forecasts: dict[str, float], biases: dict[str, float], target_bins: list[str]) -> dict[str, float]:
    """
    Applies bias correction to each model, produces a blended distribution,
    and integrates the probability density function over the target Polymarket bins.
    """
    corrected_forecasts = []
    
    for model, temp in forecasts.items():
        if temp is None:
            continue
        # Apply standard bias offset
        bias_offset = biases.get(model, 0.0)
        corrected = temp + bias_offset
        corrected_forecasts.append(corrected)
        
    if not corrected_forecasts:
        return {b: 0.0 for b in target_bins}
        
    mean_temp = np.mean(corrected_forecasts)
    
    # Dynamic standard deviation based on model disagreement, floor at 1.5°F
    std_dev = max(1.5, np.std(corrected_forecasts))
    
    probs = {}
    for bin_title in target_bins:
        bounds = parse_polymarket_bin(bin_title)
        if not bounds:
            # Handle edge cases later ("60 or higher")
            probs[bin_title] = 0.0
            continue
            
        low, high = bounds
        # Widen bounds slightly to cover the continuous distribution space (e.g. 34-35 covers 33.5 to 35.5)
        # Polymarket resolves to whole integers, so normal distribution mapping should extend +/- 0.5
        adjusted_low = low - 0.5
        adjusted_high = high + 0.5
        
        # CDF of Normal Dist
        def cdf(v):
            return (1.0 + math.erf((v - mean_temp) / (std_dev * math.sqrt(2.0)))) / 2.0
            
        prob = cdf(adjusted_high) - cdf(adjusted_low)
        probs[bin_title] = max(0.001, prob) # Keep a floor probability
        
    # Normalize
    total = sum(probs.values())
    if total > 0:
        for k in probs:
            probs[k] /= total
            
    return probs
