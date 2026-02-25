"""
weather_arb/edge_calculator.py
Calculates exact bet sizing using Fractional Kelly and constructs betting ladders.
"""
from weather_arb.config import TradingMode, MODE_THRESHOLDS, MODE_KELLY_MULTIPLIER

def calculate_position(market_price: float, model_prob: float, mode: TradingMode, bankroll: float, is_new_launch: bool = False) -> dict | None:
    """
    Returns a dict with the recommended bet size if the edge meets the mode threshold.
    """
    if market_price <= 0.001 or market_price >= 0.99:
        return None # Too extreme or already resolved
        
    edge = model_prob - market_price
    
    threshold = MODE_THRESHOLDS.get(mode.name, 0.25)
    
    # New launch markets are incredibly inefficient, we can lower the threshold slightly
    if is_new_launch:
        threshold *= 0.8
        
    if edge < threshold:
        return None
        
    # Kelly Criterion for discrete outcomes
    # f* = (bp - q) / b  where b = Decimal odds - 1
    # Payout is what you win purely (excluding your stake)
    payout = (1.0 / market_price) - 1.0
    q = 1.0 - model_prob
    
    kelly_fraction = (model_prob * payout - q) / payout
    if kelly_fraction <= 0:
        return None
        
    # Apply fractional safety modifier based on mode
    fractional_multiplier = MODE_KELLY_MULTIPLIER.get(mode.name, 0.25)
    adjusted_fraction = kelly_fraction * fractional_multiplier
    
    # Add a hard cap on maximum position size (10% of total bankroll)
    max_position_pct = 0.10
    final_fraction = min(adjusted_fraction, max_position_pct)
    
    bet_size = bankroll * final_fraction
    
    # Only bet if it's over $1.00 micro-bet threshold to avoid dusting
    if bet_size < 1.00:
        return None
        
    return {
        "edge": edge,
        "kelly_fraction": final_fraction,
        "size_usdc": round(bet_size, 2),
        "expected_ev": round(bet_size * edge, 2),
        "mode_used": mode.name
    }
