"""
weather_arb/performance_dashboard.py
Generates Markdown performance reports for Telegram based on SQLite history.
"""
import aiosqlite
from weather_arb.db import WEATHER_DB_PATH
from weather_arb.config import TradingMode

async def get_dashboard(period="daily") -> str:
    """Generate a Markdown string summarizing performance."""
    try:
        async with aiosqlite.connect(WEATHER_DB_PATH) as db:
            # Aggregate stats
            async with db.execute("SELECT COUNT(*), SUM(pnl_usdc), SUM(size_usdc) FROM trades WHERE resolved=1") as cursor:
                row = await cursor.fetchone()
                total_trades = row[0] or 0
                total_pnl = row[1] or 0.0
                total_deployed = row[2] or 0.0
                
            roi = (total_pnl / total_deployed * 100) if total_deployed > 0 else 0.0
            
            async with db.execute("SELECT COUNT(*) FROM trades WHERE resolved=1 AND pnl_usdc > 0") as cursor:
                wins = (await cursor.fetchone())[0] or 0
                
    except Exception as e:
        return f"Error loading dashboard: {e}"
        
    win_rate = (wins / total_trades * 100) if total_trades else 0.0
        
    # Format nicely
    return f"""
📊 **Weather Arbitrage Dashboard**
━━━━━━━━━━━━━━━━━━━━━━
**Deployed:** ${total_deployed:.2f}
**Net P&L:** ${total_pnl:+.2f} 
**ROI:** {roi:.1f}%

**Win Rate:** {wins}/{total_trades} ({win_rate:.1f}%)
    """
