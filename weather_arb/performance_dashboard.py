"""
weather_arb/performance_dashboard.py
Generates Markdown performance reports for Telegram based on SQLite history.
"""
import aiosqlite
import os
import matplotlib
matplotlib.use('Agg') # Headless backend
import matplotlib.pyplot as plt
from datetime import datetime
from weather_arb.db import WEATHER_DB_PATH, init_db
from weather_arb.config import TradingMode

async def get_dashboard(period="daily") -> tuple[str, str | None]:
    """
    Generate a Markdown string summarizing performance and 
    render an equity curve chart if trades exist.
    Returns: (markdown_text, path_to_png)
    """
    await init_db()
    total_trades = 0
    total_pnl = 0.0
    total_deployed = 0.0
    wins = 0
    
    # Charting data
    dates = []
    equity_curve = [0.0] # start at 0
    
    try:
        async with aiosqlite.connect(WEATHER_DB_PATH) as db:
            # Aggregate stats
            async with db.execute("SELECT COUNT(*), SUM(pnl_usdc), SUM(size_usdc) FROM trades WHERE resolved=1") as cursor:
                row = await cursor.fetchone()
                total_trades = row[0] or 0
                total_pnl = row[1] or 0.0
                total_deployed = row[2] or 0.0
                
            async with db.execute("SELECT COUNT(*) FROM trades WHERE resolved=1 AND pnl_usdc > 0") as cursor:
                wins = (await cursor.fetchone())[0] or 0
                
            # Chronological PnL for charting
            async with db.execute("SELECT timestamp, pnl_usdc FROM trades WHERE resolved=1 ORDER BY timestamp ASC") as cursor:
                rows = await cursor.fetchall()
                current_eq = 0.0
                for r in rows:
                    current_eq += float(r[1])
                    equity_curve.append(current_eq)
                    # Convert float timestamp to short date string (e.g. "Feb 24")
                    dt = datetime.fromtimestamp(r[0])
                    dates.append(dt.strftime("%m-%d"))
                    
    except Exception as e:
        return f"Error loading dashboard: {e}", None
        
    roi = (total_pnl / total_deployed * 100) if total_deployed > 0 else 0.0
    win_rate = (wins / total_trades * 100) if total_trades else 0.0
    
    # Build chart if we have data
    img_path = None
    if len(equity_curve) > 1:
        date_labels = ["Start"] + dates
        plt.figure(figsize=(8, 4))
        plt.style.use('dark_background')
        
        # Color line green if positive, red if negative overall
        line_color = '#00ff88' if equity_curve[-1] >= 0 else '#ff4444'
        plt.plot(date_labels, equity_curve, color=line_color, marker='o', linewidth=2, markersize=4)
        
        plt.fill_between(date_labels, equity_curve, 0, alpha=0.1, color=line_color)
        plt.axhline(0, color='grey', linestyle='--', linewidth=1)
        
        plt.title('Weather Arbitrage Equity Curve (USDC)', fontsize=12, pad=10)
        plt.ylabel('Cumulative Profit ($)', fontsize=10)
        plt.grid(True, alpha=0.2)
        
        # Simplify x-axis if too many trades
        if len(date_labels) > 8:
            plt.xticks(range(0, len(date_labels), max(1, len(date_labels)//8)))
        else:
            plt.xticks(rotation=45)
            
        plt.tight_layout()
        img_path = "weather_perf.png"
        plt.savefig(img_path, facecolor='#121212', edgecolor='none')
        plt.close()

    # Format nicely
    md = f"""
📊 **Weather Arbitrage Dashboard**
━━━━━━━━━━━━━━━━━━━━━━
**Deployed:** ${total_deployed:.2f}
**Net P&L:** ${total_pnl:+.2f} 
**ROI:** {roi:.1f}%

**Win Rate:** {wins}/{total_trades} ({win_rate:.1f}%)
    """
    return md, img_path
