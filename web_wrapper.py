"""
web_wrapper.py — Wraps the bot in a tiny web server for Render's free Web Service tier.
The bot runs in a background thread while a simple HTTP server keeps Render happy.
"""
import threading
import time
import os
import logging
import traceback
from http.server import HTTPServer, BaseHTTPRequestHandler
logger = logging.getLogger("arb_bot.wrapper")
# Bot status (shared between threads)
bot_status = {
    "started": time.time(),
    "cycles": 0,
    "last_scan": "Not yet",
    "opportunities_found": 0,
    "bot_alive": True,
    "last_error": "",
    "name": "Polymarket Arb Bot",
}
class StatusHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler that shows bot status."""
    def do_GET(self):
        uptime = int(time.time() - bot_status["started"])
        hours = uptime // 3600
        minutes = (uptime % 3600) // 60
        error_html = ""
        if bot_status["last_error"]:
            error_html = (
                f'<p style="color:#ff6666;">Last error: '
                f'{bot_status["last_error"][:500]}</p>'
            )
        html = f"""
        <html><body style="font-family:monospace; padding:20px; background:#1a1a2e; color:#e0e0e0;">
        <h2>🤖 {bot_status['name']}</h2>
        <p>Status: <b style="color:{'#00ff88' if bot_status['bot_alive'] else '#ff6666'};">
            {'RUNNING' if bot_status['bot_alive'] else 'ERROR — RESTARTING'}</b></p>
        <p>Uptime: {hours}h {minutes}m</p>
        <p>Scan cycles: {bot_status['cycles']}</p>
        <p>Last scan: {bot_status['last_scan']}</p>
        <p>Opportunities found: {bot_status['opportunities_found']}</p>
        {error_html}
        </body></html>
        """
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(html.encode())
    def do_HEAD(self):
        """Handle HEAD requests (used by UptimeRobot)."""
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
    def log_message(self, format, *args):
        pass  # Suppress request logs
def run_bot():
    """Run the main bot loop. Called in a background thread."""
    from datetime import datetime, timezone
    from config_loader import load_config
    from main_v2 import run_cycle, setup_logging
    from telegram_bot import TelegramBotHandler
    from telegram_alerts_v2 import (
        send_startup_message,
        send_no_opportunities_message,
    )
    
    cfg = load_config("config.yaml")
    
    # Update status name
    bot_status["name"] = cfg["telegram"].get("bot_name", "PocketMoney")
    # Add defaults
    cfg.setdefault("cross_platform", {"min_profit_pct": 1.0, "similarity_threshold": 0.60})
    cfg.setdefault("bonds", {"min_price": 0.93, "min_roi_pct": 0.5})
    cfg.setdefault("mispricing", {"max_sum": 0.98})
    cfg.setdefault("whales", {
        "enabled": True,
        "min_trade_size": 1000,
        "convergence_count": 3,
        "convergence_window_min": 60,
        "lookback_minutes": 120,
    })
    cfg.setdefault("new_markets", {
        "enabled": True,
        "cache_file": "known_markets.json",
    })
    
    setup_logging(cfg)
    log = logging.getLogger("arb_bot.main")
    
    log.info("=" * 60)
    log.info("  Multi-Platform Arb Bot v2.0 (Web Service Mode)")
    log.info(f"  Platforms: Polymarket + Kalshi")
    log.info("=" * 60)
    
    if cfg["telegram"]["enabled"]:
        try:
            send_startup_message(cfg)
        except Exception as e:
            log.error(f"Startup message error: {e}", exc_info=True)
            
    # Start interactive bot handler
    bot_handler = TelegramBotHandler(cfg)
    
    # Initialize Execution Engine for trading
    try:
        from execution_engine import ExecutionEngine
        bot_handler.execution_engine = ExecutionEngine(cfg)
        log.info("Execution Engine initialized")
    except ImportError:
        bot_handler.execution_engine = None
        log.warning("ExecutionEngine not imported! ImportError occurred silently.")
        if cfg["telegram"]["enabled"]:
            from telegram_alerts_v2 import send_telegram_message
            send_telegram_message(f"🚨 <b>ExecutionEngine not imported!</b> ImportError occurred silently.", cfg)
    except Exception as e:
        bot_handler.execution_engine = None
        import traceback
        err = traceback.format_exc()
        log.warning(f"Execution Engine init error: {err}")
        if cfg["telegram"]["enabled"]:
            from telegram_alerts_v2 import send_telegram_message
            send_telegram_message(f"🚨 <b>Execution Engine Init Failed</b>\n<pre>{str(err)[:500]}</pre>", cfg)

    # Initialize Bond Spreader
    if cfg.get("bond_spreader", {}).get("enabled", False):
        try:
            from bond_spreader import BondSpreader
            bot_handler._bond_spreader = BondSpreader(cfg, bot_handler.execution_engine)
            log.info("Bond Spreader initialized")
        except Exception as e:
            bot_handler._bond_spreader = None
            log.error(f"Bond Spreader init error: {e}")

    # Initialize Weather Arbitrage
    if cfg.get("weather_arb", {}).get("enabled", False):
        try:
            from weather_arb.trader import WeatherArbitrage
            bot_handler._weather_arb = WeatherArbitrage(cfg, bot_handler.execution_engine, getattr(bot_handler, 'pnl_tracker', None))
            log.info("Weather Arbitrage initialized")
        except Exception as e:
            bot_handler._weather_arb = None
            log.error(f"Weather Arb init error: {e}")

    # Start Speed Listener (10s fast polling)
    if cfg.get("speed_listener", {}).get("enabled", True):
        try:
            from speed_listener import SpeedListener
            speed = SpeedListener(cfg)
            speed.start()
            bot_handler.speed_listener = speed
        except Exception as e:
            log.warning(f"Speed Listener init error: {e}")

    bot_handler.start_polling()
    log.info("Interactive signal selector active")
    
    cycle = 0
    interval = cfg["scanner"]["interval_seconds"]
    
    while True:
        cycle += 1
        bot_status["cycles"] = cycle
        bot_status["last_scan"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
        bot_status["bot_alive"] = True
        
        try:
            # RUN THE REAL CYCLE FROM MAIN_V2 WHICH INCLUDES BOND SPREADER
            opportunities = run_cycle(cfg, cycle, bot_handler)
            bot_status["opportunities_found"] += len(opportunities)
            
            if opportunities:
                log.info(f"🚨 {len(opportunities)} opportunities found — distributing!")
                bot_handler.distribute_signals(opportunities, cfg)
            else:
                send_no_opportunities_message(cycle, cfg)
                
        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)
            bot_status["last_error"] = f"Cycle: {str(e)[:200]}"
            
        time.sleep(interval)
def _run_bot_safe():
    """
    Wrapper that catches ANY crash in run_bot() and keeps retrying.
    Without this, a daemon thread crash = silent death, bot stops
    responding to commands forever while the web server stays up.
    """
    while True:
        try:
            run_bot()
        except Exception as e:
            bot_status["bot_alive"] = False
            bot_status["last_error"] = f"CRASH: {str(e)[:300]}"
            logger.critical(
                f"Bot thread crashed! Restarting in 30s...\n"
                f"{traceback.format_exc()}"
            )
            time.sleep(30)  # Wait before restart
def main():
    # Setup basic logging for the wrapper itself
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Start bot in background thread (with crash protection)
    bot_thread = threading.Thread(target=_run_bot_safe, daemon=True)
    bot_thread.start()
    print("🤖 Bot thread started")
    # Start web server in main thread
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), StatusHandler)
    print(f"🌐 Web server running on port {port}")
    server.serve_forever()
if __name__ == "__main__":
    main()
