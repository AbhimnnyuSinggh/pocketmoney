"""
web_wrapper.py — Wraps the bot in a tiny web server for Render's free Web Service tier.
The bot runs in a background thread while a simple HTTP server keeps Render happy.
"""
import threading
import time
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
# Bot status (shared between threads)
bot_status = {
    "started": time.time(),
    "cycles": 0,
    "last_scan": "Not yet",
    "opportunities_found": 0,
}
class StatusHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler that shows bot status."""
    def do_GET(self):
        uptime = int(time.time() - bot_status["started"])
        hours = uptime // 3600
        minutes = (uptime % 3600) // 60
        html = f"""
        <html><body style="font-family:monospace; padding:20px; background:#1a1a2e; color:#e0e0e0;">
        <h2>🤖 Polymarket Arb Bot v2.0</h2>
        <p>Status: <b style="color:#00ff88;">RUNNING</b></p>
        <p>Uptime: {hours}h {minutes}m</p>
        <p>Scan cycles: {bot_status['cycles']}</p>
        <p>Last scan: {bot_status['last_scan']}</p>
        <p>Opportunities found: {bot_status['opportunities_found']}</p>
        </body></html>
        """
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(html.encode())
    def log_message(self, format, *args):
        pass  # Suppress request logs
def run_bot():
    """Run the main bot loop in a background thread."""
    import logging
    import signal
    from datetime import datetime, timezone
    from config_loader import load_config
    from cross_platform_scanner import run_full_cross_platform_scan
    from whale_tracker import find_whale_opportunities
    from new_market_sniper import find_new_market_opportunities
    from telegram_bot import TelegramBotHandler
    from telegram_alerts_v2 import (
        send_opportunities_batch,
        send_startup_message,
        send_no_opportunities_message,
        send_telegram_message,
    )
    cfg = load_config("config.yaml")
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
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("arb_bot.main")
    logger.info("=" * 60)
    logger.info("  Multi-Platform Arb Bot v2.0 (Web Service Mode)")
    logger.info(f"  Platforms: Polymarket + Kalshi")
    logger.info("=" * 60)
    if cfg["telegram"]["enabled"]:
        send_startup_message(cfg)
    # Start interactive bot handler
    bot_handler = TelegramBotHandler(cfg)
    bot_handler.start_polling()
    logger.info("Interactive signal selector active")
    # Deduplication
    seen = {}
    COOLDOWN = 600
    cycle = 0
    interval = cfg["scanner"]["interval_seconds"]
    while True:
        cycle += 1
        bot_status["cycles"] = cycle
        bot_status["last_scan"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        logger.info(f"Scan cycle #{cycle}")
        try:
            opportunities = run_full_cross_platform_scan(cfg)
            # Whale convergence
            try:
                whale_opps = find_whale_opportunities(cfg)
                opportunities.extend(whale_opps)
            except Exception as e:
                logger.error(f"Whale tracker error: {e}", exc_info=True)
            # New market sniper
            try:
                new_market_opps = find_new_market_opportunities(cfg)
                opportunities.extend(new_market_opps)
            except Exception as e:
                logger.error(f"New market sniper error: {e}", exc_info=True)
            # Filter already-seen
            now = time.time()
            new_opps = []
            for opp in opportunities:
                key = f"{opp.opp_type}:{opp.title[:50]}"
                if now - seen.get(key, 0) > COOLDOWN:
                    seen[key] = now
                    new_opps.append(opp)
            bot_status["opportunities_found"] += len(new_opps)
            if new_opps:
                logger.info(f"🚨 {len(new_opps)} NEW opportunities!")
                bot_handler.distribute_signals(new_opps, cfg)
            else:
                send_no_opportunities_message(cycle, cfg)
        except Exception as e:
            logger.error(f"Scan error: {e}", exc_info=True)
            try:
                send_telegram_message(f"❌ Scan error: {str(e)[:200]}", cfg)
            except:
                pass
        time.sleep(interval)
def main():
    # Start bot in background thread
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    print("🤖 Bot thread started")
    # Start web server in main thread
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), StatusHandler)
    print(f"🌐 Web server running on port {port}")
    server.serve_forever()
if __name__ == "__main__":
    main()
