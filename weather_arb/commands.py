"""
weather_arb/commands.py
Telegram commands for the Weather Arbitrage module.
"""
import logging
from weather_arb.performance_dashboard import get_dashboard

logger = logging.getLogger("arb_bot.weather.commands")

def register_weather_commands(handler):
    """Register weather bot commands to the TelegramBotHandler."""
    
    def cmd_weather_status(chat_id: str, text: str):
        if not handler._is_admin(chat_id):
            return
            
        wa = getattr(handler, '_weather_arb', None)
        if not wa or not wa.enabled:
            handler._send(chat_id, "🌤 **Weather Arb Module**\nModule is not enabled or initialized yet.")
            return
            
        status_msg = (
            f"🌤 **Weather Arb Scanner Active**\n"
            f"Mode: `{wa.mode.name}`\n"
            f"Target Cities: {wa.cfg.get('weather_arb', {}).get('cities', [])}\n"
            f"Dry Run: `{wa.dry_run}`\n\n"
            f"Use `/perf` to see P&L."
        )
        handler._send(chat_id, status_msg)
        
    async def cmd_perf(chat_id: str, text: str):
        if not handler._is_admin(chat_id):
            return
        report = await get_dashboard()
        handler._send(chat_id, report)

    # Bind to handler 
    handler.routes["/weather_status"] = cmd_weather_status
    handler.routes["/perf"] = cmd_perf
    
    logger.info("Registered Weather Arb Telegram commands")
