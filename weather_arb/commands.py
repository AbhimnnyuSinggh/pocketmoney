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

        # Session stats from WeatherSession
        session_msg = ""
        wa = getattr(handler, '_weather_arb', None)
        if wa and hasattr(wa, 'session'):
            s = wa.session
            session_msg = (
                f"💰 <b>Capital:</b> ${s.available_capital:.2f} available / "
                f"${s.total_deployed:.2f} deployed\n"
                f"📈 <b>P&L:</b> ${s.net_pnl:+.2f} "
                f"({s.trades_won}W / {s.trades_lost}L = {s.win_rate:.0f}%)\n"
                f"🎯 <b>Phase:</b> {s.phase}\n"
                f"📊 <b>Bankroll:</b> ${s.current_bankroll:.2f}\n"
                f"🔁 Active positions: {len(s.active_positions)}\n\n"
            )

        report, img_path = await get_dashboard()
        full_report = session_msg + report

        if img_path:
            handler._send_photo(chat_id, img_path, caption=full_report)
        else:
            handler._send(chat_id, full_report, parse_mode="HTML")

    def cmd_weather_dryrun(chat_id: str, text: str):
        if not handler._is_admin(chat_id):
            return
            
        wa = getattr(handler, '_weather_arb', None)
        if not wa:
            handler._send(chat_id, "🌤 **Weather Arb Module**\nNot initialized.")
            return

        parts = text.split()
        if len(parts) > 1:
            val = parts[1].lower()
            if val in ("on", "true", "1"):
                wa.dry_run = True
            elif val in ("off", "false", "0"):
                wa.dry_run = False
            else:
                handler._send(chat_id, "Usage: `/weather_dryrun on` or `/weather_dryrun off`")
                return
            
            # Save to config.yaml to persist
            try:
                import yaml
                with open("config.yaml", "r") as f:
                    full_cfg = yaml.safe_load(f)
                if "weather_arb" not in full_cfg:
                    full_cfg["weather_arb"] = {}
                full_cfg["weather_arb"]["dry_run"] = wa.dry_run
                with open("config.yaml", "w") as f:
                    yaml.dump(full_cfg, f, default_flow_style=False, sort_keys=False)
            except Exception as e:
                logger.error(f"Failed to save dry_run to config: {e}")

        status = "ON 🟡 (Simulated)" if wa.dry_run else "OFF 🔴 (LIVE TRADING)"
        handler._send(chat_id, f"🌤 **Weather Dry Run:** {status}")

    def cmd_weather_mode(chat_id: str, text: str):
        if not handler._is_admin(chat_id):
            return
            
        wa = getattr(handler, '_weather_arb', None)
        if not wa:
            handler._send(chat_id, "🌤 **Weather Arb Module**\nNot initialized.")
            return

        parts = text.split()
        if len(parts) > 1:
            new_mode = parts[1].upper()
            from weather_arb.config import TradingMode
            try:
                wa.mode = TradingMode[new_mode]
                wa.mode_str = new_mode
                
                # Save to config.yaml
                try:
                    import yaml
                    with open("config.yaml", "r") as f:
                        full_cfg = yaml.safe_load(f)
                    if "weather_arb" not in full_cfg:
                        full_cfg["weather_arb"] = {}
                    full_cfg["weather_arb"]["mode"] = new_mode
                    with open("config.yaml", "w") as f:
                        yaml.dump(full_cfg, f, default_flow_style=False, sort_keys=False)
                except Exception as e:
                    logger.error(f"Failed to save mode to config: {e}")
                    
            except KeyError:
                handler._send(chat_id, "Invalid mode. Use SAFE, NEUTRAL, or AGGRESSIVE.")
                return

        handler._send(chat_id, f"🌤 **Weather Mode:** `{wa.mode.name}`")

    # Bind to handler 
    handler.routes["/weather_status"] = cmd_weather_status
    handler.routes["/perf"] = cmd_perf
    handler.routes["/weather_dryrun"] = cmd_weather_dryrun
    handler.routes["/weather_mode"] = cmd_weather_mode
    
    logger.info("Registered Weather Arb Telegram commands")
