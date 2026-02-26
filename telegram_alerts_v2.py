"""
telegram_alerts_v2.py — Sends alerts for all opportunity types:
  - Cross-platform arbitrage
  - High-probability bonds
  - Intra-market mispricing
"""
import logging
import requests as http_requests
from cross_platform_scanner import Opportunity
from html import escape as html_escape
try:
    from scoring import score_emoji
except ImportError:
    score_emoji = None
logger = logging.getLogger("arb_bot.telegram")
def send_telegram_message(text: str, cfg: dict) -> bool:
    """Send a message via Telegram Bot API."""
    if not cfg["telegram"]["enabled"]:
        return False
    token = cfg["telegram"]["bot_token"]
    chat_id = cfg["telegram"]["chat_id"]
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = http_requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            return True
        else:
            logger.warning(f"Telegram API error: {resp.status_code}")
            return False
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False
def _format_weather_signal(opp) -> str:
    """Rich format for weather forecast signals with source confluence data."""
    wd = opp._weather_data

    # Build source lines
    source_lines = []
    for src in wd["forecasts"]:
        agrees = src.get("bin") == wd["best_bin"]
        icon = "✅" if agrees else "❌"
        src_name = html_escape(src['source'])
        bin_label = src.get('bin', '?')
        source_lines.append(
            f"  {icon} {src_name:14s} {src['high_f']:.1f}°F → {bin_label}°F"
        )

    msg = (
        f"🌤 <b>WEATHER FORECAST</b> 🌤\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 <b>{html_escape(wd['city'])} Daily High — {html_escape(str(wd['date']))}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Resolves: Weather Underground ({html_escape(wd.get('station', ''))})\n\n"
        f"🔬 <b>Source Confluence ({wd['agree_count']}/{wd['total_sources']} agree):</b>\n"
        + "\n".join(source_lines) + "\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Best Range: {wd['best_bin']}°F</b>\n"
        f"  Confluence: {wd['agree_count']}/{wd['total_sources']} ({wd['agree_pct']:.0f}%)\n"
        f"  Market Price: {wd['market_price']*100:.0f}¢ (implied {wd['market_price']:.0%})\n"
        f"  ⚡ Edge: +{wd['edge']:.0%}\n\n"
    )

    if wd.get("adjacent_bin"):
        msg += (
            f"Adjacent: {wd['adjacent_bin']}°F\n"
            f"  Confluence: {wd['adjacent_count']}/{wd['total_sources']}\n"
            f"  Market Price: {wd['adjacent_price']*100:.0f}¢\n\n"
        )

    if wd.get("current_obs"):
        obs = wd["current_obs"]
        msg += (
            f"🌡 <b>LIVE OBSERVATION:</b>\n"
            f"  Current temp at {html_escape(wd.get('station', ''))}: "
            f"{obs['temp_f']:.0f}°F (as of {obs['time']})\n"
        )
        if obs.get("already_reached"):
            msg += f"  → {wd['best_bin']}°F range <b>ALREADY REACHED</b>\n"
        msg += "\n"

    # Bug 5: Market disagreement warning
    if wd.get("market_disagrees") and wd.get("market_leader_bin"):
        msg += (
            f"⚠️ <b>Market disagrees:</b> {wd['market_leader_bin']}°F leads at "
            f"{wd['market_leader_price']*100:.0f}¢.\n"
            f"Our models say {wd['best_bin']}°F — could be edge OR incomplete data.\n\n"
        )

    risk_emoji = {"low": "🟢", "medium": "🟡", "high": "🔴"}.get(opp.risk_level, "⚪")
    msg += (
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 <b>ROI if correct: {opp.profit_pct:.0f}%</b>\n"
        f"💰 $1 at {wd['market_price']*100:.0f}¢ → ${1.0/max(wd['market_price'], 0.01):.2f} payout\n"
        f"{risk_emoji} Risk: {opp.risk_level.title()}\n"
        f"⏰ Resolves: {html_escape(opp.hold_time)}\n"
    )

    if opp.urls and opp.urls[0]:
        msg += f'\n🔗 <a href="{opp.urls[0]}">Polymarket</a>'

    return msg


def format_opportunity(opp: Opportunity) -> str:
    """Format any opportunity type into a Telegram message."""
    # Weather forecasts have custom rich formatting
    if opp.opp_type == "weather_forecast" and hasattr(opp, '_weather_data'):
        return _format_weather_signal(opp)

    # Type-specific emoji and label
    type_info = {
        "cross_platform_arb": ("🔄", "CROSS-PLATFORM ARB", "💎"),
        "high_prob_bond": ("🏦", "HIGH-PROB BOND", "📊"),
        "intra_market_arb": ("🎯", "INTRA-MARKET ARB", "🔍"),
        "whale_convergence": ("🐋", "WHALE CONVERGENCE", "🐋"),
        "new_market": ("🆕", "NEW MARKET ALERT", "⚡"),
        "anti_hype": ("🔻", "ANTI-HYPE ALERT", "🧊"),
        "data_arb": ("📊", "DATA-DRIVEN EDGE", "🔬"),
        "longshot": ("🎯", "ASYMMETRIC LONGSHOT", "🎲"),
        "resolution_intel": ("🔍", "RESOLUTION INTEL", "📝"),
        "micro_arb": ("⚡", "MICRO ARB", "🔬"),
        "spread_arb": ("📐", "SPREAD ARB", "📊"),
        "weather_forecast": ("🌤", "WEATHER FORECAST", "🔬"),
    }
    emoji, label, icon = type_info.get(opp.opp_type, ("📌", "OPPORTUNITY", "📌"))
    # Risk level emoji
    risk_emoji = {
        "very_low": "🟢",
        "low": "🟢",
        "medium": "🟡",
        "high": "🔴",
    }
    risk_icon = risk_emoji.get(opp.risk_level, "⚪")

    # Edge Score badge (v3.0)
    score_badge = ""
    if hasattr(opp, "edge_score") and opp.edge_score > 0:
        se = score_emoji(opp.edge_score) if score_emoji else "📊"
        score_badge = f"{se} Edge Score: <b>{opp.edge_score:.0f}/100</b> | "

    # Build legs detail
    legs_text = ""
    for leg in opp.legs:
        legs_text += (
            f"  → <b>{leg['platform']}</b>: Buy {leg['side']} "
            f"@ ${leg['price']:.4f}\n"
        )
    # Build URLs
    links_text = ""
    for i, url in enumerate(opp.urls):
        if url:
            platform_name = opp.platforms[i].title() if i < len(opp.platforms) else "Link"
            links_text += f'🔗 <a href="{url}">{platform_name}</a>  '
    msg = (
        f"{emoji} <b>{label}</b> {emoji}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{score_badge}📋 <b>{html_escape(opp.title[:100])}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\n"
        f"{opp.description}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 <b>ROI: {opp.profit_pct:.2f}%</b>\n"
        f"💰 Profit per $100: <b>${opp.profit_amount:.2f}</b>\n"
        f"💵 Cost per unit: ${opp.total_cost:.4f}\n"
        f"{risk_icon} Risk: {opp.risk_level.replace('_', ' ').title()}\n"
    )
    if opp.hold_time:
        msg += f"⏰ Resolves: {opp.hold_time[:10]}\n"
    if legs_text:
        msg += f"\n{icon} <b>How to execute:</b>\n{legs_text}"
    if links_text:
        msg += f"\n{links_text}"
    return msg
def send_opportunities_batch(opportunities: list[Opportunity], cfg: dict):
    """Send all opportunities to Telegram, grouped by type."""
    if not cfg["telegram"]["enabled"] or not opportunities:
        return
    min_pct = cfg["telegram"].get("min_alert_profit_pct", 0.5)
    # Filter by minimum profit
    filtered = [o for o in opportunities if o.profit_pct >= min_pct]
    if not filtered:
        return
    # Send summary header
    summary = (
        f"📡 <b>SCAN RESULTS</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Found <b>{len(filtered)}</b> opportunities:\n"
    )
    # Count by type
    type_counts = {}
    for o in filtered:
        type_counts[o.opp_type] = type_counts.get(o.opp_type, 0) + 1
    type_labels = {
        "cross_platform_arb": "🔄 Cross-platform arbs",
        "high_prob_bond": "🏦 High-prob bonds",
        "intra_market_arb": "🎯 Intra-market arbs",
        "whale_convergence": "🐋 Whale convergence",
        "new_market": "🆕 New markets",
    }
    for t, count in type_counts.items():
        label = type_labels.get(t, t)
        summary += f"  {label}: <b>{count}</b>\n"
    send_telegram_message(summary, cfg)
    # Send top opportunities (max 10 to avoid spam)
    for opp in filtered[:10]:
        msg = format_opportunity(opp)
        send_telegram_message(msg, cfg)
        import time
        time.sleep(0.5)  # Avoid Telegram rate limits
def send_startup_message(cfg: dict):
    """Send a message when the bot starts."""
    msg = (
        f"🤖 <b>Polymarket Multi-Platform Arb Bot v2.0</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode: <code>{cfg['execution']['mode']}</code>\n"
        f"Bankroll: ${cfg['bankroll']['total_usdc']:.2f}\n"
        f"Min ROI: {cfg['bankroll']['min_profit_pct']}%\n"
        f"Scan interval: {cfg['scanner']['interval_seconds']}s\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Scanning: Polymarket + Kalshi\n"
        f"🔍 Strategies: Cross-platform arb, Bonds, Intra-market arb\n"
        f"🐋 Whale Tracker: {'ON' if cfg.get('whales', {}).get('enabled') else 'OFF'}\n"
        f"🆕 New Market Sniper: {'ON' if cfg.get('new_markets', {}).get('enabled') else 'OFF'}"
    )
    send_telegram_message(msg, cfg)
def send_no_opportunities_message(cycle: int, cfg: dict):
    """Optionally notify every N cycles if nothing found."""
    # Only send every 20 cycles (5 min at 15s interval) to avoid spam
    if cycle % 20 == 0:
        msg = f"💤 Scan cycle #{cycle} — no new opportunities. Still watching..."
        send_telegram_message(msg, cfg)
