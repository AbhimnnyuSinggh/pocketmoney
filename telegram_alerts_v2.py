"""
telegram_alerts_v2.py — Sends alerts for all opportunity types:
  - Cross-platform arbitrage
  - High-probability bonds
  - Intra-market mispricing
"""
import logging
import requests as http_requests
from cross_platform_scanner import Opportunity
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
def format_opportunity(opp: Opportunity) -> str:
    """Format any opportunity type into a Telegram message."""
    # Type-specific emoji and label
    type_info = {
        "cross_platform_arb": ("🔄", "CROSS-PLATFORM ARB", "💎"),
        "high_prob_bond": ("🏦", "HIGH-PROB BOND", "📊"),
        "intra_market_arb": ("🎯", "INTRA-MARKET ARB", "🔍"),
        "whale_convergence": ("🐋", "WHALE CONVERGENCE", "🐋"),
        "new_market": ("🆕", "NEW MARKET ALERT", "⚡"),
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
            links_text += f"🔗 <a href=\"{url}\">{platform_name}</a>  "
    msg = (
        f"{emoji} <b>{label}</b> {emoji}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 <b>{opp.title[:100]}</b>\n"
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
