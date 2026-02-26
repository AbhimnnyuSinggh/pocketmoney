#!/usr/bin/env python3
"""
main_v2.py — Polymarket Multi-Platform Arbitrage Bot v2.0
Scans MULTIPLE prediction markets for THREE types of opportunities:
  1. Cross-Platform Arb: Same event priced differently on Polymarket vs Kalshi
  2. High-Probability Bonds: Near-certain outcomes priced 93-99¢ → pays $1.00
  3. Intra-Market Arb: YES + NO < $1.00 within a single platform
Usage:
  python3 main_v2.py                  # Continuous scanning
  python3 main_v2.py --once           # Single scan then exit
  python3 main_v2.py --config my.yaml # Custom config file
"""
import sys
import time
import signal
import logging
import argparse
from datetime import datetime, timezone
from config_loader import load_config
from cross_platform_scanner import run_full_cross_platform_scan, Opportunity
from whale_tracker import find_whale_opportunities
from new_market_sniper import find_new_market_opportunities
from telegram_bot import TelegramBotHandler
from telegram_alerts_v2 import (
    send_opportunities_batch,
    send_startup_message,
    send_no_opportunities_message,
    send_telegram_message,
)
# Elite Edge modules (v3.0) — graceful import
try:
    from elite_edges.anti_hype import find_anti_hype_opportunities
except ImportError:
    find_anti_hype_opportunities = None
try:
    from elite_edges.data_arb import find_data_arb_opportunities
except ImportError:
    find_data_arb_opportunities = None
try:
    from elite_edges.longshot_scanner import find_longshot_opportunities
except ImportError:
    find_longshot_opportunities = None
try:
    from elite_edges.bond_compounder import enrich_bond_opportunities
except ImportError:
    enrich_bond_opportunities = None
# Phase B modules (v3.0 Sprint 4) — graceful import
try:
    from elite_edges.resolution_intel import find_resolution_intel_opportunities
except ImportError:
    find_resolution_intel_opportunities = None
try:
    from speed_listener import SpeedListener
except ImportError:
    SpeedListener = None
try:
    from sentiment_engine import SentimentEngine
except ImportError:
    SentimentEngine = None
# Phase B Sprint 5 modules — graceful import
try:
    from elite_edges.micro_arb import find_micro_arb_opportunities
except ImportError:
    find_micro_arb_opportunities = None
try:
    from elite_edges.spread_arb import find_spread_arb_opportunities
except ImportError:
    find_spread_arb_opportunities = None
try:
    from platforms.manifold_adapter import find_manifold_cross_platform_opps
except ImportError:
    find_manifold_cross_platform_opps = None
try:
    from portfolio_rotator import PortfolioRotator
except ImportError:
    PortfolioRotator = None
# Execution Engine + Bond Spreader
try:
    from execution_engine import ExecutionEngine
except ImportError:
    ExecutionEngine = None
try:
    from bond_spreader import BondSpreader
except Exception as e:
    import logging
    logging.getLogger("arb_bot.main").error(f"CRITICAL: Failed to import BondSpreader: {e}", exc_info=True)
    BondSpreader = None
# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logging(cfg: dict):
    level_str = cfg["logging"]["level"].upper()
    level = getattr(logging, level_str, logging.INFO)
    handlers = []
    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if cfg["logging"]["console"]:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        handlers.append(ch)
    log_file = cfg["logging"].get("file")
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        handlers.append(fh)
    logging.basicConfig(level=level, handlers=handlers)
logger = logging.getLogger("arb_bot.main")
# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------
def run_cycle(cfg: dict, cycle: int, bot_handler: TelegramBotHandler | None = None) -> list[Opportunity]:
    """
    Main loop iteration:
    1. Check resolutions
    2. Fetch live data
    3. Analyze/score
    4. Auto-trade & distribute
    """
    if bot_handler and getattr(bot_handler, 'global_kill', False):
        logger.warning("🚨 GLOBAL KILL SWITCH ACTIVE — Skipping scan cycle 🚨")
        return []

    logger.info(f"--- Starting Scan Cycle #{cycle} | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info(f"{'='*60}")
    try:
        opportunities, poly_markets = run_full_cross_platform_scan(cfg)
    except Exception as e:
        logger.error(f"Scan error: {e}", exc_info=True)
        send_telegram_message(f"❌ Scan error: {str(e)[:200]}", cfg)
        opportunities = []
        poly_markets = []

    # Build combined market list for elite modules
    all_markets = poly_markets  # Kalshi already included in scanner
    logger.info(
        f"[CYCLE #{cycle} HEALTH] poly_markets={len(poly_markets)} | "
        f"opp_after_scanner={len(opportunities)}"
    )

    # --- Whale Convergence Scan ---
    try:
        whale_opps = find_whale_opportunities(cfg)
        opportunities.extend(whale_opps)
        # v3.0: Feed whale trades into vault for persistent scoring
        if bot_handler and hasattr(bot_handler, 'whale_vault') and bot_handler.whale_vault:
            try:
                from whale_tracker import fetch_recent_large_trades
                trades = fetch_recent_large_trades(cfg)
                if trades:
                    bot_handler.whale_vault.record_trades_batch(trades)
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Whale tracker error: {e}", exc_info=True)
    # --- New Market Sniper ---
    try:
        new_market_opps = find_new_market_opportunities(cfg, existing_markets=poly_markets)
        opportunities.extend(new_market_opps)
    except Exception as e:
        logger.error(f"New market sniper error: {e}", exc_info=True)

    # --- Elite Edge Modules (v3.0) ---
    # Anti-Hype Detector
    if find_anti_hype_opportunities and cfg.get("anti_hype", {}).get("enabled", True):
        try:
            anti_hype_opps = find_anti_hype_opportunities(all_markets, cfg)
            opportunities.extend(anti_hype_opps)
            if anti_hype_opps:
                logger.info(f"🔻 Anti-Hype: {len(anti_hype_opps)} signals")
        except Exception as e:
            logger.error(f"Anti-Hype module error: {e}", exc_info=True)

    # External Data Arb
    if find_data_arb_opportunities and cfg.get("data_arb", {}).get("enabled", True):
        try:
            data_opps = find_data_arb_opportunities(all_markets, cfg)
            opportunities.extend(data_opps)
            if data_opps:
                logger.info(f"📊 Data Arb: {len(data_opps)} signals")
        except Exception as e:
            logger.error(f"Data Arb module error: {e}", exc_info=True)

    # Longshot Scanner
    if find_longshot_opportunities and cfg.get("longshot", {}).get("enabled", True):
        try:
            longshot_opps = find_longshot_opportunities(all_markets, cfg)
            opportunities.extend(longshot_opps)
            if longshot_opps:
                logger.info(f"🎯 Longshots: {len(longshot_opps)} signals")
        except Exception as e:
            logger.error(f"Longshot module error: {e}", exc_info=True)

    # === Weather Forecast Signals (for Climate category users) ===
    if cfg.get("weather_forecast", {}).get("enabled", True):
        try:
            from elite_edges.weather_forecast import scan_weather_forecasts
            import asyncio
            weather_signals = asyncio.run(scan_weather_forecasts(poly_markets, cfg))
            if weather_signals:
                opportunities.extend(weather_signals)
                logger.info(f"🌤 Weather Forecast: {len(weather_signals)} signals")
        except Exception as e:
            logger.error(f"Weather forecast scanner error: {e}", exc_info=True)

    # Bond Compounder — enriches existing bonds (doesn't add new ones)
    if enrich_bond_opportunities:
        try:
            enrich_bond_opportunities(opportunities, cfg)
        except Exception as e:
            logger.error(f"Bond Compounder error: {e}", exc_info=True)

    # Resolution Intel (Phase B)
    if find_resolution_intel_opportunities and cfg.get("resolution_intel", {}).get("enabled", True):
        try:
            ri_opps = find_resolution_intel_opportunities(all_markets, cfg)
            opportunities.extend(ri_opps)
            if ri_opps:
                logger.info(f"🔍 Resolution Intel: {len(ri_opps)} signals")
        except Exception as e:
            logger.error(f"Resolution Intel error: {e}", exc_info=True)

    # Sentiment enrichment (Phase B) — boost Edge Scores with sentiment
    if bot_handler and hasattr(bot_handler, 'sentiment_engine') and bot_handler.sentiment_engine:
        try:
            bot_handler.sentiment_engine.refresh()
        except Exception:
            pass

    # Micro Arb (Phase B Sprint 5)
    if find_micro_arb_opportunities and cfg.get("micro_arb", {}).get("enabled", True):
        try:
            micro_opps = find_micro_arb_opportunities(all_markets, cfg)
            opportunities.extend(micro_opps)
            if micro_opps:
                logger.info(f"⚡ Micro Arb: {len(micro_opps)} signals")
        except Exception as e:
            logger.error(f"Micro Arb error: {e}", exc_info=True)

    # Spread Arb (Phase B Sprint 5)
    if find_spread_arb_opportunities and cfg.get("spread_arb", {}).get("enabled", True):
        try:
            spread_opps = find_spread_arb_opportunities(all_markets, cfg)
            opportunities.extend(spread_opps)
            if spread_opps:
                logger.info(f"📐 Spread Arb: {len(spread_opps)} signals")
        except Exception as e:
            logger.error(f"Spread Arb error: {e}", exc_info=True)

    # Manifold Cross-Platform (Phase B Sprint 5)
    if find_manifold_cross_platform_opps and cfg.get("manifold", {}).get("enabled", True):
        try:
            manifold_opps = find_manifold_cross_platform_opps(poly_markets, cfg)
            opportunities.extend(manifold_opps)
            if manifold_opps:
                logger.info(f"🌐 Manifold: {len(manifold_opps)} cross-platform arbs")
        except Exception as e:
            logger.error(f"Manifold adapter error: {e}", exc_info=True)

    # === Bond Spread Automator ===
    if cfg.get("bond_spreader", {}).get("enabled", False):
        try:
            bs = getattr(bot_handler, '_bond_spreader', None)
            if bs:
                # 1. Check resolutions (frees capital)
                resolved = bs.check_resolutions()
                for r in resolved:
                    emoji = "✅" if r["won"] else "❌"
                    pnl_str = f"+${r['pnl']:.2f}" if r["pnl"] >= 0 else f"-${abs(r['pnl']):.2f}"
                    bot_handler._send_admin(
                        f"{emoji} Bond {r['tier']}: {r['title']}\n"
                        f"{'Won' if r['won'] else 'Lost'} {pnl_str} "
                        f"({r['side']} @ {r['price']:.2f})"
                    )

                # 2. Monitor active bets (loss cutting + early exit)
                monitor = bs.monitor_active_bets()
                for sold in monitor.get("sold_loss", []):
                    bot_handler._send_admin(
                        f"🛡 LOSS CUT: {sold['title']}\n"
                        f"Entry: ${sold['entry']:.2f} → Exit: ${sold['exit']:.2f}\n"
                        f"Saved: ${sold['saved']:.2f} vs full loss"
                    )
                for sold in monitor.get("sold_profit", []):
                    bot_handler._send_admin(
                        f"📈 EARLY EXIT: {sold['title']}\n"
                        f"Entry: ${sold['entry']:.2f} → Exit: ${sold['exit']:.2f}\n"
                        f"Profit: +${sold['profit']:.2f} in {sold['days_held']:.1f}d\n"
                        f"Daily ROI: {sold['daily_roi_sell']:.1f}% vs hold {sold['daily_roi_hold']:.1f}%"
                    )

                # 3. Deploy new bets
                new_bets = bs.scan_and_deploy(poly_markets)
                if new_bets:
                    total_amt = sum(b.get("amount", 0) for b in new_bets)
                    buckets = {}
                    for b in new_bets:
                        bk = b.get("time_bucket", "?")
                        buckets[bk] = buckets.get(bk, 0) + 1
                    bucket_str = " ".join(f"{k}:{v}" for k, v in sorted(buckets.items()))
                    bot_handler._send_admin(
                        f"🏦 Spread: +{len(new_bets)} bets (${total_amt:.2f})\n"
                        f"Buckets: {bucket_str}"
                    )

        except Exception as e:
            logger.error(f"Bond spreader error: {e}", exc_info=True)

    # === Weather Arbitrage Module ===
    if cfg.get("weather_arb", {}).get("enabled", False):
        try:
            wa = getattr(bot_handler, '_weather_arb', None)
            if wa:
                import threading
                if not hasattr(run_cycle, '_weather_lock'):
                    run_cycle._weather_lock = threading.Lock()

                if run_cycle._weather_lock.locked():
                    logger.warning("Weather scan still running from previous cycle, skipping")
                else:
                    def _weather_thread():
                        with run_cycle._weather_lock:
                            import asyncio
                            try:
                                # 1. Check resolutions first (frees capital)
                                resolved = asyncio.run(wa.check_resolutions())
                                for r in resolved:
                                    emoji = "✅" if r["won"] else "❌"
                                    bot_handler._send_admin(
                                        f"{emoji} Weather {r['bin']}: "
                                        f"{'Won' if r['won'] else 'Lost'} "
                                        f"${r['profit']:+.2f} (stake ${r['stake']:.2f})"
                                    )

                                # 2. Scan for new trades
                                weather_opps = asyncio.run(wa.scan_and_deploy(poly_markets))
                                if weather_opps:
                                    bot_handler.distribute_signals(weather_opps, cfg)
                                asyncio.run(wa.update_dashboard())
                            except Exception as e:
                                logger.error(f"Weather arb thread error: {e}")

                    threading.Thread(target=_weather_thread, daemon=True, name="weather-arb").start()
        except Exception as e:
            logger.error(f"Weather arb init error: {e}", exc_info=True)

    if not opportunities:
        logger.info("No opportunities this cycle")
        send_no_opportunities_message(cycle, cfg)
        return opportunities
    logger.info(f"🚨 {len(opportunities)} opportunities found!")
    # Route through interactive handler if available
    # Per-user dedup is handled inside distribute_signals
    if bot_handler:
        # 2. Add fast alerts from SpeedListener if enabled
        fast_alerts = getattr(bot_handler, 'speed_listener', None)
        if fast_alerts:
            alerts = fast_alerts.get_fast_alerts()
            if alerts:
                opportunities.extend(alerts)

        bot_handler.distribute_signals(opportunities, cfg)
    else:
        send_opportunities_batch(opportunities, cfg)

    # === Cycle Diagnostic (every 10 cycles = ~10 min) ===
    if bot_handler and cycle % 10 == 0:
        active_mod = cfg.get("execution", {}).get("active_autotrader", "none")
        exec_mode = cfg.get("execution", {}).get("mode", "dry_run")

        diag_parts = [f"\ud83d\udd04 <b>Cycle {cycle} Diagnostic</b>"]
        diag_parts.append(f"Mode: <code>{exec_mode}</code> | AutoTrader: <code>{active_mod}</code>")
        diag_parts.append(f"Markets scanned: {len(poly_markets)}")
        diag_parts.append(f"Opportunities found: {len(opportunities)}")

        # Bond spreader status
        bs = getattr(bot_handler, '_bond_spreader', None)
        if bs and bs.enabled:
            active_bets = len([b for b in bs.session.active_bets if b.get("status") == "active"])
            pool = bs.session.current_pool
            deployed = bs.session.total_deployed
            diag_parts.append(f"\ud83c\udfe6 Bonds: {active_bets} active, ${deployed:.2f} deployed, ${pool:.2f} pool")
            if active_mod != "bonds":
                diag_parts.append("  \u26a0\ufe0f AutoTrader not set to 'bonds' — trades blocked")

        # Weather status
        wa = getattr(bot_handler, '_weather_arb', None)
        if wa and wa.enabled:
            diag_parts.append(f"\ud83c\udf24 Weather: dry_run={wa.dry_run}")
            if wa.dry_run:
                diag_parts.append("  \u26a0\ufe0f Weather in dry_run — use /wallet live")

        if exec_mode == "dry_run":
            diag_parts.append("\n\u26a0\ufe0f <b>Global mode is DRY RUN — no real trades possible</b>")
            diag_parts.append("Use /wallet live to enable trading")

        send_telegram_message("\n".join(diag_parts), cfg)

    return opportunities
def main():
    parser = argparse.ArgumentParser(description="Multi-Platform Prediction Market Arb Bot")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--once", action="store_true", help="Single scan then exit")
    args = parser.parse_args()
    cfg = load_config(args.config)
    # Add default cross-platform settings if not in config
    cfg.setdefault("cross_platform", {
        "min_profit_pct": 1.0,
        "similarity_threshold": 0.60,
    })
    cfg.setdefault("bonds", {
        "min_price": 0.93,
        "min_roi_pct": 0.5,
    })
    cfg.setdefault("mispricing", {
        "max_sum": 0.98,
    })
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
    cfg.setdefault("bond_spreader", {
        "enabled": False,
        "mode": "dry_run",
        "base_amount": 1.00,
        "max_total_deployed": 100.00,
        "max_per_category_pct": 20,
        "min_liquidity": 5000,
        "min_volume": 1000,
        "min_price": 0.93,
        "max_resolution_days": 7,
        "reinvest_rate": 0.80,
        "adaptive_sizing": True,
        "adaptive_min_samples": 50,
        "limit_order_offset": 0.005,
        "limit_order_timeout_minutes": 15,
    })
    setup_logging(cfg)
    logger.info("=" * 60)
    logger.info("  Multi-Platform Prediction Market Arb Bot v2.0")
    logger.info(f"  Mode: {cfg['execution']['mode']}")
    logger.info(f"  Platforms: Polymarket + Kalshi")
    logger.info(f"  Strategies: Cross-Platform Arb | Bonds | Intra-Market")
    logger.info(f"  Whale Tracker: {'ON' if cfg.get('whales', {}).get('enabled') else 'OFF'}")
    logger.info(f"  New Market Sniper: {'ON' if cfg.get('new_markets', {}).get('enabled') else 'OFF'}")
    logger.info(f"  Bankroll: ${cfg['bankroll']['total_usdc']:.2f}")
    logger.info(f"  Min ROI: {cfg['bankroll']['min_profit_pct']}%")
    logger.info(f"  Scan interval: {cfg['scanner']['interval_seconds']}s")
    logger.info("=" * 60)
    if cfg["telegram"]["enabled"]:
        send_startup_message(cfg)
    # Initialize Execution Engine for trading
    if ExecutionEngine:
        try:
            bot_handler.execution_engine = ExecutionEngine(cfg)
            logger.info("Execution Engine initialized")
        except Exception as e:
            bot_handler.execution_engine = None
            import traceback
            err = traceback.format_exc()
            logger.warning(f"Execution Engine init error: {err}")
            if cfg["telegram"]["enabled"]:
                from telegram_alerts_v2 import send_telegram_message
                send_telegram_message(f"🚨 <b>Execution Engine Init Failed</b>\n<pre>{str(err)[:500]}</pre>", cfg)
    else:
        bot_handler.execution_engine = None
        if cfg["telegram"]["enabled"]:
            from telegram_alerts_v2 import send_telegram_message
            send_telegram_message(f"🚨 <b>ExecutionEngine not imported!</b> ImportError occurred silently.", cfg)

    # Initialize Bond Spreader
    if BondSpreader and cfg.get("bond_spreader", {}).get("enabled", False):
        try:
            bot_handler._bond_spreader = BondSpreader(cfg, bot_handler.execution_engine)
            logger.info("Bond Spreader initialized")
        except Exception as e:
            bot_handler._bond_spreader = None
            logger.error(f"Bond Spreader init error: {e}")

    # Initialize Weather Arbitrage
    if cfg.get("weather_arb", {}).get("enabled", False):
        try:
            from weather_arb.trader import WeatherArbitrage
            bot_handler._weather_arb = WeatherArbitrage(cfg, bot_handler.execution_engine, getattr(bot_handler, 'pnl_tracker', None))
            logger.info("Weather Arbitrage initialized")
        except Exception as e:
            bot_handler._weather_arb = None
            logger.error(f"Weather Arb init error: {e}")

    bot_handler.start_polling()
    logger.info("Interactive signal selector active")

    # Phase B: Start Speed Listener (10s fast polling)
    speed = None
    if SpeedListener and cfg.get("speed_listener", {}).get("enabled", True):
        try:
            speed = SpeedListener(cfg)
            speed.start()
            bot_handler.speed_listener = speed
        except Exception as e:
            logger.warning(f"Speed Listener init error: {e}")

    # Phase B: Start Sentiment Engine
    if SentimentEngine and cfg.get("sentiment", {}).get("enabled", True):
        try:
            bot_handler.sentiment_engine = SentimentEngine(cfg)
        except Exception as e:
            logger.warning(f"Sentiment Engine init error: {e}")

    # Strand.trade Whale: Initialize WhaleVault and attach to bot
    try:
        from whale_vault import WhaleVault
        vault_path = cfg.get("whale_vault", {}).get("vault_path", "whale_vault.json")
        whale_vault = WhaleVault(vault_path)
        bot_handler.whale_vault = whale_vault
        # Run initial leaderboard merge on startup
        whale_vault.merge_leaderboard_data("30d")
        logger.info("🏆 Whale Vault initialized + leaderboard merged")
    except Exception as e:
        whale_vault = None
        logger.warning(f"WhaleVault init error: {e}")
    # Graceful shutdown
    running = True
    def handle_signal(signum, frame):
        nonlocal running
        logger.info("Shutdown signal received...")
        running = False
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    # Main loop
    cycle = 0
    interval = cfg["scanner"]["interval_seconds"]
    last_leaderboard_ts = 0  # Track daily leaderboard refresh
    while running:
        cycle += 1
        start = time.time()
        try:
            run_cycle(cfg, cycle, bot_handler)
        except Exception as e:
            logger.error(f"Cycle #{cycle} error: {e}", exc_info=True)
        # Daily leaderboard refresh (every 24h)
        if whale_vault and time.time() - last_leaderboard_ts > 86400:
            try:
                whale_vault.merge_leaderboard_data("30d")
                last_leaderboard_ts = time.time()
                logger.info("🏆 Daily leaderboard refresh complete")
            except Exception as e:
                logger.warning(f"Leaderboard refresh error: {e}")
        if args.once:
            logger.info("Single-run mode — done")
            break
        elapsed = time.time() - start
        sleep_time = max(1, interval - elapsed)
        logger.info(f"Next scan in {sleep_time:.0f}s...")
        wake = time.time() + sleep_time
        # During the sleep window, fast-drain new market alerts every second
        # This is what achieves sub-10s new market detection between full scans
        while time.time() < wake and running:
            if speed and bot_handler:
                try:
                    fast_opps = speed.get_fast_new_markets()
                    if fast_opps:
                        logger.info(
                            f"🔥 Fast-dispatching {len(fast_opps)} new market alert(s) "
                            f"between scan cycles!"
                        )
                        bot_handler.distribute_signals(fast_opps, cfg)
                except Exception as e:
                    logger.debug(f"Fast new-market dispatch error: {e}")
            time.sleep(1)
    bot_handler.stop_polling()
    logger.info("Bot stopped")
if __name__ == "__main__":
    main()
