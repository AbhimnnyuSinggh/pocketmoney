#!/usr/bin/env python3
"""
test_bot.py — Quick test that checks EVERY component in 30 seconds.
Run this FIRST before running the main bot.

Tests:
  1. Can we reach Polymarket API? 
  2. Can we reach Kalshi API?
  3. Can we fetch real market prices?
  4. Can we send a Telegram message?
  5. Can we find any high-probability bonds RIGHT NOW?
  6. Can we find any price discrepancies?
"""
import sys
import json
import requests
import yaml
import os

# Colors for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

def ok(msg):
    print(f"  {GREEN}✅ PASS{RESET} — {msg}")

def fail(msg):
    print(f"  {RED}❌ FAIL{RESET} — {msg}")

def warn(msg):
    print(f"  {YELLOW}⚠️  WARN{RESET} — {msg}")

def header(msg):
    print(f"\n{BOLD}{'='*50}")
    print(f"  {msg}")
    print(f"{'='*50}{RESET}")


def load_config():
    """Load config.yaml"""
    if os.path.exists("config.yaml"):
        with open("config.yaml") as f:
            return yaml.safe_load(f)
    return None


def test_polymarket_api():
    """Test 1: Can we reach Polymarket and get real markets?"""
    header("TEST 1: Polymarket API Connection")

    try:
        resp = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={"limit": 5, "closed": "false", "active": "true"},
            timeout=15,
        )
        resp.raise_for_status()
        markets = resp.json()

        if not markets:
            fail("API returned empty response")
            return None

        ok(f"Connected! Got {len(markets)} markets")

        # Show a sample market
        m = markets[0]
        title = m.get("question", "Unknown")
        prices = m.get("outcomePrices", "[]")
        if isinstance(prices, str):
            prices = json.loads(prices)

        print(f"       Sample: \"{title[:70]}\"")
        if len(prices) >= 2:
            print(f"       YES: ${float(prices[0]):.4f}  |  NO: ${float(prices[1]):.4f}")

        return markets

    except requests.RequestException as e:
        fail(f"Cannot reach Polymarket: {e}")
        return None


def test_kalshi_api():
    """Test 2: Can we reach Kalshi and get real markets?"""
    header("TEST 2: Kalshi API Connection")

    try:
        resp = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"status": "open", "limit": 5},
            headers={"Accept": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        markets = data.get("markets", [])

        if not markets:
            warn("Kalshi returned no markets (may be region-blocked)")
            return None

        ok(f"Connected! Got {len(markets)} markets")

        m = markets[0]
        title = m.get("title", "Unknown")
        yes_ask = m.get("yes_ask", 0)
        print(f"       Sample: \"{title[:70]}\"")
        print(f"       YES ask: {yes_ask}¢")

        return markets

    except requests.RequestException as e:
        warn(f"Cannot reach Kalshi: {e}")
        print(f"       (This is OK — Kalshi may be region-blocked. Bot will still work with Polymarket)")
        return None


def test_find_bonds(poly_markets):
    """Test 3: Can we find high-probability bonds right now?"""
    header("TEST 3: Finding High-Probability Bonds (Live)")

    if not poly_markets:
        # Fetch more markets
        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"limit": 100, "closed": "false", "active": "true", "order": "volume24hr", "ascending": "false"},
                timeout=15,
            )
            resp.raise_for_status()
            poly_markets = resp.json()
        except:
            fail("Cannot fetch markets for bond scan")
            return

    bonds_found = []
    for m in poly_markets:
        try:
            prices_raw = m.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                prices = json.loads(prices_raw)
            else:
                prices = prices_raw or []

            if len(prices) < 2:
                continue

            yes_p = float(prices[0])
            no_p = float(prices[1])
            title = m.get("question", "")

            # Check for bonds (93¢+)
            for side, price in [("YES", yes_p), ("NO", no_p)]:
                if 0.93 <= price < 0.995:
                    profit = 1.0 - price
                    roi = (profit / price) * 100
                    bonds_found.append({
                        "title": title,
                        "side": side,
                        "price": price,
                        "roi": roi,
                    })
        except:
            continue

    if bonds_found:
        ok(f"Found {len(bonds_found)} bond opportunities!")
        # Show top 3
        bonds_found.sort(key=lambda b: b["roi"], reverse=True)
        for b in bonds_found[:3]:
            print(f"       🏦 {b['title'][:60]}")
            print(f"          Buy {b['side']} @ ${b['price']:.4f} → ROI: {b['roi']:.2f}%")
            print()
    else:
        warn("No bonds found in this sample (will find more with full scan of 2000+ markets)")


def test_find_mispricing(poly_markets):
    """Test 4: Check for any YES+NO mispricing."""
    header("TEST 4: Checking for Price Discrepancies (Live)")

    if not poly_markets:
        warn("No markets to check")
        return

    mispriced = []
    for m in poly_markets:
        try:
            prices_raw = m.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                prices = json.loads(prices_raw)
            else:
                prices = prices_raw or []

            if len(prices) < 2:
                continue

            yes_p = float(prices[0])
            no_p = float(prices[1])

            if yes_p <= 0 or no_p <= 0:
                continue

            total = yes_p + no_p
            if total < 0.98 and total > 0.50:
                profit = 1.0 - total
                roi = (profit / total) * 100
                mispriced.append({
                    "title": m.get("question", ""),
                    "yes": yes_p,
                    "no": no_p,
                    "total": total,
                    "roi": roi,
                })
        except:
            continue

    if mispriced:
        ok(f"Found {len(mispriced)} mispriced markets!")
        mispriced.sort(key=lambda x: x["roi"], reverse=True)
        for mp in mispriced[:3]:
            print(f"       🎯 {mp['title'][:60]}")
            print(f"          YES={mp['yes']:.4f} + NO={mp['no']:.4f} = {mp['total']:.4f} (profit: {mp['roi']:.2f}%)")
            print()
    else:
        print(f"       No obvious mispricing in this {len(poly_markets)}-market sample")
        print(f"       (Normal — full bot scans 2000+ markets to find rare ones)")


def test_telegram(cfg):
    """Test 5: Can we send a Telegram message?"""
    header("TEST 5: Telegram Connection")

    if not cfg:
        warn("No config.yaml found — skipping Telegram test")
        return

    tg = cfg.get("telegram", {})
    if not tg.get("enabled"):
        warn("Telegram is disabled in config.yaml — set enabled: true")
        return

    token = os.environ.get("TELEGRAM_BOT_TOKEN", tg.get("bot_token", ""))
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", tg.get("chat_id", ""))

    if not token or not chat_id:
        fail("bot_token or chat_id is empty in config.yaml")
        return

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": "🧪 <b>TEST MESSAGE</b>\n\nIf you see this, your Telegram connection is working!\n\nYour bot will send alerts here when it finds opportunities.",
            "parse_mode": "HTML",
        }
        resp = requests.post(url, json=payload, timeout=10)

        if resp.status_code == 200:
            ok("Telegram message sent! Check your phone 📱")
        else:
            error = resp.json().get("description", resp.text)
            fail(f"Telegram error: {error}")

    except Exception as e:
        fail(f"Telegram failed: {e}")


def test_full_scan_preview():
    """Test 6: Quick preview of what the full bot will find."""
    header("TEST 6: Full Scan Preview (100 markets)")

    try:
        resp = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={
                "limit": 100,
                "closed": "false",
                "active": "true",
                "order": "volume24hr",
                "ascending": "false",
            },
            timeout=15,
        )
        resp.raise_for_status()
        markets = resp.json()

        total = len(markets)
        bonds = 0
        mispriced = 0

        for m in markets:
            try:
                prices_raw = m.get("outcomePrices", "[]")
                if isinstance(prices_raw, str):
                    prices = json.loads(prices_raw)
                else:
                    prices = prices_raw or []

                if len(prices) < 2:
                    continue

                yes_p = float(prices[0])
                no_p = float(prices[1])

                # Count bonds
                if yes_p >= 0.93 or no_p >= 0.93:
                    bonds += 1

                # Count mispriced
                total_p = yes_p + no_p
                if 0.50 < total_p < 0.98:
                    mispriced += 1
            except:
                continue

        ok(f"Scanned {total} markets")
        print(f"       🏦 High-prob bonds (93¢+): {bonds}")
        print(f"       🎯 Potential mispricing: {mispriced}")
        print(f"       📊 Full bot scans 2000+ markets = ~{bonds * 20}+ bond opportunities")

    except Exception as e:
        fail(f"Scan preview failed: {e}")


# =========================================================================
# RUN ALL TESTS
# =========================================================================

if __name__ == "__main__":
    print(f"\n{BOLD}🧪 POLYMARKET ARB BOT v2.0 — SYSTEM TEST{RESET}")
    print(f"{'='*50}\n")

    cfg = load_config()

    # Test 1: Polymarket
    poly_markets = test_polymarket_api()

    # Test 2: Kalshi
    kalshi_markets = test_kalshi_api()

    # Test 3: Find bonds
    test_find_bonds(poly_markets)

    # Test 4: Find mispricing
    test_find_mispricing(poly_markets)

    # Test 5: Telegram
    test_telegram(cfg)

    # Test 6: Full scan preview
    test_full_scan_preview()

    # Summary
    header("SUMMARY")
    print(f"  If tests 1 and 5 passed, your bot is ready to run!")
    print(f"  Run: python3 main_v2.py --once")
    print(f"  Or continuously: python3 main_v2.py")
    print()
