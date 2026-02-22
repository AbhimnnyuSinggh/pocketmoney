"""
cross_platform_scanner.py — Scans MULTIPLE prediction market platforms for:
1. Cross-Platform Arbitrage: Same event priced differently on Polymarket vs Kalshi/others
   → Buy YES on one platform + NO on the other = guaranteed profit
   
2. High-Probability Bonds: Markets priced 95¢+ that resolve within days
   → Near-certain profit, small but consistent
   
3. Multi-Outcome Intra-Market: Sum of all outcomes < $1.00 within Polymarket
   → Original strategy, still checked
Data Sources:
  - Polymarket Gamma API (events + prices)
  - Kalshi public API (events + prices, no auth needed for reading)
  - ArbBets-style comparison (manual matching by event title similarity)
"""
import time
import json
import logging
import requests
from collections import defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher
try:
    from scoring import compute_edge_score, score_emoji
except ImportError:
    compute_edge_score = None
    score_emoji = None
logger = logging.getLogger("arb_bot.cross_platform")
# =========================================================================
# DATA FETCHING — Each Platform
# =========================================================================
def fetch_polymarket_markets(cfg: dict) -> list[dict]:
    """Fetch active markets from Polymarket Gamma API with prices."""
    base = cfg["scanner"]["gamma_api_url"]
    all_markets = []
    offset = 0
    limit = 100
    while offset < cfg["scanner"]["max_markets"]:
        params = {
            "limit": limit,
            "offset": offset,
            "closed": "false",
            "active": "true",
            "order": "volume24hr",
            "ascending": "false",
        }
        try:
            resp = requests.get(f"{base}/markets", params=params, timeout=15)
            resp.raise_for_status()
            markets = resp.json()
        except requests.RequestException as e:
            logger.error(f"Polymarket /markets error: {e}")
            break
        if not markets:
            break
        for m in markets:
            try:
                prices_raw = m.get("outcomePrices", "[]")
                if isinstance(prices_raw, str):
                    prices = json.loads(prices_raw)
                else:
                    prices = prices_raw or []
                outcomes_raw = m.get("outcomes", "[]")
                if isinstance(outcomes_raw, str):
                    outcomes = json.loads(outcomes_raw)
                else:
                    outcomes = outcomes_raw or []
                yes_price = float(prices[0]) if len(prices) > 0 else 0
                no_price = float(prices[1]) if len(prices) > 1 else 0
                # Build the correct Polymarket URL using the EVENT slug
                # (not the market slug, which gives 404 errors)
                events_list = m.get("events", [])
                if events_list and isinstance(events_list, list):
                    event_slug = events_list[0].get("slug", m.get("slug", ""))
                else:
                    event_slug = m.get("slug", "")
                poly_url = f"https://polymarket.com/event/{event_slug}"
                all_markets.append({
                    "platform": "polymarket",
                    "title": m.get("question", ""),
                    "slug": m.get("slug", ""),
                    "event_slug": event_slug,
                    "market_id": m.get("id", ""),
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "volume": float(m.get("volume", 0) or 0),
                    "volume_24h": float(m.get("volume24hr", 0) or 0),
                    "liquidity": float(m.get("liquidity", 0) or 0),
                    "end_date": m.get("endDate", ""),
                    "category": m.get("category", ""),
                    "url": poly_url,
                    "outcomes": outcomes,
                    "outcome_prices": [float(p) for p in prices] if prices else [],
                    "active": m.get("active", True),
                    "closed": m.get("closed", False),
                    "created_at": m.get("createdAt", ""),
                    "start_date": m.get("startDate", ""),
                    "condition_id": m.get("conditionId", ""),
                    "clob_token_ids": json.loads(m["clobTokenIds"]) if isinstance(m.get("clobTokenIds"), str) else (m.get("clobTokenIds") or []),
                })
            except (ValueError, IndexError, TypeError):
                continue
        offset += limit
        time.sleep(0.3)
    logger.info(f"Fetched {len(all_markets)} markets from Polymarket")
    return all_markets
def fetch_kalshi_markets(cfg: dict) -> list[dict]:
    """
    Fetch active markets from Kalshi's public API.
    Kalshi's API is publicly readable without auth for market data.
    """
    all_markets = []
    cursor = None
    for _ in range(20):  # Max 20 pages
        params = {"status": "open", "limit": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            resp = requests.get(
                "https://api.elections.kalshi.com/trade-api/v2/markets",
                params=params,
                timeout=15,
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.warning(f"Kalshi API error: {e}")
            break
        markets = data.get("markets", [])
        if not markets:
            break
        for m in markets:
            try:
                yes_price = (m.get("yes_ask") or 0) / 100.0  # Kalshi prices in cents
                no_price = (m.get("no_ask") or 0) / 100.0
                # Fallback to bid if ask not available
                if yes_price == 0:
                    yes_price = (m.get("last_price") or 0) / 100.0
                if no_price == 0 and yes_price > 0:
                    no_price = 1.0 - yes_price
                all_markets.append({
                    "platform": "kalshi",
                    "title": m.get("title", ""),
                    "slug": m.get("ticker", ""),
                    "market_id": m.get("ticker", ""),
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "volume": float(m.get("volume", 0) or 0),
                    "volume_24h": float(m.get("volume_24h", 0) or 0),
                    "liquidity": float(m.get("open_interest", 0) or 0),
                    "end_date": m.get("close_time", ""),
                    "category": m.get("category", ""),
                    "url": f"https://kalshi.com/markets/{m.get('event_ticker') or m.get('ticker', '')}",
                    "outcomes": ["Yes", "No"],
                    "outcome_prices": [yes_price, no_price],
                    "active": m.get("status") == "open",
                    "closed": m.get("status") != "open",
                })
            except (ValueError, TypeError):
                continue
        cursor = data.get("cursor")
        if not cursor:
            break
        time.sleep(0.3)
    logger.info(f"Fetched {len(all_markets)} markets from Kalshi")
    return all_markets
# =========================================================================
# OPPORTUNITY TYPES
# =========================================================================
@dataclass
class Opportunity:
    """A detected trading opportunity of any type."""
    opp_type: str          # "cross_platform_arb", "high_prob_bond", "intra_market_arb"
    title: str
    description: str
    profit_pct: float      # Expected ROI %
    profit_amount: float   # Profit per $100 invested
    total_cost: float      # Cost per unit/bundle
    platforms: list[str]   # Which platforms involved
    legs: list[dict] = field(default_factory=list)
    urls: list[str] = field(default_factory=list)
    risk_level: str = "low"
    hold_time: str = ""    # Estimated time until resolution
    category: str = ""     # Market category (e.g. "Sports", "Crypto", "Politics")
    edge_score: float = 0.0  # Unified edge score (0-100) from scoring.py
    # LP / Execution fields (optional — populated by reward_farming & scanner)
    market_slug: str = ""
    condition_id: str = ""
    yes_token_id: str = ""
    no_token_id: str = ""
    token_ids: list = field(default_factory=list)  # [yes_token, no_token] for bond spreader
# =========================================================================
# STRATEGY 1: Cross-Platform Arbitrage
# =========================================================================
# STRATEGY 1: Cross-Platform Arbitrage (improved matching)
# =========================================================================

# Common words that don't help distinguish markets
_STOP_WORDS = frozenset(
    "will the a an of in on at to by for is be do does "
    "this that it its if or and not no yes any more than "
    "before after above below between market price "
    "what who when where how much many".split()
)


def _extract_keywords(title: str) -> set[str]:
    """Extract meaningful keywords from a market title."""
    # Normalize: lowercase, keep only alphanumeric + spaces
    cleaned = ""
    for ch in title.lower():
        if ch.isalnum() or ch == ' ':
            cleaned += ch
        else:
            cleaned += ' '

    words = cleaned.split()
    # Remove stop words, keep words with 2+ characters
    keywords = {w for w in words if w not in _STOP_WORDS and len(w) >= 2}
    return keywords


def _keyword_similarity(kw_a: set[str], kw_b: set[str]) -> float:
    """Jaccard similarity between keyword sets."""
    if not kw_a or not kw_b:
        return 0.0
    intersection = kw_a & kw_b
    union = kw_a | kw_b
    return len(intersection) / len(union) if union else 0.0


def _title_similarity(title_a: str, title_b: str) -> float:
    """Calculate similarity between two market titles (0 to 1)."""
    # Normalize
    a = title_a.lower().strip()
    b = title_b.lower().strip()
    return SequenceMatcher(None, a, b).ratio()


def find_cross_platform_arbs(
    poly_markets: list[dict],
    kalshi_markets: list[dict],
    cfg: dict,
) -> list[Opportunity]:
    """
    Find the same event on both platforms with price discrepancies.
    Uses keyword-based pre-filtering for speed, then fuzzy matching for accuracy.
    """
    opportunities = []
    min_profit = cfg.get("cross_platform", {}).get("min_profit_pct", 1.0)
    similarity_threshold = cfg.get("cross_platform", {}).get("similarity_threshold", 0.60)

    logger.info(f"Matching {len(poly_markets)} Polymarket vs {len(kalshi_markets)} Kalshi markets...")

    # Pre-compute keywords for all markets
    poly_keywords = []
    for p in poly_markets:
        if p["yes_price"] <= 0:
            poly_keywords.append(set())
        else:
            poly_keywords.append(_extract_keywords(p["title"]))

    kalshi_keywords = []
    for k in kalshi_markets:
        if k["yes_price"] <= 0:
            kalshi_keywords.append(set())
        else:
            kalshi_keywords.append(_extract_keywords(k["title"]))

    # Build inverted index: keyword → list of kalshi indices
    # This avoids O(n²) full scan — only compare markets sharing keywords
    keyword_index: dict[str, list[int]] = defaultdict(list)
    for i, kws in enumerate(kalshi_keywords):
        for kw in kws:
            keyword_index[kw].append(i)

    matched_count = 0
    near_miss_count = 0

    for pi, poly in enumerate(poly_markets):
        if not poly_keywords[pi]:
            continue

        # Find candidate Kalshi markets that share at least 2 keywords
        candidate_indices: dict[int, int] = defaultdict(int)
        for kw in poly_keywords[pi]:
            for ki in keyword_index.get(kw, []):
                candidate_indices[ki] += 1

        # Only check candidates with 2+ shared keywords
        for ki, shared_count in candidate_indices.items():
            if shared_count < 2:
                continue

            kalshi = kalshi_markets[ki]

            # Keyword similarity check (fast)
            kw_sim = _keyword_similarity(poly_keywords[pi], kalshi_keywords[ki])
            if kw_sim < 0.30:
                continue

            # Full fuzzy match (slower, only on candidates)
            full_sim = _title_similarity(poly["title"], kalshi["title"])

            # Use combined score: weight keywords more since they handle
            # different phrasing better than SequenceMatcher
            combined_sim = (kw_sim * 0.6) + (full_sim * 0.4)

            if combined_sim < similarity_threshold * 0.75:
                continue

            matched_count += 1

            # Check arb: Poly YES + Kalshi NO
            combo_1_cost = poly["yes_price"] + kalshi["no_price"]
            combo_1_profit = 1.0 - combo_1_cost

            # Check arb: Kalshi YES + Poly NO
            combo_2_cost = kalshi["yes_price"] + poly["no_price"]
            combo_2_profit = 1.0 - combo_2_cost

            # Take the better combo
            if combo_1_profit > combo_2_profit and combo_1_profit > 0:
                cost = combo_1_cost
                profit = combo_1_profit
                roi = (profit / cost) * 100 if cost > 0 else 0
                legs = [
                    {"platform": "Polymarket", "side": "YES", "price": poly["yes_price"]},
                    {"platform": "Kalshi", "side": "NO", "price": kalshi["no_price"]},
                ]
            elif combo_2_profit > 0:
                cost = combo_2_cost
                profit = combo_2_profit
                roi = (profit / cost) * 100 if cost > 0 else 0
                legs = [
                    {"platform": "Kalshi", "side": "YES", "price": kalshi["yes_price"]},
                    {"platform": "Polymarket", "side": "NO", "price": poly["no_price"]},
                ]
            else:
                # Matched but no arb opportunity — still worth logging
                near_miss_count += 1
                continue

            if roi < min_profit:
                near_miss_count += 1
                continue

            opp = Opportunity(
                opp_type="cross_platform_arb",
                title=poly["title"],
                description=(
                    f"Same event priced differently!\n"
                    f"Polymarket: YES={poly['yes_price']:.2f} NO={poly['no_price']:.2f}\n"
                    f"Kalshi: YES={kalshi['yes_price']:.2f} NO={kalshi['no_price']:.2f}\n"
                    f"Buy {legs[0]['side']} on {legs[0]['platform']} + "
                    f"{legs[1]['side']} on {legs[1]['platform']}"
                ),
                profit_pct=round(roi, 2),
                profit_amount=round(profit * 100, 2),
                total_cost=round(cost, 4),
                platforms=["polymarket", "kalshi"],
                legs=legs,
                urls=[poly["url"], kalshi["url"]],
                risk_level="very_low",
                hold_time=poly.get("end_date", ""),
                category=poly.get("category", ""),
                market_slug=poly.get("slug", ""),
                condition_id=poly.get("condition_id", ""),
                token_ids=poly.get("clob_token_ids", []),
            )
            opportunities.append(opp)
            logger.info(
                f"[CROSS-PLATFORM ARB] {poly['title'][:60]} | "
                f"ROI: {roi:.2f}% | Match: kw={kw_sim:.2f} full={full_sim:.2f}"
            )

    logger.info(
        f"Cross-platform matching: {matched_count} title matches found, "
        f"{near_miss_count} near-misses (matched but no profitable arb), "
        f"{len(opportunities)} arb opportunities"
    )

    opportunities.sort(key=lambda o: o.profit_pct, reverse=True)
    return opportunities
# =========================================================================
# STRATEGY 2: High-Probability Bonds
# =========================================================================
def find_high_prob_bonds(
    markets: list[dict],
    cfg: dict,
) -> list[Opportunity]:
    """
    Find markets where YES or NO is priced 93¢+ (high probability).
    These are near-certain outcomes that pay $1.00 at resolution.
    Profit = $1.00 - price.
    """
    opportunities = []
    min_price = cfg.get("bonds", {}).get("min_price", 0.93)
    min_roi = cfg.get("bonds", {}).get("min_roi_pct", 0.5)
    for m in markets:
        if m.get("closed") or not m.get("active", True):
            continue
        for side, price in [("YES", m["yes_price"]), ("NO", m["no_price"])]:
            if price < min_price or price >= 0.995:
                continue
            profit = 1.0 - price
            roi = (profit / price) * 100
            if roi < min_roi:
                continue
            opp = Opportunity(
                opp_type="high_prob_bond",
                title=m["title"],
                description=(
                    f"Buy {side} @ ${price:.4f} → pays $1.00 if correct\n"
                    f"Profit: ${profit:.4f} per share ({roi:.2f}% ROI)\n"
                    f"Platform: {m['platform'].title()}\n"
                    f"Volume 24h: ${m.get('volume_24h', 0):,.0f}"
                ),
                profit_pct=round(roi, 2),
                profit_amount=round(profit * 100, 2),
                total_cost=round(price, 4),
                platforms=[m["platform"]],
                legs=[{"platform": m["platform"], "side": side, "price": price}],
                urls=[m.get("url", "")],
                risk_level="low" if price >= 0.95 else "medium",
                hold_time=m.get("end_date", ""),
                category=m.get("category", ""),
                market_slug=m.get("slug", ""),
                condition_id=m.get("condition_id", ""),
                token_ids=m.get("clob_token_ids", []),
            )
            opportunities.append(opp)
    # Sort by ROI descending
    opportunities.sort(key=lambda o: o.profit_pct, reverse=True)
    # Limit to top 20 to avoid spam
    opportunities = opportunities[:20]
    if opportunities:
        logger.info(f"Found {len(opportunities)} high-probability bond opportunities")
    return opportunities
# =========================================================================
# STRATEGY 3: Price Discrepancy Alerts (Same platform, mispriced)
# =========================================================================
def find_mispriced_markets(
    markets: list[dict],
    cfg: dict,
) -> list[Opportunity]:
    """
    Find markets where YES + NO prices don't add up to ~$1.00.
    If YES + NO < $0.98, there might be an arb or the market is thin.
    If YES + NO > $1.02, the market is overpriced on both sides.
    """
    opportunities = []
    max_sum = cfg.get("mispricing", {}).get("max_sum", 0.98)
    for m in markets:
        if m.get("closed") or not m.get("active", True):
            continue
        yes_p = m["yes_price"]
        no_p = m["no_price"]
        if yes_p <= 0 or no_p <= 0:
            continue
        total = yes_p + no_p
        if total < max_sum and total > 0.50:  # Avoid empty/broken markets
            profit = 1.0 - total
            roi = (profit / total) * 100
            if roi < 0.3:
                continue
            opp = Opportunity(
                opp_type="intra_market_arb",
                title=m["title"],
                description=(
                    f"YES ({yes_p:.4f}) + NO ({no_p:.4f}) = {total:.4f} < $1.00\n"
                    f"Buy both → guaranteed ${profit:.4f} profit per pair\n"
                    f"Platform: {m['platform'].title()}"
                ),
                profit_pct=round(roi, 2),
                profit_amount=round(profit * 100, 2),
                total_cost=round(total, 4),
                platforms=[m["platform"]],
                legs=[
                    {"platform": m["platform"], "side": "YES", "price": yes_p},
                    {"platform": m["platform"], "side": "NO", "price": no_p},
                ],
                urls=[m.get("url", "")],
                risk_level="very_low",
                category=m.get("category", ""),
            )
            opportunities.append(opp)
    opportunities.sort(key=lambda o: o.profit_pct, reverse=True)
    return opportunities
# =========================================================================
# MASTER SCAN — Runs All Strategies
# =========================================================================
def run_full_cross_platform_scan(cfg: dict) -> tuple[list[Opportunity], list[dict]]:
    """
    Run all scanning strategies across all platforms.
    Returns (all_opportunities, poly_markets) tuple.
    """
    all_opportunities = []
    # --- Fetch data from all platforms ---
    logger.info("=" * 50)
    logger.info("Fetching markets from all platforms...")
    logger.info("=" * 50)
    poly_markets = fetch_polymarket_markets(cfg)
    kalshi_markets = fetch_kalshi_markets(cfg)
    all_markets = poly_markets + kalshi_markets
    logger.info(f"Total markets across all platforms: {len(all_markets)}")
    # --- Strategy 1: Cross-Platform Arbitrage ---
    logger.info("Scanning for cross-platform arbitrage...")
    cross_arbs = find_cross_platform_arbs(poly_markets, kalshi_markets, cfg)
    all_opportunities.extend(cross_arbs)
    # --- Strategy 2: High-Probability Bonds ---
    logger.info("Scanning for high-probability bonds...")
    bonds = find_high_prob_bonds(all_markets, cfg)
    all_opportunities.extend(bonds)
    # --- Strategy 3: Intra-Market Mispricing ---
    logger.info("Scanning for intra-market mispricing...")
    mispriced = find_mispriced_markets(all_markets, cfg)
    all_opportunities.extend(mispriced)
    # --- Sort all by profit % ---
    all_opportunities.sort(key=lambda o: o.profit_pct, reverse=True)

    # --- Edge Scoring: Score every opportunity ---
    if compute_edge_score is not None:
        min_score = cfg.get("edge_scoring", {}).get("min_score", 0)
        # Build market lookup for liquidity data
        market_lookup = {}
        for m in all_markets:
            market_lookup[m["title"]] = m

        scored_count = 0
        for opp in all_opportunities:
            mdata = market_lookup.get(opp.title)
            opp.edge_score = compute_edge_score(opp, market_data=mdata, cfg=cfg)
            scored_count += 1

        # Filter by min_score if configured
        if min_score > 0:
            before = len(all_opportunities)
            all_opportunities = [o for o in all_opportunities if o.edge_score >= min_score]
            filtered_out = before - len(all_opportunities)
            if filtered_out:
                logger.info(f"Edge Score filter: removed {filtered_out} opps below {min_score}")

        # Re-sort by edge score (highest first)
        all_opportunities.sort(key=lambda o: o.edge_score, reverse=True)
        logger.info(f"Scored {scored_count} opportunities (min_score={min_score})")

    logger.info(f"{'=' * 50}")
    logger.info(
        f"SCAN COMPLETE | "
        f"Cross-platform arbs: {len(cross_arbs)} | "
        f"Bonds: {len(bonds)} | "
        f"Mispriced: {len(mispriced)} | "
        f"TOTAL: {len(all_opportunities)}"
    )
    logger.info(f"{'=' * 50}")
    return all_opportunities, poly_markets
