"""
sentiment_engine.py — Free Sentiment Engine for Market Intel

Aggregates news sentiment from FREE sources and matches to active markets:
  - Google News RSS feeds (no key required)
  - NewsAPI.org free tier (100 req/day, requires free API key)

Produces a sentiment score per market keyword that feeds into Edge Score.

Usage:
    engine = SentimentEngine(cfg)
    engine.refresh()  # Call every 5-10 minutes
    score = engine.get_sentiment("Bitcoin")  # Returns -100 to +100
"""
import time
import logging
import re
import xml.etree.ElementTree as ET
import requests
from collections import defaultdict

logger = logging.getLogger("arb_bot.sentiment")

# Sentiment keyword dictionaries
BULLISH_WORDS = {
    "surge", "soar", "rally", "jump", "gain", "rise", "boost", "record",
    "breakout", "bullish", "optimistic", "win", "success", "approved",
    "passed", "confirmed", "strong", "growth", "positive", "upgrade",
    "breakthrough", "milestone", "deal", "agreement", "victory",
    "momentum", "outperform", "beat", "exceed", "launch",
}

BEARISH_WORDS = {
    "crash", "plunge", "drop", "fall", "decline", "loss", "risk",
    "bearish", "fear", "concern", "fail", "reject", "ban", "lawsuit",
    "investigate", "fraud", "scandal", "collapse", "crisis", "warn",
    "threat", "hack", "exploit", "delay", "miss", "underperform",
    "negative", "recession", "default", "bankruptcy", "shutdown",
}


class SentimentEngine:
    """Aggregates free news sentiment and scores markets."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        sent_cfg = cfg.get("sentiment", {})
        self.enabled = sent_cfg.get("enabled", True)
        self.newsapi_key = sent_cfg.get("newsapi_key", "")
        self.refresh_interval = sent_cfg.get("refresh_interval", 600)  # 10 min

        # Cache: keyword → {"score": -100..100, "headlines": [...], "updated": ts}
        self._cache: dict[str, dict] = {}
        self._last_refresh = 0
        self._headlines: list[dict] = []  # Raw headlines

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def refresh(self):
        """Refresh sentiment data from all sources. Call periodically."""
        if not self.enabled:
            return

        now = time.time()
        if now - self._last_refresh < self.refresh_interval:
            return  # Still fresh

        headlines = []

        # Source 1: Google News RSS (always free, no key)
        try:
            google_headlines = self._fetch_google_news()
            headlines.extend(google_headlines)
        except Exception as e:
            logger.debug(f"Google News fetch error: {e}")

        # Source 2: NewsAPI (if key configured)
        if self.newsapi_key:
            try:
                newsapi_headlines = self._fetch_newsapi()
                headlines.extend(newsapi_headlines)
            except Exception as e:
                logger.debug(f"NewsAPI fetch error: {e}")

        self._headlines = headlines
        self._last_refresh = now

        if headlines:
            logger.info(f"Sentiment Engine refreshed: {len(headlines)} headlines")

    def get_sentiment(self, keyword: str) -> dict:
        """
        Get sentiment score for a keyword/market title.

        Returns:
            {"score": -100..100, "signal": "bullish"/"bearish"/"neutral",
             "headlines": int, "confidence": 0..100}
        """
        if not self._headlines:
            return {"score": 0, "signal": "neutral", "headlines": 0, "confidence": 0}

        # Check cache
        cache_key = keyword.lower().strip()
        cached = self._cache.get(cache_key)
        if cached and time.time() - cached.get("updated", 0) < self.refresh_interval:
            return cached

        # Score headlines matching this keyword
        keyword_lower = keyword.lower()
        keywords = self._extract_keywords(keyword_lower)

        matching = []
        bullish_count = 0
        bearish_count = 0

        for h in self._headlines:
            title = h.get("title", "").lower()

            # Check if headline matches any keywords
            if not any(kw in title for kw in keywords):
                continue

            matching.append(h)

            # Score the headline
            title_words = set(title.split())
            bull = len(title_words & BULLISH_WORDS)
            bear = len(title_words & BEARISH_WORDS)
            bullish_count += bull
            bearish_count += bear

        if not matching:
            result = {"score": 0, "signal": "neutral", "headlines": 0, "confidence": 0}
            self._cache[cache_key] = {**result, "updated": time.time()}
            return result

        # Calculate score (-100 to 100)
        total = bullish_count + bearish_count
        if total == 0:
            score = 0
        else:
            score = int(((bullish_count - bearish_count) / total) * 100)

        # Confidence based on number of matching headlines
        confidence = min(100, len(matching) * 15)

        # Signal classification
        if score >= 25:
            signal = "bullish"
        elif score <= -25:
            signal = "bearish"
        else:
            signal = "neutral"

        result = {
            "score": score,
            "signal": signal,
            "headlines": len(matching),
            "confidence": confidence,
        }

        self._cache[cache_key] = {**result, "updated": time.time()}
        return result

    def get_market_sentiment(self, title: str) -> dict:
        """Get sentiment for a market by its title."""
        return self.get_sentiment(title)

    # ------------------------------------------------------------------
    # Data Sources
    # ------------------------------------------------------------------
    def _fetch_google_news(self) -> list[dict]:
        """Fetch headlines from Google News RSS (free, no key)."""
        categories = [
            "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB",  # Business
            "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGRqTVhZU0FtVnVHZ0pWVXlnQVAB",  # Technology
            "https://news.google.com/rss/search?q=cryptocurrency+bitcoin",  # Crypto
            "https://news.google.com/rss/search?q=politics+election",      # Politics
        ]

        headlines = []
        for url in categories:
            try:
                resp = requests.get(url, timeout=8, headers={
                    "User-Agent": "Mozilla/5.0 (compatible; ArbitrageBot/1.0)"
                })
                if resp.status_code != 200:
                    continue

                root = ET.fromstring(resp.content)
                for item in root.findall(".//item")[:20]:
                    title_el = item.find("title")
                    pub_date = item.find("pubDate")

                    if title_el is not None and title_el.text:
                        headlines.append({
                            "title": title_el.text.strip(),
                            "source": "google_news",
                            "date": pub_date.text if pub_date is not None else "",
                        })
            except (ET.ParseError, requests.RequestException):
                continue

        return headlines

    def _fetch_newsapi(self) -> list[dict]:
        """Fetch headlines from NewsAPI.org (free tier: 100 req/day)."""
        if not self.newsapi_key:
            return []

        headlines = []
        try:
            resp = requests.get(
                "https://newsapi.org/v2/top-headlines",
                params={
                    "apiKey": self.newsapi_key,
                    "language": "en",
                    "pageSize": 50,
                    "category": "business",
                },
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                for article in data.get("articles", []):
                    title = article.get("title", "")
                    if title:
                        headlines.append({
                            "title": title,
                            "source": "newsapi",
                            "date": article.get("publishedAt", ""),
                        })
        except requests.RequestException:
            pass

        return headlines

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_keywords(text: str) -> list[str]:
        """Extract meaningful keywords from a market title."""
        # Remove common filler words
        stop_words = {
            "will", "the", "be", "to", "in", "on", "at", "by", "of", "a",
            "an", "is", "it", "or", "and", "for", "this", "that", "with",
            "from", "has", "have", "was", "were", "been", "being", "before",
            "after", "above", "below", "between", "than", "more", "less",
            "yes", "no", "market", "price",
        }
        words = re.findall(r'\b[a-z]{3,}\b', text)
        keywords = [w for w in words if w not in stop_words]
        # Also include bigrams for better matching
        bigrams = [f"{words[i]} {words[i+1]}" for i in range(len(words)-1)]
        return keywords[:8] + bigrams[:3]

    def format_sentiment_summary(self) -> str:
        """Format sentiment summary for Telegram display."""
        if not self._headlines:
            return (
                "📰 <b>SENTIMENT ENGINE</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "No headlines loaded yet.\n"
                "Sentiment data refreshes every 10 minutes."
            )

        source_counts = defaultdict(int)
        for h in self._headlines:
            source_counts[h["source"]] += 1

        msg = (
            f"📰 <b>SENTIMENT ENGINE</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Headlines loaded: <b>{len(self._headlines)}</b>\n"
        )
        for src, count in source_counts.items():
            label = "Google News" if src == "google_news" else "NewsAPI"
            msg += f"  {label}: {count}\n"

        msg += f"⏱ Updated: {time.strftime('%H:%M UTC', time.gmtime(self._last_refresh))}\n"

        # Show cached sentiment for top keywords
        if self._cache:
            msg += "\n<b>Active Sentiment:</b>\n"
            for kw, data in list(self._cache.items())[:5]:
                emoji = "📈" if data["score"] > 0 else "📉" if data["score"] < 0 else "➡️"
                msg += f"  {emoji} {kw}: {data['score']:+d} ({data['headlines']} headlines)\n"

        return msg
