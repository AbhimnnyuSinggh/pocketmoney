"""
weather_arb/db.py
Asynchronous SQLite database wrapper for the Weather Arbitrage module.
"""
import aiosqlite
import logging
from datetime import datetime, timezone

from weather_arb.config import WEATHER_DB_PATH

logger = logging.getLogger("arb_bot.weather.db")

async def init_db():
    """Initialize the SQLite database with required schemas."""
    async with aiosqlite.connect(WEATHER_DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS station_bias (
                city TEXT,
                station TEXT,
                model TEXT,
                avg_error REAL DEFAULT 0,
                samples INTEGER DEFAULT 0,
                last_update TEXT,
                PRIMARY KEY (city, station, model)
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS resolved_history (
                market_slug TEXT PRIMARY KEY,
                city TEXT,
                resolved_bin TEXT,
                actual_high REAL,
                model_forecasts JSON,
                resolved_at TEXT
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                market_slug TEXT,
                outcome_bin TEXT,
                side TEXT,
                size_usdc REAL,
                entry_price REAL,
                exit_price REAL DEFAULT NULL,
                pnl_usdc REAL DEFAULT 0,
                mode TEXT,
                edge REAL,
                resolved BOOLEAN DEFAULT 0
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY,
                deployed REAL DEFAULT 0,
                pnl REAL DEFAULT 0,
                trades INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0
            )
        ''')
        await db.commit()
        logger.info(f"Weather database initialized at {WEATHER_DB_PATH}")

async def get_station_bias(city: str, station: str, model: str) -> float:
    """Fetch the current Exponentially Weighted Moving Average (EWMA) bias for a model/station."""
    async with aiosqlite.connect(WEATHER_DB_PATH) as db:
        async with db.execute(
            "SELECT avg_error FROM station_bias WHERE city=? AND station=? AND model=?", 
            (city, station, model)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0.0

async def update_station_bias(city: str, station: str, model: str, error: float):
    """Update the EWMA bias for a model/station after a market resolves."""
    async with aiosqlite.connect(WEATHER_DB_PATH) as db:
        async with db.execute(
            "SELECT avg_error, samples FROM station_bias WHERE city=? AND station=? AND model=?", 
            (city, station, model)
        ) as cursor:
            row = await cursor.fetchone()
        
        now_str = datetime.now(timezone.utc).isoformat()
        if row:
            old_avg, samples = row
            # EWMA update: 90% old, 10% new
            new_avg = old_avg * 0.9 + error * 0.1
            await db.execute(
                "UPDATE station_bias SET avg_error=?, samples=?, last_update=? WHERE city=? AND station=? AND model=?",
                (new_avg, samples + 1, now_str, city, station, model)
            )
        else:
            await db.execute(
                "INSERT INTO station_bias (city, station, model, avg_error, samples, last_update) VALUES (?, ?, ?, ?, ?, ?)",
                (city, station, model, error, 1, now_str)
            )
        await db.commit()

async def log_trade(market_slug: str, outcome_bin: str, side: str, size: float, price: float, mode: str, edge: float):
    """Log a new trade execution."""
    now_str = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(WEATHER_DB_PATH) as db:
        await db.execute(
            '''INSERT INTO trades 
               (timestamp, market_slug, outcome_bin, side, size_usdc, entry_price, mode, edge, resolved)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)''',
            (now_str, market_slug, outcome_bin, side, size, price, mode, edge)
        )
        await db.commit()
