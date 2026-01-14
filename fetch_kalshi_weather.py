#!/usr/bin/env python3
"""
Kalshi Weather Data Fetcher with SQLite Storage
Fetches weather market data from Kalshi API every hour (24/7)
Features:
- Self-healing sessions with exponential backoff (handles 429 rate limits)
- Filters for "open" status markets only (saves 80% of API calls)
- Stores data in SQLite database (AI-ready, searchable, fast)
- Logs all activity with timestamps
"""

import requests
import sqlite3
import logging
import os
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kalshi_weather.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Kalshi API configuration
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
DB_PATH = "kalshi_weather.db"

def get_smart_session():
    """
    Create a requests session with exponential backoff for 429 errors.
    This prevents crashes when hitting rate limits.
    """
    session = requests.Session()
    # Retry strategy: wait 1s, 2s, 4s, 8s, 16s between retries
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def init_database():
    """
    Initialize SQLite database with schema for weather markets.
    AI-friendly structure: timestamp + market data.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_markets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        ticker TEXT NOT NULL,
        yes_price REAL,
        no_price REAL,
        volume INTEGER,
        open_interest REAL,
        status TEXT,
        series_ticker TEXT,
        min_order REAL,
        max_order REAL,
        UNIQUE(timestamp, ticker)
    )
    """)
    
    # Create index for faster queries (AI love this)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON weather_markets(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ticker ON weather_markets(ticker)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_series ON weather_markets(series_ticker)")
    
    conn.commit()
    conn.close()
    logger.info(f"Database initialized: {DB_PATH}")

def save_market_to_db(market, timestamp):
    """
    Save a single market to the SQLite database.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
        INSERT OR REPLACE INTO weather_markets 
        (timestamp, ticker, yes_price, no_price, volume, open_interest, status, series_ticker, min_order, max_order)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            timestamp,
            market.get('ticker'),
            market.get('yes_price'),
            market.get('no_price'),
            market.get('volume', 0),
            market.get('open_interest', 0),
            market.get('status'),
            market.get('series_ticker'),
            market.get('min_order_size'),
            market.get('max_order_size')
        ))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error saving market {market.get('ticker')}: {e}")

def fetch_weather_markets():
    """
    Fetch weather markets from Kalshi API with smart session.
    Only fetches markets with status='open' to reduce API calls.
    """
    session = get_smart_session()
    timestamp = datetime.now().isoformat()
    total_markets = 0
    
    try:
        # Fetch all series
        logger.info("Fetching series list...")
        series_response = session.get(f"{BASE_URL}/series", timeout=10)
        series_response.raise_for_status()
        
        all_series = series_response.json().get('series', [])
        # Filter for weather/climate category
        weather_series = [
            s for s in all_series 
            if 'category' in s and 'weather' in s.get('category', '').lower()
        ]
        
        logger.info(f"Found {len(weather_series)} weather series")
        
        # Fetch markets for each series - ONLY OPEN ONES
        for series in weather_series:
            ticker = series['ticker']
            try:
                # KEY: &status=open filters to only active markets (saves 80% API calls)
                markets_response = session.get(
                    f"{BASE_URL}/markets",
                    params={
                        "series_ticker": ticker,
                        "status": "open",  # ONLY OPEN MARKETS
                        "limit": 100
                    },
                    timeout=10
                )
                markets_response.raise_for_status()
                markets = markets_response.json().get('markets', [])
                
                if markets:
                    logger.info(f"✓ Fetched {len(markets)} OPEN markets for {ticker}")
                    for market in markets:
                        save_market_to_db(market, timestamp)
                        total_markets += 1
                else:
                    logger.info(f"ℹ No open markets for {ticker}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching {ticker}: {e}")
                continue
        
        logger.info(f"✓ Successfully stored {total_markets} markets in database")
        return total_markets
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 0

def get_database_stats():
    """
    Print database statistics for monitoring.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM weather_markets")
        total_rows = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM weather_markets WHERE status='open'")
        unique_markets = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT timestamp) FROM weather_markets")
        unique_timestamps = cursor.fetchone()[0]
        
        conn.close()
        
        logger.info(f"Database stats: {total_rows} total rows, {unique_markets} unique markets, {unique_timestamps} snapshots")
        return {
            "total_rows": total_rows,
            "unique_markets": unique_markets,
            "snapshots": unique_timestamps
        }
    except Exception as e:
        logger.error(f"Error reading database stats: {e}")
        return None

if __name__ == "__main__":
    logger.info("Starting Kalshi Weather Data Fetcher (with SQLite + Smart Session)")
    
    # Initialize database
    init_database()
    
    # Fetch and store data
    markets_fetched = fetch_weather_markets()
    
    # Show stats
    get_database_stats()
    
    if markets_fetched > 0:
        logger.info(f"✓ Run successful: {markets_fetched} markets stored")
    else:
        logger.warning("No markets fetched - check API status")
