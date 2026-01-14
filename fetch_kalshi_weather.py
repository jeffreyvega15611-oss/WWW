#!/usr/bin/env python3
"""
Kalshi Weather Data Fetcher
Fetches weather market data from Kalshi API every hour (24/7)
Stores data in CSV format with timestamps
"""

import requests
import json
import os
from datetime import datetime
import time
import logging

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
WEATHER_SERIES = ["KXHIGHNY", "KXLOWNY", "KXRAIN"]  # Common weather series

def fetch_weather_markets():
    """
    Fetch weather markets from Kalshi API
    Uses public endpoint - no authentication required
    """
    try:
        # Fetch all available weather series
        series_response = requests.get(
            f"{BASE_URL}/series",
            timeout=10
        )
        series_response.raise_for_status()
        
        # Filter for weather/climate category
        all_series = series_response.json().get('series', [])
        weather_series = [s for s in all_series if 'category' in s and 'weather' in s.get('category', '').lower()]
        
        if not weather_series:
            logger.warning("No weather series found, using defaults")
            weather_tickers = WEATHER_SERIES
        else:
            weather_tickers = [s['ticker'] for s in weather_series]
        
        # Fetch markets for each weather series
        all_markets = []
        for ticker in weather_tickers:
            try:
                markets_response = requests.get(
                    f"{BASE_URL}/markets",
                    params={"series_ticker": ticker, "limit": 100},
                    timeout=10
                )
                markets_response.raise_for_status()
                markets = markets_response.json().get('markets', [])
                all_markets.extend(markets)
                logger.info(f"Fetched {len(markets)} markets for series {ticker}")
            except requests.RequestException as e:
                logger.error(f"Error fetching markets for {ticker}: {e}")
                continue
        
        return all_markets
    
    except requests.RequestException as e:
        logger.error(f"Error fetching from Kalshi API: {e}")
        return []

def save_data(markets):
    """
    Save fetched market data to JSON file with timestamp
    """
    if not markets:
        logger.warning("No markets data to save")
        return
    
    timestamp = datetime.now().isoformat()
    filename = f"data/weather_markets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    try:
        with open(filename, 'w') as f:
            json.dump({
                'timestamp': timestamp,
                'markets': markets,
                'count': len(markets)
            }, f, indent=2)
        logger.info(f"Data saved to {filename}")
    except IOError as e:
        logger.error(f"Error saving data to file: {e}")

def continuous_fetch(interval_hours=1):
    """
    Run continuous fetching every N hours (24/7)
    """
    interval_seconds = interval_hours * 3600
    logger.info(f"Starting continuous weather data fetch every {interval_hours} hour(s)")
    
    try:
        while True:
            logger.info("Fetching weather market data...")
            markets = fetch_weather_markets()
            
            if markets:
                save_data(markets)
                logger.info(f"Successfully processed {len(markets)} weather markets")
            else:
                logger.warning("No markets data retrieved")
            
            logger.info(f"Waiting {interval_hours} hour(s) until next fetch...")
            time.sleep(interval_seconds)
    
    except KeyboardInterrupt:
        logger.info("Gracefully shutting down weather data fetcher")

if __name__ == "__main__":
    # Run once immediately
    logger.info("Starting Kalshi Weather Data Fetcher")
    markets = fetch_weather_markets()
    if markets:
        save_data(markets)
    
    # Then run continuously every hour
    continuous_fetch(interval_hours=1)
