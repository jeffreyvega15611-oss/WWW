#!/usr/bin/env python3
"""
Query helper for Kalshi weather database.
Useful examples for analyzing the stored data.
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = "kalshi_weather.db"

def query_latest_prices(ticker=None, limit=10):
    """
    Get the latest prices for markets (optionally filtered by ticker).
    Perfect for: Quick price snapshots, price tracking.
    """
    conn = sqlite3.connect(DB_PATH)
    
    if ticker:
        query = f"""
        SELECT timestamp, ticker, yes_price, no_price, volume, open_interest
        FROM weather_markets
        WHERE ticker LIKE '%{ticker}%'
        ORDER BY timestamp DESC
        LIMIT {limit}
        """
    else:
        query = f"""
        SELECT timestamp, ticker, yes_price, no_price, volume, open_interest
        FROM weather_markets
        ORDER BY timestamp DESC
        LIMIT {limit}
        """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def query_price_history(ticker, days=7):
    """
    Get price history for a specific market over N days.
    Perfect for: Time-series analysis, trend detection, ML training.
    """
    conn = sqlite3.connect(DB_PATH)
    
    query = f"""
    SELECT timestamp, ticker, yes_price, no_price, volume
    FROM weather_markets
    WHERE ticker = '{ticker}'
    AND timestamp > datetime('now', '-{days} days')
    ORDER BY timestamp ASC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def query_average_prices_by_series(series_ticker):
    """
    Get average YES price by series (aggregated view).
    Perfect for: Comparing similar weather events, market consensus.
    """
    conn = sqlite3.connect(DB_PATH)
    
    query = f"""
    SELECT 
        series_ticker,
        COUNT(DISTINCT ticker) as num_markets,
        ROUND(AVG(yes_price), 2) as avg_yes_price,
        ROUND(AVG(volume), 0) as avg_volume,
        MAX(timestamp) as latest_update
    FROM weather_markets
    WHERE series_ticker = '{series_ticker}'
    GROUP BY series_ticker
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def export_to_csv(output_file="weather_data.csv", days=7):
    """
    Export all recent data to CSV for analysis in Excel/Jupyter.
    Perfect for: Data sharing, external analysis tools.
    """
    conn = sqlite3.connect(DB_PATH)
    
    query = f"""
    SELECT *
    FROM weather_markets
    WHERE timestamp > datetime('now', '-{days} days')
    ORDER BY timestamp DESC
    """
    
    df = pd.read_sql_query(query, conn)
    df.to_csv(output_file, index=False)
    conn.close()
    print(f"✓ Exported {len(df)} rows to {output_file}")

if __name__ == "__main__":
    print("=" * 60)
    print("KALSHI WEATHER DATA - QUERY EXAMPLES")
    print("=" * 60)
    
    # Example 1: Latest 5 markets
    print("\n1. Latest 5 market snapshots:")
    print(query_latest_prices(limit=5))
    
    # Example 2: Price history for first market
    print("\n2. Price history (last 7 days) - first market found:")
    recent = query_latest_prices(limit=1)
    if not recent.empty:
        ticker = recent.iloc[0]['ticker']
        print(f"   Ticker: {ticker}")
        history = query_price_history(ticker, days=7)
        print(history)
    
    # Example 3: Export to CSV
    print("\n3. Exporting to CSV...")
    export_to_csv("weather_data_export.csv", days=1)
    
    print("\n" + "=" * 60)
    print("For more complex queries, use pandas + SQL directly!")
    print("=" * 60)
