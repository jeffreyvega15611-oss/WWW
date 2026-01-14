#!/usr/bin/env python3
"""
Multivariate Market Analysis for Kalshi Weather Data

For markets with multiple linked options (e.g., temperature ranges that sum to 100%),
this script detects arbitrage, volatility, and probability anomalies.

Perfect for: AI model training on probabilistic outcomes.
"""

import sqlite3
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import datetime

DB_PATH = "kalshi_weather.db"

def group_related_markets(series_ticker):
    """
    Group all markets by their series and identify "parent events."
    Example: All markets in KXHIGHNY (NYC High Temp) are siblings.
    
    Returns dict: {event_id: [list of related market tickers]}
    """
    conn = sqlite3.connect(DB_PATH)
    
    # Find all markets for this series
    query = f"""
    SELECT DISTINCT ticker, series_ticker
    FROM weather_markets
    WHERE series_ticker = '{series_ticker}'
    ORDER BY ticker
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Group by extracting common pattern (e.g., KXHIGHNY-* all belong to same event)
    related_groups = defaultdict(list)
    for ticker in df['ticker'].values:
        # Extract base name (before the last hyphen, which usually indicates the strike)
        parts = ticker.rsplit('-', 1)
        if len(parts) > 1:
            base_ticker = parts[0]
            related_groups[base_ticker].append(ticker)
        else:
            related_groups[ticker].append(ticker)
    
    return dict(related_groups)

def calculate_implied_probability_sum(event_tickers):
    """
    For a group of related markets (e.g., temp ranges), calculate if probabilities sum to ~100%.
    
    If sum > 110% = Market is "expensive" (arbitrage opportunity)
    If sum < 90% = Market is "cheap" (potential misprice)
    
    Perfect for: Detecting market inefficiencies.
    """
    conn = sqlite3.connect(DB_PATH)
    
    # Get the latest price for each market
    placeholders = ','.join('?' * len(event_tickers))
    query = f"""
    SELECT ticker, yes_price, timestamp
    FROM weather_markets
    WHERE ticker IN ({placeholders})
    ORDER BY timestamp DESC
    """
    
    df = pd.read_sql_query(query, conn, params=event_tickers)
    conn.close()
    
    if df.empty:
        return None
    
    # Get latest prices only
    latest_timestamp = df['timestamp'].max()
    latest_prices = df[df['timestamp'] == latest_timestamp]
    
    total_probability = latest_prices['yes_price'].sum()
    
    return {
        "event_tickers": event_tickers,
        "timestamp": latest_timestamp,
        "total_probability_sum": round(total_probability, 3),
        "implied_efficiency": "Fair" if 0.95 < total_probability < 1.05 else ("Expensive" if total_probability > 1.05 else "Cheap"),
        "market_prices": latest_prices[['ticker', 'yes_price']].to_dict('records')
    }

def detect_probability_anomalies(event_tickers, threshold=0.15):
    """
    Detect if one option is significantly mispriced relative to neighbors.
    Example: If TEMP-70-75 is 10% but TEMP-65-70 and TEMP-75-80 are both 40%,
    flag TEMP-70-75 as an outlier.
    
    Perfect for: Mean reversion strategies.
    """
    conn = sqlite3.connect(DB_PATH)
    
    placeholders = ','.join('?' * len(event_tickers))
    query = f"""
    SELECT ticker, yes_price, timestamp
    FROM weather_markets
    WHERE ticker IN ({placeholders})
    ORDER BY timestamp DESC
    LIMIT {len(event_tickers)}  -- Get latest only
    """
    
    df = pd.read_sql_query(query, conn, params=event_tickers)
    conn.close()
    
    if len(df) < 3:
        return None
    
    latest = df.nlargest(1, 'timestamp').iloc[0]
    prices = df[df['timestamp'] == latest['timestamp']]['yes_price'].values
    
    mean_price = np.mean(prices)
    std_price = np.std(prices)
    
    anomalies = []
    for idx, row in df[df['timestamp'] == latest['timestamp']].iterrows():
        z_score = (row['yes_price'] - mean_price) / (std_price + 0.001)  # Avoid division by zero
        if abs(z_score) > 1.5:  # Statistically significant outlier
            anomalies.append({
                "ticker": row['ticker'],
                "price": row['yes_price'],
                "z_score": round(z_score, 2),
                "anomaly_type": "UNUSUALLY_HIGH" if z_score > 0 else "UNUSUALLY_LOW"
            })
    
    return {
        "event_tickers": event_tickers,
        "mean_price": round(mean_price, 3),
        "std_dev": round(std_price, 3),
        "anomalies_detected": len(anomalies) > 0,
        "anomalies": anomalies
    }

if __name__ == "__main__":
    print("=" * 70)
    print("MULTIVARIATE MARKET ANALYSIS - AI-READY PROBABILITY DISTRIBUTION")
    print("=" * 70)
    
    # Example: Analyze NYC High Temperature markets
    series = "KXHIGHNY"  # Replace with actual series from your data
    
    print(f"\n1. Grouping related markets for series: {series}")
    related_markets = group_related_markets(series)
    print(f"   Found {len(related_markets)} event groups")
    
    # Analyze each group
    for i, (event_id, tickers) in enumerate(list(related_markets.items())[:3]):  # First 3 for demo
        if len(tickers) > 1:
            print(f"\n2. Event Group {i+1}: {event_id}")
            print(f"   Related tickers: {tickers}")
            
            # Check probability sum
            prob_sum = calculate_implied_probability_sum(tickers)
            if prob_sum:
                print(f"   ✓ Probability Sum: {prob_sum['total_probability_sum']:.1%}")
                print(f"   ✓ Market Status: {prob_sum['implied_efficiency']}")
                print(f"   ✓ Current Prices:")
                for market in prob_sum['market_prices']:
                    print(f"      - {market['ticker']}: {market['yes_price']:.2%}")
            
            # Check for anomalies
            anomalies = detect_probability_anomalies(tickers)
            if anomalies and anomalies['anomalies_detected']:
                print(f"   ⚠ ANOMALIES DETECTED:")
                for anom in anomalies['anomalies']:
                    print(f"      - {anom['ticker']}: {anom['anomaly_type']} (z={anom['z_score']})")
            else:
                print(f"   ✓ No significant anomalies detected")
    
    print("\n" + "=" * 70)
    print("Use this data to:")
    print("  1. Train ML models on probability distributions")
    print("  2. Detect market inefficiencies (arbitrage)")
    print("  3. Build volatility surfaces for forecasting")
    print("=" * 70)
