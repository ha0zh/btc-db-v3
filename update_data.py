#!/usr/bin/env python3
"""
BTC OHLC Data Updater
Fetches latest hourly candles from multiple API sources with fallback.
Designed to run via GitHub Actions every hour.

API Priority:
1. Bybit API (no geo-restrictions)
2. OKX API (no geo-restrictions)
3. CryptoCompare API (free, global)
"""

import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import os
import sys

# Configuration
CSV_FILE = 'BTC_OHLC_1h_gmt8_updated.csv'
LIMIT = 100  # Fetch last 100 candles to ensure overlap

def log(message):
    """Print timestamped log message"""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    print(f"[{timestamp}] {message}")

def get_latest_timestamp_from_csv(csv_file):
    """Get the latest timestamp from the existing CSV file"""
    try:
        df = pd.read_csv(csv_file, parse_dates=['timestamp'])
        latest_timestamp = df['timestamp'].max()
        log(f"✓ Current CSV latest timestamp: {latest_timestamp}")
        return latest_timestamp, df
    except FileNotFoundError:
        log(f"⚠ CSV file not found: {csv_file}")
        return None, None
    except Exception as e:
        log(f"❌ Error reading CSV: {e}")
        return None, None

def fetch_from_bybit(limit=100):
    """
    Fetch from Bybit API (no geo-restrictions)
    https://bybit-exchange.github.io/docs/v5/market/kline
    """
    url = "https://api.bybit.com/v5/market/kline"
    
    params = {
        'category': 'spot',
        'symbol': 'BTCUSDT',
        'interval': '60',  # 60 minutes = 1 hour
        'limit': limit
    }
    
    try:
        log("Trying Bybit API...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('retCode') != 0:
            log(f"⚠ Bybit API error: {data.get('retMsg')}")
            return None
        
        klines = data.get('result', {}).get('list', [])
        
        if not klines:
            log("⚠ No data returned from Bybit")
            return None
        
        # Bybit returns: [startTime, openPrice, highPrice, lowPrice, closePrice, volume, turnover]
        df = pd.DataFrame(klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume', 'turnover'
        ])
        
        # Convert timestamp to datetime (UTC)
        df['timestamp'] = pd.to_datetime(df['open_time'].astype(int), unit='ms', utc=True)
        
        # Convert to GMT+8 (Asia/Singapore timezone)
        df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Singapore').dt.tz_localize(None)
        
        # Select columns
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
        
        # Convert to float
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Sort by timestamp (Bybit returns newest first)
        df = df.sort_values('timestamp')
        df.set_index('timestamp', inplace=True)
        
        log(f"✓ Bybit: Fetched {len(df)} rows")
        log(f"  Date range: {df.index.min()} to {df.index.max()}")
        
        return df
        
    except Exception as e:
        log(f"⚠ Bybit failed: {e}")
        return None

def fetch_from_cryptocompare(limit=100):
    """
    Fetch from CryptoCompare API (free, no geo-restrictions)
    https://min-api.cryptocompare.com/
    """
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    
    params = {
        'fsym': 'BTC',
        'tsym': 'USDT',
        'limit': limit,
        'e': 'binance'  # Use Binance as exchange reference
    }
    
    try:
        log("Trying CryptoCompare API...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('Response') != 'Success':
            log(f"⚠ CryptoCompare error: {data.get('Message')}")
            return None
        
        klines = data.get('Data', {}).get('Data', [])
        
        if not klines:
            log("⚠ No data returned from CryptoCompare")
            return None
        
        df = pd.DataFrame(klines)
        
        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['time'], unit='s', utc=True)
        
        # Convert to GMT+8
        df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Singapore').dt.tz_localize(None)
        
        # Rename columns
        df = df.rename(columns={
            'open': 'open',
            'high': 'high', 
            'low': 'low',
            'close': 'close',
            'volumefrom': 'volume'
        })
        
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df.set_index('timestamp', inplace=True)
        df = df.sort_index()
        
        log(f"✓ CryptoCompare: Fetched {len(df)} rows")
        log(f"  Date range: {df.index.min()} to {df.index.max()}")
        
        return df
        
    except Exception as e:
        log(f"⚠ CryptoCompare failed: {e}")
        return None

def fetch_from_okx(limit=100):
    """
    Fetch from OKX API (no geo-restrictions for market data)
    https://www.okx.com/docs-v5/en/
    """
    url = "https://www.okx.com/api/v5/market/candles"
    
    params = {
        'instId': 'BTC-USDT',
        'bar': '1H',
        'limit': str(limit)
    }
    
    try:
        log("Trying OKX API...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('code') != '0':
            log(f"⚠ OKX error: {data.get('msg')}")
            return None
        
        klines = data.get('data', [])
        
        if not klines:
            log("⚠ No data returned from OKX")
            return None
        
        # OKX returns: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        df = pd.DataFrame(klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume', 
            'volCcy', 'volCcyQuote', 'confirm'
        ])
        
        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['open_time'].astype(int), unit='ms', utc=True)
        
        # Convert to GMT+8
        df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Singapore').dt.tz_localize(None)
        
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Sort (OKX returns newest first)
        df = df.sort_values('timestamp')
        df.set_index('timestamp', inplace=True)
        
        log(f"✓ OKX: Fetched {len(df)} rows")
        log(f"  Date range: {df.index.min()} to {df.index.max()}")
        
        return df
        
    except Exception as e:
        log(f"⚠ OKX failed: {e}")
        return None

def fetch_recent_klines(limit=100):
    """
    Try multiple APIs with fallback
    """
    log(f"Fetching {limit} most recent hourly candles...")
    
    # Try APIs in order of preference
    apis = [
        ("Bybit", fetch_from_bybit),
        ("OKX", fetch_from_okx),
        ("CryptoCompare", fetch_from_cryptocompare),
    ]
    
    for name, fetch_func in apis:
        df = fetch_func(limit)
        if df is not None and len(df) > 0:
            log(f"✓ Successfully fetched data from {name}")
            return df
    
    log("❌ All APIs failed")
    return None

def update_csv():
    """Main function to update CSV with latest data"""
    log("=" * 50)
    log("Starting BTC OHLC Data Update")
    log("=" * 50)
    
    # Get current CSV data
    latest_csv_time, df_existing = get_latest_timestamp_from_csv(CSV_FILE)
    
    if df_existing is None:
        log("❌ Cannot proceed without existing CSV file")
        return False
    
    # Ensure existing data has proper index
    if 'timestamp' in df_existing.columns:
        df_existing.set_index('timestamp', inplace=True)
    
    # Fetch recent data
    df_new = fetch_recent_klines(limit=LIMIT)
    
    if df_new is None or len(df_new) == 0:
        log("❌ Failed to fetch new data from any API")
        return False
    
    # Combine with existing data
    df_combined = pd.concat([df_existing, df_new])
    
    # Remove duplicates (keep last - API data is more recent)
    df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
    
    # Sort by timestamp
    df_combined.sort_index(inplace=True)
    
    # Check if we have new data
    new_rows = len(df_combined) - len(df_existing)
    
    if new_rows > 0:
        # Save updated data
        df_combined.to_csv(CSV_FILE)
        
        log(f"✓ CSV file updated successfully:")
        log(f"  • Total rows: {len(df_combined)}")
        log(f"  • Earliest timestamp: {df_combined.index.min()}")
        log(f"  • Latest timestamp: {df_combined.index.max()}")
        log(f"  • New rows added: {new_rows}")
        
        return True
    else:
        # Still save to update any corrected values
        df_combined.to_csv(CSV_FILE)
        log("✓ CSV updated (no new rows, but data refreshed)")
        log(f"  • Latest timestamp: {df_combined.index.max()}")
        return True

def main():
    try:
        success = update_csv()
        if success:
            log("✓ Data update completed successfully")
            sys.exit(0)
        else:
            log("❌ Data update failed")
            sys.exit(1)
    except Exception as e:
        log(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
