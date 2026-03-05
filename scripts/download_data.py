"""
download_data.py - Download Historical Equity Data for KDB+ Project
===================================================================
Downloads daily OHLCV data from Yahoo Finance for 20 stocks across 6 sectors.

Usage:
    pip install yfinance pandas
    python scripts/download_data.py
"""
import os, sys
from datetime import datetime, timedelta

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("Install required packages: pip install yfinance pandas")
    sys.exit(1)

# Configuration
SYMBOLS = {
    "AAPL": "Technology", "MSFT": "Technology", "GOOG": "Technology",
    "AMZN": "Technology", "NVDA": "Technology", "META": "Technology",
    "TSLA": "Technology",
    "JPM": "Finance", "GS": "Finance", "BAC": "Finance",
    "JNJ": "Healthcare", "PFE": "Healthcare", "UNH": "Healthcare",
    "XOM": "Energy", "CVX": "Energy", "COP": "Energy",
    "WMT": "Consumer", "KO": "Consumer", "PG": "Consumer",
    "BA": "Industrial",
}

END_DATE = datetime.now().strftime("%Y-%m-%d")
START_DATE = (datetime.now() - timedelta(days=3*365)).strftime("%Y-%m-%d")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")


def download_symbol(symbol, start, end):
    """Download OHLCV data for one symbol."""
    try:
        df = yf.Ticker(symbol).history(start=start, end=end)
        if df.empty:
            return None
        df = df.reset_index()
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        df = df[["date", "open", "high", "low", "close", "volume"]]
        df.insert(0, "sym", symbol)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y.%m.%d")
        for col in ["open", "high", "low", "close"]:
            df[col] = df[col].round(2)
        df["volume"] = df["volume"].astype(int)
        return df
    except Exception as e:
        print(f"  ERROR: {symbol}: {e}")
        return None


def main():
    print("=" * 60)
    print("KDB+ Trading Analytics - Data Downloader")
    print(f"Symbols: {len(SYMBOLS)} | Range: {START_DATE} to {END_DATE}")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_data = []
    for i, (sym, sector) in enumerate(SYMBOLS.items(), 1):
        print(f"[{i:2d}/{len(SYMBOLS)}] {sym:5s} ({sector:12s})...", end=" ")
        df = download_symbol(sym, START_DATE, END_DATE)
        if df is not None:
            df.to_csv(os.path.join(OUTPUT_DIR, f"{sym}.csv"), index=False)
            print(f"OK ({len(df)} rows)")
            all_data.append(df)
        else:
            print("FAILED")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(os.path.join(OUTPUT_DIR, "all_daily.csv"), index=False)
        print(f"\nCombined: {len(combined)} rows -> data/raw/all_daily.csv")

    # Reference table
    ref = pd.DataFrame([{"sym": s, "sector": sec} for s, sec in SYMBOLS.items()])
    ref.to_csv(os.path.join(OUTPUT_DIR, "reference.csv"), index=False)

    print("\n" + "=" * 60)
    print("DONE! Next: cd to project root and run:")
    print("  q src/load_data.q")
    print("=" * 60)


if __name__ == "__main__":
    main()
