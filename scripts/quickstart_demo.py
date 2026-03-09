"""
scripts/quickstart_demo.py
---------------------------
Run this to test DataFlow WITHOUT needing Docker/PostgreSQL.
Uses SQLite locally and demonstrates the full ETL pipeline.

Run: python scripts/quickstart_demo.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# ── 1. Patch settings to use SQLite (no Postgres needed) ──────────────────────
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["LOG_LEVEL"] = "INFO"


def demo_yahoo_finance_fetch():
    """Demonstrates fetching data from Yahoo Finance."""
    print("\n" + "="*60)
    print("STEP 1: Fetching market data from Yahoo Finance (FREE)")
    print("="*60)

    import yfinance as yf

    tickers = ["AAPL", "MSFT", "SPY"]

    for ticker in tickers:
        stock = yf.Ticker(ticker)
        df = stock.history(period="5d", interval="1d")

        if not df.empty:
            latest = df.iloc[-1]
            print(f"\n{ticker}:")
            print(f"  Latest Close: ${latest['Close']:.2f}")
            print(f"  Volume:       {latest['Volume']:,}")
            print(f"  Date:         {df.index[-1].strftime('%Y-%m-%d')}")

    return df


def demo_quality_checks(df: pd.DataFrame):
    """Demonstrates the data quality checker."""
    print("\n" + "="*60)
    print("STEP 2: Running Data Quality Checks")
    print("="*60)

    # Prepare df in expected format
    df_clean = df.reset_index()
    df_clean.columns = [c.lower() for c in df_clean.columns]
    df_clean = df_clean.rename(columns={"date": "timestamp"})
    df_clean["ticker"] = "SPY"

    # Add a deliberate data quality issue for demo
    df_clean.loc[0, "high"] = df_clean.loc[0, "low"] - 1  # High < Low (impossible!)
    print("\n⚠️ Injected a bad row: High < Low (simulating dirty data)")

    from core.quality.checker import OHLCVQualityChecker
    checker = OHLCVQualityChecker()
    results = checker.run_all_checks(df_clean, source="demo")

    print("\nQuality Check Results:")
    for r in results:
        status = "✅" if r.passed else "❌"
        print(f"  {status} {r.check_name}: {r.records_failed}/{r.records_checked} failed")
        if not r.passed:
            print(f"     Details: {r.details}")


def demo_fred_catalog():
    """Shows FRED macro data catalog (no API key needed for this)."""
    print("\n" + "="*60)
    print("STEP 3: FRED Macro Data Catalog")
    print("="*60)

    from core.ingestion.fred_ingester import FRED_SERIES_CATALOG
    print("\nAvailable macroeconomic series:")
    for series_id, info in FRED_SERIES_CATALOG.items():
        print(f"  {series_id:12} → {info['name']}")

    print("\nTo fetch FRED data, add your free API key to .env:")
    print("  FRED_API_KEY=your_key_here")
    print("  Get it free at: https://fred.stlouisfed.org/docs/api/api_key.html")


def demo_api_structure():
    """Shows the API structure without running it."""
    print("\n" + "="*60)
    print("STEP 4: API Endpoints Available")
    print("="*60)
    print("""
  GET  /health                         → System health
  GET  /api/v1/market/tickers          → List all tickers
  GET  /api/v1/market/ohlcv/{ticker}   → OHLCV price data
  GET  /api/v1/market/stats/{ticker}   → Price statistics
  GET  /api/v1/macro/series/{id}       → Macro indicator data
  GET  /api/v1/macro/catalog           → Available FRED series
  POST /api/v1/pipeline/trigger/market-data  → Trigger ingestion
  GET  /api/v1/pipeline/runs           → Pipeline run history

  Start API: uvicorn api.main:app --reload
  Swagger UI: http://localhost:8000/docs
    """)


if __name__ == "__main__":
    print("🌊 DataFlow — Institutional Financial Data Pipeline")
    print("Quick Start Demo (No Docker required)\n")

    df = demo_yahoo_finance_fetch()
    demo_quality_checks(df)
    demo_fred_catalog()
    demo_api_structure()

    print("\n" + "="*60)
    print("✅ Demo complete!")
    print("Next steps:")
    print("  1. cp .env.example .env  # Configure your environment")
    print("  2. docker-compose up -d  # Start full stack")
    print("  3. python -c 'from core.storage.database import init_database; init_database()'")
    print("  4. uvicorn api.main:app --reload")
    print("  5. Visit http://localhost:8000/docs")
    print("="*60)
