"""
core/ingestion/yahoo_finance.py
-------------------------------
Ingests OHLCV market data from Yahoo Finance using yfinance.
yfinance is FREE — no API key needed.

WHAT YOU'LL LEARN HERE:
1. How to fetch real market data programmatically
2. How to handle rate limits gracefully
3. How to write idempotent ingestion (safe to re-run)
4. How to use bulk inserts for performance
"""

import time
import uuid
from datetime import datetime, timezone
from typing import Optional
import pandas as pd
import yfinance as yf
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.storage.database import get_db_session, OHLCVRecord, PipelineRun
from core.logger import get_logger

logger = get_logger(__name__)


class YahooFinanceIngester:
    """
    Fetches and stores market data from Yahoo Finance.

    DESIGN DECISION: Class-based ingester
    - Holds configuration state (tickers, interval)
    - Easy to mock in tests
    - Can be extended with rate limiting, retries, etc.
    """

    def __init__(self, tickers: list[str], interval: str = "1d"):
        self.tickers = tickers
        self.interval = interval
        self.source = "yahoo_finance"

    def fetch_raw(
        self,
        ticker: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        period: str = "1y",
    ) -> pd.DataFrame:
        """
        Fetches raw OHLCV data from Yahoo Finance.

        Args:
            ticker: Stock symbol e.g. "AAPL"
            start: Start date "YYYY-MM-DD" (optional)
            end: End date "YYYY-MM-DD" (optional)
            period: If no start/end, use period: "1d","5d","1mo","3mo","6mo","1y","2y","5y","max"

        Returns:
            DataFrame with OHLCV columns indexed by datetime
        """
        logger.info(f"Fetching {ticker} from Yahoo Finance", extra={"ticker": ticker, "interval": self.interval})

        try:
            stock = yf.Ticker(ticker)

            if start and end:
                df = stock.history(start=start, end=end, interval=self.interval)
            else:
                df = stock.history(period=period, interval=self.interval)

            if df.empty:
                logger.warning(f"No data returned for {ticker}")
                return pd.DataFrame()

            # Standardize column names to lowercase
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]
            df.index.name = "timestamp"
            df = df.reset_index()

            logger.info(f"Fetched {len(df)} rows for {ticker}")
            return df

        except Exception as e:
            logger.error(f"Failed to fetch {ticker}", extra={"error": str(e), "ticker": ticker})
            raise

    def transform(self, df: pd.DataFrame, ticker: str) -> list[dict]:
        """
        Transforms raw Yahoo Finance DataFrame into clean records
        ready for database insertion.

        This is your ETL "Transform" step.
        """
        if df.empty:
            return []

        records = []
        for _, row in df.iterrows():
            # Make timestamp timezone-aware (UTC) — always store in UTC!
            ts = row["timestamp"]
            if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            records.append({
                "ticker": ticker.upper(),
                "timestamp": ts,
                "open": float(row.get("open", 0)),
                "high": float(row.get("high", 0)),
                "low": float(row.get("low", 0)),
                "close": float(row.get("close", 0)),
                "adj_close": float(row.get("close", 0)),  # yfinance returns adjusted by default
                "volume": int(row.get("volume", 0)),
                "interval": self.interval,
                "source": self.source,
            })

        return records

    def load(self, records: list[dict]) -> int:
        """
        Bulk-inserts records into PostgreSQL.

        KEY PATTERN: "Upsert" (insert or update on conflict)
        - If record already exists (same ticker + timestamp + interval), skip it
        - This makes the pipeline IDEMPOTENT — safe to re-run without duplicates
        """
        if not records:
            return 0

        with get_db_session() as db:
            # PostgreSQL-specific bulk upsert
            # ON CONFLICT DO NOTHING = idempotent insert
            stmt = pg_insert(OHLCVRecord).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["ticker", "timestamp", "interval"]
            )
            result = db.execute(stmt)
            inserted = result.rowcount

        logger.info(f"Loaded {inserted} new records (skipped duplicates)")
        return inserted

    def run(self, period: str = "1y") -> dict:
        """
        Full ETL pipeline for all configured tickers.
        Returns a summary dict for monitoring.
        """
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        total_inserted = 0
        errors = []

        logger.info(f"Starting Yahoo Finance ingestion run", extra={"run_id": run_id, "tickers": self.tickers})

        for i, ticker in enumerate(self.tickers):
            try:
                # ── Extract ──
                raw_df = self.fetch_raw(ticker, period=period)

                # ── Transform ──
                records = self.transform(raw_df, ticker)

                # ── Load ──
                inserted = self.load(records)
                total_inserted += inserted

                # Rate limiting — be polite to Yahoo Finance
                # Without this you'll get blocked after ~100 requests
                if i < len(self.tickers) - 1:
                    time.sleep(0.5)  # 500ms between requests

            except Exception as e:
                errors.append({"ticker": ticker, "error": str(e)})
                logger.error(f"Pipeline failed for {ticker}", extra={"run_id": run_id, "error": str(e)})

        # Log pipeline run metadata
        self._save_run_record(
            run_id=run_id,
            started_at=started_at,
            records_ingested=total_inserted,
            errors=errors,
        )

        summary = {
            "run_id": run_id,
            "tickers_processed": len(self.tickers) - len(errors),
            "tickers_failed": len(errors),
            "records_inserted": total_inserted,
            "errors": errors,
        }
        logger.info("Ingestion run complete", extra=summary)
        return summary

    def _save_run_record(self, run_id: str, started_at: datetime, records_ingested: int, errors: list):
        """Persists pipeline run metadata for monitoring."""
        with get_db_session() as db:
            run = PipelineRun(
                run_id=run_id,
                dag_id="yahoo_finance_ingestion",
                status="failed" if errors else "success",
                started_at=started_at,
                ended_at=datetime.now(timezone.utc),
                records_ingested=records_ingested,
                error_message=str(errors) if errors else None,
            )
            db.add(run)
