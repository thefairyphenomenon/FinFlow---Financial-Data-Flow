"""
core/ingestion/fred_ingester.py
--------------------------------
Ingests macroeconomic data from the Federal Reserve (FRED).
FRED API is completely FREE — just register at fred.stlouisfed.org

WHY MACRO DATA MATTERS:
Quant funds use macro indicators as features in trading models.
e.g. "When unemployment rises, consumer stocks tend to fall"
Having a pipeline that ingests this + correlates with price data
is exactly what Two Sigma / Citadel engineers build.
"""

import requests
import uuid
from datetime import datetime, timezone
from typing import Optional
import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.settings import settings
from core.storage.database import get_db_session, MacroIndicator, PipelineRun
from core.logger import get_logger

logger = get_logger(__name__)

# Most important free FRED series for quant finance
FRED_SERIES_CATALOG = {
    "GDP": {"name": "Gross Domestic Product", "units": "Billions of Dollars"},
    "UNRATE": {"name": "Unemployment Rate", "units": "Percent"},
    "CPIAUCSL": {"name": "CPI - All Urban Consumers", "units": "Index 1982-84=100"},
    "FEDFUNDS": {"name": "Federal Funds Effective Rate", "units": "Percent"},
    "DGS10": {"name": "10-Year Treasury Yield", "units": "Percent"},
    "DGS2": {"name": "2-Year Treasury Yield", "units": "Percent"},
    "T10Y2Y": {"name": "10Y-2Y Yield Curve Spread", "units": "Percent"},  # Recession predictor!
    "VIXCLS": {"name": "CBOE Volatility Index (VIX)", "units": "Index"},
    "M2SL": {"name": "M2 Money Supply", "units": "Billions of Dollars"},
    "DEXUSEU": {"name": "USD/EUR Exchange Rate", "units": "USD per EUR"},
}


class FREDIngester:
    """
    Fetches macroeconomic data from the FRED API.

    The FRED API is remarkably well-documented and reliable.
    It's used by economists, central banks, and quant funds worldwide.
    """

    BASE_URL = "https://api.stlouisfed.org/fred"

    def __init__(self, series_ids: Optional[list[str]] = None):
        self.api_key = settings.fred_api_key
        self.series_ids = series_ids or list(FRED_SERIES_CATALOG.keys())

        if not self.api_key:
            logger.warning(
                "FRED_API_KEY not set. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"
            )

    def fetch_series(
        self,
        series_id: str,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetches a single FRED series.

        FRED API returns JSON like:
        {
            "observations": [
                {"date": "2024-01-01", "value": "3.7"},
                ...
            ]
        }
        """
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end

        url = f"{self.BASE_URL}/series/observations"

        logger.info(f"Fetching FRED series {series_id}")

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        observations = data.get("observations", [])

        if not observations:
            logger.warning(f"No observations returned for {series_id}")
            return pd.DataFrame()

        df = pd.DataFrame(observations)[["date", "value"]]

        # FRED uses "." for missing values — drop them
        df = df[df["value"] != "."].copy()
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["value"])

        logger.info(f"Fetched {len(df)} observations for {series_id}")
        return df

    def transform(self, df: pd.DataFrame, series_id: str) -> list[dict]:
        """Transforms FRED observations into database records."""
        if df.empty:
            return []

        catalog_entry = FRED_SERIES_CATALOG.get(series_id, {})
        records = []

        for _, row in df.iterrows():
            ts = row["date"]
            if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            records.append({
                "series_id": series_id,
                "series_name": catalog_entry.get("name", series_id),
                "timestamp": ts,
                "value": float(row["value"]),
                "units": catalog_entry.get("units", ""),
                "source": "FRED",
            })

        return records

    def load(self, records: list[dict]) -> int:
        """Bulk upsert into macro_indicators table."""
        if not records:
            return 0

        with get_db_session() as db:
            stmt = pg_insert(MacroIndicator).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["series_id", "timestamp"]
            )
            result = db.execute(stmt)
            return result.rowcount

    def run(self, observation_start: Optional[str] = None) -> dict:
        """Runs the full FRED ingestion pipeline."""
        if not self.api_key:
            return {"error": "FRED_API_KEY not configured"}

        run_id = str(uuid.uuid4())
        total_inserted = 0
        errors = []

        for series_id in self.series_ids:
            try:
                raw_df = self.fetch_series(series_id, observation_start=observation_start)
                records = self.transform(raw_df, series_id)
                inserted = self.load(records)
                total_inserted += inserted

            except Exception as e:
                errors.append({"series": series_id, "error": str(e)})
                logger.error(f"Failed to ingest FRED series {series_id}", extra={"error": str(e)})

        return {
            "run_id": run_id,
            "series_processed": len(self.series_ids) - len(errors),
            "records_inserted": total_inserted,
            "errors": errors,
        }
