"""
api/routers/features.py
-----------------------
Endpoints for engineered market features used by analytics/ML consumers.
"""

from datetime import date, datetime, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, select

from core.feature_engineering import MarketFeatureEngineer
from core.storage.cache import cache, make_cache_key
from core.storage.database import OHLCVRecord, get_db_session

router = APIRouter()


class EngineeredFeatureRow(BaseModel):
    ticker: str
    timestamp: datetime
    close: float
    volume: int
    return_1d: Optional[float] = None
    log_return_1d: Optional[float] = None
    rolling_volatility_20d: Optional[float] = None
    volume_change_1d: Optional[float] = None
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_hist: Optional[float] = None


class FeatureResponse(BaseModel):
    ticker: str
    interval: str
    count: int
    features: list[EngineeredFeatureRow]
    cached: bool = False


@router.get("/{ticker}", response_model=FeatureResponse)
async def get_engineered_features(
    ticker: str,
    interval: str = Query(default="1d", description="Bar interval (1d, 1h, 5m)"),
    start_date: Optional[date] = Query(default=None, description="Start date YYYY-MM-DD"),
    end_date: Optional[date] = Query(default=None, description="End date YYYY-MM-DD"),
    limit: int = Query(default=252, le=5000, description="Maximum raw bars to use"),
):
    ticker = ticker.upper()
    cache_key = make_cache_key("features", ticker, interval, str(start_date), str(end_date), str(limit))

    cached_payload = cache.get(cache_key)
    if cached_payload:
        cached_payload["cached"] = True
        return cached_payload

    with get_db_session() as db:
        query = select(OHLCVRecord).where(
            and_(
                OHLCVRecord.ticker == ticker,
                OHLCVRecord.interval == interval,
            )
        )
        if start_date:
            query = query.where(
                OHLCVRecord.timestamp >= datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
            )
        if end_date:
            query = query.where(
                OHLCVRecord.timestamp <= datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)
            )

        rows = db.execute(query.order_by(OHLCVRecord.timestamp.desc()).limit(limit)).scalars().all()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No data found for {ticker} ({interval})")

    records = [
        {
            "ticker": row.ticker,
            "timestamp": row.timestamp,
            "close": row.close,
            "volume": int(row.volume or 0),
        }
        for row in rows
    ]

    feature_df = MarketFeatureEngineer().transform(pd.DataFrame(records))
    feature_df = feature_df.sort_values("timestamp")

    feature_rows = [EngineeredFeatureRow(**row) for row in feature_df.to_dict(orient="records")]

    payload = FeatureResponse(
        ticker=ticker,
        interval=interval,
        count=len(feature_rows),
        features=feature_rows,
    )

    cache.set(cache_key, payload.model_dump(mode="json"), ttl_seconds=1800)
    return payload
