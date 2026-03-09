"""
api/routers/market_data.py
--------------------------
REST endpoints for market (OHLCV) data.

WHAT YOU LEARN HERE:
- FastAPI path params, query params, response models
- Cache-aside pattern in a real endpoint
- SQLAlchemy queries with filters
- Proper API response structure
"""

from datetime import date, datetime, timezone
from typing import Optional
import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from core.storage.database import get_db_session, OHLCVRecord
from core.storage.cache import cache, make_cache_key
from core.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


# ── Response Schemas ──────────────────────────────────────────────────────────
# Pydantic models define the API contract — what the client receives

class OHLCVBar(BaseModel):
    """A single OHLCV candlestick bar."""
    ticker: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    adj_close: Optional[float]
    volume: int
    interval: str

    class Config:
        from_attributes = True  # Allows ORM model → Pydantic conversion


class MarketDataResponse(BaseModel):
    ticker: str
    interval: str
    count: int
    data: list[OHLCVBar]
    cached: bool = False


class TickerStatsResponse(BaseModel):
    ticker: str
    period_start: Optional[datetime]
    period_end: Optional[datetime]
    latest_close: Optional[float]
    period_high: Optional[float]
    period_low: Optional[float]
    avg_volume: Optional[float]
    total_return_pct: Optional[float]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/ohlcv/{ticker}", response_model=MarketDataResponse)
async def get_ohlcv(
    ticker: str,
    interval: str = Query(default="1d", description="Data interval: 1d, 1h, 5m"),
    start_date: Optional[date] = Query(default=None, description="Start date YYYY-MM-DD"),
    end_date: Optional[date] = Query(default=None, description="End date YYYY-MM-DD"),
    limit: int = Query(default=252, le=5000, description="Max records (252 = 1 trading year)"),
):
    """
    Returns OHLCV price data for a single ticker.

    **Example**: `/api/v1/market/ohlcv/AAPL?interval=1d&limit=30`
    """
    ticker = ticker.upper()

    # Try cache first
    cache_key = make_cache_key("ohlcv", ticker, interval, str(start_date), str(end_date), str(limit))
    cached_data = cache.get(cache_key)
    if cached_data:
        cached_data["cached"] = True
        return cached_data

    # Build query
    with get_db_session() as db:
        query = select(OHLCVRecord).where(
            and_(
                OHLCVRecord.ticker == ticker,
                OHLCVRecord.interval == interval,
            )
        )

        if start_date:
            query = query.where(OHLCVRecord.timestamp >= datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc))
        if end_date:
            query = query.where(OHLCVRecord.timestamp <= datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc))

        query = query.order_by(OHLCVRecord.timestamp.desc()).limit(limit)
        records = db.execute(query).scalars().all()

    if not records:
        raise HTTPException(status_code=404, detail=f"No data found for {ticker} ({interval})")

    bars = [OHLCVBar.from_orm(r) for r in records]
    # Return in chronological order
    bars.sort(key=lambda x: x.timestamp)

    response = MarketDataResponse(
        ticker=ticker,
        interval=interval,
        count=len(bars),
        data=bars,
    )

    # Cache for 1 hour (market data doesn't change intraday after close)
    cache.set(cache_key, response.model_dump(), ttl_seconds=3600)

    return response


@router.get("/stats/{ticker}", response_model=TickerStatsResponse)
async def get_ticker_stats(
    ticker: str,
    days: int = Query(default=252, description="Lookback period in calendar days"),
):
    """
    Returns summary statistics for a ticker over a lookback period.

    Useful for quick screening and portfolio monitoring.
    """
    ticker = ticker.upper()
    cache_key = make_cache_key("stats", ticker, str(days))

    cached = cache.get(cache_key)
    if cached:
        return cached

    with get_db_session() as db:
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        query = (
            select(OHLCVRecord)
            .where(OHLCVRecord.ticker == ticker)
            .where(OHLCVRecord.interval == "1d")
            .where(OHLCVRecord.timestamp >= cutoff)
            .order_by(OHLCVRecord.timestamp)
        )
        records = db.execute(query).scalars().all()

    if not records:
        raise HTTPException(status_code=404, detail=f"No data found for {ticker}")

    closes = [r.close for r in records]
    volumes = [r.volume for r in records if r.volume]

    total_return = ((closes[-1] - closes[0]) / closes[0] * 100) if len(closes) >= 2 else None

    response = TickerStatsResponse(
        ticker=ticker,
        period_start=records[0].timestamp,
        period_end=records[-1].timestamp,
        latest_close=closes[-1],
        period_high=max(r.high for r in records),
        period_low=min(r.low for r in records),
        avg_volume=sum(volumes) / len(volumes) if volumes else None,
        total_return_pct=round(total_return, 2) if total_return else None,
    )

    cache.set(cache_key, response.model_dump(), ttl_seconds=1800)
    return response


@router.get("/tickers", response_model=list[str])
async def list_available_tickers():
    """Lists all tickers available in the database."""
    with get_db_session() as db:
        from sqlalchemy import distinct
        result = db.execute(select(distinct(OHLCVRecord.ticker)).order_by(OHLCVRecord.ticker))
        tickers = [row[0] for row in result]
    return tickers
