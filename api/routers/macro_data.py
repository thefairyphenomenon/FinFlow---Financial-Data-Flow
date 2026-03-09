"""
api/routers/macro_data.py — Macroeconomic data endpoints
"""
from datetime import datetime, timezone, date
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, and_

from core.storage.database import get_db_session, MacroIndicator
from core.storage.cache import cache, make_cache_key
from core.ingestion.fred_ingester import FRED_SERIES_CATALOG

router = APIRouter()


class MacroDataPoint(BaseModel):
    series_id: str
    series_name: Optional[str]
    timestamp: datetime
    value: float
    units: Optional[str]

    class Config:
        from_attributes = True


@router.get("/series/{series_id}", response_model=list[MacroDataPoint])
async def get_macro_series(
    series_id: str,
    limit: int = Query(default=100, le=1000),
    start_date: Optional[date] = None,
):
    """
    Returns observations for a FRED macro series.

    **Available series**: GDP, UNRATE, CPIAUCSL, FEDFUNDS, DGS10, T10Y2Y, VIXCLS
    """
    series_id = series_id.upper()
    cache_key = make_cache_key("macro", series_id, str(start_date), str(limit))

    cached = cache.get(cache_key)
    if cached:
        return cached

    with get_db_session() as db:
        query = (
            select(MacroIndicator)
            .where(MacroIndicator.series_id == series_id)
            .order_by(MacroIndicator.timestamp.desc())
            .limit(limit)
        )
        if start_date:
            query = query.where(MacroIndicator.timestamp >= datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc))

        records = db.execute(query).scalars().all()

    if not records:
        raise HTTPException(status_code=404, detail=f"No data found for series {series_id}")

    result = [MacroDataPoint.from_orm(r) for r in records]
    result.sort(key=lambda x: x.timestamp)
    cache.set(cache_key, [r.model_dump() for r in result], ttl_seconds=7200)
    return result


@router.get("/catalog")
async def get_series_catalog():
    """Lists all available FRED series with descriptions."""
    return FRED_SERIES_CATALOG
