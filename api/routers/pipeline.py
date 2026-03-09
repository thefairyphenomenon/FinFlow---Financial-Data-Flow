"""
api/routers/pipeline.py — Pipeline management endpoints
Trigger ingestion runs and monitor their status via API.
"""
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, desc

from core.storage.database import get_db_session, PipelineRun
from core.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


class PipelineRunResponse(BaseModel):
    run_id: str
    dag_id: Optional[str]
    status: str
    started_at: Optional[str]
    ended_at: Optional[str]
    records_ingested: Optional[int]
    error_message: Optional[str]

    class Config:
        from_attributes = True

from typing import Optional


def _run_yahoo_ingestion_bg():
    """Background task — runs ingestion without blocking the API response."""
    from core.ingestion.yahoo_finance import YahooFinanceIngester
    import yaml
    with open("config.yaml") as f:
        config = yaml.safe_load(f)
    tickers = config["ingestion"]["sources"]["yahoo_finance"]["tickers"]
    ingester = YahooFinanceIngester(tickers=tickers)
    ingester.run(period="1y")


def _run_fred_ingestion_bg():
    from core.ingestion.fred_ingester import FREDIngester
    FREDIngester().run()


@router.post("/trigger/market-data")
async def trigger_market_data_ingestion(background_tasks: BackgroundTasks):
    """
    Triggers a Yahoo Finance ingestion run asynchronously.
    Returns immediately — the run happens in the background.
    """
    background_tasks.add_task(_run_yahoo_ingestion_bg)
    logger.info("Market data ingestion triggered via API")
    return {"message": "Market data ingestion started", "status": "running"}


@router.post("/trigger/macro-data")
async def trigger_macro_data_ingestion(background_tasks: BackgroundTasks):
    """Triggers a FRED macro data ingestion run."""
    background_tasks.add_task(_run_fred_ingestion_bg)
    return {"message": "Macro data ingestion started", "status": "running"}


@router.get("/runs", response_model=list[PipelineRunResponse])
async def list_pipeline_runs(limit: int = 20):
    """Returns recent pipeline run history."""
    with get_db_session() as db:
        query = select(PipelineRun).order_by(desc(PipelineRun.started_at)).limit(limit)
        runs = db.execute(query).scalars().all()
    return [PipelineRunResponse.from_orm(r) for r in runs]


@router.get("/runs/{run_id}", response_model=PipelineRunResponse)
async def get_pipeline_run(run_id: str):
    """Returns details for a specific pipeline run."""
    with get_db_session() as db:
        run = db.execute(select(PipelineRun).where(PipelineRun.run_id == run_id)).scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return PipelineRunResponse.from_orm(run)
