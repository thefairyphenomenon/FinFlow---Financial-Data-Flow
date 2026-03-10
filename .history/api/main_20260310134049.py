"""
api/main.py
-----------
DataFlow REST API built with FastAPI.

FastAPI is the industry standard for Python APIs because:
- Auto-generates OpenAPI (Swagger) docs at /docs
- Type-safe request/response validation via Pydantic
- Async-ready for high concurrency
- Used by Netflix, Uber, Microsoft
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routers import market_data, macro_data, pipeline, health, features
from core.logger import get_logger
from core.settings import settings

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs on startup and shutdown.
    Use for DB connections, loading ML models, etc.
    """
    logger.info("DataFlow API starting up", extra={"env": settings.app_env})
    yield
    logger.info("DataFlow API shutting down")


app = FastAPI(
    title="DataFlow — Institutional Financial Data API",
    description="""
    ## DataFlow API

    Clean, reliable financial data at your fingertips.

    ### Available Data
    - **Market Data**: OHLCV prices for equities (daily, hourly, minute)
    - **Macro Data**: Economic indicators from FRED (GDP, CPI, rates)
    - **Pipeline**: Trigger and monitor data ingestion runs
    - **Quality**: Data quality check results

    ### Authentication
    All endpoints require a Bearer token header:
    `Authorization: Bearer <your_token>`
    *(Demo mode: use any token)*
    """,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",      # Swagger UI
    redoc_url="/redoc",    # ReDoc UI (cleaner alternative)
)

# CORS — allows your frontend to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production: ["https://yourdomain.com"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers — each router handles a domain
app.include_router(health.router, prefix="/health", tags=["Health"])
app.include_router(market_data.router, prefix="/api/v1/market", tags=["Market Data"])
app.include_router(macro_data.router, prefix="/api/v1/macro", tags=["Macro Data"])
app.include_router(features.router, prefix="/api/v1/features", tags=["Feature Engineering"])
app.include_router(pipeline.router, prefix="/api/v1/pipeline", tags=["Pipeline"])


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Catch-all error handler — never leak stack traces to clients."""
    logger.error(f"Unhandled exception", extra={"error": str(exc), "path": str(request.url)})
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": "Check logs for details"},
    )


@app.get("/", include_in_schema=False)
async def root():
    return {
        "name": "DataFlow API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }
