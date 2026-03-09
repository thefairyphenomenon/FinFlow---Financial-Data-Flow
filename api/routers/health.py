"""
api/routers/health.py — System health endpoints
"""
from fastapi import APIRouter
from core.storage.cache import cache
from core.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.get("")
async def health_check():
    """Basic liveness probe — used by Docker/Kubernetes to check if app is up."""
    return {"status": "ok", "service": "dataflow-api"}


@router.get("/detailed")
async def detailed_health():
    """
    Checks all dependencies.
    Used by monitoring systems (Datadog, PagerDuty) to detect partial failures.
    """
    redis_ok = cache.health_check()

    # Check DB
    db_ok = False
    try:
        from core.storage.database import engine
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        db_ok = True
    except Exception:
        pass

    all_ok = redis_ok and db_ok
    return {
        "status": "healthy" if all_ok else "degraded",
        "dependencies": {
            "postgres": "ok" if db_ok else "unreachable",
            "redis": "ok" if redis_ok else "unreachable",
        }
    }
