"""
core/storage/database.py
------------------------
Database layer using SQLAlchemy ORM + TimescaleDB hypertables.

KEY CONCEPTS FOR YOU TO LEARN:
1. SQLAlchemy ORM — Python classes map to database tables
2. TimescaleDB — PostgreSQL extension optimized for time-series data
   - Automatically partitions data by time (huge performance win)
   - Can compress old data automatically
3. Connection pooling — reuse DB connections instead of creating new ones
"""

from datetime import datetime
from sqlalchemy import (
    create_engine, Column, String, Float, BigInteger,
    DateTime, Text, Boolean, Index, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.dialects.postgresql import JSONB
from contextlib import contextmanager
from core.settings import settings
from core.logger import get_logger

logger = get_logger(__name__)
Base = declarative_base()


# ── ORM Models (Tables) ───────────────────────────────────────────────────────

class OHLCVRecord(Base):
    """
    OHLCV = Open, High, Low, Close, Volume
    The fundamental unit of financial market data.
    This becomes a TimescaleDB hypertable partitioned by timestamp.
    """
    __tablename__ = "ohlcv"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ticker = Column(String(20), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    adj_close = Column(Float)             # Adjusted for splits/dividends
    volume = Column(BigInteger)
    interval = Column(String(10))         # "1d", "1h", "5m"
    source = Column(String(50))           # "yahoo_finance", "alpha_vantage"
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    # Composite unique constraint: one record per ticker per timestamp per interval
    __table_args__ = (
        UniqueConstraint("ticker", "timestamp", "interval", name="uq_ohlcv_ticker_ts_interval"),
        # Index speeds up the most common query pattern
        Index("ix_ohlcv_ticker_timestamp", "ticker", "timestamp"),
    )


class MacroIndicator(Base):
    """
    Macroeconomic data from FRED (Federal Reserve Economic Data).
    Examples: GDP, Unemployment Rate, CPI, Fed Funds Rate
    """
    __tablename__ = "macro_indicators"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    series_id = Column(String(50), nullable=False)   # FRED series code e.g. "UNRATE"
    series_name = Column(String(200))
    timestamp = Column(DateTime(timezone=True), nullable=False)
    value = Column(Float, nullable=False)
    units = Column(String(100))
    source = Column(String(50), default="FRED")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("series_id", "timestamp", name="uq_macro_series_ts"),
        Index("ix_macro_series_timestamp", "series_id", "timestamp"),
    )


class DataQualityLog(Base):
    """
    Audit trail for data quality checks.
    Every pipeline run logs its quality results here.
    Recruiters LOVE seeing that you think about data quality.
    """
    __tablename__ = "data_quality_logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(String(100), nullable=False)       # Unique pipeline run ID
    source = Column(String(100), nullable=False)
    check_name = Column(String(200), nullable=False)
    passed = Column(Boolean, nullable=False)
    details = Column(JSONB)                            # Flexible JSON for check details
    records_checked = Column(BigInteger)
    records_failed = Column(BigInteger)
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow)


class PipelineRun(Base):
    """
    Metadata about each pipeline execution.
    Enables monitoring, alerting, and debugging.
    """
    __tablename__ = "pipeline_runs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(String(100), unique=True, nullable=False)
    dag_id = Column(String(200))
    status = Column(String(50))    # "running" | "success" | "failed"
    started_at = Column(DateTime(timezone=True))
    ended_at = Column(DateTime(timezone=True))
    records_ingested = Column(BigInteger, default=0)
    error_message = Column(Text)
    metadata = Column(JSONB)


# ── Engine & Session Factory ──────────────────────────────────────────────────

def create_db_engine():
    """
    Creates SQLAlchemy engine with connection pooling.
    pool_size=10: Keep 10 connections alive (reused, not recreated)
    max_overflow=20: Allow 20 extra connections under heavy load
    """
    return create_engine(
        settings.database_url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,  # Check connection health before using it
        echo=(settings.app_env == "development"),  # Log SQL in dev mode
    )


engine = create_db_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def get_db_session() -> Session:
    """
    Context manager for database sessions.
    Automatically handles commit/rollback/close.

    Usage:
        with get_db_session() as db:
            db.add(record)
            # auto-committed on exit, rolled back on exception
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error, rolling back", extra={"error": str(e)})
        raise
    finally:
        session.close()


def init_database():
    """
    Creates all tables and sets up TimescaleDB hypertables.
    Run this once on first deployment.
    """
    logger.info("Initializing database schema...")
    Base.metadata.create_all(bind=engine)

    # Convert OHLCV table to TimescaleDB hypertable
    # This partitions data by time automatically — massive query speedup
    with engine.connect() as conn:
        try:
            conn.execute("""
                SELECT create_hypertable(
                    'ohlcv', 'timestamp',
                    if_not_exists => TRUE,
                    chunk_time_interval => INTERVAL '1 month'
                );
            """)
            # Enable compression on data older than 6 months
            conn.execute("""
                ALTER TABLE ohlcv SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'ticker'
                );
                SELECT add_compression_policy('ohlcv', INTERVAL '6 months');
            """)
            conn.commit()
            logger.info("TimescaleDB hypertable configured for ohlcv")
        except Exception as e:
            # TimescaleDB extension might not be installed (falls back to plain Postgres)
            logger.warning(f"TimescaleDB setup skipped (using plain PostgreSQL): {e}")

    logger.info("Database initialization complete")
