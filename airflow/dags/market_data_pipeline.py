"""
airflow/dags/market_data_pipeline.py
-------------------------------------
Apache Airflow DAG for orchestrating the DataFlow market data pipeline.

WHAT IS AIRFLOW:
- It's a workflow scheduler used by Airbnb, LinkedIn, NASA, banks
- You define tasks and their dependencies as a DAG (Directed Acyclic Graph)
- Airflow handles scheduling, retries, alerting, and monitoring
- Has a web UI to visualize and manage pipelines

HOW TO READ A DAG:
- Tasks run in the order defined by >> (dependency operator)
- check_market_open >> fetch_data >> quality_check >> update_cache
  means: first check, then fetch, then validate, then update

This DAG runs every weekday at 6PM (after US market close).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago


# ── DAG Default Arguments ─────────────────────────────────────────────────────
# These apply to all tasks unless overridden

default_args = {
    "owner": "dataflow",
    "depends_on_past": False,          # Don't wait for yesterday's run to succeed
    "email_on_failure": False,         # Set True with real email config
    "email_on_retry": False,
    "retries": 3,                       # Auto-retry failed tasks 3 times
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# ── Task Functions ─────────────────────────────────────────────────────────────

def check_market_hours(**context) -> bool:
    """
    Task 1: Check if today is a trading day.
    Skip the pipeline on weekends and US market holidays.

    In production you'd use pandas_market_calendars for this.
    """
    import pandas as pd

    today = datetime.now()

    # Skip weekends
    if today.weekday() >= 5:
        print(f"Today ({today.strftime('%A')}) is not a trading day. Skipping.")
        return False

    print(f"Market check passed — proceeding with ingestion")
    return True


def ingest_yahoo_finance(**context) -> dict:
    """
    Task 2: Pull OHLCV data from Yahoo Finance.
    Uses XCom to pass results to the next task.

    XCom (Cross-Communication) = Airflow's way for tasks to share data.
    """
    import yaml
    import sys
    sys.path.insert(0, "/opt/airflow/dataflow")

    from core.ingestion.yahoo_finance import YahooFinanceIngester

    with open("/opt/airflow/dataflow/config.yaml") as f:
        config = yaml.safe_load(f)

    tickers = config["ingestion"]["sources"]["yahoo_finance"]["tickers"]
    interval = config["ingestion"]["sources"]["yahoo_finance"]["interval"]

    ingester = YahooFinanceIngester(tickers=tickers, interval=interval)
    summary = ingester.run(period="5d")  # Last 5 days on daily runs

    print(f"Ingestion summary: {summary}")

    # Push to XCom so next task can access it
    context["ti"].xcom_push(key="ingestion_summary", value=summary)
    return summary


def run_quality_checks(**context) -> dict:
    """
    Task 3: Run data quality checks on freshly ingested data.
    Pulls today's data from DB and validates it.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/dataflow")

    from datetime import timezone
    from sqlalchemy import select
    import pandas as pd
    from core.storage.database import get_db_session, OHLCVRecord
    from core.quality.checker import OHLCVQualityChecker
    import uuid

    # Get ingestion summary from previous task
    ti = context["ti"]
    ingestion_summary = ti.xcom_pull(key="ingestion_summary", task_ids="ingest_market_data")

    # Fetch today's data for quality checking
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    with get_db_session() as db:
        records = db.execute(
            select(OHLCVRecord).where(OHLCVRecord.created_at >= cutoff)
        ).scalars().all()

    if not records:
        print("No new records to quality check")
        return {"passed": True, "checks": []}

    df = pd.DataFrame([{
        "ticker": r.ticker, "timestamp": r.timestamp,
        "open": r.open, "high": r.high, "low": r.low,
        "close": r.close, "volume": r.volume,
    } for r in records])

    checker = OHLCVQualityChecker()
    run_id = str(uuid.uuid4())
    results = checker.run_all_checks(df, source="yahoo_finance")
    checker.save_results(results, run_id=run_id, source="yahoo_finance")

    failed = [r.check_name for r in results if not r.passed]
    summary = {"passed": len(failed) == 0, "failed_checks": failed}

    if failed:
        print(f"⚠️ QUALITY CHECKS FAILED: {failed}")
        # In production: send Slack alert, PagerDuty, etc.
    else:
        print("✅ All quality checks passed")

    return summary


def invalidate_api_cache(**context):
    """
    Task 4: Clear Redis cache so API serves fresh data.
    After new data is loaded, old cached responses are stale.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/dataflow")

    from core.storage.cache import cache

    deleted = cache.delete_pattern("ohlcv:*")
    deleted += cache.delete_pattern("stats:*")
    print(f"Invalidated {deleted} cache keys")


# ── DAG Definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="market_data_pipeline",
    description="Daily OHLCV ingestion from Yahoo Finance",
    default_args=default_args,
    schedule_interval="0 18 * * 1-5",  # 6PM weekdays (cron expression)
    start_date=days_ago(1),
    catchup=False,       # Don't backfill missed runs
    max_active_runs=1,   # Only one instance at a time
    tags=["market-data", "production"],
) as dag:

    # ── Define Tasks ──────────────────────────────────────────────────────────

    t1_check_market = PythonOperator(
        task_id="check_market_hours",
        python_callable=check_market_hours,
    )

    t2_ingest = PythonOperator(
        task_id="ingest_market_data",
        python_callable=ingest_yahoo_finance,
        provide_context=True,
    )

    t3_quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
        provide_context=True,
    )

    t4_cache = PythonOperator(
        task_id="invalidate_cache",
        python_callable=invalidate_api_cache,
    )

    # ── Define Dependencies (the DAG shape) ───────────────────────────────────
    # Read as: check → ingest → quality → cache
    t1_check_market >> t2_ingest >> t3_quality >> t4_cache
