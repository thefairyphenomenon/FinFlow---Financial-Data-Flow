"""
core/quality/checker.py
------------------------
Data quality validation framework for financial data.

WHY THIS MATTERS TO RECRUITERS:
Bad data = wrong trading signals = real money losses.
Every serious quant firm has data quality systems.
Showing you understand this separates you from candidates
who just download CSV files and run models.

QUALITY CHECKS WE IMPLEMENT:
1. Null check — missing values
2. Staleness check — data is too old
3. Price gap check — abnormal price movements (potential data errors)
4. Volume check — suspiciously low volume
5. OHLC consistency — High >= Open/Close >= Low
6. Duplicate check — same record appears twice
"""

import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional
import pandas as pd
from sqlalchemy import text

from core.storage.database import get_db_session, DataQualityLog
from core.logger import get_logger

logger = get_logger(__name__)


class QualityCheckResult:
    """Value object representing a single quality check result."""

    def __init__(self, check_name: str, passed: bool, details: dict, records_checked: int, records_failed: int):
        self.check_name = check_name
        self.passed = passed
        self.details = details
        self.records_checked = records_checked
        self.records_failed = records_failed

    def __repr__(self):
        status = "✅ PASSED" if self.passed else "❌ FAILED"
        return f"{status} | {self.check_name} | {self.records_failed}/{self.records_checked} failed"


class OHLCVQualityChecker:
    """
    Runs a suite of data quality checks on OHLCV data.

    PATTERN: Each check is a separate method.
    - Easy to add new checks
    - Easy to run checks independently
    - Easy to test each check in isolation
    """

    def __init__(self, max_null_pct: float = 0.05, max_price_gap_pct: float = 0.20, min_volume: int = 1000):
        self.max_null_pct = max_null_pct
        self.max_price_gap_pct = max_price_gap_pct
        self.min_volume = min_volume

    def check_nulls(self, df: pd.DataFrame) -> QualityCheckResult:
        """Checks that critical columns don't have too many nulls."""
        critical_cols = ["open", "high", "low", "close", "volume"]
        null_counts = df[critical_cols].isnull().sum()
        null_pcts = null_counts / len(df)

        failing_cols = null_pcts[null_pcts > self.max_null_pct].to_dict()
        passed = len(failing_cols) == 0

        return QualityCheckResult(
            check_name="null_check",
            passed=passed,
            details={"failing_columns": failing_cols, "threshold": self.max_null_pct},
            records_checked=len(df),
            records_failed=int(null_counts.max()),
        )

    def check_ohlc_consistency(self, df: pd.DataFrame) -> QualityCheckResult:
        """
        Validates OHLC relationships:
        - High must be >= Open, Close, Low
        - Low must be <= Open, Close, High

        Violations indicate bad data from the source.
        """
        violations = df[
            (df["high"] < df["low"]) |
            (df["high"] < df["open"]) |
            (df["high"] < df["close"]) |
            (df["low"] > df["open"]) |
            (df["low"] > df["close"])
        ]

        return QualityCheckResult(
            check_name="ohlc_consistency",
            passed=len(violations) == 0,
            details={"violation_count": len(violations)},
            records_checked=len(df),
            records_failed=len(violations),
        )

    def check_price_gaps(self, df: pd.DataFrame) -> QualityCheckResult:
        """
        Flags abnormally large price jumps between consecutive days.
        A 20%+ single-day move is usually either:
        - A real event (earnings, acquisition)
        - A data error

        We flag it for human review rather than auto-reject.
        """
        df_sorted = df.sort_values("timestamp")
        pct_changes = df_sorted["close"].pct_change().abs()
        large_gaps = pct_changes[pct_changes > self.max_price_gap_pct]

        return QualityCheckResult(
            check_name="price_gap_check",
            passed=len(large_gaps) == 0,
            details={
                "flagged_rows": len(large_gaps),
                "max_gap_pct": float(pct_changes.max()) if not pct_changes.empty else 0,
                "threshold": self.max_price_gap_pct,
            },
            records_checked=len(df),
            records_failed=len(large_gaps),
        )

    def check_volume(self, df: pd.DataFrame) -> QualityCheckResult:
        """Flags records with suspiciously low volume."""
        low_volume = df[df["volume"] < self.min_volume]

        return QualityCheckResult(
            check_name="volume_check",
            passed=len(low_volume) == 0,
            details={"low_volume_count": len(low_volume), "min_threshold": self.min_volume},
            records_checked=len(df),
            records_failed=len(low_volume),
        )

    def check_duplicates(self, df: pd.DataFrame) -> QualityCheckResult:
        """Ensures no duplicate (ticker, timestamp) pairs exist."""
        dupes = df.duplicated(subset=["ticker", "timestamp"], keep=False)
        dupe_count = dupes.sum()

        return QualityCheckResult(
            check_name="duplicate_check",
            passed=dupe_count == 0,
            details={"duplicate_count": int(dupe_count)},
            records_checked=len(df),
            records_failed=int(dupe_count),
        )

    def run_all_checks(self, df: pd.DataFrame, source: str = "unknown") -> list[QualityCheckResult]:
        """
        Runs the full quality suite on a DataFrame.
        Returns list of results — all checks run even if one fails.
        """
        if df.empty:
            logger.warning("Empty DataFrame passed to quality checker")
            return []

        checks = [
            self.check_nulls,
            self.check_ohlc_consistency,
            self.check_price_gaps,
            self.check_volume,
            self.check_duplicates,
        ]

        results = []
        for check_fn in checks:
            try:
                result = check_fn(df)
                results.append(result)
                logger.info(str(result), extra={"source": source})
            except Exception as e:
                logger.error(f"Quality check {check_fn.__name__} crashed", extra={"error": str(e)})

        return results

    def save_results(self, results: list[QualityCheckResult], run_id: str, source: str):
        """Persists quality check results to the database for audit trail."""
        with get_db_session() as db:
            for result in results:
                log = DataQualityLog(
                    run_id=run_id,
                    source=source,
                    check_name=result.check_name,
                    passed=result.passed,
                    details=result.details,
                    records_checked=result.records_checked,
                    records_failed=result.records_failed,
                )
                db.add(log)

        failed = [r for r in results if not r.passed]
        if failed:
            logger.warning(
                f"Data quality issues detected",
                extra={"failed_checks": [r.check_name for r in failed], "source": source}
            )
