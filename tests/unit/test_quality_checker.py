"""
tests/unit/test_quality_checker.py
------------------------------------
Unit tests for the OHLCV data quality checker.

HOW TO RUN: pytest tests/ -v

Tests are what make your code production-grade.
Recruiters at top firms look for test coverage.
"""

import pytest
import pandas as pd
from datetime import datetime, timezone

import sys
sys.path.insert(0, ".")

from core.quality.checker import OHLCVQualityChecker


@pytest.fixture
def valid_ohlcv_df():
    """A clean, valid OHLCV DataFrame for testing."""
    return pd.DataFrame([
        {"ticker": "AAPL", "timestamp": datetime(2024, 1, i, tzinfo=timezone.utc),
         "open": 180.0 + i, "high": 185.0 + i, "low": 178.0 + i,
         "close": 182.0 + i, "volume": 50_000_000}
        for i in range(1, 11)
    ])


@pytest.fixture
def checker():
    return OHLCVQualityChecker()


class TestNullCheck:
    def test_passes_clean_data(self, checker, valid_ohlcv_df):
        result = checker.check_nulls(valid_ohlcv_df)
        assert result.passed is True

    def test_fails_when_too_many_nulls(self, checker, valid_ohlcv_df):
        df = valid_ohlcv_df.copy()
        df.loc[:7, "close"] = None  # 80% nulls — above 5% threshold
        result = checker.check_nulls(df)
        assert result.passed is False


class TestOHLCConsistency:
    def test_passes_valid_ohlc(self, checker, valid_ohlcv_df):
        result = checker.check_ohlc_consistency(valid_ohlcv_df)
        assert result.passed is True

    def test_fails_when_high_below_low(self, checker, valid_ohlcv_df):
        df = valid_ohlcv_df.copy()
        df.loc[0, "high"] = 170.0   # high < low — impossible
        df.loc[0, "low"] = 190.0
        result = checker.check_ohlc_consistency(df)
        assert result.passed is False
        assert result.records_failed >= 1


class TestPriceGapCheck:
    def test_passes_normal_moves(self, checker, valid_ohlcv_df):
        result = checker.check_price_gaps(valid_ohlcv_df)
        assert result.passed is True

    def test_flags_large_price_jump(self, checker, valid_ohlcv_df):
        df = valid_ohlcv_df.copy()
        df.loc[5, "close"] = 300.0  # ~60% jump — clearly anomalous
        result = checker.check_price_gaps(df)
        assert result.passed is False


class TestDuplicateCheck:
    def test_passes_no_duplicates(self, checker, valid_ohlcv_df):
        result = checker.check_duplicates(valid_ohlcv_df)
        assert result.passed is True

    def test_fails_with_duplicates(self, checker, valid_ohlcv_df):
        df = pd.concat([valid_ohlcv_df, valid_ohlcv_df.iloc[[0]]])  # Duplicate first row
        result = checker.check_duplicates(df)
        assert result.passed is False
