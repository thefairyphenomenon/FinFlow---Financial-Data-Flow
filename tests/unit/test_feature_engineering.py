"""Unit tests for market feature engineering."""

import pandas as pd
import pytest

from core.feature_engineering import MarketFeatureEngineer


def test_feature_engineer_adds_expected_columns():
    dates = pd.date_range(start="2024-01-01", periods=60, freq="D", tz="UTC")
    df = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "timestamp": ts.to_pydatetime(),
                "close": 100.0 + idx,
                "volume": 1_000_000 + idx * 100,
            }
            for idx, ts in enumerate(dates, start=1)
        ]
    )

    features = MarketFeatureEngineer().transform(df)

    expected_columns = {
        "return_1d",
        "log_return_1d",
        "rolling_volatility_20d",
        "volume_change_1d",
        "rsi_14",
        "macd",
        "macd_signal",
        "macd_hist",
    }
    assert expected_columns.issubset(features.columns)
    assert len(features) == len(df)


def test_feature_engineer_raises_on_missing_columns():
    bad_df = pd.DataFrame([{"ticker": "AAPL", "close": 10.0}])

    with pytest.raises(ValueError, match="Missing required columns"):
        MarketFeatureEngineer().transform(bad_df)
