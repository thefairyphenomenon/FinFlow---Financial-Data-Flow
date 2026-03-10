"""
core/feature_engineering.py
---------------------------
Feature engineering layer for market OHLCV data.

WHY THIS EXISTS:
Raw OHLCV is useful, but most quant workflows need engineered features such as
returns, rolling volatility, RSI, and MACD before data can feed models.
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class FeatureConfig:
    """Configuration for the default feature set."""
    return_lag: int = 1
    volatility_window: int = 20
    rsi_window: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9


class MarketFeatureEngineer:
    """Builds model-friendly features from OHLCV price bars."""

    def __init__(self, config: Optional[FeatureConfig] = None):
        self.config = config or FeatureConfig()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns a new DataFrame with engineered features.

        Expected input columns:
        - ticker
        - timestamp
        - close
        - volume
        """
        if df.empty:
            return df.copy()

        required_cols = {"ticker", "timestamp", "close", "volume"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns for feature engineering: {sorted(missing)}")

        features = df.sort_values("timestamp").copy()

        features["return_1d"] = features["close"].pct_change(self.config.return_lag)
        features["log_return_1d"] = (features["close"] / features["close"].shift(self.config.return_lag)).apply(
            lambda x: np.nan if pd.isna(x) or x <= 0 else np.log(x)  # type: ignore[attr-defined]
        )
        features["rolling_volatility_20d"] = features["return_1d"].rolling(self.config.volatility_window).std()
        features["volume_change_1d"] = features["volume"].pct_change(self.config.return_lag)

        features["rsi_14"] = self._compute_rsi(features["close"], self.config.rsi_window)

        macd_line, signal_line, hist = self._compute_macd(
            features["close"],
            fast=self.config.macd_fast,
            slow=self.config.macd_slow,
            signal=self.config.macd_signal,
        )
        features["macd"] = macd_line
        features["macd_signal"] = signal_line
        features["macd_hist"] = hist

        return features

    @staticmethod
    def _compute_rsi(close: pd.Series, window: int) -> pd.Series:
        delta = close.diff()
        gains = delta.clip(lower=0)
        losses = -delta.clip(upper=0)

        avg_gain = gains.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
        avg_loss = losses.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()

        rs = avg_gain / avg_loss.replace(0, pd.NA)
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def _compute_macd(close: pd.Series, fast: int, slow: int, signal: int) -> tuple[pd.Series, pd.Series, pd.Series]:
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram
