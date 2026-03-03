"""
Tests for fetch_quote_ohlcdf().

Fast tests use monkeypatched fetch_quotes (no API calls).
"""

import numpy as np
import pandas as pd
import pytest

from pdpolygonapi import PolygonApi


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_api():
    """PolygonApi instance with a dummy key — for monkeypatched tests only."""
    return PolygonApi(apikey="test_key_1234567890", cache=False)


def _make_1s_quote_data(date="2025-03-03", n_minutes=2, secs_per_min=3):
    """Create deterministic 1S quote data for one trading day.

    Mimics the output of fetch_quotes(): 11 columns with a tz-naive
    (US/Eastern) DatetimeIndex named 'Timestamp'.

    Bid prices start at 100.00 and increment by 0.01 per second within
    each minute, and by 0.10 per minute.  Ask = Bid + 0.20.
    Count = 10 + second_index within the minute.
    """
    timestamps = []
    bids = []
    asks = []
    counts = []

    base_ts = pd.Timestamp(f"{date} 09:30:00")
    for m in range(n_minutes):
        for s in range(secs_per_min):
            timestamps.append(base_ts + pd.Timedelta(minutes=m, seconds=s))
            bid = round(100.0 + m * 0.10 + s * 0.01, 2)
            bids.append(bid)
            asks.append(round(bid + 0.20, 2))
            counts.append(10 + s)

    n = len(timestamps)
    index = pd.DatetimeIndex(timestamps, name="Timestamp")
    return pd.DataFrame(
        {
            "Ask": asks,
            "AsizeA": [50] * n,
            "AsizeM": [50] * n,
            "AsizeH": [100] * n,
            "AsizeL": [10] * n,
            "Bid": bids,
            "BsizeA": [50] * n,
            "BsizeM": [50] * n,
            "BsizeH": [100] * n,
            "BsizeL": [10] * n,
            "Count": counts,
        },
        index=index,
    )


_1S_COLUMNS = [
    "Ask", "AsizeA", "AsizeM", "AsizeH", "AsizeL",
    "Bid", "BsizeA", "BsizeM", "BsizeH", "BsizeL", "Count",
]

_OHLC_COLUMNS = [
    "BidOpen", "BidHigh", "BidLow", "BidClose",
    "AskOpen", "AskHigh", "AskLow", "AskClose",
    "Mid", "Count",
]


def _empty_1s_df():
    """Empty DataFrame matching fetch_quotes() output shape."""
    return pd.DataFrame(
        columns=_1S_COLUMNS,
        index=pd.DatetimeIndex([], name="Timestamp"),
    )


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------


class TestFetchQuoteOhlcdfValidation:

    def test_bad_span(self, mock_api):
        with pytest.raises(ValueError, match="only supports span='minute'"):
            mock_api.fetch_quote_ohlcdf("SPY", span="hour")

    def test_bad_span_day(self, mock_api):
        with pytest.raises(ValueError, match="only supports span='minute'"):
            mock_api.fetch_quote_ohlcdf("SPY", span="day")

    def test_span_multiplier_not_int(self, mock_api):
        with pytest.raises(TypeError, match="must be an int"):
            mock_api.fetch_quote_ohlcdf("SPY", span_multiplier=1.5)

    def test_span_multiplier_too_low(self, mock_api):
        with pytest.raises(ValueError, match="must be >= 1 and <= 30"):
            mock_api.fetch_quote_ohlcdf("SPY", span_multiplier=0)

    def test_span_multiplier_too_high(self, mock_api):
        with pytest.raises(ValueError, match="must be >= 1 and <= 30"):
            mock_api.fetch_quote_ohlcdf("SPY", span_multiplier=31)

    def test_span_multiplier_negative(self, mock_api):
        with pytest.raises(ValueError, match="must be >= 1 and <= 30"):
            mock_api.fetch_quote_ohlcdf("SPY", span_multiplier=-1)

    def test_cache_true_raises(self, mock_api):
        with pytest.raises(NotImplementedError, match="not yet implemented"):
            mock_api.fetch_quote_ohlcdf("SPY", cache=True)


# ---------------------------------------------------------------------------
# Core functionality tests (monkeypatched)
# ---------------------------------------------------------------------------


class TestFetchQuoteOhlcdf:

    @pytest.fixture(autouse=True)
    def _patch_fetch_quotes(self, mock_api, monkeypatch):
        """Monkeypatch fetch_quotes to return deterministic 1S data."""
        self.api = mock_api
        monkeypatch.setattr(
            mock_api,
            "fetch_quotes",
            lambda ticker, str_date, show_request=False: _make_1s_quote_data(
                date=str_date, n_minutes=2, secs_per_min=3
            ),
        )

    def test_returns_correct_columns(self):
        df = self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03")
        assert list(df.columns) == _OHLC_COLUMNS

    def test_index_is_tz_naive(self):
        df = self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03")
        assert df.index.tz is None

    def test_index_name_is_ticker(self):
        df = self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03")
        assert df.index.name == "SPY"

    def test_row_count(self):
        """2 minutes of 1S data -> 2 one-minute bars."""
        df = self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03")
        assert len(df) == 2

    def test_ohlc_first_bar(self):
        """Verify OHLC values for the first minute bar.

        Minute 0: Bid = [100.00, 100.01, 100.02], Ask = [100.20, 100.21, 100.22]
        """
        df = self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03")
        bar = df.iloc[0]
        assert bar["BidOpen"] == 100.00
        assert bar["BidHigh"] == 100.02
        assert bar["BidLow"] == 100.00
        assert bar["BidClose"] == 100.02
        assert bar["AskOpen"] == 100.20
        assert bar["AskHigh"] == 100.22
        assert bar["AskLow"] == 100.20
        assert bar["AskClose"] == 100.22

    def test_ohlc_second_bar(self):
        """Verify OHLC values for the second minute bar.

        Minute 1: Bid = [100.10, 100.11, 100.12], Ask = [100.30, 100.31, 100.32]
        """
        df = self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03")
        bar = df.iloc[1]
        assert bar["BidOpen"] == 100.10
        assert bar["BidHigh"] == 100.12
        assert bar["BidLow"] == 100.10
        assert bar["BidClose"] == 100.12
        assert bar["AskOpen"] == 100.30
        assert bar["AskHigh"] == 100.32
        assert bar["AskLow"] == 100.30
        assert bar["AskClose"] == 100.32

    def test_mid_is_average_of_closes(self):
        df = self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03")
        # Bar 0: Mid = (100.02 + 100.22) / 2 = 100.12
        assert df.iloc[0]["Mid"] == 100.12
        # Bar 1: Mid = (100.12 + 100.32) / 2 = 100.22
        assert df.iloc[1]["Mid"] == 100.22

    def test_count_is_sum_of_raw_ticks(self):
        """Count should be the total raw ticks in the minute.

        Each minute has 3 seconds with counts [10, 11, 12] -> sum = 33.
        """
        df = self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03")
        assert df.iloc[0]["Count"] == 33
        assert df.iloc[1]["Count"] == 33

    def test_count_is_int(self):
        df = self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03")
        assert df["Count"].dtype in (np.int64, np.int32, int)

    def test_multi_day(self):
        """Two consecutive business days should produce 4 bars (2 per day)."""
        df = self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-04")
        assert len(df) == 4
        # First two bars from 2025-03-03, next two from 2025-03-04
        assert df.index[0].date() == pd.Timestamp("2025-03-03").date()
        assert df.index[2].date() == pd.Timestamp("2025-03-04").date()

    def test_empty_when_no_data(self, monkeypatch):
        """When fetch_quotes returns empty for all days, result is empty."""
        monkeypatch.setattr(
            self.api,
            "fetch_quotes",
            lambda ticker, str_date, show_request=False: _empty_1s_df(),
        )
        df = self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03")
        assert len(df) == 0
        assert list(df.columns) == _OHLC_COLUMNS


# ---------------------------------------------------------------------------
# Span multiplier tests
# ---------------------------------------------------------------------------


class TestFetchQuoteOhlcdfSpanMultiplier:

    @pytest.fixture(autouse=True)
    def _patch_fetch_quotes(self, mock_api, monkeypatch):
        """Monkeypatch with 4 minutes of data to test multi-minute resampling."""
        self.api = mock_api
        monkeypatch.setattr(
            mock_api,
            "fetch_quotes",
            lambda ticker, str_date, show_request=False: _make_1s_quote_data(
                date=str_date, n_minutes=4, secs_per_min=3
            ),
        )

    def test_span_multiplier_reduces_rows(self):
        """4 minutes of data with span_multiplier=2 -> 2 bars."""
        df = self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", span_multiplier=2
        )
        assert len(df) == 2

    def test_span_multiplier_ohlc_aggregation(self):
        """Verify OHLC values when two 1-minute bars are combined.

        Minute 0: BidO=100.00 BidH=100.02 BidL=100.00 BidC=100.02
                  AskO=100.20 AskH=100.22 AskL=100.20 AskC=100.22
        Minute 1: BidO=100.10 BidH=100.12 BidL=100.10 BidC=100.12
                  AskO=100.30 AskH=100.32 AskL=100.30 AskC=100.32
        Combined: BidO=first(100.00) BidH=max(100.02,100.12) BidL=min(100.00,100.10)
                  BidC=last(100.12) AskO=first(100.20) AskH=max(100.22,100.32)
                  AskL=min(100.20,100.30) AskC=last(100.32)
        """
        df = self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", span_multiplier=2
        )
        bar = df.iloc[0]
        assert bar["BidOpen"] == 100.00
        assert bar["BidHigh"] == 100.12
        assert bar["BidLow"] == 100.00
        assert bar["BidClose"] == 100.12
        assert bar["AskOpen"] == 100.20
        assert bar["AskHigh"] == 100.32
        assert bar["AskLow"] == 100.20
        assert bar["AskClose"] == 100.32

    def test_span_multiplier_count_summed(self):
        """Count should be the sum across combined bars."""
        df = self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", span_multiplier=2
        )
        # Each minute has count 33. Two combined = 66.
        assert df.iloc[0]["Count"] == 66

    def test_span_multiplier_mid_recalculated(self):
        """Mid should be recalculated from the multi-minute BidClose/AskClose."""
        df = self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", span_multiplier=2
        )
        bar = df.iloc[0]
        expected_mid = round((bar["BidClose"] + bar["AskClose"]) / 2, 2)
        assert bar["Mid"] == expected_mid

    def test_columns_unchanged(self):
        """Columns should be the same regardless of span_multiplier."""
        df = self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", span_multiplier=2
        )
        assert list(df.columns) == _OHLC_COLUMNS
