"""
Tests for _fetch_raw_quotes() and fetch_quotes().

Fast tests use monkeypatched _req_get_json (no API calls).
Slow tests (marked @pytest.mark.api) hit the real Polygon API.
"""

import numpy as np
import pandas as pd
import pytest

from pdpolygonapi import PolygonApi


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_api():
    """PolygonApi instance with a dummy key — for monkeypatched tests only."""
    return PolygonApi(apikey="test_key_1234567890", cache=False)


def _make_tick_response(n_ticks=50, base_ask=605.0, base_bid=604.9, date="2025-03-03"):
    """Build a synthetic Polygon v3/quotes JSON response dict.

    Returns ``n_ticks`` ticks spread across a few seconds starting at 09:30 ET.
    """
    start_ns = int(
        pd.Timestamp(date + " 09:30:00", tz="US/Eastern").tz_convert("UTC").timestamp() * 1e9
    )
    step_ns = int(0.1e9)  # 100 ms between ticks

    results = []
    for i in range(n_ticks):
        results.append(
            {
                "sip_timestamp": start_ns + i * step_ns,
                "ask_price": round(base_ask + np.random.uniform(-0.05, 0.05), 2),
                "ask_size": int(np.random.randint(1, 100)),
                "bid_price": round(base_bid + np.random.uniform(-0.05, 0.05), 2),
                "bid_size": int(np.random.randint(1, 100)),
                "sequence_number": 1000 + i,
                "conditions": [1],
                "participant_timestamp": start_ns + i * step_ns - 1000,
            }
        )
    return {"status": "OK", "results": results, "request_id": "test123"}


# ---------------------------------------------------------------------------
# _fetch_raw_quotes tests (monkeypatched, no API)
# ---------------------------------------------------------------------------

class TestFetchRawQuotes:
    """Unit tests for the low-level _fetch_raw_quotes helper."""

    def test_returns_dataframe_with_correct_columns(self, mock_api, monkeypatch):
        resp = _make_tick_response(n_ticks=20)
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: resp)

        df = mock_api._fetch_raw_quotes("SPY", "2025-03-03")

        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["Ask", "AskSize", "Bid", "BidSize"]
        assert df.index.name == "Timestamp"
        assert len(df) == 20

    def test_index_is_utc(self, mock_api, monkeypatch):
        resp = _make_tick_response(n_ticks=5)
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: resp)

        df = mock_api._fetch_raw_quotes("SPY", "2025-03-03")

        assert df.index.tz is not None
        assert str(df.index.tz) == "UTC"

    def test_sorted_by_timestamp(self, mock_api, monkeypatch):
        resp = _make_tick_response(n_ticks=30)
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: resp)

        df = mock_api._fetch_raw_quotes("SPY", "2025-03-03")

        assert df.index.is_monotonic_increasing

    def test_no_extra_columns(self, mock_api, monkeypatch):
        """Polygon fields like conditions, sequence_number should be dropped."""
        resp = _make_tick_response(n_ticks=10)
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: resp)

        df = mock_api._fetch_raw_quotes("SPY", "2025-03-03")

        assert "sequence_number" not in df.columns
        assert "conditions" not in df.columns
        assert "participant_timestamp" not in df.columns
        assert "sip_timestamp" not in df.columns

    def test_empty_on_bad_status(self, mock_api, monkeypatch):
        monkeypatch.setattr(
            mock_api, "_req_get_json",
            lambda req: {"status": "ERROR", "error": "bad request"},
        )

        df = mock_api._fetch_raw_quotes("SPY", "2025-03-03")

        assert len(df) == 0
        assert list(df.columns) == ["Ask", "AskSize", "Bid", "BidSize"]

    def test_empty_on_no_results(self, mock_api, monkeypatch):
        monkeypatch.setattr(
            mock_api, "_req_get_json",
            lambda req: {"status": "OK", "results": []},
        )

        df = mock_api._fetch_raw_quotes("SPY", "2025-03-03")

        assert len(df) == 0

    def test_empty_on_missing_results_key(self, mock_api, monkeypatch):
        monkeypatch.setattr(
            mock_api, "_req_get_json",
            lambda req: {"status": "OK"},
        )

        df = mock_api._fetch_raw_quotes("SPY", "2025-03-03")

        assert len(df) == 0

    def test_empty_on_empty_response(self, mock_api, monkeypatch):
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: {})

        df = mock_api._fetch_raw_quotes("SPY", "2025-03-03")

        assert len(df) == 0

    def test_pagination(self, mock_api, monkeypatch):
        """Simulate two pages of results via next_url."""
        page1 = _make_tick_response(n_ticks=10)
        page1["next_url"] = "https://api.polygon.io/v3/quotes/SPY?cursor=abc"

        page2 = _make_tick_response(n_ticks=5)
        # page2 has no next_url → pagination stops

        pages = iter([page1, page2])
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: next(pages))

        df = mock_api._fetch_raw_quotes("SPY", "2025-03-03")

        assert len(df) == 15


# ---------------------------------------------------------------------------
# fetch_quotes tests (monkeypatched, no API)
# ---------------------------------------------------------------------------

class TestFetchQuotes:
    """Unit tests for the 1S-resampled fetch_quotes method."""

    def test_returns_expected_columns(self, mock_api, monkeypatch):
        resp = _make_tick_response(n_ticks=50)
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: resp)

        df = mock_api.fetch_quotes("SPY", "2025-03-03")

        expected_cols = [
            "Ask", "AsizeA", "AsizeM", "AsizeH", "AsizeL",
            "Bid", "BsizeA", "BsizeM", "BsizeH", "BsizeL", "Count",
        ]
        assert list(df.columns) == expected_cols

    def test_index_is_eastern_tz_naive(self, mock_api, monkeypatch):
        resp = _make_tick_response(n_ticks=50)
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: resp)

        df = mock_api.fetch_quotes("SPY", "2025-03-03")

        assert df.index.tz is None  # tz-naive (US/Eastern converted then localized to None)
        assert df.index.name == "Timestamp"

    def test_empty_returns_correct_columns(self, mock_api, monkeypatch):
        monkeypatch.setattr(
            mock_api, "_req_get_json",
            lambda req: {"status": "OK", "results": []},
        )

        df = mock_api.fetch_quotes("SPY", "2025-03-03")

        assert len(df) == 0
        assert "Ask" in df.columns
        assert "Count" in df.columns

    def test_count_is_tick_count(self, mock_api, monkeypatch):
        """Count column should equal number of ticks in each 1S bucket."""
        # 20 ticks at 100ms intervals → all within 2 seconds
        resp = _make_tick_response(n_ticks=20)
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: resp)

        df = mock_api.fetch_quotes("SPY", "2025-03-03")

        assert df["Count"].sum() == 20

    def test_size_columns_are_int(self, mock_api, monkeypatch):
        resp = _make_tick_response(n_ticks=30)
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: resp)

        df = mock_api.fetch_quotes("SPY", "2025-03-03")

        for col in ("AsizeA", "AsizeM", "AsizeH", "AsizeL",
                     "BsizeA", "BsizeM", "BsizeH", "BsizeL", "Count"):
            assert df[col].dtype in (np.int64, np.int32, int), f"{col} should be int"

    def test_ask_bid_rounded_to_cents(self, mock_api, monkeypatch):
        resp = _make_tick_response(n_ticks=30)
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: resp)

        df = mock_api.fetch_quotes("SPY", "2025-03-03")

        for col in ("Ask", "Bid"):
            # All values should have at most 2 decimal places
            assert (df[col].round(2) == df[col]).all(), f"{col} not rounded to cents"

    def test_size_high_gte_low(self, mock_api, monkeypatch):
        resp = _make_tick_response(n_ticks=50)
        monkeypatch.setattr(mock_api, "_req_get_json", lambda req: resp)

        df = mock_api.fetch_quotes("SPY", "2025-03-03")

        assert (df["AsizeH"] >= df["AsizeL"]).all()
        assert (df["BsizeH"] >= df["BsizeL"]).all()


# ---------------------------------------------------------------------------
# Slow API tests — only run when POLYGON_API env var is set
# ---------------------------------------------------------------------------

class TestFetchQuotesAPI:
    """Integration tests that hit the real Polygon API.

    Requires a Polygon subscription tier that includes the v3/quotes endpoint.
    Tests skip automatically when POLYGON_API is not set or the subscription
    does not authorize quote access.
    """

    @pytest.fixture
    def api(self):
        import os
        key = os.environ.get("POLYGON_API")
        if not key:
            pytest.skip("POLYGON_API not set")
        api = PolygonApi(envkey="POLYGON_API", cache=False)
        # Probe quotes endpoint with a known trading day
        df = api._fetch_raw_quotes("SPY", "2025-03-03")
        if len(df) == 0:
            status = api._last_response.get("status", "NO_RESPONSE") if api._last_response else "NO_RESPONSE"
            pytest.skip(f"Quotes endpoint unavailable (status={status})")
        return api

    def test_raw_quotes_spy(self, api):
        """Fetch raw ticks for SPY on a known trading day."""
        df = api._fetch_raw_quotes("SPY", "2025-03-03")

        assert len(df) > 0
        assert list(df.columns) == ["Ask", "AskSize", "Bid", "BidSize"]
        assert df.index.is_monotonic_increasing
        assert str(df.index.tz) == "UTC"
        # SPY should have at least thousands of NBBO updates per day
        assert len(df) > 1000

    def test_fetch_quotes_spy(self, api):
        """Fetch 1S-resampled quotes for SPY on a known trading day."""
        df = api.fetch_quotes("SPY", "2025-03-03")

        assert len(df) > 0
        expected_cols = [
            "Ask", "AsizeA", "AsizeM", "AsizeH", "AsizeL",
            "Bid", "BsizeA", "BsizeM", "BsizeH", "BsizeL", "Count",
        ]
        assert list(df.columns) == expected_cols
        assert df.index.tz is None
        # Regular market hours: 09:30 to 16:00 = 23400 seconds max
        assert len(df) <= 23400
        # SPY should have quotes for most seconds
        assert len(df) > 20000

    def test_fetch_quotes_no_data_date(self, api):
        """A weekend date should return empty DataFrame."""
        # 2025-03-01 is a Saturday
        df = api.fetch_quotes("SPY", "2025-03-01")

        assert len(df) == 0
