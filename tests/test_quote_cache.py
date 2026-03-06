"""
Tests for fetch_quote_ohlcdf() file caching (Step 4).

Fast tests — all API calls are monkeypatched.
"""

import pandas as pd
import pytest

from pdpolygonapi import PolygonApi


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_OHLC_COLUMNS = [
    "BidOpen", "BidHigh", "BidLow", "BidClose",
    "AskOpen", "AskHigh", "AskLow", "AskClose",
    "Mid", "Count",
]


def _make_1s_quote_data(date="2025-03-03", n_minutes=2, secs_per_min=3):
    """Create deterministic 1S quote data for one trading day.

    Mimics the output of fetch_quotes().
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_cached_files():
    """Ensure class-level cached_files is clean between tests."""
    PolygonApi.cached_files = dict()
    yield
    PolygonApi.cached_files = dict()


@pytest.fixture
def api_with_cache(tmp_path):
    """PolygonApi with quote_cache_dir pointing to tmp_path."""
    return PolygonApi(
        apikey="test_key_1234567890",
        cache=False,
        quote_cache_dir=tmp_path / "quote_cache",
    )


# ---------------------------------------------------------------------------
# Cache file creation
# ---------------------------------------------------------------------------


class TestQuoteCacheFileCreation:

    @pytest.fixture(autouse=True)
    def _setup(self, api_with_cache, monkeypatch):
        self.api = api_with_cache
        monkeypatch.setattr(
            self.api,
            "fetch_quotes",
            lambda ticker, str_date, show_request=False: _make_1s_quote_data(
                date=str_date, n_minutes=2, secs_per_min=3
            ),
        )

    def test_cache_file_created(self, tmp_path):
        """cache=True should create a .csv.gz file in the cache dir."""
        self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-05", cache=True
        )
        cache_dir = tmp_path / "quote_cache"
        cache_files = [f for f in cache_dir.iterdir() if not f.name.endswith(".lock")]
        assert len(cache_files) == 1
        assert cache_files[0].name == "SPY.minute.1.2025-03.csv.gz"

    def test_no_cache_file_without_cache(self, tmp_path):
        """cache=False should not create any cache files."""
        self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-05", cache=False
        )
        cache_dir = tmp_path / "quote_cache"
        assert not cache_dir.exists() or not any(
            f for f in cache_dir.iterdir() if not f.name.endswith(".lock")
        )

    def test_two_months_creates_two_files(self, tmp_path):
        """Date range spanning Feb-Mar should create 2 cache files."""
        self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-02-28", end="2025-03-03", cache=True
        )
        cache_dir = tmp_path / "quote_cache"
        cache_files = sorted(
            f.name for f in cache_dir.iterdir() if not f.name.endswith(".lock")
        )
        assert len(cache_files) == 2
        assert "SPY.minute.1.2025-02.csv.gz" in cache_files
        assert "SPY.minute.1.2025-03.csv.gz" in cache_files

    def test_cache_stores_1min_regardless_of_multiplier(self, tmp_path):
        """Cache file name should always contain .minute.1. (1-min bars)."""
        self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03",
            span_multiplier=5, cache=True,
        )
        cache_dir = tmp_path / "quote_cache"
        cache_files = [f for f in cache_dir.iterdir() if not f.name.endswith(".lock")]
        assert len(cache_files) == 1
        assert "minute.1." in cache_files[0].name


# ---------------------------------------------------------------------------
# Cache hit / miss
# ---------------------------------------------------------------------------


class TestQuoteCacheHit:

    @pytest.fixture(autouse=True)
    def _setup(self, api_with_cache, monkeypatch):
        self.api = api_with_cache
        self.fetch_quotes_call_count = 0

        def counting_fetch_quotes(ticker, str_date, show_request=False):
            self.fetch_quotes_call_count += 1
            return _make_1s_quote_data(date=str_date, n_minutes=2, secs_per_min=3)

        monkeypatch.setattr(self.api, "fetch_quotes", counting_fetch_quotes)

    def test_second_call_is_cache_hit(self):
        """Second call should not invoke fetch_quotes at all."""
        self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", cache=True
        )
        first_count = self.fetch_quotes_call_count
        assert first_count > 0

        self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", cache=True
        )
        assert self.fetch_quotes_call_count == first_count  # no new calls

    def test_cache_stats_miss(self):
        self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", cache=True
        )
        stats = self.api.get_fetch_quote_ohlcdf_cache_stats()
        assert stats["misses"] == 1

    def test_cache_stats_hit(self):
        # First call: miss (fills cache)
        self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", cache=True
        )
        # Second call: in-memory hit
        self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", cache=True
        )
        stats = self.api.get_fetch_quote_ohlcdf_cache_stats()
        assert stats["hits"] == 1

    def test_cache_stats_lookup(self):
        """Evict from in-memory cache to force a disk lookup."""
        self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", cache=True
        )
        # Evict in-memory entry
        cf = self.api._quote_cache_file("SPY", "2025-03")
        PolygonApi.cached_files.pop(cf, None)

        self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", cache=True
        )
        stats = self.api.get_fetch_quote_ohlcdf_cache_stats()
        assert stats["lookups"] == 1


# ---------------------------------------------------------------------------
# Data integrity
# ---------------------------------------------------------------------------


class TestQuoteCacheDataIntegrity:

    @pytest.fixture(autouse=True)
    def _setup(self, api_with_cache, monkeypatch):
        self.api = api_with_cache
        monkeypatch.setattr(
            self.api,
            "fetch_quotes",
            lambda ticker, str_date, show_request=False: _make_1s_quote_data(
                date=str_date, n_minutes=2, secs_per_min=3
            ),
        )

    def test_cached_matches_uncached(self):
        """Cached and uncached results should be identical."""
        df_uc = self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-05", cache=False
        )
        df_c = self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-05", cache=True
        )
        pd.testing.assert_frame_equal(df_uc, df_c)

    def test_span_multiplier_with_cache(self):
        """span_multiplier > 1 resamples from cached 1-min bars."""
        df = self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03",
            span_multiplier=2, cache=True,
        )
        assert list(df.columns) == _OHLC_COLUMNS
        # 2 one-minute bars resampled to 2-min -> 1 bar
        assert len(df) == 1

    def test_cached_round_trip_preserves_data(self):
        """Data read from cache file should match the original fetch."""
        # First call writes cache
        df_first = self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", cache=True
        )
        # Evict in-memory, force disk read on second call
        cf = self.api._quote_cache_file("SPY", "2025-03")
        PolygonApi.cached_files.pop(cf, None)

        df_second = self.api.fetch_quote_ohlcdf(
            "SPY", start="2025-03-03", end="2025-03-03", cache=True
        )
        pd.testing.assert_frame_equal(df_first, df_second)


# ---------------------------------------------------------------------------
# clear_quote_cache
# ---------------------------------------------------------------------------


class TestClearQuoteCache:

    @pytest.fixture(autouse=True)
    def _setup(self, api_with_cache, monkeypatch):
        self.api = api_with_cache
        monkeypatch.setattr(
            self.api,
            "fetch_quotes",
            lambda ticker, str_date, show_request=False: _make_1s_quote_data(
                date=str_date, n_minutes=2, secs_per_min=3
            ),
        )

    def test_clear_specific_ticker(self, tmp_path):
        """clear_quote_cache('SPY') removes only SPY files."""
        self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03", cache=True)
        self.api.fetch_quote_ohlcdf("AAPL", start="2025-03-03", end="2025-03-03", cache=True)

        cleared = self.api.clear_quote_cache("SPY")
        assert len(cleared) == 1
        assert "SPY" in cleared[0]

        cache_dir = tmp_path / "quote_cache"
        remaining = [f for f in cache_dir.iterdir() if not f.name.endswith(".lock")]
        assert len(remaining) == 1
        assert "AAPL" in remaining[0].name

    def test_clear_all(self, tmp_path):
        """clear_quote_cache('all') removes all cache files."""
        self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03", cache=True)
        self.api.fetch_quote_ohlcdf("AAPL", start="2025-03-03", end="2025-03-03", cache=True)

        cleared = self.api.clear_quote_cache("all")
        assert len(cleared) == 2

        cache_dir = tmp_path / "quote_cache"
        remaining = [f for f in cache_dir.iterdir() if not f.name.endswith(".lock")]
        assert len(remaining) == 0

    def test_clear_resets_stats(self):
        """clear_quote_cache should reset cache stats."""
        self.api.fetch_quote_ohlcdf("SPY", start="2025-03-03", end="2025-03-03", cache=True)
        assert self.api.fetch_quote_ohlcdf_cache_stats["misses"] > 0

        self.api.clear_quote_cache("all")
        stats = self.api.get_fetch_quote_ohlcdf_cache_stats()
        assert stats == {"hits": 0, "lookups": 0, "misses": 0}
