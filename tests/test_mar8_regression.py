"""
End-to-end regression tests for the Mar-8 2026 silent-corruption scenario.

The bug: on 2026-03-08 the user's Polygon subscription lapsed; pdpolygonapi
silently produced truncated DataFrames and (worse) wrote *partial* cache
files mid-pagination, contaminating ~56 files.

Layers 1+2+3+4 of the 2026-05-18 fix should make this impossible.  These
tests simulate the failure mode end-to-end and assert:

  (a) the fetcher RAISES PolygonApiError instead of returning truncated data,
  (b) no cache file is written when the fetch fails mid-stream,
  (c) error_stats are correctly incremented.

All tests are pure unit tests — requests.get and time.sleep are monkeypatched.
"""

import logging

import pandas as pd
import pytest

from pdpolygonapi import PolygonApi, PolygonApiError


# ---------------------------------------------------------------------------
# Shared fake-HTTP plumbing
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


def _make_get(responses):
    """Return a fake requests.get that yields ``responses`` in sequence."""
    state = {"i": 0}

    def fake_get(req, timeout=None):
        i = state["i"]
        state["i"] += 1
        if i >= len(responses):
            raise AssertionError(
                f"fake_get called {i + 1} times; only {len(responses)} responses queued"
            )
        r = responses[i]
        body = r(req) if callable(r) else r
        return FakeResponse(body)

    fake_get.call_count = lambda: state["i"]
    return fake_get


@pytest.fixture
def mock_api(tmp_path):
    """PolygonApi instance with isolated cache dirs so tests don't collide."""
    api = PolygonApi(
        apikey="test_key_1234567890",
        cache=True,
        ohlcv_cache_dir=str(tmp_path / "ohlcv_cache"),
        quote_cache_dir=str(tmp_path / "quote_cache"),
    )
    api.logger.setLevel(logging.DEBUG)
    return api


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    import pdpolygonapi._pdpolygonapi_base as base
    monkeypatch.setattr(base.time, "sleep", lambda s: None)


# Helper: NOT_AUTHORIZED body matching the Mar-8 production response.
_NOT_AUTH = {
    "status": "NOT_AUTHORIZED",
    "request_id": "test",
    "message": "You are not entitled to this data.",
}


def _ticks_page(n, with_next=False):
    """Build a quotes-endpoint success page with ``n`` synthetic ticks."""
    base_ts = 1_741_012_200_000_000_000  # 2025-03-03 09:30:00 UTC in ns
    body = {
        "status": "OK",
        "results": [
            {
                "sip_timestamp": base_ts + i * 1_000_000_000,
                "ask_price": 100.0 + i * 0.01,
                "ask_size": 10,
                "bid_price": 99.0 + i * 0.01,
                "bid_size": 10,
            }
            for i in range(n)
        ],
    }
    if with_next:
        body["next_url"] = "https://api.polygon.io/v3/quotes/SPY?cursor=next"
    return body


# ---------------------------------------------------------------------------
# _fetch_raw_quotes: pagination breaks mid-stream
# ---------------------------------------------------------------------------

class TestRawQuotesPagination:
    """The exact Mar-8 scenario: first page returns OK + next_url; second page
    returns NOT_AUTHORIZED.  Old behavior: silent break, returned truncated df.
    New behavior: PolygonApiError raised."""

    def test_first_page_not_authorized_raises(self, mock_api, monkeypatch):
        import pdpolygonapi._pdpolygonapi_base as base
        # NOT_AUTHORIZED + one-shot retry also NOT_AUTHORIZED.
        fake_get = _make_get([_NOT_AUTH, _NOT_AUTH])
        monkeypatch.setattr(base.requests, "get", fake_get)

        with pytest.raises(PolygonApiError) as excinfo:
            mock_api._fetch_raw_quotes("SPY", "2025-03-03")
        assert excinfo.value.status == "NOT_AUTHORIZED"

    def test_pagination_breaks_mid_stream_raises(self, mock_api, monkeypatch):
        import pdpolygonapi._pdpolygonapi_base as base
        # Page 1: OK with next_url and some ticks.
        # Page 2: NOT_AUTHORIZED (the Mar-8 mid-pagination failure).
        # One-shot retry on page 2: also NOT_AUTHORIZED.
        fake_get = _make_get([
            _ticks_page(5, with_next=True),
            _NOT_AUTH,
            _NOT_AUTH,
        ])
        monkeypatch.setattr(base.requests, "get", fake_get)

        with pytest.raises(PolygonApiError) as excinfo:
            mock_api._fetch_raw_quotes("SPY", "2025-03-03")
        assert excinfo.value.status == "NOT_AUTHORIZED"
        # Must NOT return a truncated DataFrame.

    def test_pagination_succeeds_when_all_pages_ok(self, mock_api, monkeypatch):
        import pdpolygonapi._pdpolygonapi_base as base
        fake_get = _make_get([
            _ticks_page(3, with_next=True),
            _ticks_page(2, with_next=False),
        ])
        monkeypatch.setattr(base.requests, "get", fake_get)

        df = mock_api._fetch_raw_quotes("SPY", "2025-03-03")
        assert len(df) == 5  # 3 + 2, no silent truncation


# ---------------------------------------------------------------------------
# error_stats correctness
# ---------------------------------------------------------------------------

class TestErrorStatsOnMar8Scenario:

    def test_failed_fetch_increments_exceptions_raised(self, mock_api, monkeypatch):
        import pdpolygonapi._pdpolygonapi_base as base
        fake_get = _make_get([_NOT_AUTH, _NOT_AUTH])
        monkeypatch.setattr(base.requests, "get", fake_get)

        with pytest.raises(PolygonApiError):
            mock_api._fetch_raw_quotes("SPY", "2025-03-03")

        assert mock_api.error_stats["exceptions_raised"] == 1
        assert mock_api.error_stats["non_ok_responses"].get("NOT_AUTHORIZED", 0) >= 1
        assert mock_api.error_stats["retries_attempted"] == 1
        assert mock_api.error_stats["retries_succeeded"] == 0

    def test_successful_fetch_no_exception_counted(self, mock_api, monkeypatch):
        import pdpolygonapi._pdpolygonapi_base as base
        fake_get = _make_get([_ticks_page(3)])
        monkeypatch.setattr(base.requests, "get", fake_get)

        mock_api._fetch_raw_quotes("SPY", "2025-03-03")
        assert mock_api.error_stats["exceptions_raised"] == 0
        assert mock_api.error_stats["fetches_succeeded"] == 1


# ---------------------------------------------------------------------------
# Layer 3: cache atomicity — no partial cache file on failure
# ---------------------------------------------------------------------------

class TestNoPartialCacheOnFailure:
    """fetch_quote_ohlcdf with cache=True must NOT leave a partial cache
    file on disk if any day's fetch fails mid-month."""

    def test_no_cache_file_when_fetch_fails(self, mock_api, monkeypatch, tmp_path):
        import pdpolygonapi._pdpolygonapi_base as base

        # Build a sequence where the first day's fetch succeeds but a later
        # day raises NOT_AUTHORIZED (Mar-8-style subscription lapse).
        # March 2025 has many business days; we let the first succeed then
        # fail on the second.
        responses = [
            _ticks_page(5),    # day 1 page 1: OK, no next_url
            _NOT_AUTH,         # day 2 page 1: NOT_AUTHORIZED
            _NOT_AUTH,         # day 2 one-shot retry: also NOT_AUTHORIZED
        ]
        fake_get = _make_get(responses)
        monkeypatch.setattr(base.requests, "get", fake_get)

        with pytest.raises(PolygonApiError):
            mock_api.fetch_quote_ohlcdf(
                "SPY",
                start="2025-03-03",
                end="2025-03-05",  # 3 business days; fails on day 2
                cache=True,
            )

        # The Mar-8 contamination signature: NO partial cache file written.
        cache_files = list((tmp_path / "quote_cache").glob("*.csv.gz"))
        assert cache_files == [], f"unexpected partial cache files: {cache_files}"

        # And the stats record it.
        assert mock_api.error_stats["cache_writes"] == 0
        assert mock_api.error_stats["cache_writes_skipped_due_to_error"] >= 1

    def test_cache_written_on_full_success(self, mock_api, monkeypatch, tmp_path):
        import pdpolygonapi._pdpolygonapi_base as base
        # The cache is per-MONTH; fetch_quote_ohlcdf recursively fetches the
        # entire month (21 business days for March 2025) with cache=False.
        # Use an always-successful fake so every day's request gets a tick.
        def fake_get(req, timeout=None):
            return FakeResponse(_ticks_page(3))
        monkeypatch.setattr(base.requests, "get", fake_get)

        df = mock_api.fetch_quote_ohlcdf(
            "SPY",
            start="2025-03-03",
            end="2025-03-04",
            cache=True,
        )
        assert isinstance(df, pd.DataFrame)
        cache_files = list((tmp_path / "quote_cache").glob("*.csv.gz"))
        assert len(cache_files) >= 1
        assert mock_api.error_stats["cache_writes"] >= 1
        assert mock_api.error_stats["cache_writes_skipped_due_to_error"] == 0


# ---------------------------------------------------------------------------
# Rate-limit bound (free-tier subscription)
# ---------------------------------------------------------------------------

class TestRateLimitBoundEndToEnd:
    """Verify that the rate-limit retry is bounded — a permanently
    rate-limited endpoint must NOT loop forever."""

    def test_rate_limit_exhaustion_raises(self, mock_api, monkeypatch):
        import pdpolygonapi._pdpolygonapi_base as base
        rate_limit_body = {
            "status": "ERROR",
            "error": "You've exceeded the maximum requests per minute, "
                     "please upgrade your plan at ...",
        }
        # 6 consecutive rate-limit responses — Layer 1 should give up.
        fake_get = _make_get([rate_limit_body] * 6)
        monkeypatch.setattr(base.requests, "get", fake_get)

        with pytest.raises(PolygonApiError):
            mock_api._fetch_raw_quotes("SPY", "2025-03-03")
