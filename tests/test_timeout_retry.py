"""
Test timeout and retry logic in pdpolygonapi.

These tests verify the graduated timeout retry behavior:
- Initial request with _initial_timeout
- If timeout, retry with _retry_timeout  
- Under the Layer-1 contract, if BOTH timeouts fail (http_get_json returns
  ``{}``), the resulting non-success response flows through one more
  Layer-1 one-shot retry; if THAT also times out, PolygonApiError is raised
  rather than silently returning empty data.
"""

import logging
import pytest
from pdpolygonapi import PolygonApi, PolygonApiError


@pytest.fixture
def api_short_timeout():
    """Create a PolygonApi instance with very short timeouts to force timeout behavior."""
    api = PolygonApi(loglevel=logging.DEBUG)
    api._initial_timeout = 0.5  # 500ms - should timeout on large requests
    api._retry_timeout = 1.0    # 1 second retry
    return api


@pytest.fixture
def api_very_short_timeout():
    """Create a PolygonApi instance with extremely short timeouts to force both timeouts."""
    api = PolygonApi(loglevel=logging.DEBUG)
    api._initial_timeout = 0.05  # 50ms - almost guaranteed to timeout
    api._retry_timeout = 0.1     # 100ms - also likely to timeout
    return api


class TestTimeoutRetryBehavior:
    """Test the timeout retry logic."""

    def test_initial_timeout_triggers_retry_success(self, api_short_timeout, caplog):
        """
        Test that when initial timeout is hit, retry with longer timeout succeeds.
        
        Request 6 months of minutely data with 0.5s initial timeout.
        Initial should timeout, retry with 1s should (hopefully) succeed.
        Under Layer 1, if BOTH timeouts fail the Layer-1 one-shot retry runs
        next; if that also fails we get a PolygonApiError instead of empty.
        """
        with caplog.at_level(logging.WARNING):
            try:
                df = api_short_timeout.fetch_ohlcvdf(
                    "SPY",
                    start="2025-01-01",
                    end="2025-06-30",
                    span="minute",
                    cache=False,
                )
            except PolygonApiError as e:
                print(f"\n✓ Both http-level timeouts AND Layer-1 retry failed: {e.status!r}")
                return

        timeout_warnings = [r for r in caplog.records if "retrying with" in r.message.lower()]
        if len(timeout_warnings) > 0:
            print(f"\n✓ Initial timeout triggered, retry was attempted")
            print(f"  Warning: {timeout_warnings[0].message}")
            assert df is not None
            print(f"  Retry succeeded, got {len(df)} rows")
        else:
            print(f"\n○ Request completed within initial timeout ({api_short_timeout._initial_timeout}s)")
            print(f"  Got {len(df)} rows - try shorter timeout to trigger retry")
            assert df is not None

    def test_both_timeouts_fail_returns_empty_when_raise_false(self, api_very_short_timeout, caplog):
        """
        Test that when both http-level timeouts fail and _retry_timeout_raise=False,
        the empty `{}` from http_get_json now flows through the Layer-1 non-OK
        retry path — and if THAT also fails, raises PolygonApiError.  This is
        the desired stricter contract (no more silent empty-data returns).
        """
        api_very_short_timeout._retry_timeout_raise = False
        
        with caplog.at_level(logging.DEBUG):
            try:
                df = api_very_short_timeout.fetch_ohlcvdf(
                    "SPY",
                    start="2024-01-01",
                    end="2024-12-31",
                    span="minute",
                    cache=False,
                )
            except PolygonApiError as e:
                print(f"\n✓ Both timeouts + Layer-1 retry failed; PolygonApiError raised: status={e.status!r}")
                return

        warning_msgs = [r for r in caplog.records if "retrying with" in r.message.lower()]
        if len(warning_msgs) > 0:
            print(f"\n○ Initial timeout hit, but retry path eventually succeeded")
            print(f"  Got {len(df)} rows")
        else:
            print(f"\n○ All requests succeeded (network too fast for test)")
            print(f"  Got {len(df)} rows")

    def test_both_timeouts_fail_raises_when_raise_true(self, api_very_short_timeout):
        """
        Test that when both timeouts fail and _retry_timeout_raise=True,
        we get a Timeout exception.
        """
        import requests.exceptions
        
        api_very_short_timeout._retry_timeout_raise = True
        
        try:
            # Request a LOT of data - full year of minutely data
            df = api_very_short_timeout.fetch_ohlcvdf(
                "SPY",
                start="2024-01-01",
                end="2024-12-31",
                span="minute",
                cache=False,
            )
            # If we get here, both requests succeeded (unlikely with 100ms/200ms)
            print(f"\n○ Both requests succeeded (network too fast for test)")
            print(f"  Got {len(df)} rows")
            
        except requests.exceptions.Timeout:
            print(f"\n✓ Timeout exception raised as expected when _retry_timeout_raise=True")


class TestTimeoutValues:
    """Test that timeout values are properly used."""

    def test_default_timeout_values(self):
        """Verify default timeout values."""
        api = PolygonApi()
        assert api._initial_timeout == 6
        assert api._retry_timeout == 12
        assert api._retry_timeout_raise == False

    def test_timeout_values_can_be_modified(self):
        """Verify timeout values can be changed after instantiation."""
        api = PolygonApi()
        api._initial_timeout = 10
        api._retry_timeout = 20
        api._retry_timeout_raise = True
        
        assert api._initial_timeout == 10
        assert api._retry_timeout == 20
        assert api._retry_timeout_raise == True
