"""
Layer-1 tests for _req_get_json — the rewritten contract.

Contract (as of 2026-05-18 fix):
    * HTTP timeout: existing graduated retry preserved (covered by test_timeout_retry.py).
    * JSON status OK or DELAYED, or response containing a "results" key: return dict.
    * Free-tier rate-limit (error contains "exceeded" AND "upgrade"): sleep-and-retry,
      bounded to 6 attempts total (1 initial + 5 retries).  On exhaustion: raise
      PolygonApiError.
    * Any other non-OK status (NOT_AUTHORIZED, ERROR, etc.): one retry after a short
      sleep.  If retry succeeds, log ERROR "first attempt got X; retry succeeded —
      INVESTIGATE" and return.  If retry also fails: raise PolygonApiError.
    * error_stats counters incremented as appropriate (Layer 4 hook).

All tests monkeypatch ``requests.get`` and ``time.sleep`` so the suite runs instantly.
"""

import logging

import pytest

from pdpolygonapi import PolygonApi, PolygonApiError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeResponse:
    """Minimal requests.Response stand-in returning a fixed JSON body."""

    def __init__(self, json_body):
        self._json_body = json_body

    def json(self):
        return self._json_body


def _make_get(responses):
    """Build a fake requests.get that returns ``responses`` in sequence.

    ``responses`` is a list of JSON dicts (or callables taking req->dict).
    Each call to the returned function consumes the next entry.
    """
    state = {"i": 0}

    def fake_get(req, timeout=None):
        i = state["i"]
        state["i"] += 1
        if i >= len(responses):
            raise AssertionError(f"fake_get called {i + 1} times; only {len(responses)} responses queued")
        r = responses[i]
        body = r(req) if callable(r) else r
        return FakeResponse(body)

    fake_get.call_count = lambda: state["i"]
    fake_get._state = state
    return fake_get


@pytest.fixture
def mock_api():
    """PolygonApi with a dummy key — never hits real API."""
    api = PolygonApi(apikey="test_key_1234567890", cache=False)
    api.logger.setLevel(logging.DEBUG)
    return api


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Speed up tests by skipping all time.sleep calls inside _pdpolygonapi_base."""
    import pdpolygonapi._pdpolygonapi_base as base
    monkeypatch.setattr(base.time, "sleep", lambda s: None)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestHappyPath:

    def test_status_ok_returns_dict(self, mock_api, monkeypatch):
        body = {"status": "OK", "results": [1, 2, 3]}
        fake_get = _make_get([body])
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        out = mock_api._req_get_json("https://example/api?apiKey=xxx")
        assert out == body
        assert fake_get.call_count() == 1

    def test_status_delayed_returns_dict(self, mock_api, monkeypatch):
        body = {"status": "DELAYED", "results": [{"x": 1}]}
        fake_get = _make_get([body])
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        out = mock_api._req_get_json("https://example/api?apiKey=xxx")
        assert out == body
        assert fake_get.call_count() == 1


# ---------------------------------------------------------------------------
# Non-OK statuses (NOT_AUTHORIZED, ERROR, etc.) — one-shot retry
# ---------------------------------------------------------------------------

class TestNonOkOneShotRetry:

    def test_not_authorized_retry_succeeds_logs_error_and_returns(self, mock_api, monkeypatch, caplog):
        good = {"status": "OK", "results": [{"x": 1}]}
        bad = {"status": "NOT_AUTHORIZED", "message": "subscription required"}
        fake_get = _make_get([bad, good])
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        with caplog.at_level(logging.ERROR, logger="pdpolygonapi"):
            out = mock_api._req_get_json("https://example/api?apiKey=xxx")

        assert out == good
        assert fake_get.call_count() == 2
        # Must log at ERROR level so the operator notices, with the magic word INVESTIGATE
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert any("INVESTIGATE" in r.message for r in error_records), \
            f"expected an ERROR log with 'INVESTIGATE'; got: {[r.message for r in error_records]}"
        assert any("NOT_AUTHORIZED" in r.message for r in error_records)

    def test_not_authorized_retry_also_fails_raises(self, mock_api, monkeypatch):
        bad = {"status": "NOT_AUTHORIZED", "message": "subscription required"}
        fake_get = _make_get([bad, bad])
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        with pytest.raises(PolygonApiError) as excinfo:
            mock_api._req_get_json("https://example/api?apiKey=xxx")

        err = excinfo.value
        assert err.status == "NOT_AUTHORIZED"
        assert err.response_json == bad
        assert "apiKey=xxx" not in err.request_url, "API key should be redacted in request_url"
        assert fake_get.call_count() == 2

    def test_generic_error_status_retry_then_raise(self, mock_api, monkeypatch):
        bad = {"status": "ERROR", "error": "internal server error"}
        fake_get = _make_get([bad, bad])
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        with pytest.raises(PolygonApiError) as excinfo:
            mock_api._req_get_json("https://example/api?apiKey=xxx")
        assert excinfo.value.status == "ERROR"
        assert fake_get.call_count() == 2

    def test_missing_status_treated_as_failure(self, mock_api, monkeypatch):
        """A response with neither 'status' nor 'results' is a failure."""
        bad = {"queryCount": 0}
        fake_get = _make_get([bad, bad])
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        with pytest.raises(PolygonApiError):
            mock_api._req_get_json("https://example/api?apiKey=xxx")


# ---------------------------------------------------------------------------
# Free-tier rate-limit (bounded retries)
# ---------------------------------------------------------------------------

class TestRateLimitBounded:

    RATE_LIMIT_BODY = {
        "status": "ERROR",
        "error": "You've exceeded the maximum requests per minute, please wait "
                 "or upgrade your subscription to continue. https://polygon.io/pricing",
    }

    def test_rate_limit_then_success(self, mock_api, monkeypatch):
        good = {"status": "OK", "results": []}
        fake_get = _make_get([self.RATE_LIMIT_BODY, self.RATE_LIMIT_BODY, good])
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        out = mock_api._req_get_json("https://example/api?apiKey=xxx")
        assert out == good
        assert fake_get.call_count() == 3

    def test_rate_limit_bounded_to_six_attempts(self, mock_api, monkeypatch):
        """6 consecutive rate-limit responses → raise PolygonApiError (not infinite loop)."""
        fake_get = _make_get([self.RATE_LIMIT_BODY] * 6)
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        with pytest.raises(PolygonApiError):
            mock_api._req_get_json("https://example/api?apiKey=xxx")
        assert fake_get.call_count() == 6

    def test_rate_limit_does_not_consume_oneshot_retry_budget(self, mock_api, monkeypatch):
        """Rate-limit retries are separate from the non-OK one-shot retry."""
        good = {"status": "OK", "results": []}
        # 5 rate-limits then success — should NOT raise.
        fake_get = _make_get([self.RATE_LIMIT_BODY] * 5 + [good])
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        out = mock_api._req_get_json("https://example/api?apiKey=xxx")
        assert out == good
        assert fake_get.call_count() == 6


# ---------------------------------------------------------------------------
# error_stats counters (Layer 4 hook)
# ---------------------------------------------------------------------------

class TestErrorStats:

    def test_error_stats_attribute_exists(self, mock_api):
        assert hasattr(mock_api, "error_stats")
        assert isinstance(mock_api.error_stats, dict)
        for key in (
            "fetches_attempted",
            "fetches_succeeded",
            "retries_attempted",
            "retries_succeeded",
            "non_ok_responses",
            "exceptions_raised",
        ):
            assert key in mock_api.error_stats, f"missing key: {key}"

    def test_success_increments_fetches(self, mock_api, monkeypatch):
        body = {"status": "OK", "results": []}
        fake_get = _make_get([body])
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        before = dict(mock_api.error_stats)
        mock_api._req_get_json("https://example/api?apiKey=xxx")
        after = mock_api.error_stats
        assert after["fetches_attempted"] == before["fetches_attempted"] + 1
        assert after["fetches_succeeded"] == before["fetches_succeeded"] + 1
        assert after["exceptions_raised"] == before["exceptions_raised"]

    def test_retry_success_increments_retry_counters(self, mock_api, monkeypatch):
        good = {"status": "OK", "results": []}
        bad = {"status": "NOT_AUTHORIZED"}
        fake_get = _make_get([bad, good])
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        before = dict(mock_api.error_stats)
        mock_api._req_get_json("https://example/api?apiKey=xxx")
        after = mock_api.error_stats
        assert after["retries_attempted"] >= before["retries_attempted"] + 1
        assert after["retries_succeeded"] >= before["retries_succeeded"] + 1
        assert after["non_ok_responses"].get("NOT_AUTHORIZED", 0) >= 1

    def test_failure_increments_exceptions_raised(self, mock_api, monkeypatch):
        bad = {"status": "NOT_AUTHORIZED"}
        fake_get = _make_get([bad, bad])
        import pdpolygonapi._pdpolygonapi_base as base
        monkeypatch.setattr(base.requests, "get", fake_get)

        before = dict(mock_api.error_stats)
        with pytest.raises(PolygonApiError):
            mock_api._req_get_json("https://example/api?apiKey=xxx")
        after = mock_api.error_stats
        assert after["exceptions_raised"] == before["exceptions_raised"] + 1
