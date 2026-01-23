"""
Test _json_response_to_ohlcvdf() method in _PolygonApiBase.

These tests verify all branches of the response parsing logic:
- Empty dict (timeout case)
- Response with "message" field
- Response with "error" field  
- Response with missing status
- Response with invalid status
- Valid empty results (status OK/DELAYED but no results)
- Successful response with results (various spans)
"""

import logging
import pytest
import pandas as pd
from pdpolygonapi import PolygonApi


@pytest.fixture
def api():
    """Create a PolygonApi instance for testing."""
    return PolygonApi(loglevel=logging.DEBUG)


class TestJsonResponseToOhlcvdfNoResults:
    """Test cases where 'results' is not in rjson."""

    def test_empty_dict_returns_empty_df_no_warning(self, api, caplog):
        """Empty dict (from timeout) returns empty DataFrame, no warning logged."""
        rjson = {}
        
        with caplog.at_level(logging.WARNING):
            df = api._json_response_to_ohlcvdf("day", rjson)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]
        # No warning should be logged for empty dict (already logged in _req_get_json)
        assert len([r for r in caplog.records if r.levelno >= logging.WARNING]) == 0

    def test_message_field_logs_warning(self, api, caplog):
        """Response with 'message' field logs the message as warning."""
        rjson = {"message": "No data found for ticker XYZ"}
        
        with caplog.at_level(logging.WARNING):
            df = api._json_response_to_ohlcvdf("day", rjson)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert any("No data found for ticker XYZ" in r.message for r in caplog.records)

    def test_message_field_with_status_appends_status(self, api, caplog):
        """Response with 'message' and 'status' appends status to warning."""
        rjson = {"message": "No data found", "status": "NOT_FOUND"}
        
        with caplog.at_level(logging.WARNING):
            df = api._json_response_to_ohlcvdf("day", rjson)
        
        assert len(df) == 0
        warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "No data found" in warnings[0]
        assert "(NOT_FOUND)" in warnings[0]

    def test_error_field_logs_warning(self, api, caplog):
        """Response with 'error' field logs the error as warning."""
        rjson = {"error": "Rate limit exceeded"}
        
        with caplog.at_level(logging.WARNING):
            df = api._json_response_to_ohlcvdf("day", rjson)
        
        assert len(df) == 0
        assert any("Rate limit exceeded" in r.message for r in caplog.records)

    def test_error_field_with_status_appends_status(self, api, caplog):
        """Response with 'error' and 'status' appends status to warning."""
        rjson = {"error": "Rate limit exceeded", "status": "ERROR"}
        
        with caplog.at_level(logging.WARNING):
            df = api._json_response_to_ohlcvdf("day", rjson)
        
        assert len(df) == 0
        warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "Rate limit exceeded" in warnings[0]
        assert "(ERROR)" in warnings[0]

    def test_missing_status_logs_warning(self, api, caplog):
        """Response with no status (and no message/error) logs warning."""
        rjson = {"queryCount": 0, "resultsCount": 0}  # No status, message, or error
        
        with caplog.at_level(logging.WARNING):
            df = api._json_response_to_ohlcvdf("day", rjson)
        
        assert len(df) == 0
        warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "No results returned" in warnings[0]

    def test_invalid_status_logs_warning(self, api, caplog):
        """Response with invalid status (not OK/DELAYED) logs warning."""
        rjson = {"status": "FAILED", "queryCount": 0}
        
        with caplog.at_level(logging.WARNING):
            df = api._json_response_to_ohlcvdf("day", rjson)
        
        assert len(df) == 0
        warnings = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "No results returned" in warnings[0]
        assert "(FAILED)" in warnings[0]

    def test_valid_empty_results_status_ok_no_warning(self, api, caplog):
        """Response with status OK but no results - valid empty, no warning."""
        rjson = {"status": "OK", "queryCount": 0, "resultsCount": 0}
        
        with caplog.at_level(logging.WARNING):
            df = api._json_response_to_ohlcvdf("day", rjson)
        
        assert len(df) == 0
        # No warning for valid empty results
        assert len([r for r in caplog.records if r.levelno >= logging.WARNING]) == 0

    def test_valid_empty_results_status_delayed_no_warning(self, api, caplog):
        """Response with status DELAYED but no results - valid empty, no warning."""
        rjson = {"status": "DELAYED", "queryCount": 0, "resultsCount": 0}
        
        with caplog.at_level(logging.WARNING):
            df = api._json_response_to_ohlcvdf("day", rjson)
        
        assert len(df) == 0
        # No warning for valid empty results
        assert len([r for r in caplog.records if r.levelno >= logging.WARNING]) == 0


class TestJsonResponseToOhlcvdfWithResults:
    """Test cases where 'results' is present in rjson."""

    def test_daily_span_returns_date_index(self, api):
        """Daily span data returns DataFrame with date index (not datetime)."""
        rjson = {
            "ticker": "SPY",
            "status": "OK",
            "results": [
                {"t": 1704067200000, "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1000000},
                {"t": 1704153600000, "o": 100.5, "h": 102.0, "l": 100.0, "c": 101.5, "v": 1100000},
            ]
        }
        
        df = api._json_response_to_ohlcvdf("day", rjson)
        
        assert len(df) == 2
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]
        assert df.index.name == "SPY"
        # Daily span should have date-based index (time component is 00:00:00)
        assert df.index[0].hour == 0
        assert df.index[0].minute == 0

    def test_weekly_span_returns_date_index(self, api):
        """Weekly span data returns DataFrame with date index."""
        rjson = {
            "ticker": "SPY",
            "status": "OK",
            "results": [
                {"t": 1704067200000, "o": 100.0, "h": 105.0, "l": 99.0, "c": 104.0, "v": 5000000},
            ]
        }
        
        df = api._json_response_to_ohlcvdf("week", rjson)
        
        assert len(df) == 1
        assert df.index.name == "SPY"
        # Weekly span should have date-based index (time component is 00:00:00)
        assert df.index[0].hour == 0
        assert df.index[0].minute == 0

    def test_minute_span_returns_datetime_index(self, api):
        """Minute span data returns DataFrame with datetime index."""
        rjson = {
            "ticker": "SPY",
            "status": "OK",
            "results": [
                {"t": 1704117000000, "o": 100.0, "h": 100.1, "l": 99.9, "c": 100.05, "v": 10000},
                {"t": 1704117060000, "o": 100.05, "h": 100.2, "l": 100.0, "c": 100.15, "v": 11000},
            ]
        }
        
        df = api._json_response_to_ohlcvdf("minute", rjson)
        
        assert len(df) == 2
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]
        assert df.index.name == "SPY"
        # Minute span should have datetime index
        assert isinstance(df.index[0], pd.Timestamp)

    def test_hour_span_returns_datetime_index(self, api):
        """Hour span data returns DataFrame with datetime index."""
        rjson = {
            "ticker": "SPY",
            "status": "OK",
            "results": [
                {"t": 1704117000000, "o": 100.0, "h": 101.0, "l": 99.5, "c": 100.5, "v": 100000},
            ]
        }
        
        df = api._json_response_to_ohlcvdf("hour", rjson)
        
        assert len(df) == 1
        assert isinstance(df.index[0], pd.Timestamp)

    def test_second_span_returns_datetime_index(self, api):
        """Second span data returns DataFrame with datetime index."""
        rjson = {
            "ticker": "SPY",
            "status": "OK",
            "results": [
                {"t": 1704117000000, "o": 100.0, "h": 100.01, "l": 99.99, "c": 100.0, "v": 100},
            ]
        }
        
        df = api._json_response_to_ohlcvdf("second", rjson)
        
        assert len(df) == 1
        assert isinstance(df.index[0], pd.Timestamp)

    def test_timezone_conversion(self, api):
        """Verify timezone is applied correctly for intraday data."""
        rjson = {
            "ticker": "SPY",
            "status": "OK",
            "results": [
                {"t": 1704117000000, "o": 100.0, "h": 100.1, "l": 99.9, "c": 100.05, "v": 10000},
            ]
        }
        
        # Default is US/Eastern
        df_eastern = api._json_response_to_ohlcvdf("minute", rjson, tz="US/Eastern")
        df_utc = api._json_response_to_ohlcvdf("minute", rjson, tz="UTC")
        
        # The times should differ by the timezone offset
        assert df_eastern.index[0] != df_utc.index[0]

    def test_column_renaming(self, api):
        """Verify OHLCV columns are renamed correctly."""
        rjson = {
            "ticker": "TEST",
            "status": "OK",
            "results": [
                {"t": 1704067200000, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 999},
            ]
        }
        
        df = api._json_response_to_ohlcvdf("day", rjson)
        
        assert df.loc[df.index[0], "Open"] == 1.0
        assert df.loc[df.index[0], "High"] == 2.0
        assert df.loc[df.index[0], "Low"] == 0.5
        assert df.loc[df.index[0], "Close"] == 1.5
        assert df.loc[df.index[0], "Volume"] == 999
