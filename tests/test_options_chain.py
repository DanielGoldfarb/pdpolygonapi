"""
Test pdpolygonapi.fetch_options_chain() and OptionsChain class.
"""

import logging
import pytest
import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger("test_pdpgapi")


class TestFetchOptionsChain:
    """Tests for the fetch_options_chain() method."""

    def test_fetch_options_chain_basic(self, pdpgapi):
        """Test fetching options chain with just underlying ticker."""
        oc = pdpgapi.fetch_options_chain("SPY")

        # Should return an OptionsChain object
        assert oc is not None
        assert hasattr(oc, "underlying")
        assert hasattr(oc, "tickers")
        assert hasattr(oc, "expirations")
        assert hasattr(oc, "strikes")

        # Underlying should match what we requested
        assert oc.underlying == "SPY"

        # Should have some expirations (SPY has many)
        assert len(oc.expirations) > 0

        # Tickers should be a Series
        assert isinstance(oc.tickers, pd.Series)
        assert len(oc.tickers) > 0

    def test_fetch_options_chain_with_start_expiration(self, pdpgapi):
        """Test fetching options chain with start_expiration specified."""
        # Use a date 30 days in the future
        start_date = datetime.today() + timedelta(days=30)
        start_str = start_date.strftime("%Y-%m-%d")

        oc = pdpgapi.fetch_options_chain("SPY", start_expiration=start_str)

        assert oc.underlying == "SPY"
        assert len(oc.expirations) > 0

        # All expirations should be >= start_date
        for exp in oc.expirations.values:
            exp_date = pd.Timestamp(exp).date()
            assert exp_date >= start_date.date(), (
                f"Expiration {exp_date} is before start_expiration {start_date.date()}"
            )

    def test_fetch_options_chain_with_end_expiration(self, pdpgapi):
        """Test fetching options chain with end_expiration specified."""
        # Use a date 60 days in the future
        end_date = datetime.today() + timedelta(days=60)
        end_str = end_date.strftime("%Y-%m-%d")

        oc = pdpgapi.fetch_options_chain("SPY", end_expiration=end_str)

        assert oc.underlying == "SPY"
        assert len(oc.expirations) > 0

        # All expirations should be <= end_date
        for exp in oc.expirations.values:
            exp_date = pd.Timestamp(exp).date()
            assert exp_date <= end_date.date(), (
                f"Expiration {exp_date} is after end_expiration {end_date.date()}"
            )

    def test_fetch_options_chain_with_date_range(self, pdpgapi):
        """Test fetching options chain with both start and end expiration."""
        start_date = datetime.today() + timedelta(days=30)
        end_date = datetime.today() + timedelta(days=90)
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        oc = pdpgapi.fetch_options_chain("SPY", start_expiration=start_str, end_expiration=end_str)

        assert oc.underlying == "SPY"

        # All expirations should be within the range
        for exp in oc.expirations.values:
            exp_date = pd.Timestamp(exp).date()
            assert exp_date >= start_date.date(), (
                f"Expiration {exp_date} is before start_expiration {start_date.date()}"
            )
            assert exp_date <= end_date.date(), (
                f"Expiration {exp_date} is after end_expiration {end_date.date()}"
            )

    def test_fetch_options_chain_with_int_dates(self, pdpgapi):
        """Test fetching options chain with integer date offsets."""
        # start_expiration=30 means 30 days from today
        # end_expiration=60 means 60 days from today
        oc = pdpgapi.fetch_options_chain("SPY", start_expiration=30, end_expiration=60)

        assert oc.underlying == "SPY"
        assert len(oc.expirations) > 0

        # Verify dates are in expected range
        start_date = datetime.today() + timedelta(days=30)
        end_date = datetime.today() + timedelta(days=60)

        for exp in oc.expirations.values:
            exp_date = pd.Timestamp(exp).date()
            assert exp_date >= start_date.date()
            assert exp_date <= end_date.date()

    def test_fetch_options_chain_different_underlying(self, pdpgapi):
        """Test fetching options chain for different underlyings."""
        # Test with QQQ (another liquid ETF)
        oc = pdpgapi.fetch_options_chain("QQQ", start_expiration=7, end_expiration=45)

        assert oc.underlying == "QQQ"
        assert len(oc.expirations) > 0
        assert len(oc.tickers) > 0


class TestOptionsChainProperties:
    """Tests for OptionsChain class properties."""

    @pytest.fixture
    def spy_options_chain(self, pdpgapi):
        """Fixture providing a SPY options chain for testing."""
        # Get options expiring 30-60 days out for consistent testing
        return pdpgapi.fetch_options_chain("SPY", start_expiration=1, end_expiration=60)

    def test_underlying_property(self, spy_options_chain):
        """Test that underlying property returns the correct ticker."""
        assert spy_options_chain.underlying == "SPY"

    def test_tickers_property_type(self, spy_options_chain):
        """Test that tickers property returns a pandas Series."""
        tickers = spy_options_chain.tickers
        assert isinstance(tickers, pd.Series)
        assert tickers.name == "Ticker"

    def test_tickers_property_index(self, spy_options_chain):
        """Test that tickers Series has correct multi-index structure."""
        tickers = spy_options_chain.tickers

        # Should have a MultiIndex with 3 levels: Expiration, Strike, Type
        assert isinstance(tickers.index, pd.MultiIndex)
        assert tickers.index.nlevels == 3
        assert tickers.index.names == ["Expiration", "Strike", "Type"]

    def test_tickers_contain_valid_option_symbols(self, spy_options_chain):
        """Test that ticker values look like valid OCC option symbols."""
        tickers = spy_options_chain.tickers

        for ticker in tickers.values[:10]:  # Check first 10
            # OCC format: SPY250117C00580000 (underlying + YYMMDD + C/P + strike*1000)
            assert ticker.startswith("O:SPY"), f"Ticker {ticker} should start with O:SPY"
            assert len(ticker) > 10, f"Ticker {ticker} seems too short"

    def test_expirations_property_type(self, spy_options_chain):
        """Test that expirations property returns a pandas Series."""
        expirations = spy_options_chain.expirations
        assert isinstance(expirations, pd.Series)
        assert expirations.name == "Expiration"

    def test_expirations_property_sorted(self, spy_options_chain):
        """Test that expirations are sorted in ascending order."""
        expirations = spy_options_chain.expirations

        exp_list = list(expirations.values)
        assert exp_list == sorted(exp_list), "Expirations should be sorted"

    def test_expirations_index_is_datetime(self, spy_options_chain):
        """Test that expirations index is a DatetimeIndex."""
        expirations = spy_options_chain.expirations
        assert isinstance(expirations.index, pd.DatetimeIndex)

    def test_strikes_property_type(self, spy_options_chain):
        """Test that strikes property returns a dictionary."""
        strikes = spy_options_chain.strikes
        assert isinstance(strikes, dict)
        assert len(strikes) > 0

    def test_strikes_keys_match_expirations(self, spy_options_chain):
        """Test that strikes dict keys correspond to expirations."""
        strikes = spy_options_chain.strikes
        expirations = spy_options_chain.expirations

        # Each expiration should have an entry in strikes
        for exp in expirations.values:
            assert str(exp) in strikes or exp in strikes, f"Expiration {exp} not found in strikes dict"

    def test_strikes_values_are_series(self, spy_options_chain):
        """Test that strikes dict values are pandas Series."""
        strikes = spy_options_chain.strikes

        breakpoint()

        for exp, strike_series in strikes.items():
            assert isinstance(strike_series, pd.Series), f"Strikes for {exp} should be a Series"

    def test_strikes_are_numeric(self, spy_options_chain):
        """Test that strike prices are numeric values."""
        import numpy as np

        strikes = spy_options_chain.strikes

        for exp, strike_series in strikes.items():
            for strike in strike_series.values:
                assert isinstance(strike, (int, float, np.integer, np.floating)), (
                    f"Strike {strike} should be numeric"
                )
                assert strike > 0, f"Strike {strike} should be positive"


class TestOptionsChainGetStrikesByExpiration:
    """Tests for OptionsChain.get_strikes_by_expiration() method."""

    @pytest.fixture
    def spy_options_chain(self, pdpgapi):
        """Fixture providing a SPY options chain for testing."""
        return pdpgapi.fetch_options_chain("SPY", start_expiration=30, end_expiration=60)

    def test_get_strikes_by_expiration_valid(self, spy_options_chain):
        """Test getting strikes for a valid expiration date."""
        # Get the first expiration from the chain
        first_exp = spy_options_chain.expirations.values[0]

        strikes = spy_options_chain.get_strikes_by_expiration(first_exp)

        assert strikes is not None
        assert isinstance(strikes, pd.Series)
        assert len(strikes) > 0

    def test_get_strikes_by_expiration_string_format(self, spy_options_chain):
        """Test getting strikes using string date format."""
        first_exp = spy_options_chain.expirations.values[0]
        exp_str = str(first_exp)[:10]  # Get YYYY-MM-DD format

        strikes = spy_options_chain.get_strikes_by_expiration(exp_str)

        assert strikes is not None
        assert len(strikes) > 0

    def test_get_strikes_by_expiration_invalid(self, spy_options_chain):
        """Test getting strikes for an invalid/nonexistent expiration."""
        # Use a date far in the past that won't exist
        invalid_exp = "2020-01-01"

        strikes = spy_options_chain.get_strikes_by_expiration(invalid_exp)

        assert strikes is None

    def test_get_strikes_contains_reasonable_values(self, spy_options_chain):
        """Test that strikes contain reasonable values for SPY."""
        first_exp = spy_options_chain.expirations.values[0]
        strikes = spy_options_chain.get_strikes_by_expiration(first_exp)

        # SPY typically trades in the 400-600 range (as of 2024-2025)
        # There should be strikes in a reasonable range
        min_strike = strikes.min()
        max_strike = strikes.max()

        # Sanity checks - SPY should have a wide range of strikes
        assert max_strike > min_strike
        assert max_strike - min_strike > 50, "SPY should have strikes spanning > $50"


class TestOptionsChainTradingScenarios:
    """Integration tests simulating real trading scenarios."""

    def test_find_options_for_specific_expiration_range(self, pdpgapi):
        """Test finding options within a specific DTE range (e.g., 30-45 DTE)."""
        # This simulates: "I want options expiring between 30 and 45 days from now"
        oc = pdpgapi.fetch_options_chain("SPY", start_expiration=30, end_expiration=45)

        assert len(oc.expirations) > 0

        today = datetime.today().date()
        for exp in oc.expirations.values:
            exp_date = pd.Timestamp(exp).date()
            dte = (exp_date - today).days
            assert 30 <= dte <= 45, f"DTE {dte} outside expected range"

    def test_chain_has_both_calls_and_puts(self, pdpgapi):
        """Test that options chain contains both calls and puts."""
        oc = pdpgapi.fetch_options_chain("SPY", start_expiration=30, end_expiration=45)

        # Get the Type level from the MultiIndex
        types = oc.tickers.index.get_level_values("Type").unique()

        assert "call" in types.str.lower().values or "C" in types.values, "Options chain should contain calls"
        assert "put" in types.str.lower().values or "P" in types.values, "Options chain should contain puts"

    def test_can_access_specific_option_ticker(self, pdpgapi):
        """Test that we can look up a specific option by expiration/strike/type."""
        oc = pdpgapi.fetch_options_chain("SPY", start_expiration=30, end_expiration=45)

        # Get first expiration
        first_exp = oc.expirations.values[0]
        first_exp_str = str(first_exp)[:10]

        # Get strikes for that expiration
        strikes = oc.get_strikes_by_expiration(first_exp)
        assert strikes is not None

        # Get a strike near the middle
        mid_idx = len(strikes) // 2
        mid_strike = strikes.iloc[mid_idx]

        # Try to access the ticker using the multi-index
        # The index is (Expiration, Strike, Type)
        tickers = oc.tickers

        # Filter to this expiration and strike
        try:
            filtered = tickers.loc[first_exp_str]
            strike_options = filtered.loc[mid_strike]
            logger.info(f"Found options at {first_exp_str}, strike {mid_strike}: {strike_options}")
            assert len(strike_options) > 0
        except KeyError:
            # Try with the full timestamp format
            filtered = tickers.xs(mid_strike, level="Strike")
            assert len(filtered) > 0
