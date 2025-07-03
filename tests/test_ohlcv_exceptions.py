"""
Test pdpolgonapi.fetch_ohlcvdf() for stocks.
"""
import logging
import pytest
import pandas as pd

logger = logging.getLogger("test_pdpgapi")

ticker_param_data = [
    # ["ticker", "start", "end", "span", "span_multiplier", "exception"],
    ("SPY", "2023-10-01", "2025-03-01", "day", 3, ValueError ),
    ("SPY", "2023-10-01", "2025-03-01", "week", 2, ValueError ),
    ("SPY", "2023-10-01", "2025-03-01", "month", 2, ValueError ),
    ("SPY", "2023-10-01", "2025-03-01", "month", "two", TypeError ),
    ("SPY", "2023-10-01", "2025-03-01", "month", 0, ValueError ),
]

@pytest.mark.parametrize("ticker, start, end, span, span_multiplier, exception", ticker_param_data)
def test_ohlcv_exceptions(pdpgapi, regolden, ticker, start, end, span, span_multiplier, exception):

    with pytest.raises(exception):
        df = pdpgapi.fetch_ohlcvdf(ticker, start=start, end=end, span=span, span_multiplier=span_multiplier)

