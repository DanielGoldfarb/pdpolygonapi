"""
Test pdpolgonapi.fetch_ohlcvdf() caching
"""
import logging
import pytest
import time
import pandas as pd

logger = logging.getLogger("test_pdpgapi")

ticker_param_data = [
    # ["ticker", "start", "end", "span", "span_multiplier"],
#   ("SPY", "2023-05-01", "2025-05-01", "day", 1),
    ("SPY", "2023-05-01", "2025-05-01", "week", 1),
#   ("SPY", "2023-05-01", "2025-05-01", "week", 2),
#   ("SPY", "2023-05-01", "2025-05-01", "month", 1),
#   ("SPY", "2023-05-01", "2025-05-01", "quarter", 1),
#   ("SPY", "2025-01-01", "2025-05-01", "minute", 1),
#   ("SPY", "2025-01-01", "2025-05-01", "hour", 1),
#   ("SPY", "2025-01-01", "2025-05-01", "minute", 30),
]

LOOP = 30
MAX_LOOP = 51

def call_no_cache(api, number, ticker, start, end, span, span_multiplier):
    assert number > 1 and number <= MAX_LOOP
    t0 = time.perf_counter()
    for count in range(number):
        df = api.fetch_ohlcvdf(ticker, start=start, end=end, span=span, span_multiplier=span_multiplier,
                               show_request=True, cache=False)
    logger.debug(f"NO cache: len(df)={len(df)}")
    t1 = time.perf_counter()
    return(t0,t1,df)

def call_yes_cache(api, number, ticker, start, end, span, span_multiplier):
    assert number > 1 and number <= MAX_LOOP
    t0 = time.perf_counter()
    for count in range(number):
        df = api.fetch_ohlcvdf(ticker, start=start, end=end, span=span, span_multiplier=span_multiplier,
                               show_request=True, cache=True)
    logger.debug(f"YES cache: len(df)={len(df)}")
    t1 = time.perf_counter()
    return(t0,t1,df)


@pytest.mark.parametrize("ticker, start, end, span, span_multiplier", ticker_param_data)
def test_ohlcv_cache(pdpgapi, regolden, ticker, start, end, span, span_multiplier):

    t0, t1, df_noc = call_no_cache(pdpgapi, LOOP, ticker, start, end, span, span_multiplier)
    elapsed_no = t1 - t0
    logger.debug(f"NO  cache: t0, t1, elapsed = {t0}, {t1}, {t1-t0}")

    t0, t1, df_yec = call_yes_cache(pdpgapi, LOOP, ticker, start, end, span, span_multiplier)
    elapsed_yes = t1 - t0
    logger.debug(f"YES cache: t0, t1, elapsed = {t0}, {t1}, {t1-t0}")

    logger.info(f"elapsed_no/elapsed_yes = {elapsed_no/elapsed_yes}")
    assert elapsed_no > elapsed_yes
    assert (elapsed_no/elapsed_yes) > 2.0
    df_noc.to_csv("noc.csv")
    df_yec.to_csv("yec.csv")
    # print("df_noc=\n",df_noc)
    # print("df_yec=\n",df_yec)
    # df_diff = df_noc.compare(df_yec)
    # print("df_diff=\n",df_diff)
    pd.testing.assert_frame_equal(df_noc, df_yec)

def test_clear_ohlcv_cache(pdpgapi):

    # Clear the cache:
    pdpgapi.clear_ohlcv_cache("all")

    # Generate some cache files by actually requesting data:
    tickers = ["ZS", "ZBRA", "TSLA", "EA"]
    for ticker in tickers:
        df = pdpgapi.fetch_ohlcvdf(ticker, start="2024-01-02", end="2024-12-31", span="week",
                               span_multiplier=1, show_request=False, cache=True)
        df = pdpgapi.fetch_ohlcvdf(ticker, start="2024-01-02", end="2024-12-31", span="month",
                               span_multiplier=1, show_request=False, cache=True)

    cleared = pdpgapi.clear_ohlcv_cache("EA")
    assert len(cleared) == 2

    cleared = pdpgapi.clear_ohlcv_cache("all")
    assert len(cleared) == 6

