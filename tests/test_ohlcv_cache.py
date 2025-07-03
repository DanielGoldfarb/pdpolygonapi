"""
Test pdpolgonapi.fetch_ohlcvdf() caching
"""

import logging
import pytest
import time
import pandas as pd

logger = logging.getLogger("test_pdpgapi")


def test_clear_ohlcv_cache(pdpgapi):
    # Clear the cache:
    pdpgapi.clear_ohlcv_cache("all")

    # Generate some cache files by actually requesting data:
    tickers = ["ZS", "ZBRA", "TSLA", "EA"]
    for ticker in tickers:
        df = pdpgapi.fetch_ohlcvdf(
            ticker,
            start="2024-01-02",
            end="2024-12-31",
            span="week",
            span_multiplier=1,
            show_request=False,
            cache=True,
        )
        df = pdpgapi.fetch_ohlcvdf(
            ticker,
            start="2024-01-02",
            end="2024-12-31",
            span="month",
            span_multiplier=1,
            show_request=False,
            cache=True,
        )

    cleared = pdpgapi.clear_ohlcv_cache("EA")
    assert len(cleared) == 2

    cleared = pdpgapi.clear_ohlcv_cache("all")
    assert len(cleared) == 6


ticker_param_data = [
    # ["ticker", "start", "end", "span", "span_multiplier", expected_cache_ratio],
    ("SPY", "2023-05-01", "2025-05-01", "day", 1, 2.0),
    ("SPY", "2023-05-01", "2025-05-01", "week", 1, 1.8),
    # This would(should) raise an exception because span multiplier > 1 is only
    # valid for spans less than a day, that is, for "hour" or "minute":
    # ("SPY", "2023-05-01", "2025-05-01", "week", 2, 1.6),
    ("SPY", "2023-05-01", "2025-05-01", "month", 1, 1.4),
    ("SPY", "2023-05-01", "2025-05-01", "quarter", 1, 1.2),
    ("SPY", "2025-01-01", "2025-05-01", "minute", 5, 12.0),
    ("SPY", "2025-01-01", "2025-05-01", "minute", 15, 12.0),
    ("SPY", "2025-01-01", "2025-05-01", "minute", 30, 10.0),
    ("SPY", "2025-01-01", "2025-05-01", "hour", 1, 10.0),
    ("SPY", "2025-01-01", "2025-05-01", "hour", 2, 10.0),
]

LOOP = 36
MAX_LOOP = 51


def call_no_cache(api, number, ticker, start, end, span, span_multiplier):
    assert number > 1 and number <= MAX_LOOP
    t0 = time.perf_counter()
    for count in range(number):
        df = api.fetch_ohlcvdf(
            ticker,
            start=start,
            end=end,
            span=span,
            span_multiplier=span_multiplier,
            show_request=False,
            cache=False,
        )
    logger.debug(f"NO cache: len(df)={len(df)}")
    t1 = time.perf_counter()
    return (t0, t1, df)


def call_yes_cache(api, number, ticker, start, end, span, span_multiplier):
    assert number > 1 and number <= MAX_LOOP
    t0 = time.perf_counter()
    for count in range(number):
        df = api.fetch_ohlcvdf(
            ticker,
            start=start,
            end=end,
            span=span,
            span_multiplier=span_multiplier,
            show_request=False,
            cache=True,
        )
    logger.debug(f"YES cache: len(df)={len(df)}")
    t1 = time.perf_counter()
    return (t0, t1, df)


@pytest.mark.parametrize("ticker, start, end, span, span_multiplier, expected_cache_ratio", ticker_param_data)
def test_ohlcv_cache(pdpgapi, regolden, ticker, start, end, span, span_multiplier, expected_cache_ratio):
    num_tries = 0
    while num_tries < 2:
        num_tries += 1
        logger.info("CLEAR ALL CACHE...")
        pdpgapi.clear_ohlcv_cache("all")

        t0, t1, df_noc = call_no_cache(pdpgapi, LOOP, ticker, start, end, span, span_multiplier)
        elapsed_no = t1 - t0
        logger.debug(f"NO  cache: t0, t1, elapsed = {t0}, {t1}, {t1 - t0}")

        t0, t1, df_yec = call_yes_cache(pdpgapi, LOOP, ticker, start, end, span, span_multiplier)
        elapsed_yes = t1 - t0
        logger.debug(f"YES cache: t0, t1, elapsed = {t0}, {t1}, {t1 - t0}")

        assert elapsed_no > elapsed_yes

        ratio = elapsed_no / elapsed_yes
        logger.info(
            f"try#{num_tries}: elapsed_no/elapsed_yes = {ratio:.5f}  (expected > {expected_cache_ratio})"
        )
        if ratio > expected_cache_ratio:
            break

    assert ratio > expected_cache_ratio

    df_noc.to_csv("noc.csv")
    df_yec.to_csv("yec.csv")
    # print("df_noc=\n",df_noc)
    # print("df_yec=\n",df_yec)
    # df_diff = df_noc.compare(df_yec)
    # print("df_diff=\n",df_diff)
    pd.testing.assert_frame_equal(df_noc, df_yec)
