"""
-- Created by: Ashok Kumar Pant
-- Email: asokpant@gmail.com
-- Created on: 14/08/2024
"""
import logging
import os
import timeit

import pytest

from pdpolygonapi import PolygonApi

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%d"


@pytest.fixture
def api():
    # polygon_api_key = "XXX"
    # os.environ.setdefault("POLYGON_API", polygon_api_key)

    api_env_key = "POLYGON_API"
    api = PolygonApi(envkey=api_env_key)

    if api.APIKEY is None or api.APIKEY == "":
        logger.error(f"Polygon API key is not set in the environment variable [{api_env_key}]")
    assert api.APIKEY is not None and api.APIKEY != ""

    return api


def test_fetch_ohlc(api):
    data = api.fetch_ohlcvdf('SPY', start='2023-01-01', end='2023-02-01', span='day', show_request=True)
    assert len(data) == 21
    assert len(data.columns) == 5
    assert data.index[0].date().strftime(DATE_FORMAT) == '2023-01-03'
    assert data.index[-1].date().strftime(DATE_FORMAT) == '2023-02-01'


ticker_param_data = [
    # ["ticker", "start", "end", "span", "span_multiplier"],
    ("O:SPY230728P00435000", "2023-01-03", 2, "day", 1),
    ("O:SPY230728C00440000", "2023-01-03", 2, "day", 1),
    ("O:SPY230721C00435000", "2023-01-03", 2, "day", 1),
    ("O:SPY230721P00440000", "2023-01-03", 2, "day", 1),
]


@pytest.mark.parametrize("ticker,start, end, span, span_multiplier", ticker_param_data)
def test_fetch_tickers(api, ticker, start, end, span, span_multiplier):
    df = api.fetch_ohlcvdf(ticker, start=start, end=end, span=span, show_request=False, span_multiplier=span_multiplier,
                           cache=False)
    logger.debug(
        f"ticker={ticker}, start={start}, end={end}, span={span}, span_multiplier={span_multiplier}, len(df)={len(df)}")
    assert len(df) > 0


@pytest.mark.parametrize("ticker,start,end,span,span_multiplier", ticker_param_data)
def test_fetch_tickers_time(api, ticker, start, end, span, span_multiplier):
    def fetch_data(cache):
        return api.fetch_ohlcvdf(ticker, start=start, end=end, span=span, show_request=False,
                                 span_multiplier=span_multiplier, cache=cache)

    time_nocache = timeit.timeit(lambda: fetch_data(cache=False), number=1)
    time_cache = timeit.timeit(lambda: fetch_data(cache=True), number=1)

    logger.debug(
        f"ticker={ticker}, start={start}, end={end}, span={span}, span_multiplier={span_multiplier}, "
        f"time (nocache/cache) ={time_nocache:.4f}s/{time_cache:.4f}s")
