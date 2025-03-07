"""
-- Created by: Ashok Kumar Pant
-- Email: asokpant@gmail.com
-- Created on: 14/08/2024
"""
import logging
import os
import timeit

import pytest
import pandas as pd

from pdpolygonapi import PolygonApi

#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print('0:__name__=',__name__)
logger.setLevel(logging.DEBUG)

DATE_FORMAT = "%Y-%m-%d"

@pytest.fixture
def api():
    api_env_key = "POLYGON_API"
    api = PolygonApi(envkey=api_env_key, loglevel='INFO', wait=True, cache=False)
    if api.APIKEY is None or api.APIKEY == "":
        logger.error(f"Polygon API key is not set in the environment variable [{api_env_key}]")
    assert isinstance(api.APIKEY,str) and len(api.APIKEY) > 10
    return api


# fetch_tickers_parameters
# fetch_cache_parameters

ticker_param_data = [
    # ["ticker", "start", "end", "span", "span_multiplier", "reference"],
    ("SPY",  -30,   -1, "day", 1, None),
    ("SPY", "2024-03-01", "2024-03-31", "day", 1, "SPY2024-03-01-03-31.csv"),
    ("SPY", "2024-02-01", "2024-02-29", "day", 1, "SPY2024-02-01-02-29.csv"),
    ("SPY", "2025-02-01", "2025-02-28", "day", 1, "SPY2025-02-01-02-28.csv"),
   #("SPY", -430, -400, "day", 1),
   #("SPY", -830, -800, "day", 1),
   #("O:zzz999999x12345678", "2023-01-03", 2, "day", 1),
   #("O:SPY230728C00440000", "2023-01-03", 2, "day", 1),
   #("O:SPY230721C00435000", "2023-01-03", 2, "day", 1),
   #("O:SPY230721P00440000", "2023-01-03", 2, "day", 1),
]
ticker_param = [ data for data in ticker_param_data ]


##  @pytest.mark.parametrize("ticker,start, end, span, span_multiplier", ticker_param_data)
##  def test_fetch_tickers(api, ticker, start, end, span, span_multiplier):
##      df = api.fetch_ohlcvdf(ticker, start=start, end=end, span=span, show_request=False, span_multiplier=span_multiplier,
##                             cache=False)
##      logger.debug(
##          f"ticker={ticker}, start={start}, end={end}, span={span}, span_multiplier={span_multiplier}, len(df)={len(df)}")
##      assert len(df) > 0

@pytest.mark.parametrize("ticker, start, end, span, span_multiplier, reference", ticker_param_data)
def test_ohlcv_stocks(api, ticker, start, end, span, span_multiplier, reference):

    df = api.fetch_ohlcvdf(ticker, start=start, end=end, span=span, span_multiplier=span_multiplier)

    logger.info(f"(ticker, start, end, span, span_multiplier)={(ticker, start, end, span, span_multiplier)}")
    if isinstance(df, pd.DataFrame):
        logger.info(f"len(df)={len(df)}")
        logger.info(f"df=\n{df.iloc[[0,-1]]}")
        # if regenerate_reference and isinstance(start,str):
        #     fname = ticker+start+end[-6:]+'.csv'
        #     df.to_csv(fname)
        if isinstance(reference,str):
             rdf = pd.read_csv(reference, index_col=ticker, parse_dates=True)
             logger.info(f"rdf=\n{rdf.iloc[[0,-1]]}")
             # https://saturncloud.io/blog/how-to-confirm-equality-of-two-pandas-dataframes
             pd.testing.assert_frame_equal(df, rdf)

