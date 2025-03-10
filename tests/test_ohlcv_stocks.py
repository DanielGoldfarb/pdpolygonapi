"""
-- Created by: Ashok Kumar Pant
-- Email: asokpant@gmail.com
-- Created on: 14/08/2024
"""
import logging
import pytest
import pandas as pd

print(f" Using logger({__name__})")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DATE_FORMAT = "%Y-%m-%d"

ticker_param_data = [
    # ["ticker", "start", "end", "span", "span_multiplier"],
    ("SPY", "2023-10-01", "2025-03-01", "day", 1),
    ("SPY", "2023-10-01", "2025-03-01", "week", 1),
    ("SPY", "2023-10-01", "2025-03-01", "month", 1),
    ("SPY", "2023-10-01", "2025-03-01", "quarter", 1),
]

@pytest.mark.parametrize("ticker, start, end, span, span_multiplier", ticker_param_data)
def test_ohlcv_stocks(pdpgapi, regolden, ticker, start, end, span, span_multiplier):

    df = pdpgapi.fetch_ohlcvdf(ticker, start=start, end=end, span=span, span_multiplier=span_multiplier)

    ref_name = ""
    # logger.info(f"(ticker, start, end, span, span_multiplier)={(ticker, start, end, span, span_multiplier)}")
    if isinstance(df, pd.DataFrame):
        logger.info(f"len(df)={len(df)}")
        # if len(df) > 20:
        #     logger.info(f"df=\n{df.iloc[[0,-1]]}")
        # else:
        #     logger.info(f"df=\n{df}")
        # SPY_231001_250301_day_1.csv
        sd = start[2:].replace("-","")
        ed = end[2:].replace("-","")
        ref_name = f"reference_data/{ticker}_{sd}_{ed}_{span}_{span_multiplier}.csv"
        if regolden:
            df.to_csv(ref_name)
        else:
            rdf = pd.read_csv(ref_name, index_col=ticker, parse_dates=True)
            # logger.info(f"rdf=\n{rdf.iloc[[0,-1]]}")
            # https://saturncloud.io/blog/how-to-confirm-equality-of-two-pandas-dataframes
            pd.testing.assert_frame_equal(df, rdf)

    logger.info(f"regolden={regolden}  ref_name={ref_name}")
