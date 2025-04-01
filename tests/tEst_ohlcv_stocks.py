"""
-- Created by: Ashok Kumar Pant
-- Email: asokpant@gmail.com
-- Created on: 14/08/2024
"""
import logging
import pytest
import pandas as pd

logger = logging.getLogger("test_pdpgapi")

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
        sd = start[2:].replace("-","")
        ed = end[2:].replace("-","")
        ref_name = f"reference_data/{ticker}_{sd}_{ed}_{span}_{span_multiplier}.csv"
        logger.info(f"len(df)={len(df)}  regolden = {regolden}")
        logger.info(f"{ref_name}")
        if "all" in regolden or ref_name in regolden:
            df.to_csv(ref_name)
            message = f"NOT TESTING: REGENERATING 'golden' reference file: '{ref_name}'"
            pytest.fail(message)
        else:
            rdf = pd.read_csv(ref_name, index_col=ticker, parse_dates=True)
            # logger.info(f"rdf=\n{rdf.iloc[[0,-1]]}")
            # https://saturncloud.io/blog/how-to-confirm-equality-of-two-pandas-dataframes
            try:
                pd.testing.assert_frame_equal(df, rdf)
            except:
                logger.error(f"Data Failed to match reference: {ref_name}")
                raise
                

