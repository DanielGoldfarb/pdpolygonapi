"""
Test pdpolgonapi.fetch_ohlcvdf() for stocks.
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
    ("SPY", "2025-01-01", "2025-03-01", "minute", 1),
    ("SPY", "2025-01-01", "2025-03-01", "hour", 1),
    ("SPY", "2025-01-01", "2025-03-01", "minute", 30),
]

@pytest.mark.parametrize("ticker, start, end, span, span_multiplier", ticker_param_data)
def test_ohlcv_stocks(pdpgapi, regolden, ticker, start, end, span, span_multiplier):

    logger.info(f"In test_ohlcv_stocks: logger.getEffectiveLevel()={logger.getEffectiveLevel()}")
    for h in logger.handlers:
        level_name = logging.getLevelName(h.level)
        logger.info(f"In test_ohlcv_stocks: handler:{h.name} level={level_name}({h.level})")

    df = pdpgapi.fetch_ohlcvdf(ticker, start=start, end=end, span=span, span_multiplier=span_multiplier)

    ref_name = ""
    # logger.info(f"(ticker, start, end, span, span_multiplier)={(ticker, start, end, span, span_multiplier)}")
    if isinstance(df, pd.DataFrame):
        sd = start[2:].replace("-","")
        ed = end[2:].replace("-","")
        ref_name = f"tests/reference_data/{ticker}_{sd}_{ed}_{span}_{span_multiplier}.csv"
        logger.debug(f"len(df)={len(df)}  regolden = {regolden}")
        logger.debug(f"{ref_name}")
        if "all" in regolden or ref_name in regolden:
            df.to_csv(ref_name)
            logger.info(f"REGENERATING 'golden' reference file: '{ref_name}'")

        # logger.info(f"rdf=\n{rdf.iloc[[0,-1]]}")
        # https://saturncloud.io/blog/how-to-confirm-equality-of-two-pandas-dataframes
        try:
            rdf = pd.read_csv(ref_name, index_col=ticker, parse_dates=True)
            pd.testing.assert_frame_equal(df, rdf)
        except (FileNotFoundError, AssertionError) as e:
            if "failures" in regolden:
                logger.info(f"REGENERATING failed 'golden' reference file: '{ref_name}'")
                df.to_csv(ref_name)
                rdf = pd.read_csv(ref_name, index_col=ticker, parse_dates=True)
                # rdf.iloc[4,2] = -99.99
                try:
                    # df_diff = df.compare(rdf)
                    # are_equal = df.equals(rdf)
                    pd.testing.assert_frame_equal(df, rdf)
                except AssertionError as e2:
                    logger.error(f"GOT ASSERTION ERROR for '{ref_name}':\n{e2}")
                    raise
                except Exception as e2:
                    logger.error(f"{ref_name} assert exception: {e2}")
                    raise
            else:
                logger.error(f"Data Failed to match reference: {ref_name}")
                logger.error(f"exception: {e}")
                raise
