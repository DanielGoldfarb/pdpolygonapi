#!/usr/bin/env python
# coding: utf-8

# ---
#  class for accessing polygon.io REST api.
# ---

import datetime
import logging
import os
import pathlib
import warnings

# from  multiprocess    import Lock as MultiProcessLock # prefer
from multiprocessing import Lock as MultiProcessLock  # more common

import numpy as np
import pandas as pd
import requests

from pdpolygonapi._pdpolygonapi_base import _PolygonApiBase


def plain_warning(w, wtype, wpath, wlnum, wdum, **kwargs):
    wclass = str(wtype).split("'")[1]
    wfile = wpath.split("/")[-1]
    return "\n" + wclass + ": " + wfile + ":" + str(wlnum) + ": " + str(w) + "\n"


warnings.formatwarning = plain_warning


class PolygonApi(_PolygonApiBase):
    """
    Class to provide an instance of a python polygon.io API

    It is recommend to instantiate with `envkey=` (environment key).
    The environment key is the NAME of the ENVIRONMENT VARIABLE that
    contains the polygon apikey.

    Alternatively instantiate with `apikey=` to directly pass in the api key.

    Methods of this class include:

    fetch_ohlcvdf()       - given a ticker, returns a dataframe of OHLCV data.

    fetch_options_chain() - given an underlying ticker, and optionally given
                            also first and last expiration dates, returns all
                            options tickers with those criteria.

    """

    # TODO:
    # 1. TEST WITH ALL SPANS
    # 2. ADD RESAMPLING TO HANDLE SPAN_MULTIPLIER
    # 3. MAYBE PASS SPAN_MULTIPLIER WHEN SPAN >= 'day' ?

    # --------------------------------------------------------------------- #
    # Note: regarding the cache_file_lock:  We want to lock to avoid having
    #       one process reading a cache file while another process is in the
    #       middle of writing a cache file.  (This is so we can multiprocess
    #       many model scenarios at the same time).  It turns out that the
    #       lock is costly, slowing down a job of 24 scenarios on 8 cores by
    #       two and a half times.  I was locking all reads and writes to all
    #       cache files.  But then I realized, that created more contention
    #       than necessary: maybe it makes sense to have a separate lock for
    #       each file.  Then (before implementing that) it occured to me that
    #       once we "know" that a cache file exists, there is no longer a need
    #       to lock since multiple simulateous reads are fine.  So that is
    #       what I am going to implement next: Only lock for each cache until
    #       we know that we have a cache file for that request.

    cache_file_lock = MultiProcessLock()
    cached_files = dict()

    def cflock_acquire():
        PolygonApi.cache_file_lock.acquire()

    def cflock_release():
        try:
            PolygonApi.cache_file_lock.release()
        except:
            pass

    def __init__(
        self,
        envkey: str | None = "POLYGON_API",
        apikey: str | None = None,
        loglevel: int | str = logging.WARNING,
        wait: bool = True,
        cache: bool = False,
    ) -> None:
        """
        Class to provide interface methods to access the Polygon.io REST api.
        These methods typically return Pandas objects (DataFrames and Series)
        or return structures (classes) containing various Pandas objects.

        Args:
            envkey:   If set, this is the name of the environment variable
                      that contains your Polygon.io API key.
                      Default value is "POLYGON_API"

            apikey:   If set, this is your Polygon.io API key.
                      NOTE WELL: `apikey` overrides `envkey`.

            loglevel: Loglevel to be used for logger "pdpolygonapi"

            wait:     If Polygon allowed requests-per-minute exceeded, then wait and re-try.

            cache:    default value for cache, to be used when not specified within the
                      arguments of individual methods.

        Returns:
            An instance of the PolygonApi class
        """
        if apikey is not None:
            self.APIKEY = apikey
        elif envkey is not None:
            self.APIKEY = os.environ.get(envkey)
        else:
            self.APIKEY = os.environ.get("POLYGON_API")

        if not isinstance(self.APIKEY, str) or len(self.APIKEY) < 10:
            raise ValueError("APIKEY must be type str len >= 10")

        # create 'pdpolygonapi' logger, and set loglevel:
        self.logger = logging.getLogger("pdpolygonapi")
        if isinstance(loglevel, (str, int)):
            if isinstance(loglevel, str):
                loglevel = loglevel.upper()
            self.logger.setLevel(loglevel)
        else:  # default level for 'pdpolygonapi':
            self.logger.setLevel(logging.WARNING)
            self.logger.info("Unrecognized type for `loglevel`; default to `logging.WARNING`")

        # make sure the root logger has a handler,
        # to avoid using logging.lastResort handler:
        if not logging.getLogger("root").hasHandlers():  # root logger
            self.logger.info("no handlers in root logger: create basicConfig()")
            logging.basicConfig()  # creates basic handler and formatter

        self.current_log_level = self.logger.getEffectiveLevel()

        self.wait = wait
        self.cache_initializer = cache

    def _cache_dir(self):
        cache_dir = pathlib.Path.home() / ".pdpolygonapi/ohlcv_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    def _cache_file(self, ticker, span, span_multiplier, year=""):
        if isinstance(year, int) and year > 1970 and year < 2100:
            return self._cache_dir() / (
                ticker + "." + str(span) + "." + str(span_multiplier) + "." + str(year) + ".csv.gz"
            )
        elif isinstance(year, int):
            raise ValueError("Bad year=" + str(year))
        else:
            warnings.warn(
                "\n=========\n"
                + "This REALLY should NOT happen anymore!  Rather, always cache by YEAR!\n"
                + "===========\n"
            )
            return self._cache_dir() / (
                ticker + "." + str(span) + "." + str(span_multiplier) + ".csv.gz"
            )

    def clear_ohlcv_cache(self, ticker):
        cleared = []
        p = self._cache_dir()
        for child in p.iterdir():
            if (ticker == "all" or
                ((tlen := len(ticker)+1) > 1 and ticker+"." == child.name[0:tlen])
               ):
                print("==> rm", child)
                child.unlink()
                cleared.append(child.name)
        return cleared

    def fetch_ohlcvdf(
        self,
        ticker,
        start=-30,
        end=0,
        span="day",
        market="regular",
        cache=None,
        span_multiplier=1,
        tz="US/Eastern",
        show_request=False,
    ):
        """
        Given an ticker, fetch and return the OHLCV data (Open, High, Low, Close,
        and Volume) for that ticker as a Pandas DataFrame with a DatetimeIndex.
        Fetch the data within the `start` and `end` dates specified, and for the `span`
        specified (where `span` is the amount of time between adjacent OHLCV data points).
        Valid spans are 'second','minute','hour','day','week','month', 'quarter', and 'year'.

        Parameters
        ----------
        ticker (str): Ticker symbol for the security

        start: Earliest date to be include.  May be specified as:
               `int` : 0=today, <0 number of days before today, >0 number of days after today
                       Note that when `start` is an `int`, then it specifies a number of
                       *days* regardless of what `span` is.
               `str` : Any datetime string recognized by Pandas, for example 'YYYY-MM-DD'
                       or 'YYYY-MM-DD HH:MM:SS'
               Default value is -30 (days)

        end:   Latest date to be include.  May be specified as:
               `int` : 0=today, <0 number of days before today, >0 number of days after today
                       Note that when `end` is an `int`, then it specifies a number of
                       *days* regardless of what `span` is.
               `str` : Any datetime string recognized by Pandas, for example 'YYYY-MM-DD'
                       or 'YYYY-MM-DD HH:MM:SS'
               Default value is 0 (today)

        span (str)   : Time between adjacent data points.  Valid spans are:
                       'second','minute','hour','day','week','month', 'quarter', 'year'.

        market (str) : 'regular' or 'all' (Default is 'regular')
                       'regular' provide data only from 9:30 till 16:00.
                       'all'     include also data from extended-hours trading.

        cache (bool) : Create and/or use cache files.  Cache files are under
                       `Path.home()/.pdpolygonapi/ohlcv_cache/` keyed by
                       ticker symbol, span, span_multiplier, and year.

        span_multiplier (int): If span_multiplier > 1 then the time between adjacent
                        data points is (span * span_multipler).  The span_multiplier
                        is passed to Polygon.io
                        NOTE: When span_multiplier is passed to Polygon.io then
                              it is possible to have some time periods missing data,
                              whereas when span multiplication is done via resampling
                              then empty time periods can be *back* filled, or foward
                              filled.  As of June 2025 resampling was removed from
                              this interface; users wanting resampled data should do
                              resampling on their own after calling this api.

        tz (str)     :  Time Zone for data returned.  Default is 'US/Eastern'

        Returns
        -------
        DataFrame of OHLCV data for `ticker`, with a DatetimeIndex based on the specified
        `span` and `span_multiplier`

        """
        #  def fetch_ohlcvdf(self,ticker,start=-30,end=0,span='day',market='regular',cache=False,
        #                    span_multiplier=1,tz='US/Eastern',show_request=False):

        #  -------------------------------
        #  USE LAZY FORMATING FOR LOGGING:
        #  -------------------------------
        #   In [24]: logging.error("ticker=%s, date=%s, i=%s, x=%s" % (t,pydt,i,x))
        #   ERROR:root:ticker=SPY, date=2025-06-09 22:12:44.237548, i=12345, x=1.2345
        #   
        #   In [25]: logging.error("ticker=%s, date=%s, i=%s, x=%s",t,pydt,i,x)
        #   ERROR:root:ticker=SPY, date=2025-06-09 22:12:44.237548, i=12345, x=1.2345

        self.logger.debug("fetch_ohlcvdf: ticker=%s, start=%s, end=%s",ticker,start,end)
        self.logger.debug("fetch_ohlcvdf: span=%s, span_multiplier=%s",span,span_multiplier)
        self.logger.debug("fetch_ohlcvdf: market=%s, cache=%s",market,cache)
        self.logger.debug("fetch_ohlcvdf: tz=%s, show_request=%s",tz,show_request)

        valid_markets = ("regular", "all")
        if market not in valid_markets:
            raise ValueError("market must be one of " + str(valid_markets))

        valid_spans = ("second", "minute", "hour", "day", "week", "month", "quarter", "year")
        if span not in valid_spans:
            raise ValueError("span must be one of " + str(valid_spans))

        if not isinstance(cache, bool):
            cache = self.cache_initializer

        if span == "second" and cache:
            cache = False
            warnings.warn(
                "\n=========\n"
                + "cache will not be used for less than minutely data.\n"
                + "===========\n"
            )

        if not isinstance(span_multiplier, int):
            warnings.warn(
                "\n=========\n"
                + "span_multiplier ("
                + str(span_multiplier)
                + ") must be integer.\n"
                + "Resetting span_multipler to 1\n"
                + "===========\n"
            )
            span_multiplier = 1

        # --------------------------------------------
        # Note that polygon.io REST api accepts dates
        # in YYYY-MM-DD format, but NOT with HH:MM:SS
        # To specify hours, minutes, and seconds, the
        # dates/times must be converted to millisecond
        # unix timestamps:

        end_msts = self._input_to_mstimestamp(end, "end")
        start_msts = self._input_to_mstimestamp(start, 0)

        # print(f"  end=\"{end}\"      end_msts={end_msts}")
        # print(f"start=\"{start}\"  start_msts={start_msts}")

        req = (
            "https://api.polygon.io/v2/aggs/ticker/"
            + ticker
            + "/range/"
            + str(span_multiplier)
            + "/"
            + span
            + "/"
            + start_msts
            + "/"
            + end_msts
            + "?"
            + "adjusted=true&sort=asc&limit=50000&apiKey="
            + self.APIKEY
        )

        if show_request:
            print("req=\n", req[: req.find("&apiKey=")] + "&apiKey=***")

        if cache:
            y0 = self._input_to_datetime(start).year
            y1 = self._input_to_datetime(end).year
            years = [y for y in range(y0, y1 + 1)]
        else:
            years = None

        self.logger.debug(f"years={years}, start,end={start},{end}")
        self.logger.debug(f"req={req[: req.find('&apiKey=')]}&apiKey=***")

        def regular_market(tempdf):
            if span in ("hour", "minute", "second") and market == "regular":
                dlist = np.unique(tempdf.index.date)
                # print('dlist=',dlist)
                mktdf = pd.DataFrame(columns=tempdf.columns)
                mktdf.index.name = tempdf.index.name
                for d in dlist:
                    t1 = pd.Timestamp(d, tz="US/Eastern") + pd.Timedelta(hours=9, minutes=30)
                    t1 = t1.tz_convert(tz).tz_localize(tz=None)
                    t2 = pd.Timestamp(d, tz="US/Eastern") + pd.Timedelta(hours=16)
                    t2 = t2.tz_convert(tz).tz_localize(tz=None)
                    # print(t1,t2,'\n',tempdf.loc[t1:t2].head(),'\n')
                    # self.logger.info(f'len(mktdf)={len(mktdf)}  mktdf:\n{mktdf.head(3)}{mktdf.tail(3)}\n\n')
                    if len(mktdf) < 1:
                        mktdf = pd.concat([tempdf.loc[t1:t2]])
                    else:
                        mktdf = pd.concat([mktdf, tempdf.loc[t1:t2]])
                    # print('len(mktdf)=',len(mktdf),'mktdf:\n',mktdf.head(3),mktdf.tail(3),'\n\n')
                tempdf = mktdf
            return tempdf

        def request_data():
            rjson = self._req_get_json(req)

            tempdf = self._json_response_to_ohlcvdf(span, rjson, tz=tz)
            if tempdf is None or len(tempdf) == 0:
                return tempdf

            # print('len(tempdf)=',len(tempdf))
            if "next_url" in rjson:
                while "next_url" in rjson:
                    self.logger.info('\n==> GETTING NEXT URL: "' + rjson["next_url"] + '"')
                    nxtr = rjson["next_url"] + "&apikey=" + self.APIKEY
                    rjson = self._req_get_json(nxtr)
                    tempdf = pd.concat([tempdf, self._json_response_to_ohlcvdf(span, rjson)])

            # print('len(tempdf)=',len(tempdf))
            # print(tempdf.head(2))
            # print(tempdf.tail(2))
            # =======================================================
            # From:
            # https://support.tastyworks.com/support/solutions/articles/43000435335-options-that-trade-until-3-15-pm-central-
            #
            # When do equity and ETF options stop trading?
            #
            # MOST STOP TRADING AT THE MARKET CLOSE, HOWEVER SOME TRADE 15-MIN. AFTER THE CLOSE
            # Options on most underlyings close when the market closes at 3:00 pm Central Time (Chicago Time).
            # However, there is a handful of ETF options that trade until 3:15 pm Central Time
            # or 15-minutes after the equity markets close (3:00 pm Central).
            #
            # OPTIONS THAT TRADE UNTIL 3:15 PM CENTRAL TIME (CHICAGO TIME)
            # AUM, AUX, BACD, BPX, BRB, BSZ, BVZ, CDD, CITD, DBA, DBB, DBC, DBO, DBS, DIA, DJX, EEM, EFA, EUI, EUU,
            # GAZ, GBP, GSSD, IWM, IWN, IWO, IWV, JJC, JPMD, KBE, KRE, MDY, MLPN, MNX, MOO, MRUT, MSTD, NDO, NDX, NZD,
            # OEF, OEX, OIL, PZO, QQQ, RUT, RVX, SFC, SKA, SLX, SPX, SPX (PM Expiration), SPY, SVXY, UNG, UUP, UVIX,
            # UVXY, VIIX, VIX, VIXM, VIXY, VXEEM, VXST, VXX, VXZ, XEO, XHB, XLB, XLE, XLF, XLI, XLK, XLP, XLU, XLV,
            # XLY, XME, XRT, XSP, XSP (AM Expiration), & YUK
            #
            # EXCEPTION FOR CASH-SETTLED INDICES
            # All PM-settled day of expiration options for NDX, RUT, SPX, OEX and XEO stop trading at 3:00 pm.
            # -------------------------------------------------------
            # Despite the above information, for now we will continue
            # to return 9:30 - 16:00 for "regular" trading hours.
            # =======================================================

            return regular_market(tempdf)

        def request_data_to_cache(year=None):
            # Determine cache_start and cache_end
            today = self._input_to_datetime(0, "end")
            if year:
                cache_end = datetime.datetime(year, 12, 31)
                cache_start = datetime.datetime(year, 1, 1)
            else:
                raise ValueError("request_data_to_cache() should always have YEAR")
                cache_end = today
                cache_start = datetime.datetime(today.year, 1, 1)
            cache_start = cache_start.replace(hour=9, minute=30, second=0, microsecond=0)
            cache_end = cache_end.replace(hour=16, minute=00, second=00, microsecond=0)
            #cache_start = cache_start.replace(hour=0, minute=0, second=0, microsecond=1)
            #cache_end = cache_end.replace(hour=23, minute=59, second=59, microsecond=999999)
            self.logger.debug(f"cache_start={cache_start}, cache_end={cache_end}")
            tempdf = self.fetch_ohlcvdf(
                ticker,
                start=cache_start,
                end=cache_end,
                span=span,
                span_multiplier=span_multiplier,
                show_request=True,
                cache=False,
            )
            # When requesting aggregate ohlcv data from polygon.io, if the start timestamp
            # or end timestamp is in the middle of an aggregate, then that entire aggregate
            # will be included in the response.  The causes the following affects:

            # (1) If the *start* timestamp is within an aggregate then, since the entire
            # aggregate is included in the response, and since the timestamp returned for
            # an aggregate is always the Open time of the aggregate, then it is possible
            # that the first aggregate returned will have a timestamp *earlier* than the start
            # time we requested.  Since we are caching by year, this may result in the first
            # row being from the previous year.  For the cache, we will filter this out.

            # (2) If the *end* timestamp is within an aggregate then, since the entire aggregate
            # is included, and since the timestamp returned for an aggregate is always the Open
            # time of the aggregate, then (even though the timestamp of the last aggregate will
            # be at or earlier than the requested end time) it's possible that the last aggregate
            # will contain data *beyond* then end time requested.  While this may result in some
            # of the data being in the following year, (particularly the Close, but possibly also
            # the High or Low) we are not going  to worry about it because the timestamp of the
            # aggregate will be in this year, and as long as we filter out the first aggregate,
            # then we will be consistent by keeping the last aggregate, even if some of its data
            # is in the following year and/or beyond the requested end timestamp.

            # (3) Note also that, depending on the size of the aggregate, if the requested end 
            # time is close to the present or in the future, it is possible that the last aggregate
            # returned from polygon.io *will not be complete* since in order to be complete it
            # would have to contain data from the future. 

            # Example:
            #  Request *weekly* data from 2021-01-01 till 2021-06-30:
            #  Weekly aggregates go from Sunday 00:00:00 thru Saturday 23:59:99.

            #  The requested start time, 2021-01-01, is a Friday, so
            #  The first aggregate will have a timestamp of 2020-12-27 and will contain data
            #  through 2020-12-31 (since there is no trading on 2021-01-01 nor 2021-01-02)

            #  The requested end time, 2021-06-30, is a Wednesday, so:
            #  The last aggregate will have a timestamp of 2021-05-30 and will contain data
            #  through 2021-06-05 (or if no trading on Saturday, then through 2021-06-04).

            #  Note that if the above request (*weekly* data from 2021-01-01 to 2021-06-30)
            #  were made when the actual true live datetime was 2021-06-29 12:00 then the
            #  last aggregate returned would be incomplete: although it is a "weekly" aggregate
            #  it would contain data only through the present time (2021-06-29 12:00) or somewhat
            #  less if one is enrolled for delayed prices.  Thus the final "weekly"
            #  aggregate would effectively be Open at 2021-06-27 and Close at 2021-06-29.

            # print("type(tempdf.index[0].date()), tempdf.index[0].date()=",
            #       type( tempdf.index[0].date()), tempdf.index[0].date())
            # print("type(cache_start.date()), cache_start.date()=",
            #        type(cache_start.date()), cache_start.date())
            # ix_start = 1 if tempdf.index[0].date() < cache_start.date() else 0
            # return tempdf.iloc[ix_start:]
            return tempdf

        def _str_df(prefix, df):
            if len(df) > 1:
                return prefix + "=\n" + str(df.iloc[[0, -1]]) + " \n" + str(len(df)) + " rows.\n"
            else:
                return prefix + "=\n" + str(df) + " \n" + str(len(df)) + " rows.\n"

        if cache:
            # determine current trade date and year, because we age out
            # the current year cache each trade date.  However for now
            # we will use NY time to determine current trade date.
            # later we can implement time zones:
            ts_now = pd.Timestamp.now()
            cache_files = []
            if years:
                for year in years:
                    cache_files.append(self._cache_file(ticker, span, span_multiplier, year))
            else:
                print("not `years` ... THIS SHOULD NOT HAPPEN ANYMORE!")
                raise RuntimeError("if `cache`, then should always have `years`")

            tempdf = pd.DataFrame()
            for jj, cf in enumerate(cache_files):
                year = years[jj] if years else None
                if cf in PolygonApi.cached_files:
                    # We have already, at least once in this instance, encountered
                    # this cache file; therefore this `read_csv()` should work ok:
                    # (no need to get lock on cache file).
                    tempdf = pd.concat([tempdf, pd.read_csv(cf, index_col=0, parse_dates=True)])
                    self.logger.debug(f"jj={jj} read-in cache file:{cf}")
                    self.logger.debug(_str_df(f"jj={jj} tempdf(0)", tempdf))
                    continue
                try:
                    # We haven't seen the file yet during this run (instance) but the cache
                    # file _may_ exist from a previous run, so look for it (with a lock):
                    PolygonApi.cflock_acquire()
                    stat_result = pathlib.Path(cf).stat()
                    size = stat_result.st_size
                    if not size > 0:
                        print("Found zero byte cache file:" + cf)
                        raise RuntimeError("Found zero byte cache file:" + cf)
                    if year == ts_now.year:
                        # The current year's cache file should be replaced
                        # (or appended to) each new trading day.  Should
                        # review this code and write a pytest for it as well.
                        mtime = pd.Timestamp.fromtimestamp(stat_result.st_mtime)
                        start_trade_date = ts_now.replace(
                            hour=9, minute=30, second=0, microsecond=0, nanosecond=0
                        )
                        # print("TDB: year=", year)
                        # print("TDB: ts_now=", ts_now)
                        # print("TDB: start_trade_date=", start_trade_date)
                        # print("TDB: mtime=", mtime)
                        if ts_now > start_trade_date:
                            current_trade_date = ts_now.date()
                        else:
                            current_trade_date = (ts_now - pd.tseries.offsets.BDay(1)).date()
                        # print("TDB: mtime.date()=", mtime.date())
                        # print("TDB: current_trade_date=", current_trade_date)
                        if mtime.date() < current_trade_date:
                            print(
                                "Current Trade Date=",
                                current_trade_date,
                                " Cached Trade Date=",
                                mtime.date(),
                            )
                            print("Refresh cache file:", cf)
                            cf.unlink(missing_ok=True)
                            raise ValueError  # jump to except:
                    self.logger.info(f"jj={jj}: using cache file {cf}, size={size}")
                    nextdf = pd.read_csv(cf, index_col=0, parse_dates=True)
                    self.logger.debug(_str_df("nextdf(1)", nextdf))
                    if year == years[-1] and len(nextdf) > 0:
                        end_dtm = self._input_to_datetime(end)
                        dtm1 = nextdf.index[-1]
                        self.logger.debug(f"year,end_dtm,dtm1={year},{end_dtm},{dtm1}")
                        if end_dtm > dtm1:
                            self.logger.warning(f"cache ({cf}) too short ... requesting more data.")
                            raise RuntimeError(f"cache ({cf}) too short ... requesting more data.")
                    #self.logger.info("BEF: tempdf.iloc[[0,1,-2,-1]]=\n%s",tempdf.iloc[[0,1,-2,-1]])
                    #self.logger.info("BEF: nextdf.iloc[[0,1,-2,-1]]=\n%s",nextdf.iloc[[0,1,-2,-1]])
                    tlen = len(tempdf)
                    tempdf = pd.concat([tempdf, nextdf])
                    #self.logger.info("AFT: tempdf.iloc[%s:%s]=\n%s",tlen-2,tlen+3,tempdf.iloc[tlen-2:tlen+3])
                    PolygonApi.cached_files[cf] = True
                    PolygonApi.cflock_release()
                except:
                    self.logger.info(f"cache not found, requesting data for cache file: {cf}")
                    cache_df = request_data_to_cache(year)
                    if isinstance(cache_df, pd.DataFrame):  # zero length ok to cache
                        self.logger.info(f"caching data to file: {cf}")
                        cache_df.to_csv(cf)
                        tempdf = pd.concat([tempdf, cache_df])
                        PolygonApi.cached_files[cf] = True
                        self.logger.debug(_str_df("tempdf(2)", tempdf))
                    PolygonApi.cflock_release()

            if len(tempdf) > 1:
                end_dtm = self._input_to_datetime(end, "end")
                start_dtm = self._input_to_datetime(start, 0)
                dtm0 = tempdf.index[0]
                dtm1 = tempdf.index[-1]

                dd = 0.05 * (dtm1 - dtm0)
                if start_dtm.date() < (dtm0 - dd).date():
                    self.logger.debug(f"dtm0,dtm1={dtm0}, {dtm1}")
                    warnings.warn(
                        "Requested START "
                        + str(start_dtm)
                        + " outside of cache (i.e. unavailable)\n"
                        + "cache file(s): "
                        + str(cache_files)
                    )

                # The time stamp on polygon.io aggregates corresponds to the Open of
                # the aggregate; therefore the aggregate Closes just before the begining
                # of the next aggregate and so we allow 99.9% past the last time stamp:
                dd = 0.999 * (dtm1 - dtm0)
                if end_dtm.date() > (dtm1 + dd).date():
                    self.logger.debug(f"dtm0,dtm1={dtm0}, {dtm1}")
                    warnings.warn(
                        "Requested END "
                        + str(end_dtm)
                        + " outside of cache (i.e. unavailable)\n"
                        + "cache file(s): "
                        + str(cache_files)
                    )
                self.logger.debug(_str_df("tempdf(3)", tempdf))
                self.logger.debug(f"type(start_dtm)={type(start_dtm)}")
                self.logger.debug(f"type(end_dtm)={type(end_dtm)}")
                self.logger.debug(f"start_dtm:end_dtm={start_dtm}:{end_dtm}")
                tempdf = tempdf.loc[start_dtm:end_dtm]
        else:
            tempdf = request_data()

        #print("BOTTOM of fetch_ohlcv(): tempdf.iloc[[0,1,-2,-1]]=\n",tempdf.iloc[[0,1,-2,-1]])

        start_date = self._input_to_datetime(start).date()
        first_date = tempdf.index[0].date()
        ix_start = 1 if first_date < start_date else 0
        return tempdf.iloc[ix_start:]

    class OptionsChain:
        """
        Options Chain class
        """

        def __init__(self, underlying, tickers):
            self._underlying = underlying
            self._tickers = tickers
            expvalues = self._tickers.index.get_level_values(0).unique().sort_values().values
            expindex = pd.DatetimeIndex(expvalues)
            self._expirations = pd.Series(expvalues, index=expindex, name="Expiration")
            self._strikes = {}
            for xp in expvalues:
                self._strikes[xp] = (
                    self._tickers.loc[xp].index.get_level_values(0).unique().to_series()
                )

        @property
        def tickers(self):
            return self._tickers

        @property
        def underlying(self):
            return self._underlying

        @property
        def expirations(self):
            return self._expirations

        @property
        def strikes(self):
            return self._strikes

        def get_strikes_by_expiration(self, expiration):
            exp = str(_PolygonApiBase._input_to_datetime(_PolygonApiBase, expiration).date())
            if str(exp) in self._strikes:
                return self._strikes[exp]
            return None

    def fetch_options_chain(
        self, underlying, start_expiration=None, end_expiration=None, show_request=False
    ):
        """
        Given an underlying ticker, fetch all of the options for that underlying
        that have expiration dates between (and including) `start_expiration` and
        `end_expiration`.

        Parameters
        ----------
        underlying (str): Ticker symbol for the options underlying

        start_expiration: Earliest expiration date to include.  May be specified as:
                          `None`: Treated as today; only UN-expired options will be returned.
                          `int` : 0=today, <0 number of days before today, >0 number of days after today
                          `str` : Any string date recognized by Pandas, for example 'YYYY-MM-DD'
                          Default value is `None`

        end_expiration:   Latest expiration date to include.  May be specified as:
                          `None`: return ALL future expirations that exist.
                          `int` : 0=today, <0 number of days before today, >0 number of days after today
                          `str` : Any string date recognized by Pandas, for example 'YYYY-MM-DD'
                          Default value is `None`

        Returns
        -------
        an `OptionsChain` object that contains:
            underlying:   Ticker symbol of the underlying security
            tickers:      Dataframe of option tickers keyed by expiration date and strike price
            expirations:  List of all expiration dates in this options chain.
            strikes(exp): Method to return a list of strike prices given an expiration date
                          from the list of expiration dates within the OptionChain object.

            Note: The default values (`None`) for `start_expiration` and `end_expiration` will return
                  ALL existing UN-expired options (and no expired options).

        """
        if start_expiration is None:
            start_expiration = 0

        start_dtm = self._input_to_datetime(start_expiration, adj=0)

        if end_expiration is None:
            end_dtm = None
        else:
            end_dtm = self._input_to_datetime(end_expiration, adj=0)

        expval = []
        today = datetime.datetime.today().date()
        if start_dtm.date() < today:
            expval.append("true")
        if end_dtm is None or end_dtm.date() >= today or start_dtm.date() >= today:
            expval.append("false")

        def _gen_contracts_request(underlying, expired, start_dtm, end_dtm):
            req = (
                "https://api.polygon.io/v3/reference/options/contracts?"
                + "underlying_ticker="
                + underlying
                + "&expired="
                + expired
                + "&expiration_date.gte="
                + start_dtm.strftime("%Y-%m-%d")
            )
            if end_dtm is not None:
                req += "&expiration_date.lte=" + end_dtm.strftime("%Y-%m-%d")
            req += "&limit=1000&apiKey=" + self.APIKEY
            return req

        totdf = None
        for expired in expval:
            req = _gen_contracts_request(underlying, expired, start_dtm, end_dtm)
            if show_request:
                print("req=\n", req[: req.find("&apiKey=")] + "&apiKey=***")
            else:
                print("Requesting options chain data ...", end="")
            rd = self._req_get_json(req)
            if "results" not in rd:
                totdf = pd.DataFrame(
                    columns=["contract_type", "expiration_date", "strike_price", "ticker"]
                )
                break
            rdf = pd.DataFrame(rd["results"])
            rdf.drop(
                [
                    "cfi",
                    "exercise_style",
                    "primary_exchange",
                    "shares_per_contract",
                    "underlying_ticker",
                ],
                axis=1,
                inplace=True,
            )
            if totdf is None:
                totdf = pd.DataFrame(columns=rdf.columns)
            totdf = pd.concat([totdf, rdf])
            while "next_url" in rd:
                print(".", end="")
                req = rd["next_url"] + "&apiKey=" + self.APIKEY
                r = requests.get(req)
                rd = r.json()
                if "results" not in rd:
                    break
                rdf = pd.DataFrame(rd["results"])
                rdf.drop(
                    [
                        "cfi",
                        "exercise_style",
                        "primary_exchange",
                        "shares_per_contract",
                        "underlying_ticker",
                    ],
                    axis=1,
                    inplace=True,
                )
                totdf = pd.concat([totdf, rdf])
            if not show_request:
                print()

        totdf.rename(
            columns={
                "contract_type": "Type",
                "expiration_date": "Expiration",
                "strike_price": "Strike",
                "ticker": "Ticker",
            },
            inplace=True,
        )

        totdf.set_index(["Expiration", "Strike", "Type"], inplace=True)
        totdf.sort_index(inplace=True)

        # oc = OptionsChain(underlying,totdf)
        return self.OptionsChain(underlying, totdf.Ticker)

    def fetch_quotes(self, ticker, str_date, show_request=False):
        # Format nanosecond UTC unix timestamps:
        ts1 = str(
            int(
                pd.Timestamp(str_date + " 09:30", tz="US/Eastern").tz_convert("UTC").timestamp()
                * (10**9)
            )
        )
        ts2 = str(
            int(
                pd.Timestamp(str_date + " 16:00", tz="US/Eastern").tz_convert("UTC").timestamp()
                * (10**9)
            )
        )

        req = (
            "https://api.polygon.io/v3/quotes/" + ticker + "?"
            "timestamp.gte="
            + ts1
            + "&timestamp.lte="
            + ts2
            + "&limit=50000&"
            + "apiKey="
            + self.APIKEY
        )

        print(
            'Requesting quote data for "' + ticker + '"\n',
            "from",
            pd.Timestamp(int(ts1)),
            " to ",
            pd.Timestamp(int(ts2)),
            "UTC",
        )

        if show_request:
            print("req=\n", req[: req.find("&apiKey=")] + "&apiKey=***")

        rd = requests.get(req).json()

        self.logger.debug(f"response status: {rd['status']}")  # ,'  response keys:',rd.keys())

        columns = [
            "Ask",
            "AsizeA",
            "AsizeM",
            "AsizeH",
            "AsizeL",
            "Bid",
            "BsizeA",
            "BsizeM",
            "BsizeH",
            "BsizeL",
            "Count",
        ]
        ix = pd.DatetimeIndex([], name="Timestamp")
        empty = pd.DataFrame(columns=columns, index=ix)

        if rd["status"] != "OK":
            print("Got status =", rd["status"])
            return empty

        if "results" not in rd:
            print("No results in response.")
            return empty

        if len(rd["results"]) == 0:
            print("zero length results.")
            return empty

        qdf = pd.DataFrame(rd["results"])
        ts = [pd.Timestamp(t, tz="UTC") for t in qdf.sip_timestamp]
        qdf.index = pd.DatetimeIndex(ts)
        self.logger.debug(f"received {len(qdf)} quotes so far ...")

        while rd["status"] == "OK" and "next_url" in rd:
            self.logger.debug(f"getting next_url ... ")
            req = rd["next_url"] + "&apikey=" + self.APIKEY
            rd = requests.get(req).json()
            self.logger.debug(f"response status: {rd['status']}")
            tdf = pd.DataFrame(rd["results"])
            ts = [pd.Timestamp(t, tz="UTC") for t in tdf.sip_timestamp]
            tdf.index = pd.DatetimeIndex(ts)
            qdf = pd.concat([qdf, tdf])
            if "next_url" in rd:
                self.logger.debug(f"received {len(qdf)} quotes so far ...")
            else:
                self.logger.debug(f"received {len(qdf)} quotes.")

        if rd["status"] != "OK":
            print("WARNING: status=", rd["status"])

        qdf.sort_index(inplace=True)

        qdf.rename(
            columns=dict(
                ask_price="Ask",
                ask_size="AsizeA",
                bid_price="Bid",
                bid_size="BsizeA",
                sequence_number="Count",
            ),
            inplace=True,
        )
        qdf.index.name = "Timestamp"

        # add columns for size: median, high, low:
        qdf["AsizeM"] = qdf["AsizeA"]
        qdf["AsizeH"] = qdf["AsizeA"]
        qdf["AsizeL"] = qdf["AsizeA"]
        qdf["BsizeM"] = qdf["BsizeA"]
        qdf["BsizeH"] = qdf["BsizeA"]
        qdf["BsizeL"] = qdf["BsizeA"]

        print("resampling to 1S intervals ...")
        sqdf = (
            qdf.resample("1S")
            .agg(
                {
                    "Ask": "mean",  # Ask Price
                    "AsizeA": "mean",  # Ask Size Average
                    "AsizeM": "median",  # Ask Size Median
                    "AsizeH": "max",  # Ask Size High
                    "AsizeL": "min",  # Ask Size Low
                    "Bid": "mean",  # Bid Price
                    "BsizeA": "mean",  # Bid Size Average
                    "BsizeM": "median",  # Bid Size Median
                    "BsizeH": "max",  # Bid Size High
                    "BsizeL": "min",  # Bid Size Low
                    "Count": "count",
                }  # Count Bids/Asks in this second.
            )
            .dropna(how="any")
        )

        sqdf.AsizeA = [int(round(item, 0)) for item in sqdf.AsizeA]
        sqdf.BsizeA = [int(round(item, 0)) for item in sqdf.BsizeA]
        sqdf.AsizeM = [int(round(item, 0)) for item in sqdf.AsizeM]
        sqdf.BsizeM = [int(round(item, 0)) for item in sqdf.BsizeM]
        sqdf.AsizeH = [int(round(item, 0)) for item in sqdf.AsizeH]
        sqdf.BsizeH = [int(round(item, 0)) for item in sqdf.BsizeH]
        sqdf.AsizeL = [int(round(item, 0)) for item in sqdf.AsizeL]
        sqdf.BsizeL = [int(round(item, 0)) for item in sqdf.BsizeL]
        sqdf.Ask = [round(item, 2) for item in sqdf.Ask]
        sqdf.Bid = [round(item, 2) for item in sqdf.Bid]
        sqdf.Count = [int(round(item, 0)) for item in sqdf.Count]

        sqdf.index = sqdf.index.tz_convert("US/Eastern").tz_localize(None)

        print("returning", len(sqdf), "quotes.")

        return sqdf


##########################################################################################
#
#  Copyright 2023, Daniel Goldfarb, dgoldfarb.github@gmail.com
#
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use
#  this package and its associated files except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#  A copy of the License may also be found in the package repository.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
##########################################################################################
