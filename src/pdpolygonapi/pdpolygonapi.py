#!/usr/bin/env python
# coding: utf-8

# ---
#  class for accessing polygon.io REST api.
# ---

import pandas as pd
import requests
import os
import datetime
import numpy as np
import warnings
import collections
from   pdpolygonapi._pdpolygonapi_base import _PolygonApiBase

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

    def __init__(self,envkey=None,apikey=None):
        if apikey is not None:
            self.APIKEY = apikey
        elif envkey is not None:
            self.APIKEY=os.environ.get(envkey)
        else:
            self.APIKEY=os.environ.get('POLYGON_API')
    
    def fetch_ohlcvdf(self,ticker,start=-30,end=0,span='day',market='regular',
                      span_multiplier=1,tz='US/Eastern',show_request=False):
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
                       
        span_multiplier (int):  Presently ignored.  When implemented, will resample data
                        to the specified multiple of span.  For example, if `span` is minute
                        and the multiplier is 5, then there will be one set of OHLCV data
                        every 5 minutes.
                        
        tz (str)     :  Time Zone for data returned.  Default is 'US/Eastern'    
                       
        Returns
        -------
        DataFrame of OHLCV data for `ticker`, with a DatetimeIndex based on the specified 
        `span` and `span_multiplier`

        """    
        
        valid_markets = ('regular','all')
        if market not in valid_markets:
            raise ValueError('market must be one of '+str(valid_markets))
            
        valid_spans = ('second','minute','hour','day',
                        'week','month','quarter','year')
        if span not in valid_spans:
            raise ValueError('span must be one of '+str(valid_spans))
            
        if span_multiplier != 1:
            warnings.warn('\n=========\n'+
                          'This version: span_multiplier != 1 ignored.\n'+
                          'Use DataFrame resampling instead.\n'+
                          '===========\n')
            span_multiplier = 1
            
        end_dtm   = self._input_to_mstimestamp(end,'end')
        start_dtm = self._input_to_mstimestamp(start,0)
                
        # The request always uses span_multiplier of 1.
        # If caller specifies span_multiplier then we use
        # Pandas resample to adjust the span.
        req=('https://api.polygon.io/v2/aggs/ticker/'+ticker+
             '/range/1/'+span+
             '/'+start_dtm+'/'+end_dtm+'?'+
             'adjusted=true&sort=asc&limit=50000&apiKey='+self.APIKEY)
        if show_request:
            print('req=\n',req[:req.find('&apiKey=')]+'&apiKey=***')
        else:
            print('requesting data ...')

        rjson = self._req_get_json(req)
        
        tempdf = self._json_response_to_ohlcvdf(span,rjson,tz=tz)
        if len(tempdf) == 0:
            return tempdf

        #print('len(tempdf)=',len(tempdf))
        if 'next_url' in rjson: 
            while 'next_url' in rjson:
                print('\n==> GETTING NEXT URL:',rjson['next_url'])
                req=rjson['next_url']+'&apikey='+self.APIKEY
                rjson = self._req_get_json(req)
                tempdf = pd.concat([tempdf,self._json_response_to_ohlcvdf(span,rjson)])
                #print('len(build)=',len(build))

        #print('len(tempdf)=',len(tempdf))
        #print(tempdf.head(2))
        #print(tempdf.tail(2))
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
        
        if span in ('hour','minute','second') and market == 'regular':
            dlist  = np.unique(tempdf.index.date)
            #print('dlist=',dlist)
            mktdf  = pd.DataFrame(columns=tempdf.columns)
            for d in dlist:
                t1 = pd.Timestamp(d,tz='US/Eastern') + pd.Timedelta(hours=9,minutes=30)
                t1 = t1.tz_convert(tz).tz_localize(tz=None)
                t2 = pd.Timestamp(d,tz='US/Eastern') + pd.Timedelta(hours=16)
                t2 = t2.tz_convert(tz).tz_localize(tz=None)
                #print(t1,t2,'\n',tempdf.loc[t1:t2].head(),'\n')
                mktdf = pd.concat([mktdf,tempdf.loc[t1:t2]])
                #print('len(mktdf)=',len(mktdf),'mktdf:\n',mktdf.head(3),mktdf.tail(3),'\n\n')
            tempdf = mktdf
        
#         if span_multiplier > 1:
#             smult = str(span_multiplier)
#             sdict = dict(second='S',minute='T',hour='H',day='D',
#                          week='W',month='M',quarter='Q',year='A')
#             freq  = smult+sdict[span]
#             if span == 'day' and span_multiplier=7:
#                freq = '1W'
#             ntdf = tempdf.resample('1W').agg(
#                  {'Open'  :'first',
#                   'High'  :'max',
#                   'Low'   :'min',
#                   'Close' :'last',
#                   'Volume':'sum'
#                  })
            
        return tempdf
    
    class OptionsChain:
        """
        Options Chain class
        """
        def __init__(self,underlying,tickers):
            self._underlying  = underlying
            self._tickers     = tickers
            expvalues = self._tickers.index.get_level_values(0).unique().sort_values().values
            expindex  = pd.DatetimeIndex(expvalues)
            self._expirations = pd.Series(expvalues,index=expindex,name='Expiration')
            self._strikes = {}
            for xp in expvalues:
                self._strikes[xp] = self._tickers.loc[xp].index.get_level_values(0).unique().to_series()

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

        def get_strikes_by_expiration(self,expiration):
            if str(expiration) in self._strikes:
                return self._strikes[expiration]
            return None
    

    def fetch_options_chain(self,underlying,start_expiration=None,end_expiration=None,show_request=False):
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

        start_dtm = self._input_to_datetime(start_expiration,adj=0)

        if end_expiration is None:
            end_dtm = None
        else:
            end_dtm = self._input_to_datetime(end_expiration,adj=0)

        expval = []
        today = datetime.datetime.today().date()
        if start_dtm.date() < today:
            expval.append('true')
        if end_dtm is None or end_dtm.date() >= today or start_dtm.date() >= today:
            expval.append('false')

        def _gen_contracts_request(underlying,expired,start_dtm,end_dtm):
            req=('https://api.polygon.io/v3/reference/options/contracts?'+
                 'underlying_ticker='+underlying+
                 '&expired='+expired+
                 '&expiration_date.gte='+start_dtm.strftime('%Y-%m-%d')
                )
            if end_dtm is not None:
                req += '&expiration_date.lte='+end_dtm.strftime('%Y-%m-%d')
            req += '&limit=1000&apiKey='+self.APIKEY
            return req

        totdf = None
        for expired in expval:
            req = _gen_contracts_request(underlying,expired,start_dtm,end_dtm)
            if show_request:
                print('req=\n',req[:req.find('&apiKey=')]+'&apiKey=***')
            else:
                print('Requesting data ...',end='')
            rd = self._req_get_json(req)
            if 'results' not in rd:
               totdf = pd.DataFrame(columns=['contract_type','expiration_date','strike_price','ticker'])
               break
            rdf = pd.DataFrame(rd['results'])
            rdf.drop(['cfi','exercise_style','primary_exchange','shares_per_contract','underlying_ticker'],axis=1,inplace=True)
            if totdf is None: totdf = pd.DataFrame(columns=rdf.columns)
            totdf = pd.concat([totdf,rdf])
            while 'next_url' in rd:
                print('.',end='')
                req=rd['next_url']+"&apiKey="+self.APIKEY
                r = requests.get(req)
                rd = r.json()
                if 'results' not in rd:
                    break
                rdf = pd.DataFrame(rd['results'])
                rdf.drop(['cfi','exercise_style','primary_exchange','shares_per_contract','underlying_ticker'],axis=1,inplace=True)
                totdf = pd.concat([totdf,rdf])
                
        totdf.rename(columns={'contract_type':'Type',
                              'expiration_date':'Expiration',
                              'strike_price':'Strike',
                              'ticker':'Ticker'},inplace=True)

        totdf.set_index(['Expiration','Strike','Type'],inplace=True)
        totdf.sort_index(inplace=True)
        
        #oc = OptionsChain(underlying,totdf)
        return self.OptionsChain(underlying,totdf.Ticker)
    

    def fetch_quotes(self,ticker,str_date,show_request=False):
        
        # Format nanosecond UTC unix timestamps:
        ts1 = str(int(pd.Timestamp(str_date+' 09:30',tz='US/Eastern').tz_convert('UTC').timestamp()*(10**9)))
        ts2 = str(int(pd.Timestamp(str_date+' 16:00',tz='US/Eastern').tz_convert('UTC').timestamp()*(10**9)))
    
        req = ('https://api.polygon.io/v3/quotes/'+ticker+'?'
               'timestamp.gte='+ts1+'&timestamp.lte='+ts2+'&limit=50000&'+
               'apiKey='+self.APIKEY)

        print('Requesting quote data for "'+ticker+'"\n',
              'from',pd.Timestamp(int(ts1)),' to ',pd.Timestamp(int(ts2)),'UTC')

        if show_request:
            print('req=\n',req[:req.find('&apiKey=')]+'&apiKey=***')
    
        rd  = requests.get(req).json()
        
        print('response status:',rd['status'])#,'  response keys:',rd.keys())
        
        if rd['status'] != 'OK':
            return None
        
        if 'results' not in rd:
            print('No results')
            return None
    
        qdf = pd.DataFrame(rd['results'])
        ts = [pd.Timestamp(t,tz='UTC') for t in qdf.sip_timestamp]
        qdf.index = pd.DatetimeIndex(ts)
        print('received',len(qdf),'quotes so far ...')
        
        while rd['status']=='OK' and 'next_url' in rd:
            print('getting next_url ... ',end='')#,rd['next_url'])
            req = rd['next_url']+'&apikey='+self.APIKEY
            rd  = requests.get(req).json()
            print('response status:',rd['status'],end='  ')#,'  response keys:',rd.keys())
            tdf = pd.DataFrame(rd['results'])
            ts = [pd.Timestamp(t,tz='UTC') for t in tdf.sip_timestamp]
            tdf.index = pd.DatetimeIndex(ts)
            qdf = pd.concat([qdf,tdf])
            if 'next_url' in rd:
                print('received',len(qdf),'quotes so far ...')
            else:
                print('received',len(qdf),'quotes.')
    
        if rd['status'] != 'OK':
            print('WARNING: status=',rd['status'])
    
        qdf.sort_index(inplace=True)
        
        qdf.rename(columns=dict(ask_price='Ask',
                                ask_size ='AsizeA',
                                bid_price='Bid',
                                bid_size ='BsizeA',
                                sequence_number='Count'),inplace=True)
        qdf.index.name = 'Timestamp'
        
        # add columns for size: median, high, low:
        qdf['AsizeM'] = qdf['AsizeA']
        qdf['AsizeH'] = qdf['AsizeA']
        qdf['AsizeL'] = qdf['AsizeA']
        qdf['BsizeM'] = qdf['BsizeA']
        qdf['BsizeH'] = qdf['BsizeA']
        qdf['BsizeL'] = qdf['BsizeA']
    
        print('resampling to 1S intervals ...')
        sqdf = qdf.resample('1S').agg({'Ask'    : 'mean',    # Ask Price
                                       'AsizeA' : 'mean',    # Ask Size Average
                                       'AsizeM' : 'median',  # Ask Size Median
                                       'AsizeH' : 'max',     # Ask Size High
                                       'AsizeL' : 'min',     # Ask Size Low
                                       'Bid'    : 'mean',    # Bid Price
                                       'BsizeA' : 'mean',    # Bid Size Average
                                       'BsizeM' : 'median',  # Bid Size Median
                                       'BsizeH' : 'max',     # Bid Size High
                                       'BsizeL' : 'min',     # Bid Size Low
                                       'Count'  : 'count'}   # Count Bids/Asks in this second.
                                     ).dropna(how='any')
        
        sqdf.AsizeA = [int(round(item,0)) for item in sqdf.AsizeA]
        sqdf.BsizeA = [int(round(item,0)) for item in sqdf.BsizeA]
        sqdf.AsizeM = [int(round(item,0)) for item in sqdf.AsizeM]
        sqdf.BsizeM = [int(round(item,0)) for item in sqdf.BsizeM]
        sqdf.AsizeH = [int(round(item,0)) for item in sqdf.AsizeH]
        sqdf.BsizeH = [int(round(item,0)) for item in sqdf.BsizeH]
        sqdf.AsizeL = [int(round(item,0)) for item in sqdf.AsizeL]
        sqdf.BsizeL = [int(round(item,0)) for item in sqdf.BsizeL]
        sqdf.Ask    = [    round(item,2)  for item in sqdf.Ask   ]
        sqdf.Bid    = [    round(item,2)  for item in sqdf.Bid   ]
        sqdf.Count  = [int(round(item,0)) for item in sqdf.Count ]
        
        sqdf.index = sqdf.index.tz_convert('US/Eastern').tz_localize(None)

        print('returning',len(sqdf),'quotes.')
        
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
