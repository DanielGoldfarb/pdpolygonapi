#!/usr/bin/env python
# coding: utf-8

# ---
#  base class for accessing polygon.io REST api.
#  do NOT call this class directly.
# ---

import pandas as pd
import requests
import datetime
import warnings

class _PolygonApiBase:
    _OHLCV_COLMAP = dict(o='Open',h='High',
                         l='Low',c='Close',
                         v='Volume')#,vw='VolWgtPx')

    def __init__(self):
        self.APIKEY = None    
            
    def _input_to_datetime(self,input,adj=None):
        if isinstance(input,int):
            dtm = datetime.datetime.today() + datetime.timedelta(days=input)
        elif isinstance(input,str):
            dtm = pd.Timestamp(input).to_pydatetime()
        else:
            dtm = input
        if adj == 0:
            dtm = dtm.replace(hour=0,minute=0,second=0,microsecond=0)
        elif adj is not None:
            dtm = dtm.replace(hour=23,minute=59,second=59,microsecond=99999)
        return dtm

    def _input_to_mstimestamp(self,input,adj=None):
        dtm = self._input_to_datetime(input,adj)
        print('dtm=',dtm)
        return str(int(dtm.timestamp() * 1000))
    
    def _req_get_json(self,req):
        r = requests.get(req)
        rjson = r.json()
        if 'results' not in rjson:
            message = rjson['message'] if 'message' in rjson else 'No results returned!'
            warnings.warn('\n'+message)
        return rjson
        
    def _json_response_to_ohlcvdf(self,span,rjson,tz='US/Eastern'):
        #print('rjson.keys()=',rjson.keys())
        if 'results' not in rjson:
            return pd.DataFrame(columns=self._OHLCV_COLMAP.values())
        
#         for key in ['ticker', 'queryCount', 'resultsCount', 'adjusted', 'status', 'request_id', 'count']:
#             print('rjson['+key+']=',rjson[key])
#         print('len(rjson[results])=',len(rjson['results']))
#         t = rjson['results'][0]['t']
#         print(pd.Timestamp(t*1000000.,tz='UTC'),':',end='')
#         print(rjson['results'][0])
#         t = rjson['results'][-1]['t']
#         print(pd.Timestamp(t*1000000.,tz='UTC'),':',end='')
#         print(rjson['results'][-1])
            
        tempdf = pd.DataFrame(rjson['results'])
        
        tempdf.index = [pd.Timestamp(t*1000000.,tz='UTC') for t in tempdf.t.values]
        if span in ('day','week','month','quarter','year'):
            tempdf.index = pd.DatetimeIndex([t.date() for t in tempdf.index])
        else: # span is hour, minute or second:
            tempdf.index = tempdf.index.tz_convert(tz=tz).tz_localize(tz=None)
        
        tempdf.rename(columns=self._OHLCV_COLMAP,inplace=True)
        tempdf.index.name = rjson['ticker']
        return tempdf[ self._OHLCV_COLMAP.values() ]


##########################################################################################
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
##########################################################################################
