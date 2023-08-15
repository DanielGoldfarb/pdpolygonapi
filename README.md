# pdpolygonapi

Python API for Polygon.io returning Pandas Objects.

This package provides a class, **`PolygonApi`** that provides methods for accessing 
polygon.io's REST API and returning data in various Pandas objects (for example, DataFrames and/or Series).

This package is in no way endorsed by, nor associated with, polygon.io or its ownership.

Some of the methods include:

   - `fetch_ohlcvdf()`       ... Returns a DataFrame containing OHLCV data with a Datetime Index
   - `fetch_options_chain()` ... Returns a DataFrame of all options for an underlying for a range of expiration dates.
                               The DataFrame is Indexed by Expiration Date, Strike, and Put/Call
   - `fetch_quotes()`        ... Returns Bid/Ask BidSize/AskSize data for a Ticker, with a Datetime Index

### [For more detailed information see the apiPolygon jupyter notebook in the examples folder](https://github.com/DanielGoldfarb/pdpolygonapi/blob/main/examples/apiPolygon.ipynb).

---


```
NOTE: It is strongly recommended that you store your Polygon.io APIKEY in an ENVIRONMENT VARIABLE on
      your local machine, and pass the NAME of the ENVIRONMENT VARIABLE into the class constructor.

      (Alternatively you can pass your APIKEY directly into the class constructor, but for security
       reasons this is not recommended.  At any rate, AVOID putting your APIKEY into your code).
```
## See [**`examples/apiPolygon.ipynb`**](https://github.com/DanielGoldfarb/pdpolygonapi/blob/main/examples/apiPolygon.ipynb)


---

Copyright 2023, Daniel Goldfarb, dgoldfarb.github@gmail.com

Licensed under the Apache License, Version 2.0 (the "License");  you may not 
use this package and its associated files except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
A copy of the License may also be found in the pdpolygon package repository.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
