import pytest
import timeit

from pdpolygonapi import PolygonApi

polygon = PolygonApi(envkey='POLYGON_API', wait=True)

spy = polygon.fetch_ohlcvdf('SPY', start='2023-05-19', end='2023-07-20',
                            span='day', show_request=True, cache=False)

tickers = ['O:SPY230728P00435000',
           'O:SPY230728C00440000',
           'O:SPY230721C00435000',
           'O:SPY230721P00440000']

print('len(spy)=', len(spy))

j1 = len(spy) - 20
j2 = j1 + 5


def onerun(span, span_multiplier, cache):
    start = spy.index[j1]
    for end in spy.index[j2:]:
        for ticker in tickers:
            df = polygon.fetch_ohlcvdf(ticker, start=start, end=end,
                                       span=span, show_request=False,
                                       span_multiplier=span_multiplier,
                                       cache=cache)
            # print(ticker,start,end,' len(df)=',len(df))


@pytest.mark.skip(reason="skip test until can be fleshed out correctly")
def test_cache_time_vs_no_cache():
    print()
    print('span=day,span_multiplier=1')
    t_nocache = timeit.timeit(lambda: onerun('day', 1, False), number=1)
    t_cache = timeit.timeit(lambda: onerun('day', 1, True), number=1)
    print(f'nocache={t_nocache:5.2f} cache={t_cache:5.2f}  nocache/cache={(t_nocache / t_cache):5.2f}')
    
    print()
    print('span=hour,span_multiplier=1')
    t_nocache = timeit.timeit(lambda: onerun('hour', 1, False), number=1)
    t_cache = timeit.timeit(lambda: onerun('hour', 1, True), number=1)
    print(f'nocache={t_nocache:5.2f} cache={t_cache:5.2f}  nocache/cache={(t_nocache / t_cache):5.2f}')
    
    print()
    print('span=minute,span_multiplier=15')
    t_nocache = timeit.timeit(lambda: onerun('minute', 15, False), number=1)
    t_cache = timeit.timeit(lambda: onerun('minute', 15, True), number=1)
    print(f'nocache={t_nocache:5.2f} cache={t_cache:5.2f}  nocache/cache={(t_nocache / t_cache):5.2f}')
    
    print()
    print('span=day,span_multiplier=1')
    t_nocache = timeit.timeit(lambda: onerun('day', 1, False), number=1)
    t_cache = timeit.timeit(lambda: onerun('day', 1, True), number=1)
    print(f'nocache={t_nocache:5.2f} cache={t_cache:5.2f}  nocache/cache={(t_nocache / t_cache):5.2f}')
