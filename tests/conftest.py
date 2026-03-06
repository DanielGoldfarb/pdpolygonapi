import logging
import pytest
from pdpolygonapi import PolygonApi

logger = logging.getLogger("test_pdpgapi")
logger.setLevel(logging.DEBUG)


def pytest_addoption(parser):
    parser.addoption(
        "--regolden",
        action="store",
        default=None,
        help='Regenerate reference data: filename, list of filenames, "failures", or "all"',
    )


@pytest.fixture
def regolden(request):
    r = request.config.getoption("--regolden")
    r = [s.strip() for s in r.split(",")] if r is not None else [None]
    return r


def _get_lowest_log_level():
    lowest_level = 100
    for h in logger.handlers + logger.parent.handlers:
        if h.level > 0:
            lowest_level = min(h.level, lowest_level)
    return lowest_level


@pytest.fixture
def pdpgapi():
    api_env_key = "POLYGON_API"
    api = PolygonApi(envkey=api_env_key, loglevel=_get_lowest_log_level(), wait=True, cache=False)
    if api.APIKEY is None or api.APIKEY == "":
        logger.error(f"Polygon API key is not set in the environment variable [{api_env_key}]")
    assert isinstance(api.APIKEY, str) and len(api.APIKEY) > 10
    return api


@pytest.fixture
def pdpgapi_with_cache(tmp_path):
    """PolygonApi instance with both ohlcv and quote cache dirs under tmp_path.

    Tests using this fixture will NOT touch the real cache directories.
    """
    api_env_key = "POLYGON_API"
    api = PolygonApi(
        envkey=api_env_key,
        loglevel=_get_lowest_log_level(),
        wait=True,
        cache=False,
        ohlcv_cache_dir=tmp_path / "ohlcv_cache",
        quote_cache_dir=tmp_path / "quote_cache",
    )
    assert isinstance(api.APIKEY, str) and len(api.APIKEY) > 10
    return api
