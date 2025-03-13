import logging
import pytest
from pdpolygonapi import PolygonApi

logger = logging.getLogger("test_pdpgapi")
logger.setLevel(logging.DEBUG)


def pytest_addoption(parser):
    parser.addoption(
        "--re-golden",
        action="store",
        default=None,
        help='Regenerate reference data: filename, list of filenames, or "all"',
    )


@pytest.fixture
def regolden(request):
    r = request.config.getoption("--re-golden")
    r = [s.strip() for s in r.split(",")] if r is not None else [None]
    return r


@pytest.fixture
def pdpgapi():
    api_env_key = "POLYGON_API"
    api = PolygonApi(envkey=api_env_key, loglevel="INFO", wait=True, cache=False)
    if api.APIKEY is None or api.APIKEY == "":
        logger.error(f"Polygon API key is not set in the environment variable [{api_env_key}]")
    assert isinstance(api.APIKEY, str) and len(api.APIKEY) > 10
    return api
