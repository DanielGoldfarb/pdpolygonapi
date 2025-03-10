import logging
import pytest
from pdpolygonapi import PolygonApi

print(f" Using logger({__name__})")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DATE_FORMAT = "%Y-%m-%d"


def pytest_addoption(parser):
    parser.addoption(
        "--re-golden", action="store_true", help='Regenerate reference data files ("golden" copy).'
    )


@pytest.fixture
def regolden(request):
    return request.config.getoption("--re-golden")


@pytest.fixture
def pdpgapi():
    api_env_key = "POLYGON_API"
    api = PolygonApi(envkey=api_env_key, loglevel="INFO", wait=True, cache=False)
    if api.APIKEY is None or api.APIKEY == "":
        logger.error(f"Polygon API key is not set in the environment variable [{api_env_key}]")
    assert isinstance(api.APIKEY, str) and len(api.APIKEY) > 10
    return api
