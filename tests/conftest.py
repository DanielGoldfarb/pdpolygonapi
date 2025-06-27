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


@pytest.fixture
def pdpgapi():
    logger.info(f"In pdpgapi test fixture: logger.getEffectiveLevel()={logger.getEffectiveLevel()}")
    # print(f"logger={logger}")
    # print(f"logger.handlers={logger.handlers}")
    # print(f"logger.parent={logger.parent}")
    # print(f"logger.parent.handlers={logger.parent.handlers}")
    # print(f"logger.parent.parent={logger.parent.parent}")

    lowest_level = 100
    for h in logger.handlers + logger.parent.handlers:
        if h.level > 0: lowest_level = min(h.level, lowest_level)
    api_env_key = "POLYGON_API"
    # print("lowest_level=",lowest_level)
    api = PolygonApi(envkey=api_env_key, loglevel=lowest_level, wait=True, cache=False)
    if api.APIKEY is None or api.APIKEY == "":
        logger.error(f"Polygon API key is not set in the environment variable [{api_env_key}]")
    assert isinstance(api.APIKEY, str) and len(api.APIKEY) > 10
    return api
