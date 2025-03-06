"""
-- Created by: Ashok Kumar Pant
-- Email: asokpant@gmail.com
-- Created on: 14/08/2024
"""

import logging
import os

from pdpolygonapi import PolygonApi

###     def __init__(
###         self,
###         envkey: str | None = "POLYGON_API",
###         apikey: str | None = None,
###         loglevel: int | str = logging.WARNING,
###         wait: bool = True,
###         cache: bool = False,
###     ) -> None:


def test_api_init_01():
    """Test setting the api key explicitly"""
    api_key = "POLYGON_API_KEY"
    api = PolygonApi(apikey=api_key)
    assert api.APIKEY == api_key


def test_api_init_02():
    """Test setting the api key via environment key"""
    api_key = "POLYGON_API_KEY_123"
    api_key_env_var = "POLYGON_API_KEY_ENV"
    os.environ.setdefault(api_key_env_var, api_key)
    api = PolygonApi(envkey=api_key_env_var)
    assert api.APIKEY == api_key


def test_api_init_03():
    """Test setting the api key via DEFAULT environment key"""
    api_key = "POLYGON_API_KEY_456"
    api_key_env_var = "POLYGON_API"  # DEFAULT environment key
    existing_key = os.environ.get(api_key_env_var)
    os.environ[api_key_env_var] = api_key
    api = PolygonApi()
    os.environ[api_key_env_var] = existing_key
    assert api.APIKEY == api_key


def test_api_init_04():
    """Test setting apikey kwarg overrides environment key"""
    api_key = "POLYGON_API_KEY_789"
    api_key_env_var = "POLYGON_API_KEY_ENV"
    os.environ.setdefault(api_key_env_var, api_key)
    api = PolygonApi(envkey=api_key_env_var, apikey="9876543210")
    assert api.APIKEY != api_key
    assert api.APIKEY == "9876543210"


###     def __init__(
###         self,
###         envkey: str | None = "POLYGON_API",
###         apikey: str | None = None,
###         loglevel: int | str = logging.WARNING,
###         wait: bool = True,
###         cache: bool = False,
###     ) -> None:


def test_api_init_05():
    """Test remaining constructor kwargs"""
    api = PolygonApi(loglevel=None, wait=False, cache=True)
    assert api.logger.getEffectiveLevel() == logging.WARNING
    assert api.wait == False
    assert api.cache_initializer == True

    api = PolygonApi(loglevel="DEBUG")
    assert api.logger.getEffectiveLevel() == logging.DEBUG
    assert api.wait == True
    assert api.cache_initializer == False
