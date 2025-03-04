"""
-- Created by: Ashok Kumar Pant
-- Email: asokpant@gmail.com
-- Created on: 14/08/2024
"""
import os

from pdpolygonapi import PolygonApi


def test_api_initialization():
    api_key = "POLYGON_API_KEY"
    api = PolygonApi(apikey=api_key)
    assert api.APIKEY == api_key


def test_api_initialization1():
    api_key = "POLYGON_API_KEY"
    api_key_env_var = "POLYGON_API_KEY_ENV"
    os.environ.setdefault(api_key_env_var, api_key)
    api = PolygonApi(envkey=api_key_env_var)
    assert api.APIKEY == api_key
