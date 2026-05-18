#!/usr/bin/env python
# coding: utf-8

"""Exceptions raised by pdpolygonapi.

Defined in their own module so they can be imported from both
``_pdpolygonapi_base`` and ``pdpolygonapi`` without a circular import.
"""


class PolygonApiError(Exception):
    """Raised when Polygon.io returns a non-OK status that cannot be recovered.

    Attributes
    ----------
    status : str
        The ``status`` field from the JSON response (e.g. ``"NOT_AUTHORIZED"``,
        ``"ERROR"``).  Empty string if no status was present.
    request_url : str
        The request URL that produced the failure.  API key is redacted.
    response_json : dict
        The full JSON dict returned by Polygon for the failed request.
    """

    def __init__(self, status, request_url, response_json):
        self.status = status
        self.request_url = request_url
        self.response_json = response_json
        msg = f"Polygon API returned status={status!r} for {request_url}: {response_json}"
        super().__init__(msg)


class PolygonCacheWriteFailed(Exception):
    """Raised when a cache write is aborted because the underlying fetch failed.

    The atomic unit of a cache write (a calendar month for quotes; a calendar
    year for OHLCV bars) could not be completed because at least one inner
    fetch raised.  Rather than write a partial unit to disk and pretend it is
    complete, the cache write is skipped and this exception is raised.

    Attributes
    ----------
    cache_file : str | None
        The path of the cache file that was not written.
    partial_df : pandas.DataFrame | None
        Whatever rows were successfully fetched before the failure.  Callers
        that wish to salvage may catch this exception and read ``partial_df``;
        the default behavior (uncaught) halts the run.
    cause : BaseException | None
        The underlying exception that aborted the fetch.
    """

    def __init__(self, cache_file=None, partial_df=None, cause=None):
        self.cache_file = cache_file
        self.partial_df = partial_df
        self.cause = cause
        parts = ["Cache write aborted"]
        if cache_file is not None:
            parts.append(f"for {cache_file}")
        if cause is not None:
            parts.append(f"due to {type(cause).__name__}: {cause}")
        if partial_df is not None:
            parts.append(f"({len(partial_df)} partial rows attached)")
        super().__init__(" ".join(parts))
