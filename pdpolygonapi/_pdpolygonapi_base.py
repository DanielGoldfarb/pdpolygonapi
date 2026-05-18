#!/usr/bin/env python
# coding: utf-8

# ---
#  base class for accessing polygon.io REST api.
#  do NOT call this class directly.
# ---

import datetime
import requests
import time
import warnings

import pandas as pd

from pdpolygonapi._exceptions import PolygonApiError


# Rate-limit retry policy: total attempts (1 initial + 5 retries = 6).  At a
# 12-second sleep between attempts this covers ~60s, comfortably past the
# Polygon-free-tier 60s rate-limit window.
_RATE_LIMIT_MAX_ATTEMPTS = 6
_RATE_LIMIT_SLEEP_SECONDS = 12

# One-shot retry policy for any other non-OK status: a single retry after this
# brief sleep.  Kept short because production-tier failures (NOT_AUTHORIZED,
# transient 5xx) generally won't recover on the same connection, and we want to
# fail loudly rather than mask flakes.
_NON_OK_RETRY_SLEEP_SECONDS = 2


def _redact_apikey(req):
    """Return ``req`` with the API key replaced by ``***``.

    Handles both ``apiKey=`` and ``apikey=`` query-string forms used by Polygon.
    """
    for marker in ("&apiKey=", "&apikey=", "?apiKey=", "?apikey="):
        idx = req.find(marker)
        if idx != -1:
            # Find end of the api-key value (next ampersand or end-of-string).
            end = req.find("&", idx + 1)
            if end == -1:
                return req[: idx + len(marker)] + "***"
            return req[: idx + len(marker)] + "***" + req[end:]
    return req


def _make_default_error_stats():
    return {
        "fetches_attempted": 0,
        "fetches_succeeded": 0,
        "retries_attempted": 0,
        "retries_succeeded": 0,
        "non_ok_responses": {},
        "exceptions_raised": 0,
        "cache_writes": 0,
        "cache_writes_skipped_due_to_error": 0,
    }


class _PolygonApiBase:
    _OHLCV_COLMAP = dict(o="Open", h="High", l="Low", c="Close", v="Volume")  # ,vw='VolWgtPx')

    def __init__(self):
        self.APIKEY = None
        self._initial_timeout = 6  # seconds
        self._retry_timeout = 12  # seconds
        self._retry_timeout_raise = False
        self._last_response = None  # last JSON dict returned by _req_get_json
        self.error_stats = _make_default_error_stats()

    def _input_to_datetime(self, input, adj=None):
        if isinstance(input, int):
            dtm = datetime.datetime.today() + datetime.timedelta(days=input)
        elif isinstance(input, str):
            dtm = pd.Timestamp(input).to_pydatetime()
        else:
            dtm = input
        if adj == 0:
            dtm = dtm.replace(hour=0, minute=0, second=0, microsecond=0)
        elif adj is not None:
            dtm = dtm.replace(hour=23, minute=59, second=59, microsecond=99999)
        return dtm

    def _input_to_mstimestamp(self, input, adj=None):
        dtm = self._input_to_datetime(input, adj)
        # print('dtm=',dtm)
        return str(int(dtm.timestamp() * 1000))

    # ------------------------------------------------------------------
    # _req_get_json
    # ------------------------------------------------------------------
    #
    # Contract (Layer 1 of the 2026-05-18 fix; see ONBOARDING_SUPPLEMENT_2026_05_17.md):
    #
    #   * HTTP timeout: existing graduated-timeout retry preserved.
    #   * JSON status OK or DELAYED, or a response containing a "results" key:
    #     return the dict.
    #   * Free-tier rate-limit response (error contains "exceeded" AND "upgrade"):
    #     sleep _RATE_LIMIT_SLEEP_SECONDS and retry, up to a total of
    #     _RATE_LIMIT_MAX_ATTEMPTS attempts.  On exhaustion: raise PolygonApiError.
    #   * Any other non-OK status (NOT_AUTHORIZED, ERROR, missing status, ...):
    #     wait briefly and retry exactly once.  If the retry succeeds, log at
    #     ERROR level with the magic word INVESTIGATE so the operator notices,
    #     and return the data.  If the retry also fails: raise PolygonApiError.
    #
    # ``error_stats`` is updated as a side-effect (Layer 4).
    # ------------------------------------------------------------------
    def _req_get_json(self, req):
        self.error_stats["fetches_attempted"] += 1
        try:
            rjson = self._req_get_json_inner(req)
        except PolygonApiError:
            self.error_stats["exceptions_raised"] += 1
            raise
        self.error_stats["fetches_succeeded"] += 1
        self._last_response = rjson
        return rjson

    def _req_get_json_inner(self, req):
        rate_limit_attempts = 0
        non_ok_retry_used = False

        while True:
            rjson = self._http_get_json(req)

            # ---- 1. Success ----------------------------------------------
            if self._is_success_response(rjson):
                return rjson

            status = rjson.get("status", "")
            # Record the non-OK status for forensics.
            key = status if status else "(missing)"
            self.error_stats["non_ok_responses"][key] = (
                self.error_stats["non_ok_responses"].get(key, 0) + 1
            )

            # ---- 2. Free-tier rate-limit ---------------------------------
            if self._is_rate_limit_response(rjson):
                rate_limit_attempts += 1
                if rate_limit_attempts >= _RATE_LIMIT_MAX_ATTEMPTS:
                    self.logger.error(
                        "Rate-limit exhausted after %d attempts: %s",
                        rate_limit_attempts,
                        _redact_apikey(req),
                    )
                    raise PolygonApiError(
                        status=status or "RATE_LIMIT_EXHAUSTED",
                        request_url=_redact_apikey(req),
                        response_json=rjson,
                    )
                self.logger.warning(
                    "Max requests per minute exceeded; waiting %ds (attempt %d/%d).",
                    _RATE_LIMIT_SLEEP_SECONDS,
                    rate_limit_attempts,
                    _RATE_LIMIT_MAX_ATTEMPTS,
                )
                time.sleep(_RATE_LIMIT_SLEEP_SECONDS)
                continue

            # ---- 3. Any other non-OK status: one-shot retry --------------
            if not non_ok_retry_used:
                non_ok_retry_used = True
                self.error_stats["retries_attempted"] += 1
                self.logger.warning(
                    "Non-OK response status=%r; retrying once after %ds: %s",
                    status,
                    _NON_OK_RETRY_SLEEP_SECONDS,
                    _redact_apikey(req),
                )
                time.sleep(_NON_OK_RETRY_SLEEP_SECONDS)

                retry_json = self._http_get_json(req)
                if self._is_success_response(retry_json):
                    self.error_stats["retries_succeeded"] += 1
                    # Loud signal — the operator MUST notice this and investigate.
                    self.logger.error(
                        "First attempt got status=%r; retry succeeded — INVESTIGATE: %s",
                        status,
                        _redact_apikey(req),
                    )
                    return retry_json

                # Retry also failed.  Record + raise.
                retry_status = retry_json.get("status", "")
                key2 = retry_status if retry_status else "(missing)"
                self.error_stats["non_ok_responses"][key2] = (
                    self.error_stats["non_ok_responses"].get(key2, 0) + 1
                )
                raise PolygonApiError(
                    status=retry_status or status,
                    request_url=_redact_apikey(req),
                    response_json=retry_json,
                )

            # ---- 4. Shouldn't get here (already used retry) --------------
            raise PolygonApiError(
                status=status,
                request_url=_redact_apikey(req),
                response_json=rjson,
            )

    # ------------------------------------------------------------------
    # Helpers split out so they are easy to mock in tests.
    # ------------------------------------------------------------------
    def _http_get_json(self, req):
        """One HTTP GET with the graduated-timeout retry, returns a JSON dict.

        Transient network failures \u2014 ``requests.exceptions.Timeout`` and
        ``requests.exceptions.ConnectionError`` (DNS hiccup, connection
        reset, unreachable peer, etc.) \u2014 are both routed through the same
        one-shot graduated retry.  On full failure this preserves the legacy
        behavior of returning ``{}`` when ``_retry_timeout_raise`` is False
        \u2014 but ``{}`` is not a success response, so it now propagates
        through the Layer-1 non-OK path (one-shot retry, then
        PolygonApiError) instead of being silently accepted.
        """
        # Both timeouts and connection errors are transient network problems.
        _RETRYABLE = (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
        try:
            r = requests.get(req, timeout=self._initial_timeout)
        except _RETRYABLE as e:
            sreq = _redact_apikey(req)
            self.logger.warning(
                "%s at %ds, retrying with %ds timeout...",
                type(e).__name__,
                self._initial_timeout,
                self._retry_timeout,
            )
            try:
                r = requests.get(req, timeout=self._retry_timeout)
            except _RETRYABLE as e2:
                if self._retry_timeout_raise:
                    raise
                self.logger.error(
                    "%s at %ds (retry failed): %s",
                    type(e2).__name__,
                    self._retry_timeout,
                    sreq,
                )
                return {}
        return r.json()

    def _is_success_response(self, rjson):
        """A response is a 'success' if it has an OK/DELAYED status, OR if it
        has a ``results`` key (some Polygon endpoints omit ``status`` on success).
        """
        if not isinstance(rjson, dict) or not rjson:
            return False
        status = rjson.get("status")
        if status in ("OK", "DELAYED"):
            return True
        if status is None and "results" in rjson:
            # Some endpoints omit "status" on success; treat presence of
            # "results" as success only when "status" is absent (not when it's
            # an explicit error status).
            return True
        return False

    def _is_rate_limit_response(self, rjson):
        if "results" in rjson:
            return False
        err = rjson.get("error", "")
        if not isinstance(err, str):
            return False
        return "exceeded" in err and "upgrade" in err

    def _json_response_to_ohlcvdf(self, span, rjson, tz="US/Eastern"):
        if "results" not in rjson:
            #  When have no results, so we will return an empty data frame,
            #  but we want to determine if there was an error that we should log,
            #  or whether this simply means we have valid empty results, for
            #  example, the ticker was not traded in the requested timeframe.
            message = None
            if len(rjson) == 0:
                # Empty dict returned from timeout - already logged in _req_get_json
                pass
            elif "message" in rjson:
                message = rjson["message"]
            elif "error" in rjson:
                message = rjson["error"]
            elif "status" not in rjson or (
                "status" in rjson and rjson["status"] != "OK" and rjson["status"] != "DELAYED"
            ):
                message = "No results returned (status missing or unknown)"
            else:
                #  This means we have valid empty results,
                #  for example, ticker was not traded for specified datetime.
                pass

            if message and "status" in rjson:
                message += " (" + rjson["status"] + ")"

            if message: self.logger.warning(message)
            return pd.DataFrame(columns=self._OHLCV_COLMAP.values())

        tempdf = pd.DataFrame(rjson["results"])

        tempdf.index = [pd.Timestamp(t * 1000000.0, tz="UTC") for t in tempdf.t.values]
        if span in ("day", "week", "month", "quarter", "year"):
            tempdf.index = pd.DatetimeIndex([t.date() for t in tempdf.index])
        else:  # span is hour, minute or second:
            tempdf.index = tempdf.index.tz_convert(tz=tz).tz_localize(tz=None)

        tempdf.rename(columns=self._OHLCV_COLMAP, inplace=True)
        tempdf.index.name = rjson["ticker"]
        retdf = tempdf[self._OHLCV_COLMAP.values()]
        return retdf


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
