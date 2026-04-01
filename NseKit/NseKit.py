"""
NseKit — NSE India Data Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A clean, production-grade Python client for the National Stock Exchange (NSE)
of India.  Provides live quotes, EOD archives, F&O data, corporate filings,
SEBI circulars, and much more — all via a single ``Nse`` class.

Usage::

    from NseKit import Nse
    nse = Nse()
    nse.cm_live_equity_price_info("RELIANCE")
"""

import csv
import json
import logging
import os
import random
import re
import threading
import time
import warnings
import zipfile
from datetime import datetime, timedelta
from io import BytesIO, StringIO

import feedparser
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# I. Infrastructure & Utilities
# ══════════════════════════════════════════════════════════════════════════════

# ── Constants ──────────────────────────────────────────────────────────────────

_USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 Chrome/92.0.4515.107 Safari/537.36",
]

# ══════════════════════════════════════════════════════════════════════════════
# NseConfig — central control panel (read by ALL Nse instances)
# ══════════════════════════════════════════════════════════════════════════════
#
# Three behaviours are controlled here.  Change any field at any point
# before or after creating an Nse() instance — the next request will pick
# up the new value automatically.
#
# Usage (decorator-style, all at the module level):
#
#   from NseKit_antigravity import NseConfig
#
#   NseConfig.max_rps        = 2.0    # ≤ 2 requests per second
#   NseConfig.retries        = 4      # up to 5 total attempts per call
#   NseConfig.retry_delay    = 3.0    # 3 s base delay, doubles each retry
#   NseConfig.cookie_cache   = False  # always warm-up, never touch disk
#
# All fields have safe defaults so zero-config usage still works:
#   nse = Nse()   # same behaviour as before

class NseConfig:
    """
    Central configuration panel for all ``Nse`` instances.

    Every field is a **class attribute** — there are no instances.
    Changes take effect immediately for all subsequent HTTP calls in the
    same process.

    Attributes
    ----------
    max_rps : float
        Maximum requests per second, shared across all ``Nse`` instances
        and threads via a token-bucket rate limiter.  The lock is held
        only for token arithmetic — never during the sleep — so threads
        are never serialised for the full inter-request interval.
        Default ``3.0``.
    retries : int
        Number of *extra* retry attempts after the first failure.
        Total attempts = ``retries + 1``.  Default ``2`` (3 total tries).
    retry_delay : float
        Base sleep in seconds between retries.  Doubles on each subsequent
        attempt (exponential back-off).  Default ``2.0``.
    cookie_cache : bool
        ``True`` (default) — load cookies from ``~/.nsekit_session_cache.json``
        if fresh; save after every warm-up.
        ``False`` — always perform a full warm-up; never read or write
        the on-disk cache.

    Examples
    --------
    >>> NseConfig.max_rps      = 1.5
    >>> NseConfig.retries      = 3
    >>> NseConfig.retry_delay  = 1.0
    >>> NseConfig.cookie_cache = False
    >>> nse = Nse()
    """

    max_rps:     float = 3.0
    retries:     int   = 2
    retry_delay: float = 2.0
    cookie_cache: bool = True

    # ── Internal rate-limit state (not for direct use) ────────────────────
    # A single lock guards _tokens and _last_refill so that all Nse instances
    # in a process share one rate budget (NSE sees one IP).  The lock is held
    # only for the arithmetic — never during the sleep — so threads are not
    # serialised for the full inter-request interval.
    _lock:        threading.Lock = threading.Lock()
    _tokens:      float          = 3.0   # starts full (= max_rps)
    _last_refill: float          = 0.0   # monotonic timestamp

    def __init_subclass__(cls, **kw):
        raise TypeError("NseConfig is not meant to be subclassed")

    def __new__(cls):
        raise TypeError("NseConfig is a namespace — do not instantiate it")

def show_config(nse) -> None:
    """
    Print current NSE configuration and indicate whether each value
    comes from GLOBAL defaults or INSTANCE override.

    Example
    -------
    >>> import NseKit
    >>> nse = NseKit.Nse() or nse = NseKit.Nse(5.0 , 2, 1.0, True)
    >>> NseKit.show_config(nse)
    """
    print("\nCurrent NSE Configuration")
    print("-" * 40)

    fields = ["max_rps", "retries", "retry_delay", "cookie_cache"]

    for f in fields:
        inst_val = getattr(nse, f)
        global_val = getattr(NseConfig, f)

        source = "GLOBAL" if inst_val == global_val else "INSTANCE"

        print(f"{f:<12} : {inst_val!s:<6} [{source}]")


_PERIOD_DELTA: dict[str, timedelta] = {
    "1D":  timedelta(days=1),
    "1W":  timedelta(weeks=1),
    "1M":  timedelta(days=30),
    "3M":  timedelta(days=90),
    "6M":  timedelta(days=180),
    "1Y":  timedelta(days=365),
    "2Y":  timedelta(days=730),
    "5Y":  timedelta(days=1825),
    "10Y": timedelta(days=3650),
}

_SHORT_PERIODS: list[str] = ["1D", "1W", "1M", "3M", "6M", "1Y"]
_ALL_PERIODS:   list[str] = _SHORT_PERIODS + ["2Y", "5Y", "10Y", "YTD", "MAX"]
_DATE_PATTERN:  re.Pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
_YEAR_PATTERN:  re.Pattern = re.compile(r"^\d{4}$")

_MONTH_NUM: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

_NUM_MONTH: dict[int, str] = {v: k for k, v in _MONTH_NUM.items()}

# ── Business-growth column rename maps (used by _biz_growth_fetch) ────────────
#    Defined at module level so they are not re-created on every call.
_FO_YEARLY_RENAME: dict[str, str] = {
    "date": "FY",
    "Index_Futures_QTY": "Index_Futures_Qty",   "Index_Futures_VAL": "Index_Futures_Val",
    "Stock_Futures_QTY": "Stock_Futures_Qty",   "Stock_Futures_VAL": "Stock_Futures_Val",
    "Index_Options_QTY": "Index_Options_Qty",   "Index_Options_VAL": "Index_Options_Val",
    "Index_Options_PREM_VAL": "Index_Options_Prem_Val",
    "Stock_Options_QTY": "Stock_Options_Qty",   "Stock_Options_VAL": "Stock_Options_Val",
    "Stock_Options_PREM_VAL": "Stock_Options_Prem_Val",
    "F&O_Total_QTY": "FO_Total_Qty",            "F&O_Total_VAL": "FO_Total_Val",
    "TOTAL_TRADED_PREM_VAL": "Total_Traded_Prem_Val",
    "F&O_AVG_DAILYTURNOVER": "FO_Avg_Daily_Turnover",
}

_BIZ_GROWTH_RENAME_MAPS: dict = {
    "cm": {
        "yearly": {
            "GLY_MONTH_YEAR": "FY",
            "GLY_NO_OF_CO_LISTED": "No_of_Cos_Listed",
            "GLY_NO_OF_CO_PERMITTED": "No_of_Cos_Permitted",
            "GLY_NO_OF_CO_AVAILABLE": "No_of_Cos_Available",
            "GTY_NO_OF_TRADING_DAYS": "Trading_Days",
            "GTY_NO_OF_SECURITIES_TRADED": "Securities_Traded",
            "GTY_NO_OF_TRADES": "No_of_Trades",
            "GTY_TRADED_QTY": "Traded_Qty",
            "GTY_TURNOVER": "Turnover",
            "GTY_AVG_DLY_TURNOVER": "Avg_Daily_Turnover",
            "GTY_AVG_TRD_SIZE": "Avg_Trade_Size",
            "GTY_DEMAT_SECURITIES_TRADED": "Demat_Securities_Traded",
            "GTY_DEMAT_TURNOVER": "Demat_Turnover",
            "GTY_MKT_CAP": "Market_Cap",
        },
        "monthly": {
            "GLM_MONTH_YEAR": "Month",
            "GLM_NO_OF_CO_LISTED": "No_of_Cos_Listed",
            "GLM_NO_OF_CO_PERMITTED": "No_of_Cos_Permitted",
            "GLM_NO_OF_CO_AVAILABLE": "No_of_Cos_Available",
            "GTM_NO_OF_TRADING_DAYS": "Trading_Days",
            "GTM_NO_OF_SECURITIES_TRADED": "Securities_Traded",
            "GTM_NO_OF_TRADES": "No_of_Trades",
            "GTM_TRADED_QTY": "Traded_Qty",
            "GTM_TURNOVER": "Turnover",
            "GTM_AVG_DLY_TURNOVER": "Avg_Daily_Turnover",
            "GTM_AVG_TRD_SIZE": "Avg_Trade_Size",
            "GTM_DEMAT_SECURITIES_TRADED": "Demat_Securities_Traded",
            "GTM_DEMAT_TURNOVER": "Demat_Turnover",
            "GTM_MKT_CAP": "Market_Cap",
        },
        "daily": {
            "F_TIMESTAMP": "Date",
            "CDT_NOS_OF_SECURITY_TRADES": "No_of_Security_Trades",
            "CDT_NOS_OF_TRADES": "No_of_Trades",
            "CDT_TRADES_QTY": "Traded_Qty",
            "CDT_TRADES_VALUES": "Turnover",
        },
    },
    "fo": {
        "yearly":  _FO_YEARLY_RENAME,
        "monthly": _FO_YEARLY_RENAME,    # F&O monthly uses the same mapping as yearly
        "daily": {
            "date": "Date",
            "Index_Futures_QTY": "Index_Futures_Qty",   "Index_Futures_VAL": "Index_Futures_Val",
            "Stock_Futures_QTY": "Stock_Futures_Qty",   "Stock_Futures_VAL": "Stock_Futures_Val",
            "Index_Options_QTY": "Index_Options_Qty",   "Index_Options_VAL": "Index_Options_Val",
            "Index_Options_PREM_VAL": "Index_Options_Prem_Val",
            "Index_Options_PUT_CALL_RATIO": "Index_Options_PCR",
            "Stock_Options_QTY": "Stock_Options_Qty",   "Stock_Options_VAL": "Stock_Options_Val",
            "Stock_Options_PREM_VAL": "Stock_Options_Prem_Val",
            "Stock_Options_PUT_CALL_RATIO": "Stock_Options_PCR",
            "F&O_Total_QTY": "FO_Total_Qty",            "F&O_Total_VAL": "FO_Total_Val",
            "TOTAL_TRADED_PREM_VAL": "Total_Traded_Prem_Val",
            "F&O_Total_PUT_CALL_RATIO": "FO_Total_PCR",
        },
    },
}


# ── Module-level helpers ───────────────────────────────────────────────────────

def _parse_args(args: tuple, short: bool = False) -> dict:
    """
    Auto-detect ``from_date``, ``to_date``, ``period``, and ``symbol`` from
    a positional *args* tuple.

    Parameters
    ----------
    args : tuple
        Raw positional arguments forwarded by a public method.
    short : bool, optional
        If ``True``, only the short period set (1D–1Y) is accepted.
        Default is ``False``.

    Returns
    -------
    dict
        Dictionary with keys ``from_date``, ``to_date``, ``period``,
        ``symbol``; any un-detected key is ``None``.
    """
    valid_periods = _SHORT_PERIODS if short else _ALL_PERIODS
    # result keys must match the expected return signature
    result = {"from_date": None, "to_date": None, "period": None, "symbol": None}
    # (The above is typed as dict[str, Optional[str]] by modern checkers)

    for arg in args:
        if not isinstance(arg, str):
            continue
        if _DATE_PATTERN.match(arg):
            if not result["from_date"]:
                result["from_date"] = arg
            elif not result["to_date"]:
                result["to_date"] = arg
        elif arg.upper() in valid_periods:
            result["period"] = arg.upper()
        else:
            result["symbol"] = arg.upper()

    return result


def _resolve_dates(
    from_date: str | None = None,
    to_date:   str | None = None,
    period:    str | None = None,
    days:      int        = 365,
) -> tuple[str, str]:
    """
    Resolve a ``(from_date, to_date)`` pair from an optional *period* string
    or explicit date strings, falling back to *days* ago → today.

    Parameters
    ----------
    from_date : str, optional
        Explicit start date in ``DD-MM-YYYY`` format.
    to_date : str, optional
        Explicit end date in ``DD-MM-YYYY`` format.
    period : str, optional
        Shorthand period code e.g. ``"1Y"``, ``"YTD"``, ``"MAX"``.
    days : int, optional
        Fallback lookback in days when no other input is provided.
        Default is ``365``.

    Returns
    -------
    tuple[str, str]
        ``(from_str, to_str)`` both in ``DD-MM-YYYY`` format.
    """
    today    = datetime.now()
    today_s  = today.strftime("%d-%m-%Y")

    if period:
        p = period.upper()
        if p == "YTD":
            return datetime(today.year, 1, 1).strftime("%d-%m-%Y"), today_s
        if p == "MAX":
            return "01-01-2008", today_s
        delta = _PERIOD_DELTA.get(p, timedelta(days=days))
        return (today - delta).strftime("%d-%m-%Y"), today_s

    from_s = from_date or (today - timedelta(days=days)).strftime("%d-%m-%Y")
    to_s   = to_date   or today_s
    return from_s, to_s


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """Replace all *NA*, *NaN*, and infinite values with ``None``."""
    return df.replace([pd.NA, np.nan, float("inf"), float("-inf")], None)


def _csv_from_bytes(raw: bytes) -> pd.DataFrame:
    """
    Parse a CSV from raw bytes into a clean ``DataFrame``.

    Strips the UTF-8 BOM if present, trims whitespace and quotes from
    column names, and coerces comma-separated numeric strings.

    Parameters
    ----------
    raw : bytes
        Raw CSV bytes, optionally BOM-prefixed.

    Returns
    -------
    pd.DataFrame
        Parsed data with numeric columns already cast to ``float``/``int``.
    """
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]

    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError:
        content = raw.decode("latin-1", errors="ignore")

    df = pd.read_csv(StringIO(content))
    df.columns = [c.strip().replace('"', "") for c in df.columns]

    for col in df.select_dtypes(include="object").columns:
        cleaned = df[col].str.replace(",", "", regex=False).str.strip()
        numeric = pd.to_numeric(cleaned, errors="coerce")
        # Only replace if at least one value parsed — keep original strings otherwise
        if numeric.notna().any():
            df[col] = numeric.where(numeric.notna(), df[col])
        else:
            df[col] = cleaned

    return df


def _parse_year_month_args(
    args: tuple,
    month: int | None,
    year:  int | None,
    mode:  str,
    mode_values: tuple[str, ...],
) -> tuple[str, int | None, int | None]:
    """
    Shared positional-arg parser for methods that accept a mode string,
    an optional year, and an optional month.

    Handles:
    * A bare 4-digit year string (``"2025"``) → *year*
    * A month name prefix (``"OCT"``, ``"OCTOBER"``) → *month*
    * A ``"MON-YYYY"`` combined string → *month* and *year*
    * An integer year (1900–2100) → *year*
    * An integer month (1–12) → *month*
    * Any string in *mode_values* → *mode*

    Parameters
    ----------
    args : tuple
    month, year : int or None
        Seed values; overridden when found in *args*.
    mode : str
        Seed mode value.
    mode_values : tuple of str
        Upper-case strings that are recognised as mode choices
        (e.g. ``("MONTH_WISE", "DAY_WISE")``).

    Returns
    -------
    tuple
        ``(mode, month, year)``
    """
    for arg in args:
        if isinstance(arg, str):
            upper = arg.strip().upper()
            if upper in mode_values:
                mode = upper
            elif upper.isdigit() and len(upper) == 4:
                # Pure 4-digit string → year (e.g. "2025")
                year = int(upper)
            elif upper[:3] in _MONTH_NUM:
                # Month name prefix (e.g. "OCT", "OCTOBER")
                month = _MONTH_NUM[upper[:3]]
            elif "-" in upper:
                # "MON-YYYY" or "MM-YYYY" combined string; only reachable when
                # upper is NOT all-digits (covered above) and NOT a bare month name.
                pts = upper.split("-")
                if len(pts) == 2:
                    if pts[0][:3] in _MONTH_NUM:
                        month = _MONTH_NUM[pts[0][:3]]
                    elif pts[0].isdigit():
                        month = int(pts[0])
                    if pts[1].isdigit():
                        year = int(pts[1])
        elif isinstance(arg, int):
            if 1900 <= arg <= 2100:
                year = arg
            elif 1 <= arg <= 12:
                month = arg
    return mode, month, year


def _parse_biz_growth_args(args: tuple, month: int | None, year: int | None, mode: str) -> tuple:
    """
    Shared argument parser for ``cm_dmy_biz_growth`` and ``fno_dmy_biz_growth``.

    Delegates to :func:`_parse_year_month_args` with the biz-growth mode set
    (``"YEARLY"``, ``"MONTHLY"``, ``"DAILY"``), then applies lowercase
    normalisation and safety defaults for month/year.

    Parameters
    ----------
    args : tuple
        Positional arguments forwarded from the caller.
    month : int or None
        Seed value for month (1–12); overridden if found in *args*.
    year : int or None
        Seed value for year (4-digit); overridden if found in *args*.
    mode : str
        Seed value for mode; overridden if found in *args*.

    Returns
    -------
    tuple
        ``(mode, month, year)`` with any overrides from *args* applied.
    """
    mode, month, year = _parse_year_month_args(
        args, month, year, mode, ("YEARLY", "MONTHLY", "DAILY")
    )
    # _parse_year_month_args returns mode upper-cased when matched; biz-growth
    # callers expect lowercase.
    if mode.upper() in ("YEARLY", "MONTHLY", "DAILY"):
        mode = mode.lower()
    # Safety defaults
    if month is None:
        month = datetime.now().month
    if year is None:
        year = datetime.now().year
    return mode, month, year


def _parse_settlement_args(args: tuple, from_year: int | None, to_year: int | None, period: str | None) -> tuple:
    """
    Shared argument parser for monthly settlement report methods.

    Accepts 4-digit year strings (``"2024"``) and shorthand period strings
    like ``"2Y"`` from *args*.

    Parameters
    ----------
    args : tuple
        Positional arguments forwarded from the caller.
    from_year : int or None
        Seed start financial year; overridden if found in *args*.
    to_year : int or None
        Seed end financial year; overridden if found in *args*.
    period : str or None
        Shorthand period like ``"1Y"``, ``"2Y"``; overridden if found.

    Returns
    -------
    tuple
        ``(from_year, to_year, period)``.
    """
    for arg in args:
        if isinstance(arg, str):
            if _YEAR_PATTERN.match(arg):
                if not from_year:
                    from_year = int(arg)
                elif not to_year:
                    to_year = int(arg)
            elif arg.upper() in ("1Y", "2Y", "3Y", "5Y"):
                period = arg.upper()
    return from_year, to_year, period


def _unpack_args(
    args:      tuple,
    from_date: str | None = None,
    to_date:   str | None = None,
    period:    str | None = None,
    symbol:    str | None = None,
    short:     bool       = False,
) -> tuple[str | None, str | None, str | None, str | None]:
    """
    Merge keyword values with auto-detected positional *args*.

    Calls :func:`_parse_args` and merges the result with any already-supplied
    keyword values (keywords win over positional detection).

    Parameters
    ----------
    args : tuple
        Raw positional arguments forwarded by a public method.
    from_date, to_date, period, symbol : str or None
        Already-supplied keyword values; take priority over *args*.
    short : bool, optional
        Passed through to :func:`_parse_args`.

    Returns
    -------
    tuple
        ``(from_date, to_date, period, symbol)`` with merged values.
    """
    p = _parse_args(args, short=short)
    return (
        from_date or p["from_date"],
        to_date   or p["to_date"],
        period    or p["period"],
        symbol    or p["symbol"],
    )


def _clean_str(df: pd.DataFrame) -> pd.DataFrame:
    """Replace ``inf`` values with ``""`` and fill ``NaN`` with ``""``."""
    return df.fillna("").replace({float("inf"): "", float("-inf"): ""})


def _sort_dedup_dates(
    df:        pd.DataFrame,
    col:       str   = "Date",
    fmt:       str   = "%d-%b-%Y",
    ascending: bool  = False,
) -> pd.DataFrame:
    """
    Parse *col* to datetime, sort, drop duplicates, and reformat to *fmt*.

    Centralises the repeated pattern that appeared in ``_get_csv_session``,
    ``_hist_index_df``, and ``cm_hist_security_wise_data``.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the date column.
    col : str, optional
        Name of the date column. Default is ``"Date"``.
    fmt : str, optional
        ``strptime`` / ``strftime`` format string. Default is ``"%d-%b-%Y"``.
    ascending : bool, optional
        Sort order. Default is ``False`` (newest first).

    Returns
    -------
    pd.DataFrame
        Sorted, deduped copy with the date column formatted as strings.
    """
    if col not in df.columns:
        return df
    dates = pd.to_datetime(df[col], format=fmt, errors="coerce")
    keep = "last" if not ascending else "first"
    df = (df.assign(**{col: dates})
            .sort_values(col, ascending=ascending)
            .drop_duplicates(subset=[col], keep=keep)
            .reset_index(drop=True))
    df[col] = df[col].dt.strftime(fmt)
    return df


def _keep_cols(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Return *df* with only the columns in *cols* that actually exist."""
    existing = [c for c in cols if c in df.columns]
    return df[existing]


def _fmt_trade_date(trade_date: str, fmt: str = "%d%m%Y") -> str:
    """
    Parse a ``DD-MM-YYYY`` trade-date string and reformat it.

    Parameters
    ----------
    trade_date : str
        Date in ``DD-MM-YYYY`` format.
    fmt : str, optional
        Output ``strftime`` format. Default is ``"%d%m%Y"`` (used in most
        NSE archive URLs).

    Returns
    -------
    str
        Reformatted date string.
    """
    for in_fmt in ("%d-%m-%Y", "%d-%m-%y"):
        try:
            return datetime.strptime(trade_date, in_fmt).strftime(fmt)
        except ValueError:
            continue
    raise ValueError(f"Invalid date format: {trade_date}")


# ══════════════════════════════════════════════════════════════════════════════
# II. Core Nse Client
# ══════════════════════════════════════════════════════════════════════════════

# ── Main Class ────────────────────────────────────────────────────────────────

# Process-level in-memory cookie cache — shared across all Nse instances.
# Avoids redundant disk reads when multiple Nse() objects are created in the
# same process (e.g. scripts that instantiate Nse() in a loop).
# Structure: {"ts": float, "cookies": dict[str, str]}
_PROCESS_COOKIE_CACHE: dict = {}


class Nse:
    """
    NSE India data client.

    All public methods return a ``pd.DataFrame`` (or ``dict`` / ``list`` for
    raw JSON endpoints) and ``None`` on failure.  Failed HTTP calls are
    retried automatically up to 2 extra times by the underlying transport
    helpers before giving up.

    Quick start
    -----------
    ::

        from NseKit import Nse
        nse = Nse()
        df  = nse.cm_live_equity_price_info("RELIANCE")
        print(df)
    """

    _SEBI_URL = "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp"
    _SEBI_REFERER = (
        "https://www.sebi.gov.in/sebiweb/home/"
        "HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0"
    )
    # Constant headers for all SEBI POST requests — built once at class definition,
    # not reconstructed on every call.
    _SEBI_HEADERS: dict = {
        "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer":          (
            "https://www.sebi.gov.in/sebiweb/home/"
            "HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0"
        ),
        "Origin":           "https://www.sebi.gov.in",
        "X-Requested-With": "XMLHttpRequest",
    }

    _COOKIE_CACHE = os.path.join(os.path.expanduser("~"), ".nsekit_session_cache.json")
    _COOKIE_TTL   = 3600  # 1 hour

    # ── Construction ────────────────────────────────────────────────────────

    def __init__(
        self,
        max_rps:      float | None = None,
        retries:      int   | None = None,
        retry_delay:  float | None = None,
        cookie_cache: bool  | None = None,
    ):
        """
        Initialise the HTTP session and warm up NSE cookies.

        Parameters
        ----------
        max_rps : float, optional
            Limit requests per second for this instance. Defaults to
            ``NseConfig.max_rps``.
        retries : int, optional
            Number of retries for this instance. Defaults to
            ``NseConfig.retries``.
        retry_delay : float, optional
            Base delay between retries for this instance. Defaults to
            ``NseConfig.retry_delay``.
        cookie_cache : bool, optional
            Enable/Disable on-disk cookie caching for this instance.
            Defaults to ``NseConfig.cookie_cache``.
        """
        if max_rps is not None and max_rps <= 0:
            raise ValueError("max_rps must be positive")
        if retries is not None and retries < 0:
            raise ValueError("retries must be non-negative")
        if retry_delay is not None and retry_delay < 0:
            raise ValueError("retry_delay must be non-negative")

        self.max_rps      = max_rps      if max_rps      is not None else NseConfig.max_rps
        self.retries      = retries      if retries      is not None else NseConfig.retries
        self.retry_delay  = retry_delay  if retry_delay  is not None else NseConfig.retry_delay
        self.cookie_cache = cookie_cache if cookie_cache is not None else NseConfig.cookie_cache

        self.session = requests.Session()
        self._init_session()

    def rotate_user_agent(self) -> None:
        """Randomly rotate the ``User-Agent`` header to reduce rate-limiting."""
        self.headers["User-Agent"] = random.choice(_USER_AGENTS)

    @staticmethod
    def clear_cookie_cache() -> int:
        """
        Delete the on-disk cookie cache so the next ``Nse()`` instantiation
        performs a full warm-up.
        """
        deleted = 0
        for path in (Nse._COOKIE_CACHE, Nse._COOKIE_CACHE + ".tmp"):
            try:
                if os.path.exists(path):
                    os.remove(path)
                    deleted += 1
            except OSError:
                pass

        _PROCESS_COOKIE_CACHE.clear()   # also invalidate the in-memory cache

        if deleted:
            print(f"🧹 Cookie cache cleared — {deleted} file(s) deleted")
        else:
            print("ℹ️  No cookie cache files found")

        return deleted

    # ── Rate Limiting ───────────────────────────────────────────────────────

    def _throttle(self) -> None:
        """
        Block the calling thread just long enough to stay within the
        shared process-wide rate budget using a token-bucket algorithm.

        How it works
        ------------
        A shared bucket (``NseConfig._tokens``) holds up to ``max_rps``
        tokens.  Each call consumes one token.  Tokens refill continuously
        at ``max_rps`` per second based on elapsed wall time.

        The global lock is held **only** for the arithmetic — not during
        the sleep — so threads are never serialised for the full
        inter-request interval.  Multiple ``Nse`` instances respect the
        same budget because they all modify ``NseConfig._tokens``.
        """
        cap = self.max_rps          # bucket capacity = max burst
        while True:
            with NseConfig._lock:
                now     = time.monotonic()
                elapsed = now - NseConfig._last_refill
                # Refill tokens proportional to time passed, capped at capacity
                NseConfig._tokens      = min(cap, NseConfig._tokens + elapsed * cap)
                NseConfig._last_refill = now
                if NseConfig._tokens >= 1.0:
                    NseConfig._tokens -= 1.0
                    return          # token acquired — proceed immediately
                # Calculate how long until the next token arrives
                wait = (1.0 - NseConfig._tokens) / cap
            # Sleep outside the lock so other threads can refill/acquire concurrently
            time.sleep(wait)

    # ── Session Bootstrap & Cookie Management ───────────────────────────────

    def _init_session(self) -> None:
        """
        Build request headers and warm up NSE session cookies.

        Reads ``NseConfig.cookie_cache`` at call time — changing the flag
        between two ``Nse()`` instantiations takes effect immediately.
        When ``True``, a fresh on-disk cache skips the warm-up GETs.
        """
        self.headers = {
            "User-Agent":       random.choice(_USER_AGENTS),
            "Accept":           "application/json, text/javascript, */*; q=0.01",
            "Accept-Language":  "en-US,en;q=0.9",
            "Referer":          "https://www.nseindia.com/",
            "X-Requested-With": "XMLHttpRequest",
            "Connection":       "keep-alive",
            "Origin":           "https://www.nseindia.com",
        }
        if self.cookie_cache and self._load_cookies():
            return   # fresh cached cookies loaded — skip warm-up
        self._warm_up()

    def _load_cookies(self) -> bool:
        """
        Try to restore session cookies from the process-level in-memory cache
        first, then fall back to the on-disk JSON cache.

        Returns ``True`` when a fresh, non-expired cookie set was found and
        loaded into ``self.session``.  Returns ``False`` on any cache miss,
        expiry, or read error — the caller should then call :meth:`_warm_up`.

        Two-level lookup:
        1. ``_PROCESS_COOKIE_CACHE`` — shared dict in this process. Zero I/O.
           Populated whenever the on-disk cache is successfully read.
        2. On-disk JSON file — read only when the in-memory cache is cold or
           stale. Uses a plain JSON file (no DBM overhead, no file locking).
        """
        now = time.time()

        # ── Level 1: in-memory ────────────────────────────────────────
        mem = _PROCESS_COOKIE_CACHE
        if mem and now - mem.get("ts", 0.0) < self._COOKIE_TTL:
            for k, v in mem.get("cookies", {}).items():
                self.session.cookies.set(k, v)
            logger.debug("NseKit: session cookies loaded from memory cache")
            return True

        # ── Level 2: on-disk JSON ─────────────────────────────────────
        try:
            with open(self._COOKIE_CACHE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if now - data.get("ts", 0.0) < self._COOKIE_TTL:
                cookies = data.get("cookies", {})
                for k, v in cookies.items():
                    self.session.cookies.set(k, v)
                # Populate in-memory cache for subsequent Nse() instances
                _PROCESS_COOKIE_CACHE.clear()
                _PROCESS_COOKIE_CACHE.update(data)
                logger.debug("NseKit: session cookies loaded from disk cache")
                return True
        except Exception:
            pass   # missing, corrupt, or expired — fall through to warm-up
        return False

    def _save_cookies(self) -> None:
        """
        Persist the current session cookies to the on-disk JSON cache.

        Does nothing when ``cookie_cache`` is ``False``.  Writes atomically
        via a temp file + ``os.replace`` so a crash mid-write never leaves a
        corrupt cache.  Failures are logged at DEBUG level and never propagate.
        """
        if not self.cookie_cache:
            logger.debug("NseKit: cookie cache disabled — skipping save")
            return
        cookies = dict(self.session.cookies)
        if not cookies:
            logger.debug("NseKit: no cookies to save — skipping cache write")
            return
        tmp = self._COOKIE_CACHE + ".tmp"
        try:
            payload = {"ts": time.time(), "cookies": cookies}
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(tmp, self._COOKIE_CACHE)
            # Mirror to in-memory cache so the next Nse() in this process skips disk
            _PROCESS_COOKIE_CACHE.clear()
            _PROCESS_COOKIE_CACHE.update(payload)
            logger.debug("NseKit: %d cookie(s) saved to cache", len(cookies))
        except Exception as exc:
            logger.debug("NseKit: cookie cache write failed: %s", exc)
            try:
                os.remove(tmp)
            except OSError:
                pass

    def _warm_up(self) -> None:
        """
        Perform the two NSE warm-up GETs that establish a valid session.

        Each request runs in its own ``try/except`` so that a timeout on the
        second URL does not prevent the first URL's cookies from being saved.
        After both attempts (successful or not) :meth:`_save_cookies` is
        called unconditionally.
        """
        for url in (
            "https://www.nseindia.com",
            "https://www.nseindia.com/market-data/live-equity-market",
        ):
            try:
                self._throttle()
                self.session.get(url, headers=self.headers, timeout=10)
            except Exception as exc:
                logger.debug("NseKit: warm-up GET failed (%s): %s", url, exc)
        time.sleep(0.5)
        self._save_cookies()   # always attempt — save whatever cookies we got

    # ── Logging & Retry ─────────────────────────────────────────────────────

    @staticmethod
    def _log_error(tag: str, exc: Exception) -> None:
        """
        Print a standardised one-line error message and forward to the
        ``NseKit`` logger at WARNING level.

        Parameters
        ----------
        tag : str
            Short label identifying the calling method (e.g. ``"_get_json"``).
        exc : Exception
            The caught exception.
        """
        msg = f"[NseKit.{tag}] {type(exc).__name__}: {exc}"
        print(msg)
        logger.warning(msg)

    def _retry(self, fn, retries: int | None = None, delay: float | None = None):
        """
        Call *fn()* up to ``1 + retries`` times with exponential back-off.

        When *retries* or *delay* are ``None`` (the default) the values are
        read from ``NseConfig.retries`` and ``NseConfig.retry_delay``
        respectively — so changing those fields at the module level affects
        every subsequent HTTP call without touching any method signature.

        Parameters
        ----------
        fn : callable
            A zero-argument callable that may raise an exception.
        retries : int or None, optional
            Override ``NseConfig.retries`` for this call only.
        delay : float or None, optional
            Override ``NseConfig.retry_delay`` for this call only.

        Returns
        -------
        object
            Return value of ``fn()`` on the first successful call.

        Raises
        ------
        Exception
            Re-raises the last exception if all attempts are exhausted.
        """
        n   = self.retries     if retries is None else retries
        d   = self.retry_delay if delay   is None else delay
        last_exc: Exception | None = None
        for attempt in range(n + 1):
            try:
                return fn()
            except Exception as exc:
                last_exc = exc
                if attempt < n:
                    time.sleep(d * (2 ** attempt))
        raise last_exc

    # ── Core HTTP Transport ─────────────────────────────────────────────────

    def _warm_and_fetch(
        self,
        ref_url:     str,
        api_url:     str,
        params:      dict | None = None,
        timeout:     int = 10,
        api_timeout: int | None = None,
    ) -> requests.Response:
        """
        Single warm-up GET → session API GET, returning the ``Response``.

        Centralises three cross-cutting concerns for all HTTP callers:

        1. ``rotate_user_agent()`` — randomises UA on every call.
        2. Throttle + warm-up GET on *ref_url* (establishes cookies in
           ``self.session`` automatically via the ``requests.Session`` jar).
        3. Throttle + data GET on *api_url* — session already carries the
           cookies; no manual ``cookies=`` kwarg needed.

        The caller is responsible for error handling / retry wrapping.
        Raises ``requests.HTTPError`` on non-2xx responses.

        Parameters
        ----------
        ref_url : str
            Page used to establish session cookies.
        api_url : str
            Actual data endpoint.
        timeout : int, optional
            Timeout for the warm-up GET. Default is ``10``.
        api_timeout : int or None, optional
            Timeout for the data GET; falls back to *timeout* when ``None``.

        Returns
        -------
        requests.Response
            The response from *api_url*.
        """
        if api_timeout is None:
            api_timeout = timeout
        self.rotate_user_agent()   # centralised here; callers no longer need to call it
        # Only fire the warm-up GET when the session jar is empty — if cookies
        # were loaded from cache we can skip straight to the data request.
        if not self.session.cookies:
            self._throttle()
            self.session.get(ref_url, headers=self.headers, timeout=timeout).raise_for_status()
        self._throttle()
        # Data GET — session already carries all accumulated cookies
        resp = self.session.get(
            api_url, params=params, headers=self.headers, timeout=api_timeout,
        )
        resp.raise_for_status()
        return resp

    def _get_json(self, ref_url: str, api_url: str, timeout: int = 10) -> dict | None:
        """
        Warm-up cookies from *ref_url*, then fetch *api_url* as JSON.

        Parameters
        ----------
        ref_url : str
            Page used to obtain session cookies (warm-up GET).
        api_url : str
            Actual JSON API endpoint to query.
        timeout : int, optional
            Per-request timeout in seconds. Default is ``10``.

        Returns
        -------
        dict or None
            Parsed JSON payload, or ``None`` on persistent failure after
            2 retries.
        """
        try:
            return self._retry(
                lambda: self._warm_and_fetch(ref_url, api_url, timeout=timeout).json()
            )
        except Exception as exc:
            self._log_error("_get_json", exc)
            return None

    def _get_archive(self, url: str) -> bytes | None:
        """
        Direct GET for a static archive file — no session cookies needed.

        Parameters
        ----------
        url : str
            Fully-qualified URL of the file to download.

        Returns
        -------
        bytes or None
            Raw response bytes, or ``None`` after 2 failed attempts.
        """
        def _call() -> bytes:
            self.rotate_user_agent()
            self._throttle()
            resp = self.session.get(url, headers=self.headers, timeout=10)
            resp.raise_for_status()
            return resp.content

        try:
            return self._retry(_call)
        except Exception as exc:
            self._log_error("_get_archive", exc)
            return None

    def _get_csv_archive(self, url: str) -> pd.DataFrame | None:
        """
        Fetch a static CSV archive and parse it into a ``DataFrame``.

        Parameters
        ----------
        url : str
            Fully-qualified URL of the CSV file.

        Returns
        -------
        pd.DataFrame or None
            Parsed data, or ``None`` on download/parse failure.
        """
        raw = self._get_archive(url)
        if raw is None:  # Bug 2 fix: b"" (empty file) is valid; only None signals error
            return None
        try:
            return pd.read_csv(BytesIO(raw))
        except Exception as exc:
            self._log_error("_get_csv_archive", exc)
            return None

    def _get_csv_session(
        self,
        ref_url: str,
        api_url: str,
        retries: int = 3,
    ) -> pd.DataFrame | None:
        """
        Fetch a session-authenticated CSV with BOM stripping and retry logic.

        Warm-up cookies are obtained from *ref_url*; the actual CSV is
        requested from *api_url*.  Automatically sorts by the ``Date`` column
        if present.

        Uses :meth:`_warm_and_fetch` for the HTTP call and :meth:`_retry`
        for exponential back-off — replacing the previous hand-rolled
        ``for attempt in range(retries)`` loop.

        Parameters
        ----------
        ref_url : str
            Page used to obtain session cookies.
        api_url : str
            CSV endpoint URL.
        retries : int, optional
            Maximum download attempts. Default is ``3``.

        Returns
        -------
        pd.DataFrame or None
            Sorted and cleaned data, or ``None`` on failure.
        """
        def _call() -> pd.DataFrame:
            resp = self._warm_and_fetch(ref_url, api_url, timeout=10, api_timeout=15)
            if "text/html" in resp.headers.get("Content-Type", ""):
                raise ValueError("Received HTML instead of CSV — session may have expired")
            df = _csv_from_bytes(resp.content)
            if df.empty or len(df.columns) < 2:
                raise ValueError("CSV response was empty or malformed")
            if "Date" in df.columns:
                df = _sort_dedup_dates(df, "Date", fmt="%d-%b-%Y", ascending=False)
            return _clean_str(df)

        try:
            return self._retry(_call, retries=retries - 1)
        except Exception as exc:
            self._log_error("_get_csv_session", exc)
            return None

    def _get_chunked(
        self,
        ref_url:  str,
        api_tpl:  str,
        from_date: str,
        to_date:   str,
        chunk:    int = 89,
        retries:  int = 3,
    ) -> list:
        """
        Fetch data in sliding 89-day windows and collect all records.

        *api_tpl* must be a format string accepting two positional
        ``str.format`` arguments: ``{0}`` (start) and ``{1}`` (end) both
        as ``DD-MM-YYYY`` strings.

        Uses :meth:`_warm_and_fetch` for the initial warm-up (which stores
        cookies in ``self.session`` automatically) and for mid-loop cookie
        refreshes on failed chunks.  The ``requests.Session`` jar is relied
        upon for all subsequent GETs — no manual ``cookies=`` kwarg needed.
        Respects the ``Retry-After`` header on HTTP 429 responses.

        Parameters
        ----------
        ref_url : str
            Page for cookie warm-up.
        api_tpl : str
            URL template with ``{}`` placeholders for start and end dates.
        from_date : str
            Overall range start in ``DD-MM-YYYY``.
        to_date : str
            Overall range end in ``DD-MM-YYYY``.
        chunk : int, optional
            Window size in days. Default is ``89``.
        retries : int, optional
            Attempts per chunk. Default is ``3``.

        Returns
        -------
        list
            Collected raw record dicts; empty list on session failure.
        """
        # ── Initial warm-up: establishes cookies in self.session ─────
        try:
            self._warm_and_fetch(ref_url, ref_url, timeout=10)
        except Exception as exc:
            self._log_error("_get_chunked.session", exc)
            return []

        start    = datetime.strptime(from_date, "%d-%m-%Y")
        end      = datetime.strptime(to_date,   "%d-%m-%Y")
        all_data = []

        while start <= end:
            chunk_end = min(start + timedelta(days=chunk), end)
            url       = api_tpl.format(
                start.strftime("%d-%m-%Y"),
                chunk_end.strftime("%d-%m-%Y"),
            )
            fetched = False

            for att in range(1, retries + 1):
                try:
                    self._throttle()
                    resp = self.session.get(
                        url, headers=self.headers, timeout=15 + att * 5,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        if "data" in data and isinstance(data["data"], list):
                            all_data.extend(data["data"])
                        fetched = True
                        break
                    elif resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", random.uniform(8, 12)))
                        time.sleep(retry_after)
                    else:
                        time.sleep(random.uniform(2, 4))
                except Exception as exc:
                    logger.debug("_get_chunked chunk attempt %d: %s", att, exc)
                    time.sleep(random.uniform(3, 6))

            if not fetched:
                # ── Cookie refresh ────────────────────────────────────
                try:
                    self._warm_and_fetch(ref_url, ref_url, timeout=10)
                except Exception as exc:
                    logger.debug("_get_chunked cookie refresh failed: %s", exc)
                    time.sleep(random.uniform(5, 10))

            time.sleep(random.uniform(1.5, 3.5))
            start = chunk_end + timedelta(days=1)

        return all_data

    def _live_ref_fetch(
        self,
        ref_url:     str,
        api_url:     str,
        timeout:     int = 10,
        api_timeout: int | None = None,
    ) -> requests.Response | None:
        """
        Warm-up cookies from *ref_url* then fetch *api_url* via the session.

        Returns the ``Response`` on success, or ``None`` on any exception.
        Used by live-data methods that need a simple one-shot warm-up but
        do not require the full retry logic of :meth:`_get_json`.

        Delegates the actual HTTP work to :meth:`_warm_and_fetch`.

        Parameters
        ----------
        ref_url : str
            Page used to obtain session cookies.
        api_url : str
            Actual data endpoint to query.
        timeout : int, optional
            Timeout for the warm-up GET. Default is ``10``.
        api_timeout : int or None, optional
            Timeout for the data GET. Defaults to *timeout* when ``None``.
        """
        try:
            return self._warm_and_fetch(ref_url, api_url, timeout=timeout, api_timeout=api_timeout)
        except Exception as exc:
            self._log_error("_live_ref_fetch", exc)
            return None

    def _chart_fetch(
        self,
        api_url: str,
        home:    str = "https://www.nseindia.com/option-chain",
    ) -> dict | None:
        """
        Warm-up cookies from *home*, then fetch the chart *api_url* as JSON.

        Retries up to 2 times via :meth:`_retry`.  Delegates HTTP work to
        :meth:`_warm_and_fetch`, removing the previously duplicated
        warm-up + session GET block.
        """
        try:
            return self._retry(
                lambda: self._warm_and_fetch(home, api_url, timeout=10).json()
            )
        except Exception as exc:
            self._log_error("_chart_fetch", exc)
            return None


# ══════════════════════════════════════════════════════════════════════════════
# III. Internal Data Shaping & Parsing
# ══════════════════════════════════════════════════════════════════════════════

    # ── File Format Parsers ─────────────────────────────────────────────────

    def _zip_csv(self, content: bytes) -> pd.DataFrame:
        """
        Extract the first ``.csv`` file from a ZIP archive and return it as a
        ``DataFrame``.  Returns an empty ``DataFrame`` if no CSV is found.
        """
        with zipfile.ZipFile(BytesIO(content), "r") as zf:
            for name in zf.namelist():
                if name.endswith(".csv"):
                    return pd.read_csv(zf.open(name))
        return pd.DataFrame()

    def _zip_rows(self, url: str, prefix: str) -> list | None:
        """
        Download a ZIP from *url* and extract raw CSV rows from the file
        whose name starts with *prefix*.

        Returns a list of string-lists (``csv.reader`` output) or ``None``.
        """
        raw = self._get_archive(url)
        if not raw:
            return None
        try:
            with zipfile.ZipFile(BytesIO(raw), "r") as zf:
                for name in zf.namelist():
                    if name.lower().startswith(prefix) and name.endswith(".csv"):
                        text = zf.open(name).read().decode("utf-8", "ignore")
                        return list(csv.reader(text.splitlines()))
        except Exception as exc:
            self._log_error("_zip_rows", exc)
            return None

    def _read_excel(self, content: bytes) -> pd.DataFrame | None:
        """
        Auto-detect ``xlrd`` vs ``openpyxl`` from the file magic bytes and
        parse the Excel workbook into a ``DataFrame`` with all-string dtypes.

        Returns ``None`` when the magic bytes are unrecognised.
        """
        buf = BytesIO(content)
        sig = buf.read(8)
        buf.seek(0)
        if sig.startswith(b"\xD0\xCF\x11\xE0"):
            engine = "xlrd"
        elif sig.startswith(b"\x50\x4B\x03\x04"):
            engine = "openpyxl"
        else:
            return None
        return pd.read_excel(buf, engine=engine, dtype=str)

    # ── Domain-Agnostic Shared Logic ────────────────────────────────────────

    def _json_data_df(self, ref_url: str, api_url: str) -> pd.DataFrame | None:
        """
        Fetch *api_url* as JSON, assert a top-level ``"data"`` key, and
        return the contents as a ``DataFrame``.

        Returns ``None`` when the request fails or ``"data"`` is absent.
        Centralises the ``if not data or "data" not in data`` guard that
        appeared verbatim in 20+ public methods.
        """
        data = self._get_json(ref_url, api_url)
        if not data or "data" not in data:
            return None
        return pd.DataFrame(data["data"])

    def _cm_live_simple(
        self,
        ref_url: str,
        api_url: str,
        cols:    list | None = None,
    ) -> pd.DataFrame | None:
        """
        Generic single-call helper for live CM endpoints that return a
        top-level ``{"data": [...]}`` JSON payload.

        *cols* optionally selects a subset of columns from the result.
        """
        df = self._json_data_df(ref_url, api_url)
        if df is None:
            return None
        if cols:
            df = _keep_cols(df, cols)
        return df if not df.empty else None

    def _cm_live_52wk(self, ref_url: str, api_url: str) -> pd.DataFrame | None:
        """
        Shared fetch for 52-week high / low live data.

        Returns a ``DataFrame`` with the six standard price columns or
        ``None`` on failure.
        """
        df = self._json_data_df(ref_url, api_url)
        if df is None:
            return None
        if df.empty:
            return None
        return df[["symbol", "series", "ltp", "pChange", "new52WHL", "prev52WHL", "prevHLDate"]]

    def _hist_index_df(
        self,
        ref_url:  str,
        api_tpl:  str,
        from_date: str,
        to_date:   str,
        col_map:  dict,
    ) -> pd.DataFrame:
        """
        Shared helper for index / VIX historical data.

        Fetches records via the chunked API, renames columns per *col_map*,
        sorts by ``Date``, and removes duplicates.
        """
        records = self._get_chunked(ref_url, api_tpl, from_date, to_date)
        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df = _keep_cols(df, col_map).rename(columns=col_map)

        if "Date" in df.columns:
            df = _sort_dedup_dates(df, "Date", fmt="%d-%b-%Y", ascending=True)

        return df.reset_index(drop=True)

    def _snapshot_contracts(self, index_param: str) -> pd.DataFrame | None:
        """
        Shared helper for most-active-contracts snapshots (OI / volume /
        options).  Hits the NSE ``snapshot-derivatives-equity`` endpoint
        with *index_param* as the query value.
        """
        resp = self._live_ref_fetch(
            "https://www.nseindia.com/market-data/most-active-contracts",
            f"https://www.nseindia.com/api/snapshot-derivatives-equity?index={index_param}",
        )
        try:
            df = pd.DataFrame(resp.json()["volume"]["data"])
            return df if not df.empty else None
        except Exception as exc:
            self._log_error("_snapshot_contracts", exc)
            return None

    def _hist_deal_csv(
        self,
        option_type: str,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        period:    str | None = None,
        symbol:    str | None = None,
    ) -> pd.DataFrame | None:
        """
        Shared CSV fetch for bulk deals, block deals, and short-selling.

        *option_type* must be one of ``"bulk_deals"``, ``"block_deals"``,
        or ``"short_selling"``.
        """
        from_date, to_date, period, symbol = _unpack_args(
            args, from_date, to_date, period, symbol, short=True
        )

        # Bug 4 fix: _resolve_dates already falls back to today, so the two
        # redundant "or today" lines were dead code and have been removed.
        from_date, to_date = _resolve_dates(from_date, to_date, period)

        ref_url = "https://www.nseindia.com/report-detail/display-bulk-and-block-deals"
        base    = "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals"
        api_url = (
            f"{base}?optionType={option_type}&symbol={symbol}"
            f"&from={from_date}&to={to_date}&csv=true"
            if symbol
            else f"{base}?optionType={option_type}&from={from_date}&to={to_date}&csv=true"
        )
        return self._get_csv_session(ref_url, api_url)

    def _corp_filing(
        self,
        ref_url:   str,
        base_api:  str,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        symbol:    str | None = None,
        period:    str | None = None,
        extra:     str        = "",
        keep_cols: list | None = None,
    ) -> pd.DataFrame | None:
        """
        Shared helper for all corporate-filing endpoints (announcements,
        board meetings, insider trading, BR/SR).

        Builds the correct query string from optional date/symbol parameters,
        fetches JSON via :meth:`_get_json`, and returns a filtered
        ``DataFrame``.
        """
        from_date, to_date, period, symbol = _unpack_args(
            args, from_date, to_date, period, symbol, short=True
        )

        if period:
            from_date, to_date = _resolve_dates(period=period)

        today = datetime.now().strftime("%d-%m-%Y")
        if (from_date or to_date or period) and not symbol:
            from_date = from_date or today
            to_date   = to_date   or today

        if symbol and from_date and to_date:
            api = f"{base_api}?index=equities&from_date={from_date}&to_date={to_date}&symbol={symbol}{extra}"
        elif symbol:
            api = f"{base_api}?index=equities&symbol={symbol}{extra}"
        elif from_date and to_date:
            api = f"{base_api}?index=equities&from_date={from_date}&to_date={to_date}{extra}"
        else:
            # Base URL selection: BR/SR endpoint (sustainabilitiy) prefers no 'index=equities'
            # on the base "recent" call, whereas other filings (announcements, etc.)
            # are fine with it or expect it for the standard 'NSE Equities' scope.
            if "sustainabilitiy" in base_api:
                api = f"{base_api}{extra}"
            else:
                api = f"{base_api}?index=equities{extra}"

        data = self._get_json(ref_url, api)
        if not data:
            return None
        records = data.get("data") if isinstance(data, dict) else data
        if not records or not isinstance(records, list):
            return None

        df = pd.DataFrame(records)
        if keep_cols:
            df = _keep_cols(df, keep_cols)
        return _clean_str(df)

    def _further_issue(
        self,
        base_url:     str,
        ref_page:     str,
        index_map:    dict,
        rename_maps:  dict,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        period:    str | None = None,
        symbol:    str | None = None,
        stage:     str | None = None,
    ) -> pd.DataFrame | None:
        """
        Shared helper for further-issue endpoints: QIP, Preferential Issue,
        and Rights Issue.

        Accepts stage strings ``"In-Principle"`` or ``"Listing Stage"`` via
        positional *args* or the *stage* keyword, plus the usual date/symbol
        parameters.

        Uses :meth:`_warm_and_fetch` (via :meth:`_retry`) instead of the
        previous inline warm-up GET block.
        """
        period_re = re.compile(r"^(1D|1W|1M|3M|6M|1Y)$", re.IGNORECASE)
        for arg in args:
            if not isinstance(arg, str):
                continue
            arg_t = arg.title()
            if arg_t in ("In-Principle", "Listing Stage"):
                stage = arg_t
            elif _DATE_PATTERN.match(arg):
                if not from_date:
                    from_date = arg
                elif not to_date:
                    to_date = arg
            elif period_re.match(arg.upper()):
                period = arg.upper()
            else:
                symbol = arg.upper()

        if period and not (from_date and to_date):
            from_date, to_date = _resolve_dates(period=period)

        stage  = (stage or "In-Principle").title()
        params = {"index": index_map[stage]}
        if symbol:
            params["symbol"] = symbol
        elif from_date and to_date:
            params["from_date"] = from_date
            params["to_date"]   = to_date

        api = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())

        try:
            data = self._retry(
                lambda: self._warm_and_fetch(ref_page, api, timeout=10).json().get("data", [])
            )
            if not data:
                return None
            rmap = rename_maps[stage]
            df   = pd.DataFrame(data)
            df   = _keep_cols(df, rmap).rename(columns=rmap)
            return _clean_str(df)
        except Exception as exc:
            self._log_error("_further_issue", exc)
            return None

    def _name_change_csv(self, url: str, has_header: bool = True) -> pd.DataFrame | None:
        """
        Fetch a name/symbol-change CSV and sort by the fourth column (date).

        Used by :meth:`cm_eod_eq_name_change` and
        :meth:`cm_eod_eq_symbol_change`.
        """
        raw = self._get_archive(url)
        if not raw:
            return None
        df = (
            pd.read_csv(BytesIO(raw))
            if has_header
            else pd.read_csv(BytesIO(raw), header=None)
        )
        if df.shape[1] >= 4:
            df.iloc[:, 3] = (
                pd.to_datetime(df.iloc[:, 3], format="%d-%b-%Y", errors="coerce")
                .dt.strftime("%Y-%m-%d")
            )
            df = df.sort_values(by=df.columns[3], ascending=False).reset_index(drop=True)
        return df

    def _biz_growth_fetch(self, market_type: str, mode: str, month: int | str, year: int):
        """
        Generic fetch, clean, convert, and rename for CM or F&O business-growth data.

        Parameters
        ----------
        market_type : str
            ``"cm"`` or ``"fo"``.
        mode : str
            ``"yearly"``, ``"monthly"``, or ``"daily"``.
        month : int or str
            Month as an integer (1–12) or a 3-letter abbreviation (e.g. ``"OCT"``).
        year : int
            4-digit calendar year.

        Returns
        -------
        list[dict] or None
            Records as a list of dicts, or ``None`` on failure.
        """
        if mode not in ("yearly", "monthly", "daily"):
            raise ValueError("Invalid mode. Use 'yearly', 'monthly', or 'daily'")

        # Normalise month to int then to NSE title-case string (e.g. "Oct")
        if isinstance(month, str):
            month = _MONTH_NUM[month[:3].upper()]
        month_str = _NUM_MONTH[month].title()

        # Financial-year window required only for monthly mode
        # Bug 1 fix: always define from_year / to_year before the dict
        # literal so the f-string ternary does not raise NameError when
        # mode is 'yearly' or 'daily' (variables would be unbound).
        from_year = to_year = None
        if mode == "monthly":
            from_year, to_year = (year - 1, year) if month in (1, 2, 3) else (year, year + 1)

        base_url = "https://www.nseindia.com/api/historicalOR"
        api_path_map = {
            "cm": {
                "yearly":  "cm/tbg/yearly",
                "monthly": f"cm/tbg/monthly?from={from_year}&to={to_year}" if mode == "monthly" else None,
                "daily":   f"cm/tbg/daily?month={month_str}&year={year % 100:02d}",
            },
            "fo": {
                "yearly":  "fo/tbg/yearly",
                "monthly": f"fo/tbg/monthly?from={from_year}&to={to_year}" if mode == "monthly" else None,
                "daily":   f"fo/tbg/daily?month={month_str}&year={year}",
            },
        }
        url = f"{base_url}/{api_path_map[market_type][mode]}"

        try:
            raw_data = self._retry(
                lambda: self._warm_and_fetch(
                    "https://www.nseindia.com/option-chain", url, timeout=10
                ).json().get("data", [])
            )
            data_list = [item.get("data", item) for item in raw_data if item]
            if not data_list:
                return None

            df = pd.DataFrame(data_list)

            # Normalise columns:
            # • object cols  — strip commas, try numeric coercion, keep strings on failure
            # • numeric cols — no astype(str) needed; just apply the "-" sentinel for NaN
            for col in df.select_dtypes(include="object").columns:
                cleaned = df[col].str.replace(",", "", regex=False).str.strip()
                cleaned = cleaned.replace({"None": "-", "nan": "-", "NaN": "-", "": "-"})
                numeric = pd.to_numeric(cleaned, errors="coerce")
                df[col] = numeric.where(numeric.notna(), other=cleaned)

            df = df.rename(columns=_BIZ_GROWTH_RENAME_MAPS[market_type][mode])
            # to_dict handles mixed int/float/str natively — no astype(object) copy needed
            return df.to_dict(orient="records")

        except (requests.RequestException, ValueError, KeyError) as exc:
            self._log_error("_biz_growth_fetch", exc)
            return None

    def _monthly_settlement(
        self,
        from_year:  int | None,
        to_year:    int | None,
        period:     str | None,
        api_tpl:    str,
        rename_map: dict,
    ) -> pd.DataFrame | None:
        """
        Shared helper for monthly settlement reports (CM and F&O).

        Loops over each financial year in the requested range, calls the
        NSE API for each, and returns a consolidated ``DataFrame`` sorted
        by month.
        """
        today = datetime.now()
        cfy   = today.year if today.month >= 4 else today.year - 1

        if period and not from_year:
            from_year = cfy - int(period[:-1])
            to_year   = cfy + 1
        elif not from_year:
            from_year = cfy
            to_year   = cfy + 1
        elif not to_year:
            to_year = from_year + 1

        self.rotate_user_agent()
        ref_url = "https://www.nseindia.com/report-detail/monthly-settlement-statistics"

        try:
            all_data = []

            for fy in range(from_year, to_year):
                api = api_tpl.format(fy, fy + 1)

                def _call(api=api):
                    return self._warm_and_fetch(ref_url, api, timeout=15).json()

                try:
                    js   = self._retry(_call)
                    recs = js if isinstance(js, list) else js.get("data", [])
                    for rec in recs:
                        if isinstance(rec, dict):
                            rec = dict(rec)   # shallow copy — don't mutate the original API dict
                            rec["FinancialYear"] = f"{fy}-{fy + 1}"
                            all_data.append(rec)
                        else:
                            all_data.append(rec)
                except Exception:
                    pass

            if not all_data:
                return None

            df = pd.DataFrame(all_data).rename(columns=rename_map)
            if "Month" in df.columns:
                df["_sort"] = pd.to_datetime(df["Month"], format="%b-%Y", errors="coerce")
                df.sort_values(["FinancialYear", "_sort"], inplace=True)
                df.reset_index(drop=True, inplace=True)
                df["Month"] = df["_sort"].dt.strftime("%b-%Y")
                df.drop(columns=["_sort"], inplace=True)
            return df

        except Exception as exc:
            self._log_error("_monthly_settlement", exc)
            return None

    def _pre_open(self, category: str = "All") -> dict | None:
        """
        Shared data fetch for all pre-open market methods.

        *category* accepts ``"NIFTY 50"``, ``"Nifty Bank"``, ``"Emerge"``,
        ``"Securities in F&O"``, ``"Others"``, or ``"All"``.
        """
        xref = {
            "NIFTY 50": "NIFTY", "Nifty Bank": "BANKNIFTY",
            "Emerge": "SME", "Securities in F&O": "FO",
            "Others": "OTHERS", "All": "ALL",
        }
        self.rotate_user_agent()
        ref_url = "https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market"

        def _call():
            api = (
                f"https://www.nseindia.com/api/market-data-pre-open"
                f"?key={xref.get(category, 'ALL')}"
            )
            return self._warm_and_fetch(ref_url, api, timeout=10).json()

        try:
            return self._retry(_call)
        except Exception as exc:
            self._log_error("_pre_open", exc)
            return None

    def _fao_participant_csv(self, trade_date: str, suffix: str) -> pd.DataFrame | None:
        """
        Shared fetch for participant-wise OI and volume CSVs.

        *suffix* is either ``"oi"`` or ``"vol"``.
        """
        ds = _fmt_trade_date(trade_date, "%d%m%Y").upper()
        return self._get_csv_archive(
            f"https://nsearchives.nseindia.com/content/nsccl/"
            f"fao_participant_{suffix}_{ds}.csv"
        )

    def _holidays(
        self,
        htype:     str,
        seg:       str,
        list_only: bool,
    ) -> pd.DataFrame | list | None:
        """
        Shared helper for trading and clearing holiday calendars.

        *htype* is ``"trading"`` or ``"clearing"``; *seg* is ``"CM"`` or
        ``"CD"``.  When *list_only* is ``True``, returns a plain list of date
        strings instead of a ``DataFrame``.
        """
        try:
            raw = self._get_json(
                "https://www.nseindia.com/market-data/live-equity-market",
                f"https://www.nseindia.com/api/holiday-master?type={htype}"
            )
        except Exception as exc:
            self._log_error("_holidays", exc)
            return None

        if raw is None:
            return None
        if seg not in raw:
            return None
        df = pd.DataFrame(
            raw[seg],
            columns=["Sr_no", "tradingDate", "weekDay", "description",
                     "morning_session", "evening_session"],
        )
        return df["tradingDate"].tolist() if list_only else df

    def _is_holiday(self, fn, date_str: str | None = None) -> bool | None:
        """
        Check whether *date_str* (``%d-%b-%Y``) or today is a market holiday.

        *fn* must be either :meth:`nse_trading_holidays` or
        :meth:`nse_clearing_holidays`.
        """
        holidays = fn(list_only=True)
        if holidays is None:
            return None
        try:
            dt  = datetime.strptime(date_str, "%d-%b-%Y") if date_str else datetime.today()
            return dt.strftime("%d-%b-%Y") in holidays
        except Exception as exc:
            self._log_error("_is_holiday", exc)
            return None

    def _nse_csv_list(
        self,
        url:      str,
        cols:     list,
        sym_col:  str,
        list_only: bool,
    ) -> pd.DataFrame | list | None:
        """
        Download a CSV archive, keep only *cols*, and optionally return just
        the *sym_col* values as a plain list.
        """
        raw = self._get_archive(url)
        if not raw:
            return None
        df = pd.read_csv(BytesIO(raw))
        df.columns = df.columns.str.strip()
        df = _keep_cols(df, cols)
        return df[sym_col].tolist() if list_only else df

    # ── SEBI Helpers ────────────────────────────────────────────────────────

    def _sebi_post(self, payload: dict) -> list[dict]:
        """
        POST to the SEBI circular listing endpoint, parse the HTML table,
        and return a list of row dicts.

        Parameters
        ----------
        payload : dict
            Form data to send.  Callers must supply the date/page fields.

        Returns
        -------
        list[dict]
            Parsed rows; empty list on HTTP error or missing table.
        """
        try:
            self._throttle()
            resp  = self.session.post(
                self._SEBI_URL,
                headers=self._SEBI_HEADERS,
                data=payload,
                timeout=15,
            )
            resp.raise_for_status()
            soup  = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table", {"id": "sample_1"})
            return self._parse_sebi_table(table)
        except Exception as exc:
            self._log_error("_sebi_post", exc)
            return []

    @staticmethod
    def _parse_sebi_table(table) -> list[dict]:
        """
        Extract rows from a SEBI HTML ``<table id="sample_1">`` element.

        Returns a list of ``{"Date": str, "Title": str, "Link": str|None}``
        dicts.  The Date value is the raw text from the first cell; callers
        are responsible for converting it to a datetime type.
        """
        rows = []
        if not table:
            return rows
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            a_tag = tds[1].find("a")
            title = a_tag.get("title") or a_tag.get_text(strip=True) if a_tag else tds[1].get_text(strip=True)
            href  = a_tag.get("href") if a_tag else None
            if href and not href.startswith("http"):
                href = "https://www.sebi.gov.in" + href
            rows.append({
                "Date":  tds[0].get_text(strip=True),
                "Title": title,
                "Link":  href,
            })
        return rows

    @staticmethod
    def _finalise_sebi_df(df: pd.DataFrame) -> pd.DataFrame:
        """Sort, dedup, and format the Date column of a SEBI DataFrame."""
        if df.empty:
            return df
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df.sort_values("Date", ascending=False, inplace=True)
        df.drop_duplicates(subset=["Date", "Title"], keep="first", inplace=True)
        df.reset_index(drop=True, inplace=True)
        df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")
        return df


    # ════════════════════════════════════════════════════════════════════════
    # ── NSE Market ──────────────────────────────────────────────────────────

    def nse_market_status(self, mode: str = "Market Status") -> pd.DataFrame | dict | None:
        """
        Fetch overall NSE market status, market cap, Nifty 50 info, and
        Gift Nifty snapshot.

        Combines four sub-payloads from the ``/api/marketStatus`` endpoint
        into individual ``DataFrame`` slices.

        Parameters
        ----------
        mode : str, optional
            One of ``"Market Status"``, ``"Mcap"``, ``"Nifty50"``,
            ``"Gift Nifty"`` (case-insensitive), or ``"all"`` to return
            everything packed in a dict. Default is ``"Market Status"``.

        Returns
        -------
        pd.DataFrame or dict or None
            Selected slice of the market-status payload, or ``None`` on
            failure.

        Examples
        --------
        >>> nse.nse_market_status("Nifty50")
        >>> nse.nse_market_status("Mcap")
        >>> nse.nse_market_status("Gift Nifty")
        """
        data = self._get_json(
            "https://www.nseindia.com/market-data/live-equity-market",
            "https://www.nseindia.com/api/marketStatus",
        )
        if not data:
            return None

        ms = mcap = n50 = gift = None

        if isinstance(data.get("marketState"), list):
            ms = pd.DataFrame(data["marketState"])
            ms = ms[[c for c in [
                "market", "marketStatus", "tradeDate", "index",
                "last", "variation", "percentChange", "marketStatusMessage",
            ] if c in ms.columns]]

        if isinstance(data.get("marketcap"), dict):
            mcap = pd.DataFrame([data["marketcap"]]).rename(columns={
                "timeStamp":              "Date",
                "marketCapinTRDollars":   "MarketCap_USD_Trillion",
                "marketCapinLACCRRupees": "MarketCap_INR_LakhCr",
                "marketCapinCRRupees":    "MarketCap_INR_Cr",
            })

        if isinstance(data.get("indicativenifty50"), dict):
            n50 = pd.DataFrame([data["indicativenifty50"]]).rename(columns={
                "dateTime":         "DateTime",
                "indexName":        "Index",
                "closingValue":     "ClosingValue",
                "finalClosingValue":"FinalClose",
                "change":           "Change",
                "perChange":        "PercentChange",
            })

        if isinstance(data.get("giftnifty"), dict):
            gift = pd.DataFrame([data["giftnifty"]]).rename(columns={
                "SYMBOL":           "Symbol",
                "EXPIRYDATE":       "ExpiryDate",
                "LASTPRICE":        "LastPrice",
                "DAYCHANGE":        "DayChange",
                "PERCHANGE":        "PercentChange",
                "CONTRACTSTRADED":  "ContractsTraded",
                "TIMESTMP":         "Timestamp",
            })

        lookup = {
            "market status": ms, "mcap": mcap,
            "nifty50": n50, "gift nifty": gift,
        }
        m = mode.strip().lower()
        if m in lookup:
            return lookup[m]
        if m == "all":
            return {"Market Status": ms, "Mcap": mcap, "Nifty50": n50, "Gift Nifty": gift}

        print(f"Invalid mode '{mode}'")
        return None

    def nse_is_market_open(self, market: str = "Capital Market"):
        """
        Return a ``rich.text.Text`` coloured status for *market*.

        Parameters
        ----------
        market : str, optional
            Segment name as it appears in the NSE API, e.g.
            ``"Capital Market"``, ``"Currency"``, ``"Commodity"``,
            ``"Debt"``, or ``"currencyfuture"``.
            Default is ``"Capital Market"``.

        Returns
        -------
        rich.text.Text
            Green text when the market is open, red when closed/halted.

        Examples
        --------
        >>> from rich.console import Console
        >>> Console().print(nse.nse_is_market_open("Capital Market"))
        """
        from rich.text import Text

        data = self._get_json(
            "https://www.nseindia.com/market-data/live-equity-market",
            "https://www.nseindia.com/api/marketStatus",
        )
        if not data:
            return Text("Error fetching market status", style="bold red")

        sel = next(
            (m for m in data.get("marketState", []) if m.get("market") == market),
            None,
        )
        if not sel:
            return Text(f"[{market}] Not found", style="bold yellow")

        msg  = sel.get("marketStatusMessage", "").strip()
        text = Text(f"[{market}] → ", style="bold white")
        is_closed = any(w in msg.lower() for w in ("closed", "halted", "suspended"))
        text.append(msg, style="bold red" if is_closed else "bold green")
        return text

    # ══════════════════════════════════════════════════════════════════════════════
    # IV. Global & Miscellaneous
    # ══════════════════════════════════════════════════════════════════════════════

    def nse_trading_holidays(self, list_only: bool = False):
        """Return the NSE trading-holiday calendar for the current year.

        Parameters
        ----------
        list_only : bool, optional
            If ``True``, return a plain ``list`` of date strings
            (``DD-MMM-YYYY``). Default is ``False``.

        Returns
        -------
        pd.DataFrame or list or None

        Examples
        --------
        >>> nse.nse_trading_holidays()
        >>> nse.nse_trading_holidays(list_only=True)
        """
        return self._holidays("trading", "CM", list_only)

    def nse_clearing_holidays(self, list_only: bool = False):
        """Return the NSE clearing-holiday calendar for the current year.

        Parameters
        ----------
        list_only : bool, optional
            If ``True``, return a plain ``list`` of date strings.
            Default is ``False``.

        Returns
        -------
        pd.DataFrame or list or None

        Examples
        --------
        >>> nse.nse_clearing_holidays()
        >>> nse.nse_clearing_holidays(list_only=True)
        """
        return self._holidays("clearing", "CD", list_only)

    def is_nse_trading_holiday(self, date_str: str | None = None) -> bool | None:
        """Return ``True`` if *date_str* is a trading holiday.

        Parameters
        ----------
        date_str : str, optional
            Date in ``DD-MMM-YYYY`` format, e.g. ``"25-Dec-2026"``.
            Defaults to today.

        Returns
        -------
        bool or None

        Examples
        --------
        >>> nse.is_nse_trading_holiday()
        >>> nse.is_nse_trading_holiday("25-Dec-2026")
        """
        return self._is_holiday(self.nse_trading_holidays, date_str)

    def is_nse_clearing_holiday(self, date_str: str | None = None) -> bool | None:
        """Return ``True`` if *date_str* is a clearing holiday.

        Parameters
        ----------
        date_str : str, optional
            Date in ``DD-MMM-YYYY`` format. Defaults to today.

        Returns
        -------
        bool or None

        Examples
        --------
        >>> nse.is_nse_clearing_holiday()
        >>> nse.is_nse_clearing_holiday("22-Oct-2025")
        """
        return self._is_holiday(self.nse_clearing_holidays, date_str)

    def nse_live_market_turnover(self) -> pd.DataFrame:
        """
        Fetch live market turnover across all NSE segments.

        Returns
        -------
        pd.DataFrame
            Columns: Segment, Product, Vol, Value (₹ Cr), OI, No. of Trades,
            Updated At, and previous-day equivalents.

        Examples
        --------
        >>> nse.nse_live_market_turnover()
        """
        data = self._get_json(
            "https://www.nseindia.com/option-chain",
            "https://www.nseindia.com/api/NextApi/apiClient"
            "?functionName=getMarketTurnoverSummary",
        )
        if not data:
            return pd.DataFrame()

        rows = []
        for seg, records in data.get("data", {}).items():
            if not isinstance(records, list):
                continue
            for rec in records:
                rows.append({
                    "Segment":                seg.upper(),
                    "Product":                rec.get("instrument", ""),
                    "Vol (Shares/Contracts)": rec.get("volume", 0),
                    "Value (₹ Cr)":           round(rec.get("value", 0) / 1e7, 2),
                    "OI (Contracts)":         rec.get("oivalue", 0),
                    "No. of Orders#":         rec.get("noOfOrders", 0),
                    "No. of Trades":          rec.get("noOfTrades", 0),
                    "Avg Trade Value (₹)":    rec.get("averageTrade", 0),
                    "Updated At":             rec.get("mktTimeStamp", ""),
                    "Prev Vol":               rec.get("prevVolume", 0),
                    "Prev Value (₹ Cr)":      round(rec.get("prevValue", 0) / 1e7, 2),
                    "prev OI (Contracts)":    rec.get("prevOivalue", 0),
                    "prev Orders#":           rec.get("prevNoOfOrders", 0),
                    "prev Trades":            rec.get("prevNoOfTrades", 0),
                    "prev Avg Trade Value (₹)": rec.get("prevAverageTrade", 0),
                })
        return _clean(pd.DataFrame(rows))

    def nse_live_hist_circulars(
        self,
        from_date_str: str | None = None,
        to_date_str:   str | None = None,
        filter:        str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch NSE exchange-communication circulars between two dates.

        Parameters
        ----------
        from_date_str : str, optional
            Start date in ``DD-MM-YYYY`` format. Defaults to yesterday.
        to_date_str : str, optional
            End date in ``DD-MM-YYYY`` format. Defaults to today.
        dept_filter : str, optional
            Substring to filter the ``Department`` column (case-insensitive).

        Returns
        -------
        pd.DataFrame
            Columns: Date, Circulars No, Category, Department, Subject,
            Attachment.

        Examples
        --------
        >>> nse.nse_live_hist_circulars()
        >>> nse.nse_live_hist_circulars("18-07-2025", "18-10-2025")
        >>> nse.nse_live_hist_circulars(filter="NSE Listing")
        """
        self.rotate_user_agent()
        fd    = from_date_str or (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")
        td    = to_date_str   or datetime.now().strftime("%d-%m-%Y")
        empty = pd.DataFrame(columns=["Date", "Circulars No", "Category", "Department",
                                       "Subject", "Attachment"])
        try:
            resp = self._live_ref_fetch(
                "https://www.nseindia.com/resources/exchange-communication-circulars",
                f"https://www.nseindia.com/api/circulars?&fromDate={fd}&toDate={td}",
            )
            if resp is None:
                return empty
            items = resp.json().get("data", [])
            if not isinstance(items, list) or not items:
                return empty

            df = pd.DataFrame([{
                "Date":         item.get("cirDisplayDate", ""),
                "Circulars No": item.get("circDisplayNo",  ""),
                "Category":     item.get("circCategory",   ""),
                "Department":   item.get("circDepartment", ""),
                "Subject":      item.get("sub",            ""),
                "Attachment":   item.get("circFilelink",   ""),
            } for item in items])

            if filter:
                df = df[df["Department"].str.contains(filter, case=False, na=False)]
            return df

        except Exception:
            return empty

    def nse_live_hist_press_releases(
        self,
        from_date_str: str | None = None,
        to_date_str:   str | None = None,
        filter:        str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch NSE press releases between two dates, stripping any HTML tags
        from the subject field.

        Parameters
        ----------
        from_date_str : str, optional
            Start date in ``DD-MM-YYYY`` format. Defaults to yesterday.
        to_date_str : str, optional
            End date in ``DD-MM-YYYY`` format. Defaults to today.
        dept_filter : str, optional
            Substring to filter the ``DEPARTMENT`` column (case-insensitive).
            Valid departments: Corporate Communications, Investor Services
            Cell, Member Compliance, NSE Clearing, NSE Indices, NSE Listing,
            Surveillance.

        Returns
        -------
        pd.DataFrame
            Columns: DATE, DEPARTMENT, SUBJECT, ATTACHMENT URL, LAST UPDATED.

        Examples
        --------
        >>> nse.nse_live_hist_press_releases()
        >>> nse.nse_live_hist_press_releases("18-07-2025", "18-10-2025")
        >>> nse.nse_live_hist_press_releases("01-10-2025", "04-10-2025", "NSE Listing")
        """
        self.rotate_user_agent()
        fd    = from_date_str or (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")
        td    = to_date_str   or datetime.now().strftime("%d-%m-%Y")
        cols  = ["DATE", "DEPARTMENT", "SUBJECT", "ATTACHMENT URL", "LAST UPDATED"]
        empty = pd.DataFrame(columns=cols)

        try:
            resp = self._live_ref_fetch(
                "https://www.nseindia.com/resources/exchange-communication-press-releases",
                f"https://www.nseindia.com/api/press-release-cms20?fromDate={fd}&toDate={td}",
            )
            if resp is None:
                return empty
            items = resp.json()
            if not isinstance(items, list):
                return empty

            rows = []
            for item in items:
                if not isinstance(item, dict) or "content" not in item:
                    continue
                c    = item["content"]
                subj = c.get("body", "")
                if "<" in subj and ">" in subj:
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
                        subj = BeautifulSoup(subj, "html.parser").get_text(separator=" ").strip()
                try:
                    chg = datetime.strptime(
                        item.get("changed", ""), "%a, %m/%d/%Y - %H:%M"
                    ).strftime("%a %d-%b-%Y %I:%M %p")
                except ValueError:
                    chg = item.get("changed", "")

                rows.append({
                    "DATE":           c.get("field_date", ""),
                    "SUBJECT":        subj,
                    "DEPARTMENT":     c.get("field_type", ""),
                    "ATTACHMENT URL": (c.get("field_file_attachement") or {}).get("url"),
                    "LAST UPDATED":   chg,
                })

            if not rows:
                return empty
            df = pd.DataFrame(rows)
            if filter:
                df = df[df["DEPARTMENT"].str.contains(filter, case=False, na=False)]
            return df[cols]

        except Exception as exc:
            self._log_error("nse_live_hist_press_releases", exc)
            return empty

    def nse_reference_rates(self) -> pd.DataFrame | None:
        """
        Fetch the latest NSE currency reference rates for major pairs.

        Returns
        -------
        pd.DataFrame or None
            Columns: currency, unit, value, prevDayValue.

        Examples
        --------
        >>> nse.nse_reference_rates()
        """
        data = self._get_json(
            "https://www.nseindia.com/option-chain",
            "https://www.nseindia.com/api/NextApi/apiClient"
            "?functionName=getReferenceRates&&type=null&&flag=CUR",
        )
        if not data:
            return None
        rates = data.get("data", {}).get("currencySpotRates", [])
        if not rates:
            return None
        return pd.DataFrame(rates)[["currency", "unit", "value", "prevDayValue"]].fillna(0)

    def nse_eod_top10_nifty50(self, trade_date: str) -> pd.DataFrame | None:
        """
        Fetch the top-10 Nifty 50 constituents by turnover for a date.

        Parameters
        ----------
        trade_date : str
            Trade date in ``DD-MM-YY`` (two-digit year) format,
            e.g. ``"17-10-25"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.nse_eod_top10_nifty50("17-10-25")
        """
        dt  = _fmt_trade_date(trade_date, "%d%m%y")
        raw = self._get_archive(
            f"https://nsearchives.nseindia.com/content/indices/"
            f"top10nifty50_{dt.upper()}.csv"
        )
        return pd.read_csv(BytesIO(raw)) if raw else None

    def nse_6m_nifty_50(self, list_only: bool = False):
        """Return the current Nifty 50 constituent list (updated 6-monthly).

        Parameters
        ----------
        list_only : bool, optional
            If ``True``, return a plain ``list`` of symbols.

        Returns
        -------
        pd.DataFrame or list or None

        Examples
        --------
        >>> nse.nse_6m_nifty_50()
        >>> nse.nse_6m_nifty_50(list_only=True)
        """
        return self._nse_csv_list(
            "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv",
            ["Company Name", "Industry", "Symbol", "Series", "ISIN Code"],
            "Symbol", list_only,
        )

    def nse_6m_nifty_500(self, list_only: bool = False):
        """Return the current Nifty 500 constituent list (updated 6-monthly).

        Parameters
        ----------
        list_only : bool, optional
            If ``True``, return a plain ``list`` of symbols.

        Returns
        -------
        pd.DataFrame or list or None

        Examples
        --------
        >>> nse.nse_6m_nifty_500()
        >>> nse.nse_6m_nifty_500(list_only=True)
        """
        return self._nse_csv_list(
            "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
            ["Company Name", "Industry", "Symbol", "Series", "ISIN Code"],
            "Symbol", list_only,
        )

    def nse_eod_equity_full_list(self, list_only: bool = False):
        """Return the full NSE equity listing with symbols, names, and dates.

        Parameters
        ----------
        list_only : bool, optional
            If ``True``, return only a ``list`` of symbol strings.

        Returns
        -------
        pd.DataFrame or list or None

        Examples
        --------
        >>> nse.nse_eod_equity_full_list()
        >>> nse.nse_eod_equity_full_list(list_only=True)
        """
        return self._nse_csv_list(
            "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
            ["SYMBOL", "NAME OF COMPANY", "SERIES", "DATE OF LISTING", "FACE VALUE"],
            "SYMBOL", list_only,
        )

    def nse_eom_fno_full_list(self, mode: str = "stocks", list_only: bool = False):
        """
        Return the full F&O underlying list (stocks or indices).

        Parameters
        ----------
        mode : str, optional
            ``"stocks"`` (default) for equity F&O underlyings, or
            ``"index"`` for index F&O underlyings.
        list_only : bool, optional
            If ``True``, return a plain ``list`` of symbol strings instead
            of the full ``DataFrame``.

        Returns
        -------
        pd.DataFrame or list or None

        Examples
        --------
        >>> nse.nse_eom_fno_full_list()
        >>> nse.nse_eom_fno_full_list(list_only=True)
        >>> nse.nse_eom_fno_full_list("index")
        >>> nse.nse_eom_fno_full_list("index", list_only=True)
        """
        ref_url = (
            "https://www.nseindia.com/products-services/"
            "equity-derivatives-list-underlyings-information"
        )
        data = self._get_json(ref_url, "https://www.nseindia.com/api/underlying-information")
        if not data:
            return None
        key = "IndexList" if mode.strip().lower() == "index" else "UnderlyingList"
        df  = pd.DataFrame(data["data"][key]).rename(columns={
            "serialNumber": "Serial Number",
            "symbol":       "Symbol",
            "underlying":   "Underlying",
        })
        return df["Symbol"].tolist() if list_only else df[["Serial Number", "Symbol", "Underlying"]]

    def state_wise_registered_investors(self):
        """Return state-wise count of registered NSE investors.

        Returns
        -------
        dict or None
            Raw JSON response from NSE.

        Examples
        --------
        >>> nse.state_wise_registered_investors()
        """
        return self._get_json(
            "https://www.nseindia.com/registered-investors/",
            "https://www.nseindia.com/api/registered-investors",
        )

    def list_of_indices(self):
        """Return the master list of all NSE index categories.

        Returns
        -------
        dict or None
            Raw JSON with index category metadata.

        Examples
        --------
        >>> nse.list_of_indices()
        """
        return self._get_json(
            "https://www.nseindia.com/option-chain",
            "https://www.nseindia.com/api/equity-master",
        )


    # ════════════════════════════════════════════════════════════════════════
    # ── IPO ─────────────────────────────────────────────────────────────────

    def ipo_current(self) -> pd.DataFrame | None:
        """Fetch all currently active / ongoing IPOs on the NSE.

        Returns
        -------
        pd.DataFrame or None
            Columns: symbol, companyName, series, issueStartDate, issueEndDate,
            status, issueSize, issuePrice, noOfSharesOffered,
            noOfsharesBid, noOfTime.

        Examples
        --------
        >>> nse.ipo_current()
        """
        data = self._get_json(
            "https://www.nseindia.com/market-data/all-upcoming-issues-ipo",
            "https://www.nseindia.com/api/ipo-current-issue",
        )
        if not isinstance(data, list):
            return None
        df   = pd.DataFrame(data)
        cols = [
            "symbol", "companyName", "series", "issueStartDate", "issueEndDate",
            "status", "issueSize", "issuePrice", "noOfSharesOffered",
            "noOfsharesBid", "noOfTime",
        ]
        return _clean(_keep_cols(df, cols).fillna(0)) if not df.empty else None

    def ipo_preopen(self) -> pd.DataFrame | None:
        """Fetch pre-open session data for IPOs listed today.

        Returns
        -------
        pd.DataFrame or None
            Includes IEP, order-book summary, ATO quantities, and
            cancellation counts.

        Examples
        --------
        >>> nse.ipo_preopen()
        """
        data = self._get_json(
            "https://www.nseindia.com/market-data/new-stock-exchange-listings-today",
            "https://www.nseindia.com/api/special-preopen-listing",
        )
        if not data or "data" not in data:
            return None

        rows = []
        for item in data["data"]:
            pb  = item.get("preopenBook", {})
            pre = (pb.get("preopen") or [{}])[0]
            ato = pb.get("ato", {})
            row = {k: item.get(k, "") for k in [
                "symbol", "series", "prevClose", "iep", "change", "perChange",
                "ieq", "ieVal", "buyOrderCancCnt", "buyOrderCancVol",
                "sellOrderCancCnt", "sellOrderCancVol", "isin", "status",
            ]}
            row.update({
                "preopen_buyQty":          pre.get("buyQty",  0),
                "preopen_sellQty":         pre.get("sellQty", 0),
                "ato_totalBuyQuantity":    ato.get("totalBuyQuantity",  0),
                "ato_totalSellQuantity":   ato.get("totalSellQuantity", 0),
                "totalBuyQuantity":        pb.get("totalBuyQuantity",  0),
                "totalSellQuantity":       pb.get("totalSellQuantity", 0),
                "totTradedQty":            pb.get("totTradedQty",       0),
                "lastUpdateTime":          pb.get("lastUpdateTime",     ""),
            })
            rows.append(row)

        df = pd.DataFrame(rows).fillna(0)
        return df if not df.empty else None

    def ipo_tracker_summary(self, category_filter: str | None = None) -> pd.DataFrame | None:
        """
        Return the IPO tracker summary table from NSE.

        Parameters
        ----------
        category_filter : str, optional
            Substring to filter the ``MARKETTYPE`` column, e.g. ``"SME"``
            or ``"Mainboard"`` (case-insensitive).

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.ipo_tracker_summary()
        >>> nse.ipo_tracker_summary("SME")
        >>> nse.ipo_tracker_summary("Mainboard")
        """
        data = self._get_json(
            "https://www.nseindia.com/ipo-tracker?type=ipo_year",
            "https://www.nseindia.com/api/NextApi/apiClient"
            "?functionName=getIPOTrackerSummary",
        )
        if not isinstance(data, dict) or "data" not in data:
            return None

        df = pd.DataFrame(data["data"])
        df["MARKETTYPE"] = df["MARKETTYPE"].str.upper().fillna("")
        if category_filter:
            df = df[df["MARKETTYPE"].str.contains(category_filter.upper(), case=False, na=False)]

        keep = [
            "SYMBOL", "COMPANYNAME", "LISTED_ON", "ISSUE_PRICE",
            "LISTED_DAY_CLOSE", "LISTED_DAY_GAIN", "LISTED_DAY_GAIN_PER",
            "LTP", "GAIN_LOSS", "GAIN_LOSS_PER", "MARKETTYPE",
        ]
        df = _keep_cols(df, keep)
        for col in ["ISSUE_PRICE", "LISTED_DAY_CLOSE", "LISTED_DAY_GAIN",
                    "LISTED_DAY_GAIN_PER", "LTP", "GAIN_LOSS", "GAIN_LOSS_PER"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "LISTED_ON" in df.columns:
            df["LISTED_ON"] = pd.to_datetime(df["LISTED_ON"], format="%d-%m-%Y", errors="coerce")
            df = df.sort_values("LISTED_ON", ascending=False)
            df["LISTED_ON"] = df["LISTED_ON"].dt.strftime("%Y-%m-%d")

        return df.reset_index(drop=True)


    # ════════════════════════════════════════════════════════════════════════
    # ── Pre-Open ────────────────────────────────────────────────────────────

    def pre_market_nifty_info(self, category: str = "All") -> pd.DataFrame | None:
        """Return the pre-open summary for the selected index category.

        Parameters
        ----------
        category : str, optional
            One of ``"NIFTY 50"``, ``"Nifty Bank"``, ``"Emerge"``,
            ``"Securities in F&O"``, ``"Others"``, or ``"All"``.
            Default is ``"All"``.

        Returns
        -------
        pd.DataFrame or None
            Single-row with lastPrice, change, pChange, advances,
            declines, unchanged, timestamp.

        Examples
        --------
        >>> nse.pre_market_nifty_info("NIFTY 50")
        """
        data = self._pre_open(category)
        if not data:
            return None
        ns = data.get("niftyPreopenStatus", {})
        return pd.DataFrame([{
            "lastPrice": ns.get("lastPrice"),
            "change":    ns.get("change"),
            "pChange":   ns.get("pChange"),
            "advances":  data.get("advances",  0),
            "declines":  data.get("declines",  0),
            "unchanged": data.get("unchanged", 0),
            "timestamp": data.get("timestamp", ""),
        }])

    def pre_market_all_nse_adv_dec_info(self, category: str = "All") -> pd.DataFrame | None:
        """Return aggregate advances/declines for the pre-open session.

        Parameters
        ----------
        category : str, optional
            Index/segment filter. Default is ``"All"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.pre_market_all_nse_adv_dec_info()
        """
        data = self._pre_open(category)
        if not data:
            return None
        return pd.DataFrame([{
            "advances":  data.get("advances",  0),
            "declines":  data.get("declines",  0),
            "unchanged": data.get("unchanged", 0),
            "timestamp": data.get("timestamp", ""),
        }])

    def pre_market_info(self, category: str = "All") -> pd.DataFrame | None:
        """
        Return detailed per-stock pre-open data for *category*.

        Parameters
        ----------
        category : str, optional
            One of ``"All"``, ``"NIFTY 50"``, ``"Nifty Bank"``,
            ``"Emerge"``, ``"Securities in F&O"``. Default ``"All"``.

        Returns
        -------
        pd.DataFrame or None
            Indexed by symbol; includes IEP, turnover, 52-week range,
            and order-book buy/sell quantities.

        Examples
        --------
        >>> nse.pre_market_info("All")
        >>> nse.pre_market_info("NIFTY 50")
        >>> nse.pre_market_info("Securities in F&O")
        """
        data = self._pre_open(category)
        if not data:
            return None

        rows = [{
            "symbol":           i["metadata"]["symbol"],
            "previousClose":    i["metadata"]["previousClose"],
            "iep":              i["metadata"]["iep"],
            "change":           i["metadata"]["change"],
            "pChange":          i["metadata"]["pChange"],
            "lastPrice":        i["metadata"]["lastPrice"],
            "finalQuantity":    i["metadata"]["finalQuantity"],
            "totalTurnover":    i["metadata"]["totalTurnover"],
            "marketCap":        i["metadata"]["marketCap"],
            "yearHigh":         i["metadata"]["yearHigh"],
            "yearLow":          i["metadata"]["yearLow"],
            "totalBuyQuantity": i["detail"]["preOpenMarket"]["totalBuyQuantity"],
            "totalSellQuantity":i["detail"]["preOpenMarket"]["totalSellQuantity"],
            "atoBuyQty":        i["detail"]["preOpenMarket"]["atoBuyQty"],
            "atoSellQty":       i["detail"]["preOpenMarket"]["atoSellQty"],
            "lastUpdateTime":   i["detail"]["preOpenMarket"]["lastUpdateTime"],
        } for i in data.get("data", [])]

        return pd.DataFrame(rows).set_index("symbol", drop=False)

    def pre_market_derivatives_info(self, category: str = "Index Futures") -> pd.DataFrame | None:
        """
        Return pre-open data for F&O contracts.

        Parameters
        ----------
        category : str, optional
            ``"Index Futures"`` (default) or ``"Stock Futures"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.pre_market_derivatives_info("Index Futures")
        >>> nse.pre_market_derivatives_info("Stock Futures")
        """
        xref = {"Index Futures": "FUTIDX", "Stock Futures": "FUTSTK"}
        self.rotate_user_agent()

        def _call():
            resp = self._warm_and_fetch(
                "https://www.nseindia.com/market-data/pre-open-market-fno",
                f"https://www.nseindia.com/api/market-data-pre-open-fno?key={xref[category]}",
                timeout=10
            )
            return resp.json().get("data", [])

        try:
            items = self._retry(_call)
        except Exception as exc:
            self._log_error("pre_market_derivatives_info", exc)
            return None

        rows = [{
            "symbol":              i["metadata"]["symbol"],
            "expiryDate":          i["metadata"]["expiryDate"],
            "previousClose":       i["metadata"]["previousClose"],
            "iep":                 i["metadata"]["iep"],
            "change":              i["metadata"]["change"],
            "pChange":             i["metadata"]["pChange"],
            "lastPrice":           i["metadata"]["lastPrice"],
            "finalQuantity":       i["metadata"]["finalQuantity"],
            "totalTurnover":       i["metadata"]["totalTurnover"],
            "totalBuyQuantity":    i["detail"]["preOpenMarket"]["totalBuyQuantity"],
            "totalSellQuantity":   i["detail"]["preOpenMarket"]["totalSellQuantity"],
            "atoBuyQty":           i["detail"]["preOpenMarket"]["atoBuyQty"],
            "atoSellQty":          i["detail"]["preOpenMarket"]["atoSellQty"],
            "lastUpdateTime":      i["detail"]["preOpenMarket"]["lastUpdateTime"],
        } for i in items]

        return pd.DataFrame(rows).set_index("symbol", drop=False)



# ══════════════════════════════════════════════════════════════════════════════
# IV. Indices & VIX
# ══════════════════════════════════════════════════════════════════════════════

    def index_live_all_indices_data(self) -> pd.DataFrame | None:
        """Return live snapshot for all NSE indices.

        Returns
        -------
        pd.DataFrame or None
            Columns include index, last, percentChange, pe, pb, dy,
            advances, declines, perChange30d, perChange365d.

        Examples
        --------
        >>> nse.index_live_all_indices_data()
        """
        df = self._json_data_df(
            "https://www.nseindia.com/market-data/index-performances",
            "https://www.nseindia.com/api/allIndices",
        )
        if df is None:
            return None

        cols = [
            "key", "index", "indexSymbol", "last", "variation", "percentChange",
            "open", "high", "low", "previousClose", "yearHigh", "yearLow",
            "pe", "pb", "dy", "declines", "advances", "unchanged",
            "perChange30d", "perChange365d", "previousDayVal",
            "oneWeekAgoVal", "oneMonthAgoVal", "oneYearAgoVal",
        ]
        df = _keep_cols(df, cols)
        return _clean(df.fillna(0))

    def index_live_indices_stocks_data(
        self,
        category:  str,
        list_only: bool = False,
    ) -> pd.DataFrame | list | None:
        """
        Return live stock data for all constituents in an NSE index.

        Parameters
        ----------
        category : str
            Index name, e.g. ``"NIFTY 50"``, ``"NIFTY BANK"``,
            ``"NIFTY IT"``.
        list_only : bool, optional
            If ``True``, return only the list of symbol strings.
            Default is ``False``.

        Returns
        -------
        pd.DataFrame or list or None

        Examples
        --------
        >>> nse.index_live_indices_stocks_data("NIFTY 50")
        >>> nse.index_live_indices_stocks_data("NIFTY IT", list_only=True)
        """
        try:
            enc = category.upper().replace("&", "%26").replace(" ", "%20")
            raw = self._get_json(
                "https://www.nseindia.com/market-data/live-equity-market",
                f"https://www.nseindia.com/api/equity-stockIndices?index={enc}"
            )
        except Exception as exc:
            self._log_error("index_live_indices_stocks_data", exc)
            return None

        df = (
            pd.DataFrame(raw["data"])
            .drop(["meta"], axis=1, errors="ignore")
            .set_index("symbol", drop=False)
        )
        if list_only:
            return df["symbol"].tolist()

        col_order = [
            "symbol", "previousClose", "open", "dayHigh", "dayLow", "lastPrice",
            "change", "pChange", "totalTradedVolume", "totalTradedValue",
            "nearWKH", "nearWKL", "perChange30d", "perChange365d", "ffmc",
        ]
        return _clean(_keep_cols(df, col_order))

    def index_live_nifty_50_returns(self) -> pd.DataFrame | None:
        """Return the periodic returns of the Nifty 50 index (1W to 5Y).

        Returns
        -------
        pd.DataFrame or None
            One row with columns for 1W, 1M, 3M, 6M, 1Y, 2Y, 3Y, 5Y
            percentage changes.

        Examples
        --------
        >>> nse.index_live_nifty_50_returns()
        """
        df = self._json_data_df(
            "https://www.nseindia.com/option-chain",
            "https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi"
            "?functionName=getIndicesReturn&&index=NIFTY%2050",
        )
        if df is None:
            return None
        cols = [
            "one_week_chng_per", "one_month_chng_per", "three_month_chng_per",
            "six_month_chng_per", "one_year_chng_per", "two_year_chng_per",
            "three_year_chng_per", "five_year_chng_per",
        ]
        return _clean(_keep_cols(df, cols).fillna(0))

    def index_live_contribution(
        self,
        *args,
        Index: str = "NIFTY 50",
        Mode:  str = "First Five",
    ) -> pd.DataFrame | None:
        """
        Return per-stock contribution data for an NSE index.

        Parameters
        ----------
        *args : str
            Positional shorthand: pass an index name and/or ``"Full"``.
        Index : str, optional
            NSE index name. Default is ``"NIFTY 50"``.
        Mode : str, optional
            ``"First Five"`` for the top-5 contributors, or ``"Full"`` for
            all constituents. Default is ``"First Five"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.index_live_contribution()
        >>> nse.index_live_contribution("Full")
        >>> nse.index_live_contribution("NIFTY IT")
        >>> nse.index_live_contribution("NIFTY IT", "Full")
        """
        if len(args) == 1:
            if args[0] in ("First Five", "Full"):
                Mode = args[0]
            else:
                Index = args[0]
        elif len(args) == 2:
            Index, Mode = args

        Index = str(Index).upper()
        Mode  = str(Mode)
        if Mode not in ("First Five", "Full"):
            raise ValueError("Mode must be 'First Five' or 'Full'")

        enc   = Index.replace("&", "%26").replace(" ", "%20")
        extra = "" if Mode == "First Five" else "&noofrecords=0"
        flag  = "0" if Mode == "First Five" else "1"

        df = self._json_data_df(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi"
            f"?functionName=getContributionData&index={enc}{extra}&flag={flag}",
        )
        if df is None:
            return None

        cols = ["icSymbol", "icSecurity", "lastTradedPrice", "changePer",
                "isPositive", "rnNegative", "changePoints"]
        return _clean(_keep_cols(df, cols).fillna(0))


    # ════════════════════════════════════════════════════════════════════════
    # ── Index — EOD & Historical ────────────────────────────────────────────

    def index_eod_bhav_copy(self, trade_date: str) -> pd.DataFrame | None:
        """
        Download the index bhavcopy (EOD close prices) for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format, e.g. ``"17-10-2025"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.index_eod_bhav_copy("17-10-2025")
        """
        raw = self._get_archive(
            f"https://nsearchives.nseindia.com/content/indices/"
            f"ind_close_all_{_fmt_trade_date(trade_date)}.csv"
        )
        return pd.read_csv(BytesIO(raw)) if raw else None

    def index_historical_data(
        self,
        index: str,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        period:    str | None = None,
    ) -> pd.DataFrame:
        """
        Return OHLCV + turnover history for an NSE index.

        Parameters
        ----------
        index : str
            Index name, e.g. ``"NIFTY 50"``, ``"NIFTY BANK"``.
        *args : str
            Positional shorthand for from_date, to_date, or period.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``. Defaults to today.
        period : str, optional
            Shorthand: ``"1D"``, ``"1W"``, ``"1M"``, ``"3M"``, ``"6M"``,
            ``"1Y"``, ``"2Y"``, ``"5Y"``, ``"10Y"``, ``"YTD"``, ``"MAX"``.

        Returns
        -------
        pd.DataFrame
            Columns: Date, Index Name, Open, High, Low, Close,
            Shares Traded, Turnover (₹ Cr).

        Examples
        --------
        >>> nse.index_historical_data("NIFTY 50", "01-01-2025", "17-10-2025")
        >>> nse.index_historical_data("NIFTY 50", "01-12-2025")
        >>> nse.index_historical_data("NIFTY BANK", "1W")
        """
        from_date, to_date, period, _ = _unpack_args(args, from_date, to_date, period)
        from_date, to_date = _resolve_dates(from_date, to_date, period)
        enc = index.replace(" ", "%20").upper()

        return self._hist_index_df(
            "https://www.nseindia.com/reports-indices-historical-index-data",
            f"https://www.nseindia.com/api/historicalOR/indicesHistory"
            f"?indexType={enc}&from={{}}&to={{}}",
            from_date, to_date,
            {
                "EOD_TIMESTAMP":      "Date",
                "EOD_INDEX_NAME":     "Index Name",
                "EOD_OPEN_INDEX_VAL": "Open",
                "EOD_HIGH_INDEX_VAL": "High",
                "EOD_LOW_INDEX_VAL":  "Low",
                "EOD_CLOSE_INDEX_VAL":"Close",
                "HIT_TRADED_QTY":     "Shares Traded",
                "HIT_TURN_OVER":      "Turnover (₹ Cr)",
            },
        )

    def index_pe_pb_div_historical_data(
        self,
        index: str,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        period:    str | None = None,
    ) -> pd.DataFrame:
        """
        Return historical P/E, P/B, and dividend-yield data for an NSE index.

        Parameters
        ----------
        index : str
            Index name, e.g. ``"NIFTY 50"``.
        *args : str
            Positional shorthand for from_date, to_date, or period.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``. Defaults to today.
        period : str, optional
            Shorthand period code (same set as :meth:`index_historical_data`).

        Returns
        -------
        pd.DataFrame
            Columns: Index Name, Date, P/E, P/B, Div Yield%.

        Examples
        --------
        >>> nse.index_pe_pb_div_historical_data("NIFTY 50", "01-01-2025", "17-10-2025")
        >>> nse.index_pe_pb_div_historical_data("NIFTY 50", "01-12-2025")
        >>> nse.index_pe_pb_div_historical_data("NIFTY BANK", "1Y")
        """
        from_date, to_date, period, _ = _unpack_args(args, from_date, to_date, period)
        from_date, to_date = _resolve_dates(from_date, to_date, period)
        enc = index.replace(" ", "%20").upper()

        df = self._hist_index_df(
            "https://www.nseindia.com/reports-indices-yield",
            f"https://www.nseindia.com/api/historicalOR/indicesYield"
            f"?indexType={enc}&from={{}}&to={{}}",
            from_date, to_date,
            {
                "IY_INDEX": "Index Name",
                "IY_DT":    "Date",
                "IY_PE":    "P/E",
                "IY_PB":    "P/B",
                "IY_DY":    "Div Yield%",
            },
        )
        for col in ("P/E", "P/B", "Div Yield%"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df.replace([np.inf, -np.inf], None, inplace=True)
        if "P/E" in df.columns:
            df.dropna(subset=["P/E"], inplace=True)
            df.ffill(inplace=True)
        return df

    def india_vix_historical_data(
        self,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        period:    str | None = None,
    ) -> pd.DataFrame:
        """
        Return historical India VIX OHLC and percentage-change data.

        Parameters
        ----------
        *args : str
            Positional shorthand for from_date, to_date, or period.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``. Defaults to today.
        period : str, optional
            Shorthand: ``"1M"``, ``"3M"``, ``"6M"``, ``"1Y"`` etc.

        Returns
        -------
        pd.DataFrame
            Columns: Date, Symbol, Open Price, High Price, Low Price,
            Close Price, Prev Close, VIX Pts Chg, VIX % Chg.

        Examples
        --------
        >>> nse.india_vix_historical_data("01-08-2025", "17-10-2025")
        >>> nse.india_vix_historical_data("1M")
        """
        from_date, to_date, period, _ = _unpack_args(args, from_date, to_date, period)
        from_date, to_date = _resolve_dates(from_date, to_date, period)

        return self._hist_index_df(
            "https://www.nseindia.com/report-detail/eq_security",
            "https://www.nseindia.com/api/historicalOR/vixhistory?from={}&to={}",
            from_date, to_date,
            {
                "EOD_TIMESTAMP":      "Date",
                "EOD_INDEX_NAME":     "Symbol",
                "EOD_OPEN_INDEX_VAL": "Open Price",
                "EOD_HIGH_INDEX_VAL": "High Price",
                "EOD_LOW_INDEX_VAL":  "Low Price",
                "EOD_CLOSE_INDEX_VAL":"Close Price",
                "EOD_PREV_CLOSE":     "Prev Close",
                "VIX_PTS_CHG":        "VIX Pts Chg",
                "VIX_PERC_CHG":       "VIX % Chg",
            },
        )


    # ════════════════════════════════════════════════════════════════════════
    # ── Index — Charts ──────────────────────────────────────────────────────

    def index_chart(self, index: str, timeframe: str = "1D") -> pd.DataFrame | None:
        """
        Return intraday / short-term chart data for an NSE index.

        Parameters
        ----------
        index : str
            Index name, e.g. ``"NIFTY 50"``, ``"NIFTY BANK"``.
        timeframe : str, optional
            Chart span: ``"1D"`` (default), ``"1M"``, ``"3M"``, ``"6M"``,
            ``"1Y"``.

        Returns
        -------
        pd.DataFrame or None
            Columns: datetime_utc, price, change, pct_change.

        Examples
        --------
        >>> nse.index_chart("NIFTY 50", "1D")
        >>> nse.index_chart("NIFTY 50", "3M")
        """
        enc  = index.upper().replace(" ", "%20").replace("&", "%26")
        data = self._chart_fetch(
            f"https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi"
            f"?functionName=getIndexChart&&index={enc}&flag={timeframe}"
        )
        if not data:
            return None
        rows = [
            {
                "datetime_utc": pd.to_datetime(r[0], unit="ms", utc=True).strftime("%Y-%m-%d %H:%M:%S"),
                "price":        r[1],
                "change":       r[3] if len(r) > 3 else None,
                "pct_change":   r[4] if len(r) > 4 else None,
                "flag":         r[2] if len(r) > 2 else None,
            }
            for r in (data.get("data") or {}).get("grapthData", [])
        ]
        return pd.DataFrame(rows)

    def india_vix_chart(self) -> pd.DataFrame | None:
        """Return live intraday chart data for the India VIX index.

        Returns
        -------
        pd.DataFrame or None
            Columns: datetime_utc, price, flag.

        Examples
        --------
        >>> nse.india_vix_chart()
        """

        try:
            obj = self._get_json(
                "https://www.nseindia.com/market-data/live-market-indices",
                "https://www.nseindia.com/api/chart-databyindex-dynamic?index=INDIA%20VIX&type=index"
            )
        except Exception as exc:
            self._log_error("india_vix_chart", exc)
            return None

        if obj is None:
            return None

        rows = [
            {
                "datetime_utc": pd.to_datetime(ts, unit="ms", utc=True).strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "flag":  flag,
            }
            for ts, price, flag in obj.get("grapthData", [])
        ]
        return pd.DataFrame(rows)



# ══════════════════════════════════════════════════════════════════════════════
# V. Capital Market (Equities)
# ══════════════════════════════════════════════════════════════════════════════

    def cm_live_gifty_nifty(self) -> pd.DataFrame | None:
        """Return the live Gift Nifty price along with USD/INR spot info.

        Returns
        -------
        pd.DataFrame or None
            Single-row with Gift Nifty symbol, lastprice, daychange,
            perchange, and USD/INR ltp.

        Examples
        --------
        >>> nse.cm_live_gifty_nifty()
        """
        data = self._get_json(
            "https://www.nseindia.com/option-chain",
            "https://www.nseindia.com/api/NextApi/apiClient"
            "?functionName=getGiftNifty",
        )
        if not data or "data" not in data:
            return None
        gn = data["data"].get("giftNifty", {})
        ui = data["data"].get("usdInr",    {})
        return pd.DataFrame([{
            "symbol":           gn.get("symbol"),
            "lastprice":        gn.get("lastprice"),
            "daychange":        gn.get("daychange"),
            "perchange":        gn.get("perchange"),
            "contractstraded":  gn.get("contractstraded"),
            "timestmp":         gn.get("timestmp"),
            "expirydate":       gn.get("expirydate"),
            "usdInr_symbol":    ui.get("symbol"),
            "usdInr_ltp":       ui.get("ltp"),
            "usdInr_updated_time": ui.get("updated_time"),
            "usdInr_expiry_dt": ui.get("expiry_dt"),
        }])

    def cm_live_market_statistics(self) -> pd.DataFrame | None:
        """
        Return today's market breadth summary.

        Returns
        -------
        pd.DataFrame or None
            Single row: Total, Advances, Declines, Unchanged, 52W High/Low,
            Upper/Lower Circuit, Market Cap ₹ Lac Crs, Market Cap Tn $,
            Registered Investors.

        Examples
        --------
        >>> nse.cm_live_market_statistics()
        """
        data = self._get_json(
            "https://www.nseindia.com/option-chain",
            "https://www.nseindia.com/api/NextApi/apiClient"
            "?functionName=getMarketStatistics",
        )
        if not data or "data" not in data:
            return None

        d    = data["data"]
        snap = d.get("snapshotCapitalMarket", {})
        fw   = d.get("fiftyTwoWeek",          {})
        ckt  = d.get("circuit",               {})
        ri   = d.get("regInvestors", "0")
        ri_cr = round(int(ri.replace(",", "").strip()) / 1e7, 2) if ri else 0.0

        return pd.DataFrame([{
            "Total":                      snap.get("total"),
            "Advances":                   snap.get("advances"),
            "Declines":                   snap.get("declines"),
            "Unchanged":                  snap.get("unchange"),
            "52W High":                   fw.get("high"),
            "52W Low":                    fw.get("low"),
            "Upper Circuit":              ckt.get("upper"),
            "Lower Circuit":              ckt.get("lower"),
            "Market Cap ₹ Lac Crs":       round(d.get("tlMKtCapLacCr",  0), 2),
            "Market Cap Tn $":            round(d.get("tlMKtCapTri",     0), 3),
            "Registered Investors":       ri,
            "Registered Investors (Cr)":  ri_cr,
            "Date":                       d.get("asOnDate"),
        }])

    def cm_live_equity_info(self, symbol: str) -> dict | None:
        """
        Return basic equity information for a symbol.

        Parameters
        ----------
        symbol : str
            NSE equity symbol, e.g. ``"RELIANCE"``.

        Returns
        -------
        dict or None
            Keys: Symbol, companyName, industry, boardStatus,
            tradingStatus, tradingSegment, derivatives, surveillance,
            surveillanceDesc, Facevalue, TotalSharesIssued.

        Examples
        --------
        >>> nse.cm_live_equity_info("RELIANCE")
        """
        symbol = symbol.replace(" ", "%20").replace("&", "%26")
        self.rotate_user_agent()

        def _call():
            data = self._get_json(
                f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}",
                f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
            )
            if not data or "error" in data:
                raise ValueError("No data")
            return {
                "Symbol":          symbol,
                "companyName":     data["info"]["companyName"],
                "industry":        data["info"]["industry"],
                "boardStatus":     data["securityInfo"]["boardStatus"],
                "tradingStatus":   data["securityInfo"]["tradingStatus"],
                "tradingSegment":  data["securityInfo"]["tradingSegment"],
                "derivatives":     data["securityInfo"]["derivatives"],
                "surveillance":    data["securityInfo"]["surveillance"]["surv"],
                "surveillanceDesc":data["securityInfo"]["surveillance"]["desc"],
                "Facevalue":       data["securityInfo"]["faceValue"],
                "TotalSharesIssued":data["securityInfo"]["issuedSize"],
            }

        try:
            return self._retry(_call)
        except Exception as exc:
            self._log_error("cm_live_equity_info", exc)
            return None

    def cm_live_equity_price_info(self, symbol: str) -> dict | None:
        """
        Return live price info including OHLC, VWAP, and the 5-level order book.

        Parameters
        ----------
        symbol : str
            NSE equity symbol, e.g. ``"RELIANCE"``.

        Returns
        -------
        dict or None
            Keys: Symbol, PreviousClose, LastTradedPrice, Change,
            PercentChange, Open, Close, High, Low, VWAP, UpperCircuit,
            LowerCircuit, Sector, Bid/Ask Price & Quantity 1–5.

        Examples
        --------
        >>> nse.cm_live_equity_price_info("RELIANCE")
        """
        symbol = symbol.replace(" ", "%20").replace("&", "%26")
        self.rotate_user_agent()

        def _call():
            ref_url = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
            data = self._get_json(
                ref_url,
                f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
            )
            trd  = self._get_json(
                ref_url,
                f"https://www.nseindia.com/api/quote-equity?symbol={symbol}&section=trade_info"
            )

            if not data or "error" in data:
                raise ValueError("No data")

            # Bug 3 fix: trade-info call may return None; guard before accessing .get()
            trd  = trd or {}
            ob   = trd.get("marketDeptOrderBook", {})
            bids = ob.get("bid", [])
            asks = ob.get("ask", [])
            bp   = [e.get("price",    0) for e in bids[:5]] + [0] * (5 - len(bids))
            bq   = [e.get("quantity", 0) for e in bids[:5]] + [0] * (5 - len(bids))
            ap   = [e.get("price",    0) for e in asks[:5]] + [0] * (5 - len(asks))
            aq   = [e.get("quantity", 0) for e in asks[:5]] + [0] * (5 - len(asks))
            pi   = data["priceInfo"]
            ii   = data["industryInfo"]

            result = {
                "Symbol":                    symbol,
                "PreviousClose":             pi["previousClose"],
                "LastTradedPrice":           pi["lastPrice"],
                "Change":                    pi["change"],
                "PercentChange":             pi["pChange"],
                "deliveryToTradedQuantity":  trd.get("securityWiseDP", {}).get("deliveryToTradedQuantity"),
                "Open":                      pi["open"],
                "Close":                     pi["close"],
                "High":                      pi["intraDayHighLow"]["max"],
                "Low":                       pi["intraDayHighLow"]["min"],
                "VWAP":                      pi["vwap"],
                "UpperCircuit":              pi["upperCP"],
                "LowerCircuit":              pi["lowerCP"],
                "Macro":                     ii["macro"],
                "Sector":                    ii["sector"],
                "Industry":                  ii["industry"],
                "BasicIndustry":             ii["basicIndustry"],
                "totalBuyQuantity":          ob.get("totalBuyQuantity",  0),
                "totalSellQuantity":         ob.get("totalSellQuantity", 0),
            }
            for i in range(5):
                result[f"Bid Price {i+1}"]    = bp[i]
                result[f"Bid Quantity {i+1}"] = bq[i]
                result[f"Ask Price {i+1}"]    = ap[i]
                result[f"Ask Quantity {i+1}"] = aq[i]
            return result

        try:
            return self._retry(_call)
        except Exception as exc:
            self._log_error("cm_live_equity_price_info", exc)
            return None

    def cm_live_equity_full_info(self, symbol: str) -> dict | None:
        """
        Return comprehensive live data for a symbol.

        Combines price, trade, security, margin, order-book, and
        fundamental data (P/E, market cap, annualised volatility) into
        a single flat dict.

        Parameters
        ----------
        symbol : str
            NSE equity symbol, e.g. ``"RELIANCE"``.

        Returns
        -------
        dict or None
            ~40 keys covering all aspects of the live quote.

        Examples
        --------
        >>> nse.cm_live_equity_full_info("RELIANCE")
        """
        symbol = symbol.replace(" ", "%20").replace("&", "%26")
        self.rotate_user_agent()
        ref_url = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
        api_url = (
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getSymbolData&marketType=N&series=EQ&symbol={symbol}"
        )

        def _call():
            resp = self._warm_and_fetch(ref_url, api_url, timeout=10)
            eq = (resp.json().get("equityResponse") or [None])[0]
            if not eq:
                raise ValueError("Empty equityResponse")
            return eq

        try:
            eq = self._retry(_call)
        except Exception as exc:
            self._log_error("cm_live_equity_full_info", exc)
            return None

        meta  = eq.get("metaData", {})
        trade = eq.get("tradeInfo", {})
        price = eq.get("priceInfo", {})
        sec   = eq.get("secInfo",   {})
        order = eq.get("orderBook", {})

        result = {
                # ================= BASIC =================
                "Symbol":                   meta.get("symbol"),
                "CompanyName":              meta.get("companyName"),
                "Index":                    sec.get("index"),
                "ISIN":                     meta.get("isinCode"),
                "Series":                   meta.get("series"),
                "MarketType":               meta.get("marketType"),
                "BoardStatus":              sec.get("boardStatus"),
                "TradingSegment":           sec.get("tradingSegment"),
                "SecurityStatus":           sec.get("secStatus"),

                # ================= PRICE =================
                "Open":                     meta.get("open"),
                "DayHigh":                  meta.get("dayHigh"),
                "DayLow":                   meta.get("dayLow"),
                "PreviousClose":            meta.get("previousClose"),
                "LastTradedPrice":          order.get("lastPrice"),
                "closePrice":               meta.get("closePrice"),
                "Change":                   meta.get("change"),
                "PercentChange":            meta.get("pChange"),
                "VWAP":                     meta.get("averagePrice"),

                # ================= VOLUME =================
                "TotalTradedVolume":        trade.get("totalTradedVolume"),
                "TotalTradedValue":         trade.get("totalTradedValue"),
                "Quantity Traded":          trade.get("quantitytraded"),
                "DeliveryQty":              trade.get("deliveryquantity"),
                "DeliveryPercent":          trade.get("deliveryToTradedQuantity"),
                "ImpactCost":               trade.get("impactCost"),                

                # ================= CIRCUIT =================
                "PriceBandRange":           price.get("priceBand"),
                "PriceBand":                price.get("ppriceBand"),
                "TickSize":                 price.get("tickSize"),
                
                # ================= ORDER BOOK =================
                "Bid Price 1": order.get("buyPrice1"), "Bid Quantity 1": order.get("buyQuantity1"),
                "Bid Price 2": order.get("buyPrice2"), "Bid Quantity 2": order.get("buyQuantity2"),
                "Bid Price 3": order.get("buyPrice3"), "Bid Quantity 3": order.get("buyQuantity3"),
                "Bid Price 4": order.get("buyPrice4"), "Bid Quantity 4": order.get("buyQuantity4"),
                "Bid Price 5": order.get("buyPrice5"), "Bid Quantity 5": order.get("buyQuantity5"),

                "Ask Price 1": order.get("sellPrice1"), "Ask Quantity 1": order.get("sellQuantity1"),
                "Ask Price 2": order.get("sellPrice2"), "Ask Quantity 2": order.get("sellQuantity2"),
                "Ask Price 3": order.get("sellPrice3"), "Ask Quantity 3": order.get("sellQuantity3"),
                "Ask Price 4": order.get("sellPrice4"), "Ask Quantity 4": order.get("sellQuantity4"),
                "Ask Price 5": order.get("sellPrice5"), "Ask Quantity 5": order.get("sellQuantity5"),

                "TotalBuyQuantity":         order.get("totalBuyQuantity"),
                "TotalSellQuantity":        order.get("totalSellQuantity"),
                "BuyQuantity%":             f"{pd.to_numeric(order.get('perBuyQty', 0), errors='coerce') or 0:.2f}",
                "SellQuantity%":            f"{pd.to_numeric(order.get('perSellQty', 0), errors='coerce') or 0:.2f}",  
                              
                # ================= FUNDAMENTAL =================
                "52WeekHigh":               price.get("yearHigh"),
                "52WeekLow":                price.get("yearLow"),
                "52WeekHighDate":           price.get("yearHightDt"),
                "52WeekLowDate":            price.get("yearLowDt"),
                "DailyVolatility":          price.get("cmDailyVolatility"),
                "AnnualisedVolatility":     price.get("cmAnnualVolatility"),               
                "SymbolPE":                 sec.get("pdSymbolPe"),                
                "FaceValue":                trade.get("faceValue"),
                "TotalIssuedShares":        trade.get("issuedSize"),
                "MarketCap":                trade.get("totalMarketCap"),
                "FreeFloatMcap":            trade.get("ffmc"),
                "DateOfListing":            sec.get("listingDate"),   

                # ================= Value at Risk (%) =================   
                "Security VaR":             sec.get("securityvar"),
                "Index VaR":                sec.get("indexvar"),
                "VaR Margin":               sec.get("varMargin"),                          
                "Extreme Loss Rate":        sec.get("extremelossMargin"),
                "Adhoc Margin":             sec.get("adhocMargin"),
                "Applicable Margin Rate":   sec.get("applicableMargin"),

                # ================= SECTOR =================
                "Macro":                    sec.get("macro"),
                "Sector":                   sec.get("sector"),
                "Industry":                 sec.get("industryInfo"),
                "BasicIndustry":            sec.get("basicIndustry"),

                # ================= META =================
                "LastUpdated":              eq.get("lastUpdateTime")
        }
        
        return result

    # ══════════════════════════════════════════════════════════════════════════════
    # VI. Capital Market (Equities)
    # ══════════════════════════════════════════════════════════════════════════════

    def cm_live_most_active_equity_by_value(self) -> pd.DataFrame | None:
        """Return today's most actively traded equities ranked by traded value.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_live_most_active_equity_by_value()
        """
        return self._cm_live_simple(
            "https://www.nseindia.com/market-data/most-active-equities",
            "https://www.nseindia.com/api/live-analysis-most-active-securities?index=value",
        )

    def cm_live_most_active_equity_by_vol(self) -> pd.DataFrame | None:
        """Return today's most actively traded equities ranked by traded volume.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_live_most_active_equity_by_vol()
        """
        return self._cm_live_simple(
            "https://www.nseindia.com/market-data/most-active-equities",
            "https://www.nseindia.com/api/live-analysis-most-active-securities?index=volume",
        )

    def cm_live_volume_spurts(self) -> pd.DataFrame | None:
        """
        Return equities with unusual intraday volume spikes (1-week and
        2-week average comparison).
        """
        df = self._json_data_df(
            "https://www.nseindia.com/market-data/volume-gainers-spurts",
            "https://www.nseindia.com/api/live-analysis-volume-gainers",
        )
        if df is None:
            return None
        df = df[[
            "symbol", "companyName", "volume", "week1AvgVolume", "week1volChange",
            "week2AvgVolume", "week2volChange", "ltp", "pChange", "turnover",
        ]]
        return df.rename(columns={
            "symbol":         "Symbol",
            "companyName":    "Security",
            "volume":         "Today Volume",
            "week1AvgVolume": "1 Week Avg. Volume",
            "week1volChange": "1 Week Change (×)",
            "week2AvgVolume": "2 Week Avg. Volume",
            "week2volChange": "2 Week Change (×)",
            "ltp":            "LTP",
            "pChange":        "% Change",
            "turnover":       "Turnover (₹ Lakhs)",
        })

    def cm_live_52week_high(self) -> pd.DataFrame | None:
        """Return equities that hit a new 52-week high today.

        Returns
        -------
        pd.DataFrame or None
            Columns: symbol, series, ltp, pChange, new52WHL, prev52WHL,
            prevHLDate.

        Examples
        --------
        >>> nse.cm_live_52week_high()
        """
        return self._cm_live_52wk(
            "https://www.nseindia.com/market-data/52-week-high-equity-market",
            "https://www.nseindia.com/api/live-analysis-data-52weekhighstock",
        )

    def cm_live_52week_low(self) -> pd.DataFrame | None:
        """Return equities that hit a new 52-week low today.

        Returns
        -------
        pd.DataFrame or None
            Columns: symbol, series, ltp, pChange, new52WHL, prev52WHL,
            prevHLDate.

        Examples
        --------
        >>> nse.cm_live_52week_low()
        """
        return self._cm_live_52wk(
            "https://www.nseindia.com/market-data/52-week-low-equity-market",
            "https://www.nseindia.com/api/live-analysis-data-52weeklowstock",
        )

    def cm_live_block_deal(self) -> pd.DataFrame | None:
        """Return today's block-deal data with session, OHLC, and volume.

        Returns
        -------
        pd.DataFrame or None
            Columns: session, symbol, series, open, dayHigh, dayLow,
            lastPrice, previousClose, pchange, totalTradedVolume,
            totalTradedValue.

        Examples
        --------
        >>> nse.cm_live_block_deal()
        """
        df = self._json_data_df(
            "https://www.nseindia.com/market-data/block-deal-watch",
            "https://www.nseindia.com/api/block-deal",
        )
        if df is None:
            return None
        if df.empty:
            return None
        return df[[
            "session", "symbol", "series", "open", "dayHigh", "dayLow",
            "lastPrice", "previousClose", "pchange", "totalTradedVolume",
            "totalTradedValue",
        ]]


    # ════════════════════════════════════════════════════════════════════════
    # ── CM — Charts ─────────────────────────────────────────────────────────

    def stock_chart(self, symbol: str, timeframe: str = "1D") -> pd.DataFrame | None:
        """
        Return intraday / short-term chart data for an NSE-listed stock.

        Parameters
        ----------
        symbol : str
            NSE equity ticker, e.g. ``"RELIANCE"``.
        timeframe : str, optional
            Chart span: ``"1D"`` (default), ``"1W"``.

        Returns
        -------
        pd.DataFrame or None
            Columns: datetime_utc, price, change, pct_change.

        Examples
        --------
        >>> nse.stock_chart("RELIANCE", "1D")
        >>> nse.stock_chart("INFY", "1W")
        """
        data = self._chart_fetch(
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getSymbolChartData&symbol={symbol}EQN&days={timeframe}"
        )
        if not data or "grapthData" not in data:
            return None
        rows = [
            {
                "datetime_utc": pd.to_datetime(r[0], unit="ms", utc=True).strftime("%Y-%m-%d %H:%M:%S"),
                "price":        r[1],
                "change":       r[3] if len(r) > 3 else None,
                "pct_change":   r[4] if len(r) > 4 else None,
                "flag":         r[2] if len(r) > 2 else None,
            }
            for r in data["grapthData"]
        ]
        return pd.DataFrame(rows)


    # ════════════════════════════════════════════════════════════════════════
    # ── CM — Corporate Filings (Live) ───────────────────────────────────────

    def cm_live_hist_insider_trading(
        self,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        period:    str | None = None,
        symbol:    str | None = None,
    ) -> pd.DataFrame | None:
        """
        Return historical insider-trading (PIT) disclosures.

        Parameters
        ----------
        *args : str
            Positional shorthand — pass a symbol, date range, or period.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``.
        period : str, optional
            Shorthand: ``"1D"``, ``"1W"``, ``"1M"``, ``"3M"``, ``"6M"``,
            ``"1Y"``.
        symbol : str, optional
            NSE equity symbol to filter, e.g. ``"RELIANCE"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_live_hist_insider_trading()
        >>> nse.cm_live_hist_insider_trading("1M")
        >>> nse.cm_live_hist_insider_trading("01-01-2025", "15-10-2025")
        >>> nse.cm_live_hist_insider_trading("RELIANCE")
        >>> nse.cm_live_hist_insider_trading("RELIANCE", "1M")
        >>> nse.cm_live_hist_insider_trading("RELIANCE", "01-01-2025", "15-10-2025")
        """
        cols = [
            "symbol", "company", "acqName", "personCategory", "secType",
            "befAcqSharesNo", "befAcqSharesPer", "remarks", "secAcq", "secVal",
            "tdpTransactionType", "securitiesTypePost", "afterAcqSharesNo",
            "afterAcqSharesPer", "acqfromDt", "acqtoDt", "intimDt", "acqMode",
            "derivativeType", "tdpDerivativeContractType", "buyValue",
            "buyQuantity", "sellValue", "sellquantity", "exchange", "date", "xbrl",
        ]
        return self._corp_filing(
            "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading",
            "https://www.nseindia.com/api/corporates-pit",
            *args,
            from_date=from_date, to_date=to_date, symbol=symbol, period=period,
            keep_cols=cols,
        )

    def cm_live_hist_corporate_announcement(
        self,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        symbol:    str | None = None,
    ) -> pd.DataFrame | None:
        """
        Return corporate announcements filed on the NSE.

        Parameters
        ----------
        *args : str
            Positional shorthand — pass a symbol, date range, or both.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``.
        symbol : str, optional
            NSE equity symbol, e.g. ``"RELIANCE"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_live_hist_corporate_announcement()
        >>> nse.cm_live_hist_corporate_announcement("12-10-2025", "15-10-2025")
        >>> nse.cm_live_hist_corporate_announcement("RELIANCE")
        >>> nse.cm_live_hist_corporate_announcement("RELIANCE", "01-01-2025", "15-10-2025")
        """
        # default → today (only when nothing provided)
        if not args and not from_date and not to_date and not symbol:
            today = datetime.now().strftime("%d-%m-%Y")
            from_date = today
            to_date   = today
            
        cols = [
            "symbol", "sm_name", "smIndustry", "desc",
            "attchmntText", "attchmntFile", "fileSize", "an_dt",
        ]
        return self._corp_filing(
            "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            "https://www.nseindia.com/api/corporate-announcements",
            *args,
            from_date=from_date, to_date=to_date, symbol=symbol,
            extra="&reqXbrl=false", keep_cols=cols,
        )

    def cm_live_hist_corporate_action(
        self,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        period:    str | None = None,
        symbol:    str | None = None,
        filter:    str | None = None,
    ) -> pd.DataFrame | None:
        """
        Return corporate actions — dividends, splits, bonus, buybacks, etc.

        When called with a *symbol* only (no date), returns all upcoming
        actions for that company.  An optional *filter* kwarg performs a
        substring search on the ``PURPOSE`` column.

        Parameters
        ----------
        *args : str
            Positional shorthand — symbol, date range, period, or purpose
            filter string (e.g. ``"Dividend"``).
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``. Defaults to 90 days from today.
        period : str, optional
            Shorthand: ``"1D"``, ``"1W"``, ``"1M"``, ``"3M"``, ``"6M"``,
            ``"1Y"``.
        symbol : str, optional
            NSE equity symbol.
        filter : str, optional
            Substring to filter the ``PURPOSE`` column (case-insensitive).

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_live_hist_corporate_action()
        >>> nse.cm_live_hist_corporate_action("1M")
        >>> nse.cm_live_hist_corporate_action("01-01-2025", "15-10-2025")
        >>> nse.cm_live_hist_corporate_action("LAURUSLABS")
        >>> nse.cm_live_hist_corporate_action("RELIANCE", "01-01-2025", "15-10-2025")
        >>> nse.cm_live_hist_corporate_action("01-01-2025", "15-03-2025", "Dividend")
        """
        from_date, to_date, period, symbol = _unpack_args(
            args, from_date, to_date, period, symbol, short=True
        )
        today     = datetime.now()
        ref_url   = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
        base      = "https://www.nseindia.com/api/corporates-corporateActions"

        if symbol and not any([from_date, to_date, period]):
            api = f"{base}?index=equities&symbol={symbol}"
        else:
            if period:
                from_date, to_date = _resolve_dates(period=period)
            from_date = from_date or (today - timedelta(days=1)).strftime("%d-%m-%Y")
            to_date   = to_date   or (today + timedelta(days=90)).strftime("%d-%m-%Y")
            api = (
                f"{base}?index=equities&from_date={from_date}&to_date={to_date}"
                f"&symbol={symbol}" if symbol
                else f"{base}?index=equities&from_date={from_date}&to_date={to_date}"
            )

        data    = self._get_json(ref_url, api)
        records = data.get("data") if isinstance(data, dict) else data
        if not records:
            return None

        df = pd.DataFrame(records).rename(columns={
            "symbol":       "SYMBOL",
            "comp":         "COMPANY NAME",
            "series":       "SERIES",
            "subject":      "PURPOSE",
            "faceVal":      "FACE VALUE",
            "exDate":       "EX-DATE",
            "recDate":      "RECORD DATE",
            "bcStartDate":  "BOOK CLOSURE START DATE",
            "bcEndDate":    "BOOK CLOSURE END DATE",
        })
        if filter and "PURPOSE" in df.columns:
            df = df[df["PURPOSE"].str.contains(filter, case=False, na=False)]

        cols = [
            "SYMBOL", "COMPANY NAME", "SERIES", "PURPOSE", "FACE VALUE",
            "EX-DATE", "RECORD DATE", "BOOK CLOSURE START DATE", "BOOK CLOSURE END DATE",
        ]
        return _clean_str(_keep_cols(df, cols))

    def cm_live_today_event_calendar(
        self,
        from_date: str | None = None,
        to_date:   str | None = None,
    ) -> pd.DataFrame | None:
        """Return the corporate event calendar for today (or a date range).

        Parameters
        ----------
        from_date : str, optional
            Start date in ``DD-MM-YYYY``. Defaults to today.
        to_date : str, optional
            End date in ``DD-MM-YYYY``. Defaults to today.

        Returns
        -------
        pd.DataFrame or None
            Columns: symbol, company, purpose, bm_desc, date.

        Examples
        --------
        >>> nse.cm_live_today_event_calendar()
        >>> nse.cm_live_today_event_calendar("01-01-2025", "01-01-2025")
        """
        today     = datetime.now().strftime("%d-%m-%Y")
        from_date = from_date or today
        to_date   = to_date   or today
        return self._corp_filing(
            "https://www.nseindia.com/companies-listing/corporate-filings-event-calendar",
            "https://www.nseindia.com/api/event-calendar",
            from_date=from_date, to_date=to_date,
            keep_cols=["symbol", "company", "purpose", "bm_desc", "date"],
        )

    def cm_live_upcoming_event_calendar(self) -> pd.DataFrame | None:
        """Return all upcoming corporate events in the NSE event calendar.

        Returns
        -------
        pd.DataFrame or None
            Columns: symbol, company, purpose, bm_desc, date.

        Examples
        --------
        >>> nse.cm_live_upcoming_event_calendar()
        """
        data = self._get_json(
            "https://www.nseindia.com/companies-listing/corporate-filings-event-calendar",
            "https://www.nseindia.com/api/event-calendar?",
        )
        if not isinstance(data, list):
            return None
        df = pd.DataFrame(data)
        return df[["symbol", "company", "purpose", "bm_desc", "date"]] if not df.empty else None

    def cm_live_hist_board_meetings(
        self,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        symbol:    str | None = None,
    ) -> pd.DataFrame | None:
        """Return historical board meeting announcements filed on NSE.

        Parameters
        ----------
        *args : str
            Positional shorthand — symbol, date range, or both.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``.
        symbol : str, optional
            NSE equity symbol.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_live_hist_board_meetings()
        >>> nse.cm_live_hist_board_meetings("01-01-2025", "15-10-2025")
        >>> nse.cm_live_hist_board_meetings("RELIANCE")
        >>> nse.cm_live_hist_board_meetings("RELIANCE", "01-01-2025", "15-10-2025")
        """
        cols = [
            "bm_symbol", "sm_name", "sm_indusrty", "bm_purpose", "bm_desc",
            "bm_date", "attachment", "attFileSize", "bm_timestamp",
        ]
        return self._corp_filing(
            "https://www.nseindia.com/companies-listing/corporate-filings-board-meetings",
            "https://www.nseindia.com/api/corporate-board-meetings",
            *args,
            from_date=from_date, to_date=to_date, symbol=symbol,
            keep_cols=cols,
        )

    def cm_live_hist_Shareholder_meetings(
        self,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        symbol:    str | None = None,
    ) -> pd.DataFrame | None:
        """Return postal ballot and shareholder meeting filings.

        Parameters
        ----------
        *args : str
            Positional shorthand — symbol, date range, or both.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``. When omitted (and no symbol given),
            NSE returns all available records.
        to_date : str, optional
            End date in ``DD-MM-YYYY``.
        symbol : str, optional
            NSE equity symbol.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_live_hist_Shareholder_meetings()
        >>> nse.cm_live_hist_Shareholder_meetings("01-01-2025", "15-10-2025")
        >>> nse.cm_live_hist_Shareholder_meetings("RELIANCE")
        >>> nse.cm_live_hist_Shareholder_meetings("RELIANCE", "01-01-2025", "15-10-2025")
        """
        # Parse positional args manually (no period support — match old behaviour)
        for arg in args:
            if not isinstance(arg, str):
                continue
            if _DATE_PATTERN.match(arg):
                if not from_date:
                    from_date = arg
                elif not to_date:
                    to_date = arg
            else:
                symbol = arg.upper()

        # Build URL — intentionally NO date defaulting to match old code:
        # calling with no args fetches the full listing from NSE.
        ref_url  = "https://www.nseindia.com/companies-listing/corporate-filings-postal-ballot"
        base_api = "https://www.nseindia.com/api/postal-ballot"

        if symbol and from_date and to_date:
            api = f"{base_api}?index=equities&from_date={from_date}&to_date={to_date}&symbol={symbol}"
        elif symbol:
            api = f"{base_api}?index=equities&symbol={symbol}"
        elif from_date and to_date:
            api = f"{base_api}?index=equities&from_date={from_date}&to_date={to_date}"
        else:
            api = f"{base_api}?index=equities"

        data    = self._get_json(ref_url, api)
        records = data.get("data") if isinstance(data, dict) else data
        if not records or not isinstance(records, list):
            return None

        df   = pd.DataFrame(records)
        cols = ["symbol", "sLN", "bdt", "text", "type", "attachment", "date"]
        df   = _keep_cols(df, cols)
        return _clean_str(df)

    def cm_live_hist_qualified_institutional_placement(self, *args, **kw) -> pd.DataFrame | None:
        """Return QIP filings — In-Principle or Listing Stage.

        Parameters
        ----------
        *args : str
            Positional shorthand — stage, symbol, date range, or period.
            Stage values: ``"In-Principle"``, ``"Listing Stage"``.
        **kw
            Keyword overrides: ``stage``, ``symbol``, ``from_date``,
            ``to_date``, ``period``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_live_hist_qualified_institutional_placement("In-Principle")
        >>> nse.cm_live_hist_qualified_institutional_placement("Listing Stage")
        >>> nse.cm_live_hist_qualified_institutional_placement("In-Principle", "1Y")
        >>> nse.cm_live_hist_qualified_institutional_placement("RELIANCE")
        """
        return self._further_issue(
            "https://www.nseindia.com/api/corporate-further-issues-qip",
            "https://www.nseindia.com/companies-listing/corporate-filings-QIP",
            {"In-Principle": "FIQIPIP", "Listing Stage": "FIQIPLS"},
            {
                "In-Principle": {
                    "nseSymbol": "Symbol", "companyName": "Company Name",
                    "stage": "Stage", "issue_type": "Issue Type",
                    "dateBrdResol": "Board Resolution Date",
                    "dateOfSHApp": "Shareholder Approval Date",
                    "totalAmtOfIssueSize": "Total Issue Size",
                    "prcntagePerSecrtyProDiscNotice": "Percentage per Security Notice",
                    "listedAt": "Listed At",
                    "dateOfSubmission": "Submission Date",
                    "xmlFileName": "XML Link",
                },
                "Listing Stage": {
                    "nsesymbol": "Symbol", "companyName": "Company Name",
                    "stage": "Stage", "issue_type": "Issue Type",
                    "boardResolutionDate": "Board Resolution Date",
                    "dtOfBIDOpening": "BID Opening Date",
                    "dtOfBIDClosing": "BID Closing Date",
                    "dtOfAllotmentOfShares": "Allotment Date",
                    "noOfSharesAllotted": "No of Shares Allotted",
                    "finalAmountOfIssueSize": "Final Issue Size",
                    "minIssPricePerUnit": "Min Issue Price",
                    "issPricePerUnit": "Issue Price Per Unit",
                    "noOfAllottees": "No of Allottees",
                    "noOfEquitySharesListed": "No of Equity Shares Listed",
                    "dateOfSubmission": "Submission Date",
                    "dateOfListing": "Listing Date",
                    "dateOfTradingApproval": "Trading Approval Date",
                    "xmlFileName": "XML Link",
                },
            },
            *args, **kw,
        )

    def cm_live_hist_preferential_issue(self, *args, **kw) -> pd.DataFrame | None:
        """Return preferential-issue filings — In-Principle or Listing Stage.

        Parameters
        ----------
        *args : str
            Stage (``"In-Principle"`` / ``"Listing Stage"``), symbol,
            date range, or period codes.
        **kw
            Keyword overrides for ``stage``, ``symbol``, ``from_date``,
            ``to_date``, ``period``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_live_hist_preferential_issue("In-Principle")
        >>> nse.cm_live_hist_preferential_issue("Listing Stage", "1Y")
        >>> nse.cm_live_hist_preferential_issue("In-Principle", "RELIANCE", "01-01-2025")
        """
        return self._further_issue(
            "https://www.nseindia.com/api/corporate-further-issues-pref",
            "https://www.nseindia.com/companies-listing/corporate-filings-PREF",
            {"In-Principle": "FIPREFIP", "Listing Stage": "FIPREFLS"},
            {
                "In-Principle": {
                    "nseSymbol": "Symbol", "nameOfTheCompany": "Company Name",
                    "stage": "Stage", "issueType": "Issue Type",
                    "dateBrdResoln": "Date of Board Resolution",
                    "boardResDate": "Board Resolution Date",
                    "categoryOfAllottee": "category Of Allottee",
                    "totalAmtRaised": "Total Amount Size",
                    "considerationBy": "considerationBy",
                    "descriptionOfOtherCon": "descriptionOfOtherCon",
                    "dateOfSubmission": "Submission Date",
                    "checklist_zip_file_name": "zip Link",
                },
                "Listing Stage": {
                    "nseSymbol": "Symbol", "nameOfTheCompany": "Company Name",
                    "stage": "Stage", "issueType": "Issue Type",
                    "boardResDate": "Board Resolution Date",
                    "dateOfAllotmentOfShares": "Allotment Date",
                    "totalNumOfSharesAllotted": "No of Shares Allotted",
                    "amountRaised": "Final Issue Size",
                    "offerPricePerSecurity": "Issue Price Per Unit",
                    "numberOfEquitySharesListed": "No of Equity Shares Listed",
                    "dateOfSubmission": "Submission Date",
                    "dateOfListing": "Listing Date",
                    "dateOfTradingApproval": "Trading Approval Date",
                    "xmlFileName": "XML Link",
                },
            },
            *args, **kw,
        )

    def cm_live_hist_right_issue(self, *args, **kw) -> pd.DataFrame | None:
        """Return rights-issue filings — In-Principle or Listing Stage.

        Parameters
        ----------
        *args : str
            Stage, symbol, date range, or period.
        **kw
            Keyword overrides for ``stage``, ``symbol``, ``from_date``,
            ``to_date``, ``period``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_live_hist_right_issue("In-Principle")
        >>> nse.cm_live_hist_right_issue("Listing Stage", "1Y")
        >>> nse.cm_live_hist_right_issue("In-Principle", "RELIANCE", "01-01-2025")
        """
        return self._further_issue(
            "https://www.nseindia.com/api/corporate-further-issues-ri",
            "https://www.nseindia.com/companies-listing/corporate-filings-RI",
            {"In-Principle": "FIPREFIP", "Listing Stage": "FIPREFLS"},
            {
                "In-Principle": {
                    "nseSymbol": "Symbol", "companyName": "Company Name",
                    "stage": "Stage", "issueType": "Issue Type",
                    "boardResolutionDt": "Board Resolution Date",
                    "dateOfBrdResIssueApproving": "Board Approval Date",
                    "dateOfSubmission": "Submission Date",
                    "considerationBy": "Consideration Type",
                    "descOfOtherConsideration": "Other Consideration Description",
                    "totalAmntRaised": "Total Amount Raised",
                    "xmlFileName": "XML Link",
                },
                "Listing Stage": {
                    "nseSymbol": "Symbol", "companyName": "Company Name",
                    "stage": "Stage", "issueType": "Issue Type",
                    "boardResolutionDt": "Board Resolution Date",
                    "recordDate": "Record Date",
                    "rightRatio": "Rights Ratio",
                    "offerPrice": "Offer Price",
                    "issueOpenDate": "Issue Open Date",
                    "issueCloseDate": "Issue Close Date",
                    "openingDtOfEnlightment": "Enlightment Open Date",
                    "closingDtOfEnlightment": "Enlightment Close Date",
                    "dtOfAllotmentsOfShare": "Allotment Date",
                    "noOfSharesAlloted": "No of Shares Allotted",
                    "noOfSharesInAbeyance": "No of Shares in Abeyance",
                    "amntRaised": "Amount Raised",
                    "noOfSharesListed": "No of Shares Listed",
                    "dtOfSubmission": "Submission Date",
                    "dateOfListing": "Listing Date",
                    "dateOfTradingApp": "Trading Approval Date",
                    "xmlFileName": "XML Link",
                },
            },
            *args, **kw,
        )

    # def cm_live_voting_results(self) -> pd.DataFrame | None:
    #     """Return corporate voting results with per-agenda breakdown.

    #     Returns
    #     -------
    #     pd.DataFrame or None
    #         One row per agenda item, flattened from the nested JSON.

    #     Examples
    #     --------
    #     >>> nse.cm_live_voting_results()
    #     """
    #     ref_url = "https://www.nseindia.com/companies-listing/corporate-filings-voting-results"
    #     data    = self._get_json(
    #         ref_url, "https://www.nseindia.com/api/corporate-voting-results?"
    #     )
    #     if not data:
    #         return None

    #     rows = []
    #     for item in data:
    #         meta    = item.get("metadata", {})
    #         agendas = meta.get("agendas", []) or item.get("agendas", [])
    #         if agendas:
    #             for ag in agendas:
    #                 rows.append({**meta, **ag})
    #         else:
    #             rows.append(meta)

    #     if not rows:
    #         return None

    #     df = pd.DataFrame(rows)
    #     df.replace({np.inf: None, -np.inf: None}, inplace=True)
    #     df.fillna("", inplace=True)

    #     def _flatten(v):
    #         if isinstance(v, (list, dict)):
    #             return json.dumps(v, ensure_ascii=False)
    #         return "" if v is None else str(v)

    #     for col in df.columns:
    #         df[col] = df[col].map(_flatten)
    #     return df.reset_index(drop=True)

    def cm_live_voting_results(self) -> pd.DataFrame | None:
        """Return corporate voting results with per-agenda breakdown.

        Returns
        -------
        pd.DataFrame or None
            One row per agenda item, flattened from the nested JSON.

        Examples
        --------
        >>> nse.cm_live_voting_results()
        """
        ref_url = "https://www.nseindia.com/companies-listing/corporate-filings-voting-results"
        data = self._get_json(
            ref_url,
            "https://www.nseindia.com/api/corporate-voting-results?"
        )

        if not data:
            return None

        # -----------------------------
        # Flatten metadata + agendas
        # -----------------------------
        rows = []
        for item in data:
            meta = item.get("metadata", {})
            agendas = meta.pop("agendas", [])   # remove nested list

            if agendas:
                for ag in agendas:
                    rows.append({**meta, **ag})
            else:
                rows.append(meta)

        if not rows:
            return None

        df = pd.DataFrame(rows)

        # -----------------------------
        # Clean values
        # -----------------------------
        df.replace({np.inf: None, -np.inf: None}, inplace=True)
        df.fillna("", inplace=True)

        # -----------------------------
        # Flatten nested values
        # -----------------------------
        def _flatten(v):
            if isinstance(v, (list, dict)):
                return json.dumps(v, ensure_ascii=False)
            return "" if v is None else str(v)

        for col in df.columns:
            df[col] = df[col].map(_flatten)

        # -----------------------------
        # Reorder important columns
        # -----------------------------
        preferred_cols = [
            "vrSymbol",
            "vrCompanyName",
            "vrMeetingType",
            "vrTimestamp",
            "vrTypeOfSubmission",
            "vrAttachment",
            "vrbroadcastDt",
            "vrRevisedDate",
            "vrRevisedRemark",
            "vrResolution",
            "vrResReq",
            "vrGrpInterested",
            "vrTotSharesOnRec",
            "vrTotSharesProPer",
            "vrTotSharesPublicPer",
            "vrTotSharesProVid",
            "vrTotSharesPublicVid",
            "vrTotPercFor",
            "vrTotPercAgainst"
        ]

        existing_cols = [c for c in preferred_cols if c in df.columns]
        df = df[existing_cols + [c for c in df.columns if c not in existing_cols]]

        # -----------------------------
        # Sort by latest broadcast date
        # -----------------------------
        if "vrbroadcastDt" in df.columns:
            try:
                df["_sort"] = pd.to_datetime(df["vrbroadcastDt"], errors="coerce")
                df.sort_values("_sort", ascending=False, inplace=True)
                df.drop(columns="_sort", inplace=True)
            except Exception:
                pass

        # -----------------------------
        # Final cleanup
        # -----------------------------
        df.reset_index(drop=True, inplace=True)

        return df

    def cm_live_qtly_shareholding_patterns(self) -> pd.DataFrame | None:
        """Return the latest quarterly shareholding-pattern filings.

        Returns
        -------
        pd.DataFrame or None
            Columns: symbol, name, pr_and_prgrp, public_val, employeeTrusts,
            revisedStatus, date, submissionDate, revisionDate.

        Examples
        --------
        >>> nse.cm_live_qtly_shareholding_patterns()
        """
        ref_url = "https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern"
        data    = self._get_json(
            ref_url,
            "https://www.nseindia.com/api/corporate-share-holdings-master?index=equities",
        )
        if not isinstance(data, list):
            return None
        df   = pd.DataFrame(data)
        cols = [
            "symbol", "name", "pr_and_prgrp", "public_val", "employeeTrusts",
            "revisedStatus", "date", "submissionDate", "revisionDate",
            "xbrl", "broadcastDate", "systemDate", "timeDifference",
        ]
        return _keep_cols(df, cols) if not df.empty else None

    def cm_live_hist_br_sr(
        self,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        symbol:    str | None = None,
    ) -> pd.DataFrame | None:
        """Return Business Responsibility and Sustainability Report (BR/SR) filings.

        Parameters
        ----------
        *args : str
            Positional shorthand — symbol, date range, or both.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``.
        symbol : str, optional
            NSE equity symbol.

        Returns
        -------
        pd.DataFrame or None
            Columns: symbol, companyName, fyFrom, fyTo,
            submissionDate, revisionDate.

        Examples
        --------
        >>> nse.cm_live_hist_br_sr()
        >>> nse.cm_live_hist_br_sr("RELIANCE")
        >>> nse.cm_live_hist_br_sr("01-01-2025", "15-10-2025")
        >>> nse.cm_live_hist_br_sr("RELIANCE", "01-01-2025", "15-10-2025")
        """
        cols = ["symbol", "companyName", "fyFrom", "fyTo", "submissionDate", "revisionDate"]
        return self._corp_filing(
            "https://www.nseindia.com/companies-listing/"
            "corporate-filings-bussiness-sustainabilitiy-reports",
            "https://www.nseindia.com/api/corporate-bussiness-sustainabilitiy",
            *args,
            from_date=from_date, to_date=to_date, symbol=symbol,
            keep_cols=cols,
        )


    # ════════════════════════════════════════════════════════════════════════
    # ── CM — EOD ────────────────────────────────────────────────────────────

    def cm_eod_fii_dii_activity(self, exchange: str = "All") -> pd.DataFrame | None:
        """
        Return the latest FII and DII activity data.

        Parameters
        ----------
        exchange : str, optional
            ``"Nse"`` for NSE-only data, or ``"All"`` (default) for combined.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_eod_fii_dii_activity()
        >>> nse.cm_eod_fii_dii_activity("Nse")
        """
        self.rotate_user_agent()
        ep = {
            "Nse": "https://www.nseindia.com/api/fiidiiTradeNse",
            "All": "https://www.nseindia.com/api/fiidiiTradeReact",
        }
        try:
            resp = self._warm_and_fetch(
                "https://www.nseindia.com/reports/fii-dii",
                ep.get(exchange, ep["All"]),
                timeout=10
            )
            return pd.DataFrame(resp.json())
        except Exception as exc:
            self._log_error("cm_eod_fii_dii_activity", exc)
            return None

    def cm_eod_market_activity_report(self, trade_date: str) -> list | None:
        """
        Return the raw market activity report (CSV rows) for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YY`` (two-digit year) format, e.g.
            ``"17-10-25"``.

        Returns
        -------
        list or None
            List of CSV rows; each row is a list of strings.

        Examples
        --------
        >>> nse.cm_eod_market_activity_report("17-10-25")
        """
        try:
            raw = self._get_archive(
                f"https://nsearchives.nseindia.com/archives/equities/mkt/"
                f"MA{_fmt_trade_date(trade_date, '%d%m%y')}.csv"
            )
            return list(csv.reader(raw.decode("utf-8", "ignore").splitlines())) if raw else None
        except Exception as exc:
            self._log_error("cm_eod_market_activity_report", exc)
            return None

    def cm_eod_bhavcopy_with_delivery(self, trade_date: str) -> pd.DataFrame | None:
        """
        Download the full CM bhavcopy with delivery data for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None
            Full bhavcopy with delivery percentage column.

        Examples
        --------
        >>> nse.cm_eod_bhavcopy_with_delivery("17-10-2025")
        """
        dt  = _fmt_trade_date(trade_date)
        raw = self._get_archive(
            f"https://nsearchives.nseindia.com/products/content/"
            f"sec_bhavdata_full_{dt}.csv"
        )
        if not raw:
            return None
        df = pd.read_csv(BytesIO(raw))
        df.columns        = [c.replace(" ", "") for c in df.columns]
        df["SERIES"]      = df["SERIES"].str.replace(" ", "")
        df["DATE1"]       = df["DATE1"].str.replace(" ", "")
        return df

    def cm_eod_equity_bhavcopy(self, trade_date: str) -> pd.DataFrame | None:
        """
        Download the NSE CM equity bhavcopy (EQ series only) for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None
            EQ-series rows only from the bhavcopy archive.

        Examples
        --------
        >>> nse.cm_eod_equity_bhavcopy("17-10-2025")
        """
        dt  = _fmt_trade_date(trade_date, "%Y%m%d")
        raw = self._get_archive(
            f"https://nsearchives.nseindia.com/content/cm/"
            f"BhavCopy_NSE_CM_0_0_0_{dt}_F_0000.csv.zip"
        )
        if not raw:
            return None
        df = self._zip_csv(raw)
        return df[df["SctySrs"] == "EQ"].reset_index(drop=True) if not df.empty else None

    def cm_eod_52_week_high_low(self, trade_date: str) -> list | None:
        """
        Return 52-week high/low CSV rows for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        list or None
            List of CSV rows (each row is a list of strings).

        Examples
        --------
        >>> nse.cm_eod_52_week_high_low("17-10-2025")
        """
        try:
            dt  = _fmt_trade_date(trade_date)
            raw = self._get_archive(
                f"https://nsearchives.nseindia.com/content/"
                f"CM_52_wk_High_low_{dt}.csv"
            )
            return list(csv.reader(raw.decode("utf-8", "ignore").splitlines())) if raw else None
        except Exception as exc:
            self._log_error("cm_eod_52_week_high_low", exc)
            return None

    def cm_eod_bulk_deal(self)  -> pd.DataFrame | None:
        """Return the latest bulk deals CSV from NSE archives.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_eod_bulk_deal()
        """
        return self._get_csv_archive(
            "https://nsearchives.nseindia.com/content/equities/bulk.csv"
        )

    def cm_eod_block_deal(self) -> pd.DataFrame | None:
        """Return the latest block deals CSV from NSE archives.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_eod_block_deal()
        """
        return self._get_csv_archive(
            "https://nsearchives.nseindia.com/content/equities/block.csv"
        )

    def cm_eod_series_change(self) -> pd.DataFrame | None:
        """Return the latest series-change CSV from NSE archives.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_eod_series_change()
        """
        return self._get_csv_archive(
            "https://nsearchives.nseindia.com/content/equities/series_change.csv"
        )

    def cm_eod_shortselling(self, trade_date: str) -> pd.DataFrame | None:
        """Return the short-selling data for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_eod_shortselling("17-10-2025")
        """
        return self._get_csv_archive(
            f"https://nsearchives.nseindia.com/archives/equities/shortSelling/"
            f"shortselling_{_fmt_trade_date(trade_date).upper()}.csv"
        )

    def cm_eod_surveillance_indicator(self, trade_date: str) -> pd.DataFrame | None:
        """
        Return the GSM/ASM surveillance indicator file for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YY`` (two-digit year) format, e.g.
            ``"17-10-25"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_eod_surveillance_indicator("17-10-25")
        """
        dt  = datetime.strptime(trade_date, "%d-%m-%y")
        raw = self._get_archive(
            f"https://nsearchives.nseindia.com/content/cm/"
            f"REG1_IND{dt.strftime('%d%m%y').upper()}.csv"
        )
        if not raw:
            return None
        df = pd.read_csv(BytesIO(raw))
        cols = df.loc[:, "GSM":"Filler31"].columns
        df[cols] = df[cols].astype("object").replace(100, "")

        return df

    def cm_eod_eq_band_changes(self, trade_date: str) -> pd.DataFrame | None:
        """Return price-band changes for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_eod_eq_band_changes("17-10-2025")
        """
        return self._get_csv_archive(
            f"https://nsearchives.nseindia.com/content/equities/"
            f"eq_band_changes_{_fmt_trade_date(trade_date).upper()}.csv"
        )

    def cm_eod_eq_price_band(self, trade_date: str) -> pd.DataFrame | None:
        """Return the full price-band list for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_eod_eq_price_band("17-10-2025")
        """
        return self._get_csv_archive(
            f"https://nsearchives.nseindia.com/content/equities/"
            f"sec_list_{_fmt_trade_date(trade_date).upper()}.csv"
        )

    def cm_eod_pe_ratio(self, trade_date: str) -> pd.DataFrame | None:
        """Return the P/E ratio file for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YY`` (two-digit year) format.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_eod_pe_ratio("17-10-25")
        """
        return self._get_csv_archive(
            f"https://nsearchives.nseindia.com/content/equities/peDetail/"
            f"PE_{_fmt_trade_date(trade_date, '%d%m%y').upper()}.csv"
        )

    def cm_eod_mcap(self, trade_date: str) -> pd.DataFrame | None:
        """
        Return the market-capitalisation file for a trade date.

        Extracted from the bhavcopy ZIP archive (``mcap*.csv`` member).

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YY`` (two-digit year) format.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_eod_mcap("17-10-25")
        """
        raw = self._get_archive(
            f"https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/"
            f"PR{_fmt_trade_date(trade_date, '%d%m%y').upper()}.zip"
        )
        if not raw:
            return None
        with zipfile.ZipFile(BytesIO(raw), "r") as zf:
            for name in zf.namelist():
                if name.lower().startswith("mcap") and name.endswith(".csv"):
                    return pd.read_csv(zf.open(name))
        return None

    def cm_eod_eq_name_change(self) -> pd.DataFrame | None:
        """Return the full historical equity name-change register.

        Returns
        -------
        pd.DataFrame or None
            Sorted descending by effective date.

        Examples
        --------
        >>> nse.cm_eod_eq_name_change()
        """
        return self._name_change_csv(
            "https://nsearchives.nseindia.com/content/equities/namechange.csv",
            has_header=True,
        )

    def cm_eod_eq_symbol_change(self) -> pd.DataFrame | None:
        """Return the full historical equity symbol-change register.

        Returns
        -------
        pd.DataFrame or None
            Sorted descending by effective date.

        Examples
        --------
        >>> nse.cm_eod_eq_symbol_change()
        """
        return self._name_change_csv(
            "https://nsearchives.nseindia.com/content/equities/symbolchange.csv",
            has_header=False,
        )


    # ════════════════════════════════════════════════════════════════════════
    # ── CM — Historical ─────────────────────────────────────────────────────

    def cm_hist_eq_price_band(
        self,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        period:    str | None = None,
        symbol:    str | None = None,
    ) -> pd.DataFrame | None:
        """
        Return historical price-band changes for equities.

        Parameters
        ----------
        *args : str
            Positional shorthand — symbol, date range, or period.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``.
        period : str, optional
            Shorthand: ``"1D"``, ``"1W"``, ``"1M"``, ``"3M"``, ``"6M"``,
            ``"1Y"``.
        symbol : str, optional
            NSE equity symbol.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_hist_eq_price_band()
        >>> nse.cm_hist_eq_price_band("1W")
        >>> nse.cm_hist_eq_price_band("01-10-2025")
        >>> nse.cm_hist_eq_price_band("15-10-2025", "17-10-2025")
        >>> nse.cm_hist_eq_price_band("WEWIN")
        >>> nse.cm_hist_eq_price_band("WEWIN", "1Y")
        >>> nse.cm_hist_eq_price_band("DSSL", "01-10-2025", "17-10-2025")
        """
        from_date, to_date, period, symbol = _unpack_args(
            args, from_date, to_date, period, symbol, short=True
        )

        today              = datetime.now().strftime("%d-%m-%Y")
        from_date, to_date = _resolve_dates(from_date, to_date, period)
        from_date          = from_date or today
        to_date            = to_date   or today

        ref_url = "https://www.nseindia.com/reports/price-band-changes"
        api_url = (
            f"https://www.nseindia.com/api/eqsurvactions"
            f"?from_date={from_date}&to_date={to_date}&symbol={symbol}&csv=true"
            if symbol
            else
            f"https://www.nseindia.com/api/eqsurvactions"
            f"?from_date={from_date}&to_date={to_date}&csv=true"
        )
        return self._get_csv_session(ref_url, api_url)

    def cm_hist_security_wise_data(
        self,
        *args,
        from_date: str | None = None,
        to_date:   str | None = None,
        period:    str | None = None,
        symbol:    str | None = None,
    ) -> pd.DataFrame | None:
        """
        Return historical price, volume, and delivery data for a symbol.

        Parameters
        ----------
        *args : str
            Positional shorthand — symbol, date range, or period.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``.
        period : str, optional
            Shorthand: ``"1D"`` – ``"1Y"``.
        symbol : str, optional
            NSE equity symbol. Required if not passed positionally.

        Returns
        -------
        pd.DataFrame or None
            Columns: Symbol, Series, Date, Prev Close, Open Price,
            High Price, Low Price, Last Price, Close Price, VWAP,
            Total Traded Quantity, Turnover ₹, No. of Trades,
            Deliverable Qty, % Dly Qt to Traded Qty.

        Examples
        --------
        >>> nse.cm_hist_security_wise_data("RELIANCE")
        >>> nse.cm_hist_security_wise_data("RELIANCE", "1Y")
        >>> nse.cm_hist_security_wise_data("RELIANCE", "01-10-2025", "17-10-2025")
        """
        from_date, to_date, period, symbol = _unpack_args(
            args, from_date, to_date, period, symbol, short=True
        )
        from_date, to_date = _resolve_dates(from_date, to_date, period)

        ref_url = "https://www.nseindia.com/report-detail/eq_security"
        api_tpl = (
            f"https://www.nseindia.com/api/historicalOR/"
            f"generateSecurityWiseHistoricalData"
            f"?from={{}}&to={{}}&symbol={symbol}&type=priceVolumeDeliverable&series=ALL"
        )
        records = self._get_chunked(ref_url, api_tpl, from_date, to_date)
        if not records:
            return None

        col_map = {
            "CH_SYMBOL":            "Symbol",
            "CH_SERIES":            "Series",
            "mTIMESTAMP":           "Date",
            "CH_PREVIOUS_CLS_PRICE":"Prev Close",
            "CH_OPENING_PRICE":     "Open Price",
            "CH_TRADE_HIGH_PRICE":  "High Price",
            "CH_TRADE_LOW_PRICE":   "Low Price",
            "CH_LAST_TRADED_PRICE": "Last Price",
            "CH_CLOSING_PRICE":     "Close Price",
            "VWAP":                 "VWAP",
            "CH_TOT_TRADED_QTY":    "Total Traded Quantity",
            "CH_TOT_TRADED_VAL":    "Turnover ₹",
            "CH_TOTAL_TRADES":      "No. of Trades",
            "COP_DELIV_QTY":        "Deliverable Qty",
            "COP_DELIV_PERC":       "% Dly Qt to Traded Qty",
        }
        df = pd.DataFrame(records)
        df = _keep_cols(df, col_map).rename(columns=col_map)
        df.replace({np.inf: 0, -np.inf: 0}, inplace=True)
        df.fillna(0, inplace=True)
        df = _sort_dedup_dates(df, "Date", fmt="%d-%b-%Y", ascending=True)
        return df

    def cm_hist_bulk_deals(self,  *args, **kw) -> pd.DataFrame | None:
        """Return historical bulk-deal records.

        Parameters
        ----------
        *args : str
            Positional shorthand — symbol, date range, or period
            (``"1D"``, ``"1W"``, ``"1M"``, ``"3M"``, ``"6M"``, ``"1Y"``).
        **kw
            Keyword overrides: ``symbol``, ``from_date``, ``to_date``,
            ``period``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_hist_bulk_deals()
        >>> nse.cm_hist_bulk_deals("1W")
        >>> nse.cm_hist_bulk_deals("01-10-2025")
        >>> nse.cm_hist_bulk_deals("RELIANCE")
        >>> nse.cm_hist_bulk_deals("DSSL", "1Y")
        >>> nse.cm_hist_bulk_deals("DSSL", "01-10-2025", "17-10-2025")
        """
        return self._hist_deal_csv("bulk_deals",   *args, **kw)

    def cm_hist_block_deals(self, *args, **kw) -> pd.DataFrame | None:
        """Return historical block-deal records.

        Parameters
        ----------
        *args : str
            Positional shorthand — symbol, date range, or period.
        **kw
            Keyword overrides: ``symbol``, ``from_date``, ``to_date``,
            ``period``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_hist_block_deals()
        >>> nse.cm_hist_block_deals("1W")
        >>> nse.cm_hist_block_deals("RELIANCE")
        >>> nse.cm_hist_block_deals("DSSL", "01-10-2025", "17-10-2025")
        """
        return self._hist_deal_csv("block_deals",  *args, **kw)

    def cm_hist_short_selling(self, *args, **kw) -> pd.DataFrame | None:
        """Return historical short-selling records.

        Parameters
        ----------
        *args : str
            Positional shorthand — symbol, date range, or period.
        **kw
            Keyword overrides: ``symbol``, ``from_date``, ``to_date``,
            ``period``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_hist_short_selling()
        >>> nse.cm_hist_short_selling("1W")
        >>> nse.cm_hist_short_selling("RELIANCE")
        >>> nse.cm_hist_short_selling("DSSL", "01-10-2025", "17-10-2025")
        """
        return self._hist_deal_csv("short_selling", *args, **kw)


    # ════════════════════════════════════════════════════════════════════════
    # ── CM — Periodic Reports ───────────────────────────────────────────────

    def cm_dmy_biz_growth(
        self,
        *args,
        mode:  str          = "daily",
        month: int  | None  = None,
        year:  int  | None  = None,
    ) -> list[dict] | None:
        """
        Return Capital Market business-growth data (daily / monthly / yearly).

        Parameters
        ----------
        *args : str or int
            Positional shorthand — mode string, month name/number, or
            4-digit year.
        mode : str, optional
            ``"daily"``, ``"monthly"`` (default), or ``"yearly"``.
        month : int, optional
            Month number 1–12 (only used for ``"daily"`` mode).
        year : int, optional
            4-digit year. Defaults to the current year.

        Returns
        -------
        list of dict or None

        Examples
        --------
        >>> nse.cm_dmy_biz_growth()
        >>> nse.cm_dmy_biz_growth("monthly")
        >>> nse.cm_dmy_biz_growth("yearly")
        >>> nse.cm_dmy_biz_growth("daily", "OCT", 2025)
        >>> nse.cm_dmy_biz_growth("monthly", 2025)
        """
        mode, month, year = _parse_biz_growth_args(args, month, year, mode)
        return self._biz_growth_fetch("cm", mode, month, year)

    def cm_monthly_settlement_report(
        self,
        *args,
        from_year: int | None = None,
        to_year:   int | None = None,
        period:    str | None = None,
    ) -> pd.DataFrame | None:
        """
        Return Capital Market monthly settlement statistics.

        Parameters
        ----------
        *args : str or int
            Positional shorthand — 4-digit year strings or period codes
            such as ``"1Y"``, ``"2Y"``, ``"3Y"``.
        from_year : int, optional
            Start financial year, e.g. ``2024`` (for FY 2024-25).
        to_year : int, optional
            End financial year.
        period : str, optional
            Shorthand period: ``"1Y"``, ``"2Y"``, ``"3Y"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.cm_monthly_settlement_report()
        >>> nse.cm_monthly_settlement_report("1Y")
        >>> nse.cm_monthly_settlement_report("2024", 2026)
        >>> nse.cm_monthly_settlement_report("3Y")
        """
        from_year, to_year, period = _parse_settlement_args(args, from_year, to_year, period)
        return self._monthly_settlement(
            from_year, to_year, period,
            "https://www.nseindia.com/api/historicalOR/monthly-sett-stats-data?finYear={}-{}",
            {
                "ST_DATE":                   "Month",
                "ST_SETTLEMENT_NO":          "Settlement No",
                "ST_NO_OF_TRADES_LACS":      "No of Trades (lakhs)",
                "ST_TRADED_QTY_LACS":        "Traded Qty (lakhs)",
                "ST_DELIVERED_QTY_LACS":     "Delivered Qty (lakhs)",
                "ST_PERC_DLVRD_TO_TRADED_QTY": "% Delivered to Traded Qty",
                "ST_TURNOVER_CRORES":        "Turnover (₹ Cr)",
                "ST_DELIVERED_VALUE_CRORES": "Delivered Value (₹ Cr)",
                "ST_FUNDS_PAYIN_CRORES":     "Funds Payin (₹ Cr)",
            },
        )

    def cm_monthly_most_active_equity(self) -> pd.DataFrame | None:
        """Return the monthly most-active equities by turnover.

        Returns
        -------
        pd.DataFrame or None
            Columns: Security, No. of Trades, Traded Quantity (Lakh Shares),
            Turnover (₹ Cr.), Avg Daily Turnover (₹ Cr.),
            Share in Total Turnover (%), Month.

        Examples
        --------
        >>> nse.cm_monthly_most_active_equity()
        """
        data = self._get_json(
            "https://www.nseindia.com/historical/most-active-securities",
            "https://www.nseindia.com/api/historicalOR/most-active-securities-monthly",
        )
        if not data or "data" not in data:
            return None
        _cols = [
            "ASM_SECURITY", "ASM_NO_OF_TRADES", "ASM_TRADED_QUANTITY",
            "ASM_TURNOVER", "ASM_AVG_DLY_TURNOVER",
            "ASM_SHARE_IN_TOTAL_TURNOVER", "ASM_DATE",
        ]
        df = _keep_cols(pd.DataFrame(data["data"]), _cols).rename(columns={
            "ASM_SECURITY":               "Security",
            "ASM_NO_OF_TRADES":           "No. of Trades",
            "ASM_TRADED_QUANTITY":        "Traded Quantity (Lakh Shares)",
            "ASM_TURNOVER":               "Turnover (₹ Cr.)",
            "ASM_AVG_DLY_TURNOVER":       "Avg Daily Turnover (₹ Cr.)",
            "ASM_SHARE_IN_TOTAL_TURNOVER":"Share in Total Turnover (%)",
            "ASM_DATE":                   "Month",
        })
        return _clean(df.fillna(0))

    def historical_advances_decline(
        self,
        *args,
        mode:  str       = "Month_wise",
        month: int | None = None,
        year:  int | None = None,
    ) -> pd.DataFrame | None:
        """
        Return historical advances/declines data.

        Parameters
        ----------
        *args : str or int
            Positional shorthand — mode string, year, or month name/number.
        mode : str, optional
            ``"Month_wise"`` (default) or ``"Day_wise"``.
        month : int, optional
            Target month number (used for Day_wise mode).
        year : int, optional
            4-digit year. Defaults to current year.

        Returns
        -------
        pd.DataFrame or None
            Columns: Month (or Day), Advances, Declines, Adv_Decline_Ratio.

        Examples
        --------
        >>> nse.historical_advances_decline()
        >>> nse.historical_advances_decline("2025")
        >>> nse.historical_advances_decline("Day_wise", "OCT", 2025)
        >>> nse.historical_advances_decline("Month_wise", 2024)
        """
        now = datetime.now()
        
        # Bug 2 fix: normalise mode to uppercase before passing to parser
        mode, month, year = _parse_year_month_args(
            args, month, year, mode.upper(), ("MONTH_WISE", "DAY_WISE")
        )
        
        if not year:
            year = now.year
        
        # Bug 1 fix: decrement year BEFORE computing previous month
        if not month:
            if now.month == 1:
                year -= 1
            month = now.month - 1 or 12

        ref = "https://www.nseindia.com/option-chain"
        if mode.lower() == "month_wise":
            api = f"https://www.nseindia.com/api/historicalOR/advances-decline-monthly?year={year}"
        else:
            mon_s = _NUM_MONTH.get(int(month), str(month)).upper()
            api = (
                f"https://www.nseindia.com/api/historicalOR/advances-decline-monthly"
                f"?year={mon_s}-{year}"
            )

        df = self._json_data_df(ref, api)
        if df is None:
            return None

        if mode.lower() == "month_wise":
            cmap = {
                "ADM_MONTH": "Month",
                "ADM_ADVANCES": "Advances",
                "ADM_DECLINES": "Declines",
                "ADM_ADV_DCLN_RATIO": "Adv_Decline_Ratio",
            }
        else:
            cmap = {
                "ADD_DAY_STRING": "Day",
                "ADD_ADVANCES": "Advances",
                "ADD_DECLINES": "Declines",
                "ADD_ADV_DCLN_RATIO": "Adv_Decline_Ratio",
            }

        # rename first
        df = df.rename(columns=cmap)

        # enforce column order safely
        df = df.reindex(columns=cmap.values())

        # convert numeric columns only
        for col in ["Advances", "Declines", "Adv_Decline_Ratio"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return _clean(df)


    # ════════════════════════════════════════════════════════════════════════
    # ── FnO — Live ──────────────────────────────────────────────────────────

    def symbol_full_fno_live_data(self, symbol: str) -> dict | None:
        """Return all live F&O data (options + futures) for a symbol.

        Parameters
        ----------
        symbol : str
            NSE equity or index ticker, e.g. ``"TCS"``.

        Returns
        -------
        dict or None
            Raw JSON with all live derivative quotes.

        Examples
        --------
        >>> nse.symbol_full_fno_live_data("TCS")
        """
        return self._get_json(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getSymbolDerivativesData&symbol={symbol}",
        )

    def symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI(
        self,
        symbol:    str,
        type_mode: str,
    ) -> dict | None:
        """
        Return the most-active calls, puts, or contracts by OI for a symbol.

        Parameters
        ----------
        symbol : str
            NSE equity or index ticker.
        type_mode : str
            ``"C"`` / ``"CALL"`` / ``"CALLS"`` for most-active calls,
            ``"P"`` / ``"PUT"`` / ``"PUTS"`` for puts, or
            ``"O"`` / ``"OI"`` / ``"CONTRACTS"`` for contracts by OI.

        Returns
        -------
        dict or None

        Examples
        --------
        >>> nse.symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI("TCS", "C")
        >>> nse.symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI("TCS", "P")
        """
        tmap = {
            "C": "C", "CALL": "C", "CALLS": "C", "MOST ACTIVE CALLS": "C",
            "P": "P", "PUT":  "P", "PUTS":  "P", "MOST ACTIVE PUTS":  "P",
            "O": "O", "OI":   "O", "CONTRACTS": "O", "MOST ACTIVE CONTRACTS BY OI": "O",
        }
        key = type_mode.strip().upper()
        if key not in tmap:
            raise ValueError("Use C / P / O (or CALL/PUT/CONTRACTS)")
        return self._get_json(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getDerivativesMostActive&symbol={symbol}&callType={tmap[key]}",
        )

    def identifier_based_fno_contracts_live_chart_data(self, identifier: str) -> dict | None:
        """Return live intraday chart data for a contract by its full identifier.

        Parameters
        ----------
        identifier : str
            Full NSE contract identifier string, e.g.
            ``"OPTSTKTCS30-12-2025CE3300.00"``.

        Returns
        -------
        dict or None

        Examples
        --------
        >>> nse.identifier_based_fno_contracts_live_chart_data("OPTSTKTCS30-12-2025CE3300.00")
        """
        return self._get_json(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getIntradayGraphDerivative"
            f"&identifier={identifier}&type=W&token=1",
        )

    # ══════════════════════════════════════════════════════════════════════════════
    # VII. Derivatives (F&O)
    # ══════════════════════════════════════════════════════════════════════════════

    def fno_live_futures_data(self, symbol: str) -> pd.DataFrame | None:
        """
        Return live futures data for all expiries of a symbol.

        Parameters
        ----------
        symbol : str
            NSE equity or index ticker, e.g. ``"RELIANCE"``, ``"NIFTY"``.

        Returns
        -------
        pd.DataFrame or None
            Indexed by ``identifier``; columns include instrumentType,
            expiryDate, OHLC, openInterest, changeinOpenInterest.

        Examples
        --------
        >>> nse.fno_live_futures_data("RELIANCE")
        >>> nse.fno_live_futures_data("NIFTY")
        """
        symbol = symbol.replace(" ", "%20").replace("&", "%26")
        self.rotate_user_agent()

        def _call():
            resp = self._warm_and_fetch(
                f"https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}",
                f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
                f"?functionName=getSymbolDerivativesData&symbol={symbol}&instrumentType=FUT",
                timeout=10
            )
            items = resp.json().get("data", [])
            if not items:
                raise ValueError("Empty response")
            return items

        try:
            items = self._retry(_call)
        except Exception as exc:
            self._log_error("fno_live_futures_data", exc)
            return None

        df = pd.DataFrame(items).set_index("identifier")
        num_cols = [
            "openPrice", "highPrice", "lowPrice", "closePrice", "prevClose",
            "lastPrice", "change", "totalTradedVolume", "totalTurnover",
            "openInterest", "changeinOpenInterest", "pchangeinOpenInterest",
            "underlyingValue", "ticksize", "pchange",
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        order = [
            "instrumentType", "expiryDate", "optionType", "strikePrice",
            "openPrice", "highPrice", "lowPrice", "closePrice", "prevClose",
            "lastPrice", "change", "pchange", "totalTradedVolume", "totalTurnover",
            "openInterest", "changeinOpenInterest", "pchangeinOpenInterest",
            "underlyingValue", "volumeFreezeQuantity",
        ]
        return _keep_cols(df, order)

    def fno_live_top_20_derivatives_contracts(
        self,
        category: str = "Stock Options",
    ) -> pd.DataFrame | None:
        """
        Return the top-20 live derivative contracts for a category.

        Parameters
        ----------
        category : str, optional
            ``"Stock Futures"`` or ``"Stock Options"`` (default).

        Returns
        -------
        pd.DataFrame or None
            Turnover columns are converted to ₹ Crore.

        Examples
        --------
        >>> nse.fno_live_top_20_derivatives_contracts("Stock Futures")
        >>> nse.fno_live_top_20_derivatives_contracts("Stock Options")
        """
        xref = {"Stock Futures": "stock_fut", "Stock Options": "stock_opt"}
        if category not in xref:
            raise ValueError("category must be 'Stock Futures' or 'Stock Options'")

        resp = self._live_ref_fetch(
            "https://www.nseindia.com/market-data/equity-derivatives-watch",
            f"https://www.nseindia.com/api/liveEquity-derivatives?index={xref[category]}",
        )
        try:
            df = pd.DataFrame(resp.json()["data"])
            if df.empty:
                return None

            df.rename(columns={
                "underlying":     "Symbol",
                "identifier":     "Contract ID",
                "instrumentType": "Instr Type",
                "instrument":     "Segment",
                "contract":       "Contract",
                "expiryDate":     "Expiry",
                "optionType":     "Option",
                "strikePrice":    "Strike",
                "lastPrice":      "LTP",
                "change":         "Chg",
                "pChange":        "Chg %",
                "openPrice":      "Open",
                "highPrice":      "High",
                "lowPrice":       "Low",
                "closePrice":     "Close",
                "volume":         "Volume (Cntr)",
                "totalTurnover":  "Turnover (₹)",
                "premiumTurnOver":"Premium Turnover (₹)",
                "underlyingValue":"Underlying LTP",
                "openInterest":   "OI (Cntr)",
                "noOfTrades":     "Trades",
            }, inplace=True)

            for col in ("Turnover (₹)", "Premium Turnover (₹)"):
                if col in df.columns:
                    df[col] = (df[col] / 1e7).round(2)
            df.rename(columns={
                "Turnover (₹)":         "Turnover (₹ Cr)",
                "Premium Turnover (₹)": "Premium Turnover (₹ Cr)",
            }, inplace=True)

            order = [
                "Segment", "Symbol", "Expiry", "Option", "Strike", "Close",
                "LTP", "Chg", "Chg %", "Open", "High", "Low",
                "Volume (Cntr)", "Trades", "OI (Cntr)",
                "Premium Turnover (₹ Cr)", "Turnover (₹ Cr)",
                "Contract", "Contract ID", "Underlying LTP",
            ]
            return _keep_cols(df, order)

        except Exception as exc:
            self._log_error("fno_live_top_20_derivatives_contracts", exc)
            return None

    def fno_live_most_active_futures_contracts(
        self,
        mode: str = "Volume",
    ) -> pd.DataFrame | None:
        """
        Return the most active futures contracts ranked by volume or value.

        Parameters
        ----------
        mode : str, optional
            ``"Volume"`` (default) or ``"Value"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_live_most_active_futures_contracts("Volume")
        >>> nse.fno_live_most_active_futures_contracts("Value")
        """
        key  = "value" if mode.lower() == "value" else "volume"
        resp = self._live_ref_fetch(
            "https://www.nseindia.com/market-data/most-active-contracts",
            "https://www.nseindia.com/api/snapshot-derivatives-equity?index=futures",
        )
        try:
            df = pd.DataFrame(resp.json()[key]["data"])
            return df if not df.empty else None
        except Exception as exc:
            self._log_error("fno_live_most_active_futures_contracts", exc)
            return None

    def fno_live_most_active(
        self,
        mode:    str = "Index",
        opt:     str = "Call",
        sort_by: str = "Volume",
    ) -> pd.DataFrame | None:
        """
        Return the most active options (calls or puts) by volume or value.

        Parameters
        ----------
        mode : str, optional
            ``"Index"`` (default) or ``"Stock"``.
        opt : str, optional
            ``"Call"`` (default) or ``"Put"``.
        sort_by : str, optional
            ``"Volume"`` (default) or ``"Value"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_live_most_active("Index", "Call", "Volume")
        >>> nse.fno_live_most_active("Stock", "Put", "Value")
        """
        mode    = mode.capitalize()
        opt     = opt.capitalize()
        sort_by = sort_by.capitalize()
        sfx     = {"Volume": "vol", "Value": "val"}[sort_by]
        api_idx = f"{opt.lower()}s-{'index' if mode == 'Index' else 'stocks'}-{sfx}"
        key     = "OPTIDX" if mode == "Index" else "OPTSTK"

        resp = self._live_ref_fetch(
            "https://www.nseindia.com/market-data/most-active-contracts",
            f"https://www.nseindia.com/api/snapshot-derivatives-equity?index={api_idx}",
        )
        try:
            df = pd.DataFrame(resp.json()[key]["data"])
            return df if not df.empty else None
        except Exception as exc:
            self._log_error("fno_live_most_active", exc)
            return None

    def fno_live_most_active_contracts_by_oi(self) -> pd.DataFrame | None:
        """Return the most active derivative contracts ranked by open interest.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_live_most_active_contracts_by_oi()
        """
        return self._snapshot_contracts("oi")

    def fno_live_most_active_contracts_by_volume(self) -> pd.DataFrame | None:
        """Return the most active derivative contracts ranked by trading volume.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_live_most_active_contracts_by_volume()
        """
        return self._snapshot_contracts("contracts")

    def fno_live_most_active_options_contracts_by_volume(self) -> pd.DataFrame | None:
        """Return the top-20 most active options contracts by trading volume.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_live_most_active_options_contracts_by_volume()
        """
        return self._snapshot_contracts("options&limit=20")

    def fno_live_most_active_underlying(self) -> pd.DataFrame | None:
        """
        Return the most active F&O underlying symbols by total volume.

        Returns
        -------
        pd.DataFrame or None
            Columns: Symbol, Fut Vol, Opt Vol, Total Vol, Fut Val, Opt Val,
            Total Val, OI.

        Examples
        --------
        >>> nse.fno_live_most_active_underlying()
        """
        resp = self._live_ref_fetch(
            "https://www.nseindia.com/market-data/most-active-underlying",
            "https://www.nseindia.com/api/live-analysis-most-active-underlying",
        )
        try:
            df = pd.DataFrame(resp.json()["data"])
            if df.empty:
                return None
            df.rename(columns={
                "symbol":       "Symbol",
                "futVolume":    "Fut Vol (Cntr)",
                "optVolume":    "Opt Vol (Cntr)",
                "totVolume":    "Total Vol (Cntr)",
                "futTurnover":  "Fut Val (₹ Lakhs)",
                "preTurnover":  "Opt Val (₹ Lakhs)(Premium)",
                "totTurnover":  "Total Val (₹ Lakhs)",
                "latestOI":     "OI (Cntr)",
                "underlying":   "Underlying",
            }, inplace=True)
            df.drop(columns=["optTurnover"], errors="ignore", inplace=True)
            order = [
                "Symbol", "Fut Vol (Cntr)", "Opt Vol (Cntr)", "Total Vol (Cntr)",
                "Fut Val (₹ Lakhs)", "Opt Val (₹ Lakhs)(Premium)", "Total Val (₹ Lakhs)",
                "OI (Cntr)", "Underlying",
            ]
            return _keep_cols(df, order)
        except Exception as exc:
            self._log_error("fno_live_most_active_underlying", exc)
            return None

    def fno_live_change_in_oi(self) -> pd.DataFrame | None:
        """
        Return OI spurts — underlyings with the largest OI change today.

        Returns
        -------
        pd.DataFrame or None
            Columns: Symbol, Latest OI, Prev OI, chng in OI, chng in OI %,
            Vol, Fut Val, Opt Val, Total Val, Price.

        Examples
        --------
        >>> nse.fno_live_change_in_oi()
        """
        resp = self._live_ref_fetch(
            "https://www.nseindia.com/market-data/oi-spurts",
            "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings",
        )
        try:
            df = pd.DataFrame(resp.json()["data"])
            if df.empty:
                return None
            df.rename(columns={
                "symbol":     "Symbol",
                "latestOI":   "Latest OI",
                "prevOI":     "Prev OI",
                "changeInOI": "chng in OI",
                "avgInOI":    "chng in OI %",
                "volume":     "Vol (Cntr)",
                "futValue":   "Fut Val (₹ Lakhs)",
                "premValue":  "Opt Val (₹ Lakhs)(Premium)",
                "total":      "Total Val (₹ Lakhs)",
                "underlyingValue": "Price",
            }, inplace=True)
            df.drop(columns=["optValue"], errors="ignore", inplace=True)
            order = [
                "Symbol", "Latest OI", "Prev OI", "chng in OI", "chng in OI %",
                "Vol (Cntr)", "Fut Val (₹ Lakhs)", "Opt Val (₹ Lakhs)(Premium)",
                "Total Val (₹ Lakhs)", "Price",
            ]
            return _keep_cols(df, order)
        except Exception as exc:
            self._log_error("fno_live_change_in_oi", exc)
            return None

    def fno_live_oi_vs_price(self) -> pd.DataFrame | None:
        """
        Return OI-vs-price signals for all F&O contracts.

        Contracts are classified into: long build-up, short build-up,
        long unwinding, and short covering.

        Returns
        -------
        pd.DataFrame or None
            Columns include OI_Price_Signal, Symbol, Expiry, Strike,
            LTP, % Price Chg, Latest OI, Chg in OI, % OI Chg.

        Examples
        --------
        >>> nse.fno_live_oi_vs_price()
        """
        resp = self._live_ref_fetch(
            "https://www.nseindia.com/market-data/oi-spurts",
            "https://www.nseindia.com/api/live-analysis-oi-spurts-contracts",
        )
        try:
            rows = []
            for block in resp.json().get("data", []):
                for cat, contracts in block.items():
                    for c in contracts:
                        c["OI_Price_Signal"] = cat
                        rows.append(c)
            if not rows:
                return None
            df = pd.DataFrame(rows).rename(columns={
                "symbol":         "Symbol",
                "instrument":     "Instrument",
                "expiryDate":     "Expiry",
                "optionType":     "Type",
                "strikePrice":    "Strike",
                "ltp":            "LTP",
                "prevClose":      "Prev Close",
                "pChange":        "% Price Chg",
                "latestOI":       "Latest OI",
                "prevOI":         "Prev OI",
                "changeInOI":     "Chg in OI",
                "pChangeInOI":    "% OI Chg",
                "volume":         "Volume",
                "turnover":       "Turnover ₹L",
                "premTurnover":   "Premium ₹L",
                "underlyingValue":"Underlying Price",
            })
            order = [
                "OI_Price_Signal", "Symbol", "Instrument", "Expiry", "Type",
                "Strike", "LTP", "% Price Chg", "Latest OI", "Prev OI",
                "Chg in OI", "% OI Chg", "Volume", "Turnover ₹L",
                "Premium ₹L", "Underlying Price",
            ]
            return _keep_cols(df, order)
        except Exception as exc:
            self._log_error("fno_live_oi_vs_price", exc)
            return None

    def fno_expiry_dates_raw(self, symbol: str = "NIFTY") -> dict | None:
        """Return the raw option-chain dropdown JSON for a symbol.

        Parameters
        ----------
        symbol : str, optional
            NSE ticker. Default is ``"NIFTY"``.

        Returns
        -------
        dict or None
            Raw JSON containing ``expiryDates`` and other dropdown metadata.

        Examples
        --------
        >>> nse.fno_expiry_dates_raw()
        >>> nse.fno_expiry_dates_raw("TCS")
        """
        return self._get_json(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getOptionChainDropdown&symbol={symbol}",
        )

    def fno_expiry_dates(
        self,
        symbol:       str = "NIFTY",
        label_filter: str | None = None,
    ) -> pd.DataFrame | str | list | None:
        """
        Return a structured expiry-date table for a symbol.

        Parameters
        ----------
        symbol : str, optional
            NSE ticker. Default is ``"NIFTY"``.
        label_filter : str or None, optional
            Controls the return format:

            - ``None`` (default) — full ``DataFrame``
            - ``"All"`` — list of three main expiry date strings
            - ``"Current"`` / ``"Next Week"`` / ``"Month"`` — single
              date string in ``DD-MM-YYYY``

        Returns
        -------
        pd.DataFrame or str or list or None
            Columns (when returning DataFrame): Expiry Date, Expiry Type,
            Label, Days Remaining, Contract Zone.

        Examples
        --------
        >>> nse.fno_expiry_dates()
        >>> nse.fno_expiry_dates("TCS")
        >>> nse.fno_expiry_dates("NIFTY", "Current")
        >>> nse.fno_expiry_dates("NIFTY", "Next Week")
        >>> nse.fno_expiry_dates("NIFTY", "Month")
        >>> nse.fno_expiry_dates("NIFTY", "All")
        """
        from datetime import time as dtime


        try:
            resp = self._warm_and_fetch(
                "https://www.nseindia.com/option-chain",
                f"https://www.nseindia.com/api/option-chain-contract-info?symbol={symbol}",
                timeout=10
            )
            data = resp.json()
            raw  = data.get("expiryDates") or data.get("records", {}).get("expiryDates")
            if not raw:
                return None
            expiry = pd.Series(
                pd.to_datetime(raw, format="%d-%b-%Y").sort_values().unique()
            )
        except Exception as exc:
            self._log_error("fno_expiry_dates", exc)
            return None

        now    = datetime.now()
        expiry = expiry[expiry >= pd.Timestamp(now.date())].reset_index(drop=True)
        if expiry.empty:
            return None

        if (len(expiry) > 0
                and expiry.iloc[0].date() == now.date()
                and now.time() > dtime(15, 30)):
            expiry = expiry.iloc[1:].reset_index(drop=True)

        def _etype(i, d):
            return (
                "Monthly Expiry"
                if i + 1 >= len(expiry) or expiry.iloc[i + 1].month != d.month
                else "Weekly Expiry"
            )

        etype = [_etype(i, d) for i, d in enumerate(expiry)]
        df    = pd.DataFrame({
            "Expiry Date":  expiry.dt.strftime("%d-%b-%Y"),
            "Expiry Type":  etype,
            "Label":        "",
        })

        if len(df) > 0:
            df.loc[0, "Label"] = "Current"
        wi = [i for i in df[df["Expiry Type"] == "Weekly Expiry"].index if i > 0]
        if wi:
            df.loc[wi[0], "Label"] = "Next Week"
        mi = [i for i in df[df["Expiry Type"] == "Monthly Expiry"].index if i > 0]
        if mi:
            df.loc[mi[0], "Label"] = "Month"

        df["Days Remaining"] = (expiry - pd.Timestamp(now.date())).dt.days

        def _zone(e):
            if e.month == now.month and e.year == now.year:
                return "Current Month"
            nm = (now.month % 12) + 1
            if e.month == nm and e.year in (now.year, now.year + 1):
                return "Next Month"
            if e.month in (3, 6, 9, 12):
                return "Quarterly"
            return "Far Month"

        df["Contract Zone"] = expiry.apply(_zone)
        df = df[["Expiry Date", "Expiry Type", "Label", "Days Remaining", "Contract Zone"]]

        if label_filter is None:
            return df.reset_index(drop=True)
        if label_filter == "All":
            sub = df[df["Label"].isin(["Current", "Next Week", "Month"])]
            return sub["Expiry Date"].apply(
                lambda x: pd.to_datetime(x, format="%d-%b-%Y").strftime("%d-%m-%Y")
            ).tolist()

        sub = df[df["Label"] == label_filter].reset_index(drop=True)
        if sub.empty:
            return None
        return pd.to_datetime(sub.loc[0, "Expiry Date"], format="%d-%b-%Y").strftime("%d-%m-%Y")

    def fno_live_option_chain_raw(
        self,
        symbol:      str,
        expiry_date: str | None = None,
    ) -> dict | None:
        """Return the raw option-chain JSON payload for a symbol.

        Parameters
        ----------
        symbol : str
            NSE equity or index ticker.
        expiry_date : str, optional
            Target expiry in any pandas-parseable format.

        Returns
        -------
        dict or None

        Examples
        --------
        >>> nse.fno_live_option_chain_raw("M&M", expiry_date="27-Jan-2026")
        """
        symbol = symbol.upper().replace(" ", "%20").replace("&", "%26")
        return self._get_json(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getOptionChainData&symbol={symbol}"
            f"&params=expiryDate={expiry_date}",
        )

    def fno_live_option_chain(
        self,
        symbol:      str,
        expiry_date: str | None = None,
        oi_mode:     str        = "full",
    ) -> pd.DataFrame:
        """
        Return a structured option chain table for a symbol.

        Parameters
        ----------
        symbol : str
            Underlying ticker, e.g. ``"NIFTY"``, ``"RELIANCE"``.
        expiry_date : str, optional
            Target expiry in any pandas-parseable format. Defaults to
            the nearest available expiry.
        oi_mode : str, optional
            ``"full"`` (default) includes bid/ask columns.
            ``"compact"`` returns only the core OI/volume/LTP columns.

        Returns
        -------
        pd.DataFrame
            Columns: Fetch_Time, Symbol, Expiry_Date, CALLS_OI, CALLS_Volume,
            CALLS_LTP, Strike_Price, PUTS_OI, PUTS_Volume, PUTS_LTP,
            Underlying_Value (and bid/ask columns in full mode).

        Examples
        --------
        >>> nse.fno_live_option_chain("RELIANCE")
        >>> nse.fno_live_option_chain("NIFTY")
        >>> nse.fno_live_option_chain("RELIANCE", expiry_date="27-Jan-2026")
        >>> nse.fno_live_option_chain("RELIANCE", oi_mode="compact")
        """
        self.rotate_user_agent()
        base_url = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"

        full_cols = [
            "Fetch_Time", "Symbol", "Expiry_Date",
            "CALLS_OI", "CALLS_Chng_in_OI", "CALLS_Volume", "CALLS_IV",
            "CALLS_LTP", "CALLS_Net_Chng",
            "CALLS_Bid_Qty", "CALLS_Bid_Price", "CALLS_Ask_Price", "CALLS_Ask_Qty",
            "Strike_Price",
            "PUTS_Bid_Qty", "PUTS_Bid_Price", "PUTS_Ask_Price", "PUTS_Ask_Qty",
            "PUTS_Net_Chng", "PUTS_LTP", "PUTS_IV",
            "PUTS_Volume", "PUTS_Chng_in_OI", "PUTS_OI",
            "Underlying_Value"
        ]

        compact_cols = [
            "Fetch_Time", "Symbol", "Expiry_Date",
            "CALLS_OI", "CALLS_Chng_in_OI", "CALLS_Volume", "CALLS_IV",
            "CALLS_LTP", "CALLS_Net_Chng",
            "Strike_Price",
            "PUTS_Net_Chng", "PUTS_LTP", "PUTS_IV",
            "PUTS_Volume", "PUTS_Chng_in_OI", "PUTS_OI",
            "Underlying_Value"
        ]

        col_names = compact_cols if oi_mode == "compact" else full_cols

        dtypes = {
            c: "float64"
            for c in col_names
            if any(x in c for x in ("Price", "IV", "Value", "OI", "Volume", "Chng", "Qty"))
        }
        dtypes.update({"Fetch_Time": "object", "Symbol": "object",
                       "Expiry_Date": "object", "Strike_Price": "float64"})

        def _call():
            r = self._warm_and_fetch(
                "https://www.nseindia.com/option-chain",
                base_url,
                params={"functionName": "getOptionChainDropdown", "symbol": symbol},
                timeout=10
            )
            dd    = r.json()
            avail = dd.get("expiryDates", [])
            if not avail:
                raise ValueError("No expiry dates found")

            if expiry_date:
                try:
                    t = pd.to_datetime(expiry_date, dayfirst=True).strftime("%d-%b-%Y")
                except Exception:
                    t = expiry_date.strip()
                target = t if t in avail else avail[0]
            else:
                target = avail[0]

            r2 = self._warm_and_fetch(
                "https://www.nseindia.com/option-chain",
                base_url,
                params={
                    "functionName": "getOptionChainData",
                    "symbol": symbol,
                    "params": f"expiryDate={target}",
                },
                timeout=15
            )
            return r2.json(), target

        try:
            res = self._retry(_call)
            payload, target = res
        except Exception as exc:
            self._log_error("fno_live_option_chain", exc)
            return pd.DataFrame(columns=col_names).astype(dtypes)

        ts      = payload.get("timestamp", datetime.now().strftime("%d-%b-%Y %H:%M:%S"))
        uv      = payload.get("underlyingValue", 0)
        records = payload.get("data", [])

        if not records:
            return pd.DataFrame(columns=col_names).astype(dtypes)

        rows = []
        for item in records:
            ce  = item.get("CE", {})
            pe  = item.get("PE", {})
            row = {
                "Fetch_Time":       ts,
                "Symbol":           symbol,
                "Expiry_Date":      target,
                "Strike_Price":     item.get("strikePrice"),
                "CALLS_OI":         ce.get("openInterest",          0),
                "CALLS_Chng_in_OI": ce.get("changeinOpenInterest",  0),
                "CALLS_Volume":     ce.get("totalTradedVolume",      0),
                "CALLS_IV":         ce.get("impliedVolatility",      0),
                "CALLS_LTP":        ce.get("lastPrice",              0),
                "CALLS_Net_Chng":   ce.get("change",                 0),
                "PUTS_OI":          pe.get("openInterest",           0),
                "PUTS_Chng_in_OI":  pe.get("changeinOpenInterest",   0),
                "PUTS_Volume":      pe.get("totalTradedVolume",       0),
                "PUTS_IV":          pe.get("impliedVolatility",       0),
                "PUTS_LTP":         pe.get("lastPrice",               0),
                "PUTS_Net_Chng":    pe.get("change",                  0),
                "Underlying_Value": uv,
            }
            if oi_mode == "full":
                row.update({
                    "CALLS_Bid_Qty":   ce.get("totalBuyQuantity",  0) or ce.get("buyQuantity1",  0),
                    "CALLS_Bid_Price": ce.get("buyPrice1",          0),
                    "CALLS_Ask_Price": ce.get("sellPrice1",         0),
                    "CALLS_Ask_Qty":   ce.get("totalSellQuantity",  0) or ce.get("sellQuantity1", 0),
                    "PUTS_Bid_Qty":    pe.get("totalBuyQuantity",   0) or pe.get("buyQuantity1",  0),
                    "PUTS_Bid_Price":  pe.get("buyPrice1",           0),
                    "PUTS_Ask_Price":  pe.get("sellPrice1",          0),
                    "PUTS_Ask_Qty":    pe.get("totalSellQuantity",   0) or pe.get("sellQuantity1", 0),
                })
            rows.append(row)

        return pd.DataFrame(rows, columns=col_names).astype(dtypes)

    def fno_live_active_contracts(
        self,
        symbol:      str,
        expiry_date: str | None = None,
    ) -> list | None:
        """
        Return all live active option contracts for a symbol.

        Parameters
        ----------
        symbol : str
            NSE equity or index ticker, e.g. ``"NIFTY"``, ``"RELIANCE"``.
        expiry_date : str, optional
            Target expiry in ``DD-MMM-YYYY`` format (e.g. ``"27-Jan-2026"``).
            Defaults to the nearest available expiry.

        Returns
        -------
        list of dict or None
            Each dict has keys: Instrument Type, Expiry Date, Option Type,
            Strike Price, Open, High, Low, Last, Change, Volume, OI, etc.

        Examples
        --------
        >>> nse.fno_live_active_contracts("NIFTY")
        >>> nse.fno_live_active_contracts("NIFTY", expiry_date="27-Jan-2026")
        >>> nse.fno_live_active_contracts("RELIANCE")
        >>> nse.fno_live_active_contracts("RELIANCE", expiry_date="27-Jan-2026")
        """
        self.rotate_user_agent()

        def _call():
            resp = self._warm_and_fetch(
                f"https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}",
                f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
                f"?functionName=getSymbolDerivativesData&symbol={symbol}&instrumentType=OPT",
                timeout=10
            )
            return resp.json().get("data", [])

        try:
            contracts = self._retry(_call)
        except Exception as exc:
            self._log_error("fno_live_active_contracts", exc)
            return None

        if expiry_date:
            exp       = pd.to_datetime(expiry_date, format="%d-%b-%Y").strftime("%d-%b-%Y")
            contracts = [c for c in contracts if c.get("expiryDate") == exp]

        return [{
            "Instrument Type":   c.get("instrumentType", ""),
            "Expiry Date":       c.get("expiryDate",     ""),
            "Option Type":       c.get("optionType",     ""),
            "Strike Price":      str(c.get("strikePrice", "")).strip(),
            "Open":              c.get("openPrice",  0),
            "High":              c.get("highPrice",  0),
            "Low":               c.get("lowPrice",   0),
            "closePrice":        c.get("closePrice", 0),
            "Prev Close":        c.get("prevClose",  0),
            "Last":              c.get("lastPrice",  0),
            "Change":            c.get("change",     0),
            "%Change":           c.get("pchange",    0),
            "Volume (Contracts)":c.get("totalTradedVolume", 0),
            "Value (₹ Lakhs)":   round(c.get("totalTurnover", 0) / 100_000, 2),
            "totalBuyQuantity":  0,
            "totalSellQuantity": 0,
            "OI":                c.get("openInterest",        0),
            "Chng in OI":        c.get("changeinOpenInterest", 0),
            "% Chng in OI":      c.get("pchangeinOpenInterest",0),
            "VWAP":              0,
        } for c in contracts]


    # ════════════════════════════════════════════════════════════════════════
    # ── FnO — Charts ────────────────────────────────────────────────────────

    def fno_chart(
        self,
        symbol:    str,
        inst_type: str,
        expiry:    str,
        strike:    str = "",
    ) -> pd.DataFrame | None:
        """
        Return intraday chart data for an F&O contract.

        Parameters
        ----------
        symbol : str
            Underlying symbol, e.g. ``"TCS"``.
        inst_type : str
            Instrument type prefix, e.g. ``"FUTSTK"``, ``"OPTIDX"``.
        expiry : str
            Expiry string used to build the contract identifier,
            e.g. ``"30-12-2025"``.
        strike : str, optional
            Strike price string for options; leave blank for futures.

        Returns
        -------
        pd.DataFrame or None
            Columns: datetime_utc, price, chng, pct.

        Examples
        --------
        >>> nse.fno_chart("TCS", "FUTSTK", "30-12-2025")
        >>> nse.fno_chart("NIFTY", "OPTIDX", "20-01-2026", "PE25700")
        """
        strike_part = "XX0" if inst_type.startswith("FUT") else strike
        contract    = f"{inst_type}{symbol.upper()}{expiry}{strike_part}.00"
        data        = self._chart_fetch(
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getIntradayGraphDerivative"
            f"&identifier={contract}&type=W&token=1"
        )
        if not data or "grapthData" not in data:
            return None
        rows = []
        for r in data["grapthData"]:
            dt = pd.to_datetime(r[0], unit="ms", utc=True)
            rows.append({
                "datetime_utc": dt,
                "price":  float(r[1]),
                "chng":   float(r[2]) if len(r) > 2 and not isinstance(r[2], str) else None,
                "pct":    float(r[3]) if len(r) > 3 else None,
            })
        return pd.DataFrame(rows)


    # ════════════════════════════════════════════════════════════════════════
    # ── FnO — EOD ───────────────────────────────────────────────────────────

    # def fno_eod_bhav_copy(self, trade_date: str = "") -> pd.DataFrame | None:
    #     """
    #     Download the F&O bhavcopy for a trade date.

    #     Tries the direct archive URL first; falls back to the NSE reports
    #     API if it returns non-200. Rows with all-zero price/volume columns
    #     are filtered out automatically.

    #     Parameters
    #     ----------
    #     trade_date : str
    #         Date in ``DD-MM-YYYY`` format.

    #     Returns
    #     -------
    #     pd.DataFrame or None

    #     Examples
    #     --------
    #     >>> nse.fno_eod_bhav_copy("17-10-2025")
    #     """
    #     self.rotate_user_agent()
    #     url = (
    #         f"https://nsearchives.nseindia.com/content/fo/"
    #         f"BhavCopy_NSE_FO_0_0_0_{_fmt_trade_date(trade_date, '%Y%m%d')}_F_0000.csv.zip"
    #     )
    #     dt_label = datetime.strptime(trade_date, "%d-%m-%Y").strftime("%d-%b-%Y")
    #     dt_label = dt_label[:3] + dt_label[3:].capitalize()
    #     try:
    #         resp = self._warm_and_fetch(
    #             "https://www.nseindia.com/reports-archives",
    #             url,
    #             timeout=15
    #         )
    #         if resp.status_code == 200:
    #             df = self._zip_csv(resp.content)
    #         else:
    #             url2 = (
    #                 f"https://www.nseindia.com/api/reports?archives="
    #                 f"%5B%7B%22name%22%3A%22F%26O%20-%20Bhavcopy(csv)%22%2C"
    #                 f"%22type%22%3A%22archives%22%2C%22category%22%3A"
    #                 f"%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D"
    #                 f"&date={dt_label}&type=equity&mode=single"
    #             )
    #             resp2 = self._warm_and_fetch(
    #                 "https://www.nseindia.com/reports-archives",
    #                 url2,
    #                 timeout=10
    #             )
    #             df = self._zip_csv(resp2.content)

    #         if not df.empty:
    #             try:
    #                 df = df[~(
    #                     (df.iloc[:, 22] == 0) & (df.iloc[:, 23] == 0) &
    #                     (df.iloc[:, 24] == 0) & (df.iloc[:, 25] == 0)
    #                 )]
    #                 df = df.sort_values(by=df.columns[24], ascending=False)
    #             except IndexError:
    #                 pass
    #         return df

    #     except Exception as exc:
    #         self._log_error("fno_eod_bhav_copy", exc)
    #         return None


    def fno_eod_bhav_copy(self, trade_date: str = "") -> pd.DataFrame | None:
        """
        Download the F&O bhavcopy for a trade date.

        Tries the direct archive URL first; falls back to the NSE reports
        API if it returns non-200. Rows with all-zero price/volume columns
        are filtered out automatically.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_eod_bhav_copy("17-10-2025")
        """
        try:
            self.rotate_user_agent()
            archive_url = (
                "https://nsearchives.nseindia.com/content/fo/"
                f"BhavCopy_NSE_FO_0_0_0_{_fmt_trade_date(trade_date, '%Y%m%d')}_F_0000.csv.zip"
            )

            # ---- 1. DIRECT ARCHIVE CALL (no warmup) ----
            resp = self.session.get(archive_url, headers=self.headers, timeout=15)

            if resp.status_code == 200:
                df = self._zip_csv(resp.content)

            else:
                # ---- 2. FALLBACK WITH COOKIE ----
                warm_url = "https://www.nseindia.com/market-data/live-equity-market"
                self.session.get(warm_url, headers=self.headers, timeout=5)

                dt_label = datetime.strptime(
                    trade_date, "%d-%m-%Y"
                ).strftime("%d-%b-%Y")

                url2 = (
                    "https://www.nseindia.com/api/reports?archives="
                    "%5B%7B%22name%22%3A%22F%26O%20-%20Bhavcopy(csv)%22%2C"
                    "%22type%22%3A%22archives%22%2C"
                    "%22category%22%3A%22derivatives%22%2C"
                    "%22section%22%3A%22equity%22%7D%5D"
                    f"&date={dt_label}&type=equity&mode=single"
                )

                resp2 = self.session.get(url2, headers=self.headers, timeout=15)

                resp2.raise_for_status()
                df = self._zip_csv(resp2.content)

            # ---- 3. FILTER ----
            if not df.empty:
                try:
                    df = df[~(
                        (df.iloc[:, 22] == 0) &
                        (df.iloc[:, 23] == 0) &
                        (df.iloc[:, 24] == 0) &
                        (df.iloc[:, 25] == 0)
                    )]
                    df = df.sort_values(by=df.columns[24], ascending=False)
                except IndexError:
                    pass
            return df

        except Exception as exc:
            self._log_error("fno_eod_bhav_copy", exc)
            return None

    def fno_eod_fii_stats(self, trade_date: str) -> pd.DataFrame | None:
        """Return the FII stats Excel file for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_eod_fii_stats("17-10-2025")
        """
        fmt = _fmt_trade_date(trade_date, "%d-%b-%Y")
        fmt = fmt[:3] + fmt[3:].capitalize()
        raw = self._get_archive(
            f"https://nsearchives.nseindia.com/content/fo/fii_stats_{fmt}.xls"
        )
        return self._read_excel(raw) if raw else None

    def fno_eod_top10_fut(self, trade_date: str) -> list | None:
        """Return the top-10 futures contracts for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        list or None
            List of CSV rows (each row is a list of strings).

        Examples
        --------
        >>> nse.fno_eod_top10_fut("17-10-2025")
        """
        return self._zip_rows(
            f"https://nsearchives.nseindia.com/archives/fo/mkt/"
            f"fo{_fmt_trade_date(trade_date).upper()}.zip",
            "ttfut",
        )

    def fno_eod_top20_opt(self, trade_date: str) -> list | None:
        """Return the top-20 options contracts for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        list or None

        Examples
        --------
        >>> nse.fno_eod_top20_opt("31-12-2025")
        """
        return self._zip_rows(
            f"https://nsearchives.nseindia.com/archives/fo/mkt/"
            f"fo{_fmt_trade_date(trade_date)}.zip",
            "ttopt",
        )

    def fno_eod_sec_ban(self, trade_date: str) -> pd.DataFrame | None:
        """Return the F&O securities in the ban period for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_eod_sec_ban("17-10-2025")
        """
        return self._get_csv_archive(
            f"https://nsearchives.nseindia.com/archives/fo/sec_ban/"
            f"fo_secban_{_fmt_trade_date(trade_date).upper()}.csv"
        )

    def fno_eod_mwpl_3(self, trade_date: str) -> pd.DataFrame | None:
        """
        Return the MWPL (Market Wide Position Limit) client-wise Excel file.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None
            Reshaped Excel data with client-wise columns.

        Examples
        --------
        >>> nse.fno_eod_mwpl_3("17-10-2025")
        """
        raw = self._get_archive(
            f"https://nsearchives.nseindia.com/content/nsccl/"
            f"mwpl_cli_{_fmt_trade_date(trade_date).upper()}.xls"
        )
        if not raw:
            return None

        df = self._read_excel(raw)
        if df is None:
            return None

        df.dropna(how="all", inplace=True)
        df.columns = df.iloc[0]
        df         = df[1:].reset_index(drop=True)

        new_cols = []
        n        = 1
        for col in df.columns:
            if "Unnamed" in str(col) or pd.isna(col):
                new_cols.append(f"Client {n}")
                n += 1
            else:
                new_cols.append(str(col).strip())
        df.columns = new_cols
        return df

    def fno_eod_combine_oi(self, trade_date: str) -> pd.DataFrame | None:
        """Return the combined open-interest file for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_eod_combine_oi("17-10-2025")
        """
        raw = self._get_archive(
            f"https://nsearchives.nseindia.com/archives/nsccl/mwpl/"
            f"combineoi_{_fmt_trade_date(trade_date).upper()}.zip"
        )
        if not raw:
            return None
        with zipfile.ZipFile(BytesIO(raw), "r") as zf:
            for name in zf.namelist():
                if name.lower().startswith("combineoi") and name.endswith(".csv"):
                    return pd.read_csv(zf.open(name))
        return None

    def fno_eod_participant_wise_oi(self, trade_date: str) -> pd.DataFrame | None:
        """Return participant-wise open interest for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_eod_participant_wise_oi("17-10-2025")
        """
        return self._fao_participant_csv(trade_date, "oi")

    def fno_eod_participant_wise_vol(self, trade_date: str) -> pd.DataFrame | None:
        """Return participant-wise trading volume for a trade date.

        Parameters
        ----------
        trade_date : str
            Date in ``DD-MM-YYYY`` format.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_eod_participant_wise_vol("17-10-2025")
        """
        return self._fao_participant_csv(trade_date, "vol")

    def fno_eom_lot_size(self, symbol: str | None = None) -> pd.DataFrame | None:
        """
        Return the current F&O lot-size table.

        Parameters
        ----------
        symbol : str, optional
            If provided, filters to that underlying only.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_eom_lot_size()
        >>> nse.fno_eom_lot_size("TCS")
        """
        try:
            raw = self._get_archive(
                "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv"
            )
            if not raw:
                return None

            rows  = list(csv.reader(raw.decode("utf-8", "ignore").splitlines()))
            if not rows:
                return None

            hdrs      = rows[0]
            nbi       = [i for i, h in enumerate(hdrs) if h.strip()]
            flat_hdr  = [hdrs[i] for i in nbi]
            flat_rows = []
            target_symbol = symbol.strip().upper() if symbol else None
            
            for row in rows[1:]:
                if len(row) <= 1:
                    continue
                
                row_symbol = str(row[1]).strip().upper()
                if target_symbol is None or row_symbol == target_symbol:
                    # Pad row if shorter than headers
                    padding = [""] * (len(hdrs) - len(row))
                    full_row = list(row) + padding
                    flat_rows.append([full_row[i] for i in nbi])
                    
            return pd.DataFrame(flat_rows, columns=flat_hdr) if flat_rows else None

        except Exception as exc:
            self._log_error("fno_eom_lot_size", exc)
            return None


    # ════════════════════════════════════════════════════════════════════════
    # ── FnO — Historical ────────────────────────────────────────────────────

    def future_price_volume_data(self, *args, **kwargs) -> pd.DataFrame:
        """
        Return historical futures price/volume data for a symbol.

        Positional *args* (in order): ``symbol``, ``instrument``, then
        optionally a period code (``"1M"``, ``"3M"``, etc.), an expiry
        string (``"MAR-25"``), and/or date strings.  All may also be
        passed as keyword arguments.

        Parameters
        ----------
        *args : str
            Required: ``symbol`` (e.g. ``"NIFTY"``), then ``instrument``
            (``"Index Futures"`` or ``"Stock Futures"``).
            Optional positional: period code (``"1M"``, ``"3M"``),
            expiry string (``"OCT-25"``), and/or date strings.
        instrument : str
            ``"Index Futures"`` / ``"index"`` or ``"Stock Futures"`` /
            ``"stock"`` (case-insensitive).
        expiry : str, optional
            Month-year string, e.g. ``"OCT-25"`` or ``"NOV-24"``. When
            provided, the API auto-resolves the exact expiry date.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``.
        period : str, optional
            Shorthand: ``"1M"``, ``"3M"``, ``"6M"``.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> nse.future_price_volume_data("NIFTY", "Index", "OCT-25", "01-10-2025", "17-10-2025")
        >>> nse.future_price_volume_data("ITC", "Stock Futures", "OCT-25", "04-10-2025")
        >>> nse.future_price_volume_data("BANKNIFTY", "Index Futures", "3M")
        >>> nse.future_price_volume_data("NIFTY", "Index Futures", "NOV-24")
        """
        REF  = "https://www.nseindia.com/report-detail/fo_eq_security"
        BASE = "https://www.nseindia.com/api/historicalOR/foCPV"
        today = datetime.now()
        dd    = "%d-%m-%Y"

        if len(args) < 2:
            raise ValueError("Provide at least symbol and instrument as positional arguments.")

        symbol     = args[0].strip().upper()
        instrument = args[1].strip().lower()
        expiry = from_date = to_date = period = None

        for arg in args[2:]:
            arg = str(arg).strip().upper()
            if arg in ("1D", "1W", "1M", "3M", "6M"):
                period = arg
            elif any(m in arg for m in _MONTH_NUM):
                expiry = arg
            elif "-" in arg and len(arg.split("-")) == 3 and all(p.isdigit() for p in arg.split("-")):
                if not from_date:
                    from_date = arg
                else:
                    to_date = arg

        expiry    = expiry    or kwargs.get("expiry")
        from_date = from_date or kwargs.get("from_date")
        to_date   = to_date   or kwargs.get("to_date")
        period    = period    or kwargs.get("period")

        if instrument in ("futidx", "index futures", "index future", "index"):
            instrument = "FUTIDX"
        elif instrument in ("futstk", "stock futures", "stock future", "stock"):
            instrument = "FUTSTK"
        else:
            raise ValueError("instrument must be 'Index Futures' or 'Stock Futures'")

        expiry_date = None
        if expiry:
            try:
                ep  = expiry.split("-")
                em  = ep[0].upper()
                ey  = 2000 + int(ep[1])
                meta_url = (
                    f"https://www.nseindia.com/api/historicalOR/meta/foCPV/expireDts"
                    f"?instrument={instrument}&symbol={symbol}&year={ey}"
                )
                meta_resp = self._warm_and_fetch(REF, meta_url, timeout=15)
                meta      = meta_resp.json().get("expiresDts", [])
                matched = [x for x in meta if em in x.upper()]
                if not matched:
                    return pd.DataFrame()
                expiry_date = matched[0]
                exp_dt      = datetime.strptime(expiry_date, "%d-%b-%Y")
                if exp_dt < today and not from_date:
                    from_date = exp_dt - timedelta(days=90)
                    to_date   = exp_dt
            except Exception as exc:
                self._log_error("future_price_volume_data expiry", exc)
                return pd.DataFrame()

        if not expiry and period:
            d         = {"1D": 1, "1W": 7, "1M": 30, "3M": 90, "6M": 180}[period.upper()]
            from_date = today - timedelta(days=d)
            to_date   = today

        from_date = (
            datetime.strptime(from_date, dd) if isinstance(from_date, str)
            else from_date or today - timedelta(days=180)
        )
        to_date = (
            datetime.strptime(to_date, dd) if isinstance(to_date, str)
            else to_date or (datetime.strptime(expiry_date, "%d-%b-%Y") if expiry_date else today)
        )

        params = {
            "from": from_date.strftime(dd), "to": to_date.strftime(dd),
            "instrumentType": instrument, "symbol": symbol, "year": today.year,
        }
        if expiry_date:
            params["expiryDate"] = expiry_date
        
        def _call():
            resp = self._warm_and_fetch(REF, BASE, params=params, timeout=15)
            return resp.json().get("data", [])
        
        try:
            data = self._retry(_call)
        except Exception as exc:
            self._log_error("future_price_volume_data", exc)
            return pd.DataFrame()

        df = pd.DataFrame(data)
        if df.empty:
            return df

        df.columns = [c.upper().replace(" ", "_") for c in df.columns]
        if "FH_TIMESTAMP" in df.columns:
            df["FH_TIMESTAMP"] = pd.to_datetime(df["FH_TIMESTAMP"], errors="coerce")
        if expiry_date and "FH_EXPIRY_DT" in df.columns:
            df = df[df["FH_EXPIRY_DT"].str.upper() == expiry_date.upper()]
        for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
            df[col] = df[col].dt.strftime("%d-%b-%Y")

        return df.sort_values(
            [c for c in ("FH_TIMESTAMP", "FH_EXPIRY_DT") if c in df.columns]
        ).reset_index(drop=True)

    def option_price_volume_data(self, *args, **kwargs) -> pd.DataFrame:
        """
        Return historical options price/volume data for a symbol.

        Parameters
        ----------
        *args : str
            Required: ``symbol``, ``instrument``.
            Optional positional: option type (``"CE"`` / ``"PE"``),
            strike price string, period code, expiry string, dates.
        instrument : str
            ``"Index Options"`` / ``"index"`` or ``"Stock Options"`` /
            ``"stock"`` (case-insensitive).
        expiry : str, optional
            Target expiry date in ``DD-MM-YYYY`` (passed as ``kwargs``).
        option_type : str, optional
            ``"CE"`` or ``"PE"``; if omitted both sides are returned.
        strike_price : str or int, optional
            Strike price filter, e.g. ``"47000"``.
        from_date : str, optional
            Start date in ``DD-MM-YYYY``.
        to_date : str, optional
            End date in ``DD-MM-YYYY``.
        period : str, optional
            Shorthand: ``"1M"``, ``"3M"``.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> nse.option_price_volume_data("NIFTY", "Index", "01-10-2025", "17-10-2025", expiry="20-10-2025")
        >>> nse.option_price_volume_data("ITC", "Stock Options", "CE", "01-10-2025", "17-10-2025", expiry="28-10-2025")
        >>> nse.option_price_volume_data("BANKNIFTY", "Index Options", "47000", "01-10-2025", "17-10-2025", expiry="28-10-2025")
        >>> nse.option_price_volume_data("BANKNIFTY", "Index Options", "3M")
        >>> nse.option_price_volume_data("NIFTY", "Index Options", "PE", "01-10-2025", expiry="28-10-2025")
        """
        dd    = "%d-%m-%Y"
        today = datetime.now()

        if len(args) < 2:
            raise ValueError("Provide at least symbol and instrument as positional arguments.")

        symbol     = args[0].strip().upper()
        instrument = args[1].strip().lower()
        expiry = from_date = to_date = period = option_type = strike_price = None

        for arg in args[2:]:
            arg = str(arg).strip().upper()
            if arg in ("1D", "1W", "1M", "3M", "6M"):
                period = arg
            elif any(m in arg for m in _MONTH_NUM):
                expiry = arg
            elif arg in ("CE", "PE"):
                option_type = arg
            elif "-" in arg and len(arg.split("-")) == 3 and all(p.isdigit() for p in arg.split("-")):
                if not from_date:
                    from_date = arg
                else:
                    to_date = arg

        expiry       = expiry       or kwargs.get("expiry")
        from_date    = from_date    or kwargs.get("from_date")
        to_date      = to_date      or kwargs.get("to_date")
        period       = period       or kwargs.get("period")
        option_type  = option_type  or kwargs.get("option_type")
        strike_price = strike_price or kwargs.get("strike_price")

        if instrument in ("optidx", "index options", "index option", "index"):
            instrument = "OPTIDX"
        elif instrument in ("optstk", "stock options", "stock option", "stock"):
            instrument = "OPTSTK"
        else:
            raise ValueError("instrument must be 'Index Options' or 'Stock Options'")

        expiry_date = None
        if expiry:
            try:
                expiry = str(expiry).upper()
                if (
                    "-" in expiry
                    and len(expiry.split("-")) == 3
                    and all(p.isdigit() for p in expiry.split("-"))
                ):
                    expiry_date = datetime.strptime(expiry, "%d-%m-%Y").strftime("%d-%b-%Y")
                elif any(m in expiry for m in _MONTH_NUM):
                    # Bug 5 fix: self.get_expiries() does not exist; replaced with
                    # the same meta-API pattern used by future_price_volume_data.
                    mon, yr = expiry.split("-")
                    ey      = 2000 + int(yr)
                    meta_url = (
                        f"https://www.nseindia.com/api/historicalOR/meta/foCPV/expireDts"
                        f"?instrument={instrument}&symbol={symbol}&year={ey}"
                    )
                    meta_resp = self._warm_and_fetch(
                        "https://www.nseindia.com/report-detail/fo_eq_security",
                        meta_url, timeout=15
                    )
                    el      = meta_resp.json().get("expiresDts", [])
                    matched = [x for x in el if mon in x.upper()]
                    if not matched:
                        return pd.DataFrame()
                    expiry_date = max(
                        matched, key=lambda d: datetime.strptime(d, "%d-%b-%Y")
                    )
            except Exception as exc:
                self._log_error("option_price_volume_data expiry", exc)
                return pd.DataFrame()

        if not expiry and period:
            d         = {"1D": 1, "1W": 7, "1M": 30, "3M": 90, "6M": 180}.get(period.upper(), 30)
            from_date = today - timedelta(days=d)
            to_date   = today

        from_date = (
            datetime.strptime(from_date, dd) if isinstance(from_date, str)
            else from_date or today - timedelta(days=180)
        )
        to_date = (
            datetime.strptime(to_date, dd) if isinstance(to_date, str)
            else to_date or (datetime.strptime(expiry_date, "%d-%b-%Y") if expiry_date else today)
        )

        params = {
            "from": from_date.strftime(dd), "to": to_date.strftime(dd),
            "instrumentType": instrument, "symbol": symbol,
            "year": today.year, "csv": "true",
        }
        if expiry_date:  params["expiryDate"]  = expiry_date
        if option_type:  params["optionType"]  = option_type
        if strike_price: params["strikePrice"] = strike_price

        def _call():
            return self._warm_and_fetch(
                "https://www.nseindia.com/option-chain",
                "https://www.nseindia.com/api/historicalOR/foCPV",
                params=params,
                timeout=15,
                api_timeout=20,
            ).json().get("data", [])

        try:
            data = self._retry(_call)
        except Exception as exc:
            self._log_error("option_price_volume_data", exc)
            return pd.DataFrame()

        df = pd.DataFrame(data)
        if df.empty:
            return df

        df.columns = [c.upper().replace(" ", "_") for c in df.columns]
        for col in [
            "FH_OPENING_PRICE", "FH_TRADE_HIGH_PRICE", "FH_TRADE_LOW_PRICE",
            "FH_CLOSING_PRICE", "FH_LAST_TRADED_PRICE", "FH_PREV_CLS",
            "FH_SETTLE_PRICE", "FH_TOT_TRADED_QTY", "FH_TOT_TRADED_VAL",
            "FH_OPEN_INT", "FH_CHANGE_IN_OI", "CALCULATED_PREMIUM_VAL",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "FH_TOT_TRADED_QTY" in df.columns:
            df = df[df["FH_TOT_TRADED_QTY"] > 0]

        def _sfmt(x, fmt):
            if pd.isna(x):
                return ""
            if isinstance(x, (pd.Timestamp, datetime)):
                return x.strftime(fmt)
            try:
                return pd.to_datetime(x, errors="coerce").strftime(fmt)
            except Exception:
                return str(x)

        for col, fmt in (
            ("FH_TIMESTAMP",       "%d-%b-%Y"),
            ("FH_EXPIRY_DT",       "%d-%b-%Y"),
            ("FH_TIMESTAMP_ORDER", "%d-%b-%Y %H:%M:%S"),
        ):
            if col in df.columns:
                df[col] = df[col].apply(lambda x, f=fmt: _sfmt(x, f))

        return df.sort_values(
            [c for c in ("FH_TIMESTAMP", "FH_EXPIRY_DT") if c in df.columns]
        ).reset_index(drop=True)


    # ════════════════════════════════════════════════════════════════════════
    # ── FnO — Periodic Reports ──────────────────────────────────────────────

    def fno_dmy_biz_growth(
        self,
        *args,
        mode:  str         = "monthly",
        month: int | None  = None,
        year:  int | None  = None,
    ) -> list[dict] | None:
        """
        Return F&O business-growth data (daily / monthly / yearly).

        Parameters
        ----------
        *args : str or int
            Positional shorthand — mode string, month name/number, or
            4-digit year.
        mode : str, optional
            ``"daily"``, ``"monthly"`` (default), or ``"yearly"``.
        month : int, optional
            Month number 1–12 (only used for ``"daily"`` mode).
        year : int, optional
            4-digit year. Defaults to the current year.

        Returns
        -------
        list of dict or None

        Examples
        --------
        >>> nse.fno_dmy_biz_growth()
        >>> nse.fno_dmy_biz_growth("yearly")
        >>> nse.fno_dmy_biz_growth("daily", month="OCT", year=2025)
        """
        mode, month, year = _parse_biz_growth_args(args, month, year, mode)
        return self._biz_growth_fetch("fo", mode, month, year)

    def fno_monthly_settlement_report(
        self,
        *args,
        from_year: int | None = None,
        to_year:   int | None = None,
        period:    str | None = None,
    ) -> pd.DataFrame | None:
        """
        Return F&O monthly settlement statistics.

        Parameters
        ----------
        *args : str or int
            Positional shorthand — 4-digit year strings or period codes.
        from_year : int, optional
            Start financial year.
        to_year : int, optional
            End financial year.
        period : str, optional
            Shorthand: ``"1Y"``, ``"2Y"``, ``"3Y"``.

        Returns
        -------
        pd.DataFrame or None

        Examples
        --------
        >>> nse.fno_monthly_settlement_report()
        >>> nse.fno_monthly_settlement_report("2024", "2025")
        >>> nse.fno_monthly_settlement_report("2Y")
        """
        from_year, to_year, period = _parse_settlement_args(args, from_year, to_year, period)
        return self._monthly_settlement(
            from_year, to_year, period,
            "https://www.nseindia.com/api/financial-monthlyStats?from_date=Apr-{}&to_date=Mar-{}",
            {
                "st_date":     "Month",
                "st_Mtm":      "Fut MTM Settlement",
                "st_Final":    "Fut Final Settlement",
                "st_Premium":  "Opt Premium Settlement",
                "st_Excercise":"Opt Exercise Settlement",
                "st_Total":    "Total",
            },
        )


    # ════════════════════════════════════════════════════════════════════════
    # ── SEBI ────────────────────────────────────────────────────────────────

    def sebi_circulars(
        self,
        *args,
        period: str = "1W",
    ) -> pd.DataFrame:
        """
        Fetch SEBI circulars within a date range or period.

        Parameters
        ----------
        *args : str
            Positional date shorthand:
            - Two dates → ``from_date``, ``to_date`` (``DD-MM-YYYY``)
            - One date  → ``from_date`` to today
            - Period string → e.g. ``"1M"`` (overrides *period* kwarg)
        period : str, optional
            Lookback period: ``"1W"`` (default), ``"2W"``, ``"3W"``,
            ``"1M"``, ``"2M"``, ``"3M"``, ``"6M"``, ``"1Y"``, ``"2Y"``.
            Ignored when date args are provided.

        Returns
        -------
        pd.DataFrame
            Columns: Date, Title, Link.

        Examples
        --------
        >>> nse.sebi_circulars()
        >>> nse.sebi_circulars("01-10-2025", "10-10-2025")
        >>> nse.sebi_circulars("01-10-2025")
        >>> nse.sebi_circulars("1M")
        """
        today = datetime.today()
        mult  = {"D": 1, "W": 7, "M": 30, "Y": 365}

        # Resolve dates from positional args or period
        if len(args) == 2:
            fd = datetime.strptime(args[0], "%d-%m-%Y")
            td = datetime.strptime(args[1], "%d-%m-%Y")
        elif len(args) == 1:
            arg = args[0].upper()
            if len(arg) >= 2 and arg[-1] in mult and arg[:-1].isdigit():
                # treat as period string
                fd = today - timedelta(days=int(arg[:-1]) * mult[arg[-1]])
                td = today
            else:
                fd = datetime.strptime(args[0], "%d-%m-%Y")
                td = today
        elif period:
            p  = period.upper()
            fd = today - timedelta(days=int(p[:-1]) * mult.get(p[-1], 7))
            td = today
        else:
            fd, td = today - timedelta(days=7), today

        payload = {
            "fromDate":   fd.strftime("%d-%m-%Y"),
            "toDate":     td.strftime("%d-%m-%Y"),
            "fromYear":   "", "toYear": "", "deptId": "-1",
            "sid": "1",   "ssid": "7", "smid": "0", "ssidhidden": "7",
            "intmid": "-1", "sText": "", "ssText": "Circulars", "smText": "",
            "doDirect": "-1", "nextValue": "1", "nextDel": "1", "totalpage": "1",
        }
        rows = self._sebi_post(payload)
        return self._finalise_sebi_df(pd.DataFrame(rows))

    def sebi_data(self, pages: int = 1) -> pd.DataFrame:
        """
        Paginate through the SEBI circular listing and return all records.

        Parameters
        ----------
        pages : int, optional
            Number of pages to fetch. Default is ``1``.

        Returns
        -------
        pd.DataFrame
            Columns: Date, Title, Link; sorted descending by date.

        Examples
        --------
        >>> nse.sebi_data()
        >>> nse.sebi_data(pages=3)
        """
        all_rows: list[dict] = []
        for page in range(1, pages + 1):
            payload = {
                "nextValue": str(page), "nextDel": str(page),
                "totalpage": str(pages), "nextPage": "",
                "doDirect":  "1",
            }
            rows = self._sebi_post(payload)
            if not rows:
                break
            all_rows.extend(rows)

        return self._finalise_sebi_df(pd.DataFrame(all_rows))


    # ════════════════════════════════════════════════════════════════════════
    # ── Miscellaneous ───────────────────────────────────────────────────────

    def quarterly_financial_results(self, symbol: str) -> dict | None:
        """Return the last 3 quarterly financial results for a symbol.

        Parameters
        ----------
        symbol : str
            NSE equity symbol, e.g. ``"TCS"``.

        Returns
        -------
        dict or None
            Consolidated/Standalone data with Income, PBT, Net Profit,
            and EPS per quarter.

        Examples
        --------
        >>> nse.quarterly_financial_results("TCS")
        """
        return self._get_json(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getIntegratedFilingData&symbol={symbol}",
        )

    def recent_annual_reports(self) -> pd.DataFrame:
        """
        Parse the NSE annual-reports RSS feed into a structured table.

        Returns
        -------
        pd.DataFrame
            Columns: symbol, companyName, fyFrom, fyTo, link,
            submissionDate, SME.

        Examples
        --------
        >>> nse.recent_annual_reports()
        """
        try:
            raw  = self._get_archive(
                "https://nsearchives.nseindia.com/content/RSS/Annual_Reports.xml"
            )
            if not raw:
                return pd.DataFrame()
            feed    = feedparser.parse(raw.decode("utf-8", "ignore"))
            records = []

            for item in feed.entries:
                fn  = item.get("link", "").split("/")[-1]
                sme = "SME" if fn.startswith("SME_AR_") else ""
                m   = re.search(
                    r"(?:SME_)?AR_\d+_(?P<symbol>[A-Z0-9]+)_(?P<fyFrom>\d{4})_(?P<fyTo>\d{4})_",
                    fn,
                )
                if not m:
                    continue
                dm  = re.search(r"(\d{2}-[A-Z]{3}-\d{2})", item.get("description", ""))
                sub = (
                    datetime.strptime(dm.group(1), "%d-%b-%y").strftime("%d-%b-%Y")
                    if dm else None
                )
                records.append({
                    "symbol":         m.group("symbol"),
                    "companyName":    item.get("title", ""),
                    "fyFrom":         int(m.group("fyFrom")),
                    "fyTo":           int(m.group("fyTo")),
                    "link":           item.get("link", ""),
                    "submissionDate": sub,
                    "SME":            sme,
                })
            return pd.DataFrame(records)

        except Exception as exc:
            self._log_error("recent_annual_reports", exc)
            return pd.DataFrame()

    def html_tables(
        self,
        url:         str,
        show_tables: bool = False,
        output:      str  = "json",
    ) -> list | None:
        """
        Fetch an NSE page and extract all HTML tables.

        Parameters
        ----------
        url : str
            Full URL of the NSE page to scrape.
        show_tables : bool, optional
            If ``True``, print a preview of each table found.
            Default is ``False``.
        output : str, optional
            ``"json"`` (default) returns a list of record dicts.
            ``"df"`` returns a list of raw ``DataFrame`` objects.

        Returns
        -------
        list or None
            List of dicts (json mode) or list of DataFrames (df mode).
        """
        try:
            resp = self._live_ref_fetch(
                "https://www.nseindia.com/option-chain",
                url,
                timeout=20,
                api_timeout=30,
            )
            if resp is None:
                return None
            tables = pd.read_html(StringIO(resp.text))
            print(f"Total tables found: {len(tables)}")

            if show_tables:
                for i, t in enumerate(tables):
                    print(f"\nTable {i}")
                    print(t.head())

            if output.lower() == "json":
                return [t.to_dict(orient="records") for t in tables]
            return tables

        except Exception as exc:
            self._log_error("html_tables", exc)
            return None

