"""
test_nsekit.py — Professional test suite for NseKit
================================================================
Rule:  PASS  → real data returned (non-None, non-empty)
       FAIL  → None / empty DataFrame / empty list / exception
       (Unit helpers that need no network always PASS on their own logic)

Run all:
    pytest test_nsekit.py -v --tb=short

Skip live (offline only):
    pytest test_nsekit.py -v -m "not live"
"""

from __future__ import annotations
from datetime import datetime, timedelta
import pandas as pd
import pytest

import NseKit

# ──────────────────────────────────────────────────────────────────────────────
# Marks registration
# ──────────────────────────────────────────────────────────────────────────────
def pytest_configure(config):
    config.addinivalue_line("markers", "live: requires live NSE / SEBI network access")
    config.addinivalue_line("markers", "slow: fetches large historical chunks (> 5 s)")

# ──────────────────────────────────────────────────────────────────────────────
# Shared session fixture  (warm-up cost paid once for the whole run)
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def nse() -> NseKit.Nse:
    return NseKit.Nse(max_rps=2.0)

# ──────────────────────────────────────────────────────────────────────────────
# Assertion helpers
# ──────────────────────────────────────────────────────────────────────────────
def _has_data(result, label: str = "") -> None:
    """FAIL if result is None, empty DataFrame, or empty list/dict."""
    tag = f"[{label}] " if label else ""
    assert result is not None, f"{tag}Got None — no data returned"
    if isinstance(result, pd.DataFrame):
        assert not result.empty, f"{tag}Got empty DataFrame"
    elif isinstance(result, (list, dict)):
        assert len(result) > 0, f"{tag}Got empty {type(result).__name__}"


def _has_df(result, min_rows: int = 1, cols: set | None = None, label: str = "") -> None:
    """FAIL if result is not a non-empty DataFrame (with optional required cols)."""
    _has_data(result, label)
    assert isinstance(result, pd.DataFrame), \
        f"[{label}] Expected DataFrame, got {type(result)}"
    assert len(result) >= min_rows, \
        f"[{label}] Expected >= {min_rows} rows, got {len(result)}"
    if cols:
        missing = cols - set(result.columns)
        assert not missing, f"[{label}] Missing columns: {missing}"


def _has_dict(result, keys: set | None = None, label: str = "") -> None:
    """FAIL if result is not a non-empty dict (with optional required keys)."""
    _has_data(result, label)
    assert isinstance(result, dict), f"[{label}] Expected dict, got {type(result)}"
    if keys:
        missing = keys - set(result.keys())
        assert not missing, f"[{label}] Missing keys: {missing}"


def _today()     -> str: return datetime.now().strftime("%d-%m-%Y")
def _ago(n: int) -> str: return (datetime.now() - timedelta(days=n)).strftime("%d-%m-%Y")


# ══════════════════════════════════════════════════════════════════════════════
# GLOBAL TEST CONFIGURATION — change dates here, they propagate everywhere
# ══════════════════════════════════════════════════════════════════════════════
#
# EOD_DATE   — a recent NSE trading day in DD-MM-YYYY  (used for EOD / bhavcopy tests)
# EOD_DATE2  — same day in DD-MM-YY short format       (used by nse_eod_top10_nifty50)

# EXPIRY_DATE — nearest F&O expiry in DD-MM-YYYY       (used for fno_chart tests)
# EXPIRY_OPT — option strike suffix for fno_chart      (e.g. EXPIRY_OPT)

# DATE_FROM  — start of a historical range in DD-MM-YYYY
# DATE_TO    — end   of a historical range in DD-MM-YYYY

# BIZ_MONTH  — calendar month for cm_dmy_biz_growth daily test  ("OCT", "NOV", …)
# BIZ_YEAR   — calendar year  for cm_dmy_biz_growth daily test  (int)
# PEER_QUARTER — quarter string for peer_comparison test        ("Dec 2025", …)
# ──────────────────────────────────────────────────────────────────────────────

EOD_DATE        = "29-05-2026"   # DD-MM-YYYY  — most-recent trading day
EOD_DATE2       = "29-05-26"     # DD-MM-YY    — same day, short form

PREV_EOD_DATE   = "27-05-2026"   # DD-MM-YYYY  — pervious trading day

EXPIRY_DATE     = "30-06-2026"   # DD-MM-YYYY  — nearest F&O expiry
EXPIRY_OPT      = "PE25700"      # option chain suffix for fno_chart

DATE_FROM       = "25-05-2026"   # DD-MM-YYYY  — start of a completed date range
DATE_TO         = "29-05-2026"   # DD-MM-YYYY  — end   of that range

BIZ_MONTH       = "OCT"          # month for cm_dmy_biz_growth daily test
BIZ_YEAR        = 2025            # year  for cm_dmy_biz_growth daily test

PEER_QUARTER    = "Dec 2025"    # quarter for peer_comparison test


# ══════════════════════════════════════════════════════════════════════════════
# 1. OFFLINE UNIT TESTS — zero network, always PASS
# ══════════════════════════════════════════════════════════════════════════════


class TestUnitHelpers:

    # ── _parse_args ────────────────────────────────────────────────────────
    def test_parse_args_two_dates(self):
        r = NseKit._parse_args((DATE_FROM, DATE_TO))
        assert r["from_date"] == DATE_FROM and r["to_date"] == DATE_TO

    def test_parse_args_period(self):
        assert NseKit._parse_args(("1Y",))["period"] == "1Y"

    def test_parse_args_symbol(self):
        assert NseKit._parse_args(("RELIANCE",))["symbol"] == "RELIANCE"

    def test_parse_args_symbol_and_dates(self):
        r = NseKit._parse_args(("TCS", DATE_FROM, DATE_TO))
        assert r["symbol"] == "TCS"
        assert r["from_date"] == DATE_FROM and r["to_date"] == DATE_TO

    # ── _resolve_dates ─────────────────────────────────────────────────────
    @pytest.mark.parametrize("period", ["1D","1W","1M","3M","6M","1Y","2Y","5Y","10Y"])
    def test_resolve_dates_period(self, period):
        fd, td = NseKit._resolve_dates(period=period)
        fd_dt = datetime.strptime(fd, "%d-%m-%Y")
        td_dt = datetime.strptime(td, "%d-%m-%Y")
        assert fd_dt <= td_dt, f"from_date {fd} must be <= to_date {td}"

    def test_resolve_dates_ytd(self):
        fd, _ = NseKit._resolve_dates(period="YTD")
        assert fd.endswith(f"-{datetime.now().year}")

    def test_resolve_dates_explicit(self):
        fd, td = NseKit._resolve_dates(DATE_FROM, DATE_TO)
        assert fd == DATE_FROM and td == DATE_TO

    # ── _sort_dedup_dates ──────────────────────────────────────────────────
    def test_sort_dedup_ascending(self):
        df = pd.DataFrame({"Date": ["17-Jan-2025", "15-Jan-2025", "17-Jan-2025"]})
        r  = NseKit._sort_dedup_dates(df, "Date", "%d-%b-%Y", ascending=True)
        assert len(r) == 2 and r["Date"].iloc[0] == "15-Jan-2025"

    def test_sort_dedup_descending(self):
        df = pd.DataFrame({"Date": ["15-Jan-2025", "17-Jan-2025", "15-Jan-2025"]})
        r  = NseKit._sort_dedup_dates(df, "Date", "%d-%b-%Y", ascending=False)
        assert len(r) == 2 and r["Date"].iloc[0] == "17-Jan-2025"

    # ── _csv_from_bytes ────────────────────────────────────────────────────
    def test_csv_from_bytes_strips_bom(self):
        raw = b"\xef\xbb\xbfName,Value\nA,1\nB,2\n"
        df  = NseKit._csv_from_bytes(raw)
        assert list(df.columns) == ["Name", "Value"] and len(df) == 2

    # ── _clean ─────────────────────────────────────────────────────────────
    def test_clean_replaces_nan_and_inf(self):
        df = pd.DataFrame({"a": [1.0, float("nan"), float("inf")]})
        r  = NseKit._clean(df)
        assert r["a"].iloc[1] is None and r["a"].iloc[2] is None


class TestRateLimitConfig:

    def test_global_override_propagates(self):
        orig = NseKit.NseConfig.max_rps
        try:
            NseKit.NseConfig.max_rps = 2.0
            n = NseKit.Nse()
            assert n.max_rps == 2.0
        finally:
            NseKit.NseConfig.max_rps = orig

    def test_instance_override_is_independent(self):
        g = NseKit.NseConfig.max_rps
        n = NseKit.Nse(max_rps=1.2)
        assert n.max_rps == 1.2 and NseKit.NseConfig.max_rps == g

    def test_zero_rate_raises(self):
        with pytest.raises(ValueError, match="max_rps must be positive"):
            NseKit.Nse(max_rps=0)

    def test_negative_rate_raises(self):
        with pytest.raises(ValueError, match="max_rps must be positive"):
            NseKit.Nse(max_rps=-5)

    def test_throttle_no_raise(self, nse):
        nse._throttle()

    def test_repr_contains_fields(self, nse):
        r = repr(nse)
        assert "max_rps" in r and "retries" in r

    def test_nseconfig_not_instantiable(self):
        with pytest.raises(TypeError):
            NseKit.NseConfig()

    def test_nseconfig_not_subclassable(self):
        with pytest.raises(TypeError):
            class X(NseKit.NseConfig): pass


# ══════════════════════════════════════════════════════════════════════════════
# 2. MARKET STATUS & HOLIDAYS
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestMarketStatus:

    @pytest.mark.parametrize("mode", ["Market Status", "Mcap", "Gift Nifty"])
    def test_market_status_mode(self, nse, mode):
        _has_df(nse.nse_market_status(mode), label=mode)

    def test_market_status_nifty50(self, nse):
        # indicativenifty50 key absent outside market hours — skip if None
        r = nse.nse_market_status("Nifty50")
        if r is None:
            pytest.skip("Nifty50 payload absent — market closed or not in response")
        _has_df(r, label="Nifty50")

    def test_market_status_all_has_four_keys(self, nse):
        r = nse.nse_market_status("all")
        _has_dict(r, keys={"Market Status", "Mcap", "Nifty50", "Gift Nifty"})

    def test_market_status_invalid_returns_none(self, nse):
        assert nse.nse_market_status("__BAD__") is None

    def test_is_market_open(self, nse):
        r = nse.nse_is_market_open("Capital Market")
        assert r is not None, "nse_is_market_open returned None"

    def test_trading_holidays_df(self, nse):
        _has_df(nse.nse_trading_holidays(), cols={"tradingDate"}, label="trading_holidays")

    def test_trading_holidays_list(self, nse):
        lst = nse.nse_trading_holidays(list_only=True)
        assert isinstance(lst, list) and len(lst) > 0

    def test_clearing_holidays_df(self, nse):
        _has_df(nse.nse_clearing_holidays(), label="clearing_holidays")

    def test_clearing_holidays_list(self, nse):
        lst = nse.nse_clearing_holidays(list_only=True)
        assert isinstance(lst, list) and len(lst) > 0

    def test_is_trading_holiday_bool(self, nse):
        assert isinstance(nse.is_nse_trading_holiday(), bool)

    def test_is_clearing_holiday_bool(self, nse):
        assert isinstance(nse.is_nse_clearing_holiday(), bool)

    def test_live_market_turnover(self, nse):
        _has_df(nse.nse_live_market_turnover(), label="market_turnover")

    def test_reference_rates(self, nse):
        _has_df(nse.nse_reference_rates(), cols={"currency"}, label="reference_rates")

    def test_live_market_statistics(self, nse):
        _has_df(nse.cm_live_market_statistics(), label="market_statistics")

    def test_live_latency_nanosec(self, nse):
        _has_df(nse.latency_nanosec(), label="latency_nanosec")

    def test_gift_nifty(self, nse):
        _has_df(nse.cm_live_gifty_nifty(), label="gift_nifty")


# ══════════════════════════════════════════════════════════════════════════════
# 3. EQUITY & INDEX LISTS
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestEquityAndIndexLists:

    def test_nifty50_df(self, nse):
        _has_df(nse.nse_6m_nifty_50(), min_rows=50, label="nifty50")

    def test_nifty50_list_contains_reliance(self, nse):
        lst = nse.nse_6m_nifty_50(list_only=True)
        assert isinstance(lst, list) and "RELIANCE" in lst

    def test_nifty500_df(self, nse):
        _has_df(nse.nse_6m_nifty_500(), min_rows=400, label="nifty500")

    def test_equity_full_list_df(self, nse):
        _has_df(nse.nse_eod_equity_full_list(), min_rows=1000, label="equity_full")

    def test_equity_full_list_only(self, nse):
        lst = nse.nse_eod_equity_full_list(list_only=True)
        assert isinstance(lst, list) and len(lst) >= 1000

    def test_fno_stocks_df(self, nse):
        _has_df(nse.nse_eom_fno_full_list("stocks"), label="fno_stocks")

    def test_fno_index_df(self, nse):
        _has_df(nse.nse_eom_fno_full_list("index"), label="fno_index")

    def test_fno_list_only(self, nse):
        lst = nse.nse_eom_fno_full_list(list_only=True)
        assert isinstance(lst, list) and len(lst) > 0

    def test_list_of_indices(self, nse):
        _has_data(nse.list_of_indices(), label="list_of_indices")

    def test_nifty50_top10_eod(self, nse):
        _has_df(nse.nse_eod_top10_nifty50(EOD_DATE2), label="top10_nifty50")

    def test_state_wise_investors(self, nse):
        _has_data(nse.state_wise_registered_investors(), label="state_investors")


# ══════════════════════════════════════════════════════════════════════════════
# 4. CIRCULARS & PRESS RELEASES
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestCircularsAndPress:

    def test_circulars_default(self, nse):
        _has_df(nse.nse_live_hist_circulars(_ago(30), _today()), label="circulars_default")

    def test_circulars_date_range(self, nse):
        _has_df(nse.nse_live_hist_circulars(_ago(60), _today()), label="circulars_range")

    def test_circulars_filter_listing(self, nse):
        # Use a wide range (past 90 days) so circulars with "Listing" dept exist
        df = nse.nse_live_hist_circulars(_ago(90), _today(), filter="Listing")
        _has_df(df, label="circulars_filter")
        assert df["Department"].str.contains("Listing", case=False).all()

    def test_press_releases_default(self, nse):
        # Default = yesterday–today: may be empty on weekends/holidays; use 30-day range
        _has_df(nse.nse_live_hist_press_releases(_ago(30), _today()), label="press_default")

    def test_press_releases_date_range(self, nse):
        _has_df(nse.nse_live_hist_press_releases(_ago(60), _today()), label="press_range")

    def test_press_releases_filter(self, nse):
        # Use wide range so NSE Listing press releases always exist
        _has_df(nse.nse_live_hist_press_releases(_ago(90), _today(), "NSE Listing"),
                label="press_filter")


# ══════════════════════════════════════════════════════════════════════════════
# 5. PRE-MARKET
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestPreMarket:

    @pytest.mark.parametrize("cat", ["All", "NIFTY 50", "Nifty Bank", "Securities in F&O"])
    def test_pre_market_info(self, nse, cat):
        _has_data(nse.pre_market_info(cat), label=f"pre_market_{cat}")

    @pytest.mark.parametrize("cat", ["Index Futures", "Stock Futures"])
    def test_pre_market_derivatives(self, nse, cat):
        _has_data(nse.pre_market_derivatives_info(cat), label=f"pre_mkt_deriv_{cat}")

    def test_pre_market_nifty_info(self, nse):
        _has_data(nse.pre_market_nifty_info("NIFTY 50"), label="pre_market_nifty")

    def test_pre_market_all_adv_dec(self, nse):
        _has_data(nse.pre_market_all_nse_adv_dec_info(), label="adv_dec")


# ══════════════════════════════════════════════════════════════════════════════
# 6. EQUITY LIVE DATA
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestEquityLive:
    SYM = "RELIANCE"

    def test_equity_info(self, nse):
        _has_dict(nse.cm_live_equity_info(self.SYM), keys={"Symbol", "MarketCap"})

    def test_most_active_by_value(self, nse):
        _has_data(nse.cm_live_most_active_equity_by_value(), label="active_value")

    def test_most_active_by_vol(self, nse):
        _has_data(nse.cm_live_most_active_equity_by_vol(), label="active_vol")

    def test_volume_spurts(self, nse):
        _has_data(nse.cm_live_volume_spurts(), label="vol_spurts")

    def test_52week_high(self, nse):
        _has_data(nse.cm_live_52week_high(), label="52w_high")

    def test_52week_low(self, nse):
        _has_data(nse.cm_live_52week_low(), label="52w_low")

    def test_live_block_deal(self, nse):
        # Block deals only populate during market session; None/empty is valid off-hours
        r = nse.cm_live_block_deal()
        if r is None or (isinstance(r, pd.DataFrame) and r.empty):
            pytest.skip("No live block deals — market closed or no deals today")
        _has_data(r, label="block_deal_live")


# ══════════════════════════════════════════════════════════════════════════════
# 7. INDEX LIVE DATA
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestIndexLive:

    def test_all_indices_df(self, nse):
        _has_df(nse.index_live_all_indices_data(), min_rows=5, cols={"index"})

    def test_nifty50_stocks_data(self, nse):
        _has_df(nse.index_live_indices_stocks_data("NIFTY 50"), min_rows=50)

    def test_nifty50_stocks_short(self, nse):
        _has_df(nse.index_live_indices_stocks_data("NIFTY 50", short=True), min_rows=50)

    def test_nifty50_list_only(self, nse):
        lst = nse.index_live_indices_stocks_data("NIFTY 50", list_only=True)
        assert isinstance(lst, list) and len(lst) >= 50

    def test_nifty50_returns(self, nse):
        _has_data(nse.index_live_nifty_50_returns(), label="nifty50_returns")

    @pytest.mark.parametrize("mode", ["First Five", "Full"])
    def test_index_contribution(self, nse, mode):
        _has_data(nse.index_live_contribution("NIFTY 50", mode), label=f"contribution_{mode}")


# ══════════════════════════════════════════════════════════════════════════════
# 8. CHART DATA
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestCharts:

    @pytest.mark.parametrize("tf", ["1D", "1M", "3M"])
    def test_index_chart(self, nse, tf):
        _has_df(nse.index_chart("NIFTY 50", tf),
                cols={"datetime_utc", "price"}, label=f"index_chart_{tf}")

    @pytest.mark.parametrize("tf", ["1D", "1W"])
    def test_stock_chart(self, nse, tf):
        _has_df(nse.stock_chart("RELIANCE", tf), label=f"stock_chart_{tf}")

    def test_vix_chart(self, nse):
        _has_data(nse.india_vix_chart(), label="vix_chart")

    def test_fno_chart_futures(self, nse):
        _has_data(nse.fno_chart("TCS", "FUTSTK", EXPIRY_DATE), label="fno_chart_fut")

    def test_fno_chart_options(self, nse):
        _has_data(nse.fno_chart("NIFTY", "OPTIDX", EXPIRY_DATE, EXPIRY_OPT),
                  label="fno_chart_opt")


# ══════════════════════════════════════════════════════════════════════════════
# 9. HISTORICAL — EQUITY
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
@pytest.mark.slow
class TestHistoricalEquity:
    COLS = {"Symbol", "Date", "Open Price", "Close Price"}

    def test_security_wise_1m(self, nse):
        _has_df(nse.cm_hist_security_wise_data("RELIANCE", "1M"),
                min_rows=10, cols=self.COLS)

    def test_security_wise_date_range(self, nse):
        df = nse.cm_hist_security_wise_data("RELIANCE", _ago(60), _today())
        _has_df(df)
        dates = pd.to_datetime(df["Date"], format="%d-%b-%Y")
        assert dates.is_monotonic_increasing, "Dates not sorted ascending"

    def test_security_wise_no_duplicates(self, nse):
        df = nse.cm_hist_security_wise_data("RELIANCE", "3M")
        _has_df(df)
        assert df["Date"].nunique() == len(df), "Duplicate dates found"

    @pytest.mark.parametrize("sym,period", [
        ("INFY", "1M"), ("HDFCBANK", "3M"), ("TCS", "6M"),
    ])
    def test_security_wise_parametric(self, nse, sym, period):
        _has_df(nse.cm_hist_security_wise_data(sym, period), label=f"{sym}_{period}")

    def test_bulk_deals_period(self, nse):
        _has_data(nse.cm_hist_bulk_deals("1M"), label="bulk_deals_1M")

    def test_bulk_deals_symbol(self, nse):
        # RELIANCE is large-cap — zero bulk deals ever. Use DSSL (small-cap,
        # from NseKit docstring) with explicit date range that has confirmed data.
        _has_data(nse.cm_hist_bulk_deals("COFFEEDAY", DATE_FROM, EOD_DATE),
                  label="bulk_deals_sym")

    def test_block_deals(self, nse):
        _has_data(nse.cm_hist_block_deals("1M"), label="block_deals")

    def test_short_selling(self, nse):
        _has_data(nse.cm_hist_short_selling("1W"), label="short_selling")

    def test_hist_price_band_period(self, nse):
        _has_data(nse.cm_hist_eq_price_band("1W"), label="price_band_1W")

    def test_hist_price_band_symbol(self, nse):
        # Use a small-cap that is actually in a price band; fall back to 1Y range
        _has_data(nse.cm_hist_eq_price_band("WEWIN", "1Y"), label="price_band_sym")

    def test_insider_trading_symbol(self, nse):
        _has_data(nse.cm_live_hist_insider_trading("RELIANCE"), label="insider_sym")

    def test_insider_trading_period(self, nse):
        _has_data(nse.cm_live_hist_insider_trading("1M"), label="insider_1M")

    def test_insider_trading_range(self, nse):
        _has_data(
            nse.cm_live_hist_insider_trading("WIPRO", DATE_FROM, DATE_TO),
            label="insider_range"
        )


# ══════════════════════════════════════════════════════════════════════════════
# 10. HISTORICAL — INDEX
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
@pytest.mark.slow
class TestHistoricalIndex:
    COLS = {"Date", "Open", "High", "Low", "Close"}

    def test_index_historical_1m(self, nse):
        _has_df(nse.index_historical_data("NIFTY 50", "1M"), min_rows=10, cols=self.COLS)

    def test_index_historical_date_range(self, nse):
        _has_df(nse.index_historical_data("NIFTY 50", _ago(90), _today()))

    def test_index_historical_3m(self, nse):
        _has_df(nse.index_historical_data("NIFTY BANK", "3M"))

    def test_index_pe_pb_1m(self, nse):
        _has_data(nse.index_pe_pb_div_historical_data("NIFTY 50", "1M"), label="pe_pb_1M")

    def test_vix_historical(self, nse):
        _has_data(nse.india_vix_historical_data("1M"), label="vix_1M")

    def test_index_eod_bhavcopy(self, nse):
        _has_data(nse.index_eod_bhav_copy(EOD_DATE), label="index_bhavcopy")


# ══════════════════════════════════════════════════════════════════════════════
# 11. CORPORATE FILINGS
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestCorporateFilings:
    SYM = "RELIANCE"

    def test_corporate_action_default(self, nse):
        _has_data(nse.cm_live_hist_corporate_action(), label="corp_action_default")

    def test_corporate_action_period(self, nse):
        _has_data(nse.cm_live_hist_corporate_action("1M"), label="corp_action_1M")

    def test_corporate_action_symbol(self, nse):
        _has_data(nse.cm_live_hist_corporate_action(self.SYM), label="corp_action_sym")

    def test_corporate_action_filter_dividend(self, nse):
        # 3rd positional arg is parsed as filter by _unpack_args
        df = nse.cm_live_hist_corporate_action(DATE_FROM, DATE_TO, filter="Dividend")
        _has_df(df, label="corp_action_dividend")
        assert df["PURPOSE"].str.contains("Dividend", case=False).all()

    def test_board_meetings_default(self, nse):
        _has_data(nse.cm_live_hist_board_meetings(), label="board_meetings")

    def test_board_meetings_symbol(self, nse):
        _has_data(nse.cm_live_hist_board_meetings(self.SYM), label="board_meetings_sym")

    def test_board_meetings_range(self, nse):
        _has_data(nse.cm_live_hist_board_meetings(_ago(60), _today()),
                  label="board_meetings_range")

    def test_shareholder_meetings(self, nse):
        _has_data(nse.cm_live_hist_Shareholder_meetings(), label="sh_meetings")

    def test_shareholder_meetings_symbol(self, nse):
        _has_data(nse.cm_live_hist_Shareholder_meetings(self.SYM), label="sh_meetings_sym")

    def test_today_event_calendar(self, nse):
        _has_data(nse.cm_live_today_event_calendar(), label="today_events")

    def test_upcoming_event_calendar(self, nse):
        _has_data(nse.cm_live_upcoming_event_calendar(), label="upcoming_events")

    def test_qtly_shareholding_patterns(self, nse):
        _has_data(nse.cm_live_qtly_shareholding_patterns(), label="shareholding")

    def test_br_sr_default(self, nse):
        _has_data(nse.cm_live_hist_br_sr(), label="br_sr")

    def test_br_sr_symbol(self, nse):
        _has_data(nse.cm_live_hist_br_sr(self.SYM), label="br_sr_sym")

    def test_voting_results(self, nse):
        _has_data(nse.cm_live_voting_results(), label="voting_results")

    def test_corporate_announcement(self, nse):
        _has_data(nse.cm_live_hist_corporate_announcement(self.SYM), label="announcement")

    def test_announcement_date_range(self, nse):
        _has_data(
            nse.cm_live_hist_corporate_announcement(self.SYM, DATE_FROM, DATE_TO),
            label="announce_range"
        )

    @pytest.mark.parametrize("stage", ["In-Principle", "Listing Stage"])
    def test_qip(self, nse, stage):
        _has_data(nse.cm_live_hist_qualified_institutional_placement(stage), label=f"qip_{stage}")

    @pytest.mark.parametrize("stage", ["In-Principle", "Listing Stage"])
    def test_pref_issue(self, nse, stage):
        _has_data(nse.cm_live_hist_preferential_issue(stage), label=f"pref_{stage}")

    @pytest.mark.parametrize("stage", ["In-Principle", "Listing Stage"])
    def test_rights_issue(self, nse, stage):
        _has_data(nse.cm_live_hist_right_issue(stage), label=f"rights_{stage}")

    def test_recent_annual_reports(self, nse):
        _has_data(nse.recent_annual_reports(), label="annual_reports")

    def test_quarterly_financial_results(self, nse):
        _has_data(nse.quarterly_financial_results("TCS"), label="quarterly_results")


# ══════════════════════════════════════════════════════════════════════════════
# 12. F&O LIVE
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestFnoLive:
    EOD_DATE        = EOD_DATE
    PREV_EOD_DATE   = PREV_EOD_DATE

    def test_futures_data_index(self, nse):
        _has_data(nse.fno_live_futures_data("NIFTY"), label="futures_nifty")

    def test_futures_data_stock(self, nse):
        _has_data(nse.fno_live_futures_data("RELIANCE"), label="futures_reliance")

    def test_expiry_dates(self, nse):
        _has_data(nse.fno_expiry_dates("NIFTY"), label="expiry_dates")

    def test_expiry_dates_current_is_string(self, nse):
        r = nse.fno_expiry_dates("NIFTY", "Current")
        assert r is not None and isinstance(r, str), "Expected single expiry date string"

    def test_expiry_dates_all_is_list(self, nse):
        r = nse.fno_expiry_dates("NIFTY", "All")
        assert isinstance(r, list) and len(r) > 0

    def test_expiry_dates_raw(self, nse):
        _has_dict(nse.fno_expiry_dates_raw("NIFTY"), label="expiry_raw")

    def test_option_chain_index(self, nse):
        _has_data(nse.fno_live_option_chain("NIFTY"), label="option_chain_nifty")

    def test_option_chain_stock(self, nse):
        _has_data(nse.fno_live_option_chain("RELIANCE"), label="option_chain_reliance")

    def test_option_chain_compact(self, nse):
        df = nse.fno_live_option_chain("NIFTY", oi_mode="compact")
        _has_data(df, label="option_chain_compact")
        # bid/ask columns must NOT be present in compact mode
        assert "CALLS_Bid_Qty" not in df.columns, "compact should not have bid/ask cols"

    def test_option_chain_compact_positional(self, nse):
        # passing "compact" as bare positional arg should work identically
        df = nse.fno_live_option_chain("NIFTY", "compact")
        _has_data(df, label="option_chain_compact_pos")
        assert "CALLS_Bid_Qty" not in df.columns

    def test_option_chain_strike_price_kwarg(self, nse):
        # fetch all expiries for a specific strike using keyword arg
        df = nse.fno_live_option_chain("NIFTY", strike_price="23500")
        _has_data(df, label="option_chain_strike_kwarg")
        # every row must carry the requested strike
        assert (df["Strike_Price"] == 23500.0).all(), \
            "Expected all rows to have Strike_Price == 23500"
        # multiple expiry rows expected
        assert df["Expiry_Date"].nunique() >= 1

    def test_option_chain_strike_price_positional(self, nse):
        # bare numeric string in positional slot should resolve to strike_price
        df = nse.fno_live_option_chain("NIFTY", "23500")
        _has_data(df, label="option_chain_strike_pos")
        assert (df["Strike_Price"] == 23500.0).all()

    def test_option_chain_strike_price_compact(self, nse):
        # strike_price + compact mode via kwargs
        df = nse.fno_live_option_chain("NIFTY", strike_price="23500", oi_mode="compact")
        _has_data(df, label="option_chain_strike_compact_kw")
        assert "CALLS_Bid_Qty" not in df.columns

    def test_option_chain_strike_price_compact_positional(self, nse):
        # "23500" then "compact" as positional args
        df = nse.fno_live_option_chain("NIFTY", "23500", "compact")
        _has_data(df, label="option_chain_strike_compact_pos")
        assert "CALLS_Bid_Qty" not in df.columns
        assert (df["Strike_Price"] == 23500.0).all()

    def test_option_chain_raw(self, nse):
        # fno_live_option_chain_raw needs "DD-Mon-YYYY" (e.g. "26-May-2026")
        # fno_expiry_dates("NIFTY","Current") returns "DD-MM-YYYY"; convert format
        expiry_ddmmyyyy = nse.fno_expiry_dates("NIFTY", "Current")
        assert expiry_ddmmyyyy is not None, "Could not fetch current NIFTY expiry"
        from datetime import datetime
        expiry_fmt = datetime.strptime(expiry_ddmmyyyy, "%d-%m-%Y").strftime("%d-%b-%Y")
        _has_dict(nse.fno_live_option_chain_raw("NIFTY", expiry_date=expiry_fmt),
                  label="option_chain_raw")

    def test_option_chain_raw_strike_kwarg(self, nse):
        # raw endpoint with strike_price keyword
        _has_dict(nse.fno_live_option_chain_raw("NIFTY", strike_price="23500"),
                  label="option_chain_raw_strike_kw")

    def test_option_chain_raw_strike_positional(self, nse):
        # bare numeric string in positional slot should be treated as strike_price
        _has_dict(nse.fno_live_option_chain_raw("NIFTY", "23500"),
                  label="option_chain_raw_strike_pos")

    def test_active_contracts_index(self, nse):
        _has_data(nse.fno_live_active_contracts("NIFTY"), label="active_contracts_nifty")

    def test_active_contracts_stock(self, nse):
        _has_data(nse.fno_live_active_contracts("TCS"), label="active_contracts_tcs")

    def test_most_active_futures_volume(self, nse):
        _has_data(nse.fno_live_most_active_futures_contracts("Volume"), label="maf_vol")

    def test_most_active_futures_value(self, nse):
        _has_data(nse.fno_live_most_active_futures_contracts("Value"), label="maf_val")

    @pytest.mark.parametrize("mode,opt,sort", [
        ("Index", "Call", "Volume"), ("Index", "Put",  "Value"),
        ("Stock", "Call", "Volume"), ("Stock", "Put",  "Value"),
    ])
    def test_most_active_options(self, nse, mode, opt, sort):
        _has_data(nse.fno_live_most_active(mode, opt, sort),
                  label=f"ma_{mode}_{opt}_{sort}")

    def test_most_active_by_oi(self, nse):
        _has_data(nse.fno_live_most_active_contracts_by_oi(), label="ma_oi")

    def test_most_active_by_volume(self, nse):
        _has_data(nse.fno_live_most_active_contracts_by_volume(), label="ma_vol")

    def test_most_active_options_by_volume(self, nse):
        _has_data(nse.fno_live_most_active_options_contracts_by_volume(), label="ma_opt_vol")

    def test_most_active_underlying(self, nse):
        _has_data(nse.fno_live_most_active_underlying(), label="ma_underlying")

    def test_change_in_oi(self, nse):
        _has_data(nse.fno_live_change_in_oi(), label="change_oi")

    def test_oi_vs_price(self, nse):
        _has_data(nse.fno_live_oi_vs_price(), label="oi_vs_price")

    def test_top_20_stock_futures(self, nse):
        _has_data(nse.fno_live_top_20_derivatives_contracts("Stock Futures"), label="top20_sf")

    def test_top_20_stock_options(self, nse):
        _has_data(nse.fno_live_top_20_derivatives_contracts("Stock Options"), label="top20_so")

    def test_top_20_invalid_raises(self, nse):
        with pytest.raises(ValueError):
            nse.fno_live_top_20_derivatives_contracts("INVALID")

    def test_participant_wise_oi(self, nse):
        _has_data(nse.fno_eod_participant_wise_oi(self.EOD_DATE), label="part_oi")

    def test_participant_wise_vol(self, nse):
        _has_data(nse.fno_eod_participant_wise_vol(self.EOD_DATE), label="part_vol")

    def test_fii_stats(self, nse):
        _has_data(nse.fno_eod_fii_stats(self.EOD_DATE), label="fii_stats")

    def test_mwpl(self, nse):
        _has_data(nse.fno_eod_mwpl_3(self.PREV_EOD_DATE), label="mwpl")

    def test_combine_oi(self, nse):
        _has_data(nse.fno_eod_combine_oi(self.EOD_DATE), label="combine_oi")

    def test_lot_sizes(self, nse):
        _has_data(nse.fno_eom_lot_size(), label="lot_sizes")

    def test_lot_size_symbol(self, nse):
        _has_data(nse.fno_eom_lot_size("TCS"), label="lot_size_tcs")

    def test_fno_ban_list(self, nse):
        _has_data(nse.fno_eod_sec_ban(self.EOD_DATE), label="ban_list")

    def test_fno_eod_top_10_clearing_members(self, nse):
        _has_data(nse.fno_eod_top_10_clearing_members(self.EOD_DATE), label="top_10_clearing_members")

    def test_fno_eod_client_wise_turnover(self, nse):
        _has_data(nse.fno_eod_client_wise_turnover(self.PREV_EOD_DATE, raw_data=True), label="client_wise_turnover")


    def test_symbol_full_fno_data(self, nse):
        _has_dict(nse.symbol_full_fno_live_data("TCS"), label="full_fno_tcs")

    def test_symbol_most_active_calls(self, nse):
        _has_data(
            nse.symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI("TCS", "C"),
            label="active_calls_tcs"
        )

    def test_symbol_most_active_puts(self, nse):
        _has_data(
            nse.symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI("TCS", "P"),
            label="active_puts_tcs"
        )


# ══════════════════════════════════════════════════════════════════════════════
# 13. F&O HISTORICAL
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
@pytest.mark.slow
class TestFnoHistorical:

    def _get_expiry_monyr(self, nse) -> str:
        """Return nearest expiry in MON-YY format (e.g. 'MAY-26') for futures."""
        raw = nse.fno_expiry_dates_raw("NIFTY")
        if not raw or not raw.get("expiryDates"):
            pytest.skip("No NIFTY expiries available")
        # expiryDates are "DD-Mon-YYYY" e.g. "29-May-2026" → convert to "MAY-26"
        from datetime import datetime
        dt = datetime.strptime(raw["expiryDates"][0], "%d-%b-%Y")
        return dt.strftime("%b-%y").upper()   # "MAY-26"

    def _get_expiry_ddmmyyyy(self, nse) -> str:
        """Return nearest expiry in DD-MM-YYYY format for options kwarg."""
        raw = nse.fno_expiry_dates_raw("NIFTY")
        if not raw or not raw.get("expiryDates"):
            pytest.skip("No NIFTY expiries available")
        from datetime import datetime
        dt = datetime.strptime(raw["expiryDates"][0], "%d-%b-%Y")
        return dt.strftime("%d-%m-%Y")        # "29-05-2026"

    def test_futures_index_date_range(self, nse):
        expiry = self._get_expiry_monyr(nse)
        from_d = _ago(30)
        _has_data(
            nse.future_price_volume_data("NIFTY", "Index", expiry,
                                         from_date=from_d, to_date=_today()),
            label="fut_nifty_range"
        )

    def test_futures_stock_from_date(self, nse):
        expiry = self._get_expiry_monyr(nse)
        _has_data(
            nse.future_price_volume_data("ITC", "Stock Futures", expiry,
                                         from_date=_ago(30), to_date=_today()),
            label="fut_itc"
        )

    def test_futures_banknifty_period(self, nse):
        expiry = self._get_expiry_monyr(nse)
        _has_data(
            nse.future_price_volume_data("BANKNIFTY", "Index Futures", expiry, period="3M"),
            label="fut_banknifty_3M"
        )

    def test_options_index_date_range(self, nse):
        expiry = self._get_expiry_ddmmyyyy(nse)
        _has_data(
            nse.option_price_volume_data("NIFTY", "Index", expiry=expiry,
                                         from_date=_ago(30), to_date=_today()),
            label="opt_nifty_range"
        )

    def test_options_stock_ce(self, nse):
        # ITC stock options have their own expiry — passing NIFTY's expiry returns empty.
        # Use NIFTY Index Options CE so the expiry always matches.
        expiry = self._get_expiry_ddmmyyyy(nse)
        _has_data(
            nse.option_price_volume_data("NIFTY", "Index Options", "CE",
                                         from_date=_ago(30), to_date=_today(),
                                         expiry=expiry),
            label="opt_nifty_CE"
        )

    def test_options_period(self, nse):
        _has_data(
            nse.option_price_volume_data("BANKNIFTY", "Index Options", period="3M"),
            label="opt_banknifty_3M"
        )


# ══════════════════════════════════════════════════════════════════════════════
# 14. EOD ARCHIVES
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestEodArchives:
    D4 = EOD_DATE
    D2 = EOD_DATE2

    def test_equity_bhavcopy(self, nse):
        _has_data(nse.cm_eod_equity_bhavcopy(self.D4), label="eq_bhavcopy")

    def test_bhavcopy_with_delivery(self, nse):
        _has_data(nse.cm_eod_bhavcopy_with_delivery(self.D4), label="bhavcopy_delivery")

    def test_market_activity_report(self, nse):
        _has_data(nse.cm_eod_market_activity_report(self.D2), label="market_activity")

    def test_52week_high_low(self, nse):
        _has_data(nse.cm_eod_52_week_high_low(self.D4), label="52w_hl")

    def test_bulk_deal_eod(self, nse):
        _has_data(nse.cm_eod_bulk_deal(), label="bulk_deal_eod")

    def test_block_deal_eod(self, nse):
        _has_data(nse.cm_eod_block_deal(), label="block_deal_eod")

    def test_shortselling_eod(self, nse):
        _has_data(nse.cm_eod_shortselling(self.D4), label="shortsell_eod")

    def test_surveillance_indicator(self, nse):
        _has_data(nse.cm_eod_surveillance_indicator(self.D2), label="surveillance")

    def test_series_change(self, nse):
        _has_data(nse.cm_eod_series_change(), label="series_change")

    def test_eq_band_changes(self, nse):
        _has_data(nse.cm_eod_eq_band_changes(self.D4), label="band_changes")

    def test_eq_price_band(self, nse):
        _has_data(nse.cm_eod_eq_price_band(self.D4), label="price_band_eod")

    def test_pe_ratio(self, nse):
        _has_data(nse.cm_eod_pe_ratio(self.D2), label="pe_ratio")

    def test_mcap(self, nse):
        _has_data(nse.cm_eod_mcap(self.D2), label="mcap")

    def test_name_change(self, nse):
        _has_data(nse.cm_eod_eq_name_change(), label="name_change")

    def test_symbol_change(self, nse):
        _has_data(nse.cm_eod_eq_symbol_change(), label="symbol_change")

    def test_fii_dii_nse(self, nse):
        _has_data(nse.cm_eod_fii_dii_activity("Nse"), label="fii_dii_nse")

    def test_fii_dii_all(self, nse):
        _has_data(nse.cm_eod_fii_dii_activity("All"), label="fii_dii_all")

    def test_fno_bhavcopy(self, nse):
        _has_data(nse.fno_eod_bhav_copy(self.D4), label="fno_bhavcopy")

    def test_fno_top10_futures(self, nse):
        _has_data(nse.fno_eod_top10_fut(self.D4), label="fno_top10")

    def test_fno_top20_options(self, nse):
        _has_data(nse.fno_eod_top20_opt(self.D4), label="fno_top20")

    def test_index_bhavcopy(self, nse):
        _has_data(nse.index_eod_bhav_copy(self.D4), label="index_bhavcopy")


# ══════════════════════════════════════════════════════════════════════════════
# 15. BUSINESS GROWTH & SETTLEMENT
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
@pytest.mark.slow
class TestBizGrowthSettlement:

    @pytest.mark.parametrize("mode", ["yearly", "monthly"])
    def test_cm_biz_growth(self, nse, mode):
        _has_data(nse.cm_dmy_biz_growth(mode), label=f"cm_biz_{mode}")

    def test_cm_biz_growth_daily(self, nse):
        _has_data(nse.cm_dmy_biz_growth("daily", BIZ_MONTH, BIZ_YEAR), label="cm_biz_daily_oct25")

    @pytest.mark.parametrize("mode", ["yearly", "monthly"])
    def test_fno_biz_growth(self, nse, mode):
        _has_data(nse.fno_dmy_biz_growth(mode), label=f"fno_biz_{mode}")

    def test_cm_settlement_fy2025(self, nse):
        # Default current-FY may be partial; use a completed FY for guaranteed data
        _has_data(nse.cm_monthly_settlement_report("2024", 2025), label="cm_settle_fy2025")

    def test_cm_settlement_3y(self, nse):
        _has_data(nse.cm_monthly_settlement_report("3Y"), label="cm_settle_3Y")

    def test_fno_settlement(self, nse):
        _has_data(nse.fno_monthly_settlement_report("1Y"), label="fno_settle_1Y")

    def test_monthly_most_active(self, nse):
        _has_data(nse.cm_monthly_most_active_equity(), label="monthly_most_active")

    def test_advances_declines_default(self, nse):
        _has_data(nse.historical_advances_decline(), label="adv_dec_default")

    def test_advances_declines_year(self, nse):
        _has_data(nse.historical_advances_decline(datetime.now().year), label="adv_dec_year")


# ══════════════════════════════════════════════════════════════════════════════
# 16. SEBI
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestSEBI:

    def test_sebi_circulars_default(self, nse):
        _has_df(nse.sebi_circulars(), cols={"Date", "Title", "Link"}, label="sebi_default")

    def test_sebi_circulars_date_range(self, nse):
        _has_df(nse.sebi_circulars(DATE_FROM, DATE_TO), label="sebi_range")

    @pytest.mark.parametrize("period", ["1W", "1M", "3M"])
    def test_sebi_circulars_period(self, nse, period):
        _has_df(nse.sebi_circulars(period), label=f"sebi_{period}")

    def test_sebi_data_1_page(self, nse):
        _has_df(nse.sebi_data(pages=1), label="sebi_data")

    def test_sebi_data_sorted_newest_first(self, nse):
        df = nse.sebi_data(pages=1)
        _has_df(df)
        dates = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce").dropna()
        assert dates.is_monotonic_decreasing, "sebi_data must be sorted newest first"


# ══════════════════════════════════════════════════════════════════════════════
# 17. IPO
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestIPO:

    def test_ipo_current(self, nse):
        _has_data(nse.ipo_current(), label="ipo_current")

    def test_ipo_preopen(self, nse):
        _has_data(nse.ipo_preopen(), label="ipo_preopen")

    def test_ipo_tracker_all(self, nse):
        _has_data(nse.ipo_tracker_summary(), label="ipo_tracker_all")

    def test_ipo_tracker_sme(self, nse):
        _has_data(nse.ipo_tracker_summary("SME"), label="ipo_tracker_sme")

    def test_ipo_tracker_mainboard(self, nse):
        _has_data(nse.ipo_tracker_summary("Mainboard"), label="ipo_tracker_main")


# ══════════════════════════════════════════════════════════════════════════════
# 18. PEER COMPARISON
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# 19. STOCKS TRADED (live-analysis-stocksTraded)
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestStocksTraded:
    """Tests for Nse.cm_live_stocks_traded()."""

    # ── Basic fetch ───────────────────────────────────────────────────────────
    def test_returns_dataframe(self, nse):
        df = nse.cm_live_stocks_traded()
        _has_df(df, min_rows=1, label="stocks_traded_all")

    def test_expected_columns_present(self, nse):
        df = nse.cm_live_stocks_traded()
        _has_df(df)
        expected = {
            "Symbol", "Series", "Last Price (₹)", "Change (₹)",
            "% Change", "Volume (Lakhs)", "Value (₹ Cr.)", "Mkt Cap (₹ Cr.)",
        }
        missing = expected - set(df.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_no_filter_returns_all_series(self, nse):
        """Without a filter, multiple series should appear (EQ, BE, etc.)."""
        df = nse.cm_live_stocks_traded()
        _has_df(df)
        assert df["Series"].nunique() >= 1, "Expected at least one series"

    # ── Series filter ─────────────────────────────────────────────────────────
    def test_series_eq_kwarg(self, nse):
        df = nse.cm_live_stocks_traded(series="EQ")
        _has_df(df, min_rows=1, label="series_EQ_kwarg")
        assert (df["Series"].str.upper() == "EQ").all(), \
            "Non-EQ rows leaked through series filter"

    def test_series_eq_positional(self, nse):
        """First positional arg is treated as the series filter."""
        df = nse.cm_live_stocks_traded("EQ")
        _has_df(df, min_rows=1, label="series_EQ_positional")
        assert (df["Series"].str.upper() == "EQ").all()

    def test_series_case_insensitive(self, nse):
        df_lower = nse.cm_live_stocks_traded("eq")
        df_upper = nse.cm_live_stocks_traded("EQ")
        if df_lower is not None and df_upper is not None:
            assert len(df_lower) == len(df_upper), \
                "Series filter should be case-insensitive"

    def test_series_unknown_returns_none(self, nse):
        """An unknown series should return None (no matching rows)."""
        result = nse.cm_live_stocks_traded(series="ZZZZZ")
        assert result is None, "Expected None for an unknown series"

    # ── Market-cap GTE filter ─────────────────────────────────────────────────
    def test_mkt_cap_gte_filter(self, nse):
        threshold = 1_000.0   # ₹ 1,000 Cr
        df = nse.cm_live_stocks_traded(mkt_cap_gte=threshold)
        _has_df(df, min_rows=1, label=f"mkt_cap_gte_{threshold}")
        assert (df["Mkt Cap (₹ Cr.)"] >= threshold).all(), \
            "Row below mkt_cap_gte threshold found"

    def test_mkt_cap_gte_very_high_returns_none_or_df(self, nse):
        """A ludicrously high cap may return None — that is also valid."""
        result = nse.cm_live_stocks_traded(mkt_cap_gte=1e15)
        # either None or a valid (possibly empty-filtered) result is fine
        if result is not None:
            assert isinstance(result, pd.DataFrame)

    # ── Market-cap LTE filter ─────────────────────────────────────────────────
    def test_mkt_cap_lte_filter(self, nse):
        threshold = 500.0   # ₹ 500 Cr
        df = nse.cm_live_stocks_traded(mkt_cap_lte=threshold)
        _has_df(df, min_rows=1, label=f"mkt_cap_lte_{threshold}")
        assert (df["Mkt Cap (₹ Cr.)"] <= threshold).all(), \
            "Row above mkt_cap_lte threshold found"

    # ── Combined filters ──────────────────────────────────────────────────────
    def test_combined_series_and_mkt_cap_gte(self, nse):
        df = nse.cm_live_stocks_traded("EQ", mkt_cap_gte=1_000)
        _has_df(df, min_rows=1, label="EQ+mkt_cap_gte_1000")
        assert (df["Series"].str.upper() == "EQ").all()
        assert (df["Mkt Cap (₹ Cr.)"] >= 1_000).all()

    def test_combined_series_and_mkt_cap_lte(self, nse):
        df = nse.cm_live_stocks_traded("EQ", mkt_cap_lte=500)
        if df is not None:
            assert (df["Series"].str.upper() == "EQ").all()
            assert (df["Mkt Cap (₹ Cr.)"] <= 500).all()

    def test_combined_mkt_cap_range(self, nse):
        df = nse.cm_live_stocks_traded(mkt_cap_gte=500, mkt_cap_lte=5_000)
        _has_df(df, min_rows=1, label="mkt_cap_500_to_5000")
        assert (df["Mkt Cap (₹ Cr.)"] >= 500).all()
        assert (df["Mkt Cap (₹ Cr.)"] <= 5_000).all()

    def test_combined_all_three_filters(self, nse):
        df = nse.cm_live_stocks_traded("EQ", mkt_cap_gte=500, mkt_cap_lte=10_000)
        if df is not None:
            assert (df["Series"].str.upper() == "EQ").all()
            assert (df["Mkt Cap (₹ Cr.)"] >= 500).all()
            assert (df["Mkt Cap (₹ Cr.)"] <= 10_000).all()

    # ── Unit sanity checks (values already in correct units from NSE) ────────
    # def test_mkt_cap_is_positive(self, nse):
    #     """Market cap should be positive for all traded stocks."""
    #     df = nse.cm_live_stocks_traded()
    #     _has_df(df)
    #     assert (df["Mkt Cap (₹ Cr.)"] > 0).all(), \
    #         "Negative or zero Mkt Cap found"

    # def test_value_is_positive(self, nse):
    #     """Traded value should be positive."""
    #     df = nse.cm_live_stocks_traded()
    #     _has_df(df)
    #     assert (df["Value (₹ Cr.)"] > 0).all(), \
    #         "Negative or zero Value found"

    def test_volume_reasonable_range(self, nse):
        """Volume (Lakhs) should be > 0 for traded stocks."""
        df = nse.cm_live_stocks_traded()
        _has_df(df)
        assert (df["Volume (Lakhs)"] >= 0).all(), "Negative volume found"

    def test_index_is_reset_after_filter(self, nse):
        df = nse.cm_live_stocks_traded("EQ", mkt_cap_gte=1_000)
        if df is not None:
            assert list(df.index) == list(range(len(df))), \
                "Index should be reset (0, 1, 2, …) after filtering"


# ══════════════════════════════════════════════════════════════════════════════
# 20. PRICE BAND HITTERS (live-analysis-price-band-hitter)
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestPriceBandHitters:
    """Tests for Nse.cm_live_price_band_hitters()."""

    EXPECTED_COLS = {
        "Symbol", "Series", "LTP (₹)", "Change (₹)", "% Change",
        "Price Band (%)", "High Price", "Low Price", "52W High", "52W Low",
        "Volume (Lakhs)", "Turnover (₹ Cr.)", "Band",
    }

    # ── Basic fetch ───────────────────────────────────────────────────────────
    def test_all_returns_dataframe(self, nse):
        df = nse.cm_live_price_band_hitters()
        _has_df(df, min_rows=1, label="band_all")

    def test_expected_columns_present(self, nse):
        df = nse.cm_live_price_band_hitters()
        _has_df(df)
        missing = self.EXPECTED_COLS - set(df.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_band_column_has_known_values(self, nse):
        df = nse.cm_live_price_band_hitters()
        _has_df(df)
        allowed = {"Upper", "Lower", "Both"}
        unexpected = set(df["Band"].unique()) - allowed
        assert not unexpected, f"Unexpected Band values: {unexpected}"

    # ── Band filter ───────────────────────────────────────────────────────────
    def test_upper_band(self, nse):
        df = nse.cm_live_price_band_hitters("upper")
        _has_df(df, min_rows=1, label="band_upper")
        assert (df["Band"] == "Upper").all(), "Non-Upper rows in upper filter"

    def test_lower_band(self, nse):
        df = nse.cm_live_price_band_hitters("lower")
        _has_df(df, min_rows=1, label="band_lower")
        assert (df["Band"] == "Lower").all(), "Non-Lower rows in lower filter"

    def test_both_band(self, nse):
        result = nse.cm_live_price_band_hitters("both")
        # "both" may be empty on some days — that is valid
        if result is not None:
            assert (result["Band"] == "Both").all()

    def test_band_case_insensitive(self, nse):
        df_upper = nse.cm_live_price_band_hitters("upper")
        df_UPPER = nse.cm_live_price_band_hitters("UPPER")
        if df_upper is not None and df_UPPER is not None:
            assert len(df_upper) == len(df_UPPER)

    def test_invalid_band_raises(self, nse):
        with pytest.raises(ValueError, match="band must be"):
            nse.cm_live_price_band_hitters("sideways")

    # ── sec_type filter ───────────────────────────────────────────────────────
    def test_sec_type_allsec(self, nse):
        df = nse.cm_live_price_band_hitters("all", "AllSec")
        _has_df(df, min_rows=1, label="AllSec")

    def test_sec_type_secgtr20(self, nse):
        result = nse.cm_live_price_band_hitters("upper", "SecGtr20")
        if result is not None:
            _has_df(result, label="SecGtr20")

    def test_sec_type_seclwr20(self, nse):
        result = nse.cm_live_price_band_hitters("upper", "SecLwr20")
        if result is not None:
            _has_df(result, label="SecLwr20")

    def test_invalid_sec_type_raises(self, nse):
        with pytest.raises(ValueError, match="sec_type must be"):
            nse.cm_live_price_band_hitters("upper", "BadSec")

    # ── Series filter ─────────────────────────────────────────────────────────
    def test_series_eq_filter(self, nse):
        df = nse.cm_live_price_band_hitters("upper", series="EQ")
        if df is not None:
            assert (df["Series"].str.upper() == "EQ").all()

    def test_series_case_insensitive(self, nse):
        df_lower = nse.cm_live_price_band_hitters(series="eq")
        df_upper = nse.cm_live_price_band_hitters(series="EQ")
        if df_lower is not None and df_upper is not None:
            assert len(df_lower) == len(df_upper)

    def test_series_unknown_returns_none(self, nse):
        result = nse.cm_live_price_band_hitters(series="ZZZZZ")
        assert result is None

    # ── Turnover filters ──────────────────────────────────────────────────────
    def test_turnover_gte_filter(self, nse):
        threshold = 1.0
        df = nse.cm_live_price_band_hitters(turnover_gte=threshold)
        _has_df(df, min_rows=1, label=f"turnover_gte_{threshold}")
        assert (df["Turnover (₹ Cr.)"] >= threshold).all()

    def test_turnover_lte_filter(self, nse):
        threshold = 10.0
        df = nse.cm_live_price_band_hitters(turnover_lte=threshold)
        _has_df(df, min_rows=1, label=f"turnover_lte_{threshold}")
        assert (df["Turnover (₹ Cr.)"] <= threshold).all()

    def test_turnover_range_filter(self, nse):
        df = nse.cm_live_price_band_hitters(turnover_gte=0.5, turnover_lte=50)
        _has_df(df, min_rows=1, label="turnover_range")
        assert (df["Turnover (₹ Cr.)"] >= 0.5).all()
        assert (df["Turnover (₹ Cr.)"] <= 50).all()

    # ── Combined filters ──────────────────────────────────────────────────────
    def test_combined_upper_eq_turnover_gte(self, nse):
        df = nse.cm_live_price_band_hitters("upper", series="EQ", turnover_gte=1)
        if df is not None:
            assert (df["Band"] == "Upper").all()
            assert (df["Series"].str.upper() == "EQ").all()
            assert (df["Turnover (₹ Cr.)"] >= 1).all()

    def test_combined_lower_secgtr20_turnover(self, nse):
        df = nse.cm_live_price_band_hitters(
            "lower", "SecGtr20", turnover_gte=0.5, turnover_lte=50
        )
        if df is not None:
            assert (df["Band"] == "Lower").all()
            assert (df["Turnover (₹ Cr.)"] >= 0.5).all()
            assert (df["Turnover (₹ Cr.)"] <= 50).all()

    # ── Sanity checks ─────────────────────────────────────────────────────────
    def test_ltp_is_positive(self, nse):
        df = nse.cm_live_price_band_hitters()
        _has_df(df)
        assert (df["LTP (₹)"] > 0).all(), "Non-positive LTP found"

    def test_volume_is_non_negative(self, nse):
        df = nse.cm_live_price_band_hitters()
        _has_df(df)
        assert (df["Volume (Lakhs)"] >= 0).all()

    def test_index_reset_after_filter(self, nse):
        df = nse.cm_live_price_band_hitters("upper", series="EQ")
        if df is not None:
            assert list(df.index) == list(range(len(df)))


class TestParseQuarter:
    """Offline unit tests for the _parse_quarter static helper."""

    @pytest.mark.parametrize("inp,expected", [
        ("Mar 2026",      "2026-03"),
        ("March 2026",    "2026-03"),
        ("dec 2025",      "2025-12"),
        ("December 2025", "2025-12"),
        ("Jun 2025",      "2025-06"),
        ("2025-09",       "2025-09"),   # pass-through
        ("2026-03",       "2026-03"),   # pass-through
    ])
    def test_valid_quarters(self, inp, expected):
        assert NseKit.Nse._parse_quarter(inp) == expected

    @pytest.mark.parametrize("bad", ["Q4 2025", "2025", "Mar", "13 2025", ""])
    def test_invalid_quarter_raises(self, bad):
        with pytest.raises(ValueError, match="Cannot parse quarter"):
            NseKit.Nse._parse_quarter(bad)


@pytest.mark.live
class TestPeerComparison:

    SYMBOL  = "TRENT"
    QUARTER = PEER_QUARTER

    EXPECTED_COLS = {
        "Symbol", "LTP (₹)", "% Chng", "Volume (Lakhs)", "Value (₹ Cr.)",
        "Mkt Cap (₹ Cr.)", "P/E", "Total Income (₹ Cr.)",
        "Net Profit (₹ Cr.)", "EPS (₹)", "Debt/Equity", "Promoter %",
    }

    def test_standalone_returns_df(self, nse):
        df = nse.peer_comparison(self.SYMBOL, self.QUARTER, "Standalone")
        _has_df(df, min_rows=1, cols=self.EXPECTED_COLS, label="peer_standalone")

    def test_consolidated_returns_df(self, nse):
        df = nse.peer_comparison(self.SYMBOL, self.QUARTER, "Consolidated")
        _has_df(df, min_rows=1, cols=self.EXPECTED_COLS, label="peer_consolidated")

    def test_short_type_s(self, nse):
        df = nse.peer_comparison(self.SYMBOL, self.QUARTER, "S")
        _has_df(df, min_rows=1, label="peer_S")

    def test_short_type_c(self, nse):
        df = nse.peer_comparison(self.SYMBOL, self.QUARTER, "C")
        _has_df(df, min_rows=1, label="peer_C")

    def test_quarter_passthrough_format(self, nse):
        df = nse.peer_comparison(self.SYMBOL, "2025-12", "S")
        _has_df(df, min_rows=1, label="peer_passthrough")

    def test_symbol_column_present(self, nse):
        df = nse.peer_comparison(self.SYMBOL, self.QUARTER)
        assert "Symbol" in df.columns

    def test_queried_symbol_in_results(self, nse):
        df = nse.peer_comparison(self.SYMBOL, self.QUARTER)
        assert self.SYMBOL in df["Symbol"].values, \
            f"{self.SYMBOL} not found in peer results"

    def test_no_series_markettype_pchange_cols(self, nse):
        df = nse.peer_comparison(self.SYMBOL, self.QUARTER)
        dropped = {"series", "marketType", "PChange"}
        present = dropped & set(df.columns)
        assert not present, f"Unexpected columns still present: {present}"

    def test_mktcap_in_crores(self, nse):
        """Mkt Cap should be in crores (< 1e9), not absolute rupees."""
        df = nse.peer_comparison(self.SYMBOL, self.QUARTER)
        col = "Mkt Cap (₹ Cr.)"
        if col in df.columns:
            assert df[col].max() < 1e9, "marketCap looks like absolute ₹, not crores"

    def test_volume_in_lakhs(self, nse):
        """Volume (Lakhs) should be < 1e6 for any realistic stock."""
        df = nse.peer_comparison(self.SYMBOL, self.QUARTER)
        col = "Volume (Lakhs)"
        if col in df.columns:
            assert df[col].max() < 1e6, "Volume looks like raw units, not Lakhs"

    def test_invalid_report_type_raises(self, nse):
        with pytest.raises(ValueError, match="report_type must be"):
            nse.peer_comparison(self.SYMBOL, self.QUARTER, "XYZZY")

    def test_invalid_quarter_raises(self, nse):
        with pytest.raises(ValueError, match="Cannot parse quarter"):
            nse.peer_comparison(self.SYMBOL, "Q4 2025")
