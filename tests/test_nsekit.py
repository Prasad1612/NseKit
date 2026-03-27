"""
test_nsekit.py — Professional test suite for NseKit
=====================================================

Run:
    pytest test_nsekit.py -v --tb=short

All tests use a *single* shared ``Nse`` instance (session-fixture scope) so
the warm-up cost is paid only once across the entire suite.

Marking convention
------------------
- ``@pytest.mark.live``  — calls a live NSE / SEBI endpoint.
  Skip if you have no internet: ``pytest -m "not live"``.
- ``@pytest.mark.slow``  — fetches multi-chunk historical data (>5 s).
  Skip with: ``pytest -m "not slow"``.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta

import pandas as pd
import pytest

import NseKit  # the module being tested


# ── Shared fixture ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def nse() -> NseKit.Nse:
    """Single Nse instance reused across the whole test session."""
    return NseKit.Nse(max_rps=1.5)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _is_df(result, min_rows: int = 0) -> None:
    """Assert result is a non-None DataFrame with at least *min_rows* rows."""
    assert result is not None, "Expected DataFrame, got None"
    assert isinstance(result, pd.DataFrame), f"Expected DataFrame, got {type(result)}"
    assert len(result) >= min_rows, f"Expected >= {min_rows} rows, got {len(result)}"


def _is_dict(result) -> None:
    assert result is not None
    assert isinstance(result, dict)


# def _graceful(result) -> None:
#     """Assert result is None OR a non-empty DataFrame — never raises."""
#     if result is not None:
#         assert isinstance(result, (pd.DataFrame, dict, list))

def _graceful(result):
    """Assert result is None OR valid type — never raises."""
    if result is not None:
        assert isinstance(result, (pd.DataFrame, dict, list))
    return True


def _today() -> str:
    return datetime.now().strftime("%d-%m-%Y")


def _days_ago(n: int) -> str:
    return (datetime.now() - timedelta(days=n)).strftime("%d-%m-%Y")


# ══════════════════════════════════════════════════════════════════════════════
# 1. Rate-limit helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestRateLimit:
    def test_global_config_override(self):
        # 1. Save original
        orig = NseKit.NseConfig.max_rps
        try:
            # 2. Test global change
            NseKit.NseConfig.max_rps = 2.0
            assert NseKit.NseConfig.max_rps == 2.0
            
            # 3. New instance should pick it up
            nse_new = NseKit.Nse()
            assert nse_new.max_rps == 2.0
        finally:
            NseKit.NseConfig.max_rps = orig

    def test_instance_config_override(self):
        # Instance limit should be independent of global limit
        global_limit = NseKit.NseConfig.max_rps
        nse_cust = NseKit.Nse(max_rps=1.2)
        assert nse_cust.max_rps == 1.2
        assert NseKit.NseConfig.max_rps == global_limit

    def test_invalid_rate_raises(self):
        # Validator in __init__ should catch this
        with pytest.raises(ValueError, match="max_rps must be positive"):
            NseKit.Nse(max_rps=0)
        with pytest.raises(ValueError, match="max_rps must be positive"):
            NseKit.Nse(max_rps=-1.0)

    def test_throttle_does_not_raise(self, nse):
        nse._throttle()   # should return without error


# ══════════════════════════════════════════════════════════════════════════════
# 1a. Cookie Cache
# ══════════════════════════════════════════════════════════════════════════════

class TestCookieCache:
    def test_cookie_attributes_exist(self, nse):
        assert hasattr(nse, "_COOKIE_CACHE")
        assert hasattr(nse, "_COOKIE_TTL")
        assert isinstance(nse._COOKIE_CACHE, str)
        assert isinstance(nse._COOKIE_TTL, int)

    def test_clear_and_warmup_flow(self, nse):
        import os
        import shelve

        # 1. Clear
        nse.clear_cookie_cache()
        for ext in ("", ".db", ".dir", ".bak", ".dat"):
            assert not os.path.exists(nse._COOKIE_CACHE + ext)

        # 2. Warm up
        nse._warm_up()
        
        # 3. Check if created
        found = False
        for ext in ("", ".db", ".dir", ".bak", ".dat"):
            if os.path.exists(nse._COOKIE_CACHE + ext):
                found = True
                break
        assert found, "Cookie cache file not created after warm_up"

        # 4. Verify content
        with shelve.open(nse._COOKIE_CACHE) as db:
            assert "cookies" in db
            assert "ts" in db
            assert isinstance(db["cookies"], dict)
            assert len(db["cookies"]) > 0

    def test_load_cookies_success(self, nse):
        # Ensure we have a fresh cache
        nse._warm_up()
        # Clear session cookies to force load from cache
        nse.session.cookies.clear()
        assert len(nse.session.cookies) == 0
        
        # Load
        success = nse._load_cookies()
        assert success
        assert len(nse.session.cookies) > 0


# ══════════════════════════════════════════════════════════════════════════════
# 2. NSE Market / Holidays
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestNseMarket:
    @pytest.mark.parametrize("mode", [
        "Market Status", "Mcap", "Nifty50", "Gift Nifty",
    ])
    def test_market_status_modes(self, nse, mode):
        result = nse.nse_market_status(mode)
        assert result is not None, f"nse_market_status('{mode}') returned None"
        assert isinstance(result, pd.DataFrame)

    def test_market_status_all(self, nse):
        result = nse.nse_market_status("all")
        assert isinstance(result, dict)
        assert set(result.keys()) == {"Market Status", "Mcap", "Nifty50", "Gift Nifty"}

    def test_market_status_invalid_mode(self, nse):
        result = nse.nse_market_status("INVALID_MODE")
        assert result is None

    def test_nse_is_market_open(self, nse):
        result = nse.nse_is_market_open("Capital Market")
        # Returns a rich Text or None — just check it doesn't crash
        assert result is not None

    def test_trading_holidays_df(self, nse):
        df = nse.nse_trading_holidays()
        _is_df(df)
        assert "tradingDate" in df.columns

    def test_trading_holidays_list(self, nse):
        lst = nse.nse_trading_holidays(list_only=True)
        assert isinstance(lst, list)
        assert all(isinstance(d, str) for d in lst)

    def test_clearing_holidays_df(self, nse):
        df = nse.nse_clearing_holidays()
        _is_df(df)

    def test_clearing_holidays_list(self, nse):
        lst = nse.nse_clearing_holidays(list_only=True)
        assert isinstance(lst, list)

    def test_is_trading_holiday_today(self, nse):
        result = nse.is_nse_trading_holiday()
        assert isinstance(result, bool)

    def test_is_trading_holiday_known_holiday(self, nse):
        # Christmas 2026 — likely a holiday
        result = nse.is_nse_trading_holiday("25-Dec-2026")
        assert result is not None

    def test_is_clearing_holiday_today(self, nse):
        result = nse.is_nse_clearing_holiday()
        assert isinstance(result, bool)

    def test_live_market_turnover(self, nse):
        result = nse.nse_live_market_turnover()
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    def test_reference_rates(self, nse):
        df = nse.nse_reference_rates()
        _is_df(df)
        assert "currency" in df.columns

    def test_nifty_50_list(self, nse):
        df = nse.nse_6m_nifty_50()
        _is_df(df, min_rows=50)

    def test_nifty_50_list_only(self, nse):
        lst = nse.nse_6m_nifty_50(list_only=True)
        assert isinstance(lst, list)
        assert "RELIANCE" in lst

    def test_nifty_500_list(self, nse):
        df = nse.nse_6m_nifty_500()
        _is_df(df, min_rows=400)

    def test_equity_full_list(self, nse):
        df = nse.nse_eod_equity_full_list()
        _is_df(df, min_rows=1000)

    def test_equity_full_list_only(self, nse):
        lst = nse.nse_eod_equity_full_list(list_only=True)
        assert isinstance(lst, list)

    def test_fno_full_list_stocks(self, nse):
        df = nse.nse_eom_fno_full_list("stocks")
        _is_df(df)

    def test_fno_full_list_index(self, nse):
        df = nse.nse_eom_fno_full_list("index")
        _is_df(df)

    def test_fno_full_list_symbol_only(self, nse):
        lst = nse.nse_eom_fno_full_list(list_only=True)
        assert isinstance(lst, list)


# ══════════════════════════════════════════════════════════════════════════════
# 3. Circulars & Press Releases
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestCircularsAndPress:
    def test_circulars_default(self, nse):
        df = nse.nse_live_hist_circulars()
        assert isinstance(df, pd.DataFrame)

    def test_circulars_date_range(self, nse):
        df = nse.nse_live_hist_circulars(_days_ago(30), _today())
        assert isinstance(df, pd.DataFrame)

    def test_circulars_with_filter(self, nse):
        df = nse.nse_live_hist_circulars(filter="Listing")
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert df["Department"].str.contains("Listing", case=False).all()

    def test_press_releases_default(self, nse):
        df = nse.nse_live_hist_press_releases()
        assert isinstance(df, pd.DataFrame)

    def test_press_releases_date_range(self, nse):
        df = nse.nse_live_hist_press_releases(_days_ago(30), _today())
        assert isinstance(df, pd.DataFrame)

    def test_press_releases_with_filter(self, nse):
        df = nse.nse_live_hist_press_releases(filter="NSE Listing")
        assert isinstance(df, pd.DataFrame)


# ══════════════════════════════════════════════════════════════════════════════
# 4. Pre-Market
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestPreMarket:
    @pytest.mark.parametrize("category", ["All", "NIFTY 50", "Securities in F&O"])
    def test_pre_market_info(self, nse, category):
        result = nse.pre_market_info(category)
        _graceful(result)

    @pytest.mark.parametrize("category", ["Index Futures", "Stock Futures"])
    def test_pre_market_derivatives(self, nse, category):
        result = nse.pre_market_derivatives_info(category)
        _graceful(result)

    # def test_preopen_index_summary(self, nse):
    #     result = nse.nse_preopen_index_summary()
    #     _graceful(result)


# ══════════════════════════════════════════════════════════════════════════════
# 5. Equity Live Data
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestEquityLive:
    SYMBOL = "RELIANCE"

    def test_equity_price_info_keys(self, nse):
        result = nse.cm_live_equity_price_info(self.SYMBOL)
        assert result is not None
        required = {"Symbol", "LastTradedPrice", "Open", "High", "Low", "Close"}
        assert required.issubset(result.keys()), f"Missing keys: {required - result.keys()}"

    # def test_equity_price_info_invalid_symbol(self, nse):
    #     result = nse.cm_live_equity_price_info("XXXXXXINVALID999")
    #     assert result is None

    def test_equity_full_info(self, nse):
        result = nse.cm_live_equity_full_info(self.SYMBOL)
        assert result is not None
        assert "Symbol" in result and "MarketCap" in result

    def test_equity_price_info(self, nse):
        result = nse.cm_live_equity_price_info(self.SYMBOL)
        assert result is not None
        assert "Symbol" in result

    @pytest.mark.parametrize("fn_name", [
        "cm_live_most_active_equity_by_value",
        "cm_live_most_active_equity_by_vol",
        "cm_live_volume_spurts",
        "cm_live_52week_high",
        "cm_live_52week_low",
    ])
    def test_live_screeners(self, nse, fn_name):
        result = getattr(nse, fn_name)()
        _graceful(result)


# ══════════════════════════════════════════════════════════════════════════════
# 6. Index Live Data
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestIndexLive:
    def test_all_indices_data(self, nse):
        df = nse.index_live_all_indices_data()
        _is_df(df, min_rows=5)
        assert "index" in df.columns

    def test_index_stocks_data(self, nse):
        df = nse.index_live_indices_stocks_data("NIFTY 50")
        _is_df(df, min_rows=51)

    def test_index_stocks_list_only(self, nse):
        lst = nse.index_live_indices_stocks_data("NIFTY 50", list_only=True)
        assert isinstance(lst, list)
        assert len(lst) == 51

    def test_nifty_50_returns(self, nse):
        result = nse.index_live_nifty_50_returns()
        _graceful(result)

    @pytest.mark.parametrize("mode", ["First Five", "Full"])
    def test_index_contribution(self, nse, mode):
        result = nse.index_live_contribution("NIFTY 50", mode)
        _graceful(result)

    # def test_india_vix(self, nse):
    #     result = nse.india_vix_chart()
    #     _graceful(result)


# ══════════════════════════════════════════════════════════════════════════════
# 7. Historical — Equity
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
@pytest.mark.slow
class TestHistoricalEquity:
    SYMBOL = "RELIANCE"
    COLS   = {"Symbol", "Date", "Open Price", "Close Price"}

    def test_security_wise_1m(self, nse):
        df = nse.cm_hist_security_wise_data(self.SYMBOL, "1M")
        _is_df(df, min_rows=10)
        assert self.COLS.issubset(df.columns)

    def test_security_wise_date_range(self, nse):
        df = nse.cm_hist_security_wise_data(
            self.SYMBOL, _days_ago(60), _today()
        )
        _is_df(df)
        # Dates must be sorted ascending
        dates = pd.to_datetime(df["Date"], format="%d-%b-%Y")
        assert dates.is_monotonic_increasing

    def test_security_wise_positional(self, nse):
        df = nse.cm_hist_security_wise_data("TCS", "3M")
        _is_df(df)

    def test_security_wise_no_duplicates(self, nse):
        df = nse.cm_hist_security_wise_data(self.SYMBOL, "6M")
        if df is not None and not df.empty:
            assert df["Date"].nunique() == len(df), "Duplicate dates found"

    @pytest.mark.parametrize("sym,period", [
        ("INFY",      "1M"),
        ("HDFCBANK",  "3M"),
        ("TCS",       "6M"),
    ])
    def test_security_wise_parametric(self, nse, sym, period):
        df = nse.cm_hist_security_wise_data(sym, period)
        _graceful(df)

    def test_bulk_deals_default(self, nse):
        df = nse.cm_hist_bulk_deals()
        _graceful(df)

    def test_bulk_deals_1w(self, nse):
        df = nse.cm_hist_bulk_deals("1W")
        _graceful(df)

    def test_bulk_deals_symbol(self, nse):
        df = nse.cm_hist_bulk_deals(self.SYMBOL)
        _graceful(df)

    def test_block_deals(self, nse):
        df = nse.cm_hist_block_deals("1M")
        _graceful(df)

    def test_short_selling(self, nse):
        df = nse.cm_hist_short_selling("1W")
        _graceful(df)

    def test_hist_insider_trading(self, nse):
        df = nse.cm_live_hist_insider_trading(self.SYMBOL)
        _graceful(df)

    def test_hist_price_band_1w(self, nse):
        df = nse.cm_hist_eq_price_band("1W")
        _graceful(df)


# ══════════════════════════════════════════════════════════════════════════════
# 8. Historical — Index
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
@pytest.mark.slow
class TestHistoricalIndex:
    INDEX = "NIFTY 50"
    COLS  = {"Date", "Open", "High", "Low", "Close"}

    def test_index_historical_1m(self, nse):
        df = nse.index_historical_data(self.INDEX, "1M")
        _is_df(df, min_rows=10)
        assert self.COLS.issubset(df.columns)

    def test_index_historical_date_range(self, nse):
        df = nse.index_historical_data(self.INDEX, _days_ago(90), _today())
        _is_df(df)

    def test_index_historical_period_shorthand(self, nse):
        df = nse.index_historical_data(self.INDEX, "3M")
        _is_df(df)

    def test_index_pe_pb(self, nse):
        df = nse.index_pe_pb_div_historical_data(self.INDEX, "1M")
        _graceful(df)

    def test_vix_historical(self, nse):
        df = nse.india_vix_historical_data("1M")
        _graceful(df)


# ══════════════════════════════════════════════════════════════════════════════
# 9. Corporate Filings
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestCorporateFilings:
    SYMBOL = "RELIANCE"

    def test_corporate_action_default(self, nse):
        _graceful(nse.cm_live_hist_corporate_action())

    def test_corporate_action_1m(self, nse):
        _graceful(nse.cm_live_hist_corporate_action("1M"))

    def test_corporate_action_symbol(self, nse):
        _graceful(nse.cm_live_hist_corporate_action(self.SYMBOL))

    def test_corporate_action_filter(self, nse):
        result = nse.cm_live_hist_corporate_action("1M", filter="Dividend")
        if result is not None and not result.empty:
            assert result["PURPOSE"].str.contains("Dividend", case=False).all()

    def test_board_meetings_default(self, nse):
        _graceful(nse.cm_live_hist_board_meetings())

    def test_board_meetings_symbol(self, nse):
        _graceful(nse.cm_live_hist_board_meetings(self.SYMBOL))

    def test_board_meetings_date_range(self, nse):
        _graceful(nse.cm_live_hist_board_meetings(_days_ago(30), _today()))

    def test_shareholder_meetings_noargs(self, nse):
        """No args → fetches all records (matches old code behaviour)."""
        _graceful(nse.cm_live_hist_Shareholder_meetings())

    def test_shareholder_meetings_symbol(self, nse):
        _graceful(nse.cm_live_hist_Shareholder_meetings(self.SYMBOL))

    def test_shareholder_meetings_date_range(self, nse):
        _graceful(nse.cm_live_hist_Shareholder_meetings(_days_ago(180), _today()))

    def test_today_event_calendar(self, nse):
        _graceful(nse.cm_live_today_event_calendar())

    def test_upcoming_event_calendar(self, nse):
        _graceful(nse.cm_live_upcoming_event_calendar())

    def test_quarterly_shareholding(self, nse):
        _graceful(nse.cm_live_qtly_shareholding_patterns())

    def test_br_sr_default(self, nse):
        _graceful(nse.cm_live_hist_br_sr())

    def test_br_sr_symbol(self, nse):
        _graceful(nse.cm_live_hist_br_sr(self.SYMBOL))

    def test_voting_results(self, nse):
        _graceful(nse.cm_live_voting_results())

    def test_insider_trading(self, nse):
        _graceful(nse.cm_live_hist_insider_trading(self.SYMBOL))

    def test_announcements(self, nse):
        _graceful(nse.cm_live_hist_corporate_announcement(self.SYMBOL))

    @pytest.mark.parametrize("stage", ["In-Principle", "Listing Stage"])
    def test_qip(self, nse, stage):
        _graceful(nse.cm_live_hist_qualified_institutional_placement(stage))

    @pytest.mark.parametrize("stage", ["In-Principle", "Listing Stage"])
    def test_pref_issue(self, nse, stage):
        _graceful(nse.cm_live_hist_preferential_issue(stage))

    @pytest.mark.parametrize("stage", ["In-Principle", "Listing Stage"])
    def test_rights_issue(self, nse, stage):
        _graceful(nse.cm_live_hist_right_issue(stage))


# ══════════════════════════════════════════════════════════════════════════════
# 10. F&O Live
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestFnoLive:
    def test_futures_data(self, nse):
        result = nse.fno_live_futures_data("NIFTY")
        _graceful(result)

    def test_expiry_dates(self, nse):
        result = nse.fno_expiry_dates("NIFTY")
        _graceful(result)

    def test_expiry_dates_raw(self, nse):
        result = nse.fno_expiry_dates_raw("NIFTY")
        _graceful(result)

    def test_live_option_chain_nifty(self, nse):
        result = nse.fno_live_option_chain("NIFTY")
        _graceful(result)

    def test_most_active_futures(self, nse):
        _graceful(nse.fno_live_most_active_futures_contracts("Volume"))
        _graceful(nse.fno_live_most_active_futures_contracts("Value"))

    @pytest.mark.parametrize("mode,opt,sort_by", [
        ("Index", "Call", "Volume"),
        ("Index", "Put",  "Value"),
        ("Stock", "Call", "Volume"),
    ])
    def test_most_active_options(self, nse, mode, opt, sort_by):
        _graceful(nse.fno_live_most_active(mode, opt, sort_by))

    def test_most_active_by_oi(self, nse):
        _graceful(nse.fno_live_most_active_contracts_by_oi())

    def test_most_active_by_volume(self, nse):
        _graceful(nse.fno_live_most_active_contracts_by_volume())

    def test_most_active_underlying(self, nse):
        _graceful(nse.fno_live_most_active_underlying())

    def test_change_in_oi(self, nse):
        _graceful(nse.fno_live_change_in_oi())

    def test_oi_vs_price(self, nse):
        _graceful(nse.fno_live_oi_vs_price())

    def test_top_20_stock_futures(self, nse):
        _graceful(nse.fno_live_top_20_derivatives_contracts("Stock Futures"))

    def test_top_20_stock_options(self, nse):
        _graceful(nse.fno_live_top_20_derivatives_contracts("Stock Options"))

    def test_top_20_invalid_category(self, nse):
        with pytest.raises(ValueError):
            nse.fno_live_top_20_derivatives_contracts("INVALID")

    DATE_4Y = "17-10-2025"   # 4-digit year format
    DATE_2Y = "17-10-25"     # 2-digit year format        

    def test_participant_wise_oi(self, nse):
        _graceful(nse.fno_eod_participant_wise_oi(self.DATE_4Y))

    def test_participant_wise_volume(self, nse):
        _graceful(nse.fno_eod_participant_wise_vol(self.DATE_4Y))

    def test_fii_stats_fo(self, nse):
        _graceful(nse.fno_eod_fii_stats(self.DATE_4Y))

    def test_mwpl_data(self, nse):
        _graceful(nse.fno_eod_mwpl_3(self.DATE_4Y))

    @pytest.mark.parametrize("symbol", ["NIFTY", "TCS"])
    def test_active_contracts(self, nse, symbol):
        assert _graceful(nse.fno_live_active_contracts(symbol))

    expiry_date = "30-Mar-2026"

    @pytest.mark.parametrize("symbol", ["NIFTY", "TCS"])
    def test_active_contracts_with_expiry(self, nse, symbol):
        assert _graceful(nse.fno_live_active_contracts(symbol, expiry_date=self.expiry_date))

    def test_combined_oi(self, nse):
        _graceful(nse.fno_eod_combine_oi(self.DATE_4Y))

    def test_lot_sizes(self, nse):
        result = nse.fno_eom_lot_size()
        _graceful(result)

    def test_fno_ban_list(self, nse):
        _graceful(nse.fno_eod_sec_ban(self.DATE_4Y))


# ══════════════════════════════════════════════════════════════════════════════
# 11. F&O Historical
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
@pytest.mark.slow
class TestFnoHistorical:

    def test_futures_historical(self, nse):
        expiries = nse.fno_expiry_dates_raw("NIFTY")
        if not expiries or not expiries.get("expiryDates"):
            pytest.skip("No expiries available for NIFTY")

        expiry = expiries["expiryDates"][0]  # first live expiry

        # Example historical futures calls
        # Replace "FUTIDX" for index futures, "FUTSTK" for stock futures
        results = [
            nse.future_price_volume_data("NIFTY", "FUTIDX", expiry, from_date="01-10-2025", to_date="17-10-2025"),
            nse.future_price_volume_data("ITC", "FUTSTK", expiry, from_date="04-10-2025"),
            nse.future_price_volume_data("BANKNIFTY", "FUTIDX", expiry, period="3M"),
            nse.future_price_volume_data("NIFTY", "FUTIDX", "30-Nov-2024")  # specific expiry
        ]

        for r in results:
            _graceful(r)

    def test_options_historical(self, nse):
        expiries = nse.fno_expiry_dates_raw("NIFTY")
        if not expiries or not expiries.get("expiryDates"):
            pytest.skip("No expiries available for NIFTY")

        expiry = expiries["expiryDates"][0]  # first live expiry

        # Example historical options calls
        results = [
            nse.option_price_volume_data("NIFTY", "OPTIDX", expiry=expiry, from_date="01-10-2025", to_date="17-10-2025"),
            nse.option_price_volume_data("ITC", "OPTSTK", "CE", from_date="01-10-2025", to_date="17-10-2025", expiry=expiry),
            nse.option_price_volume_data("BANKNIFTY", "OPTIDX", "47000", from_date="01-10-2025", to_date="17-10-2025", expiry=expiry),
            nse.option_price_volume_data("ITC", "OPTSTK", from_date="04-10-2025", expiry=expiry),
            nse.option_price_volume_data("BANKNIFTY", "OPTIDX", period="3M"),
            nse.option_price_volume_data("NIFTY", "OPTIDX", "PE", from_date="01-10-2025", expiry=expiry)
        ]

        for r in results:
            _graceful(r)

# ══════════════════════════════════════════════════════════════════════════════
# 12. Charts
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestCharts:
    @pytest.mark.parametrize("timeframe", ["1D", "1M", "3M"])
    def test_index_chart(self, nse, timeframe):
        df = nse.index_chart("NIFTY 50", timeframe)
        _graceful(df)
        if df is not None:
            assert "datetime_utc" in df.columns
            assert "price" in df.columns

    @pytest.mark.parametrize("timeframe", ["1D", "1W"])
    def test_stock_chart(self, nse, timeframe):
        df = nse.stock_chart("RELIANCE", timeframe)
        _graceful(df)

    def test_vix_chart(self, nse):
        _graceful(nse.india_vix_chart())


# ══════════════════════════════════════════════════════════════════════════════
# 13. Business Growth & Settlement
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
@pytest.mark.slow
class TestBizGrowthAndSettlement:
    @pytest.mark.parametrize("mode", ["yearly", "monthly"])
    def test_cm_biz_growth(self, nse, mode):
        result = nse.cm_dmy_biz_growth(mode)
        _graceful(result)

    def test_cm_biz_growth_daily(self, nse):
        result = nse.cm_dmy_biz_growth("daily", "JAN", datetime.now().year)
        _graceful(result)

    @pytest.mark.parametrize("mode", ["yearly", "monthly"])
    def test_fno_biz_growth(self, nse, mode):
        result = nse.fno_dmy_biz_growth(mode)
        _graceful(result)

    def test_cm_settlement_report_default(self, nse):
        _graceful(nse.cm_monthly_settlement_report())

    def test_cm_settlement_report_1y(self, nse):
        _graceful(nse.cm_monthly_settlement_report("1Y"))

    def test_fno_settlement_report(self, nse):
        _graceful(nse.fno_monthly_settlement_report("1Y"))

    def test_cm_monthly_most_active(self, nse):
        _graceful(nse.cm_monthly_most_active_equity())

    def test_historical_advances_decline(self, nse):
        _graceful(nse.historical_advances_decline())

    def test_historical_advances_decline_year(self, nse):
        _graceful(nse.historical_advances_decline(datetime.now().year))


# ══════════════════════════════════════════════════════════════════════════════
# 14. EOD Archives
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestEodArchives:
    DATE_4Y = "17-10-2025"   # 4-digit year format
    DATE_2Y = "17-10-25"     # 2-digit year format

    def test_equity_bhavcopy(self, nse):
        _graceful(nse.cm_eod_equity_bhavcopy(self.DATE_4Y))

    def test_bhavcopy_with_delivery(self, nse):
        _graceful(nse.cm_eod_bhavcopy_with_delivery(self.DATE_4Y))

    def test_index_bhavcopy(self, nse):
        _graceful(nse.index_eod_bhav_copy(self.DATE_4Y))

    def test_eod_bulk_deal(self, nse):
        _graceful(nse.cm_eod_bulk_deal())

    def test_eod_block_deal(self, nse):
        _graceful(nse.cm_eod_block_deal())

    def test_eod_surveillance(self, nse):
        _graceful(nse.cm_eod_surveillance_indicator(self.DATE_2Y))

    def test_pe_ratio(self, nse):
        _graceful(nse.cm_eod_pe_ratio(self.DATE_2Y))

    def test_fno_bhavcopy(self, nse):
        _graceful(nse.fno_eod_bhav_copy(self.DATE_4Y))

    def test_fno_top10_futures(self, nse):
        _graceful(nse.fno_eod_top10_fut(self.DATE_4Y))

    def test_fno_top20_options(self, nse):
        _graceful(nse.fno_eod_top20_opt(self.DATE_4Y))

    def test_fno_sec_ban(self, nse):
        _graceful(nse.fno_eod_sec_ban(self.DATE_4Y))

    def test_fno_combine_oi(self, nse):
        _graceful(nse.fno_eod_combine_oi(self.DATE_4Y))

    def test_name_change(self, nse):
        _graceful(nse.cm_eod_eq_name_change())

    def test_symbol_change(self, nse):
        _graceful(nse.cm_eod_eq_symbol_change())

    def test_fii_dii_nse(self, nse):
        _graceful(nse.cm_eod_fii_dii_activity("Nse"))

    def test_fii_dii_all(self, nse):
        _graceful(nse.cm_eod_fii_dii_activity("All"))


# ══════════════════════════════════════════════════════════════════════════════
# 15. SEBI
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestSEBI:
    def test_sebi_circulars_default(self, nse):
        df = nse.sebi_circulars()
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "Date" in df.columns
            assert "Title" in df.columns
            assert "Link" in df.columns

    def test_sebi_circulars_date_range(self, nse):
        df = nse.sebi_circulars("01-10-2025", "15-10-2025")
        assert isinstance(df, pd.DataFrame)

    def test_sebi_circulars_single_date(self, nse):
        df = nse.sebi_circulars("01-01-2025")
        assert isinstance(df, pd.DataFrame)

    @pytest.mark.parametrize("period", ["1W", "1M", "3M"])
    def test_sebi_circulars_period(self, nse, period):
        df = nse.sebi_circulars(period)
        assert isinstance(df, pd.DataFrame)

    def test_sebi_circulars_period_positional(self, nse):
        """Period string passed as positional arg."""
        df = nse.sebi_circulars("1M")
        assert isinstance(df, pd.DataFrame)

    def test_sebi_data_1_page(self, nse):
        df = nse.sebi_data(pages=1)
        assert isinstance(df, pd.DataFrame)

    def test_sebi_data_sorted_descending(self, nse):
        df = nse.sebi_data(pages=1)
        if not df.empty:
            dates = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce").dropna()
            assert dates.is_monotonic_decreasing, "sebi_data should be sorted newest first"


# ══════════════════════════════════════════════════════════════════════════════
# 16. IPO
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestIPO:
    def test_ipo_current(self, nse):
        _graceful(nse.ipo_current())

    def test_ipo_preopen(self, nse):
        _graceful(nse.ipo_preopen())

    def test_ipo_tracker_summary_all(self, nse):
        _graceful(nse.ipo_tracker_summary())

    def test_ipo_tracker_summary_sme(self, nse):
        _graceful(nse.ipo_tracker_summary("SME"))

    def test_ipo_tracker_summary_mainboard(self, nse):
        _graceful(nse.ipo_tracker_summary("Mainboard"))


# ══════════════════════════════════════════════════════════════════════════════
# 17. Misc
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
class TestMisc:
    def test_quarterly_financial_results(self, nse):
        result = nse.quarterly_financial_results("TCS")
        _graceful(result)

    def test_recent_annual_reports(self, nse):
        df = nse.recent_annual_reports()
        _graceful(df)

    def test_state_wise_investors(self, nse):
        result = nse.state_wise_registered_investors()
        _graceful(result)

    def test_list_of_indices(self, nse):
        result = nse.list_of_indices()
        _graceful(result)

    # def test_market_advances_declines(self, nse):
    #     _graceful(nse.nse_live_market_advances_declines())


# ══════════════════════════════════════════════════════════════════════════════
# 18. Module-level helper functions (unit tests, no network)
# ══════════════════════════════════════════════════════════════════════════════

class TestModuleHelpers:
    def test_parse_args_dates(self):
        r = NseKit._parse_args(("01-01-2025", "31-03-2025"))
        assert r["from_date"] == "01-01-2025"
        assert r["to_date"]   == "31-03-2025"

    def test_parse_args_period(self):
        r = NseKit._parse_args(("1Y",))
        assert r["period"] == "1Y"

    def test_parse_args_symbol(self):
        r = NseKit._parse_args(("RELIANCE",))
        assert r["symbol"] == "RELIANCE"

    def test_parse_args_all(self):
        r = NseKit._parse_args(("RELIANCE", "01-01-2025", "31-03-2025"))
        assert r["symbol"]    == "RELIANCE"
        assert r["from_date"] == "01-01-2025"
        assert r["to_date"]   == "31-03-2025"

    def test_resolve_dates_period(self):
        fd, td = NseKit._resolve_dates(period="1M")
        today = datetime.now().strftime("%d-%m-%Y")
        assert td == today
        d = datetime.strptime(fd, "%d-%m-%Y")
        assert (datetime.now() - d).days in range(28, 32)

    def test_resolve_dates_ytd(self):
        fd, td = NseKit._resolve_dates(period="YTD")
        assert fd.endswith(f"-{datetime.now().year}")

    def test_resolve_dates_explicit(self):
        fd, td = NseKit._resolve_dates("01-01-2025", "31-03-2025")
        assert fd == "01-01-2025"
        assert td == "31-03-2025"

    def test_sort_dedup_dates_ascending(self):
        import pandas as pd
        df = pd.DataFrame({"Date": ["17-Jan-2025", "15-Jan-2025", "17-Jan-2025"]})
        result = NseKit._sort_dedup_dates(df, "Date", "%d-%b-%Y", ascending=True)
        assert len(result) == 2
        assert result["Date"].iloc[0] == "15-Jan-2025"

    def test_sort_dedup_dates_descending(self):
        import pandas as pd
        df = pd.DataFrame({"Date": ["15-Jan-2025", "17-Jan-2025", "15-Jan-2025"]})
        result = NseKit._sort_dedup_dates(df, "Date", "%d-%b-%Y", ascending=False)
        assert len(result) == 2
        assert result["Date"].iloc[0] == "17-Jan-2025"

    def test_csv_from_bytes_bom(self):
        raw = b"\xef\xbb\xbfName,Value\nA,1\nB,2\n"
        df  = NseKit._csv_from_bytes(raw)
        assert list(df.columns) == ["Name", "Value"]
        assert len(df) == 2

    def test_clean_replaces_nan(self):
        import numpy as np
        df = pd.DataFrame({"a": [1.0, float("nan"), float("inf")]})
        result = NseKit._clean(df)
        assert result["a"].iloc[1] is None
        assert result["a"].iloc[2] is None
