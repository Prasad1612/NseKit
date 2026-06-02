import requests
import pandas as pd
import time
from typing import Union, Optional

class NdtvProfit:
    """
    A class to fetch F&O data from NDTV Profit APIs.
    Inspired by the robustness of NseKit and user-friendly API design.
    """

    HEADER_MAP = {
        "symbol": "Symbol",
        "symbol-strike": "Symbol/Strike",
        "spot-price": "Spot Price",
        "underlying-value": "Underlying Price",
        "strike-price": "Strike Price",
        "type": "Type",
        "expiry-date": "Expiry Date",
        "expiry": "Expiry",
        "premium": "Premium",
        "volumes": "Volume",
        "volume": "Volume",
        "volumes-in-contracts": "Volume (Contracts)",
        "open-interest": "OI",
        "open-interest-in-contracts": "OI (Contracts)",
        "open-interest-change": "OI Chg",
        "open-interest-change-percentage": "OI Chg %",
        "basis": "Basis",
        "1m-future": "1M Future",
        "2m-future": "2M Future",
        "roll-spread": "Roll Spread",
        "rollover": "Rollover",
        "roll-over-percentage": "Rollover %",
        "rollover-percentage": "Rollover %",
        "put-call-ratio": "PCR",
        "call-open-interest": "Call OI",
        "call-open-interest-change": "Call OI Chg",
        "call-open-interest-change-percentage": "Call OI Chg %",
        "put-open-interest": "Put OI",
        "put-open-interest-change": "Put OI Chg",
        "put-open-interest-change-percentage": "Put OI Chg %",
        "future-open-interest": "Future OI",
        "future-open-interest-change-percentage": "Future OI Chg %",
        "total-open-interest": "Total OI",
        "total-open-interest-change-percentage": "Total OI Chg %",
        "1m-open-interest-change-percentage": "1M OI Chg %",
        "pcr-open-interest-current": "PCR (OI)",
        "pcr-open-interest-previous": "PCR (OI Prev)",
        "pcr-open-interest-change": "PCR (OI Chg)",
        "pcr-volume-current": "PCR (Vol)",
        "pcr-volume-previous": "PCR (Vol Prev)",
        "pcr-volume-change": "PCR (Vol Chg)",
        "notional-turnover": "Notional Turnover",
        "premium-turnover": "Premium Turnover",
        "open-interest-turnover": "OI Turnover",
        "callOpenInterestPrev": "Call OI (Prev)",
        "putOpenInterestPrev": "Put OI (Prev)",
        "futureOpenInterestPrev": "Future OI (Prev)",
        "cmp": "CMP",
        "cmp-change": "Price Chg",
        "cmp-change-percentage": "Price Chg %",
        "future": "Future Price",
        "future-change": "Future Chg",
        "future-change-percent": "Future Chg %",
        "premium-discount": "Prem/Disc",
        "premium-discount-percentage": "Prem/Disc %",
        "accumulated-volume": "Total Volume",
        "bulk-deal-value": "Bulk Deal Value",
        "bulk-deal-volume": "Bulk Deal Volume",
        "bulk-deal-future": "Bulk Deal Future",
        "turnover": "Turnover",
        "sector": "Sector",
        "number-of-futures": "Futures Count",
        "price-up": "Price Up",
        "price-down": "Price Down",
        "price-unchanged": "Price Unchg",
        "open-interest-up": "OI Up",
        "open-interest-down": "OI Down",
        "open-interest-unchanged": "OI Unchg",
        "volume-up": "Vol Up",
        "volume-down": "Vol Down",
        "volume-unchanged": "Vol Unchg",
        "volume-change": "Vol Chg",
        "volume-change-percentage": "Vol Chg %",
        "nsecode": "NSE Code",
        "bsecode": "BSE Code",
        "COMPNAME": "Company Name",
        "STOCKID": "Stock ID",
        "ISIN": "ISIN",
        "price-change": "Price Chg",
        "price-change-percentage": "Price Chg %",
    }

    # Desired column order (Rearrangeable)
    COLUMN_ORDER = ["Symbol", "Symbol/Strike", "Type", "Expiry Date", "Strike Price", "Spot Price",
                    "CMP", "Price Chg %", "Premium", "Volume", "OI", "OI Chg %", "PCR", "Prem/Disc %", "Rollover %"]

    # Shared futures column order and exclude list (used by all futures methods)
    _FUTURES_COL_ORDER = ["Symbol", "Expiry Date", "Future Price", "Future Chg", "Future Chg %",
                          "Spot Price", "Prem/Disc", "Prem/Disc %", "Total Volume", "OI", "OI Chg",
                          "OI Chg %", "OI Turnover", "Turnover", "Rollover %"]
    _FUTURES_EXCLUDE   = ["Basis", "CMP", "Expiry", "Rollover", "Volume", "Price Chg", "Price Chg %",
                          "Bulk Deal Value", "Bulk Deal Volume", "Bulk Deal Future", "Vol Chg", "Vol Chg %"]

    # Shared options column order / exclude (used by active-volume and top-OI methods)
    _OPTIONS_COL_ORDER = ["Symbol", "Strike Price", "Type", "Symbol/Strike", "Expiry Date",
                          "Spot Price", "Premium", "Volume (Contracts)", "OI (Contracts)", "OI Chg %"]
    _OPTIONS_EXCLUDE   = ["Underlying Price"]

    def __init__(self, convert_to_cr: bool = False, filter_old_expiry: bool = False):
        self.base_url = "https://www.ndtvprofit.com/api/v2"
        self.convert_to_cr = convert_to_cr
        self.filter_old_expiry = filter_old_expiry
        self.session = requests.Session()
        self._initialize_session()

    def _initialize_session(self):
        """Initialize session with browser-like headers to avoid blocking."""
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.ndtvprofit.com/markets/option-chain",
            "Origin": "https://www.ndtvprofit.com/markets",
            "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Requested-With": "XMLHttpRequest"
        })
        try:
            self.session.get("https://www.ndtvprofit.com/markets", timeout=10)
            time.sleep(0.5)
            self.session.get("https://www.ndtvprofit.com/markets/option-chain", timeout=10)
        except:
            pass

    def _get_params(self, type_str: str = None, instrument_str: str = None, order_str: str = None) -> dict:
        """Helper to convert string arguments to API parameter values."""
        type_map  = {"call": 1, "put": 2, "premium": 1, "discount": 0}
        inst_map  = {"index": 1, "stock": 2}
        order_map = {"up": 1, "down": 0, "highest": 1, "lowest": 0}

        params = {}
        if type_str:
            params["type"] = type_map.get(type_str.lower(), type_str)
            if type_str.lower() in ("premium", "discount"):
                params["premium"] = type_map[type_str.lower()]
        if instrument_str:
            params["instrument"] = inst_map.get(instrument_str.lower(), instrument_str)
        if order_str:
            params["order"] = order_map.get(order_str.lower(), order_str)
        return params

    def _ok(self, json_data: dict) -> bool:
        """Check if the v2 API response is successful."""
        return json_data.get("responseCode") == 200 and "data" in json_data

    def _fetch(self, url: str, params: dict = None, *, output: str, col_order: list,
               exclude: list, error_msg: str, empty,
               convert_to_cr: Optional[bool] = None,
               filter_old_expiry: Optional[bool] = None):
        """
        Single shared fetch-and-format helper used by every public method.
        Eliminates the repeated try/get/raise/json/_ok/_format_output/except/return pattern.
        """
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            json_data = response.json()
            if self._ok(json_data):
                return self._format_output(
                    json_data["data"], output,
                    column_order=col_order, exclude=exclude,
                    convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry
                )
        except Exception as e:
            print(f"{error_msg}: {e}")
        return empty

    def _format_output(self, data: Union[list, dict], output: str, header_map: dict = None,
                       column_order: list = None, exclude: list = None,
                       convert_to_cr: Optional[bool] = None,
                       filter_old_expiry: Optional[bool] = None) -> Union[pd.DataFrame, dict, list]:
        """Format the API response into either a DataFrame or JSON (dict/list)."""
        convert    = convert_to_cr    if convert_to_cr    is not None else self.convert_to_cr
        filter_exp = filter_old_expiry if filter_old_expiry is not None else self.filter_old_expiry

        if output.lower() != "dataframe":
            return data

        if isinstance(data, list):
            df = pd.DataFrame([item for item in data if item is not None])
        elif isinstance(data, dict):
            df = pd.DataFrame([data])
        else:
            return pd.DataFrame()

        if df.empty:
            return df

        # Filter old expiry dates
        if filter_exp and "expiry-date" in df.columns:
            try:
                today = pd.Timestamp.now().normalize()
                df["temp_expiry"] = pd.to_datetime(df["expiry-date"])
                df = df[df["temp_expiry"] >= today].copy()
                df.drop(columns=["temp_expiry"], inplace=True)
            except Exception as e:
                print(f"Error filtering expiry dates: {e}")

        # Conversion to Crores
        if convert:
            for col in ("notional-turnover", "premium-turnover", "open-interest-turnover", "turnover"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce") / 10_000_000

        # Rename columns
        df.rename(columns=header_map if header_map is not None else self.HEADER_MAP, inplace=True)

        # Reorder: priority columns first, then the rest
        order = column_order if column_order is not None else self.COLUMN_ORDER
        all_cols      = list(df.columns)
        priority_cols = [c for c in order if c in all_cols]
        other_cols    = [c for c in all_cols if c not in priority_cols]
        df = df[priority_cols + other_cols]

        # Drop excluded columns
        if exclude:
            df.drop(columns=[c for c in exclude if c in df.columns], inplace=True, errors="ignore")

        return df

    # --------------- General ---------------

    def get_nifty_summary(self, index_symbol: str = "NIFTY 50", output: str = "dataframe", exclude: list = None) -> Union[pd.DataFrame, dict]:
        """
        Fetch Future summary for a given index.
        Example: api.get_nifty_summary("NIFTY 50")
        """
        col_order = ["Symbol", "Spot Price", "Basis", "1M Future", "2M Future", "Roll Spread", "Rollover %", "OI", "OI Chg %", "PCR"]
        try:
            response = self.session.get(f"{self.base_url}/stock-summary", params={"symbol": index_symbol}, timeout=10)
            response.raise_for_status()
            json_data = response.json()
            if self._ok(json_data):
                data = json_data["data"]
                if isinstance(data, dict) and "symbol" not in data:
                    data["symbol"] = index_symbol          # v2 omits the symbol key
                return self._format_output(data, output, column_order=col_order, exclude=exclude)
        except Exception as e:
            print(f"Error fetching Nifty Summary: {e}")
        return pd.DataFrame() if output == "dataframe" else {}

    def get_stock_details(self, mode: str = "All", output: str = "dataframe", exclude: list = None) -> Union[pd.DataFrame, list]:
        """
        Fetch Stock Details.
        mode: "All" → full dataset | "Nse" → only rows where nsecode is not blank
        """
        try:
            response = self.session.get(f"{self.base_url}/stock-search", timeout=10)
            response.raise_for_status()
            df = pd.DataFrame(response.json())

            if mode.lower() == "nse":
                df = df[df["nsecode"].notna() & (df["nsecode"].str.strip() != "")]

            df = df.rename(columns={"COMPNAME": "Company Name", "nsecode": "NSE Code",
                                    "bsecode": "BSE Code", "ISIN": "ISIN", "STOCKID": "Stock ID"})
            df = df[["Company Name", "NSE Code", "BSE Code", "ISIN", "Stock ID"]]

            if exclude:
                df.drop(columns=[c for c in exclude if c in df.columns], inplace=True, errors="ignore")
            return df if output == "dataframe" else df.to_dict(orient="records")

        except Exception as e:
            print(f"Error fetching Stock Details: {e}")
        return pd.DataFrame() if output == "dataframe" else []

    # --------------- Options ---------------

    def get_most_active_options_by_volume(self, type: str = "call", instrument: str = "stock", limit: int = 200,
                                          output: str = "dataframe", exclude: list = None,
                                          convert_to_cr: Optional[bool] = None, filter_old_expiry: Optional[bool] = None) -> Union[pd.DataFrame, list]:
        """
        Fetch Most Active Options by Volume.
        Example: api.get_most_active_options_by_volume("call", "stock", limit=5)
        """
        p = self._get_params(type_str=type, instrument_str=instrument)
        return self._fetch(
            f"{self.base_url}/active-index-stock-options",
            {"type": p["type"], "instrument": p["instrument"], "limit": limit},
            output=output, col_order=self._OPTIONS_COL_ORDER,
            exclude=(exclude or []) + self._OPTIONS_EXCLUDE,
            error_msg="Error fetching Options by Volume",
            empty=pd.DataFrame() if output == "dataframe" else [],
            convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry,
        )

    def get_top_open_interest(self, type: str = "call", instrument: str = "stock", limit: int = 200,
                              output: str = "dataframe", exclude: list = None,
                              convert_to_cr: Optional[bool] = None, filter_old_expiry: Optional[bool] = None) -> Union[pd.DataFrame, list]:
        """
        Fetch Top Open Interest Options.
        Example: api.get_top_open_interest("call", "stock", limit=5)
        """
        p = self._get_params(type_str=type, instrument_str=instrument)
        return self._fetch(
            f"{self.base_url}/open-interest-index-stock-options",
            {"type": p["type"], "instrument": p["instrument"], "limit": limit},
            output=output, col_order=self._OPTIONS_COL_ORDER,
            exclude=(exclude or []) + self._OPTIONS_EXCLUDE,
            error_msg="Error fetching Options by OI",
            empty=pd.DataFrame() if output == "dataframe" else [],
            convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry,
        )

    def get_oi_breakup(self, instrument: str = "stock", limit: int = 200, last_expiry: bool = False,
                       output: str = "dataframe", exclude: list = None,
                       convert_to_cr: Optional[bool] = None, filter_old_expiry: Optional[bool] = None) -> Union[pd.DataFrame, list]:
        """
        Fetch F&O Open Interest Break-up.
        Example: api.get_oi_breakup("stock", last_expiry=False)
        """
        p = self._get_params(instrument_str=instrument)
        col_order = ["Symbol/Strike", "Future OI", "Future OI Chg %", "Call OI", "Call OI Chg %",
                     "Put OI", "Put OI Chg %", "Total OI", "Total OI Chg %", "1M OI Chg %", "Rollover %"]
        return self._fetch(
            f"{self.base_url}/total-fo-index-stock-options",
            {"instrument": p["instrument"], "limit": limit, "lastExpiry": str(last_expiry).lower()},
            output=output, col_order=col_order,
            exclude=(exclude or []) + ["Call OI (Prev)", "Put OI (Prev)", "Future OI (Prev)", "Spot Price"],
            error_msg="Error fetching F&O OI Breakup",
            empty=pd.DataFrame() if output == "dataframe" else [],
            convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry,
        )

    def get_oi_change_since_last_expiry(self, instrument: str = "stock", limit: int = 200, last_expiry: bool = True,
                                        output: str = "dataframe", exclude: list = None,
                                        convert_to_cr: Optional[bool] = None, filter_old_expiry: Optional[bool] = None) -> Union[pd.DataFrame, list]:
        """
        Fetch F&O Open Interest Change Since Last Expiry.
        Example: api.get_oi_change_since_last_expiry("stock", last_expiry=True)
        """
        p = self._get_params(instrument_str=instrument)
        col_order = ["Symbol/Strike", "Future OI", "Future OI Chg %", "Call OI", "Call OI Chg %",
                     "Put OI", "Put OI Chg %", "Total OI", "Total OI Chg %"]
        return self._fetch(
            f"{self.base_url}/total-fo-index-stock-options",
            {"instrument": p["instrument"], "limit": limit, "lastExpiry": str(last_expiry).lower()},
            output=output, col_order=col_order,
            exclude=(exclude or []) + ["Call OI (Prev)", "Put OI (Prev)", "Future OI (Prev)",
                                       "Spot Price", "1M OI Chg %", "Rollover %"],
            error_msg="Error fetching F&O OI Change Since Last Expiry",
            empty=pd.DataFrame() if output == "dataframe" else [],
            convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry,
        )

    def get_pcr_data(self, instrument: str = "stock", limit: int = 200, output: str = "dataframe", exclude: list = None,
                     convert_to_cr: Optional[bool] = None, filter_old_expiry: Optional[bool] = None) -> Union[pd.DataFrame, list]:
        """
        Fetch Put Call Ratio data.
        Example: api.get_pcr_data("stock")
        """
        p = self._get_params(instrument_str=instrument)
        col_order = ["Symbol", "Spot Price", "Call OI", "Call OI Chg", "Put OI", "Put OI Chg",
                     "PCR (OI)", "PCR (OI Prev)", "PCR (OI Chg)", "PCR (Vol)", "PCR (Vol Prev)", "PCR (Vol Chg)"]
        return self._fetch(
            f"{self.base_url}/put-call-ratio",
            {"instrument": p["instrument"], "limit": limit},
            output=output, col_order=col_order,
            exclude=(exclude or []) + ["Call OI Chg %", "Put OI Chg %"],
            error_msg="Error fetching PCR Data",
            empty=pd.DataFrame() if output == "dataframe" else [],
            convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry,
        )

    # --------------- Futures ---------------

    def _futures_fetch(self, url: str, params: dict, extra_exclude: list = None, *,
                       output: str, error_msg: str,
                       convert_to_cr: Optional[bool], filter_old_expiry: Optional[bool],
                       user_exclude: list = None) -> Union[pd.DataFrame, list]:
        """Shared fetch for all futures endpoints (same col_order & base exclude list)."""
        exclude = self._FUTURES_EXCLUDE + (extra_exclude or []) + (user_exclude or [])
        return self._fetch(url, params, output=output, col_order=self._FUTURES_COL_ORDER,
                           exclude=exclude, error_msg=error_msg,
                           empty=pd.DataFrame() if output == "dataframe" else [],
                           convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry)

    def get_future_by_oi(self, order: str = "up", limit: int = 200, output: str = "dataframe",
                         exclude: list = None, convert_to_cr: Optional[bool] = None,
                         filter_old_expiry: Optional[bool] = None) -> Union[pd.DataFrame, list]:
        """
        Fetch Future Open Interest Gainers or Losers.
        Example: api.get_future_by_oi("up") or api.get_future_by_oi("down")
        """
        p = self._get_params(order_str=order)
        return self._futures_fetch(
            f"{self.base_url}/open-interest-up-down", {"order": p["order"], "limit": limit},
            output=output, error_msg="Error fetching Future by OI",
            convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry, user_exclude=exclude,
        )

    def get_future_by_premium_discount(self, type: str = "premium", limit: int = 200,
                                       output: str = "dataframe", exclude: list = None,
                                       convert_to_cr: Optional[bool] = None,
                                       filter_old_expiry: Optional[bool] = None) -> Union[pd.DataFrame, list]:
        """
        Fetch Future by Premium or Discount.
        Example: api.get_future_by_premium_discount("premium")
        """
        p = self._get_params(type_str=type)
        return self._futures_fetch(
            f"{self.base_url}/greatest-premium-discount", {"premium": p["premium"], "limit": limit, "order": 1},
            output=output, error_msg="Error fetching Future by Premium/Discount",
            convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry, user_exclude=exclude,
        )

    def get_future_by_rollover(self, order: str = "highest", limit: int = 200,
                               output: str = "dataframe", exclude: list = None,
                               convert_to_cr: Optional[bool] = None,
                               filter_old_expiry: Optional[bool] = None) -> Union[pd.DataFrame, list]:
        """
        Fetch Future by Rollover percentage.
        Example: api.get_future_by_rollover("highest") or api.get_future_by_rollover("lowest")
        """
        p = self._get_params(order_str=order)
        return self._futures_fetch(
            f"{self.base_url}/highest-lowest-future-rollover", {"order": p["order"], "limit": limit},
            output=output, error_msg="Error fetching Future by Rollover",
            convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry, user_exclude=exclude,
        )

    def get_buildups(self, buildup_type: str = "long_buildup", limit: int = 200,
                     output: str = "dataframe", exclude: list = None,
                     convert_to_cr: Optional[bool] = None,
                     filter_old_expiry: Optional[bool] = None) -> Union[pd.DataFrame, list]:
        """
        Fetch Hedging/Buildup status.
        buildup_type: "long_buildup" | "long_unwinding" | "short_covering" | "short_buildup"
        """
        build_map = {
            "long_buildup":   {"price": 1, "oi": 1, "order": 1},
            "long_unwinding": {"price": 0, "oi": 0, "order": 0},
            "short_covering": {"price": 1, "oi": 0, "order": 0},
            "short_buildup":  {"price": 0, "oi": 1, "order": 1},
        }
        params = {**build_map.get(buildup_type.lower(), build_map["long_buildup"]), "limit": limit}
        return self._futures_fetch(
            f"{self.base_url}/short-covering", params,
            output=output, error_msg="Error fetching Buildups",
            convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry, user_exclude=exclude,
        )

    def get_future_active_volume(self, show_large_change: bool = False, limit: int = 200,
                                 output: str = "dataframe", exclude: list = None,
                                 convert_to_cr: Optional[bool] = None,
                                 filter_old_expiry: Optional[bool] = None) -> Union[pd.DataFrame, list]:
        """
        Fetch Most Active Futures by Volume or Large Volume Change.
        """
        return self._futures_fetch(
            f"{self.base_url}/most-active-contracts",
            {"byChange": int(show_large_change), "order": 1, "limit": limit},
            output=output, error_msg="Error fetching Future Volume",
            convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry, user_exclude=exclude,
        )

    # def get_sectoral_movement(self, output: str = "dataframe", exclude: list = None) -> Union[pd.DataFrame, list]:
    #     """
    #     NOTE: Removed in NDTV Profit API v2 — endpoint discontinued.
    #     """
    #     print("Warning: get_sectoral_movement() is not available in the new NDTV Profit API v2. "
    #           "The /future/getSectoralMovement endpoint has been discontinued.")
    #     return pd.DataFrame() if output == "dataframe" else []

    def get_sectoral_movement(self, output: str = "dataframe", exclude: list = None) -> Union[pd.DataFrame, list]:
        """
        Fetch Sectoral Movement data.
        """
        url = f"{self.base_url}/future/getSectoralMovement"
        
        col_order = ["Sector", "Futures Count", "Price Up", "Price Down", "Price Unchg", "OI Up", "OI Down", "OI Unchg", "Vol Up", "Vol Down", "Vol Unchg"]
        exclude   = ["Price Chg", "Price Chg %", "OI Chg", "OI Chg %", "Vol Chg", "Vol Chg %","Type"]            
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            json_data = response.json()
            if json_data.get("status") and "data" in json_data:
                return self._format_output(json_data["data"], output, column_order=col_order, exclude=exclude)
        except Exception as e:
            print(f"Error fetching Sectoral Movement: {e}")
        return pd.DataFrame() if output == "dataframe" else []