import requests
import pandas as pd
import numpy as np
import re
import random
import feedparser
import warnings
import csv
import json
import zipfile
import time
from datetime import datetime, timedelta, time as dt_time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import StringIO, BytesIO
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from rich.text import Text

class Nse:
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
    ]

    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.nseindia.com/',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
            'Origin': 'https://www.nseindia.com'
        }
        self.rotate_user_agent()
        self._initialize_session()
        self.last_req_time = 0
        self.min_delay = 0.5  # Minimum delay between requests in seconds

    def _initialize_session(self):
        """Ultra-Fast Session Initializer with persistent cookie-chain."""
        try:
            # Stage 1: Main portal entry
            self.session.get("https://www.nseindia.com", headers=self.headers, timeout=5)
            # Stage 2: Deep cookie-link for live data
            self.session.get("https://www.nseindia.com/market-data/live-equity-market", headers=self.headers, timeout=5)
        except Exception: 
            pass # Self-heals on first call anyway

    def rotate_user_agent(self):
        self.headers['User-Agent'] = random.choice(self.USER_AGENTS)

    def _get(self, url, ref_url=None, params=None, is_json=True, timeout=8):
        """Unified GET request handler with Low-Latency Throttling."""
        t = time.time()
        elapsed = t - self.last_req_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)

        self.rotate_user_agent()
        try:
            if ref_url:
                # Optimized: Skip extra calls if cookies are fresh
                self.session.get(ref_url, headers=self.headers, timeout=timeout)
            
            resp = self.session.get(url, headers=self.headers, params=params, timeout=timeout)
            
            # Backoff & Recovery logic
            if resp.status_code in (401, 403):
                time.sleep(1.5)
                self._initialize_session()
                resp = self.session.get(url, headers=self.headers, params=params, timeout=timeout)
                
            resp.raise_for_status()
            self.last_req_time = time.time()
            return resp.json() if is_json else resp
        except Exception:
            return None

    def _to_df(self, data, key=None, columns=None, rename=None, fill_value=""):
        """Hyper-Speed JSON to DataFrame Transformer (Stage 3)."""
        if not data: return pd.DataFrame()
        
        # Navigate keys safely
        if key:
            keys = [key] if isinstance(key, (str, int)) else key
            for k in keys:
                if isinstance(data, dict): data = data.get(k, [])
                elif isinstance(data, list): break 
                else: break
        
        if not data: return pd.DataFrame()
        
        # Super-Fast vectorized construction
        df = pd.DataFrame(data if isinstance(data, list) else [data])
        if df.empty: return df

        # Filter & Rename in bulk (Vectorized)
        if columns:
            existing = [c for c in columns if c in df.columns]
            df = df[existing]
        if rename:
            df.rename(columns=rename, inplace=True)
            
        # Vectorized Mask-Purify (C-Speed)
        df = df.mask(df.isin([np.nan, np.inf, -np.inf, pd.NA, 'NaN', 'None', 'nan', 'null']), fill_value)
        return df.fillna(fill_value) if fill_value is not None else df

    def _parse_date(self, date_str, fmt="%d-%m-%Y"):
        try: return datetime.strptime(date_str, fmt) if date_str else datetime.now()
        except: return datetime.now()

    def _parse_args(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        """Vector-style Argument Detection."""
        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        periods = {"1D", "1W", "1M", "3M", "6M", "1Y"}
        
        for arg in args:
            if not isinstance(arg, str): continue
            U_arg = arg.upper()
            if date_pattern.match(arg):
                if not from_date: from_date = arg
                elif not to_date: to_date = arg
            elif U_arg in periods: period = U_arg
            else: symbol = U_arg
        
        now = datetime.now()
        if period:
            delta = {"1D": 1, "1W": 7, "1M": 30, "3M": 90, "6M": 180, "1Y": 365}[period]
            from_date = (now - timedelta(days=delta)).strftime("%d-%m-%Y")
            to_date = to_date or now.strftime("%d-%m-%Y")
        
        return symbol, from_date or (now-timedelta(days=365)).strftime("%d-%m-%Y"), to_date or now.strftime("%d-%m-%Y")

    def detect_excel_format(self, content_bytes):
        magic = content_bytes.read(8)
        content_bytes.seek(0)
        if magic.startswith(b'\xD0\xCF\x11\xE0'): return 'xls'
        if magic.startswith(b'\x50\x4B\x03\x04'):
            with zipfile.ZipFile(content_bytes) as z:
                content_bytes.seek(0)
                if any(f.endswith('.bin') for f in z.namelist()): return 'xlsb'
                return 'xlsx'
        return None


    def _get_csv_hist(self, url, ref_url, date_col="Date"):
        """Stage 3 Vectorized CSV Handler (C-Engine)."""
        resp = self._get(url, ref_url, is_json=False)
        if not resp: return None
        
        # Fast C-Engine Reading
        df = pd.read_csv(BytesIO(resp.content), engine='c', low_memory=False)
        if df.empty: return None
        
        # Batch Vectorized Cleaning
        df.columns = df.columns.str.strip().str.replace('"', '', regex=False)
        
        # Numeric Batching (Exclude Dates)
        num_cols = df.columns[df.columns != date_col]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '', regex=False).str.strip(), errors='coerce').fillna(df[col])
            
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            df = df.sort_values(date_col, ascending=False).reset_index(drop=True)
            df[date_col] = df[date_col].dt.strftime("%d-%b-%Y")
            
        return df.mask(df.isin([np.nan, np.inf, -np.inf, pd.NA]), "")


    #---------------------------------------------------------- NSE ----------------------------------------------------------------

    def nse_market_status(self, mode: str = "Market Status"):
        data = self._get('https://www.nseindia.com/api/marketStatus', 'https://www.nseindia.com/market-data/live-equity-market')
        if not data: return None
        
        mode = mode.strip().lower()
        
        if mode == "market status":
            df = self._to_df(data, "marketState")
            if not df.empty:
                keep_cols = ['market', 'marketStatus', 'tradeDate', 'index', 'last', 'variation', 'percentChange', 'marketStatusMessage']
                df = df[[c for c in keep_cols if c in df.columns]]
            return df
        elif mode == "mcap":
            df = self._to_df(data, "marketcap")
            if not df.empty:
                df.rename(columns={'timeStamp': 'Date', 'marketCapinTRDollars': 'MarketCap_USD_Trillion', 'marketCapinLACCRRupees': 'MarketCap_INR_LakhCr', 'marketCapinCRRupees': 'MarketCap_INR_Cr'}, inplace=True)
            return df
        elif mode == "nifty50":
            df = self._to_df(data, "indicativenifty50")
            if not df.empty:
                df.rename(columns={'dateTime': 'DateTime', 'indexName': 'Index', 'closingValue': 'ClosingValue', 'finalClosingValue': 'FinalClose', 'change': 'Change', 'perChange': 'PercentChange'}, inplace=True)
            return df
        elif mode == "gift nifty":
            data = self._get('https://www.nseindia.com/api/NextApi/apiClient?functionName=getGiftNifty', 'https://www.nseindia.com/')
            if data and "data" in data:
                gift_nifty_recs = data["data"].get("giftNifty", {})
                usd_inr_recs = data["data"].get("usdInr", {})
                combined_data = {**gift_nifty_recs, **{f"usdInr_{k}": v for k, v in usd_inr_recs.items()}}
                return pd.DataFrame([combined_data])
            return pd.DataFrame()
        elif mode == "all":
            return {
                "Market Status": self.nse_market_status("Market Status"),
                "Mcap": self.nse_market_status("Mcap"),
                "Nifty50": self.nse_market_status("Nifty50"),
                "Gift Nifty": self.nse_market_status("Gift Nifty")
            }
        return None

    def nse_is_market_open(self, market: str = "Capital Market") -> Text:
        data = self._get("https://www.nseindia.com/api/marketStatus", "https://www.nseindia.com/market-data/live-equity-market")
        if not data: return Text(f"Error fetching NSE Market Status", style="bold red")
        
        selected = next((m for m in data.get("marketState", []) if m.get("market") == market), None)
        if not selected: return Text(f"[{market}] → Market data not found.", style="bold yellow")

        message = selected.get('marketStatusMessage', '').strip()
        text = Text(f"[{market}] → ", style="bold white")
        style = "bold red" if any(w in message.lower() for w in ["closed", "halted", "suspended"]) else "bold green"
        text.append(message, style=style)
        return text


    def nse_trading_holidays(self, list_only=False):
        data = self._get("https://www.nseindia.com/api/holiday-master?type=trading")
        df = self._to_df(data, "CM", ["Sr_no", "tradingDate", "weekDay", "description", "morning_session", "evening_session"])
        return df["tradingDate"].tolist() if list_only and not df.empty else (df if not df.empty else None)

    def nse_clearing_holidays(self, list_only=False):
        data = self._get("https://www.nseindia.com/api/holiday-master?type=clearing")
        df = self._to_df(data, "CD", ["Sr_no", "tradingDate", "weekDay", "description", "morning_session", "evening_session"])
        return df["tradingDate"].tolist() if list_only and not df.empty else (df if not df.empty else None)
    
    def is_nse_trading_holiday(self, date_str=None):
        holidays = self.nse_trading_holidays(list_only=True)
        if holidays is None: return None
        try:
            d = datetime.strptime(date_str, "%d-%b-%Y") if date_str else datetime.today()
            return d.strftime("%d-%b-%Y") in holidays
        except: return None

    def is_nse_clearing_holiday(self, date_str=None):
        holidays = self.nse_clearing_holidays(list_only=True)
        if holidays is None: return None
        try:
            d = datetime.strptime(date_str, "%d-%b-%Y") if date_str else datetime.today()
            return d.strftime("%d-%b-%Y") in holidays
        except: return None

    def nse_live_market_turnover(self):
        """Vectorized Market Turnover summary."""
        data = self._get('https://www.nseindia.com/api/NextApi/apiClient?functionName=getMarketTurnoverSummary', 'https://www.nseindia.com/')
        if not data or 'data' not in data: return None
        
        # Super-Fast List Comprehension for nested data
        all_recs = [
            {
                "Segment": seg.upper(), "Product": i.get("instrument", ""), 
                "Vol (Shares/Contracts)": i.get("volume", 0),
                "Value (₹ Cr)": round(i.get("value", 0)/1e7, 2), "OI (Contracts)": i.get("oivalue", 0),
                "No. of Orders#": i.get("noOfOrders", 0), "No. of Trades": i.get("noOfTrades", 0),
                "Avg Trade Value (₹)": i.get("averageTrade", 0), "Updated At": i.get("mktTimeStamp", ""),
                "Prev Vol": i.get("prevVolume", 0), "Prev Value (₹ Cr)": round(i.get("prevValue", 0)/1e7, 2),
                "prev OI (Contracts)": i.get("prevOivalue", 0), "prev Orders#": i.get("prevNoOfOrders", 0),
                "prev Trades": i.get("prevNoOfTrades", 0), "prev Avg Trade Value (₹)": i.get("prevAverageTrade", 0)
            }
            for seg, recs in data['data'].items() if isinstance(recs, list) for i in recs
        ]
        return self._to_df(all_recs, fill_value=None)

    def nse_live_hist_circulars(self, from_date_str: str = None, to_date_str: str = None, filter: str = None):
        fd = from_date_str or (datetime.now() - timedelta(1)).strftime("%d-%m-%Y")
        td = to_date_str or datetime.now().strftime("%d-%m-%Y")
        data = self._get(f"https://www.nseindia.com/api/circulars?&fromDate={fd}&toDate={td}", 'https://www.nseindia.com/resources/exchange-communication-circulars')
        
        # Legacy Format: Date, Circulars No, Category, Department, Subject, Attachment
        cols = {"cirDisplayDate": "Date", "circDisplayNo": "Circulars No", "circCategory": "Category", "circDepartment": "Department", "sub": "Subject", "circFilelink": "Attachment"}
        df = self._to_df(data, "data", rename=cols)
        
        if not df.empty:
            if filter: df = df[df['Department'].str.contains(filter, case=False, na=False)]
            # Match Exactly column list and order
            final_cols = ["Date", "Circulars No", "Category", "Department", "Subject", "Attachment"]
            df = df[[c for c in final_cols if c in df.columns]]
            
        return df if not df.empty else pd.DataFrame(columns=["Date", "Circulars No", "Category", "Department", "Subject", "Attachment"])

    def nse_live_hist_press_releases(self, from_date_str: str = None, to_date_str: str = None, filter: str = None):
        """Vectorized Press Release fetcher with legacy formatting."""
        fd, td = from_date_str or (datetime.now() - timedelta(1)).strftime("%d-%m-%Y"), to_date_str or datetime.now().strftime("%d-%m-%Y")
        data = self._get(f"https://www.nseindia.com/api/press-release-cms20?fromDate={fd}&toDate={td}", 'https://www.nseindia.com/resources/exchange-communication-press-releases')
        if not data: return pd.DataFrame(columns=["DATE", "DEPARTMENT", "SUBJECT", "ATTACHMENT URL", "LAST UPDATED"])
        
        def clean_html(html):
            if not html or '<' not in html: return html
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
                return BeautifulSoup(html, "html.parser").get_text(separator=' ').strip()

        def format_changed(raw):
            try:
                ts = datetime.strptime(raw, "%a, %m/%d/%Y - %H:%M")
                return ts.strftime("%a %d-%b-%Y %I:%M %p")
            except: return raw

        res = [
            {
                "DATE": i.get('content', {}).get('field_date', ''), 
                "SUBJECT": clean_html(i.get('content', {}).get('body', '')),
                "DEPARTMENT": i.get('content', {}).get('field_type', ''), 
                "ATTACHMENT URL": (i.get('content', {}).get('field_file_attachement') or {}).get('url'),
                "LAST UPDATED": format_changed(i.get('changed', ''))
            }
            for i in data
        ]
        df = self._to_df(res)
        if not df.empty:
            if filter: df = df[df['DEPARTMENT'].str.contains(filter, case=False, na=False)]
            df = df[["DATE", "DEPARTMENT", "SUBJECT", "ATTACHMENT URL", "LAST UPDATED"]]
        return df
 
    def nse_reference_rates(self):
        data = self._get('https://www.nseindia.com/api/NextApi/apiClient?functionName=getReferenceRates&&type=null&&flag=CUR', 'https://www.nseindia.com')
        df = self._to_df(data.get('data', {}).get('currencySpotRates', [])) if data else None
        if df is not None:
            columns = ['currency', 'unit', 'value', 'prevDayValue']
            df = df[[c for c in columns if c in df.columns]]
            df = df.fillna(0).replace({float('inf'): 0, float('-inf'): 0})
        return df

    def nse_eod_top10_nifty50(self, trade_date: str):
        url = f"https://nsearchives.nseindia.com/content/indices/top10nifty50_{self._parse_date(trade_date, '%d-%m-%y').strftime('%d%m%y').upper()}.csv"
        resp = self._get(url, is_json=False)
        return pd.read_csv(BytesIO(resp.content)) if resp else None

    def nse_6m_nifty_50(self, list_only=False):
        df = self._get_csv_hist('https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv', 'https://www.nseindia.com')
        if df is None: return None
        if not df.empty:
            # Match legacy NseKit_old.py exactly: Keep spaces in column names
            df.columns = df.columns.str.strip()
            df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
            keep_cols = ['Company Name', 'Industry', 'Symbol', 'Series', 'ISIN Code']
            df = df[[c for c in keep_cols if c in df.columns]]
            
        if list_only: return df["Symbol"].tolist() if "Symbol" in df.columns else []
        return df
    
    def nse_eom_fno_full_list(self, mode="stocks", list_only=False):
        """Fetch NSE End-of-Month (EoM) F&O Full List using JSON API (100% Legacy Parity)."""
        data = self._get("https://www.nseindia.com/api/underlying-information", "https://www.nseindia.com/products-services/equity-derivatives-list-underlyings-information")
        if not data: return None
        
        mode = mode.strip().lower()
        key = "IndexList" if mode == "index" else "UnderlyingList"
        
        df = self._to_df(data, ["data", key], 
                         rename={"serialNumber": "Serial Number", "symbol": "Symbol", "underlying": "Underlying"})
        
        if list_only:
            return df["Symbol"].tolist() if not df.empty and "Symbol" in df.columns else []
            
        return df[["Serial Number", "Symbol", "Underlying"]] if not df.empty else None

    def nse_6m_nifty_500(self, list_only=False):
        df = self._get_csv_hist('https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv', 'https://www.nseindia.com')
        if df is None: return None
        if not df.empty:
            # Match legacy NseKit_old.py exactly: Keep spaces in column names
            df.columns = df.columns.str.strip()
            df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
            keep_cols = ['Company Name', 'Industry', 'Symbol', 'Series', 'ISIN Code']
            df = df[[c for c in keep_cols if c in df.columns]]
            
        if list_only: return df["Symbol"].tolist() if "Symbol" in df.columns else []
        return df

    def nse_eod_equity_full_list(self, list_only=False):
        df = self._get_csv_hist('https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv', 'https://www.nseindia.com')
        if df is None: return None
        if not df.empty:
            # Match legacy NseKit_old.py exactly: Restore exact spacing in column names
            # Legacy uses: ['SYMBOL', 'NAME OF COMPANY', ' SERIES', ' DATE OF LISTING', ' FACE VALUE']
            # _get_csv_hist strips by default, so we restore the leading space for specific columns.
            rename_map = {
                "SERIES": " SERIES",
                "DATE OF LISTING": " DATE OF LISTING",
                "FACE VALUE": " FACE VALUE"
            }
            df = df.rename(columns=rename_map)
            keep_cols = ['SYMBOL', 'NAME OF COMPANY', ' SERIES', ' DATE OF LISTING', ' FACE VALUE']
            df = df[[c for c in keep_cols if c in df.columns]]
            
        if list_only: return df["SYMBOL"].tolist() if "SYMBOL" in df.columns else []
        return df
        

    def state_wise_registered_investors(self):
        return self._get("https://www.nseindia.com/api/registered-investors", 'https://www.nseindia.com/registered-investors/')

    #---------------------------------------------------------- IPO ----------------------------------------------------------------

    def ipo_current(self):
        """Vectorized Current IPOs matching legacyExactly."""
        data = self._get('https://www.nseindia.com/api/ipo-current-issue', 'https://www.nseindia.com/market-data/all-upcoming-issues-ipo')
        if not (data and isinstance(data, list)): return None
        df = pd.DataFrame(data)
        if df.empty: return None
        cols = ['symbol', 'companyName', 'series', 'issueStartDate', 'issueEndDate', 'status', 'issueSize', 'issuePrice', 'noOfSharesOffered', 'noOfsharesBid', 'noOfTime']
        try: 
            df = df[cols].fillna(0).replace({np.inf: 0, -np.inf: 0})
            return df if not df.empty else None
        except: return None

    def ipo_preopen(self):
        """Vectorized Special Pre-Open Listing matching legacy exactly."""
        data = self._get('https://www.nseindia.com/api/special-preopen-listing', 'https://www.nseindia.com/market-data/new-stock-exchange-listings-today')
        if not data or 'data' not in data: return None
        recs = data['data']
        flat = []
        for i in recs:
            pb = i.get('preopenBook', {})
            pre = pb.get('preopen', [{}])[0] if pb.get('preopen') else {}
            ato = pb.get('ato', {})
            flat.append({
                'symbol': i.get('symbol', ''), 'series': i.get('series', ''), 'prevClose': i.get('prevClose', ''), 'iep': i.get('iep', ''),
                'change': i.get('change', ''), 'perChange': i.get('perChange', ''), 'ieq': i.get('ieq', ''), 'ieVal': i.get('ieVal', ''),
                'buyOrderCancCnt': i.get('buyOrderCancCnt', ''), 'buyOrderCancVol': i.get('buyOrderCancVol', ''),
                'sellOrderCancCnt': i.get('sellOrderCancVol', ''), 'sellOrderCancVol': i.get('sellOrderCancVol', ''),
                'isin': i.get('isin', ''), 'status': i.get('status', ''),
                'preopen_buyQty': pre.get('buyQty', 0), 'preopen_sellQty': pre.get('sellQty', 0),
                'ato_totalBuyQuantity': ato.get('totalBuyQuantity', 0), 'ato_totalSellQuantity': ato.get('totalSellQuantity', 0),
                'totalBuyQuantity': pb.get('totalBuyQuantity', 0), 'totalSellQuantity': pb.get('totalSellQuantity', 0),
                'totTradedQty': pb.get('totTradedQty', 0), 'lastUpdateTime': pb.get('lastUpdateTime', '')
            })
        return self._to_df(flat, fill_value=0)
        

    def ipo_tracker_summary(self, filter: str = None):
        """
        Fetch Year-To-Date IPO Tracker Summary from NSE India.

        Parameters
        ----------
        filter : str, optional
            Filter IPOs by 'MARKETTYPE' (e.g., "MAINBOARD", "SME").
            The filter is case-insensitive — 'mainboard' or 'sme' will also work.

        Returns
        -------
        pandas.DataFrame or None
            Cleaned IPO summary DataFrame, or None if no valid data found.
        """
        data = self._get('https://www.nseindia.com/api/NextApi/apiClient?functionName=getIPOTrackerSummary', 'https://www.nseindia.com/ipo-tracker?type=ipo_year')
        df = self._to_df(data, "data")
        if df.empty: return None

        # Step 3: Ensure MARKETTYPE uppercase for consistent comparison
        df["MARKETTYPE"] = df["MARKETTYPE"].str.upper().fillna("")

        # Step 4: Apply filter (case-insensitive)
        if filter:
            filter = filter.strip().upper()
            df = df[df["MARKETTYPE"].str.contains(filter, case=False, na=False)]

        # Step 5: Select & reorder columns
        keep_cols = [
            "SYMBOL", "COMPANYNAME", "LISTED_ON", "ISSUE_PRICE",
            "LISTED_DAY_CLOSE", "LISTED_DAY_GAIN", "LISTED_DAY_GAIN_PER",
            "LTP", "GAIN_LOSS", "GAIN_LOSS_PER", "MARKETTYPE"
        ]
        df = df[[col for col in keep_cols if col in df.columns]]

        # Step 6: Convert numerics
        num_cols = [
            "ISSUE_PRICE", "LISTED_DAY_CLOSE", "LISTED_DAY_GAIN",
            "LISTED_DAY_GAIN_PER", "LTP", "GAIN_LOSS", "GAIN_LOSS_PER"
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
    
        # Step 7: Convert LISTED_ON to datetime and sort latest first
        if "LISTED_ON" in df.columns:
            df["LISTED_ON"] = pd.to_datetime(df["LISTED_ON"], format="%d-%m-%Y", errors="coerce")
            df = df.sort_values(by="LISTED_ON", ascending=False)

        # Step 8: Convert datetime columns to string for export safety
        datetime_cols = df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns
        for col in datetime_cols:
            df[col] = df[col].dt.strftime("%Y-%m-%d")

        return df.reset_index(drop=True) if not df.empty else None


    #---------------------------------------------------------- Pre-Open Market ----------------------------------------------------------------

    def pre_market_nifty_info(self, category='All'):
        pre_market_xref = {"NIFTY 50": "NIFTY", "Nifty Bank": "BANKNIFTY", "Emerge": "SME", "Securities in F&O": "FO", "Others": "OTHERS", "All": "ALL"}
        data = self._get(f"https://www.nseindia.com/api/market-data-pre-open?key={pre_market_xref.get(category, 'ALL')}", 'https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market')
        if not data: return None
        
        nifty_status = data.get("niftyPreopenStatus", {})
        advances = data.get("advances", 0)
        declines = data.get("declines", 0)
        unchanged = data.get("unchanged", 0)
        timestamp = data.get("timestamp", "Unknown")

        df = pd.DataFrame([{
            "lastPrice": nifty_status.get("lastPrice", "N/A"),
            "change": nifty_status.get("change", "N/A"),
            "pChange": nifty_status.get("pChange", "N/A"),
            "advances": advances,
            "declines": declines,
            "unchanged": unchanged,
            "timestamp": timestamp
        }])
        return df

    def pre_market_all_nse_adv_dec_info(self, category='All'):
        pre_market_xref = {"NIFTY 50": "NIFTY", "Nifty Bank": "BANKNIFTY", "Emerge": "SME", "Securities in F&O": "FO", "Others": "OTHERS", "All": "ALL"}
        data = self._get(f"https://www.nseindia.com/api/market-data-pre-open?key={pre_market_xref.get(category, 'ALL')}", 'https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market')
        if not data: return None
        
        advances = data.get("advances", 0)
        declines = data.get("declines", 0)
        unchanged = data.get("unchanged", 0)
        timestamp = data.get("timestamp", "Unknown")

        df = pd.DataFrame([{
            "advances": advances,
            "declines": declines,
            "unchanged": unchanged,
            "timestamp": timestamp
        }])
        return df

    def pre_market_info(self, category='All'):
        """Full Vectorized Pre-Market Stock data."""
        pre_market_xref = {"NIFTY 50": "NIFTY", "Nifty Bank": "BANKNIFTY", "Emerge": "SME", "Securities in F&O": "FO", "Others": "OTHERS", "All": "ALL"}
        data = self._get(f"https://www.nseindia.com/api/market-data-pre-open?key={pre_market_xref.get(category, 'ALL')}", 'https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market')
        if not data or 'data' not in data: return None
        
        # Optimized mapping
        rows = [
            {
                "symbol": i["metadata"]["symbol"], "previousClose": i["metadata"]["previousClose"],
                "iep": i["metadata"]["iep"], "change": i["metadata"]["change"], "pChange": i["metadata"]["pChange"],
                "lastPrice": i["metadata"]["lastPrice"], "finalQuantity": i["metadata"]["finalQuantity"],
                "totalTurnover": i["metadata"]["totalTurnover"], "marketCap": i["metadata"]["marketCap"],
                "yearHigh": i["metadata"]["yearHigh"], "yearLow": i["metadata"]["yearLow"],
                "totalBuyQuantity": i["detail"]["preOpenMarket"]["totalBuyQuantity"],
                "totalSellQuantity": i["detail"]["preOpenMarket"]["totalSellQuantity"],
                "atoBuyQty": i["detail"]["preOpenMarket"]["atoBuyQty"],
                "atoSellQty": i["detail"]["preOpenMarket"]["atoSellQty"],
                "lastUpdateTime": i["detail"]["preOpenMarket"]["lastUpdateTime"]
            } for i in data["data"]
        ]
        return self._to_df(rows).set_index("symbol", drop=False)
        
    def pre_market_derivatives_info(self, category='Index Futures'):
        pre_market_xref = {"Index Futures": "FUTIDX", "Stock Futures": "FUTSTK"} 
        data = self._get(f"https://www.nseindia.com/api/market-data-pre-open-fno?key={pre_market_xref.get(category, 'FUTIDX')}", 'https://www.nseindia.com/market-data/pre-open-market-fno')
        if not data or 'data' not in data: return None
        
        rows = [
            {
                "symbol": i["metadata"]["symbol"], "expiryDate": i["metadata"]["expiryDate"],
                "previousClose": i["metadata"]["previousClose"], "iep": i["metadata"]["iep"],
                "change": i["metadata"]["change"], "pChange": i["metadata"]["pChange"],
                "lastPrice": i["metadata"]["lastPrice"], "finalQuantity": i["metadata"]["finalQuantity"],
                "totalTurnover": i["metadata"]["totalTurnover"],
                "totalBuyQuantity": i["detail"]["preOpenMarket"]["totalBuyQuantity"],
                "totalSellQuantity": i["detail"]["preOpenMarket"]["totalSellQuantity"],
                "atoBuyQty": i["detail"]["preOpenMarket"]["atoBuyQty"],
                "atoSellQty": i["detail"]["preOpenMarket"]["atoSellQty"],
                "lastUpdateTime": i["detail"]["preOpenMarket"]["lastUpdateTime"]
            } for i in data["data"]
        ]
        return self._to_df(rows).set_index("symbol", drop=False)


    #---------------------------------------------------------- Index_Live_Data ----------------------------------------------------------------
    
    def index_live_all_indices_data(self):
        data = self._get('https://www.nseindia.com/api/allIndices', 'https://www.nseindia.com/market-data/index-performances')
        df = self._to_df(data, "data")
        if df.empty: return None

        columns = ['key', 'index', 'indexSymbol', 'last', 'variation', 'percentChange', 'open', 'high', 'low',
                'previousClose', 'yearHigh', 'yearLow', 'pe', 'pb', 'dy', 'declines', 'advances', 'unchanged',
                'perChange30d', 'perChange365d', 'previousDayVal', 'oneWeekAgoVal', 'oneMonthAgoVal', 'oneYearAgoVal']
        
        df = df[[c for c in columns if c in df.columns]]
        df = df.fillna(0)
        df = df.replace({float('inf'): 0, float('-inf'): 0})
        return df

    def index_live_indices_stocks_data(self, category, list_only=False):
        """Ultra-Fast Index Stocks data with legacy formatting (set_index + None replacement)."""
        category = category.upper().replace('&', '%26').replace(' ', '%20')
        data = self._get(f"https://www.nseindia.com/api/equity-stockIndices?index={category}")
        df = self._to_df(data, "data", fill_value=None)
        if df is not None:
            # Set index to 'symbol' as per legacy NseKit_old.py
            if "symbol" in df.columns:
                df = df.set_index("symbol", drop=False)
            
            # Reorder columns as per legacy requirement
            column_order = [
                "symbol", "previousClose", "open", "dayHigh", "dayLow", "lastPrice", 
                "change", "pChange", "totalTradedVolume", "totalTradedValue", 
                "nearWKH", "nearWKL", "perChange30d", "perChange365d", "ffmc"
            ]
            column_order = [col for col in column_order if col in df.columns]
            df = df[column_order]
            
            # Ensure numeric columns are properly typed and NaN-free (None for legacy)
            for col in df.columns:
                if df[col].dtype in ['float64', 'float32']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').replace(np.nan, None)

            if list_only: return df["symbol"].tolist()
            return df
        return None   

    def index_live_nifty_50_returns(self):
        data = self._get('https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi?functionName=getIndicesReturn&&index=NIFTY%2050', 'https://www.nseindia.com')
        df = self._to_df(data, "data")
        if df is not None:
            columns = ['one_week_chng_per', 'one_month_chng_per', 'three_month_chng_per', 'six_month_chng_per', 'one_year_chng_per', 'two_year_chng_per', 'three_year_chng_per', 'five_year_chng_per']
            df = df[[c for c in columns if c in df.columns]]
            df = df.fillna(0).replace({float('inf'): 0, float('-inf'): 0})
        return df

    def index_live_contribution(self, *args, Index: str = "NIFTY 50", Mode: str = "First Five"):
        """
        Fetch index contribution data from NSE

        Valid Calls:
        -----------
        index_live_contribution()
        index_live_contribution("Full")
        index_live_contribution("NIFTY BANK")
        index_live_contribution("NIFTY BANK", "Full")
        index_live_contribution(Index="NIFTY IT", Mode="Full")
        """

        # ----------------------------------
        # Smart *args Resolver
        # ----------------------------------
        if len(args) == 1:
            if args[0] in ("First Five", "Full"):
                Mode = args[0]
            else:
                Index = args[0]

        elif len(args) == 2:
            Index, Mode = args

        elif len(args) > 2:
            raise ValueError("Max 2 positional arguments allowed")

        # ----------------------------------
        # Validation & Normalization
        # ----------------------------------
        Index = str(Index).upper()
        Mode  = str(Mode)

        if Mode not in ("First Five", "Full"):
            raise ValueError("Mode must be 'First Five' or 'Full'")

        index_encoded = Index.replace("&", "%26").replace(" ", "%20")

        # ----------------------------------
        # API URL Selection
        # ----------------------------------
        if Mode == "First Five":
            api_url = (
                "https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi"
                f"?functionName=getContributionData&index={index_encoded}&flag=0"
            )
        else:
            api_url = (
                "https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi"
                f"?functionName=getContributionData&index={index_encoded}&noofrecords=0&flag=1"
            )

        data = self._get(api_url, "https://www.nseindia.com")
        df = self._to_df(data, "data")
        if df.empty: return None

        columns = ['icSymbol', 'icSecurity', 'lastTradedPrice', 'changePer', 'isPositive', 'rnNegative', 'changePoints']
        
        df = df[[c for c in columns if c in df.columns]]
        df = df.fillna(0)
        df = df.replace({float('inf'): 0, float('-inf'): 0})
        return df

        
    #---------------------------------------------------------- Index_Eod_Data ----------------------------------------------------------------

    def index_eod_bhav_copy(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        url = f"https://nsearchives.nseindia.com/content/indices/ind_close_all_{str(trade_date.strftime('%d%m%Y').upper())}.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None
        
    def index_historical_data(self, index: str, *args, from_date=None, to_date=None, period=None):
        symbol, from_date, to_date = self._parse_args(index, *args, from_date=from_date, to_date=to_date, period=period)
        index_encoded = symbol.upper().replace(" ","%20")
        
        start_dt, end_dt = datetime.strptime(from_date, "%d-%m-%Y"), datetime.strptime(to_date, "%d-%m-%Y")
        all_data = []
        while start_dt <= end_dt:
            curr_end = min(start_dt + timedelta(days=89), end_dt)
            url = f"https://www.nseindia.com/api/historicalOR/indicesHistory?indexType={index_encoded}&from={start_dt.strftime('%d-%m-%Y')}&to={curr_end.strftime('%d-%m-%Y')}"
            data = self._get(url, "https://www.nseindia.com/reports-indices-historical-index-data")
            if data and "data" in data: all_data.extend(data["data"])
            start_dt = curr_end + timedelta(days=1)
            
        df = self._to_df(all_data, fill_value=None)
        if df.empty: return df
        
        column_map = {'EOD_TIMESTAMP':'Date', 'EOD_INDEX_NAME':'Index Name', 'EOD_OPEN_INDEX_VAL':'Open', 'EOD_HIGH_INDEX_VAL':'High', 'EOD_LOW_INDEX_VAL':'Low', 'EOD_CLOSE_INDEX_VAL':'Close', 'HIT_TRADED_QTY':'Shares Traded', 'HIT_TURN_OVER':'Turnover (₹ Cr)'}
        df = df.rename(columns=column_map)[list(column_map.values())]
        
        df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce")
        df = df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
        df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")
        
        return df.reset_index(drop=True)


    def index_pe_pb_div_historical_data(self, index: str, *args, from_date=None, to_date=None, period=None):
        """
        Fetch historical P/E, P/B, and Dividend Yield data for a given NSE index.
        Automatically splits requests into safe 89-day chunks to avoid API blocking.
        Handles YTD, MAX, and fixed period formats.

        Parameters
        ----------
        index : str
            Example: 'NIFTY 50', 'NIFTY BANK'
        from_date, to_date : str, optional
            In 'dd-mm-yyyy' format
        period : str, optional
            One of ['1D','1W','1M','3M','6M','1Y','2Y','5Y','10Y','YTD','MAX']
        """

        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        today = datetime.now()
        today_str = today.strftime("%d-%m-%Y")

        # --- Auto-detect arguments ---
        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                elif arg.upper() in [
                    '1D','1W','1M','3M','6M','1Y','2Y','5Y','10Y','YTD','MAX'
                ]:
                    period = arg.upper()

        # --- Period mapping ---
        delta_map = {
            "1D": timedelta(days=1),
            "1W": timedelta(weeks=1),
            "1M": timedelta(days=30),
            "3M": timedelta(days=90),
            "6M": timedelta(days=180),
            "1Y": timedelta(days=365),
            "2Y": timedelta(days=730),
            "5Y": timedelta(days=1825),
            "10Y": timedelta(days=3650),
        }

        if period:
            if period == "YTD":
                from_date = datetime(today.year, 1, 1).strftime("%d-%m-%Y")
                to_date = today_str
            elif period == "MAX":
                from_date = "01-01-2008"
                to_date = today_str
            else:
                delta = delta_map.get(period, timedelta(days=365))
                from_date = (today - delta).strftime("%d-%m-%Y")
                to_date = today_str

        from_date = from_date or (today - timedelta(days=365)).strftime("%d-%m-%Y")
        to_date = to_date or today_str

        ref_url = "https://www.nseindia.com/reports-indices-yield"
        base_api = "https://www.nseindia.com/api/historicalOR/indicesYield?indexType={}&from={}&to={}"

        index_encoded = index.replace(" ", "%20").upper()

        # --- Start Session ---
        self.rotate_user_agent()
        try:
            ref_resp = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_resp.raise_for_status()
            cookies = ref_resp.cookies.get_dict()
        except Exception as e:
            print(f"❌ NSE session initialization failed: {e}")
            return pd.DataFrame()

        start_dt = datetime.strptime(from_date, "%d-%m-%Y")
        end_dt = datetime.strptime(to_date, "%d-%m-%Y")
        all_data = []
        chunk_days = 89
        max_retries = 3
        fail_chunks = []

        # --- Data Fetch Loop ---
        while start_dt <= end_dt:
            chunk_start = start_dt
            chunk_end = min(start_dt + timedelta(days=chunk_days), end_dt)
            api_url = base_api.format(
                index_encoded,
                chunk_start.strftime("%d-%m-%Y"),
                chunk_end.strftime("%d-%m-%Y"),
            )

            success = False
            for attempt in range(1, max_retries + 1):
                try:
                    response = self.session.get(
                        api_url, headers=self.headers, cookies=cookies, timeout=15 + attempt * 5
                    )
                    if response.status_code == 200:
                        data = response.json()
                        if "data" in data and isinstance(data["data"], list):
                            all_data.extend(data["data"])
                        success = True
                        break
                    elif response.status_code == 429:
                        time.sleep(random.uniform(8, 12))
                    else:
                        time.sleep(random.uniform(2, 4))
                except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
                    time.sleep(random.uniform(3, 6))
                except Exception:
                    time.sleep(random.uniform(3, 6))

            if not success:
                fail_chunks.append(f"{chunk_start.strftime('%d-%b-%Y')} → {chunk_end.strftime('%d-%b-%Y')}")
                # Refresh session after consecutive failures
                try:
                    self.rotate_user_agent()
                    ref_resp = self.session.get(ref_url, headers=self.headers, timeout=10)
                    ref_resp.raise_for_status()
                    cookies = ref_resp.cookies.get_dict()
                except:
                    time.sleep(random.uniform(5, 10))

            # Safe spacing
            time.sleep(random.uniform(1.5, 3.5))
            start_dt = chunk_end + timedelta(days=1)

        # --- Data Handling ---
        if not all_data:
            print(f"⚠️ No data for {index} between {from_date} and {to_date}.")
            if fail_chunks:
                print(f"❌ Failed chunks ({len(fail_chunks)}): {fail_chunks}")
            return pd.DataFrame()

        df = pd.DataFrame(all_data)
        expected_cols = ["IY_INDEX", "IY_DT", "IY_PE", "IY_PB", "IY_DY"]
        df = df[[c for c in expected_cols if c in df.columns]]

        df.rename(columns={
            "IY_INDEX": "Index Name",
            "IY_DT": "Date",
            "IY_PE": "P/E",
            "IY_PB": "P/B",
            "IY_DY": "Div Yield%"
        }, inplace=True)

        for col in ["P/E", "P/B", "Div Yield%"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df.replace([float("inf"), float("-inf")], None, inplace=True)
        df.dropna(subset=["P/E"], inplace=True)
        df.ffill(inplace=True)

        df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce")
        df.sort_values("Date", inplace=True)
        df.drop_duplicates(subset=["Date"], keep="last", inplace=True)
        df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")

        # # --- Final Summary ---
        # if fail_chunks:
        #     print(f"⚠️ {index}: {len(fail_chunks)} failed chunks → {fail_chunks}")
        # else:
        #     print(f"✅ {index} data fetched successfully: {from_date} → {to_date}")

        return df.reset_index(drop=True)

    def india_vix_historical_data(self, *args, from_date=None, to_date=None, period=None):
        """
        Fetch India VIX historical data from NSE's API.

        Supports:
            • Direct date inputs: "01-08-2025", "01-10-2025"
            • Period shorthand: "1M", "3M", "6M", "1Y", "2Y", "5Y", "10Y", "YTD", "MAX"
            • Automatically splits requests into ~3-month chunks to avoid API limits.

        Returns:
            pandas.DataFrame with columns:
            ['Date', 'Symbol', 'Open Price', 'High Price', 'Low Price', 'Close Price',
            'Prev Close', 'VIX Pts Chg', 'VIX % Chg']
        """
        symbol = "INDIA VIX"
        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        today = datetime.now()
        today_str = today.strftime("%d-%m-%Y")

        # --- Auto-detect arguments ---
        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                elif arg.upper() in ['1D','1W','1M','3M','6M','1Y','2Y','5Y','10Y','YTD','MAX']:
                    period = arg.upper()

        # --- Compute date range from period ---
        delta_map = {
            "1D": timedelta(days=1),
            "1W": timedelta(weeks=1),
            "1M": timedelta(days=30),
            "3M": timedelta(days=90),
            "6M": timedelta(days=180),
            "1Y": timedelta(days=365),
            "2Y": timedelta(days=730),
            "5Y": timedelta(days=1825),
            "10Y": timedelta(days=3650),
        }

        if period:
            if period == "YTD":
                from_date = datetime(today.year, 1, 1).strftime("%d-%m-%Y")
                to_date = today_str
            elif period == "MAX":
                from_date = "01-01-2008"
                to_date = today_str
            else:
                delta = delta_map.get(period, timedelta(days=365))
                from_date = (today - delta).strftime("%d-%m-%Y")
                to_date = today_str

        from_date = from_date or (today - timedelta(days=365)).strftime("%d-%m-%Y")
        to_date = to_date or today_str

        # --- Setup session and headers ---
        self.rotate_user_agent()
        ref_url = "https://www.nseindia.com/report-detail/eq_security"
        base_api = "https://www.nseindia.com/api/historicalOR/vixhistory?from={}&to={}"

        try:
            ref_resp = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_resp.raise_for_status()
            cookies = ref_resp.cookies.get_dict()
        except Exception as e:
            print(f"❌ NSE session init failed: {e}")
            return pd.DataFrame()

        start_dt = datetime.strptime(from_date, "%d-%m-%Y")
        end_dt = datetime.strptime(to_date, "%d-%m-%Y")

        all_data = []
        chunk_days = 89  # ~3 months
        max_retries = 3
        fail_chunks = []

        # --- Fetch data in chunks ---
        while start_dt <= end_dt:
            chunk_start = start_dt
            chunk_end = min(start_dt + timedelta(days=chunk_days), end_dt)
            api_url = base_api.format(
                chunk_start.strftime("%d-%m-%Y"),
                chunk_end.strftime("%d-%m-%Y")
            )

            success = False
            for attempt in range(1, max_retries + 1):
                try:
                    response = self.session.get(
                        api_url, headers=self.headers, cookies=cookies, timeout=15 + attempt*5
                    )
                    if response.status_code == 200:
                        data = response.json()
                        if "data" in data and isinstance(data["data"], list):
                            all_data.extend(data["data"])
                        success = True
                        break
                    elif response.status_code == 429:
                        # Rate limit hit, wait longer
                        time.sleep(random.uniform(8, 12))
                    else:
                        time.sleep(random.uniform(2, 4))
                except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
                    time.sleep(random.uniform(3, 6))
                except Exception:
                    time.sleep(random.uniform(3, 6))

            if not success:
                fail_chunks.append(f"{chunk_start.strftime('%d-%b-%Y')} → {chunk_end.strftime('%d-%b-%Y')}")
                # Rotate session after repeated failures
                try:
                    self.rotate_user_agent()
                    ref_resp = self.session.get(ref_url, headers=self.headers, timeout=10)
                    ref_resp.raise_for_status()
                    cookies = ref_resp.cookies.get_dict()
                except:
                    time.sleep(random.uniform(5, 10))

            # Safe spacing
            time.sleep(random.uniform(1.5, 3.5))
            start_dt = chunk_end + timedelta(days=1)

        # --- Check if any data fetched ---
        if not all_data:
            print(f"⚠️ No data returned for {symbol} between {from_date} and {to_date}.")
            if fail_chunks:
                print(f"❌ Failed chunks ({len(fail_chunks)}): {fail_chunks}")
            return pd.DataFrame()

        # --- Convert to DataFrame ---
        df = pd.DataFrame(all_data)
        expected_cols = [
            "EOD_TIMESTAMP", "EOD_INDEX_NAME",
            "EOD_OPEN_INDEX_VAL", "EOD_HIGH_INDEX_VAL",
            "EOD_LOW_INDEX_VAL", "EOD_CLOSE_INDEX_VAL",
            "EOD_PREV_CLOSE", "VIX_PTS_CHG", "VIX_PERC_CHG"
        ]
        df = df[[c for c in expected_cols if c in df.columns]]

        rename_map = {
            "EOD_TIMESTAMP": "Date",
            "EOD_INDEX_NAME": "Symbol",
            "EOD_OPEN_INDEX_VAL": "Open Price",
            "EOD_HIGH_INDEX_VAL": "High Price",
            "EOD_LOW_INDEX_VAL": "Low Price",
            "EOD_CLOSE_INDEX_VAL": "Close Price",
            "EOD_PREV_CLOSE": "Prev Close",
            "VIX_PTS_CHG": "VIX Pts Chg",
            "VIX_PERC_CHG": "VIX % Chg"
        }
        df.rename(columns=rename_map, inplace=True)

        numeric_cols = [
            "Open Price", "High Price", "Low Price",
            "Close Price", "Prev Close", "VIX Pts Chg", "VIX % Chg"
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df.replace([float("inf"), float("-inf")], None, inplace=True)
        df.ffill(inplace=True)

        df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce")
        df.sort_values("Date", inplace=True)
        df.drop_duplicates(subset=["Date"], keep="last", inplace=True)
        df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")

        # # --- Print final summary ---
        # if fail_chunks:
        #     print(f"⚠️ {symbol}: {len(fail_chunks)} failed chunks → {fail_chunks}")
        # else:
        #     print(f"✅ {symbol} data fetched successfully: {from_date} → {to_date}")

        return df.reset_index(drop=True)

    #---------------------------------------------------------- Gifty_Nifty ----------------------------------------------------------------

    def cm_live_gifty_nifty(self):
        data = self._get('https://www.nseindia.com/api/NextApi/apiClient?functionName=getGiftNifty', 'https://www.nseindia.com/')
        if data and "data" in data:
            gift_nifty_data = data["data"].get("giftNifty", {})
            usd_inr_data = data["data"].get("usdInr", {})

            df = pd.DataFrame([{
                "symbol": gift_nifty_data.get("symbol"),
                "lastprice": gift_nifty_data.get("lastprice"),
                "daychange": gift_nifty_data.get("daychange"),
                "perchange": gift_nifty_data.get("perchange"),
                "contractstraded": gift_nifty_data.get("contractstraded"),
                "timestmp": gift_nifty_data.get("timestmp"),
                "expirydate": gift_nifty_data.get("expirydate"),
                "usdInr_symbol": usd_inr_data.get("symbol"),
                "usdInr_ltp": usd_inr_data.get("ltp"),
                "usdInr_updated_time": usd_inr_data.get("updated_time"),
                "usdInr_expiry_dt": usd_inr_data.get("expiry_dt"),
            }])
            return df
        return None
        
    #---------------------------------------------------------- market_statistics ----------------------------------------------------------------

    def cm_live_market_statistics(self):
        """
        Fetch live Capital Market statistics from NSE India.

        Returns
        -------
        pandas.DataFrame or None
            A single-row DataFrame containing:
            Total, Advances, Declines, Unchanged, 52W High, 52W Low,
            Upper Circuit, Lower Circuit, Market Cap ₹ Lac Crs,
            Market Cap Tn $, Registered Investors (Raw), Registered Investors (Cr), Date
        """
        data = self._get('https://www.nseindia.com/api/NextApi/apiClient?functionName=getMarketStatistics', 'https://www.nseindia.com')
        if not data or 'data' not in data: return None

        d = data['data']
        snapshot = d.get('snapshotCapitalMarket', {})
        fifty_two_week = d.get('fiftyTwoWeek', {})
        circuit = d.get('circuit', {})

        reg_inv_str = d.get('regInvestors', '0')
        reg_inv_cr = round(int(reg_inv_str.replace(',', '').strip()) / 1e7, 2) if reg_inv_str else 0.0

        df = pd.DataFrame([{
            'Total': snapshot.get('total'),
            'Advances': snapshot.get('advances'),
            'Declines': snapshot.get('declines'),
            'Unchanged': snapshot.get('unchange'),
            '52W High': fifty_two_week.get('high'),
            '52W Low': fifty_two_week.get('low'),
            'Upper Circuit': circuit.get('upper'),
            'Lower Circuit': circuit.get('lower'),
            'Market Cap ₹ Lac Crs': round(d.get('tlMKtCapLacCr', 0), 2),
            'Market Cap Tn $': round(d.get('tlMKtCapTri', 0), 3),
            'Registered Investors': reg_inv_str,           # Raw string
            'Registered Investors (Cr)': reg_inv_cr,       # Crores
            'Date': d.get('asOnDate'),
        }])
        return df if not df.empty else None

    #---------------------------------------------------------- CM_Live_Data ----------------------------------------------------------------
    
    def cm_live_equity_info(self, symbol):
        """Vectorized Equity Basic Info matching legacy structure."""
        symbol = symbol.upper().replace(' ', '%20').replace('&', '%26')
        data = self._get(f'https://www.nseindia.com/api/quote-equity?symbol={symbol}', f'https://www.nseindia.com/get-quotes/equity?symbol={symbol}')
        if not data or 'info' not in data: return None
        return {
            "Symbol": symbol,
            "companyName": data['info']['companyName'],
            "industry": data['info']['industry'],
            "boardStatus": data['securityInfo']['boardStatus'],
            "tradingStatus": data['securityInfo']['tradingStatus'],
            "tradingSegment": data['securityInfo']['tradingSegment'],
            "derivatives": data['securityInfo']['derivatives'],
            "surveillance": data['securityInfo']['surveillance']['surv'],
            "surveillanceDesc": data['securityInfo']['surveillance']['desc'],
            "Facevalue": data['securityInfo']['faceValue'],
            "TotalSharesIssued": data['securityInfo']['issuedSize']
        }

    def cm_live_equity_price_info(self, symbol):
        """Vectorized Equity Price & OrderBook Info matching legacy structure."""
        symbol = symbol.upper().replace(' ', '%20').replace('&', '%26')
        data = self._get(f'https://www.nseindia.com/api/quote-equity?symbol={symbol}', f'https://www.nseindia.com/get-quotes/equity?symbol={symbol}')
        trade_data = self._get(f'https://www.nseindia.com/api/quote-equity?symbol={symbol}&section=trade_info', f'https://www.nseindia.com/get-quotes/equity?symbol={symbol}')
        if not data or not trade_data or 'priceInfo' not in data: return None

        book = trade_data.get('marketDeptOrderBook', {})
        bids, asks = book.get('bid', []), book.get('ask', [])
        
        res = {
            "Symbol": symbol,
            "PreviousClose": data['priceInfo']['previousClose'],
            "LastTradedPrice": data['priceInfo']['lastPrice'],
            "Change": data['priceInfo']['change'],
            "PercentChange": data['priceInfo']['pChange'],
            "deliveryToTradedQuantity": trade_data.get('securityWiseDP', {}).get('deliveryToTradedQuantity', 0),
            "Open": data['priceInfo']['open'],
            "Close": data['priceInfo']['close'],
            "High": data['priceInfo']['intraDayHighLow'].get('max'),
            "Low": data['priceInfo']['intraDayHighLow'].get('min'),
            "VWAP": data['priceInfo']['vwap'],
            "UpperCircuit": data['priceInfo']['upperCP'],
            "LowerCircuit": data['priceInfo']['lowerCP'],
            "Macro": data.get('industryInfo', {}).get('macro'),
            "Sector": data.get('industryInfo', {}).get('sector'),
            "Industry": data.get('industryInfo', {}).get('industry'),
            "BasicIndustry": data.get('industryInfo', {}).get('basicIndustry'),
        }

        for i in range(5):
            res[f"Bid Price {i+1}"] = bids[i].get("price", 0) if i < len(bids) else 0
            res[f"Bid Quantity {i+1}"] = bids[i].get("quantity", 0) if i < len(bids) else 0
            res[f"Ask Price {i+1}"] = asks[i].get("price", 0) if i < len(asks) else 0
            res[f"Ask Quantity {i+1}"] = asks[i].get("quantity", 0) if i < len(asks) else 0

        res["totalBuyQuantity"] = book.get("totalBuyQuantity", 0)
        res["totalSellQuantity"] = book.get("totalSellQuantity", 0)
        return res

    def cm_live_equity_full_info(self, symbol):
        """Hyper-Speed Equity Full Info matching legacy schema."""
        symbol = symbol.upper().replace(" ", "%20").replace("&", "%26")
        data = self._get(f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getSymbolData&marketType=N&series=EQ&symbol={symbol}", f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}")
        equity = data.get("equityResponse", []) if data else []
        if not equity: return None
        eq = equity[0]
        m, t, p, s, o = eq.get("metaData",{}), eq.get("tradeInfo",{}), eq.get("priceInfo",{}), eq.get("secInfo",{}), eq.get("orderBook",{})
        
        return {
            "Symbol": m.get("symbol"),
            "CompanyName": m.get("companyName"),
            "Index": s.get("index"),
            "ISIN": m.get("isinCode"),
            "Series": m.get("series"),
            "MarketType": m.get("marketType"),
            "BoardStatus": s.get("boardStatus"),
            "TradingSegment": s.get("tradingSegment"),
            "SecurityStatus": s.get("secStatus"),
            "Open": m.get("open"),
            "DayHigh": m.get("dayHigh"),
            "DayLow": m.get("dayLow"),
            "PreviousClose": m.get("previousClose"),
            "LastTradedPrice": o.get("lastPrice"),
            "closePrice": m.get("closePrice"),
            "Change": m.get("change"),
            "PercentChange": m.get("pChange"),
            "VWAP": m.get("averagePrice"),
            "TotalTradedVolume": t.get("totalTradedVolume"),
            "TotalTradedValue": t.get("totalTradedValue"),
            "Quantity raded": t.get("quantitytraded"),
            "DeliveryQty": t.get("deliveryquantity"),
            "DeliveryPercent": t.get("deliveryToTradedQuantity"),
            "ImpactCost": t.get("impactCost"),
            "PriceBandRange": p.get("priceBand"),
            "PriceBand": p.get("ppriceBand"),
            "TickSize": p.get("tickSize"),
            "Bid Price 1": o.get("buyPrice1"), "Bid Quantity 1": o.get("buyQuantity1"),
            "Bid Price 2": o.get("buyPrice2"), "Bid Quantity 2": o.get("buyQuantity2"),
            "Bid Price 3": o.get("buyPrice3"), "Bid Quantity 3": o.get("buyQuantity3"),
            "Bid Price 4": o.get("buyPrice4"), "Bid Quantity 4": o.get("buyQuantity4"),
            "Bid Price 5": o.get("buyPrice5"), "Bid Quantity 5": o.get("buyQuantity5"),
            "Ask Price 1": o.get("sellPrice1"), "Ask Quantity 1": o.get("sellQuantity1"),
            "Ask Price 2": o.get("sellPrice2"), "Ask Quantity 2": o.get("sellQuantity2"),
            "Ask Price 3": o.get("sellPrice3"), "Ask Quantity 3": o.get("sellQuantity3"),
            "Ask Price 4": o.get("sellPrice4"), "Ask Quantity 4": o.get("sellQuantity4"),
            "Ask Price 5": o.get("sellPrice5"), "Ask Quantity 5": o.get("sellQuantity5"),
            "TotalBuyQuantity": o.get("totalBuyQuantity"),
            "TotalSellQuantity": o.get("totalSellQuantity"),
            "BuyQuantity%": f"{o.get('perBuyQty', 0):.2f}",
            "SellQuantity%": f"{o.get('perSellQty', 0):.2f}",
            "52WeekHigh": p.get("yearHigh"),
            "52WeekLow": p.get("yearLow"),
            "52WeekHighDate": p.get("yearHightDt"),
            "52WeekLowDate": p.get("yearLowDt"),
            "DailyVolatility": p.get("cmDailyVolatility"),
            "AnnualisedVolatility": p.get("cmAnnualVolatility"),
            "SymbolPE": s.get("pdSymbolPe"),
            "FaceValue": t.get("faceValue"),
            "TotalIssuedShares": t.get("issuedSize"),
            "MarketCap": t.get("totalMarketCap"),
            "FreeFloatMcap": t.get("ffmc"),
            "DateOfListing": s.get("listingDate"),
            "Security VaR": s.get("securityvar"),
            "Index VaR": s.get("indexvar"),
            "VaR Margin": s.get("varMargin"),
            "Extreme Loss Rate": s.get("extremelossMargin"),
            "Adhoc Margin": s.get("adhocMargin"),
            "Applicable Margin Rate": s.get("applicableMargin"),
            "Macro": s.get("macro"),
            "Sector": s.get("sector"),
            "Industry": s.get("industryInfo"),
            "BasicIndustry": s.get("basicIndustry"),
            "LastUpdated": eq.get("lastUpdateTime")
        }


    def cm_live_most_active_equity_by_value(self):
        """Vectorized Most Active Equities by Value with 100% legacy parity."""
        data = self._get("https://www.nseindia.com/api/live-analysis-most-active-securities?index=value", "https://www.nseindia.com/market-data/most-active-equities")
        return self._to_df(data, "data")

    def cm_live_most_active_equity_by_vol(self):
        """Vectorized Most Active Equities by Volume with 100% legacy parity."""
        data = self._get("https://www.nseindia.com/api/live-analysis-most-active-securities?index=volume", "https://www.nseindia.com/market-data/most-active-equities")
        return self._to_df(data, "data")

    def cm_live_volume_spurts(self):
        """Vectorized Volume Spurts (Gainers) matching legacy exactly."""
        data = self._get("https://www.nseindia.com/api/live-analysis-volume-gainers", "https://www.nseindia.com/market-data/volume-gainers-spurts")
        df = self._to_df(data, "data")
        if df is not None:
            cols_map = {
                'symbol': 'Symbol', 'companyName': 'Security', 'volume': 'Today Volume',
                'week1AvgVolume': '1 Week Avg. Volume', 'week1volChange': '1 Week Change (×)',
                'week2AvgVolume': '2 Week Avg. Volume', 'week2volChange': '2 Week Change (×)',
                'ltp': 'LTP', 'pChange': '% Change', 'turnover': 'Turnover (₹ Lakhs)'
            }
            cols = [c for c in ['symbol', 'companyName', 'volume', 'week1AvgVolume', 'week1volChange', 'week2AvgVolume', 'week2volChange', 'ltp', 'pChange', 'turnover'] if c in df.columns]
            df = df[cols].rename(columns=cols_map)
        return df

    def cm_live_52week_high(self):
        data = self._get("https://www.nseindia.com/api/live-analysis-data-52weekhighstock", "https://www.nseindia.com/market-data/52-week-high-equity-market")
        df = self._to_df(data, "data")
        if df is not None:
            cols = ['symbol', 'series', 'ltp', 'pChange', 'new52WHL', 'prev52WHL', 'prevHLDate']
            df = df[[c for c in cols if c in df.columns]]
        return df

    def cm_live_52week_low(self):
        data = self._get("https://www.nseindia.com/api/live-analysis-data-52weeklowstock", "https://www.nseindia.com/market-data/52-week-low-equity-market")
        df = self._to_df(data, "data")
        if df is not None:
            cols = ['symbol', 'series', 'ltp', 'pChange', 'new52WHL', 'prev52WHL', 'prevHLDate']
            df = df[[c for c in cols if c in df.columns]]
        return df

    def cm_live_block_deal(self):
        data = self._get('https://www.nseindia.com/api/block-deal', 'https://www.nseindia.com/market-data/block-deal-watch')
        df = self._to_df(data, "data")
        if df is not None:
            cols = ['session','symbol', 'series', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'previousClose', 'pchange', 'totalTradedVolume', 'totalTradedValue']
            df = df[[c for c in cols if c in df.columns]]
        return df

    def cm_live_hist_insider_trading(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        """Vectorized Insider Trading fetcher with legacy column parity."""
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        params = {"index": "equities"}
        if symbol: params["symbol"] = symbol
        if from_date: params["from_date"], params["to_date"] = from_date, to_date
        data = self._get("https://www.nseindia.com/api/corporates-pit", "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading", params=params)
        df = self._to_df(data, "data")
        if df is not None and not df.empty:
            expected_cols = [
                "symbol", "company", "acqName", "personCategory", "secType", "befAcqSharesNo",
                "befAcqSharesPer", "remarks", "secAcq", "secVal", "tdpTransactionType",
                "securitiesTypePost", "afterAcqSharesNo", "afterAcqSharesPer", "acqfromDt",
                "acqtoDt", "intimDt", "acqMode", "derivativeType", "tdpDerivativeContractType",
                "buyValue", "buyQuantity", "sellValue", "sellquantity", "exchange", "date", "xbrl"
            ]
            df = df[[c for c in expected_cols if c in df.columns]]
            return df
        return None

    def cm_live_hist_corporate_announcement(self, *args, from_date=None, to_date=None, symbol=None):
        """Vectorized Corporate Announcements matching legacy expectations exactly."""
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, symbol=symbol)
        params = {"index": "equities", "reqXbrl": "false"}
        if symbol: params["symbol"] = symbol
        if from_date: params["from_date"], params["to_date"] = from_date, to_date
        data = self._get("https://www.nseindia.com/api/corporate-announcements", "https://www.nseindia.com/companies-listing/corporate-filings-announcements", params=params)
        df = self._to_df(data) if isinstance(data, list) else self._to_df(data, "data")
        if df is not None and not df.empty:
            expected_cols = ['symbol', 'sm_name', 'smIndustry', 'desc', 'attchmntText', 'attchmntFile', 'fileSize', 'an_dt']
            df = df[[c for c in expected_cols if c in df.columns]]
            return df
        return None

    def cm_live_hist_corporate_action(self, *args, from_date=None, to_date=None, period=None, symbol=None, filter=None):
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        params = {"index": "equities"}
        if symbol: params["symbol"] = symbol
        if from_date: params["from_date"], params["to_date"] = from_date, to_date
        data = self._get("https://www.nseindia.com/api/corporates-corporateActions", "https://www.nseindia.com/companies-listing/corporate-filings-actions", params=params)
        df = self._to_df(data, "data")
        if df is not None:
            mapping = {"symbol": "SYMBOL", "comp": "COMPANY NAME", "series": "SERIES", "subject": "PURPOSE", "faceVal": "FACE VALUE", "exDate": "EX-DATE", "recDate": "RECORD DATE", "bcStartDate": "BOOK CLOSURE START DATE", "bcEndDate": "BOOK CLOSURE END DATE"}
            order = ["SYMBOL", "COMPANY NAME", "SERIES", "PURPOSE", "FACE VALUE", "EX-DATE", "RECORD DATE", "BOOK CLOSURE START DATE", "BOOK CLOSURE END DATE"]
            df = df.rename(columns=mapping)
            df = df[[c for c in order if c in df.columns]]
            if filter: df = df[df["PURPOSE"].str.contains(filter, case=False, na=False)]
        return df

    def cm_live_today_event_calendar(self, from_date=None, to_date=None):
        from_date = from_date or datetime.now().strftime("%d-%m-%Y")
        to_date = to_date or datetime.now().strftime("%d-%m-%Y")
        data = self._get(f"https://www.nseindia.com/api/event-calendar?index=equities&from_date={from_date}&to_date={to_date}", "https://www.nseindia.com/companies-listing/corporate-filings-event-calendar")
        df = self._to_df(data)
        if df is not None:
            cols = ['symbol', 'company', 'purpose', 'bm_desc', 'date']
            df = df[[c for c in cols if c in df.columns]]
        return df
        
    def cm_live_upcoming_event_calendar(self):
        data = self._get('https://www.nseindia.com/api/event-calendar?', 'https://www.nseindia.com/companies-listing/corporate-filings-event-calendar')
        df = self._to_df(data)
        if df is not None:
            cols = ['symbol', 'company', 'purpose', 'bm_desc', 'date']
            df = df[[c for c in cols if c in df.columns]]
        return df
        
    def cm_live_hist_board_meetings(self, *args, period=None, from_date=None, to_date=None, symbol=None):
        """Vectorized Board Meetings matching legacy."""
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        params = {"index": "equities"}
        if symbol: params["symbol"] = symbol
        if from_date: params["from_date"], params["to_date"] = from_date, to_date
        data = self._get("https://www.nseindia.com/api/corporate-board-meetings", "https://www.nseindia.com/companies-listing/corporate-filings-board-meetings", params=params)
        df = self._to_df(data) if isinstance(data, list) else self._to_df(data, "data")
        if df is not None and not df.empty:
            cols = ['bm_symbol', 'sm_name', 'sm_indusrty', 'bm_purpose', 'bm_desc', 'bm_date', 'attachment', 'attFileSize','bm_timestamp']
            df = df[[c for c in cols if c in df.columns]]
            return df
        return None
        
    def cm_live_hist_Shareholder_meetings(self, *args, from_date=None, to_date=None, symbol=None):
        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date: from_date = arg
                    elif not to_date: to_date = arg
                else: symbol = arg.upper()

        if symbol and from_date and to_date: api_url = f"https://www.nseindia.com/api/postal-ballot?index=equities&from_date={from_date}&to_date={to_date}&symbol={symbol}"
        elif symbol: api_url = f"https://www.nseindia.com/api/postal-ballot?index=equities&symbol={symbol}"
        elif from_date and to_date: api_url = f"https://www.nseindia.com/api/postal-ballot?index=equities&from_date={from_date}&to_date={to_date}"
        else: api_url = "https://www.nseindia.com/api/postal-ballot?index=equities"

        data = self._get(api_url, "https://www.nseindia.com/companies-listing/corporate-filings-postal-ballot")
        return self._to_df(data, "data", columns=["symbol", "sLN", "bdt", "text", "type", "attachment", "date"])

    def cm_live_hist_qualified_institutional_placement(self, *args, from_date=None, to_date=None, period=None, symbol=None, stage=None):
        """Vectorized QIP history fetcher."""
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        
        for arg in args:
            if isinstance(arg, str):
                arg_at = str(arg).title()
                if arg_at in ["In-Principle", "Listing Stage"]: stage = arg_at
        
        s_type: str = str(stage or "In-Principle")
        stage = s_type.title()
        index_map: dict[str, str] = {"In-Principle": "FIQIPIP", "Listing Stage": "FIQIPLS"}
        params = {"index": index_map.get(stage, "FIQIPIP")}
        if symbol: params["symbol"] = symbol.upper()
        elif from_date: params["from_date"], params["to_date"] = from_date, to_date
        
        data = self._get("https://www.nseindia.com/api/corporate-further-issues-qip", "https://www.nseindia.com/companies-listing/corporate-filings-QIP", params=params)
        df = self._to_df(data, "data")
        if df is not None and not df.empty:
            if stage == "In-Principle":
                r_map = {"nseSymbol": "Symbol", "companyName": "Company Name", "stage": "Stage", "issue_type": "Issue Type", "dateBrdResol": "Board Resolution Date", "dateOfSHApp": "Shareholder Approval Date", "totalAmtOfIssueSize": "Total Issue Size", "prcntagePerSecrtyProDiscNotice": "Percentage per Security Notice", "listedAt": "Listed At", "dateOfSubmission": "Submission Date", "xmlFileName": "XML Link"}
            else:
                r_map = {"nsesymbol": "Symbol", "companyName": "Company Name", "stage": "Stage", "issue_type": "Issue Type", "boardResolutionDate": "Board Resolution Date", "dtOfBIDOpening": "BID Opening Date", "dtOfBIDClosing": "BID Closing Date", "dtOfAllotmentOfShares": "Allotment Date", "noOfSharesAllotted": "No of Shares Allotted", "finalAmountOfIssueSize": "Final Issue Size", "minIssPricePerUnit": "Min Issue Price", "issPricePerUnit": "Issue Price Per Unit", "noOfAllottees": "No of Allottees", "noOfEquitySharesListed": "No of Equity Shares Listed", "dateOfSubmission": "Submission Date", "dateOfListing": "Listing Date", "dateOfTradingApproval": "Trading Approval Date", "xmlFileName": "XML Link"}
            df = df[[c for c in r_map.keys() if c in df.columns]].rename(columns=r_map)
            return df
        return None

    def cm_live_hist_preferential_issue(self, *args, from_date=None, to_date=None, period=None, symbol=None, stage=None):
        """Vectorized PREF issue with legacy parity."""
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        
        for arg in args:
            if isinstance(arg, str):
                arg_at = str(arg).title()
                if arg_at in ["In-Principle", "Listing Stage"]: stage = arg_at
        
        s_type: str = str(stage or "In-Principle")
        stage = s_type.title()
        index_map: dict[str, str] = {"In-Principle": "FIPREFIP", "Listing Stage": "FIPREFLS"}
        params = {"index": index_map.get(stage, "FIPREFIP")}
        if symbol: params["symbol"] = symbol.upper()
        elif from_date: params["from_date"], params["to_date"] = from_date, to_date
        
        data = self._get("https://www.nseindia.com/api/corporate-further-issues-pref", "https://www.nseindia.com/companies-listing/corporate-filings-PREF", params=params)
        df = self._to_df(data, "data")
        if df is not None and not df.empty:
            if stage == "In-Principle":
                r_map = {"nseSymbol": "Symbol", "nameOfTheCompany": "Company Name", "stage": "Stage", "issueType": "Issue Type", "dateBrdResoln": "Date of Board Resolution", "boardResDate": "Board Resolution Date", "categoryOfAllottee": "category Of Allottee", "totalAmtRaised": "Total Amount Size", "considerationBy": "considerationBy", "descriptionOfOtherCon": "descriptionOfOtherCon", "dateOfSubmission": "Submission Date", "checklist_zip_file_name": "zip Link"}
            else:
                r_map = {"nseSymbol": "Symbol", "nameOfTheCompany": "Company Name", "stage": "Stage", "issueType": "Issue Type", "dateBrdResoln": "Board meeting date", "boardResDate": "Board Resolution Date", "categoryOfAllottee": "category Of Allottee", "noOfSecurities": "No of Securities", "distinctiveNo": "Distinctive No", "dateOfListing": "Date of Listing", "dateOfSubmission": "Submission Date", "checklist_zip_file_name": "zip Link"}
            df = df[[c for c in r_map.keys() if c in df.columns]].rename(columns=r_map)
            return df if not df.empty else None
        return None

    def cm_live_hist_right_issue(self, *args, period=None, from_date=None, to_date=None, symbol=None, stage=None):
        """Vectorized Right issue matching legacyExactly."""
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        for arg in args:
            if isinstance(arg, str):
                t = arg.title()
                if t in ["In-Principle", "Listing Stage"]: stage = t
        stage = (stage or "In-Principle").title()
        index_map: dict[str, str] = {"In-Principle": "FIRIP", "Listing Stage": "FIRLS"}
        params = {"index": index_map.get(stage, "FIRIP")}
        if symbol: params["symbol"] = symbol.upper()
        elif from_date: params["from_date"], params["to_date"] = from_date, to_date
        data = self._get("https://www.nseindia.com/api/corporate-further-issues-ri", "https://www.nseindia.com/companies-listing/corporate-filings-RI", params=params)
        df = self._to_df(data, "data")
        if df is not None and not df.empty:
            if stage == "In-Principle": r_map = {"nseSymbol": "Symbol", "nameOfTheCompany": "Company Name", "issueType": "Issue Type", "dateBrdResoln": "Board Resolution Date", "rightsRatio": "Rights Ratio", "recordDate": "Record Date", "issuePrice": "Issue Price", "dateOfSubmission": "Submission Date", "checklist_zip_file_name": "XML Link"}
            else: r_map = {"nseSymbol": "Symbol", "nameOfTheCompany": "Company Name", "issueType": "Issue Type", "noOfSecurities": "No of securities", "distinctiveNo": "Distinctive No.", "dateOfListing": "Date of listing", "dateOfSubmission": "Submission Date", "checklist_zip_file_name": "XML Link"}
            df = df[[c for c in r_map.keys() if c in df.columns]].rename(columns=r_map)
            return df if not df.empty else None
        return None

    def cm_live_voting_results(self):
        """
        Fetch and process corporate voting results from NSE India.
        Handles both metadata and nested agendas, and flattens data
        for Google Sheets compatibility.
        """

        data = self._get("https://www.nseindia.com/api/corporate-voting-results?", "https://www.nseindia.com/companies-listing/corporate-filings-voting-results")
        if not data: return None

        all_rows = []

        for item in data:
            meta = item.get("metadata", {})
            agendas = meta.get("agendas", []) or item.get("agendas", [])
            if agendas:
                for ag in agendas:
                    merged = {**meta, **ag}
                    all_rows.append(merged)
            else:
                all_rows.append(meta)

        if not all_rows:
            print("⚠️ No data found in NSE voting results API.")
            return None

        df = pd.DataFrame(all_rows)

        df.replace({float("inf"): None, float("-inf"): None}, inplace=True)
        df.fillna("", inplace=True)

        def flatten_value(v):
            if isinstance(v, (list, dict)):
                return json.dumps(v, ensure_ascii=False)
            elif v is None:
                return ""
            else:
                return str(v)

        for col in df.columns:
            df[col] = df[col].map(flatten_value)

        preferred_cols = [
            "vrSymbol", "vrCompanyName", "vrMeetingType", "vrTimestamp",
            "vrTypeOfSubmission", "vrAttachment", "vrbroadcastDt",
            "vrRevisedDate", "vrRevisedRemark", "vrResolution",
            "vrResReq", "vrGrpInterested", "vrTotSharesOnRec",
            "vrTotSharesProPer", "vrTotSharesPublicPer",
            "vrTotSharesProVid", "vrTotSharesPublicVid",
            "vrTotPercFor", "vrTotPercAgainst"
        ]
        existing_cols = [c for c in preferred_cols if c in df.columns]
        df = df[existing_cols + [c for c in df.columns if c not in existing_cols]]

        if "vrbroadcastDt" in df.columns:
            try:
                df["vrbroadcastDt_dt"] = pd.to_datetime(df["vrbroadcastDt"], errors="coerce")
                df.sort_values(by=["vrbroadcastDt_dt"], ascending=False, inplace=True)
                df.drop(columns=["vrbroadcastDt_dt"], inplace=True)
            except Exception as e:
                print(f"⚠️ Date sort issue: {e}")

        df.reset_index(drop=True, inplace=True)
        return df

    def cm_live_qtly_shareholding_patterns(self):
        data = self._get('https://www.nseindia.com/api/corporate-share-holdings-master?index=equities', 'https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern')
        df = self._to_df(data)
        if df is not None:
            cols = ['symbol', 'name', 'pr_and_prgrp', 'public_val', 'employeeTrusts', 'revisedStatus', 'date', 'submissionDate', 'revisionDate', 'xbrl', 'broadcastDate', 'systemDate', 'timeDifference']
            df = df[[c for c in cols if c in df.columns]]
        return df

    def cm_live_hist_Shareholder_meetings(self, *args, period=None, from_date=None, to_date=None, symbol=None):
        """Vectorized Shareholder Meetings with legacy parity."""
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        params = {"index": "equities"}
        if symbol: params["symbol"] = symbol
        if from_date: params["from_date"], params["to_date"] = from_date, to_date
        data = self._get("https://www.nseindia.com/api/postal-ballot", "https://www.nseindia.com/companies-listing/corporate-filings-postal-ballot", params=params)
        df = self._to_df(data, "data")
        if df is not None and not df.empty:
            cols = ["symbol", "sLN", "bdt", "text", "type", "attachment", "date"]
            df = df[[c for c in cols if c in df.columns]]
            return df if not df.empty else None
        return None

    def cm_live_hist_br_sr(self, *args, from_date=None, to_date=None, symbol=None):
        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date: from_date = arg
                    elif not to_date: to_date = arg
                else: symbol = arg.upper()

        params = {"index": "equities"}
        if symbol: params["symbol"] = symbol
        if from_date and to_date: params["from_date"], params["to_date"] = from_date, to_date

        data = self._get("https://www.nseindia.com/api/corporate-bussiness-sustainabilitiy", "https://www.nseindia.com/companies-listing/corporate-filings-bussiness-sustainabilitiy-reports", params=params)
        return self._to_df(data, "data", columns=['symbol', 'companyName', 'fyFrom', 'fyTo', 'submissionDate', 'revisionDate'])

    
    #---------------------------------------------------------- Live Chart Data ----------------------------------------------------------------
 
    def index_chart(self, index: str, timeframe: str = "1D"):
        """Vectorized Intraday Index Chart."""
        index = index.upper().replace(' ', '%20').replace('&', '%26')
        data = self._get(f"https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi?functionName=getIndexChart&&index={index}&flag={timeframe}", "https://www.nseindia.com/")
        g_data = (data.get("data", {}).get("grapthData", []) if data else []) if data else []
        if not g_data: return None
        df = pd.DataFrame(g_data, columns=["ts", "price", "flag", "change", "pct_change"])
        df["datetime_utc"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
        return df[["datetime_utc", "price", "change", "pct_change", "flag"]]

    def stock_chart(self, symbol: str, timeframe: str = "1D"):
        """Vectorized Intraday Stock Chart."""
        data = self._get(f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getSymbolChartData&symbol={symbol}EQN&days={timeframe}", "https://www.nseindia.com/")
        g_data = data.get("grapthData", []) if data else []
        if not g_data: return None
        df = pd.DataFrame(g_data, columns=["ts", "price", "flag", "change", "pct_change"])
        df["datetime_utc"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
        return df[["datetime_utc", "price", "change", "pct_change", "flag"]]

    def fno_chart(self, symbol: str, inst_type: str, expiry: str, strike: str = ""):
        """Vectorized Intraday FnO Chart."""
        strike_part = "XX0" if inst_type.startswith("FUT") else strike
        contract = f"{inst_type}{symbol.upper()}{expiry}{strike_part}.00"
        data = self._get(f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getIntradayGraphDerivative&identifier={contract}&type=W&token=1", "https://www.nseindia.com/")
        g_data = data.get("grapthData", []) if data else []
        if not g_data: return None
        df = pd.DataFrame(g_data)
        df.columns = ["ts", "price", "chng", "pct"][:len(df.columns)]
        df["datetime_utc"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
        return df

    def india_vix_chart(self):
        """Vectorized Intraday VIX Chart."""
        data = self._get("https://www.nseindia.com/api/chart-databyindex-dynamic?index=INDIA%20VIX&type=index", "https://www.nseindia.com/market-data/live-market-indices")
        g_data = data.get("grapthData", []) if data else []
        if not g_data: return None
        df = pd.DataFrame(g_data, columns=["ts", "price", "flag"])
        df["datetime_utc"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
        return df[["datetime_utc", "price", "flag"]]


    #---------------------------------------------------------- FnO_Live_Data ----------------------------------------------------------------


    #----------------------------------------------------------JSON_Data ----------------------------------------------------------------
    
    def symbol_full_fno_live_data(self,symbol: str):
        data = self._get(f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getSymbolDerivativesData&symbol={symbol}", 'https://www.nseindia.com/')
        return data        
        

    def symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI(self, symbol: str, type_mode: str):
        """
        Fetch Most Active Calls / Puts / Contracts by Open Interest (OI)
        using NSE NextAPI.

        type_mode:
            C / CALL / CALLS / MOST ACTIVE CALLS
            P / PUT / PUTS  / MOST ACTIVE PUTS
            O / OI / CONTRACTS / MOST ACTIVE CONTRACTS BY OI
        """

        # --- Normalise Type Input ---
        type_map = {
            "C": "C", "CALL": "C", "CALLS": "C", "MOST ACTIVE CALLS": "C",
            "P": "P", "PUT": "P", "PUTS": "P", "MOST ACTIVE PUTS": "P",
            "O": "O", "OI": "O", "CONTRACTS": "O",
            "MOST ACTIVE CONTRACTS BY OI": "O",
        }

        key = type_mode.strip().upper()

        if key not in type_map:
            raise ValueError("Invalid Type. Use C / P / O")

        callType = type_map[key]

        api_url = (f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getDerivativesMostActive&symbol={symbol}&callType={callType}")
        data = self._get(api_url, "https://www.nseindia.com/")
        return data


    def identifier_based_fno_contracts_live_chart_data(self,identifier: str):
        data = self._get(f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getIntradayGraphDerivative&identifier={identifier}&type=W&token=1", 'https://www.nseindia.com/')
        return data   

    #---------------------------------------------------------- futures ---------------------------------------------------------------------

    def fno_live_futures_data(self, symbol):
        symbol = symbol.replace(" ", "%20").replace("&", "%26")
        data = self._get(f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getSymbolDerivativesData&symbol={symbol}&instrumentType=FUT", f"https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}")
        df = self._to_df(data, "data")
        if df.empty: return None
        
        # Legacy index
        if "identifier" in df.columns: df.set_index("identifier", inplace=True)
        
        # Strike formatting legacy
        if "strikePrice" in df.columns:
            df["strikePrice"] = df["strikePrice"].astype(str).str.strip()

        numeric_cols = ["openPrice", "highPrice", "lowPrice", "closePrice", "prevClose", "lastPrice", "change", "totalTradedVolume", "totalTurnover", "openInterest", "changeinOpenInterest", "pchangeinOpenInterest", "underlyingValue", "ticksize", "pchange"]
        for col in numeric_cols:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce")
        
        final_order = ["instrumentType", "expiryDate", "optionType", "strikePrice", "openPrice", "highPrice", "lowPrice", "closePrice", "prevClose", "lastPrice", "change", "pchange", "totalTradedVolume", "totalTurnover", "openInterest", "changeinOpenInterest", "pchangeinOpenInterest", "underlyingValue", "volumeFreezeQuantity"]
        return df[[c for c in final_order if c in df.columns]]
        
    def fno_live_top_20_derivatives_contracts(self, category='Stock Options'):
        xref = {"Stock Futures": "stock_fut", "Stock Options": "stock_opt"}
        if category not in xref: raise ValueError("Invalid category")
        data = self._get(f"https://www.nseindia.com/api/liveEquity-derivatives?index={xref[category]}", 'https://www.nseindia.com/market-data/equity-derivatives-watch')
        df = self._to_df(data, "data")
        if df.empty: return None
        r_map = {'underlying': 'Symbol', 'identifier': 'Contract ID', 'instrumentType': 'Instr Type', 'instrument': 'Segment', 'contract': 'Contract', 'expiryDate': 'Expiry', 'optionType': 'Option', 'strikePrice': 'Strike', 'lastPrice': 'LTP', 'change': 'Chg', 'pChange': 'Chg %', 'openPrice': 'Open', 'highPrice': 'High', 'lowPrice': 'Low', 'closePrice': 'Close', 'volume': 'Volume (Cntr)', 'totalTurnover': 'Turnover (₹)', 'premiumTurnOver': 'Premium Turnover (₹)', 'underlyingValue': 'Underlying LTP', 'openInterest': 'OI (Cntr)', 'noOfTrades': 'Trades'}
        df.rename(columns=r_map, inplace=True)
        for col in ['Turnover (₹)', 'Premium Turnover (₹)']:
            if col in df.columns: df[col] = (pd.to_numeric(df[col], errors='coerce') / 1e7).round(2)
        df.rename(columns={'Turnover (₹)': 'Turnover (₹ Cr)', 'Premium Turnover (₹)': 'Premium Turnover (₹ Cr)'}, inplace=True)
        ordered = ['Segment','Symbol', 'Expiry', 'Option', 'Strike', 'Close', 'LTP', 'Chg', 'Chg %', 'Open', 'High', 'Low', 'Volume (Cntr)', 'Trades', 'OI (Cntr)', 'Premium Turnover (₹ Cr)', 'Turnover (₹ Cr)', 'Contract', 'Contract ID', 'Underlying LTP']
        return df[[c for c in ordered if c in df.columns]]



    def fno_live_most_active_futures_contracts(self, mode="Volume"):
        """Vectorized Most Active Futures with 100% legacy parity."""
        data = self._get('https://www.nseindia.com/api/snapshot-derivatives-equity?index=futures', 'https://www.nseindia.com/market-data/most-active-contracts')
        df = self._to_df(data, mode.lower(), "data")
        if df is not None and not df.empty:
            cols = ['expiryDate', 'highPrice', 'identifier', 'instrument', 'instrumentType', 'lastPrice', 'lowPrice', 'numberOfContractsTraded', 'openInterest', 'openPrice', 'optionType', 'pChange', 'premiumTurnover', 'strikePrice', 'totalTurnover', 'underlying', 'underlyingValue']
            return df[[c for c in cols if c in df.columns]]
        return None

    def fno_live_most_active(self, mode="Index", opt="Call", sort_by="Volume"):
        """Vectorized Most Active with 100% legacy parity."""
        sort_map = {"Volume": "vol", "Value": "val"}
        suffix = sort_map.get(sort_by.capitalize(), "vol")
        api_index = f"{opt.lower()}s-{'index' if mode.capitalize() == 'Index' else 'stocks'}-{suffix}"
        key = "OPTIDX" if mode.capitalize() == 'Index' else "OPTSTK"
        data = self._get(f"https://www.nseindia.com/api/snapshot-derivatives-equity?index={api_index}", 'https://www.nseindia.com/market-data/most-active-contracts')
        df = self._to_df(data, key, "data")
        if df is not None and not df.empty:
            cols = ['expiryDate', 'identifier', 'instrument', 'instrumentType', 'lastPrice', 'numberOfContractsTraded', 'openInterest', 'optionType', 'pChange', 'premiumTurnover', 'strikePrice', 'totalTurnover', 'underlying', 'underlyingValue']
            return df[[c for c in cols if c in df.columns]]
        return None

    def fno_live_most_active_contracts_by_oi(self):
        """Vectorized OI Most Active matching legacy exactly."""
        url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=oi'
        data = self._get(url, 'https://www.nseindia.com/market-data/most-active-contracts')
        df = self._to_df(data, "volume", "data")
        return df if df is not None and not df.empty else None

    def fno_live_most_active_contracts_by_volume(self):
        url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=contracts'
        data = self._get(url, 'https://www.nseindia.com/market-data/most-active-contracts')
        df = self._to_df(data, "volume", "data")
        return df if not df.empty else None

    def fno_live_most_active_options_contracts_by_volume(self):
        url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=options&limit=20'
        data = self._get(url, 'https://www.nseindia.com/market-data/most-active-contracts')
        df = self._to_df(data, "volume", "data")
        return df if not df.empty else None
    

    def fno_live_most_active_underlying(self):
        """Vectorized Most Active Underlying matching legacy summary exactly."""
        data = self._get("https://www.nseindia.com/api/live-analysis-most-active-underlying", "https://www.nseindia.com/market-data/most-active-underlying")
        df = self._to_df(data, "data")
        if df is not None:
            mapping = {
                'symbol': 'Symbol', 'futVolume': 'Fut Vol (Cntr)', 'optVolume': 'Opt Vol (Cntr)', 'totVolume': 'Total Vol (Cntr)',
                'futTurnover': 'Fut Val (₹ Lakhs)', 'preTurnover': 'Opt Val (₹ Lakhs)(Premium)', 'totTurnover': 'Total Val (₹ Lakhs)',
                'latestOI': 'OI (Cntr)', 'underlying': 'Underlying'
            }
            df.rename(columns=mapping, inplace=True)
            order = ['Symbol', 'Fut Vol (Cntr)', 'Opt Vol (Cntr)', 'Total Vol (Cntr)', 'Fut Val (₹ Lakhs)', 'Opt Val (₹ Lakhs)(Premium)', 'Total Val (₹ Lakhs)', 'OI (Cntr)', 'Underlying']
            df = df[[c for c in order if c in df.columns]]
        return df
        
    def fno_live_change_in_oi(self):
        data = self._get('https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings', 'https://www.nseindia.com/market-data/oi-spurts')
        df = self._to_df(data, "data")
        if df is not None:
            mapping = {
                'symbol': 'Symbol', 'latestOI': 'Latest OI', 'prevOI': 'Prev OI', 'changeInOI': 'chng in OI', 'avgInOI': 'chng in OI %',
                'volume': 'Vol (Cntr)', 'futValue': 'Fut Val (₹ Lakhs)', 'premValue': 'Opt Val (₹ Lakhs)(Premium)', 'total': 'Total Val (₹ Lakhs)', 'underlyingValue': 'Price'
            }
            df.rename(columns=mapping, inplace=True)
            order = ['Symbol','Latest OI','Prev OI','chng in OI','chng in OI %','Vol (Cntr)', 'Fut Val (₹ Lakhs)','Opt Val (₹ Lakhs)(Premium)','Total Val (₹ Lakhs)','Price']
            df = df[[c for c in order if c in df.columns]]
        return df

    def fno_live_oi_vs_price(self):
        """Vectorized OI vs Price Signal analyzer matching legacy columns."""
        data = self._get('https://www.nseindia.com/api/live-analysis-oi-spurts-contracts', 'https://www.nseindia.com/market-data/oi-spurts')
        if not data: return None
        
        blocks = data.get("data", [])
        dfs = [pd.DataFrame(contracts).assign(OI_Price_Signal=cat) for block in blocks for cat, contracts in block.items() if contracts]
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        if df.empty: return None

        mapping = {
            'symbol': 'Symbol', 'instrument': 'Instrument', 'expiryDate': 'Expiry', 'optionType': 'Type', 'strikePrice': 'Strike',
            'ltp': 'LTP', 'prevClose': 'Prev Close', 'pChange': '% Price Chg', 'latestOI': 'Latest OI', 'prevOI': 'Prev OI',
            'changeInOI': 'Chg in OI', 'pChangeInOI': '% OI Chg', 'volume': 'Volume', 'turnover': 'Turnover ₹L',
            'premTurnover': 'Premium ₹L', 'underlyingValue': 'Underlying Price'
        }
        df.rename(columns=mapping, inplace=True)
        order = ['OI_Price_Signal', 'Symbol', 'Instrument', 'Expiry', 'Type', 'Strike', 'LTP', '% Price Chg', 'Latest OI', 'Prev OI', 'Chg in OI', '% OI Chg', 'Volume', 'Turnover ₹L', 'Premium ₹L', 'Underlying Price']
        df = df[[c for c in order if c in df.columns]]
        return df.replace([np.nan, np.inf, -np.inf], None)
        
    def fno_expiry_dates_raw(self, symbol: str = "NIFTY"):
        """Unified Expiry Date Fetcher (Stage 3)."""
        url = f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getOptionChainDropdown&symbol={symbol.upper()}"
        data = self._get(url, 'https://www.nseindia.com/option-chain')
        return data.get("expiryDates", []) if data else []

    def fno_expiry_dates(self, symbol="NIFTY", label_filter=None):
        data = self._get(f'https://www.nseindia.com/api/option-chain-contract-info?symbol={symbol}', 'https://www.nseindia.com/option-chain')
        dates_raw = (data.get('expiryDates') or data.get('records', {}).get('expiryDates')) if data else []
        if not dates_raw: return None
        
        dates = pd.to_datetime(dates_raw, format='%d-%b-%Y').sort_values().unique()
        now = datetime.now()
        dates = [d for d in dates if d.date() >= now.date()]
        if dates and dates[0].date() == now.date() and now.time() > dt_time(15, 30): dates.pop(0)
        if not dates: return None
        
        info = []
        for i, d in enumerate(dates):
            t = "Monthly Expiry" if (i+1 < len(dates) and dates[i+1].month != d.month) or i+1 == len(dates) else "Weekly Expiry"
            info.append(t)
            
        df = pd.DataFrame({"Expiry Date": [d.strftime("%d-%b-%Y") for d in dates], "Expiry Type": info})
        df["Label"] = ""; df["Days Remaining"] = [(d.date() - now.date()).days for d in dates]
        if not df.empty: df.loc[0, "Label"] = "Current"
        w_idx = df[df["Expiry Type"] == "Weekly Expiry"].index
        if any(w_idx > 0): df.loc[w_idx[w_idx > 0][0], "Label"] = "Next Week"
        m_idx = df[df["Expiry Type"] == "Monthly Expiry"].index
        if any(m_idx > 0): df.loc[m_idx[m_idx > 0][0], "Label"] = "Month"
        
        df["Contract Zone"] = [("Current Month" if d.month == now.month and d.year == now.year else "Next Month" if d.month == (now.month%12)+1 else "Quarterly" if d.month in [3,6,9,12] else "Far Month") for d in dates]
        if not label_filter: return df
        if label_filter == "All": return [pd.to_datetime(x).strftime("%d-%m-%Y") for x in df[df["Label"] != ""]["Expiry Date"]]
        res = df[df["Label"] == label_filter]
        return pd.to_datetime(res.iloc[0]["Expiry Date"]).strftime("%d-%m-%Y") if not res.empty else None

    def fno_live_option_chain_raw(self, symbol: str, expiry_date: str | None = None):
        symbol = symbol.upper().replace(' ', '%20').replace('&', '%26')
        return self._get(f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getOptionChainData&symbol={symbol}&params=expiryDate={expiry_date}", 'https://www.nseindia.com/option-chain')

    def fno_live_option_chain(self, symbol: str, expiry_date: str | None = None, oi_mode: str = "full"):
        """Super-Fast Vectorized Option Chain using GetQuoteApi (matches NseKit_old.py logic)."""
        symbol = symbol.upper().replace(' ', '%20').replace('&', '%26')
        
        # Step 1: Get drop down list to resolve target expiry and set cookies
        dropdown = self._get(f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getOptionChainDropdown&symbol={symbol}", "https://www.nseindia.com/option-chain")
        if not dropdown or 'expiryDates' not in dropdown: return None
        expiries = dropdown['expiryDates']
        
        target = expiry_date or expiries[0]
        if expiry_date:
             try:
                 dt_obj = pd.to_datetime(expiry_date, dayfirst=True)
                 target = dt_obj.strftime("%d-%b-%Y")
             except: target = expiry_date.strip()
        
        if target not in expiries: target = expiries[0]

        # Step 2: Fetch data using the same API as NseKit_old.py
        data = self._get(f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getOptionChainData&symbol={symbol}&params=expiryDate={target}", "https://www.nseindia.com/option-chain")
        if not data or 'data' not in data: return None
        
        underlying_price = data.get('underlyingValue', 0)
        fetch_time = data.get("timestamp", datetime.now().strftime("%d-%b-%Y %H:%M:%S"))
        
        df = pd.DataFrame(data['data'])
        if df.empty: return None

        # CE/PE mappings from NseKit_old.py
        ce_map = {'openInterest': 'CALLS_OI', 'changeinOpenInterest': 'CALLS_Chng_in_OI', 'totalTradedVolume': 'CALLS_Volume', 
                  'impliedVolatility': 'CALLS_IV', 'lastPrice': 'CALLS_LTP', 'change': 'CALLS_Net_Chng',
                  'buyPrice1': 'CALLS_Bid_Price', 'sellPrice1': 'CALLS_Ask_Price'}
        pe_map = {'openInterest': 'PUTS_OI', 'changeinOpenInterest': 'PUTS_Chng_in_OI', 'totalTradedVolume': 'PUTS_Volume', 
                  'impliedVolatility': 'PUTS_IV', 'lastPrice': 'PUTS_LTP', 'change': 'PUTS_Net_Chng',
                  'buyPrice1': 'PUTS_Bid_Price', 'sellPrice1': 'PUTS_Ask_Price'}

        def _ext(col, map_dict):
            if col not in df.columns or df[col].isna().all(): return pd.DataFrame(index=df.index)
            temp_df = pd.json_normalize(df[col].dropna())
            temp_df.index = df[col].dropna().index
            # Only keep columns we actually want to map
            temp_df = temp_df[[c for c in map_dict.keys() if c in temp_df.columns]]
            return temp_df.rename(columns=map_dict)

        ce_df = _ext('CE', ce_map)
        pe_df = _ext('PE', pe_map)
        
        if 'CE' in df.columns and not df['CE'].isna().all():
            ce_raw = pd.json_normalize(df['CE'].dropna())
            ce_raw.index = df['CE'].dropna().index
            ce_df['CALLS_Bid_Qty'] = ce_raw.get('totalBuyQuantity', ce_raw.get('buyQuantity1', 0)).fillna(0)
            ce_df['CALLS_Ask_Qty'] = ce_raw.get('totalSellQuantity', ce_raw.get('sellQuantity1', 0)).fillna(0)

        if 'PE' in df.columns and not df['PE'].isna().all():
            pe_raw = pd.json_normalize(df['PE'].dropna())
            pe_raw.index = df['PE'].dropna().index
            pe_df['PUTS_Bid_Qty'] = pe_raw.get('totalBuyQuantity', pe_raw.get('buyQuantity1', 0)).fillna(0)
            pe_df['PUTS_Ask_Qty'] = pe_raw.get('totalSellQuantity', pe_raw.get('sellQuantity1', 0)).fillna(0)

        final_df = df[['strikePrice']].join(ce_df, how='left').join(pe_df, how='left')
        final_df['Fetch_Time'] = fetch_time
        final_df['Symbol'] = symbol.replace('%20', ' ').replace('%26', '&')
        final_df['Expiry_Date'] = target
        final_df['Underlying_Value'] = underlying_price
        final_df.rename(columns={'strikePrice': 'Strike_Price'}, inplace=True)

        if oi_mode.lower() == "compact":
            order = ['Fetch_Time', 'Symbol', 'Expiry_Date', 'CALLS_OI', 'CALLS_Chng_in_OI', 'CALLS_Volume', 'CALLS_IV', 'CALLS_LTP', 'CALLS_Net_Chng', 
                    'Strike_Price', 'PUTS_OI', 'PUTS_Chng_in_OI', 'PUTS_Volume', 'PUTS_IV', 'PUTS_LTP', 'PUTS_Net_Chng', 'Underlying_Value']
        else:
            order = ['Fetch_Time', 'Symbol', 'Expiry_Date', 'CALLS_OI', 'CALLS_Chng_in_OI', 'CALLS_Volume', 'CALLS_IV', 'CALLS_LTP', 'CALLS_Net_Chng', 
                    'CALLS_Bid_Qty', 'CALLS_Bid_Price', 'CALLS_Ask_Price', 'CALLS_Ask_Qty', 'Strike_Price', 
                    'PUTS_Bid_Qty', 'PUTS_Bid_Price', 'PUTS_Ask_Price', 'PUTS_Ask_Qty', 
                    'PUTS_Net_Chng', 'PUTS_LTP', 'PUTS_IV', 'PUTS_Volume', 'PUTS_Chng_in_OI', 'PUTS_OI', 'Underlying_Value']

        final_df = final_df[[c for c in order if c in final_df.columns]]
        return final_df.replace([np.nan, np.inf, -np.inf], None)

    def fno_live_active_contracts(self, symbol: str, expiry_date: str | None = None):
        """Vectorized Active Contracts matching legacy return type (list of dicts)."""
        data = self._get(f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getSymbolDerivativesData&symbol={symbol.upper().replace(' ','%20').replace('&','%26')}&instrumentType=OPT", f"https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}")
        if not data or 'data' not in data: return None
        recs = data['data']
        if expiry_date: recs = [r for r in recs if r.get('expiryDate') == expiry_date]
        table_data = []
        for c in recs:
            table_data.append({
                "Instrument Type": c.get("instrumentType", ""), "Expiry Date": c.get("expiryDate", ""),
                "Option Type": c.get("optionType", ""), "Strike Price": str(c.get("strikePrice", "")).strip(),
                "Open": c.get("openPrice", 0), "High": c.get("highPrice", 0), "Low": c.get("lowPrice", 0),
                "closePrice": c.get("closePrice", 0), "Prev Close": c.get("prevClose", 0), "Last": c.get("lastPrice", 0),
                "Change": c.get("change", 0), "%Change": c.get("pchange", 0), "Volume (Contracts)": c.get("totalTradedVolume", 0),
                "Value (₹ Lakhs)": round(c.get("totalTurnover", 0) / 100000, 2), "totalBuyQuantity": 0, "totalSellQuantity": 0,
                "OI": c.get("openInterest", 0), "Chng in OI": c.get("changeinOpenInterest", 0), "% Chg in OI": c.get("pchangeinOpenInterest", 0), "VWAP": 0
            })
        return table_data
    
    #---------------------------------------------------------- CM_Eod_Data ----------------------------------------------------------------

    def cm_eod_fii_dii_activity(self, exchange="All"):
        url = "https://www.nseindia.com/api/fiidiiTradeReact" if exchange == "All" else "https://www.nseindia.com/api/fiidiiTradeNse"
        data = self._get(url, 'https://www.nseindia.com/reports/fii-dii')
        return self._to_df(data)
   
    def cm_eod_market_activity_report(self, trade_date: str):
        """
        Download NSE Market Activity CSV and return raw rows (list of lists).
        Fast + Safe version.
        """
        self.rotate_user_agent()
        try:
            # Convert date
            trade_date = datetime.strptime(trade_date, "%d-%m-%y")
            url = f"https://nsearchives.nseindia.com/archives/equities/mkt/MA{trade_date.strftime('%d%m%y')}.csv"

            # Fetch CSV
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()

            # Decode safely (ignore bad chars) and split lines directly (fast)
            csv_text = nse_resp.content.decode("utf-8", errors="ignore")
            rows = list(csv.reader(csv_text.splitlines()))

            return rows if rows else None

        except Exception as e:
            print(f"❌ Error fetching Market Activity Report for {trade_date.strftime('%d-%b-%Y')}: {e}")
            return None
    
    def cm_eod_bhavcopy_with_delivery(self, trade_date: str):
        """Vectorized CM Bhavcopy with Delivery (Stage 3) - Legacy Matching."""
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{dt.strftime('%d%m%Y')}.csv"
        df = self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")
        if df is not None and not df.empty:
            # Match legacy NseKit_old.py: removed spaces from column names
            df.columns = [name.replace(' ', '') for name in df.columns]
            if 'SERIES' in df.columns: df['SERIES'] = df['SERIES'].astype(str).str.replace(' ', '')
            if 'DATE1' in df.columns: df['DATE1'] = df['DATE1'].astype(str).str.replace(' ', '')
        return df

    def cm_eod_equity_bhavcopy(self, trade_date: str):
        """Vectorized EOD Equity Bhavcopy."""
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{dt.strftime('%Y%m%d')}_F_0000.csv.zip"
        df = self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")
        return df[df['SctySrs'] == 'EQ'].reset_index(drop=True) if df is not None else None

    def cm_eod_52_week_high_low(self, trade_date: str):
        """Vectorized 52W High/Low download."""
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/content/CM_52_wk_High_low_{dt.strftime('%d%m%Y')}.csv"
        return self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")

    def cm_eod_bulk_deal(self):
        """Vectorized Bulk Deal Fetcher."""
        url = "https://nsearchives.nseindia.com/content/equities/bulk.csv"
        return self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")

    def cm_eod_block_deal(self):
        """Vectorized Block Deal Fetcher."""
        url = "https://nsearchives.nseindia.com/content/equities/block.csv"
        return self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")

    def cm_eod_shortselling(self, trade_date: str):
        """Vectorized Short Selling Fetcher."""
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/archives/equities/shortSelling/shortselling_{dt.strftime('%d%m%Y')}.csv"
        return self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")

    def cm_eod_surveillance_indicator(self, trade_date: str):
        """Vectorized Surveillance Indicator Fetcher."""
        dt = self._parse_date(trade_date, "%d-%m-%y")
        url = f"https://nsearchives.nseindia.com/content/cm/REG1_IND{dt.strftime('%d%m%y').upper()}.csv"
        df = self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")
        if df is not None and not df.empty:
            try:
                g_idx, f_idx = df.columns.get_loc('GSM'), df.columns.get_loc('Filler31')
                cols = df.columns[g_idx:f_idx+1]
                df[cols] = df[cols].replace(100, '').replace("100", "")
            except: pass
        return df

    def cm_eod_series_change(self):
        """Vectorized Series Change Fetcher."""
        url = "https://nsearchives.nseindia.com/content/equities/series_change.csv"
        return self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")

    def cm_eod_eq_band_changes(self, trade_date: str):
        """Vectorized Equity Band Changes Fetcher."""
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/content/equities/eq_band_changes_{dt.strftime('%d%m%Y').upper()}.csv"
        return self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")

    def cm_eod_eq_price_band(self, trade_date: str):
        """Vectorized Equity Price Band Fetcher."""
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/content/equities/sec_list_{dt.strftime('%d%m%Y').upper()}.csv"
        return self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")
        
    def cm_hist_eq_price_band(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        """Vectorized Historical Price Band Changes."""
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        url = f"https://www.nseindia.com/api/eqsurvactions?from_date={from_date}&to_date={to_date}&csv=true"
        if symbol: url += f"&symbol={symbol}"
        return self._get_csv_hist(url, "https://www.nseindia.com/reports/price-band-changes")

    def cm_eod_pe_ratio(self, trade_date: str):
        """Vectorized EOD PE Ratio Fetcher."""
        dt = self._parse_date(trade_date, "%d-%m-%y")
        url = f"https://nsearchives.nseindia.com/content/equities/peDetail/PE_{dt.strftime('%d%m%y').upper()}.csv"
        return self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")

    def cm_eod_mcap(self, trade_date: str):
        """Vectorized EOD MCAP Fetcher (Stage 3)."""
        dt = self._parse_date(trade_date, "%d-%m-%y")
        url = f"https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{dt.strftime('%d%m%y').upper()}.zip"
        resp = self._get(url, is_json=False)
        if not resp: return None
        with zipfile.ZipFile(BytesIO(resp.content)) as z:
            for f in z.namelist():
                if f.lower().startswith("mcap") and f.endswith(".csv"):
                    return pd.read_csv(z.open(f))
        return None

    def cm_eod_eq_name_change(self):
        """Vectorized Equity Name Change Fetcher."""
        url = "https://nsearchives.nseindia.com/content/equities/namechange.csv"
        df = self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")
        if df is not None and df.shape[1] >= 4:
            df.iloc[:, 3] = pd.to_datetime(df.iloc[:, 3], format='%d-%b-%Y', errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.sort_values(by=df.columns[3], ascending=False).reset_index(drop=True)
        return df

    def cm_eod_eq_symbol_change(self):
        """Vectorized Equity Symbol Change Fetcher."""
        url = "https://nsearchives.nseindia.com/content/equities/symbolchange.csv"
        df = self._get_csv_hist(url, "https://www.nseindia.com/all-reports-bhavcopy")
        if df is not None and df.shape[1] >= 4:
            df.iloc[:, 3] = pd.to_datetime(df.iloc[:, 3], format='%d-%b-%Y', errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.sort_values(by=df.columns[3], ascending=False).reset_index(drop=True)
        return df

   
    def cm_hist_security_wise_data(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        start_dt, end_dt = datetime.strptime(from_date, "%d-%m-%Y"), datetime.strptime(to_date, "%d-%m-%Y")
        all_data = []
        while start_dt <= end_dt:
            curr_end = min(start_dt + timedelta(days=89), end_dt)
            url = f"https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData?from={start_dt.strftime('%d-%m-%Y')}&to={curr_end.strftime('%d-%m-%Y')}&symbol={symbol}&type=priceVolumeDeliverable&series=ALL"
            data = self._get(url, "https://www.nseindia.com/report-detail/eq_security")
            if data and "data" in data: all_data.extend(data["data"])
            start_dt = curr_end + timedelta(days=1)
        
        r_map = {"CH_SYMBOL": "Symbol", "CH_SERIES": "Series", "mTIMESTAMP": "Date", "CH_PREVIOUS_CLS_PRICE": "Prev Close", "CH_OPENING_PRICE": "Open Price", "CH_TRADE_HIGH_PRICE": "High Price", "CH_TRADE_LOW_PRICE": "Low Price", "CH_LAST_TRADED_PRICE": "Last Price", "CH_CLOSING_PRICE": "Close Price", "VWAP": "VWAP", "CH_TOT_TRADED_QTY": "Total Traded Quantity", "CH_TOT_TRADED_VAL": "Turnover ₹", "CH_TOTAL_TRADES": "No. of Trades", "COP_DELIV_QTY": "Deliverable Qty", "COP_DELIV_PERC": "% Dly Qt to Traded Qty"}
        df = self._to_df(all_data, columns=list(r_map.keys()), rename=r_map, fill_value=None)
        if not df.empty:
            df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce").dt.strftime("%d-%b-%Y")
            df.sort_values("Date", inplace=True)
            df.drop_duplicates(subset=["Date"], keep="last", inplace=True)
        return df

    def cm_hist_bulk_deals(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        url = f"https://www.nseindia.com/api/historicalOR/bulk-block-short-deals?optionType=bulk_deals&from={from_date}&to={to_date}&csv=true"
        if symbol: url += f"&symbol={symbol}"
        return self._get_csv_hist(url, "https://www.nseindia.com/report-detail/display-bulk-and-block-deals")

    def cm_hist_block_deals(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        url = f"https://www.nseindia.com/api/historicalOR/bulk-block-short-deals?optionType=block_deals&from={from_date}&to={to_date}&csv=true"
        if symbol: url += f"&symbol={symbol}"
        return self._get_csv_hist(url, "https://www.nseindia.com/report-detail/display-bulk-and-block-deals")

    def cm_hist_short_selling(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        symbol, from_date, to_date = self._parse_args(*args, from_date=from_date, to_date=to_date, period=period, symbol=symbol)
        url = f"https://www.nseindia.com/api/historicalOR/bulk-block-short-deals?optionType=short_deals&from={from_date}&to={to_date}&csv=true"
        if symbol: url += f"&symbol={symbol}"
        return self._get_csv_hist(url, "https://www.nseindia.com/report-detail/display-bulk-and-block-deals")

    def cm_dmy_biz_growth(self, *args, mode="monthly", month=None, year=None):
        """Vectorized CM Growth Report matching legacyExactly (list of dicts)."""
        now = datetime.now()
        rev_m = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
        for a in args:
            if isinstance(a, str):
                U = a.upper()
                if U in ["YEARLY","MONTHLY","DAILY"]: mode = U.lower()
                elif U.isdigit() and len(U)==4: year = int(U)
            elif isinstance(a, int):
                if 1900<=a<=2100: year=a
                elif 1<=a<=12: month=a
        year, month = (year or now.year), (month or now.month)
        if mode=="yearly": url = "https://www.nseindia.com/api/historicalOR/cm/tbg/yearly"
        elif mode=="monthly": url = f"https://www.nseindia.com/api/historicalOR/cm/tbg/monthly?from={year}&to={year+1}"
        else: url = f"https://www.nseindia.com/api/historicalOR/cm/tbg/daily?month={rev_m.get(month,'Jan')}&year={str(year)[-2:]}"
        data = self._get(url, "https://www.nseindia.com")
        if not data or "data" not in data: return None
        d_list = [d["data"] for d in data.get("data",[]) if "data" in d]
        if not d_list: return None
        df = pd.DataFrame(d_list)
        if mode=="yearly": df.rename(columns={"GLY_MONTH_YEAR":"FY","GLY_NO_OF_CO_LISTED":"No_of_Cos_Listed","GTY_TURNOVER":"Turnover","GTY_MKT_CAP":"Market_Cap"}, inplace=True)
        elif mode=="monthly": df.rename(columns={"GLM_MONTH_YEAR":"Month","GLM_TURNOVER":"Turnover","GLM_MKT_CAP":"Market_Cap"}, inplace=True)
        return df.to_dict(orient='records')

    def cm_monthly_settlement_report(self, *args, from_year=None, to_year=None, period=None):
        """Vectorized CM Monthly report matching legacy exactly."""
        today = datetime.now()
        cur_fy = today.year if today.month >= 4 else today.year - 1
        for arg in args:
            if isinstance(arg, str):
                if re.match(r"^\d{4}$", arg):
                    if not from_year: from_year = int(arg)
                    elif not to_year: to_year = int(arg)
                elif arg.upper().endswith("Y"): period = arg.upper()
        if period and not from_year: from_year = cur_fy - int(period.replace("Y", ""))
        from_year = from_year or cur_fy
        to_year = to_year or (from_year + 1)
        all_data = []
        for f_s in range(from_year, to_year):
            fin_year = f"{f_s}-{f_s+1}"
            data = self._get(f"https://www.nseindia.com/api/historicalOR/monthly-sett-stats-data?finYear={fin_year}", "https://www.nseindia.com/report-detail/monthly-settlement-statistics")
            if data and "data" in data:
                for rec in data["data"]:
                    rec["FinancialYear"] = fin_year
                all_data.extend(data["data"])
        
        r_map = {"ST_DATE": "Month", "ST_SETTLEMENT_NO": "Settlement No", "ST_NO_OF_TRADES_LACS": "No of Trades (lakhs)", 
                 "ST_TRADED_QTY_LACS": "Traded Qty (lakhs)", "ST_DELIVERED_QTY_LACS": "Delivered Qty (lakhs)", 
                 "ST_PERC_DLVRD_TO_TRADED_QTY": "% Delivered to Traded Qty", "ST_TURNOVER_CRORES": "Turnover (₹ Cr)", 
                 "ST_DELIVERED_VALUE_CRORES": "Delivered Value (₹ Cr)", "ST_PERC_DLVRD_VAL_TO_TURNOVER": "% Delivered Value to Turnover", 
                 "ST_SHORT_DLVRY_AUC_QTY_LACS": "Short Delivery Qty (Lacs)", "ST_PERC_SHORT_DLVRY_TO_DLVRY": "% Short Delivery to Delivery", 
                 "ST_SHORT_DLVRY_VALUE": "Short Delivery Value (₹ Cr)", "ST_PERC_SHORT_DLVRY_VAL_DLVRY": "% Short Delivery Value to Delivery", 
                 "ST_FUNDS_PAYIN_CRORES": "Funds Payin (₹ Cr)"}
        df = self._to_df(all_data)
        if df is not None and not df.empty:
            df.rename(columns=r_map, inplace=True)
            if "Month" in df.columns:
                df["Month"] = pd.to_datetime(df["Month"], format="%b-%Y", errors="coerce").dt.strftime('%b-%Y')
            cols = ["Month", "Settlement No", "No of Trades (lakhs)", "Traded Qty (lakhs)", "Delivered Qty (lakhs)", "% Delivered to Traded Qty", "Turnover (₹ Cr)", "Delivered Value (₹ Cr)", "% Delivered Value to Turnover", "Short Delivery Qty (Lacs)", "% Short Delivery to Delivery", "Short Delivery Value (₹ Cr)", "% Short Delivery Value to Delivery", "Funds Payin (₹ Cr)", "FinancialYear"]
            return df[[c for c in cols if c in df.columns]]
        return None

    def cm_monthly_most_active_equity(self):
        data = self._get('https://www.nseindia.com/api/historicalOR/most-active-securities-monthly', 'https://www.nseindia.com/historical/most-active-securities')
        r_map = {'ASM_SECURITY': 'Security', 'ASM_NO_OF_TRADES': 'No. of Trades', 'ASM_TRADED_QUANTITY': 'Traded Quantity (Lakh Shares)', 'ASM_TURNOVER': 'Turnover (₹ Cr.)', 'ASM_AVG_DLY_TURNOVER': 'Avg Daily Turnover (₹ Cr.)', 'ASM_SHARE_IN_TOTAL_TURNOVER': 'Share in Total Turnover (%)', 'ASM_DATE': 'Month'}
        return self._to_df(data, "data", columns=list(r_map.keys()), rename=r_map)


    def historical_advances_decline(self, *args, mode="Month_wise", month=None, year=None):
        now = datetime.now()
        month_map = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
        for arg in args:
            if not isinstance(arg, str): continue
            arg_u = arg.upper()
            if arg_u in ["MONTH_WISE", "DAY_WISE"]: mode = arg_u
            elif arg_u.isdigit() and len(arg_u) == 4: year = int(arg_u)
            elif arg_u[:3] in month_map: month = month_map[arg_u[:3]]
        
        year = year or now.year
        month = month or (now.month - 1 or 12)
        if mode.upper() == "MONTH_WISE":
            url = f"https://www.nseindia.com/api/historicalOR/advances-decline-monthly?year={year}"
            r_map = {"ADM_MONTH": "Month", "ADM_ADVANCES": "Advances", "ADM_DECLINES": "Declines", "ADM_ADV_DCLN_RATIO": "Adv_Decline_Ratio"}
        else:
            m_code = {v: k for k, v in month_map.items()}.get(month, "JAN")
            url = f"https://www.nseindia.com/api/historicalOR/advances-decline-monthly?year={m_code}-{year}"
            r_map = {"ADD_DAY_STRING": "Day", "ADD_ADVANCES": "Advances", "ADD_DECLINES": "Declines", "ADD_ADV_DCLN_RATIO": "Adv_Decline_Ratio"}
        
        data = self._get(url, "https://www.nseindia.com")
        return self._to_df(data, "data", columns=list(r_map.keys()), rename=r_map)

    #---------------------------------------------------------- FnO_Eod_Data ----------------------------------------------------------------

    def fno_eod_bhav_copy(self, trade_date: str = ""):
        """Vectorized FnO Bhavcopy with legacy parity."""
        t_dt = datetime.strptime(trade_date, "%d-%m-%Y")
        url = f'https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{t_dt.strftime("%Y%m%d")}_F_0000.csv.zip'
        resp = self._get(url, is_json=False)
        if not resp: return None
        df = pd.read_csv(BytesIO(resp.content), compression='zip')
        if df is not None and not df.empty:
            try:
                df = df[~((df.iloc[:, 22] == 0) & (df.iloc[:, 23] == 0) & (df.iloc[:, 24] == 0) & (df.iloc[:, 25] == 0))]
                df = df.sort_values(by=df.columns[24], ascending=False)
            except: pass
            return df if not df.empty else None
        return None

    def _extract_csv_from_zip(self, content):
        with zipfile.ZipFile(BytesIO(content)) as z:
            for f in z.namelist():
                if f.endswith('.csv'): return pd.read_csv(z.open(f))
        return pd.DataFrame()

    def fno_eod_fii_stats(self, trade_date=None):
        dt = self._parse_date(trade_date)
        f_dt = dt.strftime("%d-%b-%Y")
        f_dt = f_dt[:3] + f_dt[3:].capitalize()
        url = f"https://nsearchives.nseindia.com/content/fo/fii_stats_{f_dt}.xls"
        resp = self._get(url, is_json=False)
        if resp:
            fmt = self.detect_excel_format(BytesIO(resp.content))
            engine = {'xls': 'xlrd', 'xlsx': 'openpyxl', 'xlsb': 'pyxlsb'}.get(fmt)
            if engine: return pd.read_excel(BytesIO(resp.content), engine=engine, dtype=str)
        return None

    def fno_eod_top10_fut(self, trade_date=None):
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/archives/fo/mkt/fo{dt.strftime('%d%m%Y')}.zip"
        resp = self._get(url, is_json=False)
        if resp:
            with zipfile.ZipFile(BytesIO(resp.content)) as z:
                for f in z.namelist():
                    if f.lower().startswith("ttfut") and f.endswith(".csv"):
                        return list(csv.reader(z.open(f).read().decode("utf-8", errors="ignore").splitlines()))
        return None

    def fno_eod_top20_opt(self, trade_date=None):
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/archives/fo/mkt/fo{dt.strftime('%d%m%Y')}.zip"
        resp = self._get(url, is_json=False)
        if resp:
            with zipfile.ZipFile(BytesIO(resp.content)) as z:
                for f in z.namelist():
                    if f.lower().startswith("ttopt") and f.endswith(".csv"):
                        return list(csv.reader(z.open(f).read().decode("utf-8", errors="ignore").splitlines()))
        return None

    def fno_eod_sec_ban(self, trade_date=None):
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/archives/fo/sec_ban/fo_secban_{dt.strftime('%d%m%Y')}.csv"
        resp = self._get(url, is_json=False)
        return pd.read_csv(BytesIO(resp.content)) if resp else None

    def fno_eod_mwpl_3(self, trade_date=None):
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/content/nsccl/mwpl_cli_{dt.strftime('%d%m%Y')}.xls"
        resp = self._get(url, is_json=False)
        if resp:
            fmt = self.detect_excel_format(BytesIO(resp.content))
            engine = {'xls': 'xlrd', 'xlsx': 'openpyxl', 'xlsb': 'pyxlsb'}.get(fmt)
            if engine:
                df = pd.read_excel(BytesIO(resp.content), engine=engine, dtype=str)
                df.dropna(how='all', inplace=True)
                df.columns = [str(c).strip() if not ("Unnamed" in str(c) or pd.isna(c)) else f"Client {i}" for i, c in enumerate(df.iloc[0], 1)]
                return df[1:].reset_index(drop=True)
        return None

    def fno_eod_combine_oi(self, trade_date=None):
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/archives/nsccl/mwpl/combineoi_{dt.strftime('%d%m%Y')}.zip"
        resp = self._get(url, is_json=False)
        return self._extract_csv_from_zip(resp.content) if resp else None

    def fno_eod_participant_wise_oi(self, trade_date=None):
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_{dt.strftime('%d%m%Y')}.csv"
        resp = self._get(url, is_json=False)
        return pd.read_csv(BytesIO(resp.content)) if resp else None

    def fno_eod_participant_wise_vol(self, trade_date=None):
        dt = self._parse_date(trade_date)
        url = f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_vol_{dt.strftime('%d%m%Y')}.csv"
        resp = self._get(url, is_json=False)
        return pd.read_csv(BytesIO(resp.content)) if resp else None

    def future_price_volume_data(self, symbol, instrument="Futures", expiry=None, from_date=None, to_date=None, period=None):
        return self._fo_price_volume_data(symbol, instrument, expiry, from_date, to_date, period)

    def option_price_volume_data(self, symbol, instrument="Options", expiry=None, option_type=None, strike_price=None, from_date=None, to_date=None, period=None):
        return self._fo_price_volume_data(symbol, instrument, expiry, from_date, to_date, period, option_type, strike_price)

    def _fo_price_volume_data(self, symbol, instrument, expiry=None, from_date=None, to_date=None, period=None, opt_type=None, strike=None):
        symbol, from_date, to_date = self._parse_args(symbol, from_date=from_date, to_date=to_date, period=period)
        
        # Legacy Expiry Formatting
        expiry_date = None
        if expiry:
            try:
                # Handle DD-MM-YYYY to DD-Mon-YYYY translation
                if "-" in str(expiry) and len(str(expiry).split("-")) == 3:
                    expiry_date = datetime.strptime(str(expiry), "%d-%m-%Y").strftime("%d-%b-%Y")
                else:
                    expiry_date = str(expiry).upper()
            except:
                expiry_date = str(expiry).upper()

        inst = "OPT" if "opt" in instrument.lower() else "FUT"
        inst += "IDX" if any(x in symbol.upper() for x in ["NIFTY", "BANK", "FIN"]) else "STK"
        
        # Translate from_date/to_date to DD-MM-YYYY for this specific API params
        f_str = datetime.strptime(from_date, "%d-%m-%Y").strftime("%d-%m-%Y")
        t_str = datetime.strptime(to_date, "%d-%m-%Y").strftime("%d-%m-%Y")

        params = {"from": f_str, "to": t_str, "instrumentType": inst, "symbol": symbol, "year": datetime.now().year, "csv": "true"}
        if expiry_date: params["expiryDate"] = expiry_date
        if opt_type: params["optionType"] = opt_type.upper()
        if strike: params["strikePrice"] = strike
        
        data = self._get("https://www.nseindia.com/api/historicalOR/foCPV", params=params)
        df = self._to_df(data, "data")
        if df is not None and not df.empty:
            df.columns = [c.upper().replace(" ", "_") for c in df.columns]
            for c in df.columns:
                if any(x in c for x in ["PRICE", "CLS", "VAL", "QTY", "OI"]): 
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            return df.sort_values("FH_TIMESTAMP")
        return None

    def fno_eom_lot_size(self, symbol=None):
        url = "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv"
        resp = self._get(url, is_json=False)
        if not resp: return None
        df = pd.read_csv(BytesIO(resp.content))
        df.columns = [c.strip() for c in df.columns]
        if symbol: df = df[df.iloc[:, 1].str.strip().str.upper() == symbol.upper()]
        return df

    def fno_dmy_biz_growth(self, *args, mode="monthly", month=None, year=None):
        """Vectorized FnO Growth matching legacyExactly (list of dicts)."""
        now = datetime.now()
        reverse_month_map = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
        for arg in args:
            if isinstance(arg, str):
                U = arg.upper()
                if U in ["YEARLY", "MONTHLY", "DAILY"]: mode = U.lower()
                elif U.isdigit() and len(U) == 4: year = int(U)
            elif isinstance(arg, int):
                if 1900 <= arg <= 2100: year = arg
                elif 1 <= arg <= 12: month = arg
        year, month = year or now.year, month or now.month
        
        y_comp = str(year) if mode == "daily" else str(year)
        if mode == "yearly": url = "https://www.nseindia.com/api/historicalOR/fo/tbg/yearly"
        elif mode == "monthly": url = f"https://www.nseindia.com/api/historicalOR/fo/tbg/monthly?from={year}&to={year+1}"
        else: url = f"https://www.nseindia.com/api/historicalOR/fo/tbg/daily?month={reverse_month_map.get(month, 'Jan')}&year={year}"
        
        data = self._get(url, "https://www.nseindia.com")
        if not data or "data" not in data: return None
        data_list = [d["data"] for d in data.get("data", []) if "data" in d]
        if not data_list: return None
        df = pd.DataFrame(data_list)
        r_map = {"date": "FY" if mode=="yearly" else "Month", "F&O_Total_VAL": "FO_Total_Val", "F&O_Total_QTY": "FO_Total_Qty"}
        df.rename(columns=r_map, inplace=True)
        return df.to_dict(orient='records')

    def fno_monthly_settlement_report(self, from_year=None, to_year=None):
        fy = from_year or (datetime.now().year if datetime.now().month >= 4 else datetime.now().year-1)
        ty = to_year or fy + 1
        all_data = []
        for y in range(fy, ty):
            data = self._get(f"https://www.nseindia.com/api/financial-monthlyStats?from_date=Apr-{y}&to_date=Mar-{y+1}")
            df = self._to_df(data)
            if not df.empty:
                df["FinancialYear"] = f"{y}-{y+1}"
                all_data.append(df)
        return pd.concat(all_data).reset_index(drop=True) if all_data else None

    #---------------------------------------------------------- SEBI_Data ----------------------------------------------------------------

    def sebi_circulars(self, period="1W", from_date=None, to_date=None, pages=1):
        """Stage 3 Vectorized SEBI Circular Fetcher."""
        today = datetime.now()
        if from_date:
            f_dt, t_dt = datetime.strptime(from_date, "%d-%m-%Y"), datetime.strptime(to_date or today.strftime("%d-%m-%Y"), "%d-%m-%Y")
        else:
            m = re.search(r'\d+', str(period))
            num = int(m.group()) if m else 1
            delta_days = {"D": 1, "W": 7, "M": 30, "Y": 365}.get(str(period)[-1].upper(), 7) * num
            f_dt, t_dt = today - timedelta(days=delta_days), today

        ajax_url = "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp"
        headers = {"User-Agent": self.headers["User-Agent"], "Referer": "https://www.sebi.gov.in/", "X-Requested-With": "XMLHttpRequest"}
        
        all_rows = []
        for p in range(1, pages + 1):
            payload = {"fromDate": f_dt.strftime("%d-%m-%Y"), "toDate": t_dt.strftime("%d-%m-%Y"), "ssText": "Circulars", "sid": "1", "ssid": "7", "nextValue": str(p), "totalpage": "1"}
            try:
                resp = requests.post(ajax_url, headers=headers, data=payload, timeout=15)
                soup = BeautifulSoup(resp.text, "html.parser")
                rows = soup.find_all("tr")[1:] # Skip header
                for tr in rows:
                    tds = tr.find_all("td")
                    if len(tds) < 2: continue
                    a = tds[1].find("a")
                    all_rows.append({
                        "Date": tds[0].text.strip(),
                        "Title": a.get("title", a.text).strip() if a else tds[1].text.strip(),
                        "Link": ("https://www.sebi.gov.in" + a.get("href")) if a and a.get("href") and not a.get("href").startswith("http") else (a.get("href") if a else "")
                    })
            except Exception: break
            
        df = pd.DataFrame(all_rows).drop_duplicates()
        if not df.empty:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.sort_values("Date", ascending=False)
            df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")
        return df

    def sebi_data(self, pages=1):
        """Alias for sebi_circulars."""
        return self.sebi_circulars(pages=pages)

    def quarterly_financial_results(self, symbol):
        """Vectorized Quarterly Financial matching legacy exactly (raw json)."""
        data = self._get(f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getIntegratedFilingData&symbol={symbol.upper()}")
        return data

    def list_of_indices(self):
        return self._get("https://www.nseindia.com/api/equity-master")

    def recent_annual_reports(self):
        """Vectorized Annual Reports from RSS with 100% legacy parity."""
        resp = self._get("https://nsearchives.nseindia.com/content/RSS/Annual_Reports.xml", is_json=False)
        if not resp: return pd.DataFrame()
        feed = feedparser.parse(resp.text)
        records = []
        pattern = r"(?:SME_)?AR_\d+_(?P<symbol>[A-Z0-9]+)_(?P<fyFrom>\d{4})_(?P<fyTo>\d{4})_"
        for item in feed.entries:
            link = item.get("link", "")
            m = re.search(pattern, link)
            if m:
                desc = item.get("description", "")
                date_match = re.search(r"(\d{2}-[A-Z]{3}-\d{2})", desc)
                sub_dt = None
                if date_match:
                    try:
                        sub_dt = datetime.strptime(date_match.group(1), "%d-%b-%y").strftime("%d-%b-%Y")
                    except: pass
                
                records.append({
                    "symbol": m.group("symbol"), "companyName": item.get("title", ""),
                    "fyFrom": int(m.group("fyFrom")), "fyTo": int(m.group("fyTo")), "link": link,
                    "submissionDate": sub_dt, "SME": "SME" if "SME_AR" in link else ""
                })
        return pd.DataFrame(records)

    def html_tables(self, url, output="json"):
        """Vectorized table scraper."""
        resp = self._get(url, is_json=False)
        if not resp: return []
        tables = pd.read_html(BytesIO(resp.content))
        return [t.to_dict(orient="records") for t in tables] if output == "json" else tables
