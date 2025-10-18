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
import random
import time
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import StringIO, BytesIO
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from rich.text import Text

class Nse:

    def __init__(self):
        self.session = requests.Session()
        self._initialize_session()

    def _initialize_session(self):
        """Initialize session with proper cookies and headers"""
        self.headers = {
            'User-Agent': self._get_random_user_agent(),
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.nseindia.com/',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
            'Origin': 'https://www.nseindia.com'
        }
        try:
            self.session.get("https://www.nseindia.com", headers=self.headers, timeout=10)
            self.session.get("https://www.nseindia.com/market-data/live-equity-market", 
                           headers=self.headers, timeout=10)
            time.sleep(1)
        except requests.RequestException:
            pass

    def _get_random_user_agent(self):
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
        ]
        return random.choice(user_agents)

    def rotate_user_agent(self):
        self.headers['User-Agent'] = self._get_random_user_agent()


    #---------------------------------------------------------- NSE ----------------------------------------------------------------

    def nse_market_status(self, mode: str = "Market Status"):
        """
        Fetch NSE Market Status data.

        Modes:
        -------
        - "Market Status" : Returns overall marketState DataFrame
        - "Mcap"          : Returns marketcap DataFrame
        - "Nifty50"       : Returns indicative Nifty 50 DataFrame
        - "Gift Nifty"    : Returns GIFT Nifty DataFrame
        - "All"           : Returns dictionary with all 4 DataFrames

        Returns:
        --------
        pd.DataFrame or dict[str, pd.DataFrame] or None
        """

        self.rotate_user_agent()

        ref_url = 'https://www.nseindia.com/market-data/live-equity-market'
        api_url = 'https://www.nseindia.com/api/marketStatus'

        try:
            # Step 1: Reference cookies
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Step 2: API request
            response = self.session.get(
                api_url,
                headers=self.headers,
                cookies=ref_response.cookies.get_dict(),
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            # Step 3: Initialize DFs
            market_state_df = market_cap_df = nifty50_df = gift_nifty_df = None

            # ===== MARKET STATE =====
            if 'marketState' in data and isinstance(data['marketState'], list):
                market_state_df = pd.DataFrame(data['marketState'])
                keep_cols = ['market', 'marketStatus', 'tradeDate', 'index', 'last',
                             'variation', 'percentChange', 'marketStatusMessage']
                market_state_df = market_state_df[[c for c in keep_cols if c in market_state_df.columns]]

            # ===== MARKET CAP =====
            if 'marketcap' in data and isinstance(data['marketcap'], dict):
                market_cap_df = pd.DataFrame([data['marketcap']])
                market_cap_df.rename(columns={
                    'timeStamp': 'Date',
                    'marketCapinTRDollars': 'MarketCap_USD_Trillion',
                    'marketCapinLACCRRupees': 'MarketCap_INR_LakhCr',
                    'marketCapinCRRupees': 'MarketCap_INR_Cr',
                }, inplace=True)

            # ===== NIFTY50 =====
            if 'indicativenifty50' in data and isinstance(data['indicativenifty50'], dict):
                nifty50_df = pd.DataFrame([data['indicativenifty50']])
                nifty50_df.rename(columns={
                    'dateTime': 'DateTime',
                    'indexName': 'Index',
                    'closingValue': 'ClosingValue',
                    'finalClosingValue': 'FinalClose',
                    'change': 'Change',
                    'perChange': 'PercentChange',
                }, inplace=True)

            # ===== GIFT NIFTY =====
            if 'giftnifty' in data and isinstance(data['giftnifty'], dict):
                gift_nifty_df = pd.DataFrame([data['giftnifty']])
                gift_nifty_df.rename(columns={
                    'SYMBOL': 'Symbol',
                    'EXPIRYDATE': 'ExpiryDate',
                    'LASTPRICE': 'LastPrice',
                    'DAYCHANGE': 'DayChange',
                    'PERCHANGE': 'PercentChange',
                    'CONTRACTSTRADED': 'ContractsTraded',
                    'TIMESTMP': 'Timestamp',
                }, inplace=True)

            # ===== RETURN BASED ON MODE =====
            mode = mode.strip().lower()

            if mode == "market status":
                return market_state_df
            elif mode == "mcap":
                return market_cap_df
            elif mode == "nifty50":
                return nifty50_df
            elif mode == "gift nifty":
                return gift_nifty_df
            elif mode == "all":
                return {
                    "Market Status": market_state_df,
                    "Mcap": market_cap_df,
                    "Nifty50": nifty50_df,
                    "Gift Nifty": gift_nifty_df
                }
            else:
                print(f"Invalid mode '{mode}'. Valid modes: 'Market Status', 'Mcap', 'Nifty50', 'Gift Nifty', 'All'.")
                return None

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching NSE Market Status: {e}")
            return None

    def nse_is_market_open(self, market: str = "Capital Market") -> Text:
        """
        Fetch NSE Market Status and return a Rich Text object with colored formatting.

        Open → green, Closed → red
        """
        self.rotate_user_agent()

        ref_url = "https://www.nseindia.com/market-data/live-equity-market"
        api_url = "https://www.nseindia.com/api/marketStatus"

        try:
            # Get session cookies
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Get market status
            response = self.session.get(api_url, headers=self.headers,
                                        cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()
            data = response.json()

            markets = data.get("marketState", [])
            selected = next((m for m in markets if m.get("market") == market), None)

            if not selected:
                text = Text(f"[{market}] → Market data not found.", style="bold yellow")
                return text

            status = selected.get('marketStatus', '').lower()
            message = selected.get('marketStatusMessage', '')

            if "open" in status:
                text = Text(f"[{market}] → {message} | Status: {selected.get('marketStatus')}", style="bold green")
            else:
                text = Text(f"[{market}] → {message} | Status: {selected.get('marketStatus')}", style="bold red")

            return text

        except Exception as e:
            return Text(f"Error fetching NSE Market Status: {e}", style="bold red")

    def nse_trading_holidays(self, list_only=False):
        self.rotate_user_agent()
        holiday_type = "trading"
        try:
            response = self.session.get(
                f"https://www.nseindia.com/api/holiday-master?type={holiday_type}",
                headers=self.headers, timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            # Extract only "CM" (Capital Market) holiday data
            if "CM" in data:
                df = pd.DataFrame(data["CM"], columns=["Sr_no", "tradingDate", "weekDay", "description", "morning_session", "evening_session"])
                
                if list_only:
                    return df["tradingDate"].tolist()
                return df
            else:
                return None
        except (requests.RequestException, ValueError) as e:
            print(f"Error fetching trading holidays: {e}")
            return None

    def nse_clearing_holidays(self, list_only=False):
        self.rotate_user_agent()
        holiday_type = "clearing"
        try:
            response = self.session.get(
                f"https://www.nseindia.com/api/holiday-master?type={holiday_type}",
                headers=self.headers, timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            # Extract only "CD" (Capital Market) holiday data
            if "CM" in data:
                df = pd.DataFrame(data["CD"], columns=["Sr_no", "tradingDate", "weekDay", "description", "morning_session", "evening_session"])
                
                if list_only:
                    return df["tradingDate"].tolist()
                return df
            else:
                return None
        except (requests.RequestException, ValueError) as e:
            print(f"Error fetching clearing holidays: {e}")
            return None
    
    def is_nse_trading_holiday(self, date_str=None):
        holidays = self.nse_trading_holidays(list_only=True)
        if holidays is None:
            return None
        date_format = "%d-%b-%Y"
        try:
            if date_str:
                date_obj = datetime.strptime(date_str, date_format)
            else:
                date_obj = datetime.today()
            formatted_date = date_obj.strftime(date_format)
            return formatted_date in holidays
        except ValueError:
            return None

    def is_nse_clearing_holiday(self, date_str=None):
        holidays = self.nse_clearing_holidays(list_only=True)
        if holidays is None:
            return None
        date_format = "%d-%b-%Y"
        try:
            if date_str:
                date_obj = datetime.strptime(date_str, date_format)
            else:
                date_obj = datetime.today()
            formatted_date = date_obj.strftime(date_format)
            return formatted_date in holidays
        except ValueError:
            return None

    def nse_live_market_turnover(self):
        self.rotate_user_agent()

        ref_url = 'https://www.nseindia.com/'
        api_url = 'https://www.nseindia.com/api/NextApi/apiClient?functionName=getMarketTurnoverSummary'

        try:
            # Step 1: Get cookies
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Step 2: Fetch API data
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()

            # Step 3: Parse JSON
            data = response.json().get('data', {})

            if not data:
                print("No data returned from API.")
                return pd.DataFrame()  # Return empty DataFrame instead of None

            all_data = []

            # Step 4: Iterate through each segment (equities, derivatives, etc.)
            for segment_name, records in data.items():
                if isinstance(records, list):
                    for item in records:
                        all_data.append({
                            "Segment": segment_name.upper(),
                            "Product": item.get("instrument", ""),
                            "Vol (Shares/Contracts)": item.get("volume", 0),
                            "Value (₹ Cr)": round(item.get("value", 0) / 1e7, 2),
                            "OI (Contracts)": item.get("oivalue", 0),
                            "No. of Orders#": item.get("noOfOrders", 0),
                            "No. of Trades": item.get("noOfTrades", 0),
                            "Avg Trade Value (₹)": item.get("averageTrade", 0),
                            "Updated At": item.get("mktTimeStamp", ""),
                            "Prev Vol": item.get("prevVolume", 0),
                            "Prev Value (₹ Cr)": round(item.get("prevValue", 0) / 1e7, 2),
                            "prev OI (Contracts)": item.get("prevOivalue", 0),
                            "prev Orders#": item.get("prevNoOfOrders", 0),
                            "prev Trades": item.get("prevNoOfTrades", 0),
                            "prev Avg Trade Value (₹)": item.get("prevAverageTrade", 0),       
                        })

            # Convert all data into a single DataFrame
            df_turnover = pd.DataFrame(all_data)

            # Clean up NaNs/Infs for Google Sheets
            df_turnover.replace([pd.NA, np.nan, float('inf'), float('-inf')], None, inplace=True)

            return df_turnover

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching market turnover data: {e}")
            return pd.DataFrame()  # Return empty DataFrame if error

    def nse_live_hist_circulars(self, from_date_str: str = None, to_date_str: str = None, filter: str = None):
        self.rotate_user_agent()

        # Default date range (yesterday to today if not provided)
        if from_date_str is None:
            from_date = datetime.now() - timedelta(days=1)
            from_date_str = from_date.strftime("%d-%m-%Y")
        if to_date_str is None:
            to_date = datetime.now()
            to_date_str = to_date.strftime("%d-%m-%Y")

        # Reference URL for cookies
        ref_url = 'https://www.nseindia.com/resources/exchange-communication-circulars'
        try:
            ref = requests.get(ref_url, headers=self.headers)
        except requests.RequestException as e:
            print(f"Failed to get reference cookies: {str(e)}")
            return pd.DataFrame(columns=["Date", "Circulars No", "Category", "Department", "Subject", "Attachment"])

        try:
            # API URL for circulars
            url = f"https://www.nseindia.com/api/circulars?&fromDate={from_date_str}&toDate={to_date_str}"
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            response.raise_for_status()
            json_data = response.json().get("data", [])

            # Handle empty or unexpected data
            if not isinstance(json_data, list) or not json_data:
                # print("❌ No NSE Circular data available")
                return pd.DataFrame(columns=["Date", "Circulars No", "Category", "Department", "Subject", "Attachment"])

            circulars = []
            for item in json_data:
                circulars.append({
                    "date": item.get("cirDisplayDate", ''),
                    "circulars": item.get("circDisplayNo", ''),
                    "category": item.get("circCategory", ''),
                    "department": item.get("circDepartment", ''),
                    "subject": item.get("sub", ''),
                    "attachment": item.get("circFilelink", ''),
                })

            # Create DataFrame
            df = pd.DataFrame(circulars)

            # Apply filtering
            if filter is not None:
                df = df[df['department'].str.contains(filter, case=False, na=False)]

            # Rename and reorder columns
            column_mapping = {
                "date": "Date",
                "circulars": "Circulars No",
                "category": "Category",
                "department": "Department",
                "subject": "Subject",
                "attachment": "Attachment",
            }
            df = df.rename(columns=column_mapping)
            df = df[["Date", "Circulars No", "Category", "Department", "Subject", "Attachment"]]

            # print(f"Final number of records in DataFrame: {len(df)}")
            return df

        except (requests.RequestException, ValueError, TypeError) as e:
            # print("No circulars available")
            return pd.DataFrame(columns=["Date", "Circulars No", "Category", "Department", "Subject", "Attachment"])

    def nse_live_hist_press_releases(self, from_date_str: str = None, to_date_str: str = None, filter: str = None):
        self.rotate_user_agent()

        # Default date range (yesterday to today if not provided)
        try:
            if from_date_str is None:
                from_date = datetime.now() - timedelta(days=1)
                from_date_str = from_date.strftime("%d-%m-%Y")
            else:
                datetime.strptime(from_date_str, "%d-%m-%Y")  # Validate date format

            if to_date_str is None:
                to_date = datetime.now()
                to_date_str = to_date.strftime("%d-%m-%Y")
            else:
                datetime.strptime(to_date_str, "%d-%m-%Y")  # Validate date format
        except ValueError as e:
            print(f"Invalid date format: {e}")
            return pd.DataFrame(columns=["DATE", "DEPARTMENT", "SUBJECT", "ATTACHMENT URL", "LAST UPDATED"])

        # Reference URL for cookies
        ref_url = 'https://www.nseindia.com/resources/exchange-communication-press-releases'
        try:
            ref = requests.get(ref_url, headers=self.headers, timeout=10)
            ref.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to fetch reference URL: {e}")
            return pd.DataFrame(columns=["DATE", "DEPARTMENT", "SUBJECT", "ATTACHMENT URL", "LAST UPDATED"])

        try:
            # API URL for press releases
            url = f"https://www.nseindia.com/api/press-release-cms20?fromDate={from_date_str}&toDate={to_date_str}"
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            response.raise_for_status()
            json_data = response.json()

            # Handle case when response is not a list
            if not isinstance(json_data, list):
                print("No press releases found")
                return pd.DataFrame(columns=["DATE", "DEPARTMENT", "SUBJECT", "ATTACHMENT URL", "LAST UPDATED"])

            press_releases = []
            for item in json_data:
                if not isinstance(item, dict) or 'content' not in item:
                    continue  # Skip invalid items

                content = item['content']

                # Clean HTML from subject
                subject_raw = content.get('body', '')
                subject_clean = subject_raw  # Default to raw text as fallback
                if subject_raw and isinstance(subject_raw, str):
                    # Check if content resembles HTML (basic heuristic)
                    if '<' in subject_raw and '>' in subject_raw:
                        try:
                            with warnings.catch_warnings():
                                warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
                                soup = BeautifulSoup(subject_raw, "html.parser")
                                subject_clean = soup.get_text(separator=' ').strip()
                        except Exception as e:
                            print(f"Failed to parse HTML for subject: {e}")
                    else:
                        # Treat as plain text and strip whitespace
                        subject_clean = subject_raw.strip()

                # Format 'changed' field
                changed_raw = item.get('changed', '')
                try:
                    last_updated_ts = datetime.strptime(changed_raw, "%a, %m/%d/%Y - %H:%M")
                    last_updated_str = last_updated_ts.strftime("%a %d-%b-%Y %I:%M %p")
                except ValueError:
                    last_updated_str = changed_raw  # Fallback to raw string

                press_releases.append({
                    "date": content.get('field_date', ''),
                    "subject": subject_clean,
                    "department": content.get('field_type', ''),
                    "attachment_url": content.get('field_file_attachement', {}).get('url') if content.get('field_file_attachement') else None,
                    "changed": last_updated_str
                })

            if not press_releases:
                print("No press releases data available")
                return pd.DataFrame(columns=["DATE", "DEPARTMENT", "SUBJECT", "ATTACHMENT URL", "LAST UPDATED"])

            # Create DataFrame
            df = pd.DataFrame(press_releases)

            # Apply filtering
            if filter is not None:
                df = df[df['department'].str.contains(filter, case=False, na=False)]

            # Rename and reorder columns
            column_mapping = {
                "date": "DATE",
                "subject": "SUBJECT",
                "department": "DEPARTMENT",
                "attachment_url": "ATTACHMENT URL",
                "changed": "LAST UPDATED"
            }
            df = df.rename(columns=column_mapping)
            df = df[["DATE", "DEPARTMENT", "SUBJECT", "ATTACHMENT URL", "LAST UPDATED"]]

            # print(f"Final number of records in DataFrame: {len(df)}")
            return df

        except (requests.RequestException, ValueError, TypeError) as e:
            print(f"Error fetching press releases: {e}")
            return pd.DataFrame(columns=["DATE", "DEPARTMENT", "SUBJECT", "ATTACHMENT URL", "LAST UPDATED"])
 
    def nse_reference_rates(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com'
        api_url = 'https://www.nseindia.com/api/NextApi/apiClient?functionName=getReferenceRates&&type=null&&flag=CUR'

        try:
            # Get reference cookies
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Fetch API data
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()

            data = response.json()
            
            # Extract currencySpotRates safely
            currency_data = data.get('data', {}).get('currencySpotRates', [])
            if currency_data:
                df = pd.DataFrame(currency_data)
                columns = ['currency', 'unit', 'value', 'prevDayValue']
                df = df[columns]

                # Fix NaN / Infinite values
                df = df.fillna(0)
                df = df.replace({float('inf'): 0, float('-inf'): 0})

                return df if not df.empty else None

            return None  # No currency data found

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching reference rates: {e}")
            return None

    def nse_eod_top10_nifty50(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%y")
        url = f"https://nsearchives.nseindia.com/content/indices/top10nifty50_{str(trade_date.strftime('%d%m%y').upper())}.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None
        
    def nse_6m_nifty_50(self, list_only=False):
        self.rotate_user_agent()
        url = "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv"
        try:
            nse_resp = self.session.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            data_df = pd.read_csv(BytesIO(nse_resp.content))
            data_df.columns = data_df.columns.str.strip()  # Clean column names
            data_df = data_df[['Company Name', 'Industry', 'Symbol', 'Series', 'ISIN Code']]
            if list_only:
                return data_df['Symbol'].tolist()
            return data_df
        except (requests.RequestException, ValueError) as e:
            print("Error fetching Nifty 50 list:", e)
            return None
    
    def nse_eom_fno_full_list(self, mode: str = "stocks", list_only: bool = False):
        """
        Fetch NSE End-of-Month (EoM) F&O Full List — Underlyings or Indices.

        Parameters
        ----------
        mode : str, default 'stocks'
            Type of list to fetch.
            Options:
                'stocks' -> Underlying (Equity) List
                'index'  -> Index List
        list_only : bool, default False
            If True, return only a list of symbols.
            If False, return a DataFrame with details.

        Returns
        -------
        list or pd.DataFrame or None
            Returns list of symbols if list_only=True.
            Returns DataFrame with serial number, symbol, and underlying if list_only=False.
            Returns None on failure.
        """
        self.rotate_user_agent()
        ref_url = "https://www.nseindia.com/products-services/equity-derivatives-list-underlyings-information"
        api_url = "https://www.nseindia.com/api/underlying-information"

        try:
            # Step 1: Get reference cookies
            ref_resp = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_resp.raise_for_status()

            # Step 2: API call with cookies
            data_resp = self.session.get(api_url, headers=self.headers, cookies=ref_resp.cookies.get_dict(), timeout=10)
            data_resp.raise_for_status()
            data_dict = data_resp.json()

            # Step 3: Determine which list to fetch
            mode = mode.strip().lower()
            if mode == "index":
                data_df = pd.DataFrame(data_dict["data"]["IndexList"])
            elif mode == "stocks":
                data_df = pd.DataFrame(data_dict["data"]["UnderlyingList"])
            else:
                raise ValueError("Invalid mode. Choose either 'stocks' or 'index'.")

            # Step 4: Standardize columns
            data_df = data_df.rename(columns={
                "serialNumber": "Serial Number",
                "symbol": "Symbol",
                "underlying": "Underlying"
            })

            # Step 5: Return list or DataFrame
            if list_only:
                return data_df["Symbol"].tolist()

            return data_df[["Serial Number", "Symbol", "Underlying"]]

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching NSE EoM F&O list: {e}")
            return None

    def nse_6m_nifty_500(self, list_only=False):
        self.rotate_user_agent()
        url = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
        try:
            nse_resp = self.session.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            data_df = pd.read_csv(BytesIO(nse_resp.content))
            data_df.columns = data_df.columns.str.strip()  # Remove leading/trailing spaces
            data_df = data_df[['Company Name', 'Industry', 'Symbol', 'Series', 'ISIN Code']]
            if list_only:
                return data_df['Symbol'].tolist()
            return data_df
        except (requests.RequestException, ValueError) as e:
            print("Error fetching Nifty 500 list:", e)
            return None
    
    def nse_eod_equity_full_list(self, list_only=False):
        self.rotate_user_agent()
        url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        try:
            nse_resp = self.session.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            data_df = pd.read_csv(BytesIO(nse_resp.content))
            data_df = data_df[['SYMBOL', 'NAME OF COMPANY', ' SERIES', ' DATE OF LISTING', ' FACE VALUE']]
            if list_only:
                return data_df['SYMBOL'].tolist()
            return data_df
        except (requests.RequestException, ValueError):
            return None

    #---------------------------------------------------------- IPO ----------------------------------------------------------------

    def ipo_current(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com/market-data/all-upcoming-issues-ipo'
        api_url = 'https://www.nseindia.com/api/ipo-current-issue'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()  # Ensure request was successful

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Convert response to JSON
            data = response.json()

            if isinstance(data, list):
                # Create DataFrame from the JSON data
                df = pd.DataFrame(data)

                # Define columns to match the JSON structure
                columns = ['symbol', 'companyName', 'series', 'issueStartDate', 'issueEndDate', 'status', 
                           'issueSize', 'issuePrice', 'noOfSharesOffered', 'noOfsharesBid', 'noOfTime']
                
                # Ensure DataFrame has the correct columns
                df = df[columns]

                # Fix NaN values to avoid issues
                df = df.fillna(0)  # Replace NaN with 0
                df = df.replace({float('inf'): 0, float('-inf'): 0})  # Replace infinite values with 0

                return df if not df.empty else None  # Return DataFrame if not empty
            return None  # Return None if no data is found

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching IPO data: {e}")
            return None
        
    def ipo_preopen(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com/market-data/new-stock-exchange-listings-today'
        api_url = 'https://www.nseindia.com/api/special-preopen-listing'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()  # Ensure request was successful

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Convert response to JSON
            data = response.json()

            if 'data' in data and isinstance(data['data'], list):
                # Create a list to store flattened data
                flattened_data = []
                
                for item in data['data']:
                    # Extract preopenBook fields
                    preopen_book = item.get('preopenBook', {})
                    preopen = preopen_book.get('preopen', [{}])[0] if preopen_book.get('preopen') else {}
                    ato = preopen_book.get('ato', {})

                    # Flatten the data into a single dictionary
                    flattened_item = {
                        'symbol': item.get('symbol', ''),
                        'series': item.get('series', ''),
                        'prevClose': item.get('prevClose', ''),
                        'iep': item.get('iep', ''),
                        'change': item.get('change', ''),
                        'perChange': item.get('perChange', ''),
                        'ieq': item.get('ieq', ''),
                        'ieVal': item.get('ieVal', ''),
                        'buyOrderCancCnt': item.get('buyOrderCancCnt', ''),
                        'buyOrderCancVol': item.get('buyOrderCancVol', ''),
                        'sellOrderCancCnt': item.get('sellOrderCancCnt', ''),
                        'sellOrderCancVol': item.get('sellOrderCancVol', ''),
                        'isin': item.get('isin', ''),
                        'status': item.get('status', ''),
                        # New fields from preopenBook
                        'preopen_buyQty': preopen.get('buyQty', 0),
                        'preopen_sellQty': preopen.get('sellQty', 0),
                        'ato_totalBuyQuantity': ato.get('totalBuyQuantity', 0),
                        'ato_totalSellQuantity': ato.get('totalSellQuantity', 0),
                        'totalBuyQuantity': preopen_book.get('totalBuyQuantity', 0),
                        'totalSellQuantity': preopen_book.get('totalSellQuantity', 0),
                        'totTradedQty': preopen_book.get('totTradedQty', 0),
                        'lastUpdateTime': preopen_book.get('lastUpdateTime', '')
                    }
                    flattened_data.append(flattened_item)

                # Create DataFrame from the flattened data
                df = pd.DataFrame(flattened_data)

                # Fix NaN values to avoid issues
                df = df.fillna(0)  # Replace NaN with 0
                df = df.replace({float('inf'): 0, float('-inf'): 0})  # Replace infinite values with 0

                return df if not df.empty else None  # Return DataFrame if not empty
            return None  # Return None if no data is found

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching Special Pre-Open data: {e}")
            return None        

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
        self.rotate_user_agent()

        ref_url = "https://www.nseindia.com/ipo-tracker?type=ipo_year"
        api_url = "https://www.nseindia.com/api/NextApi/apiClient?functionName=getIPOTrackerSummary"

        try:
            # Step 1: Fetch cookies from NSE IPO page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()
            cookies = ref_response.cookies.get_dict()

            # Step 2: Fetch API JSON data
            response = self.session.get(api_url, headers=self.headers, cookies=cookies, timeout=10)
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, dict) or "data" not in data:
                print("⚠️ Unexpected API format or empty response.")
                return None

            df = pd.DataFrame(data["data"])
            if df.empty:
                print("⚠️ No IPO data available.")
                return None

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

            return df.reset_index(drop=True)

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching IPO Tracker Summary data: {e}")
            return None


    #---------------------------------------------------------- Pre-Open Market ----------------------------------------------------------------

    def pre_market_nifty_info(self, category='All'):
        pre_market_xref = {"NIFTY 50": "NIFTY", "Nifty Bank": "BANKNIFTY", "Emerge": "SME", "Securities in F&O": "FO", "Others": "OTHERS", "All": "ALL"}
        
        self.rotate_user_agent()
        ref_url = 'https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market'
        
        try:
            ref = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref.raise_for_status()
            
            url = f"https://www.nseindia.com/api/market-data-pre-open?key={pre_market_xref.get(category, 'ALL')}"
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract values from 'niftyPreopenStatus'
            nifty_status = data.get("niftyPreopenStatus", {})
            pChange = nifty_status.get("pChange", "N/A")
            change = nifty_status.get("change", "N/A")
            lastPrice = nifty_status.get("lastPrice", "N/A")
            
            # Extract Advances, Declines, Unchanged & Timestamp
            advances = data.get("advances", 0)
            declines = data.get("declines", 0)
            unchanged = data.get("unchanged", 0)
            timestamp = data.get("timestamp", "Unknown")

            # Create a single-row DataFrame
            df = pd.DataFrame([{
                "lastPrice": lastPrice,
                "change": change,
                "pChange": pChange,
                "advances": advances,
                "declines": declines,
                "unchanged": unchanged,
                "timestamp": timestamp
            }])
            return df
        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching Nifty Advance-Decline data: {e}")
            return None

    def pre_market_info(self, category='All'):
        pre_market_xref = {"NIFTY 50": "NIFTY", "Nifty Bank": "BANKNIFTY", "Emerge": "SME", "Securities in F&O": "FO", "Others": "OTHERS", "All": "ALL"}

        self.rotate_user_agent()
        ref_url = 'https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market'
        ref = requests.get(ref_url, headers=self.headers)
        url = f"https://www.nseindia.com/api/market-data-pre-open?key={pre_market_xref[category]}"
        try:
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            response.raise_for_status()
            data = response.json()["data"]
            processed_data = [{
                "symbol": i["metadata"]["symbol"],
                "previousClose": i["metadata"]["previousClose"],
                "iep": i["metadata"]["iep"],
                "change": i["metadata"]["change"],
                "pChange": i["metadata"]["pChange"],
                "lastPrice": i["metadata"]["lastPrice"],
                "finalQuantity": i["metadata"]["finalQuantity"],
                "totalTurnover": i["metadata"]["totalTurnover"],
                "marketCap": i["metadata"]["marketCap"],
                "yearHigh": i["metadata"]["yearHigh"],
                "yearLow": i["metadata"]["yearLow"],
                "totalBuyQuantity": i["detail"]["preOpenMarket"]["totalBuyQuantity"],
                "totalSellQuantity": i["detail"]["preOpenMarket"]["totalSellQuantity"],
                "atoBuyQty": i["detail"]["preOpenMarket"]["atoBuyQty"],
                "atoSellQty": i["detail"]["preOpenMarket"]["atoSellQty"],
                "lastUpdateTime": i["detail"]["preOpenMarket"]["lastUpdateTime"]
            } for i in data]
            df = pd.DataFrame(processed_data)
            df = df.set_index("symbol", drop=False)
            return df
        except (requests.RequestException, ValueError):
            return None

    def pre_market_all_nse_adv_dec_info(self, category='All'):
        pre_market_xref = {"NIFTY 50": "NIFTY", "Nifty Bank": "BANKNIFTY", "Emerge": "SME", "Securities in F&O": "FO", "Others": "OTHERS", "All": "ALL"}
        
        self.rotate_user_agent()
        ref_url = 'https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market'
        
        try:
            ref = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref.raise_for_status()
            
            url = f"https://www.nseindia.com/api/market-data-pre-open?key={pre_market_xref.get(category, 'ALL')}"
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract Advances, Declines, Unchanged & Timestamp
            advances = data.get("advances", 0)
            declines = data.get("declines", 0)
            unchanged = data.get("unchanged", 0)
            timestamp = data.get("timestamp", "Unknown")

            # Create a single-row DataFrame
            df = pd.DataFrame([{
                "advances": advances,
                "declines": declines,
                "unchanged": unchanged,
                "timestamp": timestamp
            }])
            return df
        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching All NSE Advance-Decline data: {e}")
            return None

    #---------------------------------------------------------- Index_Live_Data ----------------------------------------------------------------
    
    def index_live_all_indices_data(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com/market-data/index-performances'
        api_url = 'https://www.nseindia.com/api/allIndices'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()  # Ensure request was successful

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Convert response to JSON
            data = response.json()
            
            if 'data' in data and isinstance(data['data'], list):
                df = pd.DataFrame(data['data'])

                # Selecting and ordering columns
                columns = ['key', 'index', 'indexSymbol', 'last', 'variation', 'percentChange', 'open', 'high', 'low',
                        'previousClose', 'yearHigh', 'yearLow', 'pe', 'pb', 'dy', 'declines', 'advances', 'unchanged',
                        'perChange30d', 'perChange365d', 'previousDayVal', 'oneWeekAgoVal', 'oneMonthAgoVal', 'oneYearAgoVal']
                
                df = df[columns]

                # **Fix NaN values** to avoid JSON conversion issues
                df = df.fillna(0)  # Replace NaN with 0
                df = df.replace({float('inf'): 0, float('-inf'): 0})  # Replace infinite values with 0

                return df if not df.empty else None  # Return DataFrame if not empty
            return None  # Return None if no data is found

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching all indices data: {e}")
            return None

    def index_live_indices_stocks_data(self, category, list_only=False):
        category = category.upper().replace('&', '%26').replace(' ', '%20')
        self.rotate_user_agent()
        url = f"https://www.nseindia.com/api/equity-stockIndices?index={category}"
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            data = response.json()["data"]
            df = pd.DataFrame(data)

            # Remove 'meta' column if it exists
            df = df.drop(["meta"], axis=1, errors='ignore')

            # Set index to 'symbol' for better readability
            df = df.set_index("symbol", drop=False)

            # If only the symbol list is required
            if list_only:
                symbol_list = sorted(df.index.tolist())
                return symbol_list

            # Reorder columns as per your requirement
            column_order = [
                "symbol", "previousClose", "open", "dayHigh", "dayLow", "lastPrice", 
                "change", "pChange", "totalTradedVolume", "totalTradedValue", 
                "nearWKH", "nearWKL", "perChange30d", "perChange365d", "ffmc"
            ]
            # Filter only available columns to avoid KeyError
            column_order = [col for col in column_order if col in df.columns]
            df = df[column_order]

            # Replace invalid float values with None immediately
            df = df.replace([pd.NA, float('nan'), float('inf'), float('-inf')], None)
            # Ensure all numeric columns are properly typed and NaN-free
            for col in df.columns:
                if df[col].dtype in ['float64', 'float32']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').replace(np.nan, None)

            return df
        except requests.RequestException:
            self._initialize_session()
            return self.get_index_details(category, list_only)
        except (ValueError, KeyError):
            return None

    def index_live_nifty_50_returns(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com'
        api_url = 'https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi?functionName=getIndicesReturn&&index=NIFTY%2050'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()  # Ensure request was successful

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Convert response to JSON
            data = response.json()
            
            if 'data' in data and isinstance(data['data'], list):
                df = pd.DataFrame(data['data'])

                # Selecting and ordering columns
                columns = ['one_week_chng_per', 'one_month_chng_per', 'three_month_chng_per', 'six_month_chng_per', 'one_year_chng_per', 'two_year_chng_per', 'three_year_chng_per', 'five_year_chng_per']
                
                df = df[columns]

                # **Fix NaN values** to avoid JSON conversion issues
                df = df.fillna(0)  # Replace NaN with 0
                df = df.replace({float('inf'): 0, float('-inf'): 0})  # Replace infinite values with 0

                return df if not df.empty else None  # Return DataFrame if not empty
            return None  # Return None if no data is found

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching all indices data: {e}")
            return None

    def index_live_nifty_50_contribution(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com'
        api_url = 'https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi?functionName=getContributionData&&index=NIFTY%2050'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()  # Ensure request was successful

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Convert response to JSON
            data = response.json()
            
            if 'data' in data and isinstance(data['data'], list):
                df = pd.DataFrame(data['data'])

                # Selecting and ordering columns
                columns = ['icSymbol', 'icSecurity', 'lastTradedPrice', 'changePer', 'isPositive', 'rnNegative', 'changePoints']
                
                df = df[columns]

                # **Fix NaN values** to avoid JSON conversion issues
                df = df.fillna(0)  # Replace NaN with 0
                df = df.replace({float('inf'): 0, float('-inf'): 0})  # Replace infinite values with 0

                return df if not df.empty else None  # Return DataFrame if not empty
            return None  # Return None if no data is found

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching all indices data: {e}")
            return None
        
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
        
    def index_historical_data(self, index: str, *args, from_date: str = None, to_date: str = None, period: str = None):
        """
        Get historical index data for the specified time period.
        Apply the index name as per the NSE India site.
        
        :param index: 'NIFTY 50'/'NIFTY BANK'
        :param from_date: 'dd-mm-YYYY' (e.g., '17-03-2022')
        :param to_date: 'dd-mm-YYYY' (e.g., '17-06-2023')
        :param period: One of ['1D', '1W', '1M', '3M', '6M', '1Y', '2Y', '5Y', '10Y', 'YTD', 'MAX']
        :param args: Allows flexible positional arguments for index, from_date, to_date, or period
        :return: pandas.DataFrame
        :raise ValueError: If the parameter input is not proper
        """
        index_data_columns = ['Date', 'INDEX_NAME', 'Open', 'High', 'Close', 'Low', 'Shares Traded', 'Turnover (₹ Cr)']
        
        # Define period mappings
        period_mappings = {
            '1D': 1,
            '1W': 7,
            '1M': 30,
            '3M': 90,
            '6M': 180,
            '1Y': 365,
            '2Y': 730,
            '5Y': 1825,
            '10Y': 3650,
            'YTD': (datetime.now() - datetime(datetime.now().year, 1, 1)).days,
            'MAX': 3650  # Assuming MAX as 10 years for practical purposes
        }

        # Handle *args for flexible input
        if args:
            if len(args) == 1:
                # Check if the argument is a valid period or a date
                if args[0] in period_mappings:
                    period = args[0]
                else:
                    try:
                        # Try parsing as a date to confirm it's a valid from_date
                        datetime.strptime(args[0], "%d-%m-%Y")
                        from_date = args[0]
                    except ValueError:
                        raise ValueError(f"Invalid argument '{args[0]}'. Must be a valid period {list(period_mappings.keys())} or date in dd-mm-YYYY format")
            elif len(args) == 2:
                # Case: index, from_date, to_date (e.g., 'NIFTY 50', '01-01-2024', '10-10-2025')
                from_date, to_date = args
            else:
                raise ValueError("Invalid arguments provided. Use (index, period) or (index, from_date) or (index, from_date, to_date)")
        
        # Validate input combinations
        if period and (from_date or to_date):
            raise ValueError("Provide either 'period' or 'from_date' and optionally 'to_date', not both")
        
        # Set to_date to today if not provided
        if from_date and not to_date and not period:
            to_date = datetime.now().strftime("%d-%m-%Y")
        
        # Handle period-based date calculation
        if period:
            if period not in period_mappings:
                raise ValueError(f"Invalid period. Choose from {list(period_mappings.keys())}")
            to_dt = datetime.now()
            from_dt = to_dt - timedelta(days=period_mappings[period])
            from_date = from_dt.strftime("%d-%m-%Y")
            to_date = to_dt.strftime("%d-%m-%Y")
        
        # Validate dates
        if not from_date or not to_date:
            raise ValueError("Both from_date and to_date must be provided or derived from period")
        
        try:
            from_dt = datetime.strptime(from_date, "%d-%m-%Y")
            to_dt = datetime.strptime(to_date, "%d-%m-%Y")
            if to_dt < from_dt:
                raise ValueError("to_date must be greater than or equal to from_date")
        except ValueError as e:
            raise ValueError(f"Invalid date format for from_date={from_date} or to_date={to_date}. Use dd-mm-YYYY")
        
        nse_df = pd.DataFrame(columns=index_data_columns)
        from_date_dt = from_dt
        to_date_dt = to_dt
        load_days = (to_date_dt - from_date_dt).days
        
        while load_days > 0:
            if load_days > 365:
                end_date = (from_date_dt + timedelta(days=364)).strftime("%d-%m-%Y")
                start_date = from_date_dt.strftime("%d-%m-%Y")
            else:
                end_date = to_date_dt.strftime("%d-%m-%Y")
                start_date = from_date_dt.strftime("%d-%m-%Y")
            
            data_df = self.get_index_data(index=index, from_date=start_date, to_date=end_date)
            from_date_dt = from_date_dt + timedelta(days=365)
            load_days = (to_date_dt - from_date_dt).days
            
            if nse_df.empty:
                nse_df = data_df
            else:
                nse_df = pd.concat([nse_df, data_df], ignore_index=True)
        
        return nse_df
    
    # sub function of def index_historical_data()
    def get_index_data(self, index: str, from_date: str, to_date: str): 
        index_data_columns = ['Date', 'INDEX_NAME', 'Open', 'High', 'Close', 'Low', 'Shares Traded', 'Turnover (₹ Cr)']
        index = index.replace(' ', '%20').upper()
        ref_url = 'https://www.nseindia.com/reports-indices-historical-index-data'
        ref = requests.get(ref_url, headers=self.headers)
        url = f"https://www.nseindia.com/api/historical/indicesHistory?indexType={index}&from={from_date}&to={to_date}"
        
        try:
            data_json = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict()).json()
            data_close_df = pd.DataFrame(data_json['data']['indexCloseOnlineRecords']).drop(columns=['_id', 'EOD_TIMESTAMP'])
            data_turnover_df = pd.DataFrame(data_json['data']['indexTurnoverRecords']).drop(columns=['_id', 'HIT_INDEX_NAME_UPPER'])
            data_df = pd.merge(data_close_df, data_turnover_df, on='TIMESTAMP', how='inner')
        except Exception as e:
            raise ValueError(f"Failed to fetch data from NSE API: {str(e)}")
        
        data_df.drop(columns='TIMESTAMP', inplace=True)
        unwanted_str_list = ['FH_', 'EOD_', 'HIT_']
        new_col = data_df.columns
        for unwanted in unwanted_str_list:
            new_col = [name.replace(unwanted, '') for name in new_col]
        data_df.columns = new_col
        
        # Rename columns to match requested names
        column_mapping = {
            'TIMESTAMP': 'Date',
            'INDEX_NAME': 'INDEX_NAME',
            'OPEN_INDEX_VAL': 'Open',
            'HIGH_INDEX_VAL': 'High',
            'LOW_INDEX_VAL': 'Low',
            'CLOSE_INDEX_VAL': 'Close',
            'TRADED_QTY': 'Shares Traded',
            'TURN_OVER': 'Turnover (₹ Cr)'
        }
        data_df = data_df.rename(columns=column_mapping)
        # data_df['Date'] = pd.to_datetime(data_df['Date'], format='%d-%b-%Y')
        
        return data_df[index_data_columns]

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

        # --- Final Summary ---
        if fail_chunks:
            print(f"⚠️ {index}: {len(fail_chunks)} failed chunks → {fail_chunks}")
        else:
            print(f"✅ {index} data fetched successfully: {from_date} → {to_date}")

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

        # --- Print final summary ---
        if fail_chunks:
            print(f"⚠️ {symbol}: {len(fail_chunks)} failed chunks → {fail_chunks}")
        else:
            print(f"✅ {symbol} data fetched successfully: {from_date} → {to_date}")

        return df.reset_index(drop=True)

    #---------------------------------------------------------- Gifty_Nifty ----------------------------------------------------------------

    def cm_live_gifty_nifty(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com/'
        api_url = 'https://www.nseindia.com/api/NextApi/apiClient?functionName=getGiftNifty'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()  # Ensure request was successful

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Convert response to JSON
            data = response.json()

            # Extract Gift Nifty and USDINR data
            if isinstance(data, dict) and "data" in data:
                gift_nifty_data = data["data"].get("giftNifty", {})
                usd_inr_data = data["data"].get("usdInr", {})

                # Convert to DataFrame
                df = pd.DataFrame([{
                    "symbol": gift_nifty_data.get("symbol"),
                    "lastprice": gift_nifty_data.get("lastprice"),
                    "daychange": gift_nifty_data.get("daychange"),
                    "perchange": gift_nifty_data.get("perchange"),
                    "contractstraded": gift_nifty_data.get("contractstraded"),
                    "timestmp": gift_nifty_data.get("timestmp"),
                    "expirydate": gift_nifty_data.get("expirydate"),
                    "usdInr_symbol": usd_inr_data.get("symbol"),  # USDINR Symbol
                    "usdInr_ltp": usd_inr_data.get("ltp"),  # USDINR Last Traded Price
                    "usdInr_updated_time": usd_inr_data.get("updated_time"),  # USDINR Last Updated Time
                    "usdInr_expiry_dt": usd_inr_data.get("expiry_dt"),  # USDINR Expiry Date
                }])

                return df if not df.empty else None  # Return DataFrame if not empty
            return None  # Return None if data is missing

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching Gift Nifty and USDINR data: {e}")
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
        self.rotate_user_agent()  # Rotate User-Agent for reliability

        ref_url = 'https://www.nseindia.com'
        api_url = 'https://www.nseindia.com/api/NextApi/apiClient?functionName=getMarketStatistics'

        try:
            # Step 1: Get reference cookies
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Step 2: Fetch market statistics using cookies
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'data' in data:
                d = data['data']
                snapshot = d.get('snapshotCapitalMarket', {})
                fifty_two_week = d.get('fiftyTwoWeek', {})
                circuit = d.get('circuit', {})

                # Convert Registered Investors string to crores (Cr) rounded to 2 decimals
                reg_inv_str = d.get('regInvestors', '0')
                if reg_inv_str:
                    reg_inv_num = int(reg_inv_str.replace(',', '').strip())
                    reg_inv_cr = round(reg_inv_num / 1e7, 2)  # 1 Cr = 10,000,000
                else:
                    reg_inv_cr = 0.0

                # Step 3: Build DataFrame with both Registered Investors columns
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

            return None  # No valid data field found

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching market statistics: {e}")
            return None

    #---------------------------------------------------------- CM_Live_Data ----------------------------------------------------------------
    
    def cm_live_equity_info(self, symbol):
        symbol = symbol.replace(' ', '%20').replace('&', '%26')
        self.rotate_user_agent()
        ref_url = f'https://www.nseindia.com/get-quotes/equity?symbol={symbol}'
        ref = requests.get(ref_url, headers=self.headers)
        try:
            url = f'https://www.nseindia.com/api/quote-equity?symbol={symbol}'
            data = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10).json()
            if not data or 'error' in data:
                return None
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
                "Facevalue": data['securityInfo']['faceValue']
            }
        except (requests.RequestException, ValueError):
            return None

    def cm_live_equity_price_info(self, symbol):
        symbol = symbol.replace(' ', '%20').replace('&', '%26')
        self.rotate_user_agent()
        ref_url = f'https://www.nseindia.com/get-quotes/equity?symbol={symbol}'
        ref = requests.get(ref_url, headers=self.headers)

        try:
            url = f'https://www.nseindia.com/api/quote-equity?symbol={symbol}'
            data = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10).json()
            url = f'https://www.nseindia.com/api/quote-equity?symbol={symbol}&section=trade_info'
            trade_data = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10).json()

            if not data or 'error' in data:
                return None

            # Extract bid and ask data (handling missing values)
            bid_data = trade_data.get('marketDeptOrderBook', {}).get('bid', [])
            ask_data = trade_data.get('marketDeptOrderBook', {}).get('ask', [])
            total_buy_qty = trade_data.get('marketDeptOrderBook', {}).get('totalBuyQuantity', 0)
            total_sell_qty = trade_data.get('marketDeptOrderBook', {}).get('totalSellQuantity', 0)

            # Extract first 5 bid/ask levels (fill missing with 0)
            bid_prices = [entry.get("price", 0) or 0 for entry in bid_data[:5]] + [0] * (5 - len(bid_data))
            bid_quantities = [entry.get("quantity", 0) or 0 for entry in bid_data[:5]] + [0] * (5 - len(bid_data))

            ask_prices = [entry.get("price", 0) or 0 for entry in ask_data[:5]] + [0] * (5 - len(ask_data))
            ask_quantities = [entry.get("quantity", 0) or 0 for entry in ask_data[:5]] + [0] * (5 - len(ask_data))

            return {
                "Symbol": symbol,
                "PreviousClose": data['priceInfo']['previousClose'],
                "LastTradedPrice": data['priceInfo']['lastPrice'],
                "Change": data['priceInfo']['change'],
                "PercentChange": data['priceInfo']['pChange'],
                "deliveryToTradedQuantity": trade_data['securityWiseDP']['deliveryToTradedQuantity'],
                "Open": data['priceInfo']['open'],
                "Close": data['priceInfo']['close'],
                "High": data['priceInfo']['intraDayHighLow']['max'],
                "Low": data['priceInfo']['intraDayHighLow']['min'],
                "VWAP": data['priceInfo']['vwap'],
                "UpperCircuit": data['priceInfo']['upperCP'],
                "LowerCircuit": data['priceInfo']['lowerCP'],
                "Macro": data['industryInfo']['macro'],
                "Sector": data['industryInfo']['sector'],
                "Industry": data['industryInfo']['industry'],
                "BasicIndustry": data['industryInfo']['basicIndustry'],
                # Store bid/ask levels separately instead of lists
                "Bid Price 1": bid_prices[0], "Bid Quantity 1": bid_quantities[0],
                "Bid Price 2": bid_prices[1], "Bid Quantity 2": bid_quantities[1],
                "Bid Price 3": bid_prices[2], "Bid Quantity 3": bid_quantities[2],
                "Bid Price 4": bid_prices[3], "Bid Quantity 4": bid_quantities[3],
                "Bid Price 5": bid_prices[4], "Bid Quantity 5": bid_quantities[4],
                "Ask Price 1": ask_prices[0], "Ask Quantity 1": ask_quantities[0],
                "Ask Price 2": ask_prices[1], "Ask Quantity 2": ask_quantities[1],
                "Ask Price 3": ask_prices[2], "Ask Quantity 3": ask_quantities[2],
                "Ask Price 4": ask_prices[3], "Ask Quantity 4": ask_quantities[3],
                "Ask Price 5": ask_prices[4], "Ask Quantity 5": ask_quantities[4],
                "totalBuyQuantity": total_buy_qty,
                "totalSellQuantity": total_sell_qty
            }
        except (requests.RequestException, ValueError):
            return None

    def cm_live_most_active_equity_by_value(self):
        self.rotate_user_agent()
        ref_url = 'https://www.nseindia.com/market-data/most-active-equities'
        ref = requests.get(ref_url, headers=self.headers)
        url = 'https://www.nseindia.com/api/live-analysis-most-active-securities?index=value'
        try:
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            data = response.json()
            df = pd.DataFrame(data['data'])
            return df if not df.empty else None
        except (requests.RequestException, ValueError):
            return None

    def cm_live_most_active_equity_by_vol(self):
        self.rotate_user_agent()
        ref_url = 'https://www.nseindia.com/market-data/most-active-equities'
        ref = requests.get(ref_url, headers=self.headers)
        url = 'https://www.nseindia.com/api/live-analysis-most-active-securities?index=volume'
        try:
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            data = response.json()
            df = pd.DataFrame(data['data'])
            return df if not df.empty else None
        except (requests.RequestException, ValueError):
            return None

    def cm_live_volume_spurts(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com/market-data/volume-gainers-spurts'
        api_url = 'https://www.nseindia.com/api/live-analysis-volume-gainers'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()

            # Convert response to JSON
            data = response.json()
            
            if 'data' in data and isinstance(data['data'], list):
                df = pd.DataFrame(data['data'])

                # Selecting and ordering columns
                df = df[['symbol', 'companyName', 'volume', 'week1AvgVolume', 'week1volChange',
                        'week2AvgVolume', 'week2volChange', 'ltp', 'pChange', 'turnover']]

                # Rename columns to more user-friendly names
                df.rename(columns={
                    'symbol': 'Symbol',
                    'companyName': 'Security',
                    'volume': 'Today Volume',
                    'week1AvgVolume': '1 Week Avg. Volume',
                    'week1volChange': '1 Week Change (×)',
                    'week2AvgVolume': '2 Week Avg. Volume',
                    'week2volChange': '2 Week Change (×)',
                    'ltp': 'LTP',
                    'pChange': '% Change',
                    'turnover': 'Turnover (₹ Lakhs)'
                }, inplace=True)

                return df if not df.empty else None
            return None

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching Volume Spurts data: {e}")
            return None


    def cm_live_52week_high(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com/market-data/52-week-high-equity-market'
        api_url = 'https://www.nseindia.com/api/live-analysis-data-52weekhighstock'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()  # Ensure request was successful

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Convert response to JSON
            data = response.json()
            
            if 'data' in data and isinstance(data['data'], list):
                df = pd.DataFrame(data['data'])

                # Selecting and ordering columns
                df = df[['symbol', 'series', 'ltp', 'pChange', 'new52WHL', 'prev52WHL', 'prevHLDate']]

                return df if not df.empty else None  # Return DataFrame if not empty
            return None  # Return None if no data is found

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching 52-week high data: {e}")
            return None
        
    def cm_live_52week_low(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com/market-data/52-week-low-equity-market'
        api_url = 'https://www.nseindia.com/api/live-analysis-data-52weeklowstock'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()  # Ensure request was successful

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Convert response to JSON
            data = response.json()
            
            if 'data' in data and isinstance(data['data'], list):
                df = pd.DataFrame(data['data'])

                # Selecting and ordering columns
                df = df[['symbol', 'series', 'ltp', 'pChange', 'new52WHL', 'prev52WHL', 'prevHLDate']]

                return df if not df.empty else None  # Return DataFrame if not empty
            return None  # Return None if no data is found

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching 52-week low data: {e}")
            return None

    def cm_live_block_deal(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com/market-data/block-deal-watch'
        api_url = 'https://www.nseindia.com/api/block-deal'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()  # Ensure request was successful

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Convert response to JSON
            data = response.json()
            
            if 'data' in data and isinstance(data['data'], list):
                df = pd.DataFrame(data['data'])

                # Selecting and ordering columns
                df = df[['session','symbol', 'series', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'previousClose', 'pchange', 'totalTradedVolume', 'totalTradedValue']]

                return df if not df.empty else None  # Return DataFrame if not empty
            return None  # Return None if no data is found

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching Block deal data: {e}")
            return None

# def old_cm_live_insider_trading(self, from_date: str = None, to_date: str = None):
    #     self.rotate_user_agent()
        
    #     if from_date is None:
    #         from_date = datetime.now() - timedelta(days=30)
    #         from_date_str = from_date.strftime("%d-%m-%Y")
    #         to_date_str = datetime.now().strftime("%d-%m-%Y")
    #     else:
    #         from_date_str = from_date
        
    #     if to_date is None:
    #         to_date_str = datetime.now().strftime("%d-%m-%Y")
    #     else:
    #         to_date_str = to_date
        
    #     ref_url = 'https://www.nseindia.com/companies-listing/corporate-filings-insider-trading'
    #     ref = requests.get(ref_url, headers=self.headers)

    #     try:
    #         url = f'https://www.nseindia.com/api/corporates-pit?index=equities&from_date={from_date_str}&to_date={to_date_str}'
    #         response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
    #         data = response.json()
    #         df = pd.DataFrame(data['data'])

    #         # Changing only the column order
    #         column_order = [
    #             "symbol", "company", "anex", "acqName", "personCategory",
    #             "secType", "befAcqSharesNo", "befAcqSharesPer", "remarks", "secAcq",
    #             "secVal", "tdpTransactionType", "securitiesTypePost", "afterAcqSharesNo",
    #             "afterAcqSharesPer", "acqfromDt", "acqtoDt", "intimDt", "acqMode",
    #             "derivativeType", "tdpDerivativeContractType", "buyValue", "buyQuantity",
    #             "sellValue", "sellquantity", "exchange", "remarks", "date", "xbrl"
    #         ]

    #         df = df[column_order]  # Reordering DataFrame columns
            
    #         return df
    #     except (requests.RequestException, ValueError):
    #         return None

    def cm_live_hist_insider_trading(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        """
        Fetch insider trading disclosures from NSE India.

        Parameters
        ----------
        *args : str
            Can contain symbol and/or dates (format: DD-MM-YYYY).
        from_date : str, optional
            Start date (DD-MM-YYYY)
        to_date : str, optional
            End date (DD-MM-YYYY)
        symbol : str, optional
            NSE symbol (e.g., 'RELIANCE')

        Returns
        -------
        pd.DataFrame or None
            Insider trading disclosures as a DataFrame, or None if unavailable.
        """

        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        today = datetime.now()
        today_str = today.strftime("%d-%m-%Y")

        # --- Auto-detect arguments --- #
        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                elif arg.upper() in ["1D", "1W", "1M", "3M", "6M", "1Y"]:
                    period = arg.upper()
                else:
                    symbol = arg.upper()

        # --- Compute date range from period --- #
        if period:
            delta_map = {
                "1D": timedelta(days=1),
                "1W": timedelta(weeks=1),
                "1M": timedelta(days=30),
                "3M": timedelta(days=90),
                "6M": timedelta(days=180),
                "1Y": timedelta(days=365),
            }
            delta = delta_map.get(period, timedelta(days=365))
            from_date = (today - delta).strftime("%d-%m-%Y")
            if not to_date:
                to_date = today_str

        # --- Default fallback if symbol/date missing --- #
        if not from_date:
            from_date = today_str
        if not to_date:
            to_date = today_str

        # --- Rotate user-agent for reliability ---
        self.rotate_user_agent()

        # --- Reference URL (for cookies/session setup) ---
        ref_url = "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading"

        # --- API URL logic ---
        if symbol and from_date and to_date:
            api_url = (
                f"https://www.nseindia.com/api/corporates-pit?"
                f"index=equities&from_date={from_date}&to_date={to_date}&symbol={symbol}"
            )
        elif symbol:
            api_url = f"https://www.nseindia.com/api/corporates-pit?index=equities&symbol={symbol}"
        elif from_date and to_date:
            api_url = f"https://www.nseindia.com/api/corporates-pit?index=equities&from_date={from_date}&to_date={to_date}"
        else:
            api_url = "https://www.nseindia.com/api/cmsNote?url=corporate-filings-insider-trading"

        # --- Fetch and process data ---
        try:
            # Step 1: Establish session and cookies
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Step 2: Attempt retries for main request
            for attempt in range(3):
                try:
                    response = self.session.get(
                        api_url,
                        headers=self.headers,
                        cookies=ref_response.cookies.get_dict(),
                        timeout=15,
                    )
                    response.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt < 2:
                        print(f"⚠️ Retry {attempt+1}/3 insider trading for {symbol or 'ALL'}... ({e})")
                        time.sleep(2)
                    else:
                        raise

            # Step 3: Parse JSON response
            data = response.json()
            records = data.get("data") if isinstance(data, dict) else data

            if not records or not isinstance(records, list):
                # print(f"ℹ️ No insider trading found for {symbol or 'ALL'} between {from_date} and {to_date}")
                return None

            df = pd.DataFrame(records)

            # --- Clean columns ---
            expected_cols = [
                "symbol", "company", "acqName", "personCategory", "secType", "befAcqSharesNo",
                "befAcqSharesPer", "remarks", "secAcq", "secVal", "tdpTransactionType",
                "securitiesTypePost", "afterAcqSharesNo", "afterAcqSharesPer", "acqfromDt",
                "acqtoDt", "intimDt", "acqMode", "derivativeType", "tdpDerivativeContractType",
                "buyValue", "buyQuantity", "sellValue", "sellquantity", "exchange", "date", "xbrl"
            ]

            df = df[[c for c in expected_cols if c in df.columns]]
            df = df.fillna("").replace({float("inf"): "", float("-inf"): ""})

            # print(f"✅ Insider trading fetched for {symbol or 'ALL'} ({len(df)} records)")
            return df

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching insider trading: {e}")
            return None
        
    def cm_live_hist_corporate_announcement(self, *args, from_date=None, to_date=None, symbol=None):
        """
        Fetch corporate announcements from NSE India.
        Auto-detects whether inputs are dates or symbol.

        Logic:
        - If symbol only → use symbol-only API
        - If symbol + dates → always use date-range API (even if both = today)
        - If dates only → fetch all symbols for date range
        - If nothing → fetch all symbols for today's date

        Returns:
            pd.DataFrame: Empty DataFrame if no announcements found.
        """

        # --- Detect date pattern ---
        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        today_str = datetime.now().strftime("%d-%m-%Y")

        # --- Auto-detect arguments (dates or symbol) ---
        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                else:
                    symbol = arg.upper()

        # --- Default dates only if no symbol is provided ---
        if not symbol:
            from_date = from_date or today_str
            to_date = to_date or today_str

        # --- Rotate user-agent for reliability ---
        self.rotate_user_agent()

        # --- NSE reference URL ---
        ref_url = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"

        # --- Final URL selection logic ---
        if symbol and from_date and to_date:
            # Symbol + date range
            api_url = (
                f"https://www.nseindia.com/api/corporate-announcements?"
                f"index=equities&from_date={from_date}&to_date={to_date}&symbol={symbol}&reqXbrl=false"
            )
        elif symbol:
            # Symbol only
            api_url = (
                f"https://www.nseindia.com/api/corporate-announcements?"
                f"index=equities&symbol={symbol}&reqXbrl=false"
            )
        else:
            # Dates only (all symbols)
            api_url = (
                f"https://www.nseindia.com/api/corporate-announcements?"
                f"index=equities&from_date={from_date}&to_date={to_date}&reqXbrl=false"
            )

        # --- Fetch & process ---
        try:
            # Step 1: Establish session
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Step 2: API request
            response = self.session.get(
                api_url,
                headers=self.headers,
                cookies=ref_response.cookies.get_dict(),
                timeout=10,
            )
            response.raise_for_status()

            # Step 3: Parse JSON → DataFrame
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                df = pd.DataFrame(data)
                expected_cols = [
                    'symbol', 'sm_name', 'smIndustry', 'desc',
                    'attchmntText', 'attchmntFile', 'fileSize', 'an_dt'
                ]
                df = df[[c for c in expected_cols if c in df.columns]]
                df = df.fillna("").replace({float('inf'): "", float('-inf'): ""})
                return df
            else:
                print(f"ℹ️  No corporate announcements found for {symbol} between {from_date} and {to_date}")
                return None

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching corporate announcements: {e}")
            return None

    def cm_live_hist_corporate_action(self, from_date_str: str = None, to_date_str: str = None, filter: str = None):
        self.rotate_user_agent()
        
        # Default date range (next 90 days)
        if from_date_str is None:
            from_date = datetime.now()
            from_date_str = from_date.strftime("%d-%m-%Y")
        if to_date_str is None:
            to_date = datetime.now() + timedelta(days=90)
            to_date_str = to_date.strftime("%d-%m-%Y")

        # Reference URL for cookies
        ref_url = 'https://www.nseindia.com/companies-listing/corporate-filings-actions'
        ref = requests.get(ref_url, headers=self.headers)

        try:
            url = f"https://www.nseindia.com/api/corporates-corporateActions?index=equities&from_date={from_date_str}&to_date={to_date_str}"
            data_obj = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            
            # Check if the response is valid
            data_obj.raise_for_status()
            json_data = data_obj.json()
            
            # Convert response to DataFrame
            corp_action = pd.DataFrame(json_data)

            # Apply filtering if needed
            if filter is not None:
                corp_action = corp_action[corp_action['subject'].str.contains(filter, case=False, na=False)]
            
            # Rename columns
            column_mapping = {
                "symbol": "SYMBOL",
                "comp": "COMPANY NAME",
                "series": "SERIES",
                "subject": "PURPOSE",
                "faceVal": "FACE VALUE",
                "exDate": "EX-DATE",
                "recDate": "RECORD DATE",
                "bcStartDate": "BOOK CLOSURE START DATE",
                "bcEndDate": "BOOK CLOSURE END DATE"
            }
            corp_action = corp_action.rename(columns=column_mapping)

            # Reorder columns
            column_order = [
                "SYMBOL", "COMPANY NAME", "SERIES", "PURPOSE", "FACE VALUE",
                "EX-DATE", "RECORD DATE", "BOOK CLOSURE START DATE", "BOOK CLOSURE END DATE"
            ]
            corp_action = corp_action[column_order]

            # Debug: Print the final number of records
            # print(f"Final number of records in DataFrame: {len(corp_action)}")

            return corp_action

        except (requests.RequestException, ValueError) as e:
            print(f"Error occurred: {str(e)}")
            return None

    def cm_live_today_event_calendar(self, from_date=None, to_date=None):
        # --- Default date handling ---
        today_str = datetime.now().strftime("%d-%m-%Y")
        from_date = from_date or today_str
        to_date = to_date or today_str

        # --- Rotate user-agent for reliability ---
        self.rotate_user_agent()

        # --- NSE URLs ---
        ref_url = "https://www.nseindia.com/companies-listing/corporate-filings-event-calendar"
        api_url = (
            f"https://www.nseindia.com/api/event-calendar?"
            f"index=equities&from_date={from_date}&to_date={to_date}"
        )

        try:
            # Step 1: Get session cookies
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Step 2: API request with valid session cookies
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()

            # Step 3: Convert JSON to DataFrame
            data = response.json()

            if isinstance(data, list) and len(data) > 0:
                df = pd.DataFrame(data)

                # Expected NSE JSON keys
                columns = ['symbol', 'company', 'purpose', 'bm_desc', 'date']
                df = df[[col for col in columns if col in df.columns]]

                # Data cleaning
                df = df.fillna("").replace({float('inf'): "", float('-inf'): ""})

                return df if not df.empty else None
            else:
                print(f"No corporate Event found for {from_date} to {to_date}")
                return None

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching corporate Event: {e}")
            return None
        
    def cm_live_upcoming_event_calendar(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com/companies-listing/corporate-filings-event-calendar'
        api_url = 'https://www.nseindia.com/api/event-calendar?'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()  # Ensure request was successful

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Convert response to JSON
            data = response.json()
            
            # Directly assume data is a list instead of checking 'data' key
            if isinstance(data, list):
                df = pd.DataFrame(data)

                # Selecting and ordering columns
                required_columns = ['symbol', 'company', 'purpose', 'bm_desc', 'date']
                df = df[required_columns]

                return df if not df.empty else None  # Return DataFrame if not empty
            return None  # Return None if data is not a list or is empty

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching Event Calendar data: {e}")
            return None
        
    def cm_live_hist_board_meetings(self, *args, from_date=None, to_date=None, symbol=None):
        # --- Detect date pattern ---
        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        today_str = datetime.now().strftime("%d-%m-%Y")

        # --- Auto-detect arguments (dates or symbol) ---
        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                else:
                    symbol = arg.upper()

        # # --- Default dates only if no symbol is provided ---
        # if not symbol:
        #     from_date = from_date or today_str
        #     to_date = to_date or today_str

        # --- Rotate user-agent for reliability ---
        self.rotate_user_agent()

        # --- NSE reference URL ---
        ref_url = "https://www.nseindia.com/companies-listing/corporate-filings-board-meetings"

        # --- Final URL selection logic ---
        if symbol and from_date and to_date:
            # Symbol + date range
            api_url = (
                f"https://www.nseindia.com/api/corporate-board-meetings?"
                f"index=equities&from_date={from_date}&to_date={to_date}&symbol={symbol}"
            )
        elif symbol:
            # Symbol only
            api_url = (f"https://www.nseindia.com/api/corporate-board-meetings?index=equities&symbol={symbol}"
            )

        elif not symbol and from_date and to_date:
            # Date only
            api_url = (f"https://www.nseindia.com/api/corporate-board-meetings?index=equities&from_date={from_date}&to_date={to_date}"
            )

        else:
            api_url = ("https://www.nseindia.com/api/corporate-board-meetings?index=equities")

        # --- Fetch & process ---
        try:
            # Step 1: Establish session
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Step 2: API request
            response = self.session.get(
                api_url,
                headers=self.headers,
                cookies=ref_response.cookies.get_dict(),
                timeout=10,
            )
            response.raise_for_status()

            # Step 3: Parse JSON → DataFrame
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                df = pd.DataFrame(data)
                expected_cols = [
                    'bm_symbol', 'sm_name', 'sm_indusrty', 'bm_purpose',
                    'bm_desc', 'bm_date', 'attachment', 'attFileSize','bm_timestamp'
                ]
                df = df[[c for c in expected_cols if c in df.columns]]
                df = df.fillna("").replace({float('inf'): "", float('-inf'): ""})
                return df
            else:
                print(f"ℹ️  No Board Meetings found for {symbol} between {from_date} and {to_date}")
                return None

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching Board Meetings: {e}")
            return None
        
    def cm_live_hist_Shareholder_meetings(self, *args, from_date=None, to_date=None, symbol=None):
        """
        Fetch NSE shareholder meetings (AGM, EGM, Postal Ballot) data.
        Handles flexible inputs: symbol, date range, or none (fetch all).
        """

        # --- Detect date pattern ---
        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        today_str = datetime.now().strftime("%d-%m-%Y")

        # --- Auto-detect arguments (dates or symbol) ---
        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                else:
                    symbol = arg.upper()

        # --- Rotate user-agent for reliability ---
        self.rotate_user_agent()

        # --- NSE reference URL ---
        ref_url = "https://www.nseindia.com/companies-listing/corporate-filings-postal-ballot"

        # --- Final API URL selection logic ---
        if symbol and from_date and to_date:
            api_url = f"https://www.nseindia.com/api/postal-ballot?index=equities&from_date={from_date}&to_date={to_date}&symbol={symbol}"
        elif symbol:
            api_url = f"https://www.nseindia.com/api/postal-ballot?index=equities&symbol={symbol}"
        elif from_date and to_date:
            api_url = f"https://www.nseindia.com/api/postal-ballot?index=equities&from_date={from_date}&to_date={to_date}"
        else:
            api_url = "https://www.nseindia.com/api/postal-ballot?index=equities"

        # --- Fetch & process ---
        try:
            # Step 1: Establish session
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Step 2: API request
            response = self.session.get(
                api_url,
                headers=self.headers,
                cookies=ref_response.cookies.get_dict(),
                timeout=10,
            )
            response.raise_for_status()

            # Step 3: Parse JSON → DataFrame
            data_json = response.json()
            data = data_json.get("data", [])  # ✅ Correct key

            if isinstance(data, list) and len(data) > 0:
                df = pd.DataFrame(data)
                expected_cols = ["symbol", "sLN", "bdt", "text", "type", "attachment", "date"]
                df = df[[c for c in expected_cols if c in df.columns]]
                df = df.fillna("").replace({float("inf"): "", float("-inf"): ""})
                return df

            else:
                scope = symbol or "All symbols"
                print(f"ℹ️  No Shareholder Meetings found for {scope} between {from_date or '-'} and {to_date or '-'}")
                return None

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching Shareholder Meetings: {e}")
            return None

    def cm_live_hist_qualified_institutional_placement(self, *args, from_date=None, to_date=None, period=None, symbol=None, stage=None):
        """
        Fetch Qualified Institutional Placement (QIP) data from NSE (In-Principle or Listing Stage).

        Args (auto-detected from *args):
            - stage: "In-Principle" or "Listing Stage"
            - symbol: NSE symbol (e.g., "RELIANCE")
            - from_date: dd-mm-yyyy (e.g., "20-10-2024")
            - to_date: dd-mm-yyyy (e.g., "10-01-2025")
            - period: "1D", "1W", "1M", "3M", "6M", "1Y"

        Returns:
            pandas.DataFrame or None
        """

        today = datetime.now()
        today_str = today.strftime("%d-%m-%Y")
        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        period_pattern = re.compile(r"^(1D|1W|1M|3M|6M|1Y)$", re.IGNORECASE)

        # --- Auto-detect args ---
        for arg in args:
            if isinstance(arg, str):
                arg_title = arg.title()
                # Stage
                if arg_title in ["In-Principle", "Listing Stage"]:
                    stage = arg_title
                # Date
                elif date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                # Period
                elif period_pattern.match(arg.upper()):
                    period = arg.upper()
                # Symbol
                else:
                    symbol = arg.upper()
            elif isinstance(arg, datetime):
                if not from_date:
                    from_date = arg.strftime("%d-%m-%Y")
                elif not to_date:
                    to_date = arg.strftime("%d-%m-%Y")

        # --- Compute from period if no explicit dates ---
        if period and not (from_date and to_date):
            delta_map = {
                "1D": timedelta(days=1),
                "1W": timedelta(weeks=1),
                "1M": timedelta(days=30),
                "3M": timedelta(days=90),
                "6M": timedelta(days=180),
                "1Y": timedelta(days=365),
            }
            from_date = (today - delta_map[period]).strftime("%d-%m-%Y")
            to_date = today_str

        # --- Defaults and validation ---
        stage = (stage or "In-Principle").title()
        if stage not in ["In-Principle", "Listing Stage"]:
            print(f"❌ Invalid stage: {stage}. Must be 'In-Principle' or 'Listing Stage'.")
            return None

        if from_date and to_date:
            try:
                datetime.strptime(from_date, "%d-%m-%Y")
            except ValueError:
                print(f"❌ Invalid from_date: {from_date}. Using today's date.")
                from_date = today_str
            try:
                datetime.strptime(to_date, "%d-%m-%Y")
            except ValueError:
                print(f"❌ Invalid to_date: {to_date}. Using today's date.")
                to_date = today_str

        # --- Build API URL ---
        base_url = "https://www.nseindia.com/api/corporate-further-issues-qip"
        index_map = {"In-Principle": "FIQIPIP", "Listing Stage": "FIQIPLS"}
        params = {"index": index_map[stage]}

        if symbol:
            params["symbol"] = symbol.upper()
        elif from_date and to_date:
            params["from_date"] = from_date
            params["to_date"] = to_date

        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        api_url = f"{base_url}?{query_string}"

        # --- Rotate user agent ---
        self.rotate_user_agent()

        try:
            headers = self.headers.copy()
            headers.update({
                "Accept": "application/json",
                "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-QIP"
            })

            session = requests.Session()
            # Fetch cookies
            session.get("https://www.nseindia.com/companies-listing/corporate-filings-QIP", headers=headers, timeout=10)

            # Retry logic
            for attempt in range(3):
                try:
                    response = session.get(api_url, headers=headers, cookies=session.cookies.get_dict(), timeout=10)
                    response.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        print(f"❌ Failed to fetch data: {e}")
                        return None

            # Parse JSON
            try:
                data_json = response.json()
            except ValueError as e:
                print(f"❌ Failed to parse JSON: {e}")
                return None

            data = data_json.get("data", [])
            if not data:
                print(f"ℹ️ No QIP records found for {symbol or period or 'All symbols'} ({stage})")
                return None

            df = pd.DataFrame(data)

            # --- Column renaming ---
            rename_map = {
                "In-Principle": {
                    "nseSymbol": "Symbol",
                    "companyName": "Company Name",
                    "stage": "Stage",
                    "issue_type": "Issue Type",
                    "dateBrdResol": "Board Resolution Date",
                    "dateOfSHApp": "Shareholder Approval Date",
                    "totalAmtOfIssueSize": "Total Issue Size",
                    "prcntagePerSecrtyProDiscNotice": "Percentage per Security Notice",
                    "listedAt": "Listed At",
                    "dateOfSubmission": "Submission Date",
                    "xmlFileName": "XML Link",
                },
                "Listing Stage": {
                    "nsesymbol": "Symbol",
                    "companyName": "Company Name",
                    "stage": "Stage",
                    "issue_type": "Issue Type",
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
                }
            }

            df = df[[c for c in rename_map[stage] if c in df.columns]]
            df.rename(columns=rename_map[stage], inplace=True)

            # --- Keep Submission Date as-is; other dates can remain string ---
            df = df.fillna("").replace({float("inf"): "", float("-inf"): ""})

            # print(f"ℹ️ Successfully fetched {len(df)} QIP records for {symbol or period or 'All symbols'} ({stage})")
            return df

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching QIP data ({stage}): {e}")
            return None

    def cm_live_hist_preferential_issue(self, *args, from_date=None, to_date=None, period=None, symbol=None, stage=None):
        """
        Fetch Preferential Issue (PREF) data from NSE (In-Principle or Listing Stage).

        Args (auto-detected from *args):
            - stage: "In-Principle" or "Listing Stage"
            - symbol: NSE symbol (e.g., "RELIANCE")
            - from_date: dd-mm-yyyy (e.g., "20-10-2024")
            - to_date: dd-mm-yyyy (e.g., "10-01-2025")
            - period: "1D", "1W", "1M", "3M", "6M", "1Y"

        Returns:
            pandas.DataFrame or None
        """

        today = datetime.now()
        today_str = today.strftime("%d-%m-%Y")
        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        period_pattern = re.compile(r"^(1D|1W|1M|3M|6M|1Y)$", re.IGNORECASE)

        # --- Auto-detect args ---
        for arg in args:
            if isinstance(arg, str):
                arg_title = arg.title()
                # Stage
                if arg_title in ["In-Principle", "Listing Stage"]:
                    stage = arg_title
                # Date
                elif date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                # Period
                elif period_pattern.match(arg.upper()):
                    period = arg.upper()
                # Symbol
                else:
                    symbol = arg.upper()
            elif isinstance(arg, datetime):
                if not from_date:
                    from_date = arg.strftime("%d-%m-%Y")
                elif not to_date:
                    to_date = arg.strftime("%d-%m-%Y")

        # --- Compute from period if no explicit dates ---
        if period and not (from_date and to_date):
            delta_map = {
                "1D": timedelta(days=1),
                "1W": timedelta(weeks=1),
                "1M": timedelta(days=30),
                "3M": timedelta(days=90),
                "6M": timedelta(days=180),
                "1Y": timedelta(days=365),
            }
            from_date = (today - delta_map[period]).strftime("%d-%m-%Y")
            to_date = today_str

        # --- Defaults and validation ---
        stage = (stage or "In-Principle").title()
        if stage not in ["In-Principle", "Listing Stage"]:
            print(f"❌ Invalid stage: {stage}. Must be 'In-Principle' or 'Listing Stage'.")
            return None

        if from_date and to_date:
            try:
                datetime.strptime(from_date, "%d-%m-%Y")
            except ValueError:
                print(f"❌ Invalid from_date: {from_date}. Using today's date.")
                from_date = today_str
            try:
                datetime.strptime(to_date, "%d-%m-%Y")
            except ValueError:
                print(f"❌ Invalid to_date: {to_date}. Using today's date.")
                to_date = today_str

        # --- Build API URL ---
        base_url = "https://www.nseindia.com/api/corporate-further-issues-pref"
        index_map = {"In-Principle": "FIPREFIP", "Listing Stage": "FIPREFLS"}
        params = {"index": index_map[stage]}

        if symbol:
            params["symbol"] = symbol.upper()
        elif from_date and to_date:
            params["from_date"] = from_date
            params["to_date"] = to_date

        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        api_url = f"{base_url}?{query_string}"

        # --- Rotate user agent ---
        self.rotate_user_agent()

        try:
            headers = self.headers.copy()
            headers.update({
                "Accept": "application/json",
                "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-PREF"
            })

            session = requests.Session()
            # Fetch cookies
            session.get("https://www.nseindia.com/companies-listing/corporate-filings-PREF", headers=headers, timeout=10)

            # Retry logic
            for attempt in range(3):
                try:
                    response = session.get(api_url, headers=headers, cookies=session.cookies.get_dict(), timeout=10)
                    response.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        print(f"❌ Failed to fetch data: {e}")
                        return None

            # Parse JSON
            try:
                data_json = response.json()
            except ValueError as e:
                print(f"❌ Failed to parse JSON: {e}")
                return None

            data = data_json.get("data", [])
            if not data:
                print(f"ℹ️ No Preferential Issue records found for {symbol or period or 'All symbols'} ({stage})")
                return None

            df = pd.DataFrame(data)

            # --- Column renaming ---
            rename_map = {
                "In-Principle": {
                    "nseSymbol": "Symbol",
                    "nameOfTheCompany": "Company Name",
                    "stage": "Stage",
                    "issueType": "Issue Type",
                    "dateBrdResoln": "Date of Board Resolution",
                    "boardResDate": "Board Resolution Date",
                    "categoryOfAllottee": "category Of Allottee",
                    "totalAmtRaised": "Total Amount Size",
                    "considerationBy": "considerationBy",
                    "descriptionOfOtherCon": "descriptionOfOtherCon",
                    "dateOfSubmission": "Submission Date",
                    "checklist_zip_file_name": "zip Link"
                },
                "Listing Stage": {
                    "nseSymbol": "Symbol",
                    "nameOfTheCompany": "Company Name",
                    "stage": "Stage",
                    "issueType": "Issue Type",
                    "boardResDate": "Board Resolution Date",
                    "dateOfAllotmentOfShares": "Allotment Date",
                    "totalNumOfSharesAllotted": "No of Shares Allotted",
                    "amountRaised": "Final Issue Size",
                    "offerPricePerSecurity": "Issue Price Per Unit",
                    "numberOfEquitySharesListed": "No of Equity Shares Listed",
                    "dateOfSubmission": "Submission Date",
                    "dateOfListing": "Listing Date",
                    "dateOfTradingApproval": "Trading Approval Date",
                    "xmlFileName": "XML Link"
                }
            }

            df = df[[c for c in rename_map[stage] if c in df.columns]]
            df.rename(columns=rename_map[stage], inplace=True)

            # --- Keep Submission Date as-is; other dates can remain string ---
            df = df.fillna("").replace({float("inf"): "", float("-inf"): ""})

            # print(f"ℹ️ Successfully fetched {len(df)} QIP records for {symbol or period or 'All symbols'} ({stage})")
            return df

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching QIP data ({stage}): {e}")
            return None

    def cm_live_hist_right_issue(self, *args, from_date=None, to_date=None, period=None, symbol=None, stage=None):
        """
        Fetch Right Issue (RI) data from NSE (In-Principle or Listing Stage).

        Args (auto-detected from *args):
            - stage: "In-Principle" or "Listing Stage"
            - symbol: NSE symbol (e.g., "RELIANCE")
            - from_date: dd-mm-yyyy (e.g., "20-10-2024")
            - to_date: dd-mm-yyyy (e.g., "10-01-2025")
            - period: "1D", "1W", "1M", "3M", "6M", "1Y"

        Returns:
            pandas.DataFrame or None
        """

        today = datetime.now()
        today_str = today.strftime("%d-%m-%Y")
        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        period_pattern = re.compile(r"^(1D|1W|1M|3M|6M|1Y)$", re.IGNORECASE)

        # --- Auto-detect args ---
        for arg in args:
            if isinstance(arg, str):
                arg_title = arg.title()
                # Stage
                if arg_title in ["In-Principle", "Listing Stage"]:
                    stage = arg_title
                # Date
                elif date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                # Period
                elif period_pattern.match(arg.upper()):
                    period = arg.upper()
                # Symbol
                else:
                    symbol = arg.upper()
            elif isinstance(arg, datetime):
                if not from_date:
                    from_date = arg.strftime("%d-%m-%Y")
                elif not to_date:
                    to_date = arg.strftime("%d-%m-%Y")

        # --- Compute from period if no explicit dates ---
        if period and not (from_date and to_date):
            delta_map = {
                "1D": timedelta(days=1),
                "1W": timedelta(weeks=1),
                "1M": timedelta(days=30),
                "3M": timedelta(days=90),
                "6M": timedelta(days=180),
                "1Y": timedelta(days=365),
            }
            from_date = (today - delta_map[period]).strftime("%d-%m-%Y")
            to_date = today_str

        # --- Defaults and validation ---
        stage = (stage or "In-Principle").title()
        if stage not in ["In-Principle", "Listing Stage"]:
            print(f"❌ Invalid stage: {stage}. Must be 'In-Principle' or 'Listing Stage'.")
            return None

        if from_date and to_date:
            try:
                datetime.strptime(from_date, "%d-%m-%Y")
            except ValueError:
                print(f"❌ Invalid from_date: {from_date}. Using today's date.")
                from_date = today_str
            try:
                datetime.strptime(to_date, "%d-%m-%Y")
            except ValueError:
                print(f"❌ Invalid to_date: {to_date}. Using today's date.")
                to_date = today_str

        # --- Build API URL ---
        base_url = "https://www.nseindia.com/api/corporate-further-issues-ri"
        index_map = {"In-Principle": "FIPREFIP", "Listing Stage": "FIPREFLS"}
        params = {"index": index_map[stage]}

        if symbol:
            params["symbol"] = symbol.upper()
        elif from_date and to_date:
            params["from_date"] = from_date
            params["to_date"] = to_date

        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        api_url = f"{base_url}?{query_string}"

        # --- Rotate user agent ---
        self.rotate_user_agent()

        try:
            headers = self.headers.copy()
            headers.update({
                "Accept": "application/json",
                "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-RI"
            })

            session = requests.Session()
            # Fetch cookies
            session.get("https://www.nseindia.com/companies-listing/corporate-filings-RI", headers=headers, timeout=10)

            # Retry logic
            for attempt in range(3):
                try:
                    response = session.get(api_url, headers=headers, cookies=session.cookies.get_dict(), timeout=10)
                    response.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        print(f"❌ Failed to fetch data: {e}")
                        return None

            # Parse JSON
            try:
                data_json = response.json()
            except ValueError as e:
                print(f"❌ Failed to parse JSON: {e}")
                return None

            data = data_json.get("data", [])
            if not data:
                print(f"ℹ️ No Preferential Issue records found for {symbol or period or 'All symbols'} ({stage})")
                return None

            df = pd.DataFrame(data)

            # --- Column renaming ---
            rename_map = {
                "In-Principle": {
                    "nseSymbol": "Symbol",
                    "companyName": "Company Name",
                    "stage": "Stage",
                    "issueType": "Issue Type",
                    "boardResolutionDt": "Board Resolution Date",
                    "dateOfBrdResIssueApproving": "Board Approval Date",
                    "dateOfSubmission": "Submission Date",
                    "considerationBy": "Consideration Type",
                    "descOfOtherConsideration": "Other Consideration Description",
                    "totalAmntRaised": "Total Amount Raised",
                    "xmlFileName": "XML Link"
                },
                "Listing Stage": {
                    "nseSymbol": "Symbol",
                    "companyName": "Company Name",
                    "stage": "Stage",
                    "issueType": "Issue Type",
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
                    "xmlFileName": "XML Link"
                }
            }

            df = df[[c for c in rename_map[stage] if c in df.columns]]
            df.rename(columns=rename_map[stage], inplace=True)

            # --- Keep Submission Date as-is; other dates can remain string ---
            df = df.fillna("").replace({float("inf"): "", float("-inf"): ""})

            # print(f"ℹ️ Successfully fetched {len(df)} QIP records for {symbol or period or 'All symbols'} ({stage})")
            return df

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching QIP data ({stage}): {e}")
            return None

    def cm_live_voting_results(self):
        """
        Fetch and process corporate voting results from NSE India.
        Handles both metadata and nested agendas, and flattens data
        for Google Sheets compatibility.
        """

        self.rotate_user_agent()

        ref_url = "https://www.nseindia.com/companies-listing/corporate-filings-voting-results"
        api_url = "https://www.nseindia.com/api/corporate-voting-results?"

        try:
            # --- Step 1: Retrieve cookies for authentication ---
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # --- Step 2: Fetch API data ---
            response = self.session.get(api_url, headers=self.headers,
                                        cookies=ref_response.cookies.get_dict(), timeout=15)
            response.raise_for_status()
            raw_data = response.json()

            all_rows = []

            # --- Step 3: Process metadata and nested agendas ---
            for item in raw_data:
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

            # --- Step 4: Convert to DataFrame ---
            df = pd.DataFrame(all_rows)

            # --- Step 5: Replace NaN, inf values, and ensure string compatibility ---
            df.replace({float("inf"): None, float("-inf"): None}, inplace=True)
            df.fillna("", inplace=True)

            # --- Step 6: Flatten nested objects for Google Sheets ---
            def flatten_value(v):
                if isinstance(v, (list, dict)):
                    return json.dumps(v, ensure_ascii=False)
                elif v is None:
                    return ""
                else:
                    return str(v)

            for col in df.columns:
                df[col] = df[col].map(flatten_value)

            # --- Step 7: Reorder key columns for readability ---
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

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching voting results: {e}")
            return None

    def cm_live_qtly_shareholding_patterns(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern'
        api_url = 'https://www.nseindia.com/api/corporate-share-holdings-master?index=equities'

        try:
            # Get reference cookies from the main page
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()  # Ensure request was successful

            # Make API call using cookies from the previous request
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Convert response to JSON
            data = response.json()
            
            # Directly assume data is a list instead of checking 'data' key
            if isinstance(data, list):
                df = pd.DataFrame(data)

                # Selecting and ordering columns
                required_columns = ['symbol', 'name', 'pr_and_prgrp', 'public_val', 'employeeTrusts', 'revisedStatus', 'date', 'submissionDate', 'revisionDate', 'xbrl', 'broadcastDate', 'systemDate', 'timeDifference']
                df = df[required_columns]

                return df if not df.empty else None  # Return DataFrame if not empty
            return None  # Return None if data is not a list or is empty

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching Shareholding Patterns data: {e}")
            return None

    def cm_live_hist_annual_reports(self, *args, from_date=None, to_date=None, symbol=None):
        """
        annual reports serach symbol only so we use "BUSINESS RESPONSIBILITY AND SUSTAINABILITY REPORTS" to find annual reports.
    
        """
        # --- Detect date pattern ---
        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        today_str = datetime.now().strftime("%d-%m-%Y")

        # --- Auto-detect arguments (dates or symbol) ---
        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                else:
                    symbol = arg.upper()

        # --- Rotate user-agent for reliability ---
        self.rotate_user_agent()

        # --- NSE reference URL ---
        ref_url = "https://www.nseindia.com/companies-listing/corporate-filings-bussiness-sustainabilitiy-reports"

        # --- Final API URL selection logic ---
        if symbol and from_date and to_date:
            api_url = f"https://www.nseindia.com/api/corporate-bussiness-sustainabilitiy?index=equities&from_date={from_date}&to_date={to_date}&symbol={symbol}"
        elif symbol:
            api_url = f"https://www.nseindia.com/api/corporate-bussiness-sustainabilitiy?index=equities&symbol={symbol}"
        elif from_date and to_date:
            api_url = f"https://www.nseindia.com/api/corporate-bussiness-sustainabilitiy?index=equities&from_date={from_date}&to_date={to_date}"
        else:
            api_url = "https://www.nseindia.com/api/corporate-bussiness-sustainabilitiy"

        # --- Fetch & process ---
        try:
            # Step 1: Establish session
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Step 2: API request
            response = self.session.get(
                api_url,
                headers=self.headers,
                cookies=ref_response.cookies.get_dict(),
                timeout=10,
            )
            response.raise_for_status()

            # Step 3: Parse JSON → DataFrame
            data_json = response.json()
            data = data_json.get("data", [])  # ✅ Correct key

            if isinstance(data, list) and len(data) > 0:
                df = pd.DataFrame(data)
                expected_cols = ['symbol', 'companyName', 'fyFrom', 'fyTo','submissionDate', 'revisionDate']
                df = df[[c for c in expected_cols if c in df.columns]]
                df = df.fillna("").replace({float("inf"): "", float("-inf"): ""})
                return df

            else:
                scope = symbol or "All symbols"
                print(f"ℹ️  No Annual Reports found for {scope} between {from_date or '-'} and {to_date or '-'}")
                return None

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching Annual Reports: {e}")
            return None

    
    #---------------------------------------------------------- FnO_Live_Data ----------------------------------------------------------------
 
    #---------------------------------------------------------- futures ---------------------------------------------------------------------
    def fno_live_futures_data(self, symbol, indices=False):
        symbol = symbol.replace(' ', '%20').replace('&', '%26')
        self.rotate_user_agent()
        ref_url = f'https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}'
        ref = requests.get(ref_url, headers=self.headers)
        try:
            url = f'https://www.nseindia.com/api/quote-derivative?symbol={symbol}'
            data = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10).json()
            lst = [i["metadata"] for i in data["stocks"] if i["metadata"]["instrumentType"] == ("Index Futures" if indices else "Stock Futures")]
            df = pd.DataFrame(lst)
            df = df.set_index("identifier", drop=True)
            return df
        except (requests.RequestException, ValueError):
            return None

    def fno_live_most_active_futures_contracts(self, mode="Volume"):
        """
        Fetch most active NSE F&O futures contracts.

        Parameters
        ----------
        mode : str
            "Volume" for volume-based data, "Value" for value-based data.
            Defaults to "Volume".

        Returns
        -------
        pd.DataFrame or None
            DataFrame of most active futures contracts, or None if failed.
        """
        self.rotate_user_agent()
        ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'

        try:
            # Get reference cookies
            ref = requests.get(ref_url, headers=self.headers, timeout=10)
            url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=futures'
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            data = response.json()

            # Select data based on mode
            if mode.lower() == "value":
                df = pd.DataFrame(data['value']['data'])
            else:  # Default to volume
                df = pd.DataFrame(data['volume']['data'])

            return df if not df.empty else None

        except (requests.RequestException, ValueError, KeyError):
            return None

    #---------------------------------------------------------- index ---------------------------------------------------------------------

    def fno_live_most_active(self, mode="Index", opt="Call", sort_by="Volume"):
        """
        Fetch most active F&O contracts in a unified function.
        
        Parameters:
        -----------
        instrument : str
            "Index" or "Stock"
        option_type : str
            "Call" or "Put"
        sort_by : str
            "Volume" or "Value"
        
        Returns:
        --------
        pd.DataFrame or None
            DataFrame of most active contracts, or None if no data/error.
        
        Usage:
        ------
        fno_live_most_active("Index", "Call", "Value")
        fno_live_most_active("Stock", "Put", "Volume")
        """
        # Normalize inputs
        mode = mode.capitalize()
        opt = opt.capitalize()
        sort_by = sort_by.capitalize()

        if mode not in ["Index", "Stock"]:
            raise ValueError("mode must be 'Index' or 'Stock'")
        if opt not in ["Call", "Put"]:
            raise ValueError("opt must be 'Call' or 'Put'")
        if sort_by not in ["Volume", "Value"]:
            raise ValueError("sort_by must be 'Volume' or 'Value'")

        # Map sort_by to NSE API suffix
        sort_map = {"Volume": "vol", "Value": "val"}
        suffix = sort_map[sort_by]

        # Map parameters to NSE API ?index= string
        if mode == "Index":
            api_index = f"{opt.lower()}s-index-{suffix}"
            key = "OPTIDX"
        else:  # Stock
            api_index = f"{opt.lower()}s-stocks-{suffix}"
            key = "OPTSTK"

        # Rotate user agent and get reference cookies
        self.rotate_user_agent()
        ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
        ref = requests.get(ref_url, headers=self.headers)

        url = f"https://www.nseindia.com/api/snapshot-derivatives-equity?index={api_index}"

        try:
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            data = response.json()
            df = pd.DataFrame(data[key]['data'])
            return df if not df.empty else None
        except (requests.RequestException, ValueError, KeyError):
            return None


    # def fno_live_most_active_index_calls(self):
    #     self.rotate_user_agent()
    #     ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
    #     ref = requests.get(ref_url, headers=self.headers)
    #     url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=calls-index-vol'
    #     try:
    #         response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
    #         data = response.json()
    #         df = pd.DataFrame(data['OPTIDX']['data'])
    #         return df if not df.empty else None
    #     except (requests.RequestException, ValueError):
    #         return None

    # def fno_live_most_active_index_puts(self):
    #     self.rotate_user_agent()
    #     ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
    #     ref = requests.get(ref_url, headers=self.headers)
    #     url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=puts-index-vol'
    #     try:
    #         response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
    #         data = response.json()
    #         df = pd.DataFrame(data['OPTIDX']['data'])
    #         return df if not df.empty else None
    #     except (requests.RequestException, ValueError):
    #         return None



    # def fno_live_most_active_stock_calls(self):
    #     self.rotate_user_agent()
    #     ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
    #     ref = requests.get(ref_url, headers=self.headers)
    #     url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=calls-stocks-vol'
    #     try:
    #         response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
    #         data = response.json()
    #         df = pd.DataFrame(data['OPTSTK']['data'])
    #         return df if not df.empty else None
    #     except (requests.RequestException, ValueError):
    #         return None

    # def fno_live_most_active_stock_puts(self):
    #     self.rotate_user_agent()
    #     ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
    #     ref = requests.get(ref_url, headers=self.headers)
    #     url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=puts-stocks-vol'
    #     try:
    #         response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
    #         data = response.json()
    #         df = pd.DataFrame(data['OPTSTK']['data'])
    #         return df if not df.empty else None
    #     except (requests.RequestException, ValueError):
    #         return None

    # def fno_live_most_active_stock_calls_value(self):
    #     self.rotate_user_agent()
    #     ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
    #     ref = requests.get(ref_url, headers=self.headers)
    #     url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=calls-stocks-val'
    #     try:
    #         response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
    #         data = response.json()
    #         df = pd.DataFrame(data['OPTSTK']['data'])
    #         return df if not df.empty else None
    #     except (requests.RequestException, ValueError):
    #         return None

    # def fno_live_most_active_stock_puts_value(self):
    #     self.rotate_user_agent()
    #     ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
    #     ref = requests.get(ref_url, headers=self.headers)
    #     url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=puts-stocks-val'
    #     try:
    #         response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
    #         data = response.json()
    #         df = pd.DataFrame(data['OPTSTK']['data'])
    #         return df if not df.empty else None
    #     except (requests.RequestException, ValueError):
    #         return None 



    def fno_live_most_active_contracts_by_oi(self):
        self.rotate_user_agent()
        ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
        ref = requests.get(ref_url, headers=self.headers)
        url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=oi'
        try:
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            data = response.json()
            df = pd.DataFrame(data['volume']['data'])
            return df if not df.empty else None
        except (requests.RequestException, ValueError):
            return None

    def fno_live_most_active_contracts_by_volume(self):
        self.rotate_user_agent()
        ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
        ref = requests.get(ref_url, headers=self.headers)
        url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=contracts'
        try:
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            data = response.json()
            df = pd.DataFrame(data['volume']['data'])
            return df if not df.empty else None
        except (requests.RequestException, ValueError):
            return None


    def fno_live_most_active_options_contracts_by_volume(self):
        self.rotate_user_agent()
        ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
        ref = requests.get(ref_url, headers=self.headers)
        url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=options&limit=20'
        try:
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            data = response.json()
            df = pd.DataFrame(data['volume']['data'])
            return df if not df.empty else None
        except (requests.RequestException, ValueError):
            return None

    def fno_expiry_dates(self,symbol="NIFTY"):
        self.rotate_user_agent()
        ref_url = 'https://www.nseindia.com/option-chain'
        ref = requests.get(ref_url, headers=self.headers)
        url = f'https://www.nseindia.com/api/option-chain-contract-info?symbol={symbol}'
        try:
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            data = response.json()
            expiry_dates = pd.to_datetime(data['expiryDates'], format='%d-%b-%Y')
        except Exception:
            return None

        # Remove today if after 3:30 PM
        now = datetime.now()
        if expiry_dates[0].date() == now.date() and now.time() > time(15, 30):
            expiry_dates = expiry_dates[1:]

        # Return only the expiry dates
        df = pd.DataFrame({
            "Expiry Date": expiry_dates.strftime("%d-%b-%Y")
        })
        return df


    def fno_live_option_chain(self, symbol: str, expiry_date: str = None, oi_mode: str = "full", indices: bool = False):
        symbol = symbol.replace(' ', '%20').replace('&', '%26')
        self.rotate_user_agent()
        
        ref_url = f'https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}'
        ref = requests.get(ref_url, headers=self.headers)
        
        url = f"https://www.nseindia.com/api/option-chain-{'indices' if indices else 'equities'}?symbol={symbol}"
        
        try:
            payload = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10).json()
            
            if expiry_date:
                exp_date = pd.to_datetime(expiry_date, format='%d-%m-%Y')
                expiry_date = exp_date.strftime('%d-%b-%Y')

            # Define column names with Underlying_Value as the last column
            if oi_mode == 'compact':
                col_names = ['Fetch_Time', 'Symbol', 'Expiry_Date', 'CALLS_OI', 'CALLS_Chng_in_OI', 'CALLS_Volume', 'CALLS_IV',
                            'CALLS_LTP', 'CALLS_Net_Chng', 'Strike_Price', 'PUTS_OI', 'PUTS_Chng_in_OI', 'PUTS_Volume',
                            'PUTS_IV', 'PUTS_LTP', 'PUTS_Net_Chng', 'Underlying_Value']
            else:
                col_names = ['Fetch_Time', 'Symbol', 'Expiry_Date', 'CALLS_OI', 'CALLS_Chng_in_OI', 'CALLS_Volume', 'CALLS_IV',
                            'CALLS_LTP', 'CALLS_Net_Chng', 'CALLS_Bid_Qty', 'CALLS_Bid_Price', 'CALLS_Ask_Price',
                            'CALLS_Ask_Qty', 'Strike_Price', 'PUTS_Bid_Qty', 'PUTS_Bid_Price', 'PUTS_Ask_Price', 'PUTS_Ask_Qty',
                            'PUTS_Net_Chng', 'PUTS_LTP', 'PUTS_IV', 'PUTS_Volume', 'PUTS_Chng_in_OI', 'PUTS_OI', 'Underlying_Value']

            # Define dtypes to avoid inference issues
            dtypes = {
                'Fetch_Time': 'object',
                'Symbol': 'object',
                'Expiry_Date': 'object',
                'Strike_Price': 'float64',
                'CALLS_OI': 'int64', 'CALLS_Chng_in_OI': 'int64', 'CALLS_Volume': 'int64', 'CALLS_IV': 'float64',
                'CALLS_LTP': 'float64', 'CALLS_Net_Chng': 'float64',
                'PUTS_OI': 'int64', 'PUTS_Chng_in_OI': 'int64', 'PUTS_Volume': 'int64', 'PUTS_IV': 'float64',
                'PUTS_LTP': 'float64', 'PUTS_Net_Chng': 'float64',
                'Underlying_Value': 'float64'
            }
            if oi_mode == 'full':
                dtypes.update({
                    'CALLS_Bid_Qty': 'int64', 'CALLS_Bid_Price': 'float64', 'CALLS_Ask_Price': 'float64', 'CALLS_Ask_Qty': 'int64',
                    'PUTS_Bid_Qty': 'int64', 'PUTS_Bid_Price': 'float64', 'PUTS_Ask_Price': 'float64', 'PUTS_Ask_Qty': 'int64'
                })

            # Check if payload has data
            if not payload.get('records') or not payload['records'].get('data'):
                return pd.DataFrame(columns=col_names).astype(dtypes)

            # Collect rows in a list instead of concatenating in loop
            rows = []
            for record in payload['records']['data']:
                if not expiry_date or (record['expiryDate'] == expiry_date):
                    oi_row = {
                        'Fetch_Time': payload['records']['timestamp'],
                        'Symbol': symbol,
                        'Expiry_Date': record['expiryDate'],
                        'Strike_Price': record['strikePrice'],
                        'CALLS_OI': record.get('CE', {}).get('openInterest', 0),
                        'CALLS_Chng_in_OI': record.get('CE', {}).get('changeinOpenInterest', 0),
                        'CALLS_Volume': record.get('CE', {}).get('totalTradedVolume', 0),
                        'CALLS_IV': record.get('CE', {}).get('impliedVolatility', 0),
                        'CALLS_LTP': record.get('CE', {}).get('lastPrice', 0),
                        'CALLS_Net_Chng': record.get('CE', {}).get('change', 0),
                        'PUTS_OI': record.get('PE', {}).get('openInterest', 0),
                        'PUTS_Chng_in_OI': record.get('PE', {}).get('changeinOpenInterest', 0),
                        'PUTS_Volume': record.get('PE', {}).get('totalTradedVolume', 0),
                        'PUTS_IV': record.get('PE', {}).get('impliedVolatility', 0),
                        'PUTS_LTP': record.get('PE', {}).get('lastPrice', 0),
                        'PUTS_Net_Chng': record.get('PE', {}).get('change', 0),
                        'Underlying_Value': record.get('PE', {}).get('underlyingValue', 0)  # Added as last field
                    }
                    if oi_mode == 'full':
                        oi_row.update({
                            'CALLS_Bid_Qty': record.get('CE', {}).get('bidQty', 0),
                            'CALLS_Bid_Price': record.get('CE', {}).get('bidprice', 0),
                            'CALLS_Ask_Price': record.get('CE', {}).get('askPrice', 0),
                            'CALLS_Ask_Qty': record.get('CE', {}).get('askQty', 0),
                            'PUTS_Bid_Qty': record.get('PE', {}).get('bidQty', 0),
                            'PUTS_Bid_Price': record.get('PE', {}).get('bidprice', 0),
                            'PUTS_Ask_Price': record.get('PE', {}).get('askPrice', 0),
                            'PUTS_Ask_Qty': record.get('PE', {}).get('askQty', 0)
                        })

                    rows.append(oi_row)

            # Create DataFrame once with all rows
            if rows:
                oi_data = pd.DataFrame(rows, columns=col_names).astype(dtypes)  # Explicitly set column order
            else:
                oi_data = pd.DataFrame(columns=col_names).astype(dtypes)

            return oi_data

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return pd.DataFrame(columns=col_names)
        except ValueError as e:
            print(f"Error parsing JSON: {e}")
            return pd.DataFrame(columns=col_names)
        except Exception as e:
            print(f"Unexpected error: {e}")
            return pd.DataFrame(columns=col_names)
        

    def fno_live_nifty_active_contracts(self, symbol: str, expiry_date: str = None):
        self.rotate_user_agent()
        
        ref_url = f'https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}'
        ref = requests.get(ref_url, headers=self.headers)
        
        url = f"https://www.nseindia.com/api/quote-derivative?symbol={symbol}"
        
        try:
            payload = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10).json()
            
            if expiry_date:
                exp_date = pd.to_datetime(expiry_date, format='%d-%m-%Y')
                expiry_date = exp_date.strftime('%d-%b-%Y')
            
            # Filter only Stock Options data
            stocks_data = [
                stock for stock in payload.get("stocks", [])
                if stock["metadata"]["instrumentType"] == "Index Options"
            ]
            
            # If expiry_date is specified, filter by expiry date as well
            if expiry_date:
                stocks_data = [
                    stock for stock in stocks_data
                    if stock["metadata"]["expiryDate"] == expiry_date
                ]
            
            # Prepare table data
            table_data = []
            for stock in stocks_data:
                metadata = stock["metadata"]
                marketDeptOrderBook = stock["marketDeptOrderBook"]
                trade_info = stock["marketDeptOrderBook"]["tradeInfo"]
                table_data.append({
                    "Instrument Type": metadata["instrumentType"],
                    "Expiry Date": metadata["expiryDate"],
                    "Option Type": metadata["optionType"],
                    "Strike Price": metadata["strikePrice"],
                    "Open": metadata.get("openPrice", 0),
                    "High": metadata.get("highPrice", 0),
                    "Low": metadata.get("lowPrice", 0),
                    "closePrice":metadata.get("closePrice", 0),
                    "Prev Close": metadata.get("prevClose", 0),
                    "Last": metadata.get("lastPrice", 0),
                    "Change": metadata.get("change", 0),
                    "%Change": metadata.get("pChange", 0),
                    "Volume (Contracts)": metadata.get("numberOfContractsTraded", 0),
                    "Value (₹ Lakhs)": metadata.get("totalTurnover", 0),
                    "totalBuyQuantity": marketDeptOrderBook.get("totalBuyQuantity", 0),
                    "totalSellQuantity": marketDeptOrderBook.get("totalSellQuantity", 0),
                    "OI": trade_info.get("openInterest", 0),
                    "Chng in OI": trade_info.get("changeinOpenInterest", 0),
                    "% Chng in OI": trade_info.get("pchangeinOpenInterest", 0),
                    "VWAP": trade_info.get("vmap", 0)
                })
            
            return table_data
            
        except Exception as e:
            print(f"Error fetching nifty active contracts data: {str(e)}")
            return None
        
    def fno_live_stock_active_contracts(self, symbol: str, expiry_date: str = None):
        self.rotate_user_agent()
        
        ref_url = f'https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}'
        ref = requests.get(ref_url, headers=self.headers)
        
        url = f"https://www.nseindia.com/api/quote-derivative?symbol={symbol}"
        
        try:
            payload = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10).json()
            
            if expiry_date:
                exp_date = pd.to_datetime(expiry_date, format='%d-%m-%Y')
                expiry_date = exp_date.strftime('%d-%b-%Y')
            
            # Filter only Stock Options data
            stocks_data = [
                stock for stock in payload.get("stocks", [])
                if stock["metadata"]["instrumentType"] == "Stock Options"
            ]
            
            # If expiry_date is specified, filter by expiry date as well
            if expiry_date:
                stocks_data = [
                    stock for stock in stocks_data
                    if stock["metadata"]["expiryDate"] == expiry_date
                ]
            
            # Prepare table data
            table_data = []
            for stock in stocks_data:
                metadata = stock["metadata"]
                marketDeptOrderBook = stock["marketDeptOrderBook"]
                trade_info = stock["marketDeptOrderBook"]["tradeInfo"]
                table_data.append({
                    "Instrument Type": metadata["instrumentType"],
                    "Expiry Date": metadata["expiryDate"],
                    "Option Type": metadata["optionType"],
                    "Strike Price": metadata["strikePrice"],
                    "Open": metadata.get("openPrice", 0),
                    "High": metadata.get("highPrice", 0),
                    "Low": metadata.get("lowPrice", 0),
                    "closePrice":metadata.get("closePrice", 0),
                    "Prev Close": metadata.get("prevClose", 0),
                    "Last": metadata.get("lastPrice", 0),
                    "Change": metadata.get("change", 0),
                    "%Change": metadata.get("pChange", 0),
                    "Volume (Contracts)": metadata.get("numberOfContractsTraded", 0),
                    "Value (₹ Lakhs)": metadata.get("totalTurnover", 0),
                    "totalBuyQuantity": marketDeptOrderBook.get("totalBuyQuantity", 0),
                    "totalSellQuantity": marketDeptOrderBook.get("totalSellQuantity", 0),
                    "OI": trade_info.get("openInterest", 0),
                    "Chng in OI": trade_info.get("changeinOpenInterest", 0),
                    "% Chng in OI": trade_info.get("pchangeinOpenInterest", 0),
                    "VWAP": trade_info.get("vmap", 0)
                })
            
            return table_data
            
        except Exception as e:
            print(f"Error fetching Stock active contracts data: {str(e)}")
            return None
        

    #---------------------------------------------------------- CM_Eod_Data ----------------------------------------------------------------

    def cm_eod_fii_dii_activity(self, exchange="All"):
        """
        Fetch End-of-Day FII/DII activity from NSE.

        Parameters:
        -----------
        exchange : str
            "Nse" for NSE FII/DII data, "All" for combined/all exchange data.

        Returns:
        --------
        pd.DataFrame or None
            Returns a DataFrame containing FII/DII activity. None if request fails.
        """
        self.rotate_user_agent()  
        ref_url = 'https://www.nseindia.com/reports/fii-dii'
        ref = requests.get(ref_url, headers=self.headers)
        
        # Map exchange to API endpoint
        endpoints = {
            "Nse": "https://www.nseindia.com/api/fiidiiTradeNse",
            "All": "https://www.nseindia.com/api/fiidiiTradeReact"
        }

        url = endpoints.get(exchange, endpoints["All"])
        
        # Rotate user agent if you have this function
        try:
            response = self.session.get(url, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            return df
        
        except (requests.RequestException, ValueError) as e:
            print(f"Error fetching FII/DII data: {e}")
            return None
   
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
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        use_date = trade_date.strftime("%d%m%Y")
        url = f'https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{use_date}.csv'
        try:
            request_bhav = requests.get(url, headers=self.headers, timeout=10)
            request_bhav.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(request_bhav.content))
            bhav_df.columns = [name.replace(' ', '') for name in bhav_df.columns]
            bhav_df['SERIES'] = bhav_df['SERIES'].str.replace(' ', '')
            bhav_df['DATE1'] = bhav_df['DATE1'].str.replace(' ', '')
            return bhav_df
        except requests.RequestException:
            return None

    def cm_eod_equity_bhavcopy(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        url = 'https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_'
        payload = f"{trade_date.strftime('%Y%m%d')}_F_0000.csv.zip"
        
        try:
            response = requests.get(url + payload, headers=self.headers, timeout=10)
            response.raise_for_status()

            with zipfile.ZipFile(BytesIO(response.content)) as zip_bhav:
                # Usually only one CSV per zip, but iterate just in case
                bhav_df = pd.DataFrame()
                for file_name in zip_bhav.namelist():
                    temp_df = pd.read_csv(zip_bhav.open(file_name))
                    bhav_df = pd.concat([bhav_df, temp_df], ignore_index=True)
            
            # Filter only EQ securities
            bhav_df = bhav_df[bhav_df['SctySrs'] == 'EQ'].reset_index(drop=True)
            return bhav_df

        except requests.RequestException as e:
            print(f"Error fetching BhavCopy: {e}")
            return None
        
    def cm_eod_52_week_high_low(self, trade_date: str):
        """
        Download NSE 52 Week High Low CSV and return raw rows (list of lists).
        Fast + Safe version.
        """
        self.rotate_user_agent()
        raw_trade_date = trade_date  # keep original string

        try:
            # Convert date (expects dd-mm-YYYY)
            trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
            url = f"https://nsearchives.nseindia.com/content/CM_52_wk_High_low_{trade_date.strftime('%d%m%Y')}.csv"

            # Fetch CSV
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()

            # Decode safely (ignore bad chars) and split lines directly (fast)
            csv_text = nse_resp.content.decode("utf-8", errors="ignore")
            rows = list(csv.reader(csv_text.splitlines()))

            return rows if rows else None

        except Exception as e:
            print(f"❌ Error fetching 52 Week High/Low for {raw_trade_date}: {e}")
            return None
        
    def cm_eod_bulk_deal(self):
        self.rotate_user_agent()
        url = f"https://nsearchives.nseindia.com/content/equities/bulk.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None
        
    def cm_eod_block_deal(self):
        self.rotate_user_agent()
        url = f"https://nsearchives.nseindia.com/content/equities/block.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None
        
    def cm_eod_shortselling(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        url = f"https://nsearchives.nseindia.com/archives/equities/shortSelling/shortselling_{str(trade_date.strftime('%d%m%Y').upper())}.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None

    def cm_eod_surveillance_indicator(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%y")
        url = f"https://nsearchives.nseindia.com/content/cm/REG1_IND{str(trade_date.strftime('%d%m%y').upper())}.csv"
        
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))

            # Replace 100 with blank in columns F to R
            columns_to_replace = bhav_df.loc[:, 'GSM':'Filler31'].columns
            bhav_df[columns_to_replace] = bhav_df[columns_to_replace].replace(100, '')

            return bhav_df
        except (requests.RequestException, ValueError) as e:
            print("Error:", e)
            return None
        
    def cm_eod_series_change(self):
        self.rotate_user_agent()
        url = f"https://nsearchives.nseindia.com/content/equities/series_change.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None

    def cm_eod_eq_band_changes(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        url = f"https://nsearchives.nseindia.com/content/equities/eq_band_changes_{str(trade_date.strftime('%d%m%Y').upper())}.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None
        
    def cm_eod_eq_price_band(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        url = f"https://nsearchives.nseindia.com/content/equities/sec_list_{str(trade_date.strftime('%d%m%Y').upper())}.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None
        
    def cm_hist_eq_price_band(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        """
        Fetch Historical NSE Price Band (CSV Mode)
        Auto-detects date range, symbol, and period.

        Usage Examples:
        ----------------
        get.cm_hist_eq_price_band()                              # Today's data for all symbols
        get.cm_hist_eq_price_band("1W")                          # 1 week for all symbols
        get.cm_hist_eq_price_band("01-10-2025")                  # From date, Auto Today date for all symbols
        get.cm_hist_eq_price_band("01-10-2025", "17-10-2025")    # Custom range for all symbols
        get.cm_hist_eq_price_band("RELIANCE")                    # Today for symbol
        get.cm_hist_eq_price_band("RELIANCE", "1M")              # 1 month for symbol
        get.cm_hist_eq_price_band("RELIANCE", "01-10-2025")      # From date, Auto Today date for symbols
        get.cm_hist_eq_price_band("RELIANCE", "01-10-2025", "17-10-2025")  # Range for symbol
        """

        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        today = datetime.now()
        today_str = today.strftime("%d-%m-%Y")

        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                elif arg.upper() in ["1D", "1W", "1M", "3M", "6M", "1Y"]:
                    period = arg.upper()
                else:
                    symbol = arg.upper()

        # --- Compute date range from period --- #
        if period:
            delta_map = {
                "1D": timedelta(days=1),
                "1W": timedelta(weeks=1),
                "1M": timedelta(days=30),
                "3M": timedelta(days=90),
                "6M": timedelta(days=180),
                "1Y": timedelta(days=365),
            }
            delta = delta_map.get(period, timedelta(days=365))
            from_date = (today - delta).strftime("%d-%m-%Y")
            to_date = to_date or today_str

        from_date = from_date or today_str
        to_date = to_date or today_str

        # --- Rotate User-Agent --- #
        self.rotate_user_agent()

        # --- URLs --- #
        ref_url = "https://www.nseindia.com/reports/price-band-changes"
        base_url = "https://www.nseindia.com/api/eqsurvactions"

        if symbol:
            api_url = f"{base_url}?from_date={from_date}&to_date={to_date}&symbol={symbol}&csv=true"
        else:
            api_url = f"{base_url}?from_date={from_date}&to_date={to_date}&csv=true"

        # --- Fetch data --- #
        try:
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Retry with exponential backoff
            for attempt in range(3):
                try:
                    response = self.session.get(
                        api_url,
                        headers=self.headers,
                        cookies=ref_response.cookies.get_dict(),
                        timeout=10,
                    )
                    response.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt < 2:
                        wait_time = 2 ** attempt
                        print(f"⚠️ Attempt {attempt + 1} failed ({e}); retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise

            # --- Detect non-CSV (HTML error page) --- #
            if "text/html" in response.headers.get("Content-Type", ""):
                print("⚠️ NSE returned HTML instead of CSV. Headers or session may be invalid.")
                return None

            # --- Read CSV with UTF-8 BOM handling at byte level --- #
            # Decode response content and strip BOM at byte level
            content = response.content
            if content.startswith(b'\xef\xbb\xbf'):  # Check for UTF-8 BOM
                content = content[3:]  # Remove BOM
            cleaned_text = content.decode('utf-8')
            df = pd.read_csv(StringIO(cleaned_text))

            # --- Handle empty or invalid response --- #
            if df.empty or len(df.columns) < 2:
                print(f"ℹ️ No Price Band Changes data found for {symbol or 'ALL'} between {from_date} and {to_date}")
                return None

            # --- 🔹 FIX 1: Clean Headers (Quotes + Spaces) --- #
            df.columns = [c.strip().replace('"', '') for c in df.columns]
            df.rename(columns=lambda x: x.strip(), inplace=True)  # Ensure no leading/trailing spaces

            # --- 🔹 FIX 2: Clean numeric columns safely --- #
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
                    try:
                        df[col] = pd.to_numeric(df[col])
                    except Exception:
                        pass

            # --- Sort and format dates --- #
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce", format="%d-%b-%Y")
                df = df.sort_values("Date", ascending=False).reset_index(drop=True)
                df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")

            # --- Final cleanup --- #
            df = df.fillna("").replace({float("inf"): "", float("-inf"): ""})
            return df

        except Exception as e:
            print(f"❌ Error fetching Price Band Change: {e}")
            return None

    def cm_eod_pe_ratio(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%y")
        url = f"https://nsearchives.nseindia.com/content/equities/peDetail/PE_{str(trade_date.strftime('%d%m%y').upper())}.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None

    def cm_eod_mcap(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%y")
        url = f"https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{trade_date.strftime('%d%m%y').upper()}.zip"

        try:
            request_bhav = requests.get(url, headers=self.headers, timeout=10)
            request_bhav.raise_for_status()
            zip_bhav = zipfile.ZipFile(BytesIO(request_bhav.content), 'r')
            bhav_df = pd.DataFrame()

            for file_name in zip_bhav.namelist():
                if file_name.lower().startswith("mcap") and file_name.endswith(".csv"):
                    bhav_df = pd.read_csv(zip_bhav.open(file_name))
                    break

            if bhav_df.empty:
                print("No MCAP CSV file found in the ZIP archive.")
                return None

            return bhav_df

        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def cm_eod_eq_name_change(self):
        self.rotate_user_agent()
        url = f"https://nsearchives.nseindia.com/content/equities/namechange.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))

            # Convert the 4th column (index 3) to date format YYYY-MM-DD using explicit format
            if bhav_df.shape[1] >= 4:  # Ensure 4 columns exist
                bhav_df.iloc[:, 3] = pd.to_datetime(
                    bhav_df.iloc[:, 3], 
                    format='%d-%b-%Y',  # specify the date format explicitly
                    errors='coerce'
                ).dt.strftime('%Y-%m-%d')

                # Sort by the 4th column descending (new to old)
                bhav_df = bhav_df.sort_values(by=bhav_df.columns[3], ascending=False).reset_index(drop=True)

            return bhav_df
        except (requests.RequestException, ValueError) as e:
            print(f"Error fetching or processing data: {e}")
            return None
        
    def cm_eod_eq_symbol_change(self):
        self.rotate_user_agent()
        url = f"https://nsearchives.nseindia.com/content/equities/symbolchange.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content), header=None)  # No header specified

            # Convert the 4th column (index 3) to date format YYYY-MM-DD using explicit format
            if bhav_df.shape[1] >= 4:  # Ensure 4 columns exist
                bhav_df.iloc[:, 3] = pd.to_datetime(
                    bhav_df.iloc[:, 3], 
                    format='%d-%b-%Y',  # specify the date format explicitly
                    errors='coerce'
                ).dt.strftime('%Y-%m-%d')  # Convert to string format for JSON serialization

                # Sort by the 4th column descending (new to old)
                bhav_df = bhav_df.sort_values(by=bhav_df.columns[3], ascending=False).reset_index(drop=True)

            return bhav_df
        except (requests.RequestException, ValueError) as e:
            print(f"Error fetching or processing data: {e}")
            return None

   
    def cm_hist_security_wise_data(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        """
        Fetch historical price-volume-deliverable data for a given NSE security.
        Supports:
            - Date range (from_date, to_date)
            - Period shortcuts (1D, 1W, 1M, 3M, 6M, 1Y)
        Automatically splits requests into 3-month chunks to bypass NSE API limits.
        If to_date is not provided, defaults to today.
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
                elif arg.upper() in ["1D", "1W", "1M", "3M", "6M", "1Y"]:
                    period = arg.upper()
                else:
                    symbol = arg.upper()

        # --- Compute date range from period ---
        if period:
            delta_map = {
                "1D": timedelta(days=1),
                "1W": timedelta(weeks=1),
                "1M": timedelta(days=30),
                "3M": timedelta(days=90),
                "6M": timedelta(days=180),
                "1Y": timedelta(days=365),
            }
            delta = delta_map.get(period, timedelta(days=365))
            from_date = (today - delta).strftime("%d-%m-%Y")
            if not to_date:
                to_date = today_str

        # --- Default dates if not provided ---
        if not from_date:
            from_date = (today - timedelta(days=365)).strftime("%d-%m-%Y")
        if not to_date:
            to_date = today_str

        # --- Rotate User-Agent ---
        self.rotate_user_agent()

        ref_url = "https://www.nseindia.com/report-detail/eq_security"
        base_api = (
            "https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData?"
            "from={}&to={}&symbol={}&type=priceVolumeDeliverable&series=ALL"
        )

        try:
            # Get NSE cookies
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Convert dates
            start_dt = datetime.strptime(from_date, "%d-%m-%Y")
            end_dt = datetime.strptime(to_date, "%d-%m-%Y")

            # --- Split date range into 3-month chunks (~90 days) ---
            all_data = []
            chunk_days = 89

            while start_dt <= end_dt:
                chunk_start = start_dt
                chunk_end = min(start_dt + timedelta(days=chunk_days), end_dt)

                api_url = base_api.format(
                    chunk_start.strftime("%d-%m-%Y"),
                    chunk_end.strftime("%d-%m-%Y"),
                    symbol
                )

                response = self.session.get(
                    api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=15
                )
                if response.status_code == 200:
                    data = response.json()
                    if "data" in data and isinstance(data["data"], list):
                        all_data.extend(data["data"])

                start_dt = chunk_end + timedelta(days=1)

            if not all_data:
                print(f"⚠️ No data returned for {symbol} between {from_date} and {to_date}.")
                return None

            # --- Combine all chunks into DataFrame ---
            df = pd.DataFrame(all_data)

            # --- Keep expected columns ---
            expected_cols = [
                "CH_SYMBOL", "CH_SERIES", "mTIMESTAMP", "CH_PREVIOUS_CLS_PRICE",
                "CH_OPENING_PRICE", "CH_TRADE_HIGH_PRICE", "CH_TRADE_LOW_PRICE",
                "CH_LAST_TRADED_PRICE", "CH_CLOSING_PRICE", "VWAP", "CH_TOT_TRADED_QTY",
                "CH_TOT_TRADED_VAL", "CH_TOTAL_TRADES", "COP_DELIV_QTY", "COP_DELIV_PERC"
            ]
            df = df[[c for c in expected_cols if c in df.columns]]

            rename_map = {
                "CH_SYMBOL": "Symbol",
                "CH_SERIES": "Series",
                "mTIMESTAMP": "Date",
                "CH_PREVIOUS_CLS_PRICE": "Prev Close",
                "CH_OPENING_PRICE": "Open Price",
                "CH_TRADE_HIGH_PRICE": "High Price",
                "CH_TRADE_LOW_PRICE": "Low Price",
                "CH_LAST_TRADED_PRICE": "Last Price",
                "CH_CLOSING_PRICE": "Close Price",
                "VWAP": "VWAP",
                "CH_TOT_TRADED_QTY": "Total Traded Quantity",
                "CH_TOT_TRADED_VAL": "Turnover ₹",
                "CH_TOTAL_TRADES": "No. of Trades",
                "COP_DELIV_QTY": "Deliverable Qty",
                "COP_DELIV_PERC": "% Dly Qt to Traded Qty"
            }
            df.rename(columns=rename_map, inplace=True)

            # --- Clean numeric data ---
            df.replace({float("inf"): 0, float("-inf"): 0}, inplace=True)
            df.fillna(0, inplace=True)

            # --- Sort by date & remove duplicates ---
            df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce")
            df.sort_values("Date", inplace=True)
            df.drop_duplicates(subset=["Date"], keep="last", inplace=True)

            # --- Convert datetime columns to string for JSON/Sheets safety ---
            for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
                df[col] = df[col].dt.strftime("%d-%b-%Y")

            return df

        except Exception as e:
            print(f"❌ Error fetching historical data for {symbol}: {e}")
            return None

    def cm_hist_bulk_deals(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        """
        Fetch Historical NSE Bulk Deals (CSV Mode)
        Auto-detects date range, symbol, and period.

        Usage Examples:
        ----------------
        get.cm_hist_bulk_deals()                              # Today's data for all symbols
        get.cm_hist_bulk_deals("1W")                          # 1 week for all symbols
        get.cm_hist_bulk_deals("01-10-2025")                  # From date, Auto Today date for all symbols
        get.cm_hist_bulk_deals("01-10-2025", "17-10-2025")    # Custom range for all symbols
        get.cm_hist_bulk_deals("RELIANCE")                    # Today for symbol
        get.cm_hist_bulk_deals("RELIANCE", "1M")              # 1 month for symbol
        get.cm_hist_bulk_deals("RELIANCE", "01-10-2025")      # From date, Auto Today date for symbols
        get.cm_hist_bulk_deals("RELIANCE", "01-10-2025", "17-10-2025")  # Range for symbol
        """

        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        today = datetime.now()
        today_str = today.strftime("%d-%m-%Y")

        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                elif arg.upper() in ["1D", "1W", "1M", "3M", "6M", "1Y"]:
                    period = arg.upper()
                else:
                    symbol = arg.upper()

        # --- Compute date range from period --- #
        if period:
            delta_map = {
                "1D": timedelta(days=1),
                "1W": timedelta(weeks=1),
                "1M": timedelta(days=30),
                "3M": timedelta(days=90),
                "6M": timedelta(days=180),
                "1Y": timedelta(days=365),
            }
            delta = delta_map.get(period, timedelta(days=365))
            from_date = (today - delta).strftime("%d-%m-%Y")
            to_date = to_date or today_str

        from_date = from_date or today_str
        to_date = to_date or today_str

        # --- Rotate User-Agent --- #
        self.rotate_user_agent()

        # --- URLs --- #
        ref_url = "https://www.nseindia.com/report-detail/display-bulk-and-block-deals"
        base_url = "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals"

        if symbol:
            api_url = f"{base_url}?optionType=bulk_deals&symbol={symbol}&from={from_date}&to={to_date}&csv=true"
        else:
            api_url = f"{base_url}?optionType=bulk_deals&from={from_date}&to={to_date}&csv=true"

        # --- Fetch data --- #
        try:
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Retry with exponential backoff
            for attempt in range(3):
                try:
                    response = self.session.get(
                        api_url,
                        headers=self.headers,
                        cookies=ref_response.cookies.get_dict(),
                        timeout=10,
                    )
                    response.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt < 2:
                        wait_time = 2 ** attempt
                        print(f"⚠️ Attempt {attempt + 1} failed ({e}); retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise

            # --- Detect non-CSV (HTML error page) --- #
            if "text/html" in response.headers.get("Content-Type", ""):
                print("⚠️ NSE returned HTML instead of CSV. Headers or session may be invalid.")
                return None

            # --- Read CSV with UTF-8 BOM handling at byte level --- #
            # Decode response content and strip BOM at byte level
            content = response.content
            if content.startswith(b'\xef\xbb\xbf'):  # Check for UTF-8 BOM
                content = content[3:]  # Remove BOM
            cleaned_text = content.decode('utf-8')
            df = pd.read_csv(StringIO(cleaned_text))

            # --- Handle empty or invalid response --- #
            if df.empty or len(df.columns) < 2:
                print(f"ℹ️ No bulk deal data found for {symbol or 'ALL'} between {from_date} and {to_date}")
                return None

            # --- 🔹 FIX 1: Clean Headers (Quotes + Spaces) --- #
            df.columns = [c.strip().replace('"', '') for c in df.columns]
            df.rename(columns=lambda x: x.strip(), inplace=True)  # Ensure no leading/trailing spaces

            # --- 🔹 FIX 2: Clean numeric columns safely --- #
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
                    try:
                        df[col] = pd.to_numeric(df[col])
                    except Exception:
                        pass

            # --- Sort and format dates --- #
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce", format="%d-%b-%Y")
                df = df.sort_values("Date", ascending=False).reset_index(drop=True)
                df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")

            # --- Final cleanup --- #
            df = df.fillna("").replace({float("inf"): "", float("-inf"): ""})
            return df

        except Exception as e:
            print(f"❌ Error fetching bulk deals: {e}")
            return None
        
    def cm_hist_block_deals(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        """
        Fetch Historical NSE block Deals (CSV Mode)
        Auto-detects date range, symbol, and period.

        Usage Examples:
        ----------------
        get.cm_hist_block_deals()                              # Today's data for all symbols
        get.cm_hist_block_deals("1W")                          # 1 week for all symbols
        get.cm_hist_block_deals("01-10-2025")                  # From date, Auto Today date for all symbols
        get.cm_hist_block_deals("01-10-2025", "17-10-2025")    # Custom range for all symbols
        get.cm_hist_block_deals("RELIANCE")                    # Today for symbol
        get.cm_hist_block_deals("RELIANCE", "1M")              # 1 month for symbol
        get.cm_hist_block_deals("RELIANCE", "01-10-2025")      # From date, Auto Today date for symbols
        get.cm_hist_block_deals("RELIANCE", "01-10-2025", "17-10-2025")  # Range for symbol
        """

        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        today = datetime.now()
        today_str = today.strftime("%d-%m-%Y")

        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                elif arg.upper() in ["1D", "1W", "1M", "3M", "6M", "1Y"]:
                    period = arg.upper()
                else:
                    symbol = arg.upper()

        # --- Compute date range from period --- #
        if period:
            delta_map = {
                "1D": timedelta(days=1),
                "1W": timedelta(weeks=1),
                "1M": timedelta(days=30),
                "3M": timedelta(days=90),
                "6M": timedelta(days=180),
                "1Y": timedelta(days=365),
            }
            delta = delta_map.get(period, timedelta(days=365))
            from_date = (today - delta).strftime("%d-%m-%Y")
            to_date = to_date or today_str

        from_date = from_date or today_str
        to_date = to_date or today_str

        # --- Rotate User-Agent --- #
        self.rotate_user_agent()

        # --- URLs --- #
        ref_url = "https://www.nseindia.com/report-detail/display-bulk-and-block-deals"
        base_url = "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals"

        if symbol:
            api_url = f"{base_url}?optionType=block_deals&symbol={symbol}&from={from_date}&to={to_date}&csv=true"
        else:
            api_url = f"{base_url}?optionType=block_deals&from={from_date}&to={to_date}&csv=true"

        # --- Fetch data --- #
        try:
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Retry with exponential backoff
            for attempt in range(3):
                try:
                    response = self.session.get(
                        api_url,
                        headers=self.headers,
                        cookies=ref_response.cookies.get_dict(),
                        timeout=10,
                    )
                    response.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt < 2:
                        wait_time = 2 ** attempt
                        print(f"⚠️ Attempt {attempt + 1} failed ({e}); retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise

            # --- Detect non-CSV (HTML error page) --- #
            if "text/html" in response.headers.get("Content-Type", ""):
                print("⚠️ NSE returned HTML instead of CSV. Headers or session may be invalid.")
                return None

            # --- Read CSV with UTF-8 BOM handling at byte level --- #
            # Decode response content and strip BOM at byte level
            content = response.content
            if content.startswith(b'\xef\xbb\xbf'):  # Check for UTF-8 BOM
                content = content[3:]  # Remove BOM
            cleaned_text = content.decode('utf-8')
            df = pd.read_csv(StringIO(cleaned_text))

            # --- Handle empty or invalid response --- #
            if df.empty or len(df.columns) < 2:
                print(f"ℹ️ No block deal data found for {symbol or 'ALL'} between {from_date} and {to_date}")
                return None

            # --- 🔹 FIX 1: Clean Headers (Quotes + Spaces) --- #
            df.columns = [c.strip().replace('"', '') for c in df.columns]
            df.rename(columns=lambda x: x.strip(), inplace=True)  # Ensure no leading/trailing spaces

            # --- 🔹 FIX 2: Clean numeric columns safely --- #
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
                    try:
                        df[col] = pd.to_numeric(df[col])
                    except Exception:
                        pass

            # --- Sort and format dates --- #
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce", format="%d-%b-%Y")
                df = df.sort_values("Date", ascending=False).reset_index(drop=True)
                df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")

            # --- Final cleanup --- #
            df = df.fillna("").replace({float("inf"): "", float("-inf"): ""})
            return df

        except Exception as e:
            print(f"❌ Error fetching block deals: {e}")
            return None    

    def cm_hist_short_selling(self, *args, from_date=None, to_date=None, period=None, symbol=None):
        """
        Fetch Historical NSE Short Selling (CSV Mode)
        Auto-detects date range, symbol, and period.

        Usage Examples:
        ----------------
        get.cm_hist_short_selling()                              # Today's data for all symbols
        get.cm_hist_short_selling("1W")                          # 1 week for all symbols
        get.cm_hist_short_selling("01-10-2025")                  # From date, Auto Today date for all symbols
        get.cm_hist_short_selling("01-10-2025", "17-10-2025")    # Custom range for all symbols
        get.cm_hist_short_selling("RELIANCE")                    # Today for symbol
        get.cm_hist_short_selling("RELIANCE", "1M")              # 1 month for symbol
        get.cm_hist_short_selling("RELIANCE", "01-10-2025")      # From date, Auto Today date for symbols
        get.cm_hist_short_selling("RELIANCE", "01-10-2025", "17-10-2025")  # Range for symbol
        """

        date_pattern = re.compile(r"^\d{2}-\d{2}-\d{4}$")
        today = datetime.now()
        today_str = today.strftime("%d-%m-%Y")

        for arg in args:
            if isinstance(arg, str):
                if date_pattern.match(arg):
                    if not from_date:
                        from_date = arg
                    elif not to_date:
                        to_date = arg
                elif arg.upper() in ["1D", "1W", "1M", "3M", "6M", "1Y"]:
                    period = arg.upper()
                else:
                    symbol = arg.upper()

        # --- Compute date range from period --- #
        if period:
            delta_map = {
                "1D": timedelta(days=1),
                "1W": timedelta(weeks=1),
                "1M": timedelta(days=30),
                "3M": timedelta(days=90),
                "6M": timedelta(days=180),
                "1Y": timedelta(days=365),
            }
            delta = delta_map.get(period, timedelta(days=365))
            from_date = (today - delta).strftime("%d-%m-%Y")
            to_date = to_date or today_str

        from_date = from_date or today_str
        to_date = to_date or today_str

        # --- Rotate User-Agent --- #
        self.rotate_user_agent()

        # --- URLs --- #
        ref_url = "https://www.nseindia.com/report-detail/display-bulk-and-block-deals"
        base_url = "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals"

        if symbol:
            api_url = f"{base_url}?optionType=short_selling&symbol={symbol}&from={from_date}&to={to_date}&csv=true"
        else:
            api_url = f"{base_url}?optionType=short_selling&from={from_date}&to={to_date}&csv=true"

        # --- Fetch data --- #
        try:
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Retry with exponential backoff
            for attempt in range(3):
                try:
                    response = self.session.get(
                        api_url,
                        headers=self.headers,
                        cookies=ref_response.cookies.get_dict(),
                        timeout=10,
                    )
                    response.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt < 2:
                        wait_time = 2 ** attempt
                        print(f"⚠️ Attempt {attempt + 1} failed ({e}); retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise

            # --- Detect non-CSV (HTML error page) --- #
            if "text/html" in response.headers.get("Content-Type", ""):
                print("⚠️ NSE returned HTML instead of CSV. Headers or session may be invalid.")
                return None

            # --- Read CSV with UTF-8 BOM handling at byte level --- #
            # Decode response content and strip BOM at byte level
            content = response.content
            if content.startswith(b'\xef\xbb\xbf'):  # Check for UTF-8 BOM
                content = content[3:]  # Remove BOM
            cleaned_text = content.decode('utf-8')
            df = pd.read_csv(StringIO(cleaned_text))

            # --- Handle empty or invalid response --- #
            if df.empty or len(df.columns) < 2:
                print(f"ℹ️ No Short Selling data found for {symbol or 'ALL'} between {from_date} and {to_date}")
                return None

            # --- 🔹 FIX 1: Clean Headers (Quotes + Spaces) --- #
            df.columns = [c.strip().replace('"', '') for c in df.columns]
            df.rename(columns=lambda x: x.strip(), inplace=True)  # Ensure no leading/trailing spaces

            # --- 🔹 FIX 2: Clean numeric columns safely --- #
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
                    try:
                        df[col] = pd.to_numeric(df[col])
                    except Exception:
                        pass

            # --- Sort and format dates --- #
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce", format="%d-%b-%Y")
                df = df.sort_values("Date", ascending=False).reset_index(drop=True)
                df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")

            # --- Final cleanup --- #
            df = df.fillna("").replace({float("inf"): "", float("-inf"): ""})
            return df

        except Exception as e:
            print(f"❌ Error fetching Short Selling: {e}")
            return None
 

    def cm_dmy_biz_growth(self, *args, mode="monthly", month=None, year=None):
        """
        Fetch NSE historical business growth / capital market data:
        yearly, monthly, or daily, cleaned, renamed, and JSON-serializable.

        Returns
        -------
        List of dicts (JSON-ready), missing/null values replaced with "-"
        """

        now = datetime.now()

        # --- Month mapping ---
        month_map = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
        }
        reverse_month_map = {v: k for k, v in month_map.items()}

        # --- Auto-detect arguments ---
        for arg in args:
            if isinstance(arg, str):
                arg_upper = arg.strip().upper()
                if arg_upper in ["YEARLY", "MONTHLY", "DAILY"]:
                    mode = arg_upper.lower()
                elif arg_upper.isdigit() and len(arg_upper) == 4:
                    year = int(arg_upper)
                elif arg_upper[:3] in month_map:
                    month = month_map[arg_upper[:3]]
            elif isinstance(arg, int):
                if 1900 <= arg <= 2100:
                    year = arg
                elif 1 <= arg <= 12:
                    month = arg

        # --- Defaults ---
        if year is None:
            year = now.year
        if month is None:
            month = now.month

        # --- Prepare URLs ---
        self.rotate_user_agent()
        ref_url = "https://www.nseindia.com"

        if mode.lower() == "yearly":
            api_url = "https://www.nseindia.com/api/historicalOR/cm/tbg/yearly"
        elif mode.lower() == "monthly":
            from_year = year
            to_year = year + 1
            api_url = f"https://www.nseindia.com/api/historicalOR/cm/tbg/monthly?from={from_year}&to={to_year}"
        elif mode.lower() == "daily":
            # Convert month to 3-letter title case (Jan, Feb, Mar...)
            month_str = reverse_month_map.get(month, str(month)).title()  
            year_str = f"{year % 100:02d}"  # 2025 -> "25"
            api_url = f"https://www.nseindia.com/api/historicalOR/cm/tbg/daily?month={month_str}&year={year_str}"
        else:
            raise ValueError("Invalid mode. Use 'yearly', 'monthly', or 'daily'")

        try:
            # --- Get cookies ---
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # --- Fetch API data ---
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()
            raw_data = response.json()

            # --- Extract 'data' depending on mode ---
            data_list = []

            if mode.lower() in ["yearly", "monthly"]:
                data_list = [d["data"] for d in raw_data.get("data", []) if "data" in d]
            elif mode.lower() == "daily":
                raw_daily = raw_data.get("data", [])
                for item in raw_daily:
                    if "data" in item:
                        data_list.append(item["data"])
                    else:
                        data_list.append(item)

            if not data_list:
                print("⚠️ No valid data found.")
                return None

            df = pd.DataFrame(data_list)

            # --- Clean numeric columns ---
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).str.replace(",", "").str.strip()
                    df[col] = df[col].replace({"None": "-", "nan": "-", "NaN": "-", "": "-", pd.NA: "-"})

                    # Convert numeric strings to int/float where possible
                    def to_numeric_or_keep(x):
                        try:
                            return pd.to_numeric(x)
                        except:
                            return x
                    df[col] = df[col].apply(to_numeric_or_keep)

            # --- Rename columns ---
            if mode.lower() == "yearly":
                df.rename(columns={
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
                    "GTY_MKT_CAP": "Market_Cap"
                }, inplace=True)
            elif mode.lower() == "monthly":
                df.rename(columns={
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
                    "GTM_MKT_CAP": "Market_Cap"
                }, inplace=True)
            elif mode.lower() == "daily":
                df.rename(columns={
                    "F_TIMESTAMP": "Date",
                    "CDT_NOS_OF_SECURITY_TRADES": "No_of_Security_Trades",
                    "CDT_NOS_OF_TRADES": "No_of_Trades",
                    "CDT_TRADES_QTY": "Traded_Qty",
                    "CDT_TRADES_VALUES": "Turnover"
                }, inplace=True)

            # --- Make JSON-serializable ---
            df_json_ready = df.astype(object)  # convert int64/float64 to native Python types
            return df_json_ready.to_dict(orient='records')

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching capital market data: {e}")
            return None


    def cm_monthly_settlement_report(self, *args, from_year=None, to_year=None, period=None):
        """
        Fetch NSE Monthly Settlement Statistics (Cash Market) for multiple financial years (Apr–Mar),
        including past financial years and current FY up to the latest available month.

        Parameters
        ----------
        *args : str
            Can contain specific years or period like "1Y", "2Y", "3Y", "5Y".
        from_year : int, optional
            Start financial year (YYYY).
        to_year : int, optional
            End financial year (YYYY).
        period : str, optional
            Number of past financial years, e.g., "2Y".

        Returns
        -------
        pd.DataFrame
            Monthly settlement statistics across requested years.
        """

        # --- Auto-detect arguments ---
        for arg in args:
            if isinstance(arg, str):
                if re.match(r"^\d{4}$", arg):  # year
                    if not from_year:
                        from_year = int(arg)
                    elif not to_year:
                        to_year = int(arg)
                elif arg.upper() in ["1Y", "2Y", "3Y", "5Y"]:
                    period = arg.upper()

        # --- Determine financial years ---
        today = datetime.now()
        current_month = today.month

        # Current FY start year
        if current_month >= 4:  # Apr–Dec
            current_fy_start = today.year
        else:  # Jan–Mar
            current_fy_start = today.year - 1

        if period and not from_year:
            years_back = int(period.replace("Y", ""))
            from_year = current_fy_start - years_back
            to_year = current_fy_start + 1  # include current FY
        elif not from_year:
            from_year = current_fy_start
            to_year = current_fy_start + 1
        elif not to_year:
            to_year = from_year + 1

        # --- Rotate user-agent & base URLs ---
        self.rotate_user_agent()
        ref_url = "https://www.nseindia.com/report-detail/monthly-settlement-statistics"
        base_api = "https://www.nseindia.com/api/historicalOR/monthly-sett-stats-data?finYear={}-{}"

        try:
            # --- Get NSE cookies ---
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()
            cookies = ref_response.cookies.get_dict()

            all_data = []
            for fy_start in range(from_year, to_year):
                fy_end = fy_start + 1
                fin_year = f"{fy_start}-{fy_end}"
                api_url = base_api.format(fy_start, fy_end)

                # --- Fetch with retry ---
                for attempt in range(3):
                    try:
                        response = self.session.get(api_url, headers=self.headers, cookies=cookies, timeout=15)
                        if response.status_code == 200:
                            js = response.json()
                            if "data" in js and isinstance(js["data"], list):
                                for rec in js["data"]:
                                    rec["FinancialYear"] = fin_year
                                all_data.extend(js["data"])
                                print(f"✅ {fin_year} data fetched ({len(js['data'])} records).")
                            else:
                                print(f"⚠️ No data in API for {fin_year}.")
                            break
                        else:
                            print(f"⚠️ Failed for {fin_year} | HTTP {response.status_code}")
                    except Exception as e:
                        print(f"⚠️ Attempt {attempt+1} failed for {fin_year}: {e}")
                        time.sleep(1)
                time.sleep(0.8)  # polite delay

            if not all_data:
                print(f"⚠️ No settlement data found from FY {from_year} to current.")
                return None

            # --- Create DataFrame ---
            df = pd.DataFrame(all_data)

            # --- Rename columns ---
            rename_map = {
                "ST_DATE": "Month",
                "ST_SETTLEMENT_NO": "Settlement No",
                "ST_NO_OF_TRADES_LACS": "No of Trades (lakhs)",
                "ST_TRADED_QTY_LACS": "Traded Qty (lakhs)",
                "ST_DELIVERED_QTY_LACS": "Delivered Qty (lakhs)",
                "ST_PERC_DLVRD_TO_TRADED_QTY": "% Delivered to Traded Qty",
                "ST_TURNOVER_CRORES": "Turnover (₹ Cr)",
                "ST_DELIVERED_VALUE_CRORES": "Delivered Value (₹ Cr)",
                "ST_PERC_DLVRD_VAL_TO_TURNOVER": "% Delivered Value to Turnover",
                "ST_SHORT_DLVRY_AUC_QTY_LACS": "Short Delivery Qty (Lacs)",
                "ST_PERC_SHORT_DLVRY_TO_DLVRY": "% Short Delivery to Delivery",
                "ST_SHORT_DLVRY_VALUE": "Short Delivery Value (₹ Cr)",
                "ST_PERC_SHORT_DLVRY_VAL_DLVRY": "% Short Delivery Value to Delivery",
                "ST_FUNDS_PAYIN_CRORES": "Funds Payin (₹ Cr)"
            }
            df.rename(columns=rename_map, inplace=True)

            # --- Parse Month ---
            df["Month"] = pd.to_datetime(df["Month"], format="%b-%Y", errors="coerce")
            df["Month"] = df["Month"].dt.strftime('%b-%Y')

            # # --- Clean numeric columns ---
            # numeric_cols = [c for c in df.columns if c not in ["Month", "Settlement No", "FinancialYear"]]
            # for c in numeric_cols:
            #     df[c] = df[c].astype(str).str.replace(",", "").str.strip()
            #     df[c] = pd.to_numeric(df[c], errors="coerce")

            # # --- Sort and reset index ---
            # df.sort_values(["FinancialYear", "Month"], inplace=True)
            # df.reset_index(drop=True, inplace=True)

            return df

        except Exception as e:
            print(f"❌ Error fetching Monthly Settlement data: {e}")
            return None

    def cm_monthly_most_active_equity(self):
        self.rotate_user_agent()  # Rotating User-Agent

        ref_url = 'https://www.nseindia.com/historical/most-active-securities'
        api_url = 'https://www.nseindia.com/api/historicalOR/most-active-securities-monthly'

        try:
            # Step 1: Get reference cookies
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # Step 2: Fetch API data with cookies
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()  # Ensure request was successful

            # Step 3: Parse JSON
            data = response.json()

            if 'data' in data and isinstance(data['data'], list):
                df = pd.DataFrame(data['data'])

                # Step 4: Select and order columns
                columns = ['ASM_SECURITY', 'ASM_NO_OF_TRADES', 'ASM_TRADED_QUANTITY', 'ASM_TURNOVER', 'ASM_AVG_DLY_TURNOVER', 'ASM_SHARE_IN_TOTAL_TURNOVER', 'ASM_DATE']
                df = df[columns]

                # Step 5: Rename columns
                rename_map = {
                    'ASM_SECURITY': 'Security',
                    'ASM_NO_OF_TRADES': 'No. of Trades',
                    'ASM_TRADED_QUANTITY': 'Traded Quantity (Lakh Shares)',
                    'ASM_TURNOVER': 'Turnover (₹ Cr.)',
                    'ASM_AVG_DLY_TURNOVER': 'Avg Daily Turnover (₹ Cr.)',
                    'ASM_SHARE_IN_TOTAL_TURNOVER': 'Share in Total Turnover (%)',
                    'ASM_DATE': 'Month'
                }
                df.rename(columns=rename_map, inplace=True)

                # Step 6: Clean NaN and infinite values
                df = df.fillna(0).replace({float('inf'): 0, float('-inf'): 0})

                # Step 7: Return DataFrame
                return df if not df.empty else None

            return None

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching most active securities (monthly): {e}")
            return None

    def historical_advances_decline(self, *args, mode="Month_wise", month=None, year=None):
        """
        Fetch NSE historical advances-declines data either Month-wise or Day-wise.

        Parameters
        ----------
        *args : optional positional arguments to auto-detect mode/month/year
            Examples:
            - historical_advances_decline() → current month/year (Month_wise)
            - historical_advances_decline("2023") → 2023 Month_wise
            - historical_advances_decline("SEP", "2025") → Sep 2025 Day_wise
            - historical_advances_decline("Day_wise", "SEP", "2025") → explicit mode
        mode : str
            "Month_wise" or "Day_wise" (auto-detected if passed in *args)
        year : int or str, optional
            Year to fetch. Defaults to current year.
        month : str or int, optional
            Month to fetch (used for Day_wise). Defaults to current month.
        """

        now = datetime.now()

        # --- Month lookup maps ---
        month_map = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
        }
        reverse_month_map = {v: k for k, v in month_map.items()}

        # --- Auto-detect arguments ---
        for arg in args:
            if isinstance(arg, str):
                arg_upper = arg.strip().upper()

                # Detect mode keyword
                if arg_upper in ["MONTH_WISE", "DAY_WISE"]:
                    mode = arg_upper

                # Detect year (numeric string)
                elif arg_upper.isdigit() and len(arg_upper) == 4:
                    year = int(arg_upper)

                # Detect month name
                elif arg_upper[:3] in month_map:
                    month = month_map[arg_upper[:3]]

                # Detect MM-YYYY or MON-YYYY
                elif "-" in arg_upper:
                    parts = arg_upper.split("-")
                    if len(parts) == 2:
                        m_part, y_part = parts
                        if m_part[:3] in month_map:
                            month = month_map[m_part[:3]]
                        elif m_part.isdigit():
                            month = int(m_part)
                        if y_part.isdigit():
                            year = int(y_part)

            elif isinstance(arg, int):
                if 1900 <= arg <= 2100:
                    year = arg
                elif 1 <= arg <= 12:
                    month = arg

        # --- Defaults (Previous Month Logic) ---
        if year is None:
            year = now.year
        if month is None:
            # Move to previous month
            prev_month = now.month - 1 or 12
            if now.month == 1:
                year -= 1  # roll back to previous year
            month = prev_month

        self.rotate_user_agent()
        ref_url = "https://www.nseindia.com"

        # --- Determine API URL ---
        if mode.lower() == "month_wise":
            api_url = f"https://www.nseindia.com/api/historicalOR/advances-decline-monthly?year={year}"
        elif mode.lower() == "day_wise":
            # NSE expects month as 3-letter code (e.g. SEP)
            month_code = reverse_month_map.get(int(month), now.strftime("%b").upper())
            api_url = f"https://www.nseindia.com/api/historicalOR/advances-decline-monthly?year={month_code}-{year}"
        else:
            raise ValueError("Invalid mode. Use 'Month_wise' or 'Day_wise'.")

        try:
            # --- Get cookies ---
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # --- API call ---
            response = self.session.get(api_url, headers=self.headers, cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()

            data = response.json()
            if "data" not in data or not isinstance(data["data"], list):
                print("⚠️ No valid data received from NSE API.")
                return None

            df = pd.DataFrame(data["data"])

            # --- Field mapping ---
            if mode.lower() == "month_wise":
                column_map = {
                    "ADM_MONTH": "Month",
                    "ADM_ADVANCES": "Advances",
                    "ADM_DECLINES": "Declines",
                    "ADM_ADV_DCLN_RATIO": "Adv_Decline_Ratio",
                }
                cols = ["Month", "Advances", "Declines", "Adv_Decline_Ratio"]
            else:
                column_map = {
                    "ADD_DAY_STRING": "Day",
                    "ADD_ADVANCES": "Advances",
                    "ADD_DECLINES": "Declines",
                    "ADD_ADV_DCLN_RATIO": "Adv_Decline_Ratio",
                }
                cols = ["Day", "Advances", "Declines", "Adv_Decline_Ratio"]

            df = df.rename(columns=column_map)
            df = df[[c for c in cols if c in df.columns]]
            df = df.fillna(0).replace({float("inf"): 0, float("-inf"): 0})

            return df if not df.empty else None

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching advances-decline data: {e}")
            return None

    #---------------------------------------------------------- FnO_Eod_Data ----------------------------------------------------------------

    def fno_eod_bhav_copy(self, trade_date: str = ""):
        self.rotate_user_agent()
        bhav_df = pd.DataFrame()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        url = 'https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_'
        payload = f"{str(trade_date.strftime('%Y%m%d'))}_F_0000.csv.zip"

        try:
            request_bhav = self.session.get(url + payload, headers=self.headers, timeout=10)
            if request_bhav.status_code == 200:
                bhav_df = self._extract_csv_from_zip(request_bhav.content)
            else:
                url2 = "https://www.nseindia.com/api/reports?archives=" \
                       "%5B%7B%22name%22%3A%22F%26O%20-%20Bhavcopy(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22" \
                       f"%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D&date={str(trade_date.strftime('%d-%b-%Y'))}" \
                       "&type=equity&mode=single"
                ref = requests.get('https://www.nseindia.com/reports-archives', headers=self.headers)
                request_bhav = self.session.get(url2, headers=self.headers, cookies=ref.cookies.get_dict(), timeout=10)
                request_bhav.raise_for_status()
                bhav_df = self._extract_csv_from_zip(request_bhav.content)

            if not bhav_df.empty:
                # Filtering rows where all four columns (22, 23, 24, 25) are zero
                try:
                    bhav_df = bhav_df[~((bhav_df.iloc[:, 22] == 0) & 
                                         (bhav_df.iloc[:, 23] == 0) & 
                                         (bhav_df.iloc[:, 24] == 0) & 
                                         (bhav_df.iloc[:, 25] == 0))]
                    bhav_df = bhav_df.sort_values(by=bhav_df.columns[24], ascending=False)
                except IndexError:
                    print("Warning: The specified columns do not exist in the CSV.")

            return bhav_df

        except (requests.RequestException, FileNotFoundError):
            return None
        
    # Helper function 
    def _extract_csv_from_zip(self, zip_content):
        with zipfile.ZipFile(BytesIO(zip_content), 'r') as zip_bhav:
            for file_name in zip_bhav.namelist():
                if file_name.endswith('.csv'):
                    return pd.read_csv(zip_bhav.open(file_name))
        return pd.DataFrame()

    def fno_eod_fii_stats(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        formatted_date = trade_date.strftime("%d-%b-%Y")
        formatted_date = formatted_date[:3] + formatted_date[3:].capitalize()
        url = f"https://nsearchives.nseindia.com/content/fo/fii_stats_{formatted_date}.xls"
        
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()

            file_content = BytesIO(nse_resp.content)
            file_format = self.detect_excel_format(file_content)

            print(f"Detected File Format: {file_format}")

            if file_format == 'xls':
                bhav_df = pd.read_excel(file_content, engine='xlrd', dtype=str)
            elif file_format == 'xlsx':
                bhav_df = pd.read_excel(file_content, engine='openpyxl', dtype=str)
            elif file_format == 'xlsb':
                bhav_df = pd.read_excel(file_content, engine='pyxlsb', dtype=str)
            else:
                print("Unknown file format or corrupted file.")
                return None

            return bhav_df
        
        except requests.RequestException as e:
            print(f"Request Error: {e}")
        except ValueError as e:
            print(f"Value Error: {e}")
        except Exception as e:
            print(f"Unexpected Error: {e}")
        
        return None

    def fno_eod_top10_fut(self, trade_date: str):
        self.rotate_user_agent()
        raw_trade_date = trade_date  # keep original string

        try:
            # Convert date (expects dd-mm-YYYY)
            trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
            url = f"https://nsearchives.nseindia.com/archives/fo/mkt/fo{trade_date.strftime('%d%m%Y').upper()}.zip"

            # Fetch ZIP
            request_bhav = requests.get(url, headers=self.headers, timeout=10)
            request_bhav.raise_for_status()
            zip_bhav = zipfile.ZipFile(BytesIO(request_bhav.content), 'r')

            rows = []
            for file_name in zip_bhav.namelist():
                if file_name.lower().startswith("ttfut") and file_name.endswith(".csv"):
                    # Decode safely
                    csv_text = zip_bhav.open(file_name).read().decode("utf-8", errors="ignore")
                    rows = list(csv.reader(csv_text.splitlines()))
                    break

            return rows if rows else None

        except Exception as e:
            print(f"❌ Error fetching Top 10 Futures for {raw_trade_date}: {e}")
            return None
        
    def fno_eod_top20_opt(self, trade_date: str):
        self.rotate_user_agent()
        raw_trade_date = trade_date  # keep original string

        try:
            # Convert date (expects dd-mm-YYYY)
            trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
            url = f"https://nsearchives.nseindia.com/archives/fo/mkt/fo{trade_date.strftime('%d%m%Y').upper()}.zip"

            # Fetch ZIP
            request_bhav = requests.get(url, headers=self.headers, timeout=10)
            request_bhav.raise_for_status()
            zip_bhav = zipfile.ZipFile(BytesIO(request_bhav.content), 'r')

            rows = []
            for file_name in zip_bhav.namelist():
                if file_name.lower().startswith("ttopt") and file_name.endswith(".csv"):
                    # Decode safely
                    csv_text = zip_bhav.open(file_name).read().decode("utf-8", errors="ignore")
                    rows = list(csv.reader(csv_text.splitlines()))
                    break

            return rows if rows else None

        except Exception as e:
            print(f"❌ Error fetching Top 20 Option for {raw_trade_date}: {e}")
            return None

    def fno_eod_sec_ban(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        url = f"https://nsearchives.nseindia.com/archives/fo/sec_ban/fo_secban_{str(trade_date.strftime('%d%m%Y').upper())}.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None

    def fno_eod_mwpl_3(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        url = f"https://nsearchives.nseindia.com/content/nsccl/mwpl_cli_{trade_date.strftime('%d%m%Y').upper()}.xls"
        
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()

            file_content = BytesIO(nse_resp.content)
            file_format = self.detect_excel_format(file_content)

            print(f"Detected File Format: {file_format}")

            if file_format == 'xls':
                bhav_df = pd.read_excel(file_content, engine='xlrd', dtype=str)
            elif file_format == 'xlsx':
                bhav_df = pd.read_excel(file_content, engine='openpyxl', dtype=str)
            elif file_format == 'xlsb':
                bhav_df = pd.read_excel(file_content, engine='pyxlsb', dtype=str)
            else:
                print("Unknown file format or corrupted file.")
                return None

            # Cleaning the DataFrame
            bhav_df = self.clean_mwpl_data(bhav_df)
            return bhav_df
        
        except requests.RequestException as e:
            print(f"Request Error: {e}")
        except ValueError as e:
            print(f"Value Error: {e}")
        except Exception as e:
            print(f"Unexpected Error: {e}")
        
        return None
    
    # Helper function
    def detect_excel_format(self, file_content):
        """
        Detects the format of the Excel file using the first few bytes.
        Returns: 'xls', 'xlsx', 'xlsb', or 'unknown'.
        """
        signature = file_content.read(8)
        file_content.seek(0)  # Reset buffer pointer for reading

        print(f"File Signature: {signature}")

        if signature.startswith(b'\xD0\xCF\x11\xE0'):  # XLS (OLE2 Compound File)
            return 'xls'
        elif signature.startswith(b'\x50\x4B\x03\x04'):  # XLSX (ZIP)
            return 'xlsx'
        elif signature.startswith(b'\x09\x08\x10\x00'):  # XLSB (Binary Excel)
            return 'xlsb'
        else:
            return 'unknown'

    # Helper function
    def clean_mwpl_data(self, df):
        """
        Cleans the MWPL DataFrame by avoiding 'Unnamed' columns and filling them automatically.
        """
        # Dropping any rows without valid data (optional, for extra cleaning)
        df.dropna(how='all', inplace=True)

        # Resetting the header (first row becomes the actual header)
        df.columns = df.iloc[0]  # Set the first row as header
        df = df[1:].reset_index(drop=True)  # Remove the header row

        # Automatically filling 'Unnamed' columns with "Client X" format
        new_columns = []
        client_counter = 1

        for col in df.columns:
            if "Unnamed" in str(col) or pd.isna(col):
                new_columns.append(f"Client {client_counter}")
                client_counter += 1
            else:
                new_columns.append(str(col).strip())

        df.columns = new_columns
        return df

    def fno_eod_combine_oi(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        url = f"https://nsearchives.nseindia.com/archives/nsccl/mwpl/combineoi_{trade_date.strftime('%d%m%Y').upper()}.zip"

        try:
            request_bhav = requests.get(url, headers=self.headers, timeout=10)
            request_bhav.raise_for_status()
            zip_bhav = zipfile.ZipFile(BytesIO(request_bhav.content), 'r')
            bhav_df = pd.DataFrame()

            for file_name in zip_bhav.namelist():
                if file_name.lower().startswith("combineoi") and file_name.endswith(".csv"):
                    bhav_df = pd.read_csv(zip_bhav.open(file_name))
                    break

            if bhav_df.empty:
                print("No combineoi CSV file found in the ZIP archive.")
                return None

            return bhav_df

        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        
    def fno_eod_participant_wise_oi(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        url = f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_{str(trade_date.strftime('%d%m%Y').upper())}.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None
    
    def fno_eod_participant_wise_vol(self, trade_date: str):
        self.rotate_user_agent()
        trade_date = datetime.strptime(trade_date, "%d-%m-%Y")
        url = f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_vol_{str(trade_date.strftime('%d%m%Y').upper())}.csv"
        try:
            nse_resp = requests.get(url, headers=self.headers, timeout=10)
            nse_resp.raise_for_status()
            bhav_df = pd.read_csv(BytesIO(nse_resp.content))
            return bhav_df
        except (requests.RequestException, ValueError):
            return None

    def future_price_volume_data(self, *args, **kwargs) -> pd.DataFrame:
        """
        Fetch NSE Futures Price & Volume Data with flexible arguments.

        Usage Examples:
        ----------------
        get.future_price_volume_data("NIFTY", "Index Futures", "OCT-25", "01-10-2025", "17-10-2025")
        get.future_price_volume_data("NIFTY", "Index Futures", "NOV-24")  # past expiry → auto 90-day history
        get.future_price_volume_data("BANKNIFTY", "Index Futures", "3M")  # rolling 3 months
        get.future_price_volume_data("ITC", "Stock Futures", "OCT-25", "04-10-2025")
        """

        # ---------------- Reference URL ----------------
        ref_url = "https://www.nseindia.com/report-detail/fo_eq_security"
        base_url = "https://www.nseindia.com/api/historicalOR/foCPV"
        today = datetime.now()
        dd_mm_yyyy = "%d-%m-%Y"

        # ---------------- Argument Parsing ----------------
        if len(args) < 2:
            raise ValueError("At least symbol and instrument must be provided.")

        symbol = args[0].strip().upper()
        instrument = args[1].strip().lower()
        expiry = from_date = to_date = period = None

        for arg in args[2:]:
            arg = str(arg).strip().upper()
            if arg in ["1D", "1W", "1M", "3M", "6M"]:
                period = arg
            elif any(m in arg for m in [
                "JAN","FEB","MAR","APR","MAY","JUN",
                "JUL","AUG","SEP","OCT","NOV","DEC"
            ]):
                expiry = arg
            elif "-" in arg and len(arg.split("-")) == 3 and all(p.isdigit() for p in arg.split("-")):
                if not from_date:
                    from_date = arg
                else:
                    to_date = arg
            else:
                print(f"⚠️ Unrecognized argument ignored: {arg}")

        expiry = expiry or kwargs.get("expiry")
        from_date = from_date or kwargs.get("from_date")
        to_date = to_date or kwargs.get("to_date")
        period = period or kwargs.get("period")

        # ---------------- Instrument Mapping ----------------
        if instrument in ["futidx", "index futures", "index future", "index"]:
            instrument = "FUTIDX"
        elif instrument in ["futstk", "stock futures", "stock future", "stock"]:
            instrument = "FUTSTK"
        else:
            raise ValueError("Instrument must be 'Index Futures' or 'Stock Futures'")

        # ---------------- Expiry Handling ----------------
        expiry_date = None
        if expiry:
            try:
                expiry_parts = expiry.split("-")
                expiry_month = expiry_parts[0].upper()
                expiry_year = int(expiry_parts[1])
                full_year = 2000 + expiry_year

                # Fetch NSE expiry list for that year
                self.session.get(ref_url, headers=self.headers, timeout=15)
                meta_url = f"https://www.nseindia.com/api/historicalOR/meta/foCPV/expireDts?instrument={instrument}&symbol={symbol}&year={full_year}"
                meta_resp = self.session.get(meta_url, headers=self.headers, timeout=15)
                exp_list = meta_resp.json().get("expiresDts", [])
                exp_list = [x.upper() for x in exp_list]

                matched = [x for x in exp_list if expiry_month in x.upper()]
                if not matched:
                    print(f"⚠️ Expiry {expiry} not found for {symbol} in {full_year}")
                    return pd.DataFrame()
                expiry_date = matched[0]

                # Auto 90-day history if expiry is in the past
                expiry_dt_obj = datetime.strptime(expiry_date, "%d-%b-%Y")
                if expiry_dt_obj < today and not from_date:
                    from_date = expiry_dt_obj - timedelta(days=90)
                    to_date = expiry_dt_obj
            except Exception as e:
                print(f"⚠️ Error fetching expiry list: {e}")
                return pd.DataFrame()

        # ---------------- Period / from-to Handling ----------------
        if not expiry:
            if period:
                delta = {"1D": 1, "1W": 7, "1M": 30, "3M": 90, "6M": 180}[period.upper()]
                from_date = today - timedelta(days=delta)
                to_date = today

        # ---------------- Ensure from_date/to_date are datetime ----------------
        if from_date:
            if isinstance(from_date, str):
                from_date = datetime.strptime(from_date, dd_mm_yyyy)
        else:
            from_date = today - timedelta(days=180)

        if to_date:
            if isinstance(to_date, str):
                to_date = datetime.strptime(to_date, dd_mm_yyyy)
        else:
            # If expiry_date exists, use it, otherwise today
            to_date = expiry_date and datetime.strptime(expiry_date, "%d-%b-%Y") or today

        # ---------------- Fetch Data ----------------
        params = {
            "from": from_date.strftime(dd_mm_yyyy),
            "to": to_date.strftime(dd_mm_yyyy),
            "instrumentType": instrument,
            "symbol": symbol,
            "year": today.year
        }
        if expiry_date:
            params["expiryDate"] = expiry_date

        try:
            response = self.session.get(
                base_url,
                params=params,
                headers=self.headers,
                cookies=self.session.cookies.get_dict(),
                timeout=15
            )
            response.raise_for_status()
            data = response.json().get("data", [])
        except Exception as e:
            print(f"⚠️ Error fetching data: {e}")
            return pd.DataFrame()
    
        df = pd.DataFrame(data)
        if df.empty:
            print(f"⚠️ No data available for {symbol} ({expiry_date or 'All Expiries'})")
            return df

        # ---------------- Post-processing ----------------
        df.columns = [c.upper().replace(" ", "_") for c in df.columns]

        # Convert FH_TIMESTAMP to datetime first
        if "FH_TIMESTAMP" in df.columns:
            df["FH_TIMESTAMP"] = pd.to_datetime(df["FH_TIMESTAMP"], errors="coerce")

        # Filter by expiry if specified
        if expiry_date:
            df = df[df["FH_EXPIRY_DT"].str.upper() == expiry_date.upper()]

        # ---------------- Convert timestamps to string for JSON/worksheet export ----------------
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%d-%b-%Y")

        # Sort and reset index
        return df.sort_values(["FH_TIMESTAMP", "FH_EXPIRY_DT"]).reset_index(drop=True)

    def option_price_volume_data(self, *args, **kwargs) -> pd.DataFrame:
        """
        Fetch NSE Futures / Options Price & Volume Data.
        Supports full expiry date (DD-MM-YYYY) or month-year (MON-YY) formats.
        """
        dd_mm_yyyy = "%d-%m-%Y"
        today = datetime.now()

        # ---------------- Argument Parsing ----------------
        if len(args) < 2:
            raise ValueError("At least symbol and instrument must be provided.")

        symbol = args[0].strip().upper()
        instrument = args[1].strip().lower()

        option_type = strike_price = expiry = from_date = to_date = period = None

        for arg in args[2:]:
            arg_str = str(arg).strip().upper()
            if arg_str in ["CE", "PE"]:
                option_type = arg_str
            elif arg_str.replace('.', '', 1).isdigit():
                strike_price = float(arg_str)
            elif arg_str in ["1D","1W","1M","3M","6M"]:
                period = arg_str
            elif any(m in arg_str for m in ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]):
                expiry = arg_str
            elif "-" in arg_str and len(arg_str.split("-")) == 3 and all(p.isdigit() for p in arg_str.split("-")):
                if not from_date:
                    from_date = arg_str
                else:
                    to_date = arg_str

        # Override with kwargs
        expiry = expiry or kwargs.get("expiry")
        from_date = from_date or kwargs.get("from_date")
        to_date = to_date or kwargs.get("to_date")
        period = period or kwargs.get("period")
        option_type = option_type or kwargs.get("option_type")
        strike_price = strike_price or kwargs.get("strike_price")

        # ---------------- Instrument Mapping ----------------
        if instrument in ["optidx","index options","index option","index"]:
            instrument = "OPTIDX"
        elif instrument in ["optstk","stock options","stock option","stock"]:
            instrument = "OPTSTK"
        else:
            raise ValueError("Instrument must be 'Index Options' or 'Stock Options'")

        # ---------------- Expiry Handling ----------------
        expiry_date = None
        if expiry:
            try:
                expiry = str(expiry).upper()
                month_names = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]

                # Case 1: Full date (DD-MM-YYYY)
                if "-" in expiry and len(expiry.split("-")) == 3 and all(p.isdigit() for p in expiry.split("-")):
                    expiry_date = datetime.strptime(expiry, "%d-%m-%Y").strftime("%d-%b-%Y")

                # Case 2: Month-Year (MON-YY)
                elif any(m in expiry for m in month_names):
                    month, year_suffix = expiry.split("-")
                    expiry_year = 2000 + int(year_suffix)

                    # Ensure get_expiries exists
                    if hasattr(self, "get_expiries"):
                        exp_list = self.get_expiries(instrument, symbol, expiry_year)
                        matched = [x for x in exp_list if month in x.upper()]
                        if not matched:
                            print(f"⚠️ No expiry found for {symbol} in {expiry}")
                            return pd.DataFrame()
                        # pick last expiry of the month
                        expiry_date = max(matched, key=lambda d: datetime.strptime(d, "%d-%b-%Y"))
                    else:
                        print(f"⚠️ get_expiries method not found, cannot pick last expiry for {expiry}")
                        return pd.DataFrame()
                else:
                    print(f"⚠️ Unknown expiry format: {expiry}")
                    return pd.DataFrame()

            except Exception as e:
                print(f"⚠️ Error processing expiry: {e}")
                return pd.DataFrame()

        # ---------------- Period / From-To Handling ----------------
        if not expiry:
            if period:
                delta_days = {"1D":1,"1W":7,"1M":30,"3M":90,"6M":180}.get(period.upper(),30)
                from_date = today - timedelta(days=delta_days)
                to_date = today

        # Convert from_date/to_date to datetime
        if from_date:
            if isinstance(from_date, str):
                from_date = datetime.strptime(from_date, dd_mm_yyyy)
        else:
            from_date = today - timedelta(days=180)

        if to_date:
            if isinstance(to_date, str):
                to_date = datetime.strptime(to_date, dd_mm_yyyy)
        else:
            to_date = expiry_date and datetime.strptime(expiry_date, "%d-%b-%Y") or today

        # ---------------- Fetch Data ----------------
        base_url = "https://www.nseindia.com/api/historicalOR/foCPV"
        params = {
            "from": from_date.strftime(dd_mm_yyyy),
            "to": to_date.strftime(dd_mm_yyyy),
            "instrumentType": instrument,
            "symbol": symbol,
            "year": today.year,
            "csv": "true"
        }
        if expiry_date:
            params["expiryDate"] = expiry_date
        if option_type:
            params["optionType"] = option_type
        if strike_price:
            params["strikePrice"] = strike_price

        for _ in range(3):
            try:
                resp = self.session.get(
                    base_url,
                    params=params,
                    headers=self.headers,
                    cookies=self.session.cookies.get_dict(),
                    timeout=15
                )
                resp.raise_for_status()
                data = resp.json().get("data", [])
                break
            except Exception as e:
                time.sleep(2)
        else:
            print(f"⚠️ Failed to fetch data after retries.")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        if df.empty:
            print(f"⚠️ No data available for {symbol} ({expiry_date or 'All Expiries'})")
            return df

        # ---------------- Post-processing ----------------
        df.columns = [c.upper().replace(" ", "_") for c in df.columns]

        numeric_cols = ["FH_OPENING_PRICE","FH_TRADE_HIGH_PRICE","FH_TRADE_LOW_PRICE",
                        "FH_CLOSING_PRICE","FH_LAST_TRADED_PRICE","FH_PREV_CLS",
                        "FH_SETTLE_PRICE","FH_TOT_TRADED_QTY","FH_TOT_TRADED_VAL",
                        "FH_OPEN_INT","FH_CHANGE_IN_OI","CALCULATED_PREMIUM_VAL"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df[df["FH_TOT_TRADED_QTY"] > 0]

        sort_cols = ["FH_TIMESTAMP"]
        if "FH_EXPIRY_DT" in df.columns:
            sort_cols.append("FH_EXPIRY_DT")
        # ---------------- Format specific timestamp columns ----------------
        date_columns = {
            "FH_TIMESTAMP": "%d-%b-%Y",
            "FH_EXPIRY_DT": "%d-%b-%Y",
            "FH_TIMESTAMP_ORDER": "%d-%b-%Y %H:%M:%S"
        }

        for col, fmt in date_columns.items():
            if col in df.columns:
                def safe_format(x):
                    if pd.isna(x):
                        return ""
                    elif isinstance(x, (pd.Timestamp, datetime)):
                        return x.strftime(fmt)
                    else:
                        try:
                            dt = pd.to_datetime(x, errors="coerce")
                            if pd.notna(dt):
                                return dt.strftime(fmt)
                            else:
                                return str(x)
                        except:
                            return str(x)
                df[col] = df[col].apply(safe_format)


        return df.sort_values(sort_cols).reset_index(drop=True)

    def fno_eom_lot_size(self, symbol=None):

        try:
            url = "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv"
            resp = requests.get(url, headers=self.headers, timeout=10)
            resp.raise_for_status()

            csv_text = resp.content.decode("utf-8", errors="ignore")
            rows = list(csv.reader(csv_text.splitlines()))

            if not rows:
                return None

            headers_full = rows[0]
            non_blank_idx = [i for i, h in enumerate(headers_full) if h.strip()]
            filtered_headers = [headers_full[i] for i in non_blank_idx]

            filtered_rows = []
            for row in rows[1:]:
                if len(row) > 1 and (symbol is None or row[1].strip().upper() == symbol.upper()):
                    row_padded = (row + [""] * len(headers_full))[:len(headers_full)]
                    filtered_row = [row_padded[i] for i in non_blank_idx]
                    filtered_rows.append(filtered_row)

            if not filtered_rows:
                return None

            return pd.DataFrame(filtered_rows, columns=filtered_headers)

        except Exception as e:
            print(f"❌ Error fetching F&O Lot Size: {e}")
            return None

    
    def fno_dmy_biz_growth(self, *args, mode="monthly", month=None, year=None):

        now = datetime.now()

        # --- Month mapping ---
        month_map = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
        }
        reverse_month_map = {v: k for k, v in month_map.items()}

        # --- Auto-detect arguments ---
        for arg in args:
            if isinstance(arg, str):
                arg_upper = arg.strip().upper()
                if arg_upper in ["YEARLY", "MONTHLY", "DAILY"]:
                    mode = arg_upper.lower()
                elif arg_upper.isdigit() and len(arg_upper) == 4:
                    year = int(arg_upper)
                elif arg_upper[:3] in month_map:
                    month = month_map[arg_upper[:3]]
            elif isinstance(arg, int):
                if 1900 <= arg <= 2100:
                    year = arg
                elif 1 <= arg <= 12:
                    month = arg

        # --- Defaults ---
        if year is None:
            year = now.year
        if month is None:
            month = now.month

        # --- Prepare URLs ---
        self.rotate_user_agent()
        ref_url = "https://www.nseindia.com"

        if mode.lower() == "yearly":
            api_url = "https://www.nseindia.com/api/historicalOR/fo/tbg/yearly"
        elif mode.lower() == "monthly":
            from_year = year
            to_year = year + 1
            api_url = f"https://www.nseindia.com/api/historicalOR/fo/tbg/monthly?from={from_year}&to={to_year}"
        elif mode.lower() == "daily":
            month_str = reverse_month_map.get(month, str(month)).title()
            api_url = f"https://www.nseindia.com/api/historicalOR/fo/tbg/daily?month={month_str}&year={year}"
        else:
            raise ValueError("Invalid mode. Use 'yearly', 'monthly', or 'daily'")

        try:
            # --- Get cookies ---
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()

            # --- Fetch API data ---
            response = self.session.get(api_url, headers=self.headers,
                                        cookies=ref_response.cookies.get_dict(), timeout=10)
            response.raise_for_status()
            raw_data = response.json()

            # --- Extract 'data' ---
            data_list = []
            if mode.lower() == "yearly":
                for item in raw_data.get("data", []):
                    if "data" in item:
                        data_list.append(item["data"])
            elif mode.lower() == "monthly":
                for item in raw_data.get("data", []):
                    if "data" in item:
                        data_list.append(item["data"])
            elif mode.lower() == "daily":
                for item in raw_data.get("data", []):
                    if "data" in item:
                        data_list.append(item["data"])

            if not data_list:
                print("⚠️ No valid data found.")
                return None

            df = pd.DataFrame(data_list)

            # --- Clean numeric columns ---
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).str.replace(",", "").str.strip()
                    df[col] = df[col].replace({"None": "-", "nan": "-", "NaN": "-", "": "-", pd.NA: "-"})
                    def to_numeric_or_keep(x):
                        try:
                            return pd.to_numeric(x)
                        except:
                            return x
                    df[col] = df[col].apply(to_numeric_or_keep)

            # --- Rename columns ---
            if mode.lower() == "yearly":
                # F&O-specific column renaming
                rename_map = {
                    "date": "FY",
                    "Index_Futures_QTY": "Index_Futures_Qty",
                    "Index_Futures_VAL": "Index_Futures_Val",
                    "Stock_Futures_QTY": "Stock_Futures_Qty",
                    "Stock_Futures_VAL": "Stock_Futures_Val",
                    "Index_Options_QTY": "Index_Options_Qty",
                    "Index_Options_VAL": "Index_Options_Val",
                    "Index_Options_PREM_VAL": "Index_Options_Prem_Val",
                    "Stock_Options_QTY": "Stock_Options_Qty",
                    "Stock_Options_VAL": "Stock_Options_Val",
                    "Stock_Options_PREM_VAL": "Stock_Options_Prem_Val",
                    "F&O_Total_QTY": "FO_Total_Qty",
                    "F&O_Total_VAL": "FO_Total_Val",
                    "TOTAL_TRADED_PREM_VAL": "Total_Traded_Prem_Val",
                    "F&O_AVG_DAILYTURNOVER": "FO_Avg_Daily_Turnover"
                }
                df.rename(columns=rename_map, inplace=True)

            elif mode.lower() == "monthly":
                rename_map = {
                    "date": "Month",
                    "Index_Futures_QTY": "Index_Futures_Qty",
                    "Index_Futures_VAL": "Index_Futures_Val",
                    "Stock_Futures_QTY": "Stock_Futures_Qty",
                    "Stock_Futures_VAL": "Stock_Futures_Val",
                    "Index_Options_QTY": "Index_Options_Qty",
                    "Index_Options_VAL": "Index_Options_Val",
                    "Index_Options_PREM_VAL": "Index_Options_Prem_Val",
                    "Stock_Options_QTY": "Stock_Options_Qty",
                    "Stock_Options_VAL": "Stock_Options_Val",
                    "Stock_Options_PREM_VAL": "Stock_Options_Prem_Val",
                    "F&O_Total_QTY": "FO_Total_Qty",
                    "F&O_Total_VAL": "FO_Total_Val",
                    "TOTAL_TRADED_PREM_VAL": "Total_Traded_Prem_Val",
                    "F&O_AVG_DAILYTURNOVER": "FO_Avg_Daily_Turnover"
                }
                df.rename(columns=rename_map, inplace=True)

            elif mode.lower() == "daily":
                rename_map = {
                    "date": "Date",
                    "Index_Futures_QTY": "Index_Futures_Qty",
                    "Index_Futures_VAL": "Index_Futures_Val",
                    "Stock_Futures_QTY": "Stock_Futures_Qty",
                    "Stock_Futures_VAL": "Stock_Futures_Val",
                    "Index_Options_QTY": "Index_Options_Qty",
                    "Index_Options_VAL": "Index_Options_Val",
                    "Index_Options_PREM_VAL": "Index_Options_Prem_Val",
                    "Index_Options_PUT_CALL_RATIO": "Index_Options_PCR",
                    "Stock_Options_QTY": "Stock_Options_Qty",
                    "Stock_Options_VAL": "Stock_Options_Val",
                    "Stock_Options_PREM_VAL": "Stock_Options_Prem_Val",
                    "Stock_Options_PUT_CALL_RATIO": "Stock_Options_PCR",
                    "F&O_Total_QTY": "FO_Total_Qty",
                    "F&O_Total_VAL": "FO_Total_Val",
                    "TOTAL_TRADED_PREM_VAL": "Total_Traded_Prem_Val",
                    "F&O_Total_PUT_CALL_RATIO": "FO_Total_PCR"
                }
                df.rename(columns=rename_map, inplace=True)

            # --- Make JSON-serializable ---
            return df.astype(object).to_dict(orient='records')

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching F&O data: {e}")
            return None


    def fno_monthly_settlement_report(self, *args, from_year=None, to_year=None, period=None):
        """
        Fetch NSE Monthly Settlement Statistics (F&O) for given financial years or period.

        Parameters
        ----------
        *args : str
            Can include years (e.g., "2023") or periods ("1Y", "2Y").
        from_year : int, optional
            Financial year start (FY start year).
        to_year : int, optional
            Financial year end (FY start year + 1).
        period : str, optional
            Predefined period: "1Y", "2Y", "3Y", "5Y".

        Returns
        -------
        pd.DataFrame
            Monthly F&O settlement statistics with cleaned numeric columns.
        """

        # --- Auto-detect arguments ---
        for arg in args:
            if isinstance(arg, str):
                if re.match(r"^\d{4}$", arg):  # year
                    if not from_year:
                        from_year = int(arg)
                    elif not to_year:
                        to_year = int(arg)
                elif arg.upper() in ["1Y", "2Y", "3Y", "5Y"]:
                    period = arg.upper()

        # --- Determine financial years ---
        today = datetime.now()
        current_month = today.month
        current_fy_start = today.year if current_month >= 4 else today.year - 1

        if period and not from_year:
            years_back = int(period.replace("Y", ""))
            from_year = current_fy_start - years_back
            to_year = current_fy_start + 1  # include current FY
        elif not from_year:
            from_year = current_fy_start
            to_year = current_fy_start + 1
        elif not to_year:
            to_year = from_year + 1

        # --- Rotate user-agent & base URLs ---
        self.rotate_user_agent()
        ref_url = "https://www.nseindia.com/report-detail/monthly-settlement-statistics"
        base_api = "https://www.nseindia.com/api/financial-monthlyStats?from_date=Apr-{}&to_date=Mar-{}"

        try:
            # --- Get NSE cookies ---
            ref_response = self.session.get(ref_url, headers=self.headers, timeout=10)
            ref_response.raise_for_status()
            cookies = ref_response.cookies.get_dict()

            all_data = []
            empty_fys = []

            for fy_start in range(from_year, to_year):
                fy_end = fy_start + 1
                fin_year = f"{fy_start}-{fy_end}"
                api_url = base_api.format(fy_start, fy_end)

                # --- Fetch with retry ---
                for attempt in range(3):
                    try:
                        response = self.session.get(api_url, headers=self.headers, cookies=cookies, timeout=15)
                        if response.status_code == 200:
                            js = response.json()

                            # --- Handle top-level list or 'data' key ---
                            if isinstance(js, list) and js:
                                for rec in js:
                                    rec["FinancialYear"] = fin_year
                                all_data.extend(js)
                                print(f"✅ {fin_year} data fetched ({len(js)} records).")
                            elif "data" in js and isinstance(js["data"], list) and js["data"]:
                                for rec in js["data"]:
                                    rec["FinancialYear"] = fin_year
                                all_data.extend(js["data"])
                                print(f"✅ {fin_year} data fetched ({len(js['data'])} records).")
                            else:
                                empty_fys.append(fin_year)
                                print(f"⚠️ No data for {fin_year}.")
                            break
                        else:
                            print(f"⚠️ Failed for {fin_year} | HTTP {response.status_code}")
                    except Exception as e:
                        print(f"⚠️ Attempt {attempt+1} failed for {fin_year}: {e}")
                        time.sleep(1)
                time.sleep(0.8)  # polite delay

            if not all_data:
                print(f"⚠️ No settlement data found from FY {from_year} to current.")
                return None

            # --- Create DataFrame ---
            df = pd.DataFrame(all_data)

            # --- Rename columns ---
            rename_map = {
                "st_date": "Month",
                "st_Mtm": "Fut MTM Settlement",
                "st_Final": "Fut Final Settlement",
                "st_Premium": "Opt Premium Settlement",
                "st_Excercise": "Opt Exercise Settlement",
                "st_Total": "Total"
            }
            df.rename(columns=rename_map, inplace=True)

            # --- Clean numeric columns ---
            numeric_cols = ["Fut MTM Settlement", "Fut Final Settlement",
                            "Opt Premium Settlement", "Opt Exercise Settlement", "Total"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "").str.strip(), errors='coerce')

            # --- Parse Month as datetime for sorting ---
            df["Month_dt"] = pd.to_datetime(df["Month"], format="%b-%Y", errors='coerce')
            df.sort_values(["FinancialYear", "Month_dt"], inplace=True)
            df.reset_index(drop=True, inplace=True)
            df["Month"] = df["Month_dt"].dt.strftime('%b-%Y')  # keep display format
            df.drop(columns=["Month_dt"], inplace=True)

            print(f"ℹ️ Data fetching complete from FY {from_year} to FY {to_year-1}.")
            if empty_fys:
                print(f"⚠️ No data found for FYs: {', '.join(empty_fys)}")

            return df
        except Exception as e:
            print(f"❌ Error fetching Monthly Settlement data: {e}")
            return None

    #---------------------------------------------------------- SEBI_Data ----------------------------------------------------------------

    def sebi_circulars(self,*args, period="1W"):
        """
        Fetch SEBI circulars with flexible auto-detect arguments.
        
        Usage:
            get_sebi_circulars("01-10-2025", "10-10-2025")  # explicit from and to
            get_sebi_circulars("01-10-2025")               # from date only, to today
            get_sebi_circulars(period="1W")                # last 1 week
        """
        today = datetime.today()
        from_date_dt = None
        to_date_dt = None

        # --- Auto-detect arguments ---
        if period:  # period string takes priority
            period = period.upper()
            if period.endswith("D"):
                days = int(period[:-1])
                from_date_dt = today - timedelta(days=days)
                to_date_dt = today
            elif period.endswith("W"):
                weeks = int(period[:-1])
                from_date_dt = today - timedelta(weeks=weeks)
                to_date_dt = today
            elif period.endswith("M"):
                months = int(period[:-1])
                from_date_dt = today - timedelta(days=30*months)  # approximate
                to_date_dt = today
            elif period.endswith("Y"):
                years = int(period[:-1])
                from_date_dt = today - timedelta(days=365*years)  # approximate
                to_date_dt = today
            else:
                raise ValueError("Invalid period format. Use 1W, 1M, 3M, 6M, 1Y, etc.")
        elif len(args) == 2:  # from_date and to_date
            from_date_dt = datetime.strptime(args[0], "%d-%m-%Y")
            to_date_dt = datetime.strptime(args[1], "%d-%m-%Y")
        elif len(args) == 1:  # only from_date
            from_date_dt = datetime.strptime(args[0], "%d-%m-%Y")
            to_date_dt = today
        else:
            # default last 7 days
            from_date_dt = today - timedelta(days=7)
            to_date_dt = today

        # Format dates for SEBI
        from_date_str = from_date_dt.strftime("%d-%m-%Y")
        to_date_str = to_date_dt.strftime("%d-%m-%Y")

        # --- SEBI POST request ---
        url = "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0",
            "Origin": "https://www.sebi.gov.in",
            "X-Requested-With": "XMLHttpRequest",
        }
        payload = {
            "fromDate": from_date_str,
            "toDate": to_date_str,
            "fromYear": "",
            "toYear": "",
            "deptId": "-1",
            "sid": "1",
            "ssid": "7",
            "smid": "0",
            "ssidhidden": "7",
            "intmid": "-1",
            "sText": "",
            "ssText": "Circulars",
            "smText": "",
            "doDirect": "-1",
            "nextValue": "1",
            "nextDel": "1",
            "totalpage": "1",
        }

        resp = requests.post(url, headers=headers, data=payload, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", {"id": "sample_1"})
        rows = []
        if table:
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    date = pd.to_datetime(tds[0].text.strip(), errors='coerce')
                    link_tag = tds[1].find("a")
                    title = link_tag.get("title") if link_tag else tds[1].text.strip()
                    href = link_tag.get("href") if link_tag else None
                    if href and not href.startswith("http"):
                        href = "https://www.sebi.gov.in" + href
                    rows.append({"Date": date, "Title": title, "Link": href})
        df = pd.DataFrame(rows)
        if not df.empty:
            df.sort_values("Date", ascending=False, inplace=True)
            df.drop_duplicates(subset=["Date", "Title"], inplace=True)
            df.reset_index(drop=True, inplace=True)
            df['Date'] = df['Date'].dt.strftime('%d-%b-%Y')
        return df

    def sebi_data(self,pages=1):
        """
        Fetch latest SEBI circulars from the official AJAX endpoint.
        :param pages: Number of pages to fetch (default: 1)
        :return: DataFrame of circulars
        """
        base_url = "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0",
            "Origin": "https://www.sebi.gov.in",
            "X-Requested-With": "XMLHttpRequest",
        }

        all_rows = []
        session = requests.Session()

        for page in range(1, pages + 1):
            payload = {
                "nextValue": str(page),
                "nextDel": str(page),
                "totalpage": str(pages),
                "nextPage": "",
                "doDirect": "1"
            }

            resp = session.post(base_url, headers=headers, data=payload, timeout=15)
            if resp.status_code != 200:
                print(f"⚠️ Page {page}: HTTP {resp.status_code} error.")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table", {"id": "sample_1"})
            if not table:
                print(f"⚠️ Page {page}: No table found.")
                break

            # Extract rows
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue
                date = tds[0].get_text(strip=True)
                link_tag = tds[1].find("a")
                title = link_tag.get("title", "").strip() if link_tag else tds[1].get_text(strip=True)
                href = link_tag.get("href") if link_tag else None
                if href and not href.startswith("http"):
                    href = "https://www.sebi.gov.in" + href
                all_rows.append({"Date": date, "Title": title, "Link": href})

            # print(f"✅ Page {page} extracted ({len(all_rows)} total so far)")

        # Build DataFrame
        df = pd.DataFrame(all_rows)
        if not df.empty:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df.sort_values("Date", ascending=False, inplace=True)
            df.drop_duplicates(subset=["Date", "Title"], keep="first", inplace=True)
            df.reset_index(drop=True, inplace=True)
            df['Date'] = df['Date'].dt.strftime('%d-%b-%Y')
            # print(f"✅ Total Circulars Extracted: {len(df)}")
        else:
            print("⚠️ No data extracted — check SEBI endpoint or payload values.")

        return df
    
