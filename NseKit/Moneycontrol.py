import requests
import pandas as pd
import re
import json
import random
import time

class MC:
    def __init__(self):
        self.session = requests.Session()
        self._initialize_session()

    def _initialize_session(self):
        """Initialize session with proper cookies and headers."""
        self.headers = {
            'User-Agent': self._get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.moneycontrol.com/',
            'Connection': 'keep-alive',
        }
        try:
            # Make initial request to get cookies
            self.session.get("https://www.moneycontrol.com", headers=self.headers, timeout=10)
            time.sleep(0.5)
        except requests.RequestException:
            pass

    def _get_random_user_agent(self):
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
        ]
        return random.choice(user_agents)

    def rotate_user_agent(self):
        """Rotate User-Agent to reduce bot detection."""
        self.headers['User-Agent'] = self._get_random_user_agent()


    #---------------------------------------------------------------------------------------------------------------------------------------------------------        

    def fetch_adv_dec(self, index_name="NIFTY 50"):
        """Fetch Advances/Declines data from Moneycontrol, sorted by HH:MM.
        Returns a DataFrame or None if an error occurs.
        """
        try:
            # ---------------------------
            # URL Selection
            # ---------------------------
            if index_name == "NIFTY 50":
                url = (
                    "https://www.moneycontrol.com/markets/indian-indices/chartData?"
                    "deviceType=web&subIndicesId=9&subIndicesName=NIFTY%2050&ex=N"
                    "&current_page=marketTerminal&bridgeId=in;NSX&classic=true"
                )
            elif index_name == "NIFTY 500":
                url = (
                    "https://www.moneycontrol.com/markets/indian-indices/chartData?"
                    "deviceType=web&subIndicesId=7&subIndicesName=NIFTY%20500&ex=N"
                    "&current_page=marketTerminal&bridgeId=in;ncx&classic=true"
                )
            else:
                return None

            # ---------------------------
            # Fetch & Parse
            # ---------------------------
            self.rotate_user_agent()
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()

            match = re.search(r"createAdcDecGraph\([^,]+,\s*'(\[.*?\])'\)", response.text, re.DOTALL)
            if not match:
                return None

            df = pd.DataFrame(json.loads(match.group(1)))
            df['time'] = pd.to_datetime(df['time'], format='%H:%M', errors='coerce')
            df = df.dropna(subset=['time']).sort_values('time').reset_index(drop=True)
            df['time'] = df['time'].dt.strftime('%H:%M')
            df = df[['time', 'advances', 'declines', 'unchanged']]

            return df

        except (requests.RequestException, ValueError, json.JSONDecodeError) as e:
            print(f"Error fetching advances/declines for {index_name}: {e}")
            return None
