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
        """Fetch Advances/Declines data from Moneycontrol, human-like headers and sorted by HH:MM."""
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
            raise ValueError("Unsupported index. Use 'NIFTY 50' or 'NIFTY 500'.")

        # Rotate UA before request
        self.rotate_user_agent()

        response = self.session.get(url, headers=self.headers, timeout=10)
        response.raise_for_status()
        html_text = response.text

        # Extract JSON data from JS
        pattern = r"createAdcDecGraph\([^,]+,\s*'(\[.*?\])'\)"
        match = re.search(pattern, html_text, re.DOTALL)
        if not match:
            raise ValueError(f"Advances/Declines data not found for {index_name}.")

        df = pd.DataFrame(json.loads(match.group(1)))

        # Convert to datetime for sorting
        df['time'] = pd.to_datetime(df['time'], format='%H:%M')
        df = df.sort_values('time').reset_index(drop=True)

        # Keep only HH:MM format
        df['time'] = df['time'].dt.strftime('%H:%M')

        # Ensure column order
        df = df[['time', 'advances', 'declines', 'unchanged']]

        return df
