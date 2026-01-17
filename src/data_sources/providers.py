# data_sources.py
import requests
import pandas as pd
import datetime
import os
import akshare as ak
from openbb import obb
from selenium.webdriver.chrome.options import Options
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Import legacy scraping modules (to be wrapped)
from . import selenium_scrapers_investing
from . import selenium_scrapers_misc
from .fetch_data_core import _fetch_vietnam_index_klines_scraping # Re-use robust fallback logic

class BaseSource:
    def __init__(self, message_bus=None):
        self.bus = message_bus
        self.session = self._get_retry_session()

    def _get_retry_session(self, retries=3):
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.9"
        })
        retry = Retry(total=retries, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retry))
        session.mount('https://', HTTPAdapter(max_retries=retry))
        return session

class OpenBBSource(BaseSource):
    def fetch_historical(self, symbol, days=365):
        try:
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=days + 10)
            
            # Crypto handling (BTC-USD needs crypto provider logic ideally, but 'yfinance' via equity works often for main pairs)
            # Or use obb.crypto.price.historical if symbol is like BTC-USD
            if "BTC" in symbol:
                df = obb.crypto.price.historical(symbol=symbol, start_date=start_date.strftime('%Y-%m-%d'), provider="yfinance").to_df()
            else:
                df = obb.equity.price.historical(symbol=symbol, start_date=start_date.strftime('%Y-%m-%d'), provider="yfinance").to_df()
            
            if df is None or df.empty:
                return None, "Empty Data"
            
            df = df.reset_index()
            # Standardize columns to lowercase
            df.columns = [c.lower() for c in df.columns]
            
            # Find date column
            date_col = next((c for c in df.columns if "date" in c), None)
            if not date_col:
                return None, "No date column"
                
            df = df.rename(columns={date_col: "date"})
            df["date"] = pd.to_datetime(df["date"])
            
            # Ensure essential columns
            for c in ['open', 'high', 'low', 'close', 'volume']:
                 if c not in df.columns:
                     df[c] = 0.0
            
            return df, None
        except Exception as e:
            return None, str(e)

class AkShareSource(BaseSource):
    def fetch_stock_a_daily(self, symbol):
        try:
            # stock_zh_a_hist also works, check which is best. Using 'stock_zh_a_hist' is standard.
            df = ak.stock_zh_a_hist(symbol=symbol, adjust="qfq")
            if df.empty: return None, "Empty"
            # Renaming
            df = df.rename(columns={"日期":"date", "开盘":"open", "收盘":"close", "最高":"high", "最低":"low", "成交量":"volume", "成交额":"amount"})
            df['date'] = pd.to_datetime(df['date'])
            return df, None
        except Exception as e:
            return None, str(e)

    def fetch_index_daily(self, symbol):
        try:
            df = ak.stock_zh_index_daily_em(symbol=symbol)
            if df.empty: return None, "Empty"
            df['date'] = pd.to_datetime(df['date'])
            # columns usually already english-ish from this API: date, open, close, high, low, volume, amount
            return df, None
        except Exception as e:
            return None, str(e)

    def fetch_etf_daily(self, symbol):
        try:
            df = ak.fund_etf_hist_em(symbol=symbol, adjust="qfq")
            if df.empty: return None, "Empty"
            df = df.rename(columns={"日期":"date", "开盘":"open", "收盘":"close", "最高":"high", "最低":"low", "成交量":"volume", "成交额":"amount"})
            df['date'] = pd.to_datetime(df['date'])
            return df, None
        except Exception as e:
            return None, str(e)
            
    def fetch_future_daily(self, symbol):
        # 上海金 au0
        try:
             df = ak.futures_zh_spot(symbol=symbol, market="CF", adjust='0')
             # This might return current spot? For history: futures_zh_daily_sina(symbol='au0')
             df = ak.futures_zh_daily_sina(symbol=symbol)
             if df.empty: return None, "Empty"
             df = df.rename(columns={"date":"date", "open":"open", "close":"close", "high":"high", "low":"low", "volume":"volume"})
             df['date'] = pd.to_datetime(df['date'])
             return df, None
        except Exception as e:
            return None, str(e)

    # ... Other macro AkShare fetchers ...
    def fetch_bond_china(self):
        # Implementation from fetch_data_core
        pass

class FredSource(BaseSource):
    def fetch_series(self, series_id, days=60):
        key = os.environ.get("Fred_API_KEY")
        if not key: return [], "No API Key"
        
        base_url = "https://api.stlouisfed.org/fred/series/observations"
        obs_start = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
        
        try:
            params = {
                "series_id": series_id,
                "api_key": key,
                "file_type": "json",
                "observation_start": obs_start,
                "sort_order": "asc"
            }
            r = self.session.get(base_url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json().get("observations", [])
            
            clean_data = []
            for item in data:
                if item["value"] == ".": continue
                clean_data.append({
                    "date": item["date"],
                    "value": float(item["value"])
                })
            return clean_data, None
        except Exception as e:
            return [], str(e)

class SeleniumSource(BaseSource):
    def __init__(self, message_bus=None):
        super().__init__(message_bus)
        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("--disable-gpu")
        # ... other options ...

    def fetch_all(self, target_dict):
        # Reuse logic from selenium_core.MacroDataScraper.fetch_single_source logic
        pass
