# -*- coding:utf-8 -*-
import datetime
import pandas as pd
from openbb import obb
try:
    from config.settings import FRED_SERIES, REPORT_DAYS
except ImportError:
    # Fallback for testing/independent execution
    FRED_SERIES = {}
    REPORT_DAYS = 30

class OpenBBSource:
    """
    OpenBB Data Source Adapter
    Wraps OpenBB SDK v4 calls for Equity, Fixed Income, and Economy modules.
    """
    
    def __init__(self):
        # Initialize any specific settings if needed
        # obb.user.credentials.fred_api_key = ... (If not set in env)
        pass

    def fetch_equity_price(self, symbol, provider="yfinance", interval="1d", days=REPORT_DAYS):
        """
        Fetch historical price data for a given symbol.
        """
        start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
        try:
            # OpenBB v4: obb.equity.price.historical(symbol=..., provider=...)
            df = obb.equity.price.historical(
                symbol=symbol, 
                start_date=start_date, 
                provider=provider, 
                interval=interval
            ).to_df()
            
            if df.empty:
                return [], f"No data for {symbol}"
                
            df = df.reset_index()
            # Standardize columns: date, open, high, low, close, volume
            # OpenBB v4 usually returns 'date' as index or column
            if 'date' not in df.columns and 'index' in df.columns:
                 df.rename(columns={'index': 'date'}, inplace=True)
            
            # Ensure date is string YYYY-MM-DD
            if 'date' in df.columns:
                 df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            
            records = df.to_dict(orient="records")
            return records, None
            
        except Exception as e:
            return [], str(e)

    def fetch_fred_series(self, series_id, days=180):
        """
        Fetch economic data from FRED via OpenBB.
        """
        start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
        try:
            # v4: obb.economy.fred_series(...) or obb.economy.gdp(...) etc.
            # But the generic one is usually obb.economy.fred_series or similar?
            # Actually generic FRED fetch might be obb.economy.fred_series(symbol=...)
            # Let's verify documentation if possible, but assuming `obb.economy.fred_series` or `obb.index.price.historical` if it's treated as index.
            # Wait, OpenBB v4 has `obb.economy.fred_series`?
            # Let's try `obb.economy.fred_series` if it exists, otherwise `obb.equity.price.historical` might not work for FRED IDs.
            # Using `obb.economy.fred_search` hints at `fred_series`.
            # A safer bet for generic FRED is using `fredapi` directly if OpenBB signature is uncertain, 
            # BUT the goal is OpenBB Integration.
            # The standard v4 way for generic FRED is: obb.economy.fred_series(symbol=...)
            
            df = obb.economy.fred_series(
                symbol=series_id,
                start_date=start_date,
                provider="fred"
            ).to_df()
            
            if df.empty:
                return [], f"No data for {series_id}"
            
            df = df.reset_index()
            if 'date' not in df.columns and 'index' in df.columns:
                df.rename(columns={'index': 'date'}, inplace=True)
                
            # FRED data usually has a value column. OpenBB might name it after the series or 'value'
            # Let's normalize.
            
            records = df.to_dict(orient="records")
            # Cleaning keys: ensure we have 'date' and 'value'
            cleaned = []
            for r in records:
                # Find the value column (not date)
                val = None
                for k, v in r.items():
                    if k != 'date':
                        val = v
                        break
                cleaned.append({
                    "date": pd.to_datetime(r['date']).strftime('%Y-%m-%d'),
                    "value": val
                })
                
            cleaned.sort(key=lambda x: x['date'], reverse=True)
            return cleaned, None

        except Exception as e:
            return [], str(e)

    def fetch_treasury_rates(self):
        """
        Fetch US Treasury Rates (2Y, 10Y, 30Y)
        """
        # Could use specific OpenBB function or just FRED series
        # 10Y: DGS10, 2Y: DGS2, 30Y: DGS30
        map_series = {
            "10年": "DGS10",
            "2年": "DGS2",
            "30年": "DGS30"
        }
        
        results = {}
        error_msg = []
        
        latest_date = None
        
        for label, sid in map_series.items():
            data, err = self.fetch_fred_series(sid, days=30)
            if data:
                # Get latest non-nan value
                for item in data:
                    if item['value'] is not None and not pd.isna(item['value']):
                        results[label] = item['value']
                        if latest_date is None or item['date'] > latest_date:
                            latest_date = item['date']
                        break
            else:
                error_msg.append(f"{label}: {err}")
                
        if results:
            return [{"date": latest_date, **results}], None
        else:
            return [], "; ".join(error_msg)

if __name__ == "__main__":
    # Test
    source = OpenBBSource()
    print("Testing Equity...")
    print(source.fetch_equity_price("AAPL", days=5))
    print("Testing FRED...")
    print(source.fetch_fred_series("DGS10", days=5))
