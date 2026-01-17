# collectors.py
import concurrent.futures
import pandas as pd
from src.data_sources.providers import OpenBBSource, AkShareSource, FredSource, SeleniumSource
from src.processors.core import DataProcessor

class MarketCollector:
    def __init__(self, config_manager, message_bus):
        self.cfg = config_manager
        self.bus = message_bus
        self.processor = DataProcessor()
        
        self.obb = OpenBBSource(message_bus)
        self.ak = AkShareSource(message_bus)
        self.fred = FredSource(message_bus, api_key=self.cfg.FRED_CONFIG.get("api_key"))
        # Selenium Source initialization might be heavy, do on demand or once?
        # self.selenium = SeleniumSource(message_bus) 

    def collect_klines(self, category):
        """
        Generic K-Line collector for a category (Indices, Commodities, etc.)
        Returns: (kline_data_dict, ma_list)
        """
        targets = self.cfg.TARGETS_KLINES.get(category, [])
        kline_results = {}
        ma_results = []
        
        for item in targets:
            name = item["name"]
            provider = item.get("provider", "openbb")
            
            df = None
            err = None
            
            self.bus.publish("INFO", f"Fetching {name} via {provider}...")
            
            if provider == "openbb":
                # item["yf"] is the symbol usually
                df, err = self.obb.fetch_historical(item["yf"])
            elif provider == "akshare_stock_a":
                df, err = self.ak.fetch_stock_a_daily(item["ak"])
            elif provider == "akshare_index":
                df, err = self.ak.fetch_index_daily(item["ak"])
            elif provider == "akshare_etf":
                df, err = self.ak.fetch_etf_daily(item["ak"])
                
            if df is not None and not df.empty:
                # Calculate MA
                mas = self.processor.calculate_ma(df, name)
                if mas: ma_results.extend(mas)
                
                # Slice for Report (Last 20 days)
                # Need to convert date to datetime to filter, then formatting
                try:
                    cutoff = pd.Timestamp.now() - pd.Timedelta(days=self.cfg.REPORT_DAYS + 10)
                    if 'date' in df.columns:
                        recent_df = df[df['date'] >= cutoff].copy()
                        kline_results[name] = self.processor.clean_kline_data(recent_df)
                    else:
                        kline_results[name] = []
                except Exception as e:
                    self.bus.publish("ERROR", f"Processing {name}", False, e)
                
                self.bus.publish("DATA_FETCH", f"{name} Success", True)
            else:
                self.bus.publish("DATA_FETCH", f"{name} Failed", False, err)
                
        return kline_results, ma_results

    def collect_fred_data(self):
        results = {}
        for name, series_id in self.cfg.TARGETS_FRED.items():
            data, err = self.fred.fetch_series(series_id)
            if data:
                results[name] = data # List of {date, value}
                self.bus.publish("DATA_FETCH", f"FRED {name}", True)
            else:
                self.bus.publish("DATA_FETCH", f"FRED {name}", False, err)
        return results

    # ... Other collection methods (Selenium, Crypto) ...
