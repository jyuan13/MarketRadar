# collectors.py
import concurrent.futures
import pandas as pd
from src.data_sources.providers import OpenBBSource, AkShareSource, FredSource
from src.processors.core import DataProcessor

class MarketCollector:
    def __init__(self, config_manager, message_bus):
        self.cfg = config_manager
        self.bus = message_bus
        self.processor = DataProcessor()
        
        self.obb = OpenBBSource(message_bus)
        self.ak = AkShareSource(message_bus)
        self.fred = FredSource(message_bus, api_key=self.cfg.FRED_CONFIG.get("api_key"))
        # Selenium Source could be initialized here if needed
        # self.selenium = SeleniumSource(message_bus) 

    def collect_klines(self, category):
        """
        Generic K-Line collector for a category
        """
        targets = self.cfg.TARGETS_KLINES.get(category, [])
        kline_results = {}
        ma_results = []
        
        for item in targets:
            name = item["name"]
            provider = item.get("provider", "openbb").lower()
            
            df = None
            err = None
            
            self.bus.publish("INFO", f"Fetching {name} via {provider}...")
            
            # --- Dispatch Logic ---
            if provider == "openbb":
                df, err = self.obb.fetch_historical(item["yf"])
            elif provider == "akshare":
                # Generic fallback logic
                if item.get("ak"):
                    # Try index first for known indices
                    if category == "Indices":
                        df, err = self.ak.fetch_index_daily(item["ak"])
                    else:
                        df, err = self.ak.fetch_stock_daily(item["ak"])
            elif provider == "akshare_stock_a":
                df, err = self.ak.fetch_stock_daily(item["ak"], market="cn")
            elif provider == "akshare_index":
                df, err = self.ak.fetch_index_daily(item["ak"])
            elif provider == "akshare_etf":
                df, err = self.ak.fetch_etf_daily(item["ak"])
            elif provider == "akshare_future":
                df, err = self.ak.fetch_future_daily(item["ak"])
                
            if df is not None and not df.empty:
                # Calculate MA
                mas = self.processor.calculate_ma(df, name)
                if mas: ma_results.extend(mas)
                
                # Slice for Report
                try:
                    cutoff = pd.Timestamp.now() - pd.Timedelta(days=self.cfg.REPORT_DAYS + 10)
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
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
                results[name] = data 
                self.bus.publish("DATA_FETCH", f"FRED {name}", True)
            else:
                self.bus.publish("DATA_FETCH", f"FRED {name}", False, err)
        return results

    def collect_market_details(self):
        """
        Collect detailed/specialized market data (Star50, HSTECH 60m)
        """
        results = {
            "科创50_60分钟K线": [],
            "科创50估值": [],
            "科创50融资融券": [],
            "科创50实时快照": {},
            "恒生科技指数_60m": [] # In Legacy this was under 'hk' key, but here we collect it first
        }
        
        # 1. Sci-Tech 50 60m
        kcb_60m, err = self.ak.fetch_kcb50_60m()
        if kcb_60m: results["科创50_60分钟K线"] = kcb_60m
        
        # 2. Sci-Tech 50 Valuation
        kcb_val = self.ak.fetch_star50_valuation()
        if kcb_val: results["科创50估值"] = kcb_val
        
        # 3. Sci-Tech 50 Margin
        kcb_margin = self.ak.fetch_star50_margin()
        if kcb_margin: results["科创50融资融券"] = kcb_margin
        
        # 4. Sci-Tech 50 Spot
        kcb_spot, err = self.ak.fetch_star50_realtime_vol_ratio()
        if kcb_spot: results["科创50实时快照"] = kcb_spot

        # 5. HSTECH 60m (Proxy)
        hstech_60m, err = self.ak.fetch_hstech_60m_proxy()
        if hstech_60m: results["恒生科技指数_60m"] = hstech_60m
        
        return results
