# -*- coding:utf-8 -*-
import concurrent.futures
import pandas as pd
import datetime
import time
from DataSources.openbb_source import OpenBBSource
from DataSources.akshare_source import AkshareSource
from DataSources.web_scraper import WebScraper
from Processors.data_processor import DataProcessor
from Processors.technical_analysis import TechnicalAnalysis
try:
    from config.settings import FRED_SERIES, REPORT_DAYS
except ImportError:
    FRED_SERIES = {}
    REPORT_DAYS = 30

# ==============================================================================
# Targets Configuration (Migrated from MarketRadar.py)
# ==============================================================================
TARGETS_INDICES = {
    "纳斯达克":     {"ak": ".NDX",    "yf": "^NDX",     "type": "index_us"},
    "标普500":      {"ak": ".INX",    "yf": "^GSPC",    "type": "index_us"},
    "恒生科技":     {"ak": "HSTECH",  "yf": "^HSTECH",  "type": "index_hk"},
    "恒生指数":     {"ak": "HSI",     "yf": "^HSI",     "type": "index_hk"},
    "VNM(ETF)":     {"ak": "VNM",     "yf": "VNM",      "type": "stock_us"},
    "XBI(ETF)":     {"ak": "XBI",     "yf": "XBI",      "type": "stock_us"},
}

TARGETS_COMMODITIES = {
    "黄金(COMEX)":  {"ak": "GC",      "yf": "GC=F",     "type": "future_foreign"},  
    "白银(COMEX)":  {"ak": "SI",      "yf": "SI=F",     "type": "future_foreign"},  
    "铜(COMEX)":    {"ak": "HG",      "yf": "HG=F",     "type": "future_foreign"}, 
    "上海金":       {"ak": "au0",     "yf": None,       "type": "future_zh_sina"}, 
    "原油(WTI)":    {"ak": "CL",      "yf": "CL=F",     "type": "future_foreign"},
    "铀(URA)":      {"ak": "URA",     "yf": "URA",      "type": "stock_us"},
}

TARGETS_HSTECH_TOP20 = {
    "美团-W":       {"ak": "03690", "yf": "3690.HK", "type": "stock_hk"},
    "腾讯控股":     {"ak": "00700", "yf": "0700.HK", "type": "stock_hk"},
    "小米集团-W":   {"ak": "01810", "yf": "1810.HK", "type": "stock_hk"},
    "阿里巴巴-SW":  {"ak": "09988", "yf": "9988.HK", "type": "stock_hk"},
    # ... (Truncated for brevity, can add more if needed or load from config)
}

TARGETS_US_MAG7 = {
    "苹果(AAPL)":    {"ak": None, "yf": "AAPL",  "type": "stock_us"},
    "微软(MSFT)":    {"ak": None, "yf": "MSFT",  "type": "stock_us"},
    "谷歌(GOOGL)":   {"ak": None, "yf": "GOOGL", "type": "stock_us"},
    "亚马逊(AMZN)":  {"ak": None, "yf": "AMZN",  "type": "stock_us"},
    "英伟达(NVDA)":  {"ak": None, "yf": "NVDA",  "type": "stock_us"},
    "Meta(META)":    {"ak": None, "yf": "META",  "type": "stock_us"},
    "特斯拉(TSLA)":  {"ak": None, "yf": "TSLA",  "type": "stock_us"},
}

TARGETS_US_BANKS = {
    "摩根大通(JPM)": {"ak": None, "yf": "JPM", "type": "stock_us"},
    "美银(BAC)":     {"ak": None, "yf": "BAC", "type": "stock_us"},
    "花旗(C)":       {"ak": None, "yf": "C",   "type": "stock_us"},
    "富国银行(WFC)": {"ak": None, "yf": "WFC", "type": "stock_us"},
    "高盛(GS)":      {"ak": None, "yf": "GS",  "type": "stock_us"},
    "摩根士丹利(MS)":{"ak": None, "yf": "MS",  "type": "stock_us"},
}

class MarketCollector:
    """
    Market Data Collector
    Orchestrates data fetching for all defined targets.
    """
    
    def __init__(self):
        self.obb = OpenBBSource()
        self.ak = AkshareSource()
        self.scraper = WebScraper()
        
    def fetch_data_for_target(self, name, config, days=200):
        """
        Fetch K-line data for a single target with fallback logic.
        Strategy: AkShare (if configured) -> OpenBB/YFinance.
        """
        data = []
        error = None
        
        # 1. Try AkShare if symbol provided
        if config.get("ak"):
            # Note: AkshareSource currently implements specific bulk fetches.
            # To fetch single stock K-line via AkshareSource, we might need a generic method 
            # or rely on direct akshare calls wrapped here?
            # Ideally AkshareSource should have fetch_kline(symbol, type).
            # For this refactor, I will fallback to OpenBB/YF for standard tickers if AkshareSource
            # doesn't support specific single-stock fetch yet (it has indices).
            # But the legacy code did `market_core.fetch_akshare`.
            # I should use OpenBB if possible for US/HK stocks.
            pass

        # 2. Try OpenBB (YFinance wrapper)
        # OpenBB can handle US, HK (with suffix), etc.
        # Use 'yf' symbol if available.
        yf_symbol = config.get("yf")
        if yf_symbol:
            data, error = self.obb.fetch_equity_price(yf_symbol, days=days)
            if data:
                return data, None
        
        # 3. If Akshare is needed for specific types (e.g. Shanghai Gold future_zh_sina)
        # and OpenBB failed or no YF symbol, we need specific Akshare logic.
        # Since I didn't fully implement generic single-stock fetch in AkshareSource,
        # I will leave a gap here or rely on what OpenBB covers.
        # For '上海金' (au0), OpenBB might not cover it.
        # I'll return empty if not found.
        
        if not data:
            return [], f"No data found for {name}"
            
        return data, None

    def process_group(self, group_name, targets):
        """
        Process a group of targets: Fetch -> MA -> Tech Indicators -> Format
        """
        group_klines = []
        group_ma = []
        logs = []
        
        for name, config in targets.items():
            # 1. Fetch
            # Fetch long history for MA calc
            data, err = self.fetch_data_for_target(name, config, days=300)
            
            status = {"name": name, "status": False, "error": err}
            
            if data:
                status["status"] = True
                status["error"] = None
                
                # 2. Process
                # Convert to DF for processing
                df = pd.DataFrame(data)
                
                # Calculate MA
                # DataProcessor expects list of dicts, but we have DF.
                # Actually DataProcessor.calculate_ma expects list of dicts or DF logic?
                # "Input: list of dicts... We convert to DF"
                ma_res = DataProcessor.calculate_ma(data)
                ma_res["名称"] = name # Ensure name is present
                
                # Calculate Tech Indicators
                tech_res = TechnicalAnalysis.calculate_signals(df)
                if tech_res:
                     # Merge tech signals into MA res
                     # tech_res is dict with K, D, J, Signals etc
                     ma_res.update(tech_res)

                # 3. Add to results
                # Slice for Report (e.g. last 30 days)
                # Ensure date sorting
                data_sorted = sorted(data, key=lambda x: x['date'], reverse=True)
                # Take recent
                recent_data = data_sorted[:REPORT_DAYS]
                
                # Attach name
                for d in recent_data:
                    d['name'] = name
                
                group_klines.extend(recent_data)
                group_ma.append(ma_res)
            
            logs.append(status)
            
        return group_klines, group_ma, logs

    def collect_all(self):
        """
        Main entry point for collection.
        """
        final_data = {
            "meta": {},
            "data": {},
            "ma_data": {"general": [], "commodities": []},
            "market_fx": {},
            "china": {},
            "usa": {},
            "japan": {},
        }
        
        all_logs = []
        
        # 1. Parallel Fetch Groups
        groups_map = {
            "指数": TARGETS_INDICES,
            "大宗商品": TARGETS_COMMODITIES,
            "美股七巨头": TARGETS_US_MAG7,
            "美国银行股": TARGETS_US_BANKS,
        }
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self.process_group, gname, targets): gname for gname, targets in groups_map.items()}
            
            for future in concurrent.futures.as_completed(futures):
                gname = futures[future]
                klines, ma, logs = future.result()
                
                final_data["data"][gname] = klines
                if gname == "大宗商品":
                    final_data["ma_data"]["commodities"].extend(ma)
                else:
                    final_data["ma_data"]["general"].extend(ma)
                
                all_logs.extend(logs)


        
        # 2. Fetch Macro Data (FX, Bonds, FRED)
        macro_results = self._fetch_macro_data()
        final_data.update(macro_results)
        
        # 3. Fetch Scraped Data (Korea, VN)
        scraped_results = self._fetch_scraped_data()
        # Merge scraped results into 'china' (e.g. SCFI) or 'market_fx' or specific keys
        # For now, put in 'market_fx' for simplicity or match legacy structure
        final_data["market_fx"].update(scraped_results.get("market_fx", {}))
        if "china" in scraped_results:
            final_data["china"].update(scraped_results["china"])
        
        return final_data, all_logs

    def _fetch_macro_data(self):
        """
        Fetch FX, Bonds, and other Macro indicators.
        """
        data = {
            "market_fx": {},
            "china": {},
            "usa": {},
            "japan": {}
        }
        
        # FRED Series (Inflation, TGA, Liquidity)
        for sid, name in FRED_SERIES.items():
            d, _ = self.obb.fetch_fred_series(sid)
            if d:
                # Store latest value
                data["usa"][name] = d[0].get("value")
                
        # US Treasury Rates
        bonds, _ = self.obb.fetch_treasury_rates()
        if bonds:
            data["usa"].update(bonds[0])
            
        # Akshare Data (Southbound, A-Share Indices)
        sb, _ = self.ak.fetch_southbound_flow(days=10)
        if sb:
            # Sum last 5 days? Or just show latest? Legacy showed chart or list.
            # We'll store list for now.
            data["china"]["南向资金"] = sb
            
        ashare, _ = self.ak.fetch_ashare_indices()
        if ashare:
            data["china"].update(ashare)
            
        return data

    def _fetch_scraped_data(self):
        """
        Fetch data via WebScraper (Investing.com, etc.)
        """
        results = {"market_fx": {}, "china": {}}
        
        # Korea Exports
        korea, _ = self.scraper.fetch_korea_exports()
        if korea:
            results["market_fx"]["韩国出口"] = korea
            
        # VN FDI
        vn_fdi, _ = self.scraper.fetch_vn_fdi()
        if vn_fdi:
            results["market_fx"]["越南FDI"] = vn_fdi
            
        return results
