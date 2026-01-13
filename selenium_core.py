# selenium_core.py
# -----------------------------------------------------------------------------
# DeepSeek Finance Project - Selenium Scraper Core Logic
# -----------------------------------------------------------------------------

from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.webdriver.chrome.options import Options
import selenium_scrapers_investing
import selenium_scrapers_misc

class MacroDataScraper:
    def __init__(self):
        # 目标数据源配置
        self.targets = {
            "中国_CPI": "https://data.eastmoney.com/cjsj/cpi.html",
            "中国_PMI": "https://data.eastmoney.com/cjsj/pmi.html",
            "中国_PPI": "https://data.eastmoney.com/cjsj/ppi.html",
            "中国_货币供应量": "https://data.eastmoney.com/cjsj/hbgyl.html",
            "中国_LPR": "https://data.eastmoney.com/cjsj/globalRateLPR.html",
            "美国_ISM制造业PMI": "https://data.eastmoney.com/cjsj/foreign_0_0.html",
            "美国_ISM非制造业指数": "https://data.eastmoney.com/cjsj/foreign_0_1.html",
            "美国_非农就业": "https://data.eastmoney.com/cjsj/foreign_0_2.html",
            "美国_核心零售销售月率": "https://data.eastmoney.com/cjsj/foreign_0_9.html",
            "美国_利率决议": "https://data.eastmoney.com/cjsj/foreign_8_0.html",
            "日本_央行利率决议": "https://data.eastmoney.com/cjsj/foreign_3_0.html",
            "恒生医疗保健指数": "https://cn.investing.com/indices/hang-seng-healthcare-historical-data",
            "CNN_FearGreed": "https://edition.cnn.com/markets/fear-and-greed",
            "CBOE_PutCallRatio": "https://www.cboe.com/us/options/market_statistics/daily/",
            "Fed_Rate_Monitor": "https://www.investing.com/central-banks/fed-rate-monitor",
            "CCFI_运价指数": "https://www.sse.net.cn/index/singleIndex?indexType=ccfi",
            "BDI_波罗的海指数": "https://www.investing.com/indices/baltic-dry-historical-data",
            "USA_Initial_Jobless": "https://www.investing.com/economic-calendar/initial-jobless-claims-294",
            "CBOE_SKEW": "https://www.investing.com/indices/cboe-skew-historical-data",
            "ICE_BofA_HighYield": "https://www.investing.com/indices/ice-bofa-us-high-yield-historical-data",
            "Insider_BuySell_Ratio_USA": "https://www.gurufocus.com/economic_indicators/4359/insider-buysell-ratio-usa-overall-market",
            "USA_ISM_New_Orders": "https://www.investing.com/economic-calendar/ism-manufacturing-new-orders-index-1483"
        }

        self.key_mapping = {
            "中国_CPI": ("china", "CPI"),
            "中国_PMI": ("china", "PMI_制造业"),
            "中国_PPI": ("china", "PPI"),
            "中国_货币供应量": ("china", "货币供应量"),
            "中国_LPR": ("china", "LPR"),
            "美国_ISM制造业PMI": ("usa", "ISM_制造业PMI"),
            "美国_ISM非制造业指数": ("usa", "ISM_非制造业PMI"),
            "美国_非农就业": ("usa", "非农就业人数"),
            "美国_核心零售销售月率": ("usa", "零售销售月率"),
            "美国_利率决议": ("usa", "利率决议"),
            "日本_央行利率决议": ("japan", "央行利率"),
            "恒生医疗保健指数": ("hk", "恒生医疗保健指数"),
            "CNN_FearGreed": ("market_fx", "CNN_FearGreed"),
            "CBOE_PutCallRatio": ("market_fx", "CBOE_PutCallRatio"),
            "Fed_Rate_Monitor": ("usa", "Fed_Rate_Monitor"),
            "CCFI_运价指数": ("china", "CCFI_运价指数"),
            "BDI_波罗的海指数": ("market_fx", "BDI_波罗的海指数"),
            "USA_Initial_Jobless": ("usa", "Initial_Jobless_Claims"),
            "CBOE_SKEW": ("market_fx", "CBOE_SKEW"),
            "ICE_BofA_HighYield": ("market_fx", "ICE_BofA_HighYield"),
            "Insider_BuySell_Ratio_USA": ("usa", "Insider_BuySell_Ratio"),
            "USA_ISM_New_Orders": ("usa", "ISM_Manufacturing_New_Orders")
        }
        
        self.results = {}
        self.status_logs = []
        
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--log-level=3")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        self.chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36')
        self.chrome_options.page_load_strategy = 'eager'
        prefs = {"profile.managed_default_content_settings.images": 2}
        self.chrome_options.add_experimental_option("prefs", prefs)
        
        self.output_path = "OnlineReport.json"

    def fetch_single_source(self, name, url):
        """
        调度器：根据 name 分发到具体的 scraper 函数
        """
        # 1. Investing.com 常规历史数据
        if name == "恒生医疗保健指数":
            return selenium_scrapers_investing.fetch_investing_source(name, url, self.chrome_options)
        
        # Investing.com 近 10 天数据组
        if name in ["BDI_波罗的海指数", "CBOE_SKEW", "ICE_BofA_HighYield"]:
            return selenium_scrapers_investing.fetch_investing_source(name, url, self.chrome_options, days_to_keep=10)

        # 2. Investing.com 财经日历数据
        if name == "USA_Initial_Jobless":
            return selenium_scrapers_investing.fetch_investing_economic_calendar(name, url, self.chrome_options, days_to_keep=150)
        
        if name == "USA_ISM_New_Orders":
            return selenium_scrapers_investing.fetch_investing_economic_calendar(name, url, self.chrome_options, days_to_keep=365)
        
        if name == "Fed_Rate_Monitor":
            return selenium_scrapers_investing.fetch_fed_rate_monitor(name, url, self.chrome_options)

        # 3. 专用抓取逻辑 (其他来源)
        if name == "CNN_FearGreed":
            return selenium_scrapers_misc.fetch_cnn_fear_greed(name, url, self.chrome_options)
            
        if name == "CBOE_PutCallRatio":
            return selenium_scrapers_misc.fetch_cboe_data(name, url, self.chrome_options)
            
        if name == "CCFI_运价指数":
            return selenium_scrapers_misc.fetch_ccfi_data(name, url, self.chrome_options)
            
        if name == "Insider_BuySell_Ratio_USA":
            return selenium_scrapers_misc.fetch_gurufocus_insider_ratio(name, url, self.chrome_options)

        # 4. 默认通用抓取 (Eastmoney 等)
        days_to_keep = 30 if "南向资金" in name else 180
        return selenium_scrapers_misc.fetch_generic_source(name, url, self.chrome_options, days_to_keep)

    def run_concurrent(self):
        print("🚀 [Scraper] 正在并发抓取宏观数据 (Workers=2)...")
        self.status_logs = []
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_name = {
                executor.submit(self.fetch_single_source, name, url): name 
                for name, url in self.targets.items()
            }
            for future in as_completed(future_to_name):
                name, data, error_msg = future.result()
                if not error_msg:
                    self.results[name] = data
                    self.status_logs.append({'name': name, 'status': True, 'error': None})
                else:
                    self.results[name] = []
                    self.status_logs.append({'name': name, 'status': False, 'error': error_msg})
                    
        return self.results, self.status_logs

    def organize_data(self):
        nested_data = {
            "china": {},
            "usa": {},
            "japan": {},
            "hk": {},
            "market_fx": {}
        }
        
        for old_key, data_list in self.results.items():
            if not data_list:
                continue
            if old_key in self.key_mapping:
                country_key, metric_key = self.key_mapping[old_key]
                if country_key not in nested_data:
                    nested_data[country_key] = {}
                nested_data[country_key][metric_key] = data_list
        
        return nested_data

    def get_data_dict(self):
        self.run_concurrent()
        return self.organize_data(), self.status_logs