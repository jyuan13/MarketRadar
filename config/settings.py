# config_manager.py
import os
import datetime
from zoneinfo import ZoneInfo

class ConfigManager:
    def __init__(self):
        self.setup_env()
        self.TZ_CN = ZoneInfo("Asia/Shanghai")
        self.NOW_CN = datetime.datetime.now(self.TZ_CN)
        
        # Date Ranges
        self.REPORT_DAYS = 20
        self.CALC_DAYS = 365 # Loop back for MA calc
        
        
        # Email Configuration
        self.EMAIL = {
            "enable": True,
            "sender_email": os.environ.get("SENDER_EMAIL"),
            "sender_password": os.environ.get("SENDER_PASSWORD"),
            "receiver_email": os.environ.get("RECEIVER_EMAIL"),
            "smtp_server": "smtp.qq.com",
            "smtp_port": 465
        }

        # --- Targets Configuration ---
        
        # 1. K-Lines Targets (Indices, Stocks, Commodities)
        self.TARGETS_KLINES = {
            "Indices": [
                {"name": "纳斯达克", "ak": ".NDX", "yf": "^NDX", "provider": "openbb"},
                {"name": "标普500", "ak": ".INX", "yf": "^GSPC", "provider": "openbb"},
                {"name": "恒生科技", "ak": "HSTECH", "yf": "HSTECH.HK", "provider": "openbb"},
                {"name": "恒生指数", "ak": "HSI", "yf": "^HSI", "provider": "openbb"},
                {"name": "VNM(ETF)", "ak": "VNM", "yf": "VNM", "provider": "openbb"},
                {"name": "XBI(ETF)", "ak": "XBI", "yf": "XBI", "provider": "openbb"},
                {"name": "上证指数", "ak": "sh000001", "provider": "akshare_index"},
                {"name": "深证成指", "ak": "sz399001", "provider": "akshare_index"},
                {"name": "创业板指", "ak": "sz399006", "provider": "akshare_index"},
                {"name": "沪深300", "ak": "sh000300", "provider": "akshare_index"},
            ],
            "Commodities": [
                {"name": "黄金(COMEX)", "yf": "GC=F", "provider": "openbb"},
                {"name": "白银(COMEX)", "yf": "SI=F", "provider": "openbb"},
                {"name": "铜(COMEX)", "yf": "HG=F", "provider": "openbb"},
                {"name": "原油(WTI)", "yf": "CL=F", "provider": "openbb"},
                {"name": "铀(URA)", "yf": "URA", "provider": "openbb"},
                # "上海金" logic is special in old code (akshare future), let's mark it
                {"name": "上海金", "ak": "au0", "provider": "akshare_future"} 
            ],
            "HSTECH_Components": [
                {"name": "美团-W", "yf": "3690.HK", "provider": "openbb"},
                {"name": "腾讯控股", "yf": "0700.HK", "provider": "openbb"},
                {"name": "小米集团-W", "yf": "1810.HK", "provider": "openbb"},
                {"name": "阿里巴巴-SW", "yf": "9988.HK", "provider": "openbb"},
                {"name": "理想汽车-W", "yf": "2015.HK", "provider": "openbb"},
                {"name": "快手-W", "yf": "1024.HK", "provider": "openbb"},
                {"name": "京东集团-SW", "yf": "9618.HK", "provider": "openbb"},
                {"name": "网易-S", "yf": "9999.HK", "provider": "openbb"},
                {"name": "百度集团-SW", "yf": "9888.HK", "provider": "openbb"},
                {"name": "携程集团-S", "yf": "9961.HK", "provider": "openbb"},
                {"name": "中芯国际", "yf": "0981.HK", "provider": "openbb"},
                {"name": "海尔智家", "yf": "6690.HK", "provider": "openbb"},
                {"name": "比亚迪电子", "yf": "0285.HK", "provider": "openbb"},
                {"name": "舜宇光学科技", "yf": "2382.HK", "provider": "openbb"},
                {"name": "阅文集团", "yf": "0772.HK", "provider": "openbb"},
                {"name": "商汤-W", "yf": "0020.HK", "provider": "openbb"},
                {"name": "金山软件", "yf": "3888.HK", "provider": "openbb"},
                {"name": "华虹半导体", "yf": "1347.HK", "provider": "openbb"},
                {"name": "金蝶国际", "yf": "0268.HK", "provider": "openbb"},
                {"name": "同程旅行", "yf": "0780.HK", "provider": "openbb"},
            ],
            "Vietnam_Top10": [
                {"name": "越南繁荣银行(VPB)", "yf": "VPB.VN", "provider": "openbb"},
                {"name": "军队商业银行(MBB)", "yf": "MBB.VN", "provider": "openbb"},
                {"name": "和发集团(HPG)", "yf": "HPG.VN", "provider": "openbb"},
                {"name": "移动世界(MWG)", "yf": "MWG.VN", "provider": "openbb"},
                {"name": "FPT公司(FPT)", "yf": "FPT.VN", "provider": "openbb"},
                {"name": "西贡商信(STB)", "yf": "STB.VN", "provider": "openbb"},
                {"name": "胡志明发展银行(HDB)", "yf": "HDB.VN", "provider": "openbb"},
                {"name": "科技商业银行(TCB)", "yf": "TCB.VN", "provider": "openbb"},
                {"name": "Vingroup(VIC)", "yf": "VIC.VN", "provider": "openbb"},
                {"name": "Vinhomes(VHM)", "yf": "VHM.VN", "provider": "openbb"},
            ],
             "US_BigTech": [
                {"name": "苹果(AAPL)", "yf": "AAPL", "provider": "openbb"},
                {"name": "微软(MSFT)", "yf": "MSFT", "provider": "openbb"},
                {"name": "谷歌(GOOGL)", "yf": "GOOGL", "provider": "openbb"},
                {"name": "亚马逊(AMZN)", "yf": "AMZN", "provider": "openbb"},
                {"name": "英伟达(NVDA)", "yf": "NVDA", "provider": "openbb"},
                {"name": "Meta(META)", "yf": "META", "provider": "openbb"},
                {"name": "特斯拉(TSLA)", "yf": "TSLA", "provider": "openbb"},
                {"name": "台积电(TSM)", "yf": "TSM", "provider": "openbb"},
                {"name": "博通(AVGO)", "yf": "AVGO", "provider": "openbb"},
                {"name": "美光(MU)", "yf": "MU", "provider": "openbb"},
            ],
            "HK_Pharma": [
                {"name": "信达生物", "yf": "1801.HK", "provider": "openbb"},
                {"name": "百济神州", "yf": "6160.HK", "provider": "openbb"},
                {"name": "药明生物", "yf": "2269.HK", "provider": "openbb"},
                {"name": "康方生物", "yf": "9926.HK", "provider": "openbb"},
                {"name": "中国生物制药", "yf": "1177.HK", "provider": "openbb"},
                {"name": "石药集团", "yf": "1093.HK", "provider": "openbb"},
                {"name": "三生制药", "yf": "1530.HK", "provider": "openbb"},
                {"name": "药明康德", "yf": "2359.HK", "provider": "openbb"},
                {"name": "翰森制药", "yf": "3692.HK", "provider": "openbb"},
                {"name": "科伦博泰生物-B", "yf": "6990.HK", "provider": "openbb"},
            ],
            "US_Banks": [
                {"name": "摩根大通", "yf": "JPM", "provider": "openbb"},
                {"name": "美国银行", "yf": "BAC", "provider": "openbb"},
                {"name": "花旗集团", "yf": "C", "provider": "openbb"},
                {"name": "富国银行", "yf": "WFC", "provider": "openbb"},
                {"name": "高盛集团", "yf": "GS", "provider": "openbb"},
                {"name": "摩根士丹利", "yf": "MS", "provider": "openbb"},
            ],
             "Star50_Holdings": [
                {"name": "中芯国际", "ak": "688981", "provider": "akshare_stock_a"},
                {"name": "海光信息", "ak": "688041", "provider": "akshare_stock_a"},
                {"name": "寒武纪", "ak": "688256", "provider": "akshare_stock_a"},
                {"name": "澜起科技", "ak": "688008", "provider": "akshare_stock_a"},
                {"name": "中微公司", "ak": "688012", "provider": "akshare_stock_a"},
                {"name": "联影医疗", "ak": "688271", "provider": "akshare_stock_a"},
                {"name": "金山办公", "ak": "688111", "provider": "akshare_stock_a"},
                {"name": "芯原股份", "ak": "688521", "provider": "akshare_stock_a"},
                {"name": "石头科技", "ak": "688169", "provider": "akshare_stock_a"},
                {"name": "传音控股", "ak": "688036", "provider": "akshare_stock_a"},
                {"name": "沪硅产业", "ak": "688126", "provider": "akshare_stock_a"},
                {"name": "华海清科", "ak": "688120", "provider": "akshare_stock_a"},
                {"name": "晶晨股份", "ak": "688099", "provider": "akshare_stock_a"},
                {"name": "拓荆科技", "ak": "688072", "provider": "akshare_stock_a"},
                {"name": "恒玄科技", "ak": "688608", "provider": "akshare_stock_a"},
                {"name": "中控技术", "ak": "688777", "provider": "akshare_stock_a"},
                {"name": "佰维存储", "ak": "688525", "provider": "akshare_stock_a"},
                {"name": "思特威", "ak": "688213", "provider": "akshare_stock_a"},
                {"name": "芯联集成", "ak": "688469", "provider": "akshare_stock_a"},
                {"name": "百利天恒", "ak": "688506", "provider": "akshare_stock_a"},
            ],
             "Star50_ETF": [
                {"name": "科创50ETF", "ak": "588000", "provider": "akshare_etf"},
            ]
        }

        # 2. Macro / Online Targets (Selenium)
        self.TARGETS_SELENIUM = {
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
            "Insider_BuySell_Ratio_USA": "https://www.gurufocus.com/economic_indicators/4359/insider-buysell-ratio-usa-overall-market",
            "USA_ISM_New_Orders": "https://www.investing.com/economic-calendar/ism-manufacturing-new-orders-index-1483",
            "韩国_出口同比": "https://www.investing.com/economic-calendar/south-korean-export-growth-1316",
            "越南_FDI": "https://www.investing.com/economic-calendar/vietnamese-foreign-direct-investment-(usd)-1857"
        }

        # 3. FRED Targets
        self.TARGETS_FRED = {
            "10年期TIPS实际利率": "DFII10",
            "高收益债利差(HY OAS)": "BAMLH0A0HYM2",
            "10年盈亏平衡通胀率": "T10YIE",
            "TGA账户余额": "WTREGEN",
            "美联储ON RRP余额": "RRPONTSYD"
        }
        
    def setup_env(self):
        # Placeholder for dotenv setup if needed, currently assumes os.environ loaded
        pass
