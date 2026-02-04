import os
import requests
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging
import warnings
import socket
import market_core

# ================= 稳定性增强设置 =================
_original_request = requests.Session.request

def _patched_request(self, method, url, *args, **kwargs):
    if 'timeout' not in kwargs or kwargs['timeout'] is None:
        kwargs['timeout'] = 10
    return _original_request(self, method, url, *args, **kwargs)

requests.Session.request = _patched_request
socket.setdefaulttimeout(10)

warnings.filterwarnings("ignore")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ================= 配置区域 =================
ENABLE_EMAIL = True               
SMTP_SERVER = "smtp.qq.com"       
SMTP_PORT = 465                   
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")       
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD") 
RECEIVER_EMAIL = os.environ.get("RECEIVER_EMAIL")   

if not SENDER_EMAIL:
    print("⚠️ 警告: 未设置 SENDER_EMAIL 环境变量，邮件发送功能可能受限。")

TZ_CN = ZoneInfo("Asia/Shanghai")
NOW_CN = datetime.now(TZ_CN)
REPORT_START_DATE = (NOW_CN - timedelta(days=20)).strftime("%Y-%m-%d")
FETCH_START_DATE = (NOW_CN - timedelta(days=500)).strftime("%Y-%m-%d")
END_DATE = NOW_CN.strftime("%Y-%m-%d")

# ------------------------------------------------
# 任务组 1.1: 指数 (Indices)
# ------------------------------------------------
TARGETS_INDICES = {
    "纳斯达克":     {"ak": ".NDX",    "yf": "^NDX",     "type": "index_us"},
    "标普500":      {"ak": ".INX",    "yf": "^GSPC",    "type": "index_us"},
    "恒生科技":     {"ak": "HSTECH",  "yf": "^HSTECH",  "type": "index_hk"},
    "恒生指数":     {"ak": "HSI",     "yf": "^HSI",     "type": "index_hk"},
    # [新增] 美股生物科技 ETF (XBI)
    "XBI(ETF)":     {"ak": "XBI",     "yf": "XBI",      "type": "stock_us"},
}

# ------------------------------------------------
# 任务组 1.2: 大宗商品 (Commodities)
# ------------------------------------------------
TARGETS_COMMODITIES = {
    "黄金(COMEX)":  {"ak": "GC",      "yf": "GC=F",     "type": "future_foreign"},  
    "白银(COMEX)":  {"ak": "SI",      "yf": "SI=F",     "type": "future_foreign"},  
    "铜(COMEX)":    {"ak": "HG",      "yf": "HG=F",     "type": "future_foreign"}, 
    "上海金":       {"ak": "au0",     "yf": None,       "type": "future_zh_sina"}, 
    "原油(WTI)":    {"ak": "CL",      "yf": "CL=F",     "type": "future_foreign"},
    "铀(URA)":      {"ak": "URA",     "yf": "URA",      "type": "stock_us"},
}

# ------------------------------------------------
# 其他任务组
# ------------------------------------------------
TARGETS_HSTECH_TOP20 = {
    "美团-W":       {"ak": "03690", "yf": "3690.HK", "type": "stock_hk"},
    "腾讯控股":     {"ak": "00700", "yf": "0700.HK", "type": "stock_hk"},
    "小米集团-W":   {"ak": "01810", "yf": "1810.HK", "type": "stock_hk"},
    "阿里巴巴-SW":  {"ak": "09988", "yf": "9988.HK", "type": "stock_hk"},
    "理想汽车-W":   {"ak": "02015", "yf": "2015.HK", "type": "stock_hk"},
    "快手-W":       {"ak": "01024", "yf": "1024.HK", "type": "stock_hk"},
    "京东集团-SW":  {"ak": "09618", "yf": "9618.HK", "type": "stock_hk"},
    "网易-S":       {"ak": "09999", "yf": "9999.HK", "type": "stock_hk"},
    "百度集团-SW":  {"ak": "09888", "yf": "9888.HK", "type": "stock_hk"},
    "携程集团-S":   {"ak": "09961", "yf": "9961.HK", "type": "stock_hk"},
    "中芯国际":     {"ak": "00981", "yf": "0981.HK", "type": "stock_hk"},
    "海尔智家":     {"ak": "06690", "yf": "6690.HK", "type": "stock_hk"},
    "比亚迪电子":   {"ak": "00285", "yf": "0285.HK", "type": "stock_hk"},
    "舜宇光学科技": {"ak": "02382", "yf": "2382.HK", "type": "stock_hk"},
    "阅文集团":     {"ak": "00772", "yf": "0772.HK", "type": "stock_hk"},
    "商汤-W":       {"ak": "00020", "yf": "0020.HK", "type": "stock_hk"},
    "金山软件":     {"ak": "03888", "yf": "3888.HK", "type": "stock_hk"},
    "华虹半导体":   {"ak": "01347", "yf": "1347.HK", "type": "stock_hk"},
    "金蝶国际":     {"ak": "00268", "yf": "0268.HK", "type": "stock_hk"},
    "同程旅行":     {"ak": "00780", "yf": "0780.HK", "type": "stock_hk"},
}

# [Deleted] TARGETS_VIETNAM_TOP10 越南Top10持股配置已移除

TARGETS_US_MAG7 = {
    "苹果(AAPL)":    {"ak": None, "yf": "AAPL",  "type": "stock_us"},
    "微软(MSFT)":    {"ak": None, "yf": "MSFT",  "type": "stock_us"},
    "谷歌(GOOGL)":   {"ak": None, "yf": "GOOGL", "type": "stock_us"},
    "亚马逊(AMZN)":  {"ak": None, "yf": "AMZN",  "type": "stock_us"},
    "英伟达(NVDA)":  {"ak": None, "yf": "NVDA",  "type": "stock_us"},
    "Meta(META)":    {"ak": None, "yf": "META",  "type": "stock_us"},
    "特斯拉(TSLA)":  {"ak": None, "yf": "TSLA",  "type": "stock_us"},
    "台积电(TSM)":   {"ak": None, "yf": "TSM",   "type": "stock_us"},
    "博通(AVGO)":    {"ak": None, "yf": "AVGO",  "type": "stock_us"},
    "美光(MU)":      {"ak": None, "yf": "MU",    "type": "stock_us"},
}

TARGETS_HK_PHARMA = {
    "信达生物":       {"ak": "01801", "yf": "1801.HK", "type": "stock_hk"},
    "百济神州":       {"ak": "06160", "yf": "6160.HK", "type": "stock_hk"},
    "药明生物":       {"ak": "02269", "yf": "2269.HK", "type": "stock_hk"},
    "康方生物":       {"ak": "09926", "yf": "9926.HK", "type": "stock_hk"},
    "中国生物制药":   {"ak": "01177", "yf": "1177.HK", "type": "stock_hk"},
    "石药集团":       {"ak": "01093", "yf": "1093.HK", "type": "stock_hk"},
    "三生制药":       {"ak": "01530", "yf": "1530.HK", "type": "stock_hk"},
    "药明康德":       {"ak": "02359", "yf": "2359.HK", "type": "stock_hk"},
    "翰森制药":       {"ak": "03692", "yf": "3692.HK", "type": "stock_hk"},
    "科伦博泰生物-B": {"ak": "06990", "yf": "6990.HK", "type": "stock_hk"},
}

# [Deleted] TARGETS_HK_HEALTHCARE 已移除 (避免 AkShare 报错)

TARGETS_STAR50_ETF = {
    "科创50ETF": {"ak": "588000", "yf": "588000.SS", "type": "etf_zh"},
}

TARGETS_STAR50_HOLDINGS = {
    "中芯国际": {"ak": "688981", "yf": "688981.SS", "type": "stock_zh_a"},
    "海光信息": {"ak": "688041", "yf": "688041.SS", "type": "stock_zh_a"},
    "寒武纪":   {"ak": "688256", "yf": "688256.SS", "type": "stock_zh_a"},
    "澜起科技": {"ak": "688008", "yf": "688008.SS", "type": "stock_zh_a"},
    "中微公司": {"ak": "688012", "yf": "688012.SS", "type": "stock_zh_a"},
    "联影医疗": {"ak": "688271", "yf": "688271.SS", "type": "stock_zh_a"},
    "金山办公": {"ak": "688111", "yf": "688111.SS", "type": "stock_zh_a"},
    "芯原股份": {"ak": "688521", "yf": "688521.SS", "type": "stock_zh_a"},
    "石头科技": {"ak": "688169", "yf": "688169.SS", "type": "stock_zh_a"},
    "传音控股": {"ak": "688036", "yf": "688036.SS", "type": "stock_zh_a"},
    "沪硅产业": {"ak": "688126", "yf": "688126.SS", "type": "stock_zh_a"},
    "华海清科": {"ak": "688120", "yf": "688120.SS", "type": "stock_zh_a"},
    "晶晨股份": {"ak": "688099", "yf": "688099.SS", "type": "stock_zh_a"},
    "拓荆科技": {"ak": "688072", "yf": "688072.SS", "type": "stock_zh_a"},
    "恒玄科技": {"ak": "688608", "yf": "688608.SS", "type": "stock_zh_a"},
    "中控技术": {"ak": "688777", "yf": "688777.SS", "type": "stock_zh_a"},
    "佰维存储": {"ak": "688525", "yf": "688525.SS", "type": "stock_zh_a"},
    "思特威":   {"ak": "688213", "yf": "688213.SS", "type": "stock_zh_a"},
    "芯联集成": {"ak": "688469", "yf": "688469.SS", "type": "stock_zh_a"},
    "百利天恒": {"ak": "688506", "yf": "688506.SS", "type": "stock_zh_a"},
}

def get_all_kline_data():
    """
    对外接口函数：执行所有K线抓取任务并返回 (data_collection, status_logs)
    """
    print(f"📅 多市场数据采集器 (MarketRadar - Module)")
    print(f"🕒 报告周期: {REPORT_START_DATE} 至 {END_DATE}")
    print(f"🕒 计算周期: {FETCH_START_DATE} 至 {END_DATE}")
    
    fetcher = market_core.MarketFetcher(FETCH_START_DATE, END_DATE)
    
    # 修改 ma_data 结构，分离 大宗商品 和 其他
    all_data_collection = {
        "meta": {
            "generated_at": datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S"),
            "date_range": f"{REPORT_START_DATE} to {END_DATE}",
            "description": "Global Market Data Consolidated Report"
        },
        "data": {},
        # 分离存储 MA 数据
        "ma_data": {
            "general": [],
            "commodities": []
        }
    }

    all_status_logs = []

    # 1.1 抓取指数数据 (Indices)
    data_idx, ma_idx, logs_idx = market_core.fetch_group_data(fetcher, TARGETS_INDICES, "指数", REPORT_START_DATE, END_DATE)
    all_data_collection["data"]["指数"] = data_idx
    all_data_collection["ma_data"]["general"].extend(ma_idx)
    all_status_logs.extend(logs_idx)

    # 1.2 抓取大宗商品 (Commodities)
    data_comm, ma_comm, logs_comm = market_core.fetch_group_data(fetcher, TARGETS_COMMODITIES, "大宗商品", REPORT_START_DATE, END_DATE)
    all_data_collection["data"]["大宗商品"] = data_comm
    all_data_collection["ma_data"]["commodities"].extend(ma_comm)
    all_status_logs.extend(logs_comm)

    # 2. 抓取恒生科技
    data_hstech, ma_hstech, logs_hstech = market_core.fetch_group_data(fetcher, TARGETS_HSTECH_TOP20, "恒生科技", REPORT_START_DATE, END_DATE)
    all_data_collection["data"]["恒生科技"] = data_hstech
    all_data_collection["ma_data"]["general"].extend(ma_hstech)
    all_status_logs.extend(logs_hstech)
    
    # [Deleted] 新兴市场(TARGETS_VIETNAM_TOP10)抓取逻辑已移除
    
    # 4. 抓取美股七巨头 + 扩充
    new_group_name = "美股七巨头+台积电&博通&美光"
    data_us, ma_us, logs_us = market_core.fetch_group_data(fetcher, TARGETS_US_MAG7, new_group_name, REPORT_START_DATE, END_DATE)
    all_data_collection["data"][new_group_name] = data_us
    all_data_collection["ma_data"]["general"].extend(ma_us)
    all_status_logs.extend(logs_us)
    
    # 5. 抓取港股创新药
    data_hk, ma_hk, logs_hk = market_core.fetch_group_data(fetcher, TARGETS_HK_PHARMA, "港股创新药", REPORT_START_DATE, END_DATE)
    all_data_collection["data"]["港股创新药"] = data_hk
    all_data_collection["ma_data"]["general"].extend(ma_hk)
    all_status_logs.extend(logs_hk)
    
    # [Deleted] 6. 抓取恒生医疗保健指数 (已移除)

    # 7. 抓取科创50 ETF
    data_star_etf, ma_star_etf, logs_star_etf = market_core.fetch_group_data(fetcher, TARGETS_STAR50_ETF, "科创50ETF", REPORT_START_DATE, END_DATE)
    all_data_collection["data"]["科创50ETF"] = data_star_etf
    all_data_collection["ma_data"]["general"].extend(ma_star_etf)
    all_status_logs.extend(logs_star_etf)

    # 8. 抓取科创50持仓
    data_star_holdings, ma_star_holdings, logs_star_holdings = market_core.fetch_group_data(fetcher, TARGETS_STAR50_HOLDINGS, "科创50持仓", REPORT_START_DATE, END_DATE)
    all_data_collection["data"]["科创50持仓"] = data_star_holdings
    all_data_collection["ma_data"]["general"].extend(ma_star_holdings)
    all_status_logs.extend(logs_star_holdings)
    
    print("\n🎉 K线数据抓取 & 均线计算 任务处理完成！")
    return all_data_collection, all_status_logs

def send_email(subject, body, attachment_files):
    market_core.send_email(subject, body, attachment_files, SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAIL, SMTP_SERVER, SMTP_PORT, ENABLE_EMAIL)

if __name__ == "__main__":
    data, _ = get_all_kline_data()
    output_filename = "金融数据.json"
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"✅ 数据已保存至 {output_filename}")