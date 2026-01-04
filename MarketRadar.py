import os
import pandas as pd
import akshare as ak
import yfinance as yf
import requests
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # 新增: 时区处理
import logging
import warnings
import socket
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

# === 引入工具库 ===
import utils

# === 邮件相关库 ===
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# ================= 稳定性增强设置 (核弹级防卡死) =================
# 1. 强制 Monkey Patch: 修改 requests 底层，强制所有请求带上超时
_original_request = requests.Session.request

def _patched_request(self, method, url, *args, **kwargs):
    # 强制设置超时时间为 10 秒，防止 SSL 握手或 DNS 解析僵死
    if 'timeout' not in kwargs or kwargs['timeout'] is None:
        kwargs['timeout'] = 10
    return _original_request(self, method, url, *args, **kwargs)

requests.Session.request = _patched_request

# 2. 设置全局网络底层超时
socket.setdefaulttimeout(10)

# 忽略 YFinance 和 Pandas 的部分警告
warnings.filterwarnings("ignore")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ================= 配置区域 =================

# --- 邮件配置 (已修复为 QQ 邮箱标准 SSL 配置) ---
ENABLE_EMAIL = True               
SMTP_SERVER = "smtp.qq.com"       
SMTP_PORT = 465                   # QQ邮箱推荐使用 465 (SSL)

# === 关键：从环境变量获取，适配 GitHub Actions ===
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")       
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD") 
RECEIVER_EMAIL = os.environ.get("RECEIVER_EMAIL")   

# --- 本地测试防呆判断 ---
if not SENDER_EMAIL:
    print("⚠️ 警告: 未设置 SENDER_EMAIL 环境变量，邮件发送功能可能受限。")

# --- 数据查询时间段 (强制北京时间) ---
TZ_CN = ZoneInfo("Asia/Shanghai")
NOW_CN = datetime.now(TZ_CN)

# REPORT_START_DATE: 报告中K线展示的起始时间 (动态设置：过去20天)
REPORT_START_DATE = (NOW_CN - timedelta(days=20)).strftime("%Y-%m-%d")

# FETCH_START_DATE: API 实际拉取的起始时间 (回溯500天，确保能计算 MA250 年线)
FETCH_START_DATE = (NOW_CN - timedelta(days=500)).strftime("%Y-%m-%d")
END_DATE = NOW_CN.strftime("%Y-%m-%d")

# ------------------------------------------------
# 任务组 1: 全球市场 (指数.json)
# ------------------------------------------------
TARGETS_GLOBAL = {
    # [修改] 纳斯达克代码变更为 NDX (纳指100)
    "纳斯达克":     {"ak": ".NDX",    "yf": "^NDX",     "type": "index_us"},
    "标普500":      {"ak": ".INX",    "yf": "^GSPC",    "type": "index_us"},
    "恒生科技":     {"ak": "HSTECH",  "yf": "^HSTECH",  "type": "index_hk"},
    "恒生指数":     {"ak": "HSI",     "yf": "^HSI",     "type": "index_hk"},
    # [修改] 移除越南胡志明指数，交由 Step 4 的 fetch_data 爬虫专门处理，避免 YFinance 报错
    "黄金(COMEX)":  {"ak": "GC",      "yf": "GC=F",     "type": "future_foreign"},  
    "白银(COMEX)":  {"ak": "SI",      "yf": "SI=F",     "type": "future_foreign"},  
    "铜(COMEX)":    {"ak": "HG",      "yf": "HG=F",     "type": "future_foreign"}, 
    # [配置] 上海金改用期货主力合约 (au0)
    "上海金":       {"ak": "au0",     "yf": None,       "type": "future_zh_sina"}, 
    "VNM(ETF)":    {"ak": "VNM",      "yf": "VNM",      "type": "stock_us"},     
}

# ------------------------------------------------
# 任务组 2: 恒生科技主要成份股 (Top 20)
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

# ------------------------------------------------
# 任务组 3: 越南十大股票
# ------------------------------------------------
TARGETS_VIETNAM_TOP10 = {
    "越南繁荣银行(VPB)":    {"ak": None, "yf": "VPB.VN", "type": "stock_vn"},
    "军队商业银行(MBB)":    {"ak": None, "yf": "MBB.VN", "type": "stock_vn"},
    "和发集团(HPG)":        {"ak": None, "yf": "HPG.VN", "type": "stock_vn"},
    "移动世界(MWG)":        {"ak": None, "yf": "MWG.VN", "type": "stock_vn"},
    "FPT公司(FPT)":         {"ak": None, "yf": "FPT.VN", "type": "stock_vn"},
    "西贡商信(STB)":        {"ak": None, "yf": "STB.VN", "type": "stock_vn"},
    "胡志明发展银行(HDB)":  {"ak": None, "yf": "HDB.VN", "type": "stock_vn"},
    "科技商业银行(TCB)":    {"ak": None, "yf": "TCB.VN", "type": "stock_vn"},
    "Vingroup(VIC)":       {"ak": None, "yf": "VIC.VN", "type": "stock_vn"},
    "Vinhomes(VHM)":       {"ak": None, "yf": "VHM.VN", "type": "stock_vn"},
}

# ------------------------------------------------
# 任务组 4: 美股七巨头
# ------------------------------------------------
TARGETS_US_MAG7 = {
    "苹果(AAPL)":    {"ak": None, "yf": "AAPL",  "type": "stock_us"},
    "微软(MSFT)":    {"ak": None, "yf": "MSFT",  "type": "stock_us"},
    "谷歌(GOOGL)":   {"ak": None, "yf": "GOOGL", "type": "stock_us"},
    "亚马逊(AMZN)":  {"ak": None, "yf": "AMZN",  "type": "stock_us"},
    "英伟达(NVDA)":  {"ak": None, "yf": "NVDA",  "type": "stock_us"},
    "Meta(META)":    {"ak": None, "yf": "META",  "type": "stock_us"},
    "特斯拉(TSLA)":  {"ak": None, "yf": "TSLA",  "type": "stock_us"},
}

# ------------------------------------------------
# 任务组 5: 港股创新药
# ------------------------------------------------
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

# ------------------------------------------------
# 任务组 6: 恒生医疗保健指数 (已确保状态追踪)
# ------------------------------------------------
TARGETS_HK_HEALTHCARE = {
    "恒生医疗保健指数": {"ak": "HSHCI", "yf": "^HSHCI", "type": "index_hk"},
}

# ------------------------------------------------
# [新增] 任务组 7: 科创50 ETF (修正代码为588000)
# ------------------------------------------------
TARGETS_STAR50_ETF = {
    "科创50ETF": {"ak": "588000", "yf": "588000.SS", "type": "etf_zh"},
}

# ------------------------------------------------
# [新增] 任务组 8: 科创50 持仓股
# ------------------------------------------------
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

# 环境变量 (参考 data_provider.py 的命名)
ENV_KEYS = {
    "FMP": os.environ.get("FMP_API_Key"),
}

# ===========================================

class MarketFetcher:
    def __init__(self):
        self.session = requests.Session()
    
    def normalize_df(self, df, name):
        """统一清洗K线数据格式"""
        if df.empty: return df
        
        # 统一列名
        df.columns = [c.lower() for c in df.columns]
        
        # 处理日期列
        if 'date' not in df.columns and '日期' in df.columns:
            df.rename(columns={'日期': 'date'}, inplace=True)
        
        # 处理 AkShare 中文列名映射
        rename_map = {
            '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume', '交易量': 'volume', '持仓量': 'open_interest',
            '开盘价': 'open', '收盘价': 'close', '最高价': 'high', '最低价': 'low', 
            'date': 'date' 
        }
        df.rename(columns=rename_map, inplace=True)
        
        # 确保包含必要列
        required_cols = ['date', 'name', 'open', 'close', 'high', 'low', 'volume']
        
        # 确保日期格式为 datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # 填充缺失列
        for col in required_cols:
            if col not in df.columns:
                if col == 'name':
                     df['name'] = name
                else:
                     df[col] = 0.0
                
        if 'name' not in df.columns:
            df['name'] = name
        
        # 处理可能的无效数值 (字符串转数字)
        cols_to_numeric = ['open', 'close', 'high', 'low', 'volume']
        for col in cols_to_numeric:
            if col in df.columns and df[col].dtype == object:
                 df[col] = df[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce')

        return df[['date', 'name', 'open', 'close', 'high', 'low', 'volume']]

    def fetch_akshare(self, symbol, asset_type):
        """尝试从 AkShare 获取 K线 (带5次重试)"""
        if not symbol: return pd.DataFrame()
        
        max_retries = 5
        
        for i in range(max_retries):
            retry_msg = f" [重试{i}]" if i > 0 else ""
            print(f"   ⚡ [AkShare] 请求: {symbol} ({asset_type}){retry_msg} ...", end="", flush=True)

            try:
                df = pd.DataFrame()
                
                # 预处理日期：部分AkShare接口需要 YYYYMMDD 格式
                start_date_clean = FETCH_START_DATE.replace("-", "")
                end_date_clean = END_DATE.replace("-", "")

                if asset_type == "index_us":
                    df = ak.index_us_stock_sina(symbol=symbol)
                elif asset_type == "index_hk":
                    df = ak.stock_hk_index_daily_sina(symbol=symbol)
                elif asset_type == "gold_cn":
                    df = ak.spot_hist_sge(symbol=symbol)
                elif asset_type == "future_foreign":
                    df = ak.futures_foreign_hist(symbol=symbol)
                elif asset_type == "stock_hk":
                    df = ak.stock_hk_daily(symbol=symbol, adjust="qfq")
                elif asset_type == "stock_vn":
                    try:
                        df = ak.stock_vn_hist(symbol=symbol)
                    except:
                        df = pd.DataFrame()
                elif asset_type == "stock_us":
                    df = ak.stock_us_daily(symbol=symbol, adjust="qfq")
                elif asset_type == "future_zh_sina":
                    df = ak.futures_main_sina(symbol=symbol)
                # [新增] A股 ETF 基金 (如科创50ETF 588000)
                elif asset_type == "etf_zh":
                    df = ak.fund_etf_hist_em(symbol=symbol, period="daily", start_date=start_date_clean, end_date=end_date_clean, adjust="qfq")
                # [新增] A股 股票 (如 688981)
                elif asset_type == "stock_zh_a":
                    df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date_clean, end_date=end_date_clean, adjust="qfq")
                
                if not df.empty:
                    print(" ✅")
                    return df
                else:
                    print(" ❌ (空数据)")
                    return pd.DataFrame()

            except Exception as e:
                print(f" ❌ (Err: {str(e)[:15]})")
                if i < max_retries - 1:
                    time.sleep(2)
                continue
        
        print(" ❌ (AkShare多次重试失败, 放弃)")
        return pd.DataFrame()

    def fetch_yfinance(self, symbol):
        """尝试从 Yahoo Finance 获取 K线 (带5次重试)"""
        if not symbol: return pd.DataFrame()
        
        max_retries = 5
        
        for i in range(max_retries):
            retry_msg = f" [重试{i}]" if i > 0 else ""
            print(f"   ⚡ [YFinance] 请求: {symbol}{retry_msg} ...", end="", flush=True)
            
            try:
                # 使用 FETCH_START_DATE (回溯500天) 以确保均线计算正确
                df = yf.download(symbol, start=FETCH_START_DATE, end=END_DATE, progress=False, auto_adjust=False)
                if not df.empty:
                    df = df.reset_index()
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)
                    print(" ✅")
                    return df
                else:
                    print(" ❌ (空数据)")
                    return pd.DataFrame()
            except Exception as e:
                print(f" ❌ (Err: {str(e)[:15]})")
                if i < max_retries - 1:
                    time.sleep(2)
                continue

        print(" ❌ (YFinance多次重试失败, 放弃)")
        return pd.DataFrame()

    def fetch_fmp(self, name):
        """尝试从 FMP 获取 (商业API备选)"""
        key = ENV_KEYS.get("FMP")
        if not key: return pd.DataFrame()
        
        symbol_map = {
            "纳斯达克": "^IXIC", "标普500": "^GSPC", 
            "黄金(COMEX)": "GCUSD", "VNM(ETF)": "VNM",
            "越南胡志明指数": "^VNINDEX"
        }
        symbol = symbol_map.get(name)
        
        if not symbol: return pd.DataFrame()

        print(f"   ⚡ [FMP] 请求: {symbol} ...", end="", flush=True)
        try:
            # 使用 FETCH_START_DATE
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={FETCH_START_DATE}&to={END_DATE}&apikey={key}"
            res = requests.get(url, timeout=10) 
            data = res.json()
            if "historical" in data:
                df = pd.DataFrame(data["historical"])
                print(" ✅")
                return df
        except:
            print(" ❌")
        return pd.DataFrame()

    def get_kline_data(self, name, config):
        """核心逻辑：责任链模式获取 K线数据"""
        print(f"正在获取 K线 [{name}] ...")
        
        # === 稳定性增强：请求前随机休眠，防止触发反爬虫机制 ===
        time.sleep(random.uniform(1.0, 3.0))
        
        # 1. 尝试 AkShare (函数内自带重试机制)
        # 只有当配置了 ak 符号时才调用
        df = pd.DataFrame()
        if config.get("ak"):
            df = self.fetch_akshare(config.get("ak"), config.get("type"))
            df = self.normalize_df(df, name)
        
        # 2. 失败或未配置 AkShare 则尝试 YFinance
        if df.empty:
            df = self.fetch_yfinance(config.get("yf"))
            df = self.normalize_df(df, name)

        # 3. 失败则尝试 FMP
        if df.empty:
            df = self.fetch_fmp(name)
            df = self.normalize_df(df, name)
            
        return df

def fetch_group_data(fetcher, targets, group_name):
    """
    通用函数：返回 (K线数据列表, 均线数据列表, 状态日志列表)
    """
    print(f"\n🚀 开始处理任务组: {group_name} (并发模式)")
    
    kline_list = []
    ma_list = []
    status_logs = []
    
    # 定义单个任务函数 (用于线程池)
    def fetch_task(name, config):
        try:
            # 1. 获取长周期数据 (500天+)
            df = fetcher.get_kline_data(name, config)
            if df.empty:
                return None, None, {'name': name, 'status': False, 'error': "Data source returned empty after retries"}
            
            # 2. 确保按照日期排序
            df = df.sort_values(by='date', ascending=True)

            # 3. 计算均线 (基于长周期数据)
            ma_info_list = utils.calculate_ma(df) 
            ma_info = ma_info_list[0] if ma_info_list else None

            # 4. 切片为用户配置的短周期 (用于展示 K线)
            # 使用 REPORT_START_DATE 进行过滤，防止JSON过大
            df_slice = df[(df['date'] >= pd.to_datetime(REPORT_START_DATE)) & (df['date'] <= pd.to_datetime(END_DATE))].copy()
            
            if df_slice.empty:
                return None, ma_info, {'name': name, 'status': True, 'error': None} # 均线计算成功，只是展示区间无数据，算成功
            
            # 转换日期格式
            df_slice['date'] = df_slice['date'].dt.strftime('%Y-%m-%d')
            kline_records = df_slice.to_dict(orient='records')
            
            return kline_records, ma_info, {'name': name, 'status': True, 'error': None}

        except Exception as e:
            print(f"❌ 任务 {name} 异常: {e}")
            return None, None, {'name': name, 'status': False, 'error': str(e)}

    # 使用 ThreadPoolExecutor 进行并发
    with ThreadPoolExecutor(max_workers=4) as executor:
        # 提交所有任务
        future_to_name = {executor.submit(fetch_task, name, config): name for name, config in targets.items()}
        
        # 获取结果
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                # 设置 15 秒超时
                result = future.result(timeout=15)
                klines, ma, status = result # unpack result
                
                status_logs.append(status)
                
                if klines:
                    kline_list.extend(klines)
                else:
                    print(f"⚠️ 警告: 无法获取 {name} 的K线数据")
                
                if ma:
                    ma_list.append(ma)
                    
            except TimeoutError:
                print(f" 💀 严重超时: 获取 {name} 超过15秒无响应，强制跳过！")
                status_logs.append({'name': name, 'status': False, 'error': "Thread timed out (15s)"})
            except Exception as e:
                print(f"❌ 处理 {name} 结果时出错: {e}")
                status_logs.append({'name': name, 'status': False, 'error': f"Processing error: {str(e)}"})

    # 排序
    if kline_list:
        temp_df = pd.DataFrame(kline_list)
        temp_df.sort_values(by=['date', 'name'], ascending=[False, True], inplace=True)
        final_kline_data = temp_df.to_dict(orient='records')
    else:
        final_kline_data = []

    return final_kline_data, ma_list, status_logs


def send_email(subject, body, attachment_files):
    """
    发送带有多个附件的邮件 (QQ邮箱使用 SMTP_SSL:465)
    """
    # 检查必要配置是否齐全
    if not ENABLE_EMAIL:
        print("\n🔕 邮件功能已关闭，跳过发送。")
        return
    
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("\n❌ 错误：未检测到 SENDER_EMAIL 或 SENDER_PASSWORD 环境变量，无法发送邮件！")
        return

    print("\n📧 正在准备发送邮件...")
    
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    attachment_count = 0
    for file_path in attachment_files:
        if os.path.exists(file_path):
            try:
                with open(file_path, "rb") as attachment:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())
                
                encoders.encode_base64(part)
                
                # 正确设置中文文件名
                filename = os.path.basename(file_path)
                part.add_header('Content-Disposition', 'attachment', filename=filename)
                
                msg.attach(part)
                print(f"   📎 已添加附件: {filename}")
                attachment_count += 1
            except Exception as e:
                print(f"   ❌ 添加附件 {file_path} 失败: {e}")
        else:
            print(f"   ⚠️ 附件文件不存在: {file_path}")

    if attachment_count == 0:
        print("⚠️ 警告: 没有有效附件被添加，仍尝试发送邮件...")

    try:
        # === 核心修改：使用 SMTP_SSL 连接 465 端口 ===
        print(f"🚀 连接 SMTP 服务器 {SMTP_SERVER}:{SMTP_PORT} (SSL) 并发送...")
        
        server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT)
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
        server.quit()
        print("✅ 邮件发送成功！")
    except Exception as e:
        print(f"❌ 邮件发送失败: {e}")


def get_all_kline_data():
    """
    对外接口函数：执行所有K线抓取任务并返回 (data_collection, status_logs)
    """
    print(f"📅 多市场数据采集器 (MarketRadar - Module)")
    print(f"🕒 报告周期: {REPORT_START_DATE} 至 {END_DATE}")
    print(f"🕒 计算周期: {FETCH_START_DATE} 至 {END_DATE}")
    
    fetcher = MarketFetcher()
    
    # 汇总所有数据到一个大字典 (使用北京时间)
    all_data_collection = {
        "meta": {
            "generated_at": datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S"),
            "date_range": f"{REPORT_START_DATE} to {END_DATE}",
            "description": "Global Market Data Consolidated Report"
        },
        "data": {},
        "ma_data": [] # 专门存放均线数据
    }

    all_ma_data = []
    all_status_logs = []

    # 1. 抓取指数数据
    data_idx, ma_idx, logs_idx = fetch_group_data(fetcher, TARGETS_GLOBAL, "指数")
    all_data_collection["data"]["指数"] = data_idx
    all_ma_data.extend(ma_idx)
    all_status_logs.extend(logs_idx)

    # 2. 抓取恒生科技
    data_hstech, ma_hstech, logs_hstech = fetch_group_data(fetcher, TARGETS_HSTECH_TOP20, "恒生科技")
    all_data_collection["data"]["恒生科技"] = data_hstech
    all_ma_data.extend(ma_hstech)
    all_status_logs.extend(logs_hstech)
    
    # 3. 抓取新兴市场
    data_vn, ma_vn, logs_vn = fetch_group_data(fetcher, TARGETS_VIETNAM_TOP10, "新兴市场")
    all_data_collection["data"]["新兴市场"] = data_vn
    all_ma_data.extend(ma_vn)
    all_status_logs.extend(logs_vn)
    
    # 4. 抓取美股七巨头
    data_us, ma_us, logs_us = fetch_group_data(fetcher, TARGETS_US_MAG7, "美股七巨头")
    all_data_collection["data"]["美股七巨头"] = data_us
    all_ma_data.extend(ma_us)
    all_status_logs.extend(logs_us)
    
    # 5. 抓取港股创新药
    data_hk, ma_hk, logs_hk = fetch_group_data(fetcher, TARGETS_HK_PHARMA, "港股创新药")
    all_data_collection["data"]["港股创新药"] = data_hk
    all_ma_data.extend(ma_hk)
    all_status_logs.extend(logs_hk)
    
    # 6. 抓取恒生医疗保健指数
    data_hc, ma_hc, logs_hc = fetch_group_data(fetcher, TARGETS_HK_HEALTHCARE, "恒生医疗保健指数")
    all_data_collection["data"]["恒生医疗保健指数"] = data_hc
    all_ma_data.extend(ma_hc)
    all_status_logs.extend(logs_hc)

    # [新增] 7. 抓取科创50 ETF
    data_star_etf, ma_star_etf, logs_star_etf = fetch_group_data(fetcher, TARGETS_STAR50_ETF, "科创50ETF")
    all_data_collection["data"]["科创50ETF"] = data_star_etf
    all_ma_data.extend(ma_star_etf)
    all_status_logs.extend(logs_star_etf)

    # [新增] 8. 抓取科创50持仓
    data_star_holdings, ma_star_holdings, logs_star_holdings = fetch_group_data(fetcher, TARGETS_STAR50_HOLDINGS, "科创50持仓")
    all_data_collection["data"]["科创50持仓"] = data_star_holdings
    all_ma_data.extend(ma_star_holdings)
    all_status_logs.extend(logs_star_holdings)
    
    # 将汇总的均线数据存入
    all_data_collection["ma_data"] = all_ma_data
    
    print("\n🎉 K线数据抓取 & 均线计算 任务处理完成！")
    return all_data_collection, all_status_logs

if __name__ == "__main__":
    # 如果直接运行此脚本，执行默认逻辑并保存文件
    data, _ = get_all_kline_data()
    
    output_filename = "金融数据.json"
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    print(f"✅ 数据已保存至 {output_filename}")