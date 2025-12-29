import os
import pandas as pd
import akshare as ak
import yfinance as yf
import requests
import json
from datetime import datetime, timedelta
import logging
import warnings
import socket
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

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
    print(" ❌ not SENDER_EMAIL")
    pass
if not SENDER_PASSWORD:
    print(" ❌ not SENDER_PASSWORD")
    pass
if not RECEIVER_EMAIL:
    print(" ❌ not RECEIVER_EMAIL")
    pass

# --- 数据查询时间段 ---
START_DATE = "2025-12-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

# ------------------------------------------------
# 任务组 1: 全球市场 (原有配置) -> 存为 指数.json
# ------------------------------------------------
TARGETS_GLOBAL = {
    "纳斯达克":     {"ak": ".IXIC",   "yf": "^IXIC",    "type": "index_us"},
    "标普500":      {"ak": ".INX",    "yf": "^GSPC",    "type": "index_us"},
    "恒生科技":     {"ak": "HSTECH",  "yf": "^HSTECH",  "type": "index_hk"},
    "恒生指数":     {"ak": "HSI",     "yf": "^HSI",     "type": "index_hk"},
    "黄金(COMEX)":  {"ak": "GC",      "yf": "GC=F",     "type": "future_foreign"},  
    "上海金":       {"ak": "Au99.99", "yf": None,       "type": "gold_cn"}, 
    "VNM(ETF)":    {"ak": None,      "yf": "VNM",      "type": "etf"},     
}

# ------------------------------------------------
# 任务组 2: 恒生科技主要成份股 (Top 20) -> 存为 恒生科技.json
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
# 任务组 3: 越南十大股票 -> 存为 新兴市场.json
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
# 任务组 4: 美股七巨头 -> 存为 美股七巨头.json
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
# 任务组 5: 港股创新药 -> 存为 港股创新药.json
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
        required_cols = ['date', 'open', 'close', 'high', 'low', 'volume']
        
        # 确保日期格式为 datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # 填充缺失列
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0.0
                
        df['name'] = name
        
        # 处理可能的无效数值 (字符串转数字)
        cols_to_numeric = ['open', 'close', 'high', 'low', 'volume']
        for col in cols_to_numeric:
            if col in df.columns and df[col].dtype == object:
                 df[col] = df[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce')

        return df[['date', 'name', 'open', 'close', 'high', 'low', 'volume']]

    def fetch_akshare(self, symbol, asset_type):
        """尝试从 AkShare 获取 K线 (带3次重试)"""
        if not symbol: return pd.DataFrame()
        
        max_retries = 3
        
        for i in range(max_retries):
            # 打印请求状态 (如果是重试，打印次数)
            retry_msg = f" [重试{i}]" if i > 0 else ""
            print(f"   ⚡ [AkShare] 请求: {symbol} ({asset_type}){retry_msg} ...", end="", flush=True)

            try:
                df = pd.DataFrame()
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
                
                if not df.empty:
                    print(" ✅")
                    return df
                else:
                    # 如果数据为空，不一定是报错，可能是真没数据，不需要重试，直接退出
                    print(" ❌ (空数据)")
                    return pd.DataFrame()

            except Exception as e:
                print(f" ❌ (Err: {str(e)[:15]})")
                if i < max_retries - 1:
                    time.sleep(2) # 失败后休息2秒再重试
                continue # 继续下一次循环
        
        print(" ❌ (AkShare多次重试失败, 放弃)")
        return pd.DataFrame()

    def fetch_yfinance(self, symbol):
        """尝试从 Yahoo Finance 获取 K线 (带3次重试)"""
        if not symbol: return pd.DataFrame()
        
        max_retries = 3
        
        for i in range(max_retries):
            retry_msg = f" [重试{i}]" if i > 0 else ""
            print(f"   ⚡ [YFinance] 请求: {symbol}{retry_msg} ...", end="", flush=True)
            
            try:
                df = yf.download(symbol, start=START_DATE, end=END_DATE, progress=False, auto_adjust=False)
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
            "黄金(COMEX)": "GCUSD", "VNM(ETF)": "VNM"
        }
        symbol = symbol_map.get(name)
        
        if not symbol: return pd.DataFrame()

        print(f"   ⚡ [FMP] 请求: {symbol} ...", end="", flush=True)
        try:
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={START_DATE}&to={END_DATE}&apikey={key}"
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
        df = self.fetch_akshare(config.get("ak"), config.get("type"))
        df = self.normalize_df(df, name)
        
        # 2. 失败则尝试 YFinance (函数内自带重试机制)
        if df.empty:
            df = self.fetch_yfinance(config.get("yf"))
            df = self.normalize_df(df, name)

        # 3. 失败则尝试 FMP
        if df.empty:
            df = self.fetch_fmp(name)
            df = self.normalize_df(df, name)
            
        return df

def process_and_save_group(fetcher, targets, filename_prefix):
    """
    通用函数：使用多线程并发处理一组目标并保存到指定文件
    """
    print(f"\n🚀 开始处理任务组: 输出到 {filename_prefix} (并发模式)")
    
    kline_list = []
    
    # 定义单个任务函数 (用于线程池)
    def fetch_task(name, config):
        try:
            df = fetcher.get_kline_data(name, config)
            if df.empty:
                return None
            
            # 过滤时间段
            df = df[(df['date'] >= pd.to_datetime(START_DATE)) & (df['date'] <= pd.to_datetime(END_DATE))]
            if df.empty:
                return None
            
            # 转换日期格式
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            return df.to_dict(orient='records')
        except Exception as e:
            print(f"❌ 任务 {name} 异常: {e}")
            return None

    # 使用 ThreadPoolExecutor 进行并发
    # max_workers=4 是一个比较安全的数值，既提高了速度又不容易被封IP
    with ThreadPoolExecutor(max_workers=4) as executor:
        # 提交所有任务
        future_to_name = {executor.submit(fetch_task, name, config): name for name, config in targets.items()}
        
        # 获取结果 (加入超时保护，防止单线程无限挂起)
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                # === 核心修改：设置 15 秒超时 ===
                # 如果这个任务 15 秒都没返回结果，直接抛出 TimeoutError，不再等待
                result = future.result(timeout=15)
                
                if result:
                    kline_list.extend(result)
                else:
                    print(f"⚠️ 警告: 无法获取 {name} 的有效数据，已舍弃")
            except TimeoutError:
                print(f" 💀 严重超时: 获取 {name} 超过15秒无响应，强制跳过！")
            except Exception as e:
                print(f"❌ 处理 {name} 结果时出错: {e}")

    # 排序：因为并发返回顺序是乱的，所以必须按日期和名称排序
    if kline_list:
        temp_df = pd.DataFrame(kline_list)
        temp_df.sort_values(by=['date', 'name'], ascending=[False, True], inplace=True)
        final_kline_data = temp_df.to_dict(orient='records')
    else:
        final_kline_data = []

    # --- 保存文件逻辑 ---
    meta_data = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "date_range": f"{START_DATE} to {END_DATE}",
        "group_name": filename_prefix
    }
    
    # 确定文件名
    if "Market_Data" in filename_prefix or "指数" in filename_prefix:
        output_filename = "指数.json"
    else:
        output_filename = f"{filename_prefix}.json"

    print(f"💾 正在写入 JSON 文件: {output_filename} ...")
    
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write('{\n')
            f.write(f'    "meta": {json.dumps(meta_data, ensure_ascii=False)},\n')
            f.write('    "market_klines": [\n')
            
            total_items = len(final_kline_data)
            for i, item in enumerate(final_kline_data):
                line_str = json.dumps(item, ensure_ascii=False)
                comma = "," if i < total_items - 1 else ""
                f.write(f'        {line_str}{comma}\n')
                
            f.write('    ]\n')
            f.write('}')
            
        print(f"✅ 成功! 文件 {output_filename} 已生成。")
    except Exception as e:
        print(f"❌ 写入 JSON 失败: {e}")


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


def generate_report():
    # === 新增：酷炫的启动 Banner ===
    print(r"""
  __  __            _        _   ____          _            
 |  \/  | __ _ _ __| | _____| |_|  _ \ __ _ __| | __ _ _ __ 
 | |\/| |/ _` | '__| |/ / _ \ __| |_) / _` / _` |/ _` | '__|
 | |  | | (_| | |  |   <  __/ |_|  _ < (_| (_| | (_| | |   
 |_|  |_|\__,_|_|  |_|\_\___|\__|_| \_\__,_\__,_|\__,_|_|   
                                                            
    """)
    print("=========================================")
    print(f"📅 多市场数据采集器 (MarketRadar - GitHub Actions Ready)")
    print(f"🕒 时间段: {START_DATE} 至 {END_DATE}")
    print("=========================================\n")

    fetcher = MarketFetcher()
    
    process_and_save_group(fetcher, TARGETS_GLOBAL, "指数")
    process_and_save_group(fetcher, TARGETS_HSTECH_TOP20, "恒生科技")
    process_and_save_group(fetcher, TARGETS_VIETNAM_TOP10, "新兴市场")
    process_and_save_group(fetcher, TARGETS_US_MAG7, "美股七巨头")
    process_and_save_group(fetcher, TARGETS_HK_PHARMA, "港股创新药")
    
    print("\n🎉 所有数据抓取任务处理完成！")
    
    generated_files = [
        "指数.json",
        "恒生科技.json",
        "新兴市场.json",
        "美股七巨头.json",
        "港股创新药.json"
    ]
    
    email_subject = f"全球市场K线数据报告_{datetime.now().strftime('%Y-%m-%d')}"
    email_body = f"""
    您好，
    
    这是今天的全量市场 K 线数据（由 MarketRadar 自动生成）。
    生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    数据范围: {START_DATE} 至 {END_DATE}
    
    附件列表:
    {', '.join(generated_files)}
    
    请查收。
    """
    
    send_email(email_subject, email_body, generated_files)

if __name__ == "__main__":
    generate_report()