#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
数据获取脚本 (精简版 - 负责 FX、国债及新增的越南指数)
功能：在线获取全球汇率、VIX、中/美/日国债收益率以及越南胡志明指数K线。
注意：宏观经济指标(CPI/PPI等)已移交 scrape_economy_selenium.py 处理。
"""

import datetime
import time
import os
import pandas as pd
import yfinance as yf
import akshare as ak
import requests
import warnings
from io import StringIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo  # 新增: 时区处理

warnings.filterwarnings("ignore")

# ==============================================================================
# 配置与工具
# ==============================================================================

ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "DEMO")
TZ_CN = ZoneInfo("Asia/Shanghai") # 定义北京时区

def get_retry_session(retries=5):
    session = requests.Session()
    # 模拟浏览器 Header，防止 Investing.com 403
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    })
    retry = Retry(total=retries, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retry))
    session.mount('https://', HTTPAdapter(max_retries=retry))
    return session

SESSION = get_retry_session()
TIMEOUT = 15

def fetch_latest_yf(ticker, name):
    """yfinance 获取最新一天的数据"""
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d")
        if hist is None or hist.empty:
            return [], "No data returned from yfinance"
        latest = hist.iloc[-1]
        data = [{
            "日期": latest.name.strftime('%Y-%m-%d'),
            "最新值": float(latest['Close']),
            "名称": name
        }]
        return data, None
    except Exception as e:
        print(f"Error fetching {name} (yfinance): {e}")
        return [], str(e)

def fetch_alpha_vantage_indicator(indicator, interval="daily"):
    url = "https://www.alphavantage.co/query"
    params = {"function": indicator, "interval": interval, "apikey": ALPHA_VANTAGE_KEY}
    try:
        r = SESSION.get(url, params=params, timeout=TIMEOUT)
        data = r.json()
        if "data" in data:
            df = pd.DataFrame(data["data"])
            df.rename(columns={"date": "日期", "value": "10年"}, inplace=True) 
            return df
    except Exception:
        pass
    return pd.DataFrame()

# ==============================================================================
# 核心获取逻辑 (在线)
# ==============================================================================

def fetch_us_bond_yields():
    print("   -> 获取美国国债数据...")
    tickers_map = {"13周": "^IRX", "5年": "^FVX", "10年": "^TNX", "30年": "^TYX"}
    temp_results = {}
    latest_date = None
    errors = []
    
    # 优先使用 YFinance
    for label, ticker in tickers_map.items():
        data, err = fetch_latest_yf(ticker, label)
        if data:
            item = data[0]
            if latest_date is None or item["日期"] > latest_date:
                latest_date = item["日期"]
            temp_results[label] = item["最新值"]
        else:
            errors.append(f"{label}: {err}")

    if temp_results and latest_date:
        row = {"日期": latest_date}
        row.update(temp_results)
        return [row], None
    
    # Fallback: Alpha Vantage (仅10年)
    try:
        df_av = fetch_alpha_vantage_indicator("TREASURY_YIELD")
        if not df_av.empty:
            return [df_av.iloc[0].to_dict()], None
    except Exception as e:
        errors.append(f"AlphaVantage: {str(e)}")
        
    return [], "; ".join(errors) if errors else "All sources failed"

def fetch_china_bond_yields():
    print("   -> 获取中国国债数据...")
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=30)
    try:
        df = ak.bond_china_yield(start_date=start_date.strftime("%Y%m%d"), end_date=end_date.strftime("%Y%m%d"))
        if df is None or df.empty:
            # 备用：东方财富接口
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            params = {
                "reportName": "RPT_BOND_YIELD_CURVE",
                "columns": "TRADE_DATE,YIELD_1Y,YIELD_2Y,YIELD_10Y,YIELD_30Y",
                "filter": '(CURVE_TYPE="0")(IS_DISTINCT="1")',
                "pageNumber": "1", "pageSize": "5", "sortColumns": "TRADE_DATE", "sortTypes": "-1", "source": "WEB", "client": "WEB"
            }
            r = SESSION.get(url, params=params, timeout=TIMEOUT)
            df = pd.DataFrame(r.json()["result"]["data"])
            df.rename(columns={"TRADE_DATE": "日期","YIELD_1Y": "1年", "YIELD_2Y": "2年", "YIELD_10Y": "10年", "YIELD_30Y": "30年"}, inplace=True)
        else:
            df.rename(columns={"日期": "日期", "1年": "1年", "2年": "2年", "10年": "10年", "30年": "30年"}, inplace=True)
        
        df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
        latest = df.sort_values("日期").iloc[-1].to_dict()
        data = [{k: v for k, v in latest.items() if k in ['日期', '1年', '2年', '10年', '30年']}]
        return data, None
    except Exception as e:
        print(f"中国国债获取失败: {e}")
        return [], str(e)

def fetch_japan_bond_yields():
    """
    使用 Investing.com 爬取日本国债收益率
    目标: 2年, 10年, 30年
    改进: 全表扫描，支持多期限抓取
    """
    print("   -> 获取日本国债数据 (Investing.com)...")
    url = "https://cn.investing.com/rates-bonds/japan-government-bonds"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        
        # 解析表格
        try:
            dfs = pd.read_html(StringIO(r.text))
        except ValueError as ve:
            return [], f"No tables found in response: {ve}"

        # 定义需要抓取的目标名称和输出key的映射
        targets = {
            "日本2年期国债": "2年",
            "日本10年期国债": "10年",
            "日本30年期国债": "30年"
        }
        
        found_data = {}

        # 遍历所有表格，寻找所有目标
        for df in dfs:
            # 转换为字符串，便于查找
            df_str = df.astype(str)
            
            for target_name, output_key in targets.items():
                if output_key in found_data:
                    continue # 已经找到就不重复找了
                
                # 寻找包含目标名称的行
                mask = df_str.apply(lambda x: x.str.contains(target_name, na=False))
                
                if mask.any().any():
                    # 获取行索引
                    row_idx = mask.any(axis=1).idxmax()
                    target_row = df.loc[row_idx]
                    
                    yield_val = None
                    
                    # 策略 A: 查找列名包含 "收益率" 或 "Yield" 的列
                    cols = [str(c).strip() for c in df.columns]
                    target_col_name = None
                    possible_names = ["收益率", "债券收益率", "Yield", "最新", "最新价", "Last"]
                    for pname in possible_names:
                        for c in cols:
                            if pname in c:
                                target_col_name = c
                                break
                        if target_col_name:
                            break
                    
                    if target_col_name:
                        yield_val = target_row[target_col_name]
                    else:
                        # 策略 B: 取名称所在列右侧的第一列
                        name_col_idx = -1
                        for i, is_found in enumerate(mask.iloc[row_idx]):
                            if is_found:
                                name_col_idx = i
                                break
                        if name_col_idx != -1 and name_col_idx + 1 < len(df.columns):
                            yield_val = df.iloc[row_idx, name_col_idx + 1]

                    # 数据清洗并存储
                    if yield_val is not None:
                        try:
                            val_str = str(yield_val).replace('%', '').strip()
                            found_data[output_key] = float(val_str)
                        except ValueError:
                            pass # 解析失败则忽略

        if not found_data:
            return [], "Targets (2Y/10Y/30Y) not found in any table"

        # 构造返回结果 (单条记录包含多个期限, 强制使用北京时间)
        current_date = datetime.datetime.now(TZ_CN).strftime('%Y-%m-%d')
        result_row = {"日期": current_date}
        result_row.update(found_data)
        
        # 打印日志方便确认
        found_keys = list(found_data.keys())
        print(f"   [日本国债] 抓取成功: {found_keys}")
        
        return [result_row], None

    except Exception as e:
        print(f"日本国债获取失败: {e}")
        return [], str(e)

def fetch_vietnam_index_klines():
    """
    使用 Investing.com 爬取越南胡志明指数历史数据
    目标: K线数据 (日期, 收盘, 开盘, 高, 低, 交易量, 涨跌幅)
    """
    print("   -> 获取越南胡志明指数K线 (Investing.com)...")
    url = "https://cn.investing.com/indices/vn-historical-data"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        
        # 移除 id="curr_table" 限制，Investing.com ID 可能会变
        try:
            dfs = pd.read_html(StringIO(r.text))
        except ValueError as ve:
             # 如果解析失败，打印部分内容检查是否被反爬
             print(f"   [Debug] Read HTML failed. Response preview: {r.text[:200]}...")
             return [], f"Read HTML failed: {ve}"

        if not dfs:
            return [], "No tables found in response"
        
        df = None
        # 遍历所有表格，寻找包含特定列名的表格
        for i, temp_df in enumerate(dfs):
            cols = [str(c).strip() for c in temp_df.columns]
            # 特征列：日期 + 收盘
            if "日期" in cols and "收盘" in cols:
                df = temp_df
                # print(f"   [Debug] Found VN Index table at index {i}")
                break
        
        if df is None:
            # 再次尝试打印所有表格的列名以便调试
            # for i, temp_df in enumerate(dfs):
            #     print(f"   [Debug] Table {i} columns: {temp_df.columns.tolist()}")
            return [], "Table with columns '日期' and '收盘' not found"
        
        # 数据清洗设计
        # 1. 日期格式: 2025年12月31日 -> 2025-12-31
        def parse_date(x):
            try:
                return datetime.datetime.strptime(str(x), "%Y年%m月%d日").strftime("%Y-%m-%d")
            except:
                return str(x)
        
        df["日期"] = df["日期"].apply(parse_date)
        
        # 2. 数值处理 (移除逗号)
        cols_to_clean = ["收盘", "开盘", "高", "低"]
        for col in cols_to_clean:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")
        
        # 3. 交易量处理 (M -> *1e6, K -> *1e3, B -> *1e9)
        def parse_volume(x):
            if pd.isna(x) or x == '-':
                return 0
            s = str(x).upper()
            multi = 1
            if "M" in s:
                multi = 1_000_000
                s = s.replace("M", "")
            elif "K" in s:
                multi = 1_000
                s = s.replace("K", "")
            elif "B" in s:
                multi = 1_000_000_000
                s = s.replace("B", "")
            try:
                return float(s) * multi
            except:
                return 0
        
        if "交易量" in df.columns:
            df["交易量"] = df["交易量"].apply(parse_volume)
            
        # 4. 涨跌幅 (移除 %)
        if "涨跌幅" in df.columns:
            df["涨跌幅"] = pd.to_numeric(df["涨跌幅"].astype(str).str.replace("%", ""), errors="coerce")
        
        # 排序
        df = df.sort_values("日期", ascending=True)
        
        # 映射为标准 OHLCV 格式
        # 输出字段: date, open, high, low, close, volume
        result = []
        for _, row in df.iterrows():
            result.append({
                "date": row["日期"],
                "open": row.get("开盘"),
                "high": row.get("高"),
                "low": row.get("低"),
                "close": row.get("收盘"),
                "volume": row.get("交易量"),
                "change_pct": row.get("涨跌幅")
            })
            
        return result, None

    except Exception as e:
        print(f"越南指数获取失败: {e}")
        return [], str(e)

# ==============================================================================
# 主入口
# ==============================================================================

def get_market_fx_and_bonds():
    """
    获取汇率、VIX、国债数据
    Returns:
        tuple: (data_store, status_logs)
    """
    print(">>> [fetch_data] 开始在线获取 FX 和 国债数据...")
    
    data_store = {
        "market_fx": {},
        "china": {},
        "usa": {},
        "japan": {}
    }
    
    # 状态日志列表: {'name': str, 'status': bool, 'error': str}
    status_logs = []

    # 1. Market FX
    tickers = {
        "VIX恐慌指数": "^VIX",
        "美元/人民币": "CNY=X",
        "美元/日元": "JPY=X",
        "美元/越南盾": "VND=X"
    }
    for name, ticker in tickers.items():
        res, err = fetch_latest_yf(ticker, name)
        if res:
            data_store["market_fx"][name] = res
            status_logs.append({'name': name, 'status': True, 'error': None})
            print(f"   [{name}] OK")
        else:
            status_logs.append({'name': name, 'status': False, 'error': err})
            print(f"   [{name}] Failed")

    # 2. Bonds
    # USA
    data_us, err_us = fetch_us_bond_yields()
    if data_us:
        data_store["usa"]["国债收益率"] = data_us
        status_logs.append({'name': "美国国债收益率", 'status': True, 'error': None})
    else:
        status_logs.append({'name': "美国国债收益率", 'status': False, 'error': err_us})

    # China
    data_cn, err_cn = fetch_china_bond_yields()
    if data_cn:
        data_store["china"]["国债收益率"] = data_cn
        status_logs.append({'name': "中国国债收益率", 'status': True, 'error': None})
    else:
        status_logs.append({'name': "中国国债收益率", 'status': False, 'error': err_cn})

    # Japan (使用新接口, 支持2/10/30年)
    data_jp, err_jp = fetch_japan_bond_yields()
    if data_jp:
        data_store["japan"]["国债收益率"] = data_jp
        status_logs.append({'name': "日本国债收益率", 'status': True, 'error': None})
    else:
        status_logs.append({'name': "日本国债收益率", 'status': False, 'error': err_jp})
        print(f"   [日本国债] Failed: {err_jp}")

    return data_store, status_logs

if __name__ == "__main__":
    # Test
    data, logs = get_market_fx_and_bonds()
    vn_data, vn_err = fetch_vietnam_index_klines()
    print("VNI Data (Top 2):", vn_data[:2] if vn_data else vn_err)