#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
数据获取核心逻辑库 (Refactored from fetch_data.py)
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
from zoneinfo import ZoneInfo

warnings.filterwarnings("ignore")

ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "DEMO")
TZ_CN = ZoneInfo("Asia/Shanghai")
TIMEOUT = 15

def get_retry_session(retries=5):
    session = requests.Session()
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

def fetch_yf_data(ticker, name, days=1):
    """yfinance 获取数据"""
    try:
        t = yf.Ticker(ticker)
        # 如果需要多天数据，扩大获取范围以确保数量足够 (例如取1个月)
        period = "1mo" if days > 1 else "5d"
        hist = t.history(period=period)
        
        if hist is None or hist.empty:
            return [], "No data returned from yfinance"
        
        latest_slice = hist.iloc[-days:]
        
        data = []
        for dt, row in latest_slice.iterrows():
            data.append({
                "日期": dt.strftime('%Y-%m-%d'),
                "最新值": float(row['Close']),
                "名称": name
            })
            
        data.sort(key=lambda x: x["日期"], reverse=True)
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

def fetch_us_bond_yields():
    print("   -> 获取美国国债数据...")
    tickers_map = {"13周": "^IRX", "5年": "^FVX", "10年": "^TNX", "30年": "^TYX"}
    temp_results = {}
    latest_date = None
    errors = []
    
    for label, ticker in tickers_map.items():
        data, err = fetch_yf_data(ticker, label, days=1)
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
    print("   -> 获取日本国债数据 (Investing.com)...")
    url = "https://cn.investing.com/rates-bonds/japan-government-bonds"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        
        try:
            dfs = pd.read_html(StringIO(r.text))
        except ValueError as ve:
            return [], f"No tables found in response: {ve}"

        targets = {
            "日本2年期国债": "2年",
            "日本10年期国债": "10年",
            "日本30年期国债": "30年"
        }
        
        found_data = {}
        for df in dfs:
            df_str = df.astype(str)
            for target_name, output_key in targets.items():
                if output_key in found_data: continue
                
                mask = df_str.apply(lambda x: x.str.contains(target_name, na=False))
                if mask.any().any():
                    row_idx = mask.any(axis=1).idxmax()
                    target_row = df.loc[row_idx]
                    yield_val = None
                    
                    cols = [str(c).strip() for c in df.columns]
                    target_col_name = None
                    possible_names = ["收益率", "债券收益率", "Yield", "最新", "最新价", "Last"]
                    for pname in possible_names:
                        for c in cols:
                            if pname in c:
                                target_col_name = c
                                break
                        if target_col_name: break
                    
                    if target_col_name:
                        yield_val = target_row[target_col_name]
                    else:
                        name_col_idx = -1
                        for i, is_found in enumerate(mask.iloc[row_idx]):
                            if is_found:
                                name_col_idx = i
                                break
                        if name_col_idx != -1 and name_col_idx + 1 < len(df.columns):
                            yield_val = df.iloc[row_idx, name_col_idx + 1]

                    if yield_val is not None:
                        try:
                            val_str = str(yield_val).replace('%', '').strip()
                            found_data[output_key] = float(val_str)
                        except ValueError:
                            pass

        if not found_data:
            return [], "Targets (2Y/10Y/30Y) not found in any table"

        current_date = datetime.datetime.now(TZ_CN).strftime('%Y-%m-%d')
        result_row = {"日期": current_date}
        result_row.update(found_data)
        
        print(f"   [日本国债] 抓取成功: {list(found_data.keys())}")
        return [result_row], None

    except Exception as e:
        print(f"日本国债获取失败: {e}")
        return [], str(e)

def fetch_vietnam_index_klines():
    print("   -> 获取越南胡志明指数K线 (Investing.com)...")
    url = "https://cn.investing.com/indices/vn-historical-data"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        
        try:
            dfs = pd.read_html(StringIO(r.text))
        except ValueError as ve:
             print(f"   [Debug] Read HTML failed. Response preview: {r.text[:200]}...")
             return [], f"Read HTML failed: {ve}"

        if not dfs:
            return [], "No tables found in response"
        
        df = None
        for i, temp_df in enumerate(dfs):
            cols = [str(c).strip() for c in temp_df.columns]
            if "日期" in cols and "收盘" in cols:
                df = temp_df
                break
        
        if df is None:
            return [], "Table with columns '日期' and '收盘' not found"
        
        def parse_date(x):
            try:
                return datetime.datetime.strptime(str(x), "%Y年%m月%d日").strftime("%Y-%m-%d")
            except:
                return str(x)
        
        df["日期"] = df["日期"].apply(parse_date)
        
        cols_to_clean = ["收盘", "开盘", "高", "低"]
        for col in cols_to_clean:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")
        
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
            
        if "涨跌幅" in df.columns:
            df["涨跌幅"] = pd.to_numeric(df["涨跌幅"].astype(str).str.replace("%", ""), errors="coerce")
        
        df = df.sort_values("日期", ascending=True)
        
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
# 新增: 适配 AKShare 其他金融接口 (南向, 科创50相关)
# ==============================================================================

def fetch_southbound_flow():
    """获取南向资金净流入 (近6个月)"""
    print("   -> 获取南向资金数据 (AKShare)...")
    try:
        # symbol="南下" 对应沪深港通南向资金
        df = ak.stock_hsgt_south_net_flow_in_em(symbol="南下")
        if df.empty:
            return [], "AKShare returned empty dataframe"
        
        # 假设返回列包含日期和值，这里做通用处理
        # AKShare通常返回: date, value
        # 强制重命名以防万一
        df.columns = ['date', 'value']
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # 取近6个月 (约180天)
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=180)
        df = df[df['date'] >= cutoff_date]
        
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # 转换为 list of dicts
        data = []
        for _, row in df.iterrows():
            data.append({
                "日期": row['date'],
                "净流入": row['value']
            })
        
        # 倒序排列 (最新的在前，符合项目习惯)
        data.sort(key=lambda x: x["日期"], reverse=True)
        return data, None
    except Exception as e:
        print(f"南向资金获取失败: {e}")
        return [], str(e)

def fetch_star50_valuation():
    """获取科创50指数估值 (PE/PB) (近6个月)"""
    print("   -> 获取科创50估值数据 (AKShare)...")
    try:
        # 科创50指数代码 000688
        df = ak.stock_zh_index_value_csindex(symbol="000688")
        if df.empty:
            return [], "AKShare returned empty dataframe"
        
        # 常见列名: 日期, 市盈率1(PE), 市盈率2(PE-TTM), 股息率1, 股息率2, 市净率1(PB)
        # 我们主要关注 PE 和 PB
        # 这里的列名通常是中文
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')
        
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=180)
        df = df[df['日期'] >= cutoff_date]
        
        df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')
        
        data = []
        for _, row in df.iterrows():
            item = {"日期": row['日期']}
            # 尝试提取 PE (通常是 '市盈率1' 或 '市盈率(PE)')
            for col in df.columns:
                if "市盈率" in col and "1" in col: # 优先取 PE1
                    item["PE"] = row[col]
                elif "市净率" in col and "1" in col:
                    item["PB"] = row[col]
            
            # 如果没找到带1的，尝试模糊匹配
            if "PE" not in item:
                for col in df.columns:
                    if "市盈率" in col: item["PE"] = row[col]; break
            if "PB" not in item:
                for col in df.columns:
                    if "市净率" in col: item["PB"] = row[col]; break
                    
            data.append(item)
            
        data.sort(key=lambda x: x["日期"], reverse=True)
        return data, None
    except Exception as e:
        print(f"科创50估值获取失败: {e}")
        return [], str(e)

def fetch_star50_margin():
    """获取科创50ETF融资融券数据 (近6个月)"""
    print("   -> 获取科创50融资融券数据 (AKShare)...")
    try:
        # 科创50ETF代码 588000
        df = ak.stock_margin_detail_sse(symbol="588000")
        if df.empty:
            return [], "AKShare returned empty dataframe"
        
        # 列名: 信用交易日期, 融资余额, 融券余额, ...
        df['信用交易日期'] = pd.to_datetime(df['信用交易日期'])
        df = df.sort_values('信用交易日期')
        
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=180)
        df = df[df['信用交易日期'] >= cutoff_date]
        
        df['信用交易日期'] = df['信用交易日期'].dt.strftime('%Y-%m-%d')
        
        data = []
        for _, row in df.iterrows():
            data.append({
                "日期": row['信用交易日期'],
                "融资余额": row.get('融资余额'),
                "融券余额": row.get('融券余额'),
                "融资买入额": row.get('融资买入额')
            })
            
        data.sort(key=lambda x: x["日期"], reverse=True)
        return data, None
    except Exception as e:
        print(f"科创50融资融券获取失败: {e}")
        return [], str(e)

def fetch_star50_realtime_vol_ratio():
    """获取科创50ETF实时量比 (Spot Data)"""
    print("   -> 获取科创50ETF实时量比 (AKShare)...")
    try:
        df = ak.fund_etf_spot_em()
        # 过滤 588000
        target = df[df['代码'] == '588000']
        if target.empty:
            return None, "Symbol 588000 not found in spot data"
        
        row = target.iloc[0]
        result = {
            "代码": row['代码'],
            "名称": row['名称'],
            "最新价": row['最新价'],
            "量比": row['量比'],
            "更新时间": datetime.datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')
        }
        return result, None
    except Exception as e:
        print(f"科创50实时量比获取失败: {e}")
        return None, str(e)