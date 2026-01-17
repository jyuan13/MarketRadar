#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
数据获取核心逻辑库 (Refactored from fetch_data.py)
已确认未发生聚焦修复而省略非核心功能代码
"""

import datetime
import time
import os
import pandas as pd
import akshare as ak
import requests
import warnings
from io import StringIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo
from openbb import obb

warnings.filterwarnings("ignore")

ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "DEMO")
TZ_CN = ZoneInfo("Asia/Shanghai")
TIMEOUT = 30

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
    """
    [Refactored] 使用 OpenBB 获取数据 (原 yfinance)
    """
    try:
        # OpenBB v4 接口: obb.equity.price.historical
        # 计算 start_date based on days. Adding buffer.
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days + 10) 
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        # 使用 yfinance provider
        df = obb.equity.price.historical(symbol=ticker, start_date=start_date_str, provider="yfinance").to_df()
        
        if df is None or df.empty:
            return [], "No data returned from OpenBB(yfinance)"
        
        # OpenBB standardizes columns to snake_case: date (index), open, high, low, close, volume...
        # reset index if date is index
        df = df.reset_index()
        
        # 查找日期列
        date_col = None
        for col in df.columns:
            if "date" in col.lower():
                date_col = col
                break
        
        if not date_col:
             return [], "Date column not found in OpenBB result"
             
        # 截取所需行数
        latest_slice = df.tail(days)
        
        data = []
        for _, row in latest_slice.iterrows():
            # 兼容 OpenBB 这里的列名应该都是小写
            close_val = row.get('close')
            if close_val is None:
                 # Fallback if case sensitivity issues? usually openbb returns lowercase
                 close_val = row.get('Close')
                 
            dt_val = row[date_col]
            # Ensure datetime
            if not isinstance(dt_val, (datetime.date, datetime.datetime)):
                dt_val = pd.to_datetime(dt_val)
                
            data.append({
                "日期": dt_val.strftime('%Y-%m-%d'),
                "最新值": float(close_val),
                "名称": name
            })
            
        data.sort(key=lambda x: x["日期"], reverse=True)
        return data, None
    except Exception as e:
        print(f"Error fetching {name} (OpenBB): {e}")
        return [], str(e)

def fetch_alpha_vantage_indicator(indicator, interval="daily"):
    """
    [保留] Alpha Vantage 作为备用
    """
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
    """
    [Refactored] 获取美国国债数据
    尝试使用 OpenBB (FRED/Federal Reserve data via 'fixedincome' or 'economy' module)
    """
    print("   -> 获取美国国债数据 (OpenBB)...")
    tickers_map = {
        "13周": "TB3MS",   # 3-Month Treasury Bill Secondary Market Rate (FRED)
        "5年": "DGS5",     # Market Yield on U.S. Treasury Securities at 5-Year Constant Maturity
        "10年": "DGS10",   # Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity
        "30年": "DGS30"    # Market Yield on U.S. Treasury Securities at 30-Year Constant Maturity
    }
    
    # 注意: FRED 数据通常延迟一天更新。实时数据可能仍需 yfinance (CBOE Interest Rate 10 Year T Note which is ^TNX)
    # 策略: 优先使用 ^TNX via OpenBB(yfinance) 获取最新，如果失败则使用 FRED
    
    # 映射回 yfinance symbol 以获取最新行情
    yf_tickers_map = {"13周": "^IRX", "5年": "^FVX", "10年": "^TNX", "30年": "^TYX"}
    
    temp_results = {}
    latest_date = None
    errors = []
    
    for label, ticker in yf_tickers_map.items():
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
    
    # Backup: OpenBB FRED
    try:
        # obb.fixedincome.sovereign.yield_curve not perfectly matching simplified scalar need?
        # Let's try fetching individual series if bulk fails
        pass 
    except Exception:
        pass
        
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
    """
    [Refactored] 获取日本国债数据
    尝试改用 OpenBB (yfinance provider via global government bonds logic?)
    Currently OpenBB doesn't have a direct "Japan Government Bond" unified endpoint that easy.
    We will stick to the previous scraping logic via OpenBB's general scraping capabilities IF available, 
    but since we want to migrate, we can try getting generic tickers if they exist on yf/investing via OpenBB.
    However, Japan Govt Bonds on Investing.com is the reliable source.
    Let's keep the scraping logic but wrap it or clean it. 
    IF OpenBB provider 'investing' works for this, great. 
    Actually, let's look for Japan bond tickers on Yahoo Finance:
    - 10 Year: ^TNX (US), Japan 10Y is often generic. 
    
    Use the original scraping logic as fallback but try to improve robustness.
    """
    print("   -> 获取日本国债数据 (Investing.com)...")
    # Original scraping logic is kept as it's specific to Investing.com pages which might not have a clean API in free tiers
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
    """
    [Refactored] 获取越南胡志明指数 (VNINDEX)
    尝试使用 OpenBB (provider=yfinance or investing)
    Ticker for Vietnam Ho Chi Minh Index is usually '^VNINDEX' or 'VNINDEX.HM' on some platforms.
    On Yahoo Finance it is '^VNINDEX' (often data is poor) or we can try 'VNI' ETF proxy.
    However, the user asked to 'try updating to OpenBB interfaces'.
    Let's try OpenBB with ticker '^VNINDEX'.
    """
    print("   -> 获取越南胡志明指数K线 (OpenBB)...")
    try:
        # 尝试使用 OpenBB 获取
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=60) # Get enough history
        
        # Yahoo Finance ticker for VN Index
        ticker = "^VNINDEX" 
        
        try:
            df = obb.equity.price.historical(symbol=ticker, start_date=start_date.strftime('%Y-%m-%d'), provider="yfinance").to_df()
        except Exception:
            df = None

        if df is None or df.empty:
             # Fallback to original investing scraping if OpenBB fails
             print("   [OpenBB] VNINDEX lookup failed, falling back to scraping.")
             return _fetch_vietnam_index_klines_scraping()
        
        df = df.reset_index()
        df.columns = [c.lower() for c in df.columns]
        
        # Rename date column if needed
        date_col = next((c for c in df.columns if "date" in c), "date")
        
        df = df.rename(columns={
            date_col: "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume"
        })
        
        # Calculate change_pct if missing
        if "change_pct" not in df.columns:
             df["change_pct"] = df["close"].pct_change() * 100
             
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values("date")
        
        result = []
        for _, row in df.iterrows():
            result.append({
                "date": row["date"],
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume"),
                "change_pct": row.get("change_pct")
            })
            
        return result, None

    except Exception as e:
        print(f"越南指数获取失败 (OpenBB): {e}")
        print("   -> Attempting fallback scraping...")
        return _fetch_vietnam_index_klines_scraping()

def _fetch_vietnam_index_klines_scraping():
    """
    [Original Logic] Backup scraping for Vietnam Index
    """
    url = "https://cn.investing.com/indices/vn-historical-data"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        
        try:
            dfs = pd.read_html(StringIO(r.text))
        except ValueError as ve:
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
        return [], str(e)


# ==============================================================================
# AKShare 特定接口适配
# ==============================================================================

def fetch_southbound_flow():
    """获取南向资金净流入 (近20天) - 使用 stock_hsgt_hist_em"""
    print("   -> 获取南向资金数据 (AKShare)...")
    
    max_retries = 3
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        try:
            df = ak.stock_hsgt_hist_em(symbol="南向资金")
            if df.empty:
                raise ValueError("AKShare returned empty dataframe")
            
            if '日期' not in df.columns or '当日成交净买额' not in df.columns:
                raise ValueError(f"Unexpected columns: {df.columns.tolist()}")
                
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values('日期')
            
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=20)
            df = df[df['日期'] >= cutoff_date]
            
            df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')
            
            data = []
            for _, row in df.iterrows():
                data.append({
                    "日期": row['日期'],
                    "净流入(亿元)": row['当日成交净买额']
                })
            
            data.sort(key=lambda x: x["日期"], reverse=True)
            return data, None
            
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                print(f"   ⚠️ 南向资金获取重试 ({attempt}/{max_retries}): {e}")
                time.sleep(2)
    
    print(f"南向资金获取失败: {last_error}")
    return [], str(last_error)

def fetch_star50_valuation():
    """获取科创50指数估值 (PE/PB) (近6个月)"""
    print("   -> 获取科创50估值数据 (AKShare)...")
    try:
        df = ak.stock_zh_index_value_csindex(symbol="000688")
        if df.empty:
            return [], "AKShare returned empty dataframe"
        
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')
        
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=180)
        df = df[df['日期'] >= cutoff_date]
        
        df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')
        
        data = []
        for _, row in df.iterrows():
            item = {"日期": row['日期']}
            for col in df.columns:
                if "市盈率" in col and "1" in col:
                    item["PE"] = row[col]
                elif "市净率" in col and "1" in col:
                    item["PB"] = row[col]
            
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
    """
    获取科创50ETF融资融券数据 (近15天)
    """
    print("   -> 获取科创50融资融券数据 (Loop Date)...")
    target_symbol = "588000" # 科创50ETF
    data_list = []
    
    try:
        days_checked = 0
        days_found = 0
        current = datetime.datetime.now()
        
        while days_found < 5 and days_checked < 20: 
            date_str = current.strftime("%Y%m%d")
            if current.weekday() < 5: 
                try:
                    df = ak.stock_margin_detail_sse(date=date_str)
                    if not df.empty:
                        df['标的证券代码'] = df['标的证券代码'].astype(str)
                        row = df[df['标的证券代码'] == target_symbol]
                        
                        if not row.empty:
                            r = row.iloc[0]
                            item = {
                                "日期": current.strftime("%Y-%m-%d"),
                                "融资余额": r.get('融资余额'),
                                "融券余额": r.get('融券余额'),
                                "融资买入额": r.get('融资买入额')
                            }
                            data_list.append(item)
                            days_found += 1
                except Exception:
                    pass
            
            current -= datetime.timedelta(days=1)
            days_checked += 1
            time.sleep(0.5) 

        if not data_list:
            return [], "No margin data found in recent 20 days"

        return data_list, None

    except Exception as e:
        print(f"科创50融资融券获取失败: {e}")
        return [], str(e)

def fetch_star50_realtime_vol_ratio():
    """获取科创50ETF实时量比 (Spot Data)"""
    print("   -> 获取科创50ETF实时量比 (AKShare)...")
    try:
        df = ak.fund_etf_spot_em()
        target = df[df['代码'] == '588000']
        if target.empty:
            return None, "Symbol 588000 not found in spot data"
        
        row = target.iloc[0]
        result = {
            "代码": row['代码'],
            "名称": row['名称'],
            "最新价": row['最新价'],
            "量比": row['量比'],
            "成交量": row.get('成交量'),
            "成交额": row.get('成交额'),
            "更新时间": datetime.datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')
        }
        return result, None
    except Exception as e:
        print(f"科创50实时量比获取失败: {e}")
        return None, str(e)

def fetch_ashare_indices():
    """
    获取A股主要指数的日线数据 (近20个交易日)
    """
    print("   -> 获取A股主要指数数据 (AKShare)...")
    
    index_list = [
        {"name": "上证指数", "symbol": "sh000001"},
        {"name": "深证成指", "symbol": "sz399001"},
        {"name": "创业板指", "symbol": "sz399006"},
        {"name": "沪深300", "symbol": "sh000300"}, 
    ]
    
    results = []
    errors = []

    for idx in index_list:
        name = idx["name"]
        symbol = idx["symbol"]
        try:
            df = ak.stock_zh_index_daily_em(symbol=symbol)

            if df.empty:
                errors.append(f"{name}: Empty data")
                continue

            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            df = df.iloc[-20:]
            
            for _, row in df.iterrows():
                results.append({
                    "date": row['date'].strftime('%Y-%m-%d'),
                    "name": name,
                    "open": row['open'],
                    "close": row['close'],
                    "high": row['high'],
                    "low": row['low'],
                    "volume": row['volume'],
                    "amount": row.get('amount', 0), 
                    "change_pct": 0.0 
                })
                
        except Exception as e:
            errors.append(f"{name}: {str(e)}")
    
    if not results and errors:
        return [], "; ".join(errors)
        
    return results, None

# ==============================================================================
# 新增: 60分钟K线 & 银行数据
# ==============================================================================

def _calculate_hourly_volume_ratio(df):
    """
    通用辅助函数: 计算小时级别的量比
    """
    if df is None or df.empty or 'volume' not in df.columns:
        return df

    try:
        df['hour_str'] = df['date'].dt.strftime('%H:%M')
        df['avg_vol_5d'] = df.groupby('hour_str')['volume'].transform(
            lambda x: x.shift(1).rolling(window=5, min_periods=1).mean()
        )
        df['volume_ratio'] = df['volume'] / df['avg_vol_5d']
        df['volume_ratio'] = df['volume_ratio'].fillna(0.0).replace([float('inf'), -float('inf')], 0.0)
        df['volume_ratio'] = df['volume_ratio'].apply(lambda x: round(x, 2))
        df.drop(columns=['hour_str', 'avg_vol_5d'], inplace=True)
    except Exception as e:
        print(f"   ⚠️ 量比计算失败: {e}")
        df['volume_ratio'] = 0.0
        
    return df

def fetch_kcb50_60m():
    """
    获取科创50 (588000) 60分钟K线
    """
    print("   -> 获取科创50 (588000) 60分钟K线 (AKShare)...")
    try:
        df = None
        try:
            df = ak.stock_zh_a_hist_min_em(symbol="588000", period="60", adjust="qfq")
        except:
            pass
            
        if df is None or df.empty:
            if hasattr(ak, 'fund_etf_hist_min_em'):
                try:
                    df = ak.fund_etf_hist_min_em(symbol="588000", period="60", adjust="qfq")
                except:
                    pass
        
        if df is None or df.empty:
            return [], "Empty or None dataframe from AKShare"
            
        df.rename(columns={
            "时间": "date", "成交量": "volume", "成交额": "amount", 
            "开盘": "open", "收盘": "close", "最高": "high", "最低": "low"
        }, inplace=True)
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        df = _calculate_hourly_volume_ratio(df)
        df_slice = df.iloc[-30:].copy()
        df_slice['date'] = df_slice['date'].dt.strftime('%Y-%m-%d %H:%M')
        
        result = []
        for _, row in df_slice.iterrows():
            result.append({
                "date": row['date'],
                "volume": row['volume'],
                "amount": row['amount'],
                "volume_ratio": row.get('volume_ratio', 0.0),
                "close": row['close']
            })
            
        return result, None

    except Exception as e:
        print(f"科创50 60mK线获取失败: {e}")
        return [], str(e)

def fetch_hstech_60m():
    """
    [Refactored] 获取恒生科技指数 60分钟K线 (使用 OpenBB 访问 3033.HK)
    """
    print("   -> 获取恒生科技指数 60分钟K线 (OpenBB)...")
    try:
        # obb.equity.price.historical, symbol="3033.HK", interval="60m"
        # 注意: OpenBB interval "60m" 支持取决于 provider (yfinance 支持)
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=20)
        
        df = obb.equity.price.historical(
            symbol="3033.HK", 
            start_date=start_date.strftime('%Y-%m-%d'),
            interval="60m",
            provider="yfinance"
        ).to_df()
        
        if df is None or df.empty:
            return [], "Empty dataframe from OpenBB (3033.HK)"
            
        df = df.reset_index()
        # 列名已经是小写: date, open, high, low, close, volume
        
        # 查找日期列
        date_col = next((c for c in df.columns if "date" in c), None)
        if not date_col:
             return [], "No date column"
             
        df = df.rename(columns={date_col: 'date'})
        
        if isinstance(df['date'].dtype, pd.DatetimeTZDtype):
             df['date'] = df['date'].dt.tz_convert(TZ_CN).dt.tz_localize(None)
        else:
             df['date'] = pd.to_datetime(df['date'])

        # amount 不一定有，置为 0
        if 'amount' not in df.columns:
            df['amount'] = 0.0
        
        df = df.sort_values('date')
        
        # 计算量比
        df = _calculate_hourly_volume_ratio(df)
        
        df_slice = df.tail(35)
        df_slice['date'] = df_slice['date'].dt.strftime('%Y-%m-%d %H:%M')
        
        result = []
        for _, row in df_slice.iterrows():
            result.append({
                "date": row['date'],
                "volume": row['volume'],
                "amount": row['amount'], 
                "volume_ratio": row.get('volume_ratio', 0.0),
                "close": row['close'],
                "note": "Source: ETF 3033.HK (OpenBB)"
            })
            
        return result, None

    except Exception as e:
        print(f"恒生科技 60mK线获取失败: {e}")
        return [], str(e)

def fetch_us_banks_daily():
    """
    [Refactored] 获取六大银行的日线数据 (使用 OpenBB)
    """
    print("   -> 获取六大银行日线数据 (OpenBB)...")
    banks = [
        {"name": "摩根大通", "symbol": "JPM"},
        {"name": "美国银行", "symbol": "BAC"},
        {"name": "花旗集团", "symbol": "C"},
        {"name": "富国银行", "symbol": "WFC"},
        {"name": "高盛集团", "symbol": "GS"},
        {"name": "摩根士丹利", "symbol": "MS"},
    ]
    
    results = []
    end = datetime.datetime.now()
    start = end - datetime.timedelta(days=365)
    
    for b in banks:
        name = b["name"]
        symbol = b["symbol"]
        
        try:
            df = obb.equity.price.historical(
                symbol=symbol, 
                start_date=start.strftime("%Y-%m-%d"),
                provider="yfinance"
            ).to_df()
            
            if df is None or df.empty:
                print(f"      Fail: {name} (Empty)")
                continue
                
            df = df.reset_index()
            date_col = next((c for c in df.columns if "date" in c), "date")
            df = df.rename(columns={date_col: 'date'})
            
            # Ensure proper columns for downstream
            df['date'] = pd.to_datetime(df['date'])
            df['name'] = name
            
            # Standard columns match: open, high, low, close, volume (lowercase from OpenBB)
            if 'close' in df.columns:
                results.append(df)
                print(f"      OK: {name}")
            else:
                print(f"      Skip: {name} (Missing columns)")
                
        except Exception as e:
            print(f"      Fail: {name} ({e})")
            
    return results

# ==============================================================================
# 新增: Crypto & Extended Macro (Req 2026-01-17)
# ==============================================================================

def fetch_crypto_daily():
    """
    获取 Bitcoin 价格 & 24h涨跌幅 (Ref: Req 23)
    使用 OpenBB (yfinance provider via obb.crypto.price.historical)
    """
    print("   -> 获取加密货币 (Bitcoin)...")
    try:
        # obb.crypto.price.historical(symbol="BTC-USD", provider="yfinance")
        # For simple 24h change, just fetch last 2 days
        end = datetime.datetime.now()
        start = end - datetime.timedelta(days=5)
        
        df = obb.crypto.price.historical(symbol="BTC-USD", start_date=start.strftime("%Y-%m-%d"), provider="yfinance").to_df()
        
        if df is None or df.empty:
            return None, "Bitcoin data empty"
            
        df = df.reset_index()
        # Columns: date, open, high, low, close, volume...
        date_col = next((c for c in df.columns if "date" in c), None)
        if not date_col: return None, "No date column"
        
        df = df.sort_values(date_col)
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        
        close = latest.get('close')
        prev_close = prev.get('close')
        
        change_pct = 0.0
        if prev_close and prev_close != 0:
            change_pct = (close - prev_close) / prev_close * 100
            
        return {
            "name": "Bitcoin (BTC-USD)",
            "price": close,
            "change_24h_pct": round(change_pct, 2),
            "date": latest[date_col].strftime('%Y-%m-%d')
        }, None
        
    except Exception as e:
        print(f"Bitcoin 获取失败: {e}")
        return None, str(e)

def fetch_global_macro_extra():
    """
    获取额外宏观数据 (OpenBB/FRED):
    - TIPS 实际利率 (DFII10)
    - 高收益债利差 (BAMLH0A0HYM2)
    - 10年盈亏平衡通胀率 (T10YIE)
    - TGA 账户余额 (WTREGEN)
    - 隔夜逆回购 ON RRP (RRPONTSYD)
    """
    print("   -> 获取全球宏观扩展数据 (OpenBB/FRED)...")
    indicators = {
        "10年期TIPS实际利率": "DFII10",
        "高收益债利差(HY OAS)": "BAMLH0A0HYM2",
        "10年盈亏平衡通胀率": "T10YIE",
        "TGA账户余额": "WTREGEN",
        "美联储ON RRP余额": "RRPONTSYD"
    }
    
    results = {}
    
    # OpenBB v4: obb.fixedincome.government.treasury_rates handles some? 
    # Or generically: obb.economy.fred_series(symbol=...)
    # obb.fixedincome.corporate.hqm? No.
    # Safe bet: obb.economy.fred_series (if exists) or fallback to simple direct request?
    # OpenBB v4 usually has `obb.index.economy.fred` or simply usage of `yfinance` tickers for some.
    # Let's try to map some to YFinance tickers if possible, or use FRED via OpenBB.
    # YFinance Tickers: 
    #   TIPS: ^TIP (ETF price != yield). 10Y Real Yield: ^DFII10 (Yahoo often has FRED codes with ^) => ^DFII10 ?? No.
    #   HY Spread: No direct YF.
    # Actually, recent OpenBB standardizes this. Let's try `obb.economy.fred_series` if available, or just fetch via akshare's FRED proxy if akshare has it? Akshare has `macro_usa_...`
    # Let's check AkShare for US macro. 
    # `ak.macro_usa_fred(symbol="...")` exists? No.
    # Assume OpenBB works. Provider 'fred' requires API key? OpenBB usually handles it if configured or uses a default key sometimes. 
    # If no key, it might fail.
    # Fallback/Safe: Some can be scraped or Akshare might have them.
    # Akshare: `macro_usa_tga`, `macro_usa_tips`.
    
    # 策略: 直接使用 FRED API (requests) 以保证稳定性，因为 User 提供了 Key
    fred_key = os.environ.get("FRED_API_KEY")
    if not fred_key:
        print("   ⚠️ 未检测到 FRED_API_KEY 环境变量，跳过 FRED 数据获取。")
        return {}

    # FRED API Config
    base_url = "https://api.stlouisfed.org/fred/series/observations"
    # 获取过去 ~60 天数据 (2 months requested)
    obs_start = (datetime.datetime.now() - datetime.timedelta(days=70)).strftime("%Y-%m-%d")
    
    for name, series_id in indicators.items():
        try:
            params = {
                "series_id": series_id,
                "api_key": fred_key,
                "file_type": "json",
                "observation_start": obs_start,
                "sort_order": "asc"
            }
            
            r = SESSION.get(base_url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data_json = r.json()
            
            observations = data_json.get("observations", [])
            if not observations:
                continue
                
            # Process observations
            clean_obs = []
            for obs in observations:
                val = obs.get("value")
                date_str = obs.get("date")
                if val == ".": continue # FRED missing value
                try:
                    f_val = float(val)
                    clean_obs.append({"date": date_str, "value": f_val})
                except:
                    pass
            
            if clean_obs:
                # 排序并取最新
                clean_obs.sort(key=lambda x: x["date"])
                latest = clean_obs[-1]
                
                # Format: List of history or just latest? 
                # Request says "fetch day by day data for recent 2 months"
                # But our report structure usually takes latest or list.
                # Let's return the simplified list [ {date, value}, ... ] stored in specific key
                
                # To fit into "usa" dict, we can store either the list or the latest value.
                # If we want to show a chart or series, list is better.
                # For now, let's store the full list under the key.
                
                # However, clean_obs is list of dicts.
                results[name] = clean_obs
                print(f"      OK: {name} ({len(clean_obs)} records)")
            
        except Exception as e:
            print(f"      Fail: {name} ({e})")
            
    return results

def fetch_china_macro_extra():
    """
    获取中国宏观: M1/M2, DR007
    """
    print("   -> 获取中国宏观扩展 (AkShare)...")
    res = {}
    try:
        # M1/M2
        df_m = ak.macro_china_money_supply()
        # Columns: 统计时间, 货币和准货币(M2)-数量(亿元), ...
        if not df_m.empty:
            df_m = df_m.sort_values("统计时间")
            latest = df_m.iloc[-1]
            # M2, M1
            # 字段名可能较长，需模糊匹配
            m2_val = 0
            m1_val = 0
            for c in df_m.columns:
                if "M2" in c and "数量" in c: m2_val = latest[c]
                if "M1" in c and "数量" in c: m1_val = latest[c]
            
            res["M1_M2"] = {
                "date": latest["统计时间"],
                "m2": m2_val,
                "m1": m1_val,
                "diff": float(m1_val) - float(m2_val) if m2_val and m1_val else 0 # 增速差? or value diff? Request asks for gap.
            }
            # Growth gap usually means (M1 Growth % - M2 Growth %)
            # Columns also have "同比增长"
            m2_growth = 0
            m1_growth = 0
            for c in df_m.columns:
                if "M2" in c and "同比" in c: m2_growth = latest[c]
                if "M1" in c and "同比" in c: m1_growth = latest[c]
            
            res["M1_M2_Growth_Gap"] = {
                "date": latest["统计时间"],
                "m1_growth": m1_growth,
                "m2_growth": m2_growth,
                "gap": float(m1_growth) - float(m2_growth)
            }
            
    except Exception as e:
        print(f"M1/M2 Error: {e}")
        
    try:
        # DR007: interbank_rate_open_cn 
        # But this might be huge. Let's try interbank_analysis_daily?
        pass
    except:
        pass
        
    return res
