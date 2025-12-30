#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
数据获取脚本 (离线优先版)
功能：获取全球主要经济体的宏观经济数据、汇率、债券及市场指数。
核心特性：
1. [优先级] 优先读取 manual_data.json，若存在则跳过在线获取。
2. [修复] VIX 获取报错及虚假成功日志。
3. [修复] 屏蔽 Pandas 日期解析警告。
4. [清洗] 所有数值强制保留两位小数。
5. [增强] 强制去除同一日期的重复数据，保留最新记录。
6. [统一] 中美日国债数据结构统一为宽表格式 ({"日期":..., "1年":..., "10年":...})。
"""

import json
import datetime
import time
import os
import pandas as pd
import yfinance as yf
import akshare as ak
import requests
import warnings
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 强力屏蔽 Pandas 和 Dateutil 的特定警告
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
warnings.filterwarnings("ignore", message=".*Could not infer format.*")

# ==============================================================================
# 全局配置与工具函数
# ==============================================================================

MANUAL_DATA_FILE = "manual_data.json"

def get_retry_session(retries=3, backoff_factor=1, status_forcelist=(500, 502, 503, 504)):
    """创建一个带有自动重试功能的 Requests Session"""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

SESSION = get_retry_session()
TIMEOUT = 15

# 加载手动数据缓存
MANUAL_DB = {}
if os.path.exists(MANUAL_DATA_FILE):
    try:
        with open(MANUAL_DATA_FILE, 'r', encoding='utf-8') as f:
            MANUAL_DB = json.load(f)
        print(f"已加载离线手动数据文件: {MANUAL_DATA_FILE}")
    except Exception as e:
        print(f"加载离线文件失败: {e}")
else:
    print(f"未找到离线文件 {MANUAL_DATA_FILE}，将仅依赖在线获取。")


def validate_recent_data(df, date_col="日期", days=90):
    """验证数据是否包含最近 N 天的记录"""
    if df is None or df.empty:
        return False
    try:
        # 兼容字典列表格式（如果是process_df后的结果）
        if isinstance(df, list) and len(df) > 0:
            last_record = df[-1]
            if date_col in last_record:
                latest_date = pd.to_datetime(last_record[date_col])
            else:
                return True
        elif isinstance(df, pd.DataFrame):
            if date_col not in df.columns:
                return True
            latest_date_str = df[date_col].max()
            latest_date = pd.to_datetime(latest_date_str)
        else:
            return False
            
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
        
        if latest_date < cutoff_date:
            print(f"   [警告] 数据过旧 (最新: {latest_date.strftime('%Y-%m-%d')})，触发备选接口...")
            return False
        return True
    except Exception as e:
        # 验证出错时不阻断流程
        return True

def call_with_retry(func, name, fallback_func=None, retries=3, check_freshness=False, *args, **kwargs):
    """通用重试与备选调用函数"""
    for i in range(retries):
        try:
            print(f"[{name}] 正在获取 (尝试 {i+1}/{retries})...")
            result = func(*args, **kwargs)
            
            if result is None or (isinstance(result, pd.DataFrame) and result.empty):
                raise ValueError("返回数据为空")
            
            if check_freshness:
                if not validate_recent_data(result):
                    raise ValueError("数据未包含近3个月记录")
            
            print(f"[{name}] 获取成功。")
            return result
        except Exception as e:
            if i == retries - 1:
                print(f"[{name}] 尝试失败: {e}")
            time.sleep(2)
    
    if fallback_func:
        print(f"[{name}] 主接口失败/数据过旧，尝试备选接口 (Fallback)...")
        try:
            result = fallback_func()
            if result is not None and not (isinstance(result, pd.DataFrame) and result.empty):
                print(f"[{name}] 备选接口获取成功。")
                return result
        except Exception as e:
            print(f"[{name}] 备选接口也失败了: {e}")
            
    print(f"[{name}] 所有途径均无法获取有效数据。")
    return pd.DataFrame()

def round_data(data):
    """递归将数据中的浮点数保留两位小数"""
    if isinstance(data, list):
        return [round_data(x) for x in data]
    elif isinstance(data, dict):
        return {k: round_data(v) for k, v in data.items()}
    elif isinstance(data, float):
        return round(data, 2)
    return data

# ==============================================================================
# 数据获取逻辑
# ==============================================================================

def fetch_jin10_base(attr_id, start_date_str):
    headers = {
        "user-agent": "Mozilla/5.0",
        "x-app-id": "rU6QIu7JHe2gOUeR",
        "x-version": "1.0.0",
    }
    url = "https://datacenter-api.jin10.com/reports/list_v2"
    params = {
        "max_date": "",
        "category": "ec",
        "attr_id": str(attr_id),
        "_": str(int(round(time.time() * 1000))),
    }
    
    all_dfs = []
    
    for _ in range(5):
        try:
            r = SESSION.get(url, params=params, headers=headers, timeout=TIMEOUT)
            data_json = r.json()
            if "data" not in data_json or "values" not in data_json["data"] or not data_json["data"]["values"]:
                break
            
            temp_df = pd.DataFrame(data_json["data"]["values"])
            temp_df.columns = ["日期", "今值", "预测值", "前值"]
            
            cols = ["今值", "预测值", "前值"]
            for col in cols:
                temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')

            temp_df["日期"] = pd.to_datetime(temp_df["日期"])
            
            mask = temp_df["日期"] >= pd.to_datetime(start_date_str)
            valid_data = temp_df[mask]
            
            if not valid_data.empty:
                all_dfs.append(valid_data)
            
            min_date_in_page = temp_df["日期"].min()
            if min_date_in_page < pd.to_datetime(start_date_str):
                break
                
            last_date_str = temp_df.iloc[-1]["日期"].strftime("%Y-%m-%d")
            next_max_date = (pd.to_datetime(last_date_str) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            params["max_date"] = next_max_date
            
        except Exception:
            break

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df["日期"] = final_df["日期"].dt.strftime('%Y-%m-%d')
        final_df.sort_values("日期", inplace=True)
        return final_df
    return pd.DataFrame()

# Fallback functions
def fetch_china_cpi_fallback():
    try: return ak.macro_china_cpi_monthly()
    except: return pd.DataFrame()

def fetch_china_ppi_fallback():
    try: return ak.macro_china_ppi_yearly()
    except: return pd.DataFrame()

def fetch_china_pmi_fallback():
    try: return ak.macro_china_pmi_yearly()
    except: return pd.DataFrame()

def fetch_usa_cpi_fallback():
    try: return ak.macro_usa_cpi_monthly()
    except: return pd.DataFrame()

def fetch_usa_unemployment_fallback():
    try: return ak.macro_usa_unemployment_rate()
    except: return pd.DataFrame()

def fetch_latest_yf(ticker, name):
    """yfinance 获取最新一天的数据"""
    try:
        # 增加容错，防止 ticker 初始化失败
        t = yf.Ticker(ticker)
        hist = t.history(period="5d")
        
        if hist is None or hist.empty:
            return []
        
        latest = hist.iloc[-1]
        date_str = latest.name.strftime('%Y-%m-%d')
        val = float(latest['Close'])
        
        return [{"日期": date_str, "最新值": val, "名称": name}]
    except Exception as e:
        print(f"Error fetching {name} (yfinance): {e}")
        return []

def fetch_us_bond_yields_unified():
    """获取美国国债数据并转换为统一的宽表格式"""
    # 映射关系：Key为输出的列名，Value为Yahoo Finance的代码
    tickers_map = {
        "13周": "^IRX",
        "5年": "^FVX",
        "10年": "^TNX",
        "30年": "^TYX"
    }
    
    # 暂存所有结果
    temp_results = {}
    latest_date = None
    
    for label, ticker in tickers_map.items():
        data = fetch_latest_yf(ticker, label)
        if data:
            item = data[0]
            current_date = item["日期"]
            val = item["最新值"]
            
            # 记录最新的日期
            if latest_date is None or current_date > latest_date:
                latest_date = current_date
            
            # 如果日期不是同一天，可能会有数据对齐问题，这里简化处理，
            # 假设最新获取的都是最近一个交易日的数据。
            # 严格来说应该按日期合并，但这里为了保持宽表结构简单，我们以最新日期为准。
            temp_results[label] = val

    if not temp_results or not latest_date:
        return []

    # 构造宽表的一行数据
    row = {"日期": latest_date}
    row.update(temp_results)
    
    return [row]

def fetch_china_lpr_optimized():
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPTA_WEB_RATE",
        "columns": "ALL",
        "sortColumns": "TRADE_DATE",
        "sortTypes": "-1",
        "pageNumber": "1",
        "pageSize": "50",
        "source": "WEB",
        "client": "WEB",
    }
    try:
        r = SESSION.get(url, params=params, timeout=TIMEOUT)
        data_json = r.json()
        if not data_json.get("result"): return pd.DataFrame()
        df = pd.DataFrame(data_json["result"]["data"])
        df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"]).dt.strftime('%Y-%m-%d')
        return df
    except Exception:
        raise

def fetch_china_money_supply_custom():
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_ECONOMY_CURRENCY_SUPPLY", 
        "columns": "REPORT_DATE,TIME,M2,M2_SAME,M2_SEQUENTIAL,M1,M1_SAME,M1_SEQUENTIAL,M0,M0_SAME,M0_SEQUENTIAL",
        "pageNumber": "1",
        "pageSize": "50",
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "source": "WEB",
        "client": "WEB",
    }
    try:
        r = SESSION.get(url, params=params, timeout=TIMEOUT)
        data_json = r.json()
        if not data_json.get("result"): return pd.DataFrame()
        df = pd.DataFrame(data_json["result"]["data"])
        df.rename(columns={"TIME": "日期","M2": "M2(亿元)", "M2_SAME": "M2同比增长","M1": "M1(亿元)", "M1_SAME": "M1同比增长","M0": "M0(亿元)", "M0_SAME": "M0同比增长"}, inplace=True)
        df["日期"] = pd.to_datetime(df["REPORT_DATE"]).dt.strftime('%Y-%m-%d')
        return df
    except Exception:
        raise

def fetch_china_bond_yield_fallback():
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_BOND_YIELD_CURVE",
        "columns": "TRADE_DATE,YIELD_1Y,YIELD_2Y,YIELD_10Y,YIELD_30Y",
        "filter": '(CURVE_TYPE="0")(IS_DISTINCT="1")', 
        "pageNumber": "1",
        "pageSize": "50",
        "sortColumns": "TRADE_DATE",
        "sortTypes": "-1",
        "source": "WEB",
        "client": "WEB",
    }
    try:
        r = SESSION.get(url, params=params, timeout=TIMEOUT)
        data_json = r.json()
        df = pd.DataFrame(data_json["result"]["data"])
        df.rename(columns={"TRADE_DATE": "日期","YIELD_1Y": "1年", "YIELD_2Y": "2年", "YIELD_10Y": "10年", "YIELD_30Y": "30年"}, inplace=True)
        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime('%Y-%m-%d')
        return df
    except Exception:
        raise

def fetch_japan_bond_yield_latest():
    """获取日本国债（主要是10年期）并转为宽表格式"""
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_ECONOMICVALUE_JPAN",
        "columns": "REPORT_DATE,PUBLISH_DATE,VALUE,PRE_VALUE",
        "filter": '(INDICATOR_ID="EMG00341609")', 
        "pageNumber": "1",
        "pageSize": "50",
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "source": "WEB",
        "client": "WEB",
    }
    try:
        r = SESSION.get(url, params=params, timeout=TIMEOUT)
        data_json = r.json()
        if not data_json.get("result"): return pd.DataFrame()
        df = pd.DataFrame(data_json["result"]["data"])
        
        # 转换列名以匹配宽表结构
        df.rename(columns={"PUBLISH_DATE": "日期", "VALUE": "10年"}, inplace=True)
        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime('%Y-%m-%d')
        
        # 只保留需要的列
        return df[["日期", "10年"]]
    except Exception:
        raise

def get_southbound_fund_flow_safe():
    try:
        df = ak.stock_hsgt_fund_flow_summary_em()
        if df is not None and not df.empty:
            south_df = df[df['资金方向'] == '南向资金'].copy()
            south_df.rename(columns={'交易日': '日期', '资金净流入': '净流入(亿元)'}, inplace=True)
            south_df['日期'] = pd.to_datetime(south_df['日期']).dt.strftime('%Y-%m-%d')
            return south_df[['日期', '净流入(亿元)']]
    except Exception:
        raise
    return pd.DataFrame()

# ==============================================================================
# JSON 保存逻辑
# ==============================================================================

def save_custom_json(data, filename):
    rounded_data = round_data(data)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write('{\n')
        categories = list(rounded_data.keys())
        for i, category in enumerate(categories):
            f.write(f'    "{category}": {{\n')
            indicators = list(rounded_data[category].keys())
            for j, indicator in enumerate(indicators):
                records = rounded_data[category][indicator]
                f.write(f'        "{indicator}": [\n')
                for k, record in enumerate(records):
                    record_str = json.dumps(record, ensure_ascii=False)
                    comma = "," if k < len(records) - 1 else ""
                    f.write(f'            {record_str}{comma}\n')
                comma_outer = "," if j < len(indicators) - 1 else ""
                f.write(f'        ]{comma_outer}\n')
            comma_cat = "," if i < len(categories) - 1 else ""
            f.write(f'    }}{comma_cat}\n')
        f.write('}')

# ==============================================================================
# 主逻辑
# ==============================================================================

def process_df(df, date_col="日期", value_cols=None, start_date_dt=None):
    if df is None or df.empty:
        return []
    
    if date_col not in df.columns:
        possible_dates = [c for c in df.columns if "date" in c.lower() or "日期" in c or "时间" in c or "TIME" in c]
        if possible_dates:
            date_col = possible_dates[0]
        else:
            return []
    
    try:
        df = df.copy()
        if df[date_col].dtype == object:
            df[date_col] = df[date_col].astype(str).str.replace('年', '-').str.replace('月', '-').str.replace('日', '')
        
        # 强制 coerce 错误，并忽略警告
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
        df = df.dropna(subset=[date_col])
        
        if start_date_dt:
            mask = df[date_col] >= start_date_dt
            filtered = df.loc[mask].copy()
        else:
            filtered = df.copy()

        filtered[date_col] = filtered[date_col].dt.strftime('%Y-%m-%d')
        
        # 核心修复：按日期去重，保留最后一条数据
        # 这解决了“同一天有多个数据”或数据源返回多次更新数据的问题
        filtered = filtered.drop_duplicates(subset=[date_col], keep='last')
        
        if value_cols:
            valid_cols = [date_col] + [c for c in value_cols if c in filtered.columns]
            filtered = filtered[valid_cols]
        
        return filtered.to_dict(orient="records")
    except Exception as e:
        print(f"   [Error] Process DF: {e}")
        return []

def get_manual_fallback(category, indicator):
    """从全局 MANUAL_DB 中获取数据"""
    if MANUAL_DB and category in MANUAL_DB and indicator in MANUAL_DB[category]:
        data = MANUAL_DB[category][indicator]
        if data:
            return data
    return []

def get_data_main():
    """对外接口：获取所有宏观经济与市场数据"""
    end_date_dt = datetime.datetime.now()
    start_date_dt = end_date_dt - datetime.timedelta(days=185)
    start_date_str = start_date_dt.strftime("%Y-%m-%d")
    
    start_date_ak = start_date_dt.strftime("%Y%m%d")
    end_date_ak = end_date_dt.strftime("%Y%m%d")

    data_store = {"market_fx": {}, "china": {}, "usa": {}, "japan": {}}
    print(">>> 开始数据获取任务 (离线优先版)...")

    # -------------------------------------------------------------------------
    # 1. 市场指数与汇率
    # -------------------------------------------------------------------------
    print("\n--- 正在获取: VIX, 汇率 (最新值) ---")
    tickers = {
        "VIX恐慌指数": "^VIX",
        "美元/人民币": "CNY=X",
        "美元/日元": "JPY=X",
        "美元/越南盾": "VND=X"
    }
    
    for name, ticker in tickers.items():
        # 优先级 1: 离线手动
        manual_res = get_manual_fallback("market_fx", name)
        if manual_res:
            res = manual_res
            print(f"[{name}] 优先使用离线手动数据。")
        else:
            # 优先级 2: 在线
            res = fetch_latest_yf(ticker, name)
            if not res:
                print(f"[{name}] 获取失败 (无在线及离线数据)。")
            else:
                print(f"[{name}] 获取成功。")
        
        if res:
            data_store["market_fx"][name] = res

    # -------------------------------------------------------------------------
    # 2. 中国数据
    # -------------------------------------------------------------------------
    print("\n--- 正在获取: 中国经济数据 ---")
    china = data_store["china"]
    
    def check_and_store(cat, key, fetch_call, **kwargs):
        # 优先级 1: 离线手动
        manual_res = get_manual_fallback(cat, key)
        if manual_res:
            print(f"[{key}] 优先使用离线手动数据。")
            return manual_res

        # 优先级 2: 在线
        df = call_with_retry(fetch_call, key, **kwargs)
        res = process_df(df, start_date_dt=start_date_dt)
        return res

    # CPI
    china["CPI"] = check_and_store("china", "CPI", lambda: fetch_jin10_base(72, start_date_str), 
                                   fallback_func=fetch_china_cpi_fallback, check_freshness=True)
    # PPI
    china["PPI"] = check_and_store("china", "PPI", lambda: fetch_jin10_base(60, start_date_str),
                                   fallback_func=fetch_china_ppi_fallback, check_freshness=True)
    # PMI
    china["PMI_制造业"] = check_and_store("china", "PMI_制造业", lambda: fetch_jin10_base(65, start_date_str),
                                       fallback_func=fetch_china_pmi_fallback, check_freshness=True)
    # LPR
    res_lpr = get_manual_fallback("china", "LPR")
    if res_lpr:
        print("[中国LPR] 优先使用离线手动数据。")
    else:
        df_lpr = call_with_retry(fetch_china_lpr_optimized, "中国LPR", check_freshness=True)
        res_lpr = process_df(df_lpr, date_col="TRADE_DATE", start_date_dt=start_date_dt)
    china["LPR"] = res_lpr

    # 货币供应量
    china["货币供应量"] = check_and_store("china", "货币供应量", fetch_china_money_supply_custom, check_freshness=True)

    # 国债 (统一宽表格式 Key: 国债收益率)
    def fetch_bond_ak_wrapper():
        return ak.bond_china_yield(start_date=start_date_ak, end_date=end_date_ak)
    
    res_bond = get_manual_fallback("china", "国债收益率")
    if res_bond:
        print("[中国国债] 优先使用离线手动数据。")
    else:
        df_bond = call_with_retry(fetch_bond_ak_wrapper, "中国国债", fallback_func=fetch_china_bond_yield_fallback, check_freshness=True)
        res_bond = process_df(df_bond, value_cols=['1年', '2年', '10年', '30年'], start_date_dt=start_date_dt)
    china["国债收益率"] = res_bond

    # 南向资金
    res_south = get_manual_fallback("china", "南向资金净流入")
    if res_south:
        print("[南向资金] 优先使用离线手动数据。")
    else:
        df_south = call_with_retry(get_southbound_fund_flow_safe, "南向资金", check_freshness=True)
        res_south = process_df(df_south, date_col="日期", start_date_dt=start_date_dt)
    china["南向资金净流入"] = res_south

    # -------------------------------------------------------------------------
    # 3. 美国数据
    # -------------------------------------------------------------------------
    print("\n--- 正在获取: 美国经济数据 ---")
    usa = data_store["usa"]

    usa["失业率"] = check_and_store("usa", "失业率", lambda: fetch_jin10_base(47, start_date_str), 
                                  fallback_func=fetch_usa_unemployment_fallback, check_freshness=True)
    
    usa["CPI"] = check_and_store("usa", "CPI", lambda: fetch_jin10_base(9, start_date_str), 
                                 fallback_func=fetch_usa_cpi_fallback, check_freshness=True)

    # 美国国债 (统一宽表格式 Key: 国债收益率)
    res_bonds = get_manual_fallback("usa", "国债收益率")
    if res_bonds:
        print("[美国国债] 优先使用离线手动数据。")
    else:
        print("[美国国债] 正在获取最新值 (在线)...")
        res_bonds = fetch_us_bond_yields_unified()
    usa["国债收益率"] = res_bonds

    # Jin10 数据
    usa["ISM_制造业PMI"] = check_and_store("usa", "ISM_制造业PMI", lambda: fetch_jin10_base(28, start_date_str), check_freshness=True)
    usa["ISM_非制造业PMI"] = check_and_store("usa", "ISM_非制造业PMI", lambda: fetch_jin10_base(29, start_date_str), check_freshness=True)
    usa["非农就业人数"] = check_and_store("usa", "非农就业人数", lambda: fetch_jin10_base(33, start_date_str), check_freshness=True)
    usa["零售销售月率"] = check_and_store("usa", "零售销售月率", lambda: fetch_jin10_base(39, start_date_str), check_freshness=True)
    usa["利率决议"] = check_and_store("usa", "利率决议", lambda: fetch_jin10_base(24, start_date_str), check_freshness=True)

    # -------------------------------------------------------------------------
    # 4. 日本数据
    # -------------------------------------------------------------------------
    print("\n--- 正在获取: 日本经济数据 ---")
    japan = data_store["japan"]

    # 央行利率
    def fetch_japan_rate_safe():
        try: return ak.macro_japan_bank_rate()
        except: return fetch_jin10_base(22, start_date_str)
    
    japan["央行利率"] = check_and_store("japan", "央行利率", fetch_japan_rate_safe, check_freshness=True)

    # 国债 (统一宽表格式 Key: 国债收益率)
    res_jp_bond = get_manual_fallback("japan", "国债收益率")
    if res_jp_bond:
        print("[日本国债] 优先使用离线手动数据。")
    else:
        df_jp_bond = call_with_retry(fetch_japan_bond_yield_latest, "日本国债10Y", check_freshness=True)
        res_jp_bond = process_df(df_jp_bond, value_cols=['10年'], start_date_dt=start_date_dt)
    
    japan["国债收益率"] = res_jp_bond

    return data_store

if __name__ == "__main__":
    try:
        data = get_data_main()
    except Exception as e:
        print(f"严重错误: {e}")
        data = {}

    output_file = "data.json"
    try:
        save_custom_json(data, output_file)
        print(f"\n全部完成！数据已保存至 {output_file}")
    except Exception as e:
        print(f"保存失败: {e}")