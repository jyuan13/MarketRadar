import json
import os
import sys
import time
import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from itertools import groupby

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import fetch_data
import MarketRadar
import utils
import scrape_economy_selenium
# 引入 fetch_data_core 以直接调用新功能
import fetch_data_core

OUTPUT_FILENAME = "MarketRadar_Report.json"
LOG_FILENAME = "market_data_status.txt"
TZ_CN = ZoneInfo("Asia/Shanghai")

# 定义报告的时间范围（用于截取最终展示的数据）
# 计算均线需要更长的数据，但报告只展示近期
REPORT_DAYS = 20

class NpEncoder(json.JSONEncoder):
    """
    专门解决 'Object of type int64 is not JSON serializable' 错误的编码器
    """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

def print_banner():
    print(r"""
  __  __            _        _   ____          _            
 |  \/  | __ _ _ __| | _____| |_|  _ \ __ _ __| | __ _ _ __ 
 | |\/| |/ _` | '__| |/ / _ \ __| |_) / _` / _` |/ _` | '__|
 | |  | | (_| | |  |   <  __/ |_|  _ < (_| (_| | (_| | |   
 |_|  |_|\__,_|_|  |_|\_\___|\__|_| \_\__,_\__,_|\__,_|_|   
                                                            
    """)

def clean_and_round(data):
    if isinstance(data, dict):
        return {k: clean_and_round(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_and_round(x) for x in data]
    elif isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return None
        return round(data, 2)
    elif isinstance(data, (np.int64, np.int32)):
        return int(data)
    else:
        return data

def deep_merge(dict1, dict2):
    result = dict1.copy()
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result

def merge_final_report(macro_data_combined, kline_data_dict, ma_data_dict, kcb50_data=None):
    """
    整合所有模块的数据
    ma_data_dict: {"general": [...], "commodities": [...]}
    kcb50_data: 新增的科创50独立板块
    """
    merged = {
        "meta": kline_data_dict.get("meta", {}),
        "技术分析": {
            "指数+个股日均线": ma_data_dict.get("general", []),
            "大宗商品": ma_data_dict.get("commodities", [])
        },
        "market_fx": macro_data_combined.get("market_fx", {}),
        "科创50": kcb50_data if kcb50_data else {},  # 新增顶层项
        "china": macro_data_combined.get("china", {}),
        "usa": macro_data_combined.get("usa", {}),
        "japan": macro_data_combined.get("japan", {}),
        "hk": macro_data_combined.get("hk", {}), 
        "market_klines": kline_data_dict.get("data", {})
    }
    
    merged["meta"]["generated_at"] = datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")
    merged["meta"]["description"] = "MarketRadar Consolidated Report (Selenium Macro + Online FX + Klines)"
    
    return merged

def save_compact_json(data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('{\n')
            keys = list(data.keys())
            for i, key in enumerate(keys):
                val = data[key]
                f.write(f'    "{key}": ')
                if isinstance(val, dict):
                    f.write('{\n')
                    sub_keys = list(val.keys())
                    for j, sub_key in enumerate(sub_keys):
                        sub_val = val[sub_key]
                        f.write(f'        "{sub_key}": ')
                        if isinstance(sub_val, list):
                            f.write('[\n')
                            for k, item in enumerate(sub_val):
                                # 使用 NpEncoder 解决 int64 序列化错误
                                item_str = json.dumps(item, ensure_ascii=False, cls=NpEncoder)
                                comma = "," if k < len(sub_val) - 1 else ""
                                f.write(f'            {item_str}{comma}\n')
                            f.write('        ]')
                        else:
                            f.write(json.dumps(sub_val, ensure_ascii=False, cls=NpEncoder))
                        if j < len(sub_keys) - 1: f.write(',\n')
                        else: f.write('\n')
                    f.write('    }')
                else:
                    f.write(json.dumps(val, ensure_ascii=False, cls=NpEncoder))
                if i < len(keys) - 1: f.write(',\n')
                else: f.write('\n')
            f.write('}')
        print(f"\n✅ 成功! 报告已写入 {filename}")
        return True
    except Exception as e:
        print(f"\n❌ 写入失败: {e}")
        return False

def write_status_log(logs, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"MarketRadar Data Fetch Log - {datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*60 + "\n")
            
            for log in logs:
                status_str = "[PASS]" if log['status'] else "[FAIL]"
                timestamp = datetime.now(TZ_CN).strftime('%H:%M:%S')
                line = f"[{timestamp}] {status_str} {log['name']}"
                if not log['status'] and log['error']:
                    line += f" | Error: {log['error']}"
                f.write(line + "\n")
        print(f"📝 状态日志已写入: {filename}")
        return True
    except Exception as e:
        print(f"❌ 日志写入失败: {e}")
        return False

def generate_signals_summary(ma_data_dict):
    """
    生成技术指标信号摘要
    """
    lines = []
    
    # 合并两个列表
    all_ma = ma_data_dict.get("general", []) + ma_data_dict.get("commodities", [])
    
    signals_found = False
    lines.append("\n📈 技术指标信号扫描:")
    lines.append("-" * 30)
    
    for item in all_ma:
        # [修改] 优先获取 "名称" (中文Key)
        name = item.get('名称', item.get('name', 'Unknown'))
        signals = item.get('Signals', [])
        
        # [修改] 过滤掉 "无特殊技术形态"，不在摘要中显示
        active_signals = [s for s in signals if s != "无特殊技术形态"]
        
        if active_signals:
            signals_found = True
            lines.append(f"🔴 [{name}]: {', '.join(active_signals)}")
            
    if not signals_found:
        lines.append("今日无特殊技术信号。")
        
    return "\n".join(lines)

def generate_email_body_summary(logs, signal_summary):
    lines = ["📊 数据获取状态汇总:"]
    lines.append("-" * 30)
    
    success_count = sum(1 for l in logs if l['status'])
    fail_count = sum(1 for l in logs if not l['status'])
    
    lines.append(f"总计: {len(logs)} | 成功: {success_count} | 失败: {fail_count}")
    lines.append("")
    
    for log in logs:
        status_icon = "✅" if log['status'] else "❌"
        lines.append(f"{status_icon} {log['name']}")
    
    lines.append("\n" + signal_summary)
    
    return "\n".join(lines)

def parse_chinese_date(date_str):
    """
    解析 'YYYY年MM月DD日' 或 'YYYY-MM-DD' 格式的日期
    """
    try:
        if '年' in str(date_str):
            return datetime.strptime(str(date_str).strip(), '%Y年%m月%d日')
        return pd.to_datetime(date_str)
    except:
        return pd.to_datetime(date_str, errors='coerce')

def main():
    start_time = time.time()
    print_banner()
    print("🚀 MarketRadar 启动主程序 (Integrated Version)...")
    
    all_status_logs = []

    print("\n[Step 1/4] 获取汇率与国债数据 (fetch_data)...")
    try:
        base_macro, logs_fx = fetch_data.get_market_fx_and_bonds()
        all_status_logs.extend(logs_fx)
    except Exception as e:
        print(f"❌ fetch_data 失败: {e}")
        base_macro = {"market_fx": {}, "china": {}, "usa": {}, "japan": {}}
        all_status_logs.append({'name': 'fetch_data_module', 'status': False, 'error': str(e)})

    print("\n[Step 2/4] 抓取宏观经济指标 (Selenium)...")
    try:
        selenium_macro, logs_selenium = scrape_economy_selenium.get_macro_data()
        all_status_logs.extend(logs_selenium)
    except Exception as e:
        print(f"❌ Selenium 抓取失败 (可能是环境问题): {e}")
        selenium_macro = {}
        all_status_logs.append({'name': 'selenium_module', 'status': False, 'error': str(e)})

    combined_macro = deep_merge(base_macro, selenium_macro)

    print("\n[Step 3/4] 获取 K线数据 & 计算均线 & 技术指标...")
    try:
        kline_result, logs_klines = MarketRadar.get_all_kline_data()
        all_status_logs.extend(logs_klines)
        
        kline_data_dict = {"meta": kline_result.get("meta"), "data": kline_result.get("data")}
        ma_data_dict = kline_result.get("ma_data", {"general": [], "commodities": []})
        
        count_general = len(ma_data_dict.get("general", []))
        count_comm = len(ma_data_dict.get("commodities", []))
        print(f"✅ 获取到均线数据: 通用 {count_general} 条, 大宗商品 {count_comm} 条")
    except Exception as e:
        print(f"❌ 获取K线数据失败: {e}")
        kline_data_dict = {"meta": {}, "data": {}}
        ma_data_dict = {"general": [], "commodities": []}
        all_status_logs.append({'name': 'kline_module', 'status': False, 'error': str(e)})

    # [Step 3.5] 处理恒生医疗保健指数
    hshci_key = "恒生医疗保健指数"
    hk_data = combined_macro.get("hk", {})
    
    if "data" in kline_data_dict and kline_data_dict["data"]:
        if hshci_key in kline_data_dict["data"]:
            del kline_data_dict["data"][hshci_key]
            print(f"🧹 已从 market_klines 字段移除 {hshci_key} (仅保留 hk 字段数据，防止双份输出)")

    if hshci_key in hk_data and hk_data[hshci_key]:
        print(f"\n[Step 3.5] ⚡ 正在基于 Selenium 数据计算 {hshci_key} 均线...")
        try:
            raw_data = hk_data[hshci_key]
            df_hshci = pd.DataFrame(raw_data)
            
            if '日期' in df_hshci.columns:
                df_hshci['date'] = df_hshci['日期'].apply(parse_chinese_date)
            elif 'date' in df_hshci.columns:
                df_hshci['date'] = pd.to_datetime(df_hshci['date'])
            
            df_hshci['name'] = hshci_key
            for col in ['close', 'open', 'high', 'low', 'volume']:
                if col in df_hshci.columns:
                    df_hshci[col] = pd.to_numeric(df_hshci[col], errors='coerce')

            if 'date' in df_hshci.columns:
                 hshci_ma_list = utils.calculate_ma(df_hshci)
                 if hshci_ma_list:
                     ma_data_dict["general"].extend(hshci_ma_list)
                     print(f"✅ {hshci_key} 均线计算完成")
                 
                 cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=REPORT_DAYS)
                 df_slice = df_hshci[df_hshci['date'] >= cutoff_date].copy()
                 df_slice['date'] = df_slice['date'].dt.strftime('%Y-%m-%d')
                 
                 sliced_records = df_slice.to_dict(orient='records')
                 combined_macro['hk'][hshci_key] = sliced_records
                 print(f"✂️ {hshci_key} 数据已切片 (保留最近 {len(sliced_records)} 条)")

        except Exception as e_ma:
             print(f"⚠️ {hshci_key} 均线计算或切片失败: {e_ma}")

    # [Step 4/4] 获取越南胡志明指数 (Investing.com)...
    print("\n[Step 4/4] 获取越南胡志明指数 (Investing.com)...")
    try:
        vni_data, vni_err = fetch_data.fetch_vietnam_index_klines()
        if vni_data:
            if "data" not in kline_data_dict or kline_data_dict["data"] is None:
                kline_data_dict["data"] = {}
                
            kline_data_dict["data"]["越南胡志明指数"] = vni_data
            
            try:
                df_vni = pd.DataFrame(vni_data)
                df_vni['name'] = "越南胡志明指数"
                
                vni_ma_list = utils.calculate_ma(df_vni)
                if vni_ma_list:
                    ma_data_dict["general"].extend(vni_ma_list)
                    print(f"✅ 越南胡志明指数获取成功 ({len(vni_data)} 条记录) & 均线已计算")
                else:
                    print(f"✅ 越南胡志明指数获取成功 ({len(vni_data)} 条记录) (均线计算无结果)")
                
                all_status_logs.append({'name': '越南胡志明指数', 'status': True, 'error': None})
                
            except Exception as e_ma:
                print(f"⚠️ 越南数据获取成功但均线计算失败: {e_ma}")
                all_status_logs.append({'name': '越南胡志明指数', 'status': True, 'error': f"MA Error: {e_ma}"})
            
        else:
            all_status_logs.append({'name': '越南胡志明指数', 'status': False, 'error': vni_err})
            print(f"❌ 越南胡志明指数获取失败: {vni_err}")
    except Exception as e:
        print(f"❌ 越南指数模块异常: {e}")
        all_status_logs.append({'name': 'vni_module', 'status': False, 'error': str(e)})

    # [Step 4.5] 处理 A股指数 (新增逻辑)
    # 从 combined_macro 中提取，并计算均线
    ashare_list = combined_macro.get("market_klines", {}).pop("A股指数", None) # Pop to remove from raw macro data
    if ashare_list:
        print(f"\n[Step 4.5] ⚡ 正在计算 A股指数 均线...")
        # ashare_list 是扁平列表: [{date, name, close...}, ...]
        # 按 name 分组处理
        try:
            # Sort by name first for groupby
            ashare_list.sort(key=lambda x: x['name'])
            for name, group in groupby(ashare_list, key=lambda x: x['name']):
                records = list(group)
                # Sort by date
                records.sort(key=lambda x: x['date'])
                
                df_ashare = pd.DataFrame(records)
                df_ashare['date'] = pd.to_datetime(df_ashare['date'])
                
                # Ensure numeric columns
                cols = ['close', 'open', 'high', 'low', 'volume']
                for c in cols:
                    if c in df_ashare.columns:
                        df_ashare[c] = pd.to_numeric(df_ashare[c], errors='coerce')
                    
                # Calculate MA
                ma_res = utils.calculate_ma(df_ashare)
                if ma_res:
                    ma_data_dict["general"].extend(ma_res)
                
                # Prepare for K-line data storage (convert date back to string)
                df_ashare['date'] = df_ashare['date'].dt.strftime('%Y-%m-%d')
                
                # Update kline_data_dict
                if "data" not in kline_data_dict:
                     kline_data_dict["data"] = {}
                kline_data_dict["data"][name] = df_ashare.to_dict(orient='records')
                
                print(f"   Processed {name}: {len(records)} records")
        except Exception as e:
            print(f"⚠️ A股指数处理失败: {e}")

    # [Step 4.6] 获取 60分钟K线 (科创50 & 恒生科技)
    print("\n[Step 4.6] 获取 60分钟K线 (科创50 & 恒生科技)...")
    kcb50_dict = {}
    
    # 1. 科创50 60m
    try:
        kcb50_60m, err = fetch_data_core.fetch_kcb50_60m()
        if kcb50_60m:
            kcb50_dict["60分钟K线"] = kcb50_60m
            all_status_logs.append({'name': '科创50_60m', 'status': True, 'error': None})
        else:
            all_status_logs.append({'name': '科创50_60m', 'status': False, 'error': err})
    except Exception as e:
        print(f"⚠️ 科创50_60m 异常: {e}")
        
    # 2. 迁移原 China 下的科创50字段
    china_data = combined_macro.get("china", {})
    keys_to_move = ["科创50实时快照", "科创50融资融券", "科创50估值"]
    for k in keys_to_move:
        if k in china_data:
            kcb50_dict[k] = china_data.pop(k) # Move data
            
    # 3. 恒生科技 60m
    try:
        hstech_60m, err = fetch_data_core.fetch_hstech_60m()
        if hstech_60m:
            if "hk" not in combined_macro: combined_macro["hk"] = {}
            combined_macro["hk"]["恒生科技指数_60m"] = hstech_60m
            all_status_logs.append({'name': '恒生科技_60m', 'status': True, 'error': None})
        else:
            all_status_logs.append({'name': '恒生科技_60m', 'status': False, 'error': err})
    except Exception as e:
        print(f"⚠️ 恒生科技_60m 异常: {e}")

    # [Step 4.7] 获取六大银行 K线与均线
    print("\n[Step 4.7] 获取六大银行日线数据...")
    try:
        bank_dfs = fetch_data_core.fetch_us_banks_daily()
        for df in bank_dfs:
            name = df['name'].iloc[0]
            # 计算均线
            ma_res = utils.calculate_ma(df)
            if ma_res:
                ma_data_dict["general"].extend(ma_res)
            
            # 存储 K线 (切片)
            cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=REPORT_DAYS)
            df_slice = df[df['date'] >= cutoff_date].copy()
            df_slice['date'] = df_slice['date'].dt.strftime('%Y-%m-%d')
            
            if "data" not in kline_data_dict: kline_data_dict["data"] = {}
            kline_data_dict["data"][name] = df_slice.to_dict(orient='records')
            
            all_status_logs.append({'name': f"Bank_{name}", 'status': True, 'error': None})
            
    except Exception as e:
        print(f"⚠️ 六大银行数据获取异常: {e}")
        all_status_logs.append({'name': 'US_Banks', 'status': False, 'error': str(e)})

    print("\n[Step 5] 整合数据并清洗...")
    # 传入 kcb50_dict
    final_data = merge_final_report(combined_macro, kline_data_dict, ma_data_dict, kcb50_data=kcb50_dict)
    final_data = clean_and_round(final_data)

    success_names = set(log['name'] for log in all_status_logs if log.get('status'))
    cleaned_logs = []
    for log in all_status_logs:
        if log['status']:
            cleaned_logs.append(log)
        else:
            if log['name'] not in success_names:
                cleaned_logs.append(log)
    
    write_status_log(cleaned_logs, LOG_FILENAME)
    
    # 生成技术信号摘要
    signal_summary = generate_signals_summary(ma_data_dict)
    print(signal_summary)

    if save_compact_json(final_data, OUTPUT_FILENAME):
        try:
            email_subject = f"MarketRadar全量日报_{datetime.now(TZ_CN).strftime('%Y-%m-%d')}"
            base_body = f"生成时间: {datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}\n包含: 宏观, 汇率, K线(Stock/VNI/科创50/A股/银行), 信号扫描(MyTT)\n\n"
            
            email_body = generate_email_body_summary(cleaned_logs, signal_summary)
            email_body = base_body + email_body
            
            attachments = [OUTPUT_FILENAME, LOG_FILENAME]
            
            MarketRadar.send_email(email_subject, email_body, attachments)
        except Exception as e:
            print(f"⚠️ 邮件发送跳过或失败: {e}")

    print(f"\n✨ 任务完成，耗时: {time.time() - start_time:.2f} 秒")

if __name__ == "__main__":
    main()