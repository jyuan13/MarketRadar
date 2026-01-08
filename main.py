import json
import os
import sys
import time
import math
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import fetch_data
import MarketRadar
import utils
import scrape_economy_selenium

OUTPUT_FILENAME = "MarketRadar_Report.json"
LOG_FILENAME = "market_data_status.txt"
TZ_CN = ZoneInfo("Asia/Shanghai")

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

def merge_final_report(macro_data_combined, kline_data_dict, ma_data_dict):
    """
    整合所有模块的数据
    ma_data_dict: {"general": [...], "commodities": [...]}
    """
    merged = {
        "meta": kline_data_dict.get("meta", {}),
        "技术分析": {
            "指数+个股日均线": ma_data_dict.get("general", []),
            "大宗商品": ma_data_dict.get("commodities", [])
        },
        "market_fx": macro_data_combined.get("market_fx", {}),
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
                                item_str = json.dumps(item, ensure_ascii=False)
                                comma = "," if k < len(sub_val) - 1 else ""
                                f.write(f'            {item_str}{comma}\n')
                            f.write('        ]')
                        else:
                            f.write(json.dumps(sub_val, ensure_ascii=False))
                        if j < len(sub_keys) - 1: f.write(',\n')
                        else: f.write('\n')
                    f.write('    }')
                else:
                    f.write(json.dumps(val, ensure_ascii=False))
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

def generate_email_body_summary(logs):
    lines = ["数据获取状态汇总:"]
    lines.append("-" * 30)
    
    success_count = sum(1 for l in logs if l['status'])
    fail_count = sum(1 for l in logs if not l['status'])
    
    lines.append(f"总计: {len(logs)} | 成功: {success_count} | 失败: {fail_count}")
    lines.append("")
    
    for log in logs:
        status_icon = "✅" if log['status'] else "❌"
        lines.append(f"{status_icon} {log['name']}")
    
    return "\n".join(lines)

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

    print("\n[Step 3/4] 获取 K线数据 & 计算均线...")
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
                df_hshci.rename(columns={'日期': 'date'}, inplace=True)
            
            df_hshci['name'] = hshci_key
            
            for col in ['close', 'open', 'high', 'low', 'volume']:
                if col in df_hshci.columns:
                    df_hshci[col] = pd.to_numeric(df_hshci[col], errors='coerce')

            if 'date' in df_hshci.columns:
                 df_hshci['date'] = pd.to_datetime(df_hshci['date'])
                 hshci_ma_list = utils.calculate_ma(df_hshci)
                 if hshci_ma_list:
                     ma_data_dict["general"].extend(hshci_ma_list)
                     print(f"✅ {hshci_key} 均线计算完成")
        except Exception as e_ma:
             print(f"⚠️ {hshci_key} 均线计算失败: {e_ma}")

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

    print("\n[Step 5] 整合数据并清洗...")
    final_data = merge_final_report(combined_macro, kline_data_dict, ma_data_dict)
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

    if save_compact_json(final_data, OUTPUT_FILENAME):
        try:
            email_subject = f"MarketRadar全量日报_{datetime.now(TZ_CN).strftime('%Y-%m-%d')}"
            base_body = f"生成时间: {datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}\n包含: 宏观(Selenium), 汇率/国债(Online), K线(Stock/VNI/科创50)\n\n"
            status_body = generate_email_body_summary(cleaned_logs)
            email_body = base_body + status_body
            
            attachments = [OUTPUT_FILENAME, LOG_FILENAME]
            
            MarketRadar.send_email(email_subject, email_body, attachments)
        except Exception as e:
            print(f"⚠️ 邮件发送跳过或失败: {e}")

    print(f"\n✨ 任务完成，耗时: {time.time() - start_time:.2f} 秒")

if __name__ == "__main__":
    main()