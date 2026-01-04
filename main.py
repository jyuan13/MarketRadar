import json
import os
import sys
import time
import math
import pandas as pd  # 新增: 用于数据处理
from datetime import datetime
from zoneinfo import ZoneInfo  # 新增: 时区处理

# -----------------------------------------------------------------------------
# 路径兼容处理：确保能导入上层或同级模块
# -----------------------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 引入模块
import fetch_data  # 负责 FX 和 国债
import MarketRadar # 负责 K线 和 均线
import utils       # 新增: 负责计算均线 (calculate_ma)

try:
    import scrape_economy_selenium # 负责 CPI/PPI 等宏观指标 (Root Dir)
except ImportError:
    # 尝试从同级目录导入（如果部署结构扁平化）
    import scrape_economy_selenium

# 输出文件名称
OUTPUT_FILENAME = "MarketRadar_Report.json"
LOG_FILENAME = "market_data_status.txt"

# 定义北京时区
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
    """
    数据清洗核心逻辑：
    1. 递归遍历字典和列表
    2. 浮点数强制保留2位小数
    3. 处理特殊数值 (NaN/Inf -> None)
    """
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
    """
    深度合并两个字典 (dict2 覆盖/补充 dict1)
    """
    result = dict1.copy()
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result

def merge_final_report(macro_data_combined, kline_data_dict, ma_data_list):
    """
    整合所有模块的数据
    """
    merged = {
        "meta": kline_data_dict.get("meta", {}),
        "技术分析": {
            "周K线移动平均线": ma_data_list
        },
        # 宏观部分
        "market_fx": macro_data_combined.get("market_fx", {}),
        "china": macro_data_combined.get("china", {}),
        "usa": macro_data_combined.get("usa", {}),
        "japan": macro_data_combined.get("japan", {}),
        "hk": macro_data_combined.get("hk", {}), # [修复] 显式包含香港数据(Selenium抓取的指数)
        # K线数据部分
        "market_klines": kline_data_dict.get("data", {})
    }
    
    # 强制更新 meta 时间为当前北京时间
    merged["meta"]["generated_at"] = datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")
    merged["meta"]["description"] = "MarketRadar Consolidated Report (Selenium Macro + Online FX + Klines)"
    
    return merged

def save_compact_json(data, filename):
    """
    自定义 JSON 保存：列表强制单行显示
    """
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
    """
    写入状态日志文件
    格式: [时间] [状态] 名称 | 错误信息(如有)
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"MarketRadar Data Fetch Log - {datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*60 + "\n")
            
            for log in logs:
                status_str = "[PASS]" if log['status'] else "[FAIL]"
                # 日志条目时间也使用北京时间
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
    """
    生成邮件正文的状态汇总 (仅展示成功/失败状态，不展示具体报错)
    """
    lines = ["数据获取状态汇总:"]
    lines.append("-" * 30)
    
    # 简单的分类统计
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

    # 1. 获取基础 FX 和 国债数据 (fetch_data - Online Only)
    print("\n[Step 1/4] 获取汇率与国债数据 (fetch_data)...")
    try:
        base_macro, logs_fx = fetch_data.get_market_fx_and_bonds()
        all_status_logs.extend(logs_fx)
    except Exception as e:
        print(f"❌ fetch_data 失败: {e}")
        base_macro = {"market_fx": {}, "china": {}, "usa": {}, "japan": {}}
        all_status_logs.append({'name': 'fetch_data_module', 'status': False, 'error': str(e)})

    # 2. 获取复杂宏观数据 (Selenium Scraper)
    print("\n[Step 2/4] 抓取宏观经济指标 (Selenium)...")
    try:
        # 直接调用 scrape_economy_selenium 暴露的接口
        selenium_macro, logs_selenium = scrape_economy_selenium.get_macro_data()
        all_status_logs.extend(logs_selenium)
    except Exception as e:
        print(f"❌ Selenium 抓取失败 (可能是环境问题): {e}")
        selenium_macro = {}
        all_status_logs.append({'name': 'selenium_module', 'status': False, 'error': str(e)})

    # 合并宏观数据 (Base + Selenium)
    combined_macro = deep_merge(base_macro, selenium_macro)

    # 3. 获取K线与均线 (MarketRadar)
    print("\n[Step 3/4] 获取 K线数据 & 计算均线...")
    try:
        kline_result, logs_klines = MarketRadar.get_all_kline_data()
        all_status_logs.extend(logs_klines)
        
        kline_data_dict = {"meta": kline_result.get("meta"), "data": kline_result.get("data")}
        ma_data_list = kline_result.get("ma_data", [])
        print(f"✅ 获取到 {len(ma_data_list)} 条均线数据")
    except Exception as e:
        print(f"❌ 获取K线数据失败: {e}")
        kline_data_dict = {"meta": {}, "data": {}}
        ma_data_list = []
        all_status_logs.append({'name': 'kline_module', 'status': False, 'error': str(e)})

    # [Step 3.5 - 修复回补] 检查恒生医疗保健指数是否缺失或为空，如缺失则尝试从 Selenium 数据中回补
    # 原因：MarketRadar.py 的 API 可能失败(返回空列表)，但 scrape_economy_selenium 已成功抓取
    hshci_key = "恒生医疗保健指数"
    if "data" not in kline_data_dict:
        kline_data_dict["data"] = {}
    
    # 关键修复: 不仅检查 key 是否存在，还要检查数据是否为空 (API 失败常返回空列表 [])
    is_missing = (hshci_key not in kline_data_dict["data"])
    is_empty = (not kline_data_dict["data"].get(hshci_key))
    
    if is_missing or is_empty:
        # 尝试从 combined_macro 中的 'hk' 字段查找
        # scrape_economy_selenium 将其存放于: result['hk']['恒生医疗保健指数']
        hk_data = combined_macro.get("hk", {})
        if hshci_key in hk_data and hk_data[hshci_key]:
            print(f"\n[Step 3.5] ⚡ 检测到 {hshci_key} K线缺失/为空，正在从 Selenium 数据回补...")
            try:
                raw_data = hk_data[hshci_key]
                df_hshci = pd.DataFrame(raw_data)
                
                # 数据适配：Selenium 输出列名 ('日期', 'close', 'volume') -> Utils 需求 ('date', 'close', 'name')
                if '日期' in df_hshci.columns:
                    df_hshci.rename(columns={'日期': 'date'}, inplace=True)
                
                df_hshci['name'] = hshci_key
                
                # 确保数值类型正确
                for col in ['close', 'open', 'high', 'low', 'volume']:
                    if col in df_hshci.columns:
                        df_hshci[col] = pd.to_numeric(df_hshci[col], errors='coerce')

                # 必须包含 date 且格式正确
                if 'date' in df_hshci.columns:
                    df_hshci['date'] = pd.to_datetime(df_hshci['date'])
                    
                    # 1. 计算均线
                    hshci_ma_list = utils.calculate_ma(df_hshci)
                    if hshci_ma_list:
                        ma_data_list.extend(hshci_ma_list)
                        print(f"✅ {hshci_key} 回补成功: 均线已计算")
                    else:
                        print(f"⚠️ {hshci_key} 回补警告: 均线计算无结果 (可能数据不足)")
                    
                    # 2. 存入 K线字典 (格式化日期为字符串)
                    df_hshci['date'] = df_hshci['date'].dt.strftime('%Y-%m-%d')
                    # 清洗 NaN
                    df_hshci = df_hshci.where(pd.notnull(df_hshci), None)
                    
                    # 覆盖原来的空数据
                    kline_data_dict["data"][hshci_key] = df_hshci.to_dict(orient='records')
                    
                    # 记录回补成功日志
                    all_status_logs.append({'name': f"{hshci_key}(Recovered)", 'status': True, 'error': None})
                else:
                    print(f"❌ {hshci_key} 回补失败: 缺少 'date'/'日期' 列")
            except Exception as e_backfill:
                print(f"❌ {hshci_key} 回补过程异常: {e_backfill}")
        else:
            # 如果连 Selenium 也没抓到，那就没办法了
            pass

    # 新增: 4. 抓取越南胡志明指数 (VNI) K线 并计算均线
    print("\n[Step 4/4] 获取越南胡志明指数 (Investing.com)...")
    try:
        vni_data, vni_err = fetch_data.fetch_vietnam_index_klines()
        if vni_data:
            # 存入 kline_data_dict 的 'data' 字段，键名为 '越南胡志明指数'
            # 确保 data 字段已初始化
            if "data" not in kline_data_dict or kline_data_dict["data"] is None:
                kline_data_dict["data"] = {}
                
            # [重要] 确保数据结构与主程序一致
            kline_data_dict["data"]["越南胡志明指数"] = vni_data
            
            # --- 计算越南指数均线 ---
            try:
                # 转换为 DataFrame 格式适配 utils.calculate_ma
                df_vni = pd.DataFrame(vni_data)
                df_vni['name'] = "越南胡志明指数" # 必须添加 name 列
                
                # 计算均线
                vni_ma_list = utils.calculate_ma(df_vni)
                if vni_ma_list:
                    ma_data_list.extend(vni_ma_list)
                    print(f"✅ 越南胡志明指数获取成功 ({len(vni_data)} 条记录) & 均线已计算")
                else:
                    print(f"✅ 越南胡志明指数获取成功 ({len(vni_data)} 条记录) (均线计算无结果)")
                
                all_status_logs.append({'name': '越南胡志明指数', 'status': True, 'error': None})
                
            except Exception as e_ma:
                print(f"⚠️ 越南数据获取成功但均线计算失败: {e_ma}")
                # 仍然标记为成功，因为核心数据已获取
                all_status_logs.append({'name': '越南胡志明指数', 'status': True, 'error': f"MA Error: {e_ma}"})
            # -----------------------
            
        else:
            all_status_logs.append({'name': '越南胡志明指数', 'status': False, 'error': vni_err})
            print(f"❌ 越南胡志明指数获取失败: {vni_err}")
    except Exception as e:
        print(f"❌ 越南指数模块异常: {e}")
        all_status_logs.append({'name': 'vni_module', 'status': False, 'error': str(e)})

    # 5. 最终整合与清洗
    print("\n[Step 5] 整合数据并清洗...")
    final_data = merge_final_report(combined_macro, kline_data_dict, ma_data_list)
    final_data = clean_and_round(final_data)

    # 6. 生成日志文件
    write_status_log(all_status_logs, LOG_FILENAME)

    # 7. 保存与发送
    if save_compact_json(final_data, OUTPUT_FILENAME):
        # 邮件逻辑
        try:
            # 邮件主题使用北京时间
            email_subject = f"MarketRadar全量日报_{datetime.now(TZ_CN).strftime('%Y-%m-%d')}"
            
            # 构建正文 (使用北京时间)
            # [更新] 描述中增加 "科创50"
            base_body = f"生成时间: {datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}\n包含: 宏观(Selenium), 汇率/国债(Online), K线(Stock/VNI/科创50)\n\n"
            status_body = generate_email_body_summary(all_status_logs)
            email_body = base_body + status_body
            
            # 附件列表：数据报告 + 状态日志
            attachments = [OUTPUT_FILENAME, LOG_FILENAME]
            
            MarketRadar.send_email(email_subject, email_body, attachments)
        except Exception as e:
            print(f"⚠️ 邮件发送跳过或失败: {e}")

    print(f"\n✨ 任务完成，耗时: {time.time() - start_time:.2f} 秒")

if __name__ == "__main__":
    main()