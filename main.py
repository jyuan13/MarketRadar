import json
import os
import time
import math
from datetime import datetime

# 引入接口
import fetch_data
import MarketRadar

# 输出文件名称
OUTPUT_FILENAME = "MarketRadar_Report.json"

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
        # 检查 NaN 或 Inf，转换为 None (JSON null)
        if math.isnan(data) or math.isinf(data):
            return None
        return round(data, 2)
    else:
        # 其他类型 (int, str, None) 原样返回
        return data

def merge_data(macro_data, kline_data):
    """
    合并宏观数据和K线数据到一个统一的字典中
    """
    merged = {
        "meta": kline_data.get("meta", {}),
        # 宏观数据部分
        "market_fx": macro_data.get("market_fx", {}),
        "china": macro_data.get("china", {}),
        "usa": macro_data.get("usa", {}),
        "japan": macro_data.get("japan", {}),
        # K线数据部分 (MarketRadar原本放在 "data" 键下)
        "market_klines": kline_data.get("data", {})
    }
    
    # 更新 meta 信息
    merged["meta"]["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    merged["meta"]["description"] = "MarketRadar Consolidated Report (Macro + Klines)"
    
    return merged

def save_compact_json(data, filename):
    """
    自定义 JSON 保存函数
    功能：强制将列表内的字典对象保持在同一行，实现紧凑格式。
    结构：
    {
        "Category": {
            "Indicator": [
                {"date": "...", "val": ...},  <-- 单行
                {"date": "...", "val": ...}   <-- 单行
            ]
        }
    }
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('{\n')
            
            # 顶层键 (如 meta, market_fx, china, market_klines 等)
            keys = list(data.keys())
            for i, key in enumerate(keys):
                val = data[key]
                
                # 写入 Key
                f.write(f'    "{key}": ')
                
                if isinstance(val, dict):
                    f.write('{\n')
                    sub_keys = list(val.keys())
                    for j, sub_key in enumerate(sub_keys):
                        sub_val = val[sub_key]
                        f.write(f'        "{sub_key}": ')
                        
                        if isinstance(sub_val, list):
                            # === 核心逻辑：如果是列表，强制内部元素单行显示 ===
                            f.write('[\n')
                            for k, item in enumerate(sub_val):
                                # 使用 json.dumps 将单个字典转为单行字符串
                                item_str = json.dumps(item, ensure_ascii=False)
                                comma = "," if k < len(sub_val) - 1 else ""
                                f.write(f'            {item_str}{comma}\n')
                            f.write('        ]')
                        else:
                            # 如果不是列表（例如 meta 中的字符串值），正常 dump
                            f.write(json.dumps(sub_val, ensure_ascii=False))
                        
                        # 处理子项之间的逗号
                        if j < len(sub_keys) - 1:
                            f.write(',\n')
                        else:
                            f.write('\n')
                    f.write('    }')
                else:
                    # 如果顶层值不是字典，直接 dump
                    f.write(json.dumps(val, ensure_ascii=False))
                
                # 处理顶层项之间的逗号
                if i < len(keys) - 1:
                    f.write(',\n')
                else:
                    f.write('\n')
            
            f.write('}')
            
        print(f"\n✅ 成功! 所有数据已合并写入 {filename} (紧凑格式)")
        return True
    except Exception as e:
        print(f"\n❌ 写入合并 JSON 失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    start_time = time.time()
    print_banner()
    print("🚀 MarketRadar 启动主程序...")
    
    # 1. 获取宏观经济数据 (fetch_data)
    print("\n[Step 1/3] 开始获取宏观经济数据...")
    try:
        macro_data = fetch_data.get_data_main()
    except Exception as e:
        print(f"❌ 获取宏观数据失败: {e}")
        macro_data = {}

    # 2. 获取市场K线数据 (MarketRadar)
    print("\n[Step 2/3] 开始获取全球市场K线数据...")
    try:
        kline_data = MarketRadar.get_all_kline_data()
    except Exception as e:
        print(f"❌ 获取K线数据失败: {e}")
        kline_data = {"meta": {}, "data": {}}

    # 3. 整合数据
    print("\n[Step 3/3] 整合数据并生成报告...")
    final_data = merge_data(macro_data, kline_data)
    
    # === 新增：全局数据清洗 (保留两位小数) ===
    print("🧹 [Step 3.5] 执行全局数据清洗 (保留两位小数, 去除NaN)...")
    final_data = clean_and_round(final_data)

    # 4. 保存并发送
    if save_compact_json(final_data, OUTPUT_FILENAME):
        # 发送邮件 (调用 MarketRadar 的邮件功能)
        email_subject = f"MarketRadar全量日报_{datetime.now().strftime('%Y-%m-%d')}"
        email_body = f"""
        MarketRadar 自动化报告
        
        生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        包含模块:
        - 宏观经济数据 (中国/美国/日本/FX)
        - 全球市场K线 (指数/美股/港股/新兴市场)
        
        附件: {OUTPUT_FILENAME}
        
        System: GitHub Actions / Local
        """
        MarketRadar.send_email(email_subject, email_body, [OUTPUT_FILENAME])

    elapsed = time.time() - start_time
    print(f"\n✨ 所有任务完成，耗时: {elapsed:.2f} 秒")

if __name__ == "__main__":
    main()