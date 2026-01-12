# fetch_data.py (完整代码)
#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
数据获取脚本 (接口层)
功能：在线获取全球汇率、VIX、中/美/日国债收益率以及越南胡志明指数K线。
核心逻辑已移至 fetch_data_core.py
"""

import fetch_data_core

def get_market_fx_and_bonds():
    """
    获取汇率、VIX、国债数据及新增的科创50/南向资金数据
    """
    print(">>> [fetch_data] 开始在线获取 FX 和 国债数据...")
    
    data_store = {
        "market_fx": {},
        "china": {},
        "usa": {},
        "japan": {},
        "market_klines": {} # 确保初始化这个键
    }
    
    status_logs = []

    # 1. Market FX
    # [修改] 汇率获取天数改为 15 天 (原为 1 天)
    # [修改] 新增美元指数 (近10天)
    tickers_config = [
        {"name": "VIX恐慌指数", "ticker": "^VIX", "days": 15},
        {"name": "美元/人民币", "ticker": "CNY=X", "days": 15},
        {"name": "美元/日元", "ticker": "JPY=X", "days": 15},
        {"name": "美元/越南盾", "ticker": "VND=X", "days": 15},
        {"name": "美元指数", "ticker": "DX=F", "days": 10} 
    ]

    for item in tickers_config:
        name = item["name"]
        ticker = item["ticker"]
        days = item["days"]
        
        res, err = fetch_data_core.fetch_yf_data(ticker, name, days=days)
        if res:
            data_store["market_fx"][name] = res
            status_logs.append({'name': name, 'status': True, 'error': None})
            print(f"   [{name}] OK ({len(res)} records)")
        else:
            status_logs.append({'name': name, 'status': False, 'error': err})
            print(f"   [{name}] Failed")

    # 2. Bonds
    # USA
    data_us, err_us = fetch_data_core.fetch_us_bond_yields()
    if data_us:
        data_store["usa"]["国债收益率"] = data_us
        status_logs.append({'name': "美国国债收益率", 'status': True, 'error': None})
    else:
        status_logs.append({'name': "美国国债收益率", 'status': False, 'error': err_us})

    # China
    data_cn, err_cn = fetch_data_core.fetch_china_bond_yields()
    if data_cn:
        data_store["china"]["国债收益率"] = data_cn
        status_logs.append({'name': "中国国债收益率", 'status': True, 'error': None})
    else:
        status_logs.append({'name': "中国国债收益率", 'status': False, 'error': err_cn})

    # Japan
    data_jp, err_jp = fetch_data_core.fetch_japan_bond_yields()
    if data_jp:
        data_store["japan"]["国债收益率"] = data_jp
        status_logs.append({'name': "日本国债收益率", 'status': True, 'error': None})
    else:
        status_logs.append({'name': "日本国债收益率", 'status': False, 'error': err_jp})
        print(f"   [日本国债] Failed: {err_jp}")

    # 3. New AKShare Data (Southbound, STAR50)
    # 存入 'china' 键下
    
    # 南向资金
    sb_data, sb_err = fetch_data_core.fetch_southbound_flow()
    if sb_data:
        data_store["china"]["南向资金净流入(AK)"] = sb_data
        status_logs.append({'name': "南向资金(AK)", 'status': True, 'error': None})
    else:
        status_logs.append({'name': "南向资金(AK)", 'status': False, 'error': sb_err})

    # 科创50 估值
    pe_data, pe_err = fetch_data_core.fetch_star50_valuation()
    if pe_data:
        data_store["china"]["科创50估值"] = pe_data
        status_logs.append({'name': "科创50估值", 'status': True, 'error': None})
    else:
        status_logs.append({'name': "科创50估值", 'status': False, 'error': pe_err})

    # 科创50 融资融券
    margin_data, margin_err = fetch_data_core.fetch_star50_margin()
    if margin_data:
        data_store["china"]["科创50融资融券"] = margin_data
        status_logs.append({'name': "科创50融资融券", 'status': True, 'error': None})
    else:
        status_logs.append({'name': "科创50融资融券", 'status': False, 'error': margin_err})

    # 科创50 实时量比 (放入 market_fx 或 china 均可，这里放 china 作为快照)
    spot_data, spot_err = fetch_data_core.fetch_star50_realtime_vol_ratio()
    if spot_data:
        # 封装成列表形式以便统一处理
        data_store["china"]["科创50实时快照"] = [spot_data]
        status_logs.append({'name': "科创50实时量比", 'status': True, 'error': None})
    else:
        status_logs.append({'name': "科创50实时量比", 'status': False, 'error': spot_err})
        
    # [新增] A股主要指数 (上证/深证/创业板/北证50)
    ashare_data, ashare_err = fetch_data_core.fetch_ashare_indices()
    if ashare_data:
        # 存入 market_klines 下的 A股指数 分类
        # 注意: main.py 中的 MarketAnalyzer.analyze_all_assets 会遍历 market_klines 下的所有列表
        data_store["market_klines"]["A股指数"] = ashare_data
        status_logs.append({'name': "A股主要指数", 'status': True, 'error': None})
        print(f"   [A股指数] OK ({len(ashare_data)} records)")
    else:
        status_logs.append({'name': "A股主要指数", 'status': False, 'error': ashare_err})
        print(f"   [A股指数] Failed: {ashare_err}")

    return data_store, status_logs

# 重新暴露 fetch_vietnam_index_klines 供 main.py 调用
def fetch_vietnam_index_klines():
    return fetch_data_core.fetch_vietnam_index_klines()

if __name__ == "__main__":
    data, logs = get_market_fx_and_bonds()
    vn_data, vn_err = fetch_vietnam_index_klines()
    print("VNI Data (Top 2):", vn_data[:2] if vn_data else vn_err)