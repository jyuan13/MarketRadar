#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
MarketRadar/utils.py
通用工具函数库：
1. 技术指标计算 (移动平均线)
2. 数据清洗与格式化
"""

import pandas as pd
import numpy as np

def calculate_ma(df, windows=[5, 10, 20, 60, 120, 250]):
    """
    计算移动平均线
    :param df: 包含 'close' 列的 DataFrame (建议包含足够长的历史数据)
    :param windows: 均线周期列表
    :return: 包含最新均线数据的字典列表
    """
    if df is None or df.empty or 'close' not in df.columns:
        return []

    # 确保按日期升序排列
    df = df.sort_values('date').copy()
    
    # 确保 close 列为数值型
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    # 计算各周期均线
    result_map = {} 
    
    # 按名称分组计算 (以防传入的是混合了多只股票的DF，虽然目前架构是单只传入)
    if 'name' in df.columns:
        groups = df.groupby('name')
    else:
        # 如果没有name列，视为单只股票
        groups = [('Unknown', df)]

    final_results = []

    for name, group_df in groups:
        # 如果数据点太少，可能无法计算长周期均线，但不应崩溃
        if len(group_df) < 1:
            continue
            
        # 获取最新的一条数据记录（用于标记日期和收盘价）
        latest_record = group_df.iloc[-1].to_dict()
        
        # 格式化日期
        date_val = latest_record.get('date')
        if isinstance(date_val, pd.Timestamp):
            date_str = date_val.strftime('%Y-%m-%d')
        else:
            date_str = str(date_val)

        ma_data = {
            "名称": name,
            "日期": date_str,
            "收盘价": round(latest_record.get('close'), 2)
        }

        # 遍历计算不同周期的 MA
        for w in windows:
            col_name = f"{w}日均线"
            # 计算滚动平均
            ma_series = group_df['close'].rolling(window=w).mean()
            # 获取最新值
            latest_ma = ma_series.iloc[-1]
            
            if pd.notna(latest_ma):
                ma_data[col_name] = round(latest_ma, 2)
            else:
                ma_data[col_name] = None # 数据不足以计算该周期均线

        final_results.append(ma_data)
        
    return final_results