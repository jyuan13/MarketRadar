# selenium_utils.py
# -----------------------------------------------------------------------------
# DeepSeek Finance Project - Selenium Scraper Utilities
# -----------------------------------------------------------------------------

import pandas as pd
import re

def clean_date(date_str):
    """通用日期清洗"""
    try:
        date_str = str(date_str).strip()
        if "年" in date_str:
            clean_str = date_str.replace("月份", "").replace("月", "").replace("日", "").replace("年", "-")
            if clean_str.count("-") == 1:
                clean_str += "-01"
            return pd.to_datetime(clean_str)
        return pd.to_datetime(date_str)
    except Exception:
        return pd.NaT

def clean_investing_date(date_str):
    """Investing.com 特有日期格式清洗"""
    try:
        date_str = str(date_str).strip()
        if "年" in date_str:
            clean_str = date_str.replace("年", "-").replace("月份", "").replace("月", "-").replace("日", "")
            clean_str = re.sub(r'-+', '-', clean_str)
            if clean_str.count("-") == 1:
                clean_str += "-01"
            return pd.to_datetime(clean_str)
        return pd.to_datetime(date_str)
    except Exception:
        return pd.NaT

def parse_volume(vol_str):
    """解析交易量 (K/M/B)"""
    if not isinstance(vol_str, str):
        return vol_str
    
    vol_str = vol_str.upper().strip()
    if vol_str in ['-', '', 'NAN', 'NONE']:
        return 0.0
        
    multiplier = 1.0
    if 'K' in vol_str:
        multiplier = 1000.0
        vol_str = vol_str.replace('K', '')
    elif 'M' in vol_str:
        multiplier = 1000000.0
        vol_str = vol_str.replace('M', '')
    elif 'B' in vol_str:
        multiplier = 1000000000.0
        vol_str = vol_str.replace('B', '')
        
    try:
        return float(vol_str) * multiplier
    except:
        return 0.0

def parse_percentage(pct_str):
    """解析百分比字符串"""
    if not isinstance(pct_str, str):
        return pct_str
    try:
        return float(pct_str.replace('%', '').replace(',', ''))
    except:
        return 0.0