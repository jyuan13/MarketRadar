# -*- coding:utf-8 -*-
import os
import datetime
from zoneinfo import ZoneInfo

# ==============================================================================
# 全局配置
# ==============================================================================

# 时区
TZ_CN = ZoneInfo("Asia/Shanghai")

# 报告时间范围 (最近 N 天的数据)
REPORT_DAYS = 30  # 默认获取最近30天，部分指标可能需要更长

# API Keys
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
OPENBB_PAT = os.environ.get("OPENBB_PAT", "") # Personal Access Token if needed

# User Agent for Scrapers
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"

# ==============================================================================
# 数据源配置
# ==============================================================================

# Investing.com URLs
URLS = {
    "japan_bonds": "https://cn.investing.com/rates-bonds/japan-government-bonds",
    "vn_index": "https://cn.investing.com/indices/vn-historical-data",
    "korea_exports": "https://www.investing.com/economic-calendar/south-korean-export-growth-1316",
    "vn_fdi": "https://www.investing.com/economic-calendar/vietnamese-foreign-direct-investment-(usd)-1857",
}

# OpenBB / FRED Series IDs
FRED_SERIES = {
    "T10YIE": "10-Year Breakeven Inflation Rate",
    "T5YIE": "5-Year Breakeven Inflation Rate",
    "BAMLH0A0HYM2": "ICE BofA US High Yield Index Option-Adjusted Spread",
    "WTREGEN": "Treasury General Account (TGA)", # 注意：需确认具体Series ID，通常用 WTREGEN 或类似的
    "RRPONTSYD": "Overnight Reverse Repurchase Agreements (ON RRP)",
    "DFII10": "10-Year Treasury Inflation-Indexed Security, Constant Maturity", # Real Yield
    "M2SL": "M2 Money Stock",
    "M1SL": "M1 Money Stock",
    "WALCL": "Total Assets (Fed Balance Sheet)", # 用于全球流动性代理
}

# Akshare Symbols
AKSHARE_SYMBOLS = {
    "sh_index": "sh000001",
    "sz_index": "sz399001",
    "cyb_index": "sz399006",
    "hs300": "sh000300",
    "kc50": "000688",
    "kc50_etf": "588000",
}

# Excel/JSON Output Paths
OUTPUT_JSON_NAME = "MarketRadar_Report.json"
STATUS_LOG_NAME = "market_data_status.txt"
