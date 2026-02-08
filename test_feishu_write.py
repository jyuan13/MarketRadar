# -*- coding:utf-8 -*-
"""
Feishu Table Write Stress Test
Writes 100 rows of dummy data to ALL configured Feishu tables.
Data format:
- Number: 12.34
- Percentage: 12.34%
- Text: '测试数据'
- Date: Today
- Status Table: Updates with checkmark
"""
import os
import datetime
import random
from feishu_connector import FeishuDataWriter, KEY_TO_TABLE_MAP, TABLE_ID_STATUS

def generate_dummy_data(count=100):
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    data = []
    for i in range(count):
        row = {
            "date": today_str,
            "name": f"测试数据_{i+1}",
            "open": 12.34,
            "close": 12.34,
            "high": 12.34,
            "low": 12.34,
            "volume": 1234,
            "amount": 1234.56,
            "pct_chg": "12.34%",
            "turnover": "12.34%",
            "pe": 12.34,
            "pb": 12.34,
            "rsi6": 12.34,
            "kdj_k": 12.34,
            "kdj_d": 12.34,
            "kdj_j": 12.34,
            "signal": "测试信号",
            # Macro fields
            "value": 12.34,
            "period": "2024-Q1",
            "unit": "测试单位",
            "pub_date": today_str,
        }
        data.append(row)
    return data

def main():
    print("🚀 Starting Feishu Stress Test (100 rows per table)...")
    
    # 1. Init Writer
    try:
        writer = FeishuDataWriter()
    except Exception as e:
        print(f"❌ Failed to init FeishuDataWriter: {e}")
        return

    # 2. Iterate all tables
    dummy_data = generate_dummy_data(100)
    
    total_tables = len(KEY_TO_TABLE_MAP)
    print(f"📋 Found {total_tables} data tables to test.")

    for idx, (key, table_id) in enumerate(KEY_TO_TABLE_MAP.items(), 1):
        print(f"\n[{idx}/{total_tables}] Writing to {key} ({table_id})...")
        try:
            # Clear existing is TRUE as per user request to replace data
            writer.write_data(key, dummy_data, clear_existing=True)
            print(f"   ✅ Success: {key}")
        except Exception as e:
            print(f"   ❌ Failed: {key} - {e}")

    # 3. Test Status Table
    print("\n📝 Updating Status Table...")
    try:
        writer.update_sync_status("StressTest_Task", True, 100)
        print("   ✅ Status Updated")
    except Exception as e:
        print(f"   ❌ Status Update Failed: {e}")

    print("\n🎉 Stress Test Complete!")

if __name__ == "__main__":
    main()
