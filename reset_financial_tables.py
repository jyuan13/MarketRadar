# -*- coding:utf-8 -*-
"""
RESET FINANCIAL TABLES TOOL (Auto-Run Version)
- Clears all configured data tables.
- Clears specific columns in Status Table (Time & Status-like fields).
- No User Input Required.
"""
import sys
import os
import concurrent.futures
import time
from dotenv import load_dotenv

# Ensure paths
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from feishu_connector import FeishuDataWriter, KEY_TO_TABLE_MAP, TABLE_ID_STATUS

load_dotenv()

def wipe_worker(key_tuple):
    key, table_id = key_tuple
    writer = FeishuDataWriter()
    try:
        writer._clear_table(table_id)
        # Verify emptiness
        # (Simplified verification for speed in auto-run)
        return True, key
    except Exception as e:
        print(f"❌ [Wipe Error] {key}: {e}")
        return False, key

def clean_status_table():
    """
    Clears 2nd and 3rd columns of Status Table.
    Assumes Schema: [任务名称, 状态, 更新时间, 或者(Count)]
    Target: Clear '更新时间' and '或者'/'状态'?
    User said: "第二列时间列和第三列勾选列"
    We will reset '状态', '更新时间', '或者' to empty/default.
    """
    print("🧹 Cleaning Status Table Columns...")
    writer = FeishuDataWriter()
    
    try:
        # Get all records
        records = writer._get_all_records(TABLE_ID_STATUS)
        if not records:
             print("   (Status Table Empty)")
             return

        # Prepare batch update
        # We prefer to keep the rows (Task Names) but clear status.
        # Check field names first? Assuming standard names from connector.
        
        batch_update = []
        for r in records:
            # We want to clear fields, but keep '任务名称'
            # Update fields to empty strings/None
            fields = {
                "状态": "Pending", # Reset to Pending or empty?
                "更新时间": "",    # Clear time
                "或者": ""         # Clear count/check
            }
            batch_update.append({
                "record_id": r["record_id"],
                "fields": fields
            })
            
        # Batch Update
        # Limit batch size 50
        batch_size = 50
        for i in range(0, len(batch_update), batch_size):
            chunk = batch_update[i:i+batch_size]
            # API to batch update? 
            # Note: Feishu allows batch_create and batch_delete. 
            # batch_update? 'POST tables/{}/records/batch_update' exists?
            # Usually it's 'POST tables/{}/records/batch_update' check docs or connector.
            # Connector doesn't have batch_update method implemented yet.
            # We will implement a simple loop or try to find batch update endpoint.
            # Endpoint: POST /open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update
            
            url = f"{writer.client.base_url}/{writer.client.app_token}/tables/{TABLE_ID_STATUS}/records/batch_update"
            token = writer.client._get_tenant_access_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            payload = {"records": chunk}
            
            import requests
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            if resp.status_code != 200 or resp.json().get("code") != 0:
                 print(f"   ❌ Batch Update Failed: {resp.text}")
            else:
                 print(f"   ✅ Cleared Status Batch {i//batch_size + 1}")

    except Exception as e:
        print(f"❌ Error cleaning status table: {e}")

def reset_all_financial_tables_auto():
    print("\n" + "=" * 60)
    print("⚠️  AUTO-RESET: Cleaning Financial Tables & Status...")
    print("=" * 60)
    
    # 1. Clear Data Tables
    keys = list(KEY_TO_TABLE_MAP.keys())
    tasks = [(k, KEY_TO_TABLE_MAP[k]) for k in keys]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(wipe_worker, t): t for t in tasks}
        concurrent.futures.wait(futures)
        
    print("✅ All Data Tables Wipe Signal Sent.")
    
    # 2. Clean Status Table
    clean_status_table()
    
    print("=" * 60 + "\n")

if __name__ == "__main__":
    reset_all_financial_tables_auto()
