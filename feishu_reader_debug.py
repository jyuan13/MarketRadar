# -*- coding:utf-8 -*-
"""
Feishu Data Reader (Debug Tool) - High Performance
Iterates through all configured tables in feishu_connector.py and dumps content to file.
Features:
- 50 Concurrent Threads
- Output to feishu_all_data_debug.txt
- Preserves order in output file
"""
import sys
import os
import json
import concurrent.futures
import time
from dotenv import load_dotenv

# Ensure paths
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from feishu_connector import FeiShuClient, KEY_TO_TABLE_MAP

# Load Env
load_dotenv()

OUTPUT_FILE = os.path.join(current_dir, "feishu_all_data_debug.txt")

def fetch_table_content(key, table_id, client):
    """
    Worker function to fetch single table data.
    Returns formatted string block.
    """
    buffer = []
    buffer.append("="*60)
    buffer.append(f"📂 Reading: [{key}] (ID: {table_id})")
    buffer.append("="*60)
    
    try:
        # 1. Get Fields (Schema)
        fields_resp = client._request("GET", f"tables/{table_id}/fields", params={"page_size": 100})
        field_names = []
        if fields_resp and "items" in fields_resp:
            field_names = [f["field_name"] for f in fields_resp["items"]]
            buffer.append(f"   Schema: {field_names}")
        else:
            buffer.append("   ⚠️ Could not fetch schema.")

        # 2. Get Records
        records = []
        page_token = ""
        while True:
            params = {"page_size": 100}
            if page_token: params["page_token"] = page_token
            
            resp = client._request("GET", f"tables/{table_id}/records", params=params)
            if not resp: break
            
            items = resp.get("items", [])
            records.extend(items)
            
            if resp.get("has_more"):
                page_token = resp.get("page_token")
            else:
                break
        
        buffer.append(f"   ✅ Fetched {len(records)} records.\n")
        
        # 3. Format Data
        if records:
            if not field_names:
                field_names = list(records[0]["fields"].keys())
            
            # Header
            header_row = " | ".join([str(fn).ljust(15) for fn in field_names])
            sep_row = "-" * len(header_row)
            buffer.append(sep_row)
            buffer.append(header_row)
            buffer.append(sep_row)
            
            for r in records:
                row_vals = []
                fields = r["fields"]
                for fn in field_names:
                    val = fields.get(fn, "")
                    val_str = str(val).replace("\n", " ")
                    if len(val_str) > 20: val_str = val_str[:17] + "..."
                    row_vals.append(val_str.ljust(15))
                buffer.append(" | ".join(row_vals))
            buffer.append(sep_row)
        else:
            buffer.append("   (Empty Table)")
            
        buffer.append("\n")
        return "\n".join(buffer)

    except Exception as e:
        return f"❌ Error reading table {key}: {e}\n"

def main():
    print(">>> 🚀 Starting Feishu Data Reader (50 Threads) <<<")
    
    client = FeiShuClient()
    if not client._get_tenant_access_token():
        print("❌ Auth Failed! Please check env vars.")
        return

    # Prepare Tasks
    keys = list(KEY_TO_TABLE_MAP.keys())
    print(f"📋 Found {len(keys)} tables. Spawning workers...")
    
    results = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_key = {
            executor.submit(fetch_table_content, key, KEY_TO_TABLE_MAP[key], client): key 
            for key in keys
        }
        
        for future in concurrent.futures.as_completed(future_to_key):
            key = future_to_key[future]
            try:
                data = future.result()
                results[key] = data
                print(f"   ✅ Fetched {key}...")
            except Exception as exc:
                results[key] = f"❌ Process Error {key}: {exc}\n"
    
    # Write to file in original order
    print(f"\n💾 Writing to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"Feishu Data Dump - {time.ctime()}\n\n")
        for key in keys:
            if key in results:
                f.write(results[key])
                f.write("\n")
                
    print(f"✨ Done! Check {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
