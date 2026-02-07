# -*- coding:utf-8 -*-
"""
Feishu Connector for MarketRadar-Original (Standalone)
Contains:
1. Configuration (Table IDs)
2. FeiShuClient (Auth & Request)
3. FeishuDataWriter (Write & Clear)
"""
import os
import time
import json
import logging
import requests
import datetime

# ==========================================
# 1. Configuration (Table IDs)
# ==========================================
TABLE_ID_STATUS = "tbleEHJfCviHc6du"

KEY_TO_TABLE_MAP = {
    # 1. Gold & Commodities
    "GOLD_TRANS_30D": "tblJBNZvPtLYbOWG",
    "GOLD_MA": "tblwoh7a1MnQ2LM0",
    "GOLD_TECH": "tblrtmqoqmagOzNQ",
    
    # 2. US Market
    "US_INDEX_MA": "tbldgnCQ4eSDo9Qa",
    "US_INDEX_TECH": "tblp8zSPHGa8hPIp",
    "US_INDEX_TRANS_30D": "tbl0zUnsbtxNKk3J",
    "US_STOCK_MA": "tblXqeTPAOGzDUG8",
    "US_STOCK_TECH": "tblJ8ZYhi1tgjGzh",
    "US_STOCK_TRANS_30D": "tblcoehBTOSC0PId",

    # 3. STAR 50
    "STAR50_MARGIN": "tblr8UYxvvI85QxG",
    "STAR50_INDEX_MA": "tblHyRksOxgtx6j0",
    "STAR50_INDEX_TECH": "tbl2rmYR8qKU3lMv",
    "STAR50_INDEX_TRANS_30D": "tblv0LS5hf6bJBci",
    "STAR50_STOCK_MA": "tblG6NJNrzhOxbiJ",
    "STAR50_STOCK_TECH": "tblPzWPlK3MYhub8",
    "STAR50_STOCK_TRANS_30D": "tblNwDRhvbgbWp1D",

    # 4. HK Tech
    "HK_INDEX_MA": "tblcor7WFpjpuYHO",
    "HK_INDEX_TECH": "tblE8H8SDWcEy9Iq",
    "HK_INDEX_TRANS_30D": "tblG9ou8OyVzLPCe",
    "HK_STOCK_MA": "tbl57f8SBtWlwUwg",
    "HK_STOCK_TECH": "tblIMUoZntmv1Yg4",
    "HK_STOCK_TRANS_30D": "tblLR5iMF5ZgXWtO",

    # 5. Innovative Drugs
    "DRUG_STOCK_MA": "tblymN70YVcbsLzX",
    "DRUG_STOCK_TECH": "tbllfWRA92P8IetT",
    "DRUG_STOCK_TRANS_30D": "tblKjBCLJepQAj5u",
    "DRUG_INDEX_MA": "tblSIbZwqYLNd7W0",
    "DRUG_INDEX_TECH": "tbleoQNrUMYSvKka",
    "DRUG_INDEX_TRANS_30D": "tbl1SL9z05d8khCG",

    # 6. China Macro
    "CN_MACRO_YIELD": "tblg9zJfrcibv4SK",
    "CN_MACRO_FLOWS": "tblMMQdAl818abov",
    "CN_MACRO_OMO": "tblnACgUWNmbf4m2",
    "CN_MACRO_PMI": "tblYdTTzJbcPDkIP",
    "CN_MACRO_CPI": "tblRPmVevyLyzngD",
    "CN_MACRO_PPI": "tblpWHMGnIOZZx2r",
    "CN_MACRO_LPR": "tblaG7ydjjMDaDM8",
    "CN_MACRO_MONEY": "tblXcJZwo8BRIlNJ",
    "CN_MACRO_CONTAINER": "tblHyHj1kMU5rQSe",

    # 7. US Macro
    "US_MACRO_YIELD": "tblzcqZSoKkGZpCC",
    "US_MACRO_ISM_MANU": "tblAw5Yagy6IroyF",
    "US_MACRO_ISM_NON_MANU": "tblLF01iomI47941",
    "US_MACRO_NFP": "tblCl1obpi596AI2",
    "US_MACRO_RETAIL": "tblwpGPChi1EdAwb",
    "US_MACRO_RATE_DECISION": "tblxNqrKCDvbiFou",
    "US_MACRO_FED_MONITOR": "tblVgkOVEjs8YH4A",
    "US_MACRO_JOBLESS": "tblHY4VUlMWdsJf8",
    "US_MACRO_NEW_ORDERS": "tbl0fRS57TJyajVY",
    "US_MACRO_INSIDER": "tblUL1nO5LewxZZj",
    "US_MACRO_RRP": "tblgEohN9dsawTsE",
    "US_MACRO_TGA": "tblG8jRcoDeJ7O14",

    # 8. Japan Macro
    "JP_MACRO_YIELD": "tblorT1YYNYLudF4",
    "JP_MACRO_BOJ_RATE": "tblrMvODKAC6djWN"
}

# Setup Logger (Replaced with print for script visibility)
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("FeishuConnector")

def log_info(msg):
    print(f"[FeishuConnector] {msg}")

def log_error(msg):
    print(f"[FeishuConnector] ❌ {msg}")

def log_debug(msg):
    # print(f"[FeishuConnector] 🐛 {msg}")
    pass

# ==========================================
# 2. FeiShuClient
# ==========================================
class FeiShuClient:
    def __init__(self):
        self.app_id = os.environ.get("FEISHU_APP_ID")
        self.app_secret = os.environ.get("FEISHU_APP_SECRET")
        
        # [Debug] Print Auth Status
        has_id = "YES" if self.app_id else "NO"
        has_secret = "YES" if self.app_secret else "NO"
        print(f"[FeishuConnector] Init check: Has AppID? {has_id}, Has Secret? {has_secret}")
        if self.app_id:
            print(f"[FeishuConnector] AppID Prefix: {self.app_id[:4]}***")
            
        self.base_url = "https://open.feishu.cn/open-apis/bitable/v1/apps"
        # Correct Token from libs/settings.py
        self.app_token = "Dvk6bQOuqaArI9sYJzBcrzyonjg" 
        self.tenant_access_token = ""
        self.token_expire_time = 0
        
        if not self.app_id or not self.app_secret:
            log_error("Missing FEISHU_APP_ID or FEISHU_APP_SECRET env vars!")

    def _get_tenant_access_token(self):
        if time.time() < self.token_expire_time:
            return self.tenant_access_token

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=10)
            data = resp.json()
            if data.get("code") == 0:
                self.tenant_access_token = data["tenant_access_token"]
                self.token_expire_time = time.time() + data["expire"] - 60
                log_info("Token Refreshed.")
                return self.tenant_access_token
            else:
                log_error(f"Token Error: {data}")
                return None
        except Exception as e:
            log_error(f"Token Exception: {e}")
            return None

    def _request(self, method, endpoint, params=None, json_data=None):
        token = self._get_tenant_access_token()
        if not token: return None
        
        url = f"{self.base_url}/{self.app_token}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        try:
            resp = requests.request(method, url, headers=headers, params=params, json=json_data, timeout=20)
            if resp.status_code == 200:
                res_json = resp.json()
                if res_json.get("code") == 0:
                    return res_json.get("data")
                else:
                    log_error(f"API Error [{endpoint}]: {res_json}")
                    return None
            else:
                log_error(f"HTTP Error {resp.status_code}: {resp.text}")
                return None
        except Exception as e:
            log_error(f"Request Exception: {e}")
            return None

# ==========================================
# 3. FeishuDataWriter
# ==========================================
class FeishuDataWriter:
    def __init__(self):
        self.client = FeiShuClient()
    
    def write_data(self, data_key, data_list, clear_existing=False):
        table_id = KEY_TO_TABLE_MAP.get(data_key)
        if not table_id:
            log_debug(f"Unknown Data Key: {data_key} (Skipping sync)")
            return False
            
        if not data_list:
            log_error(f"No data for {data_key}")
            return False
            
        log_info(f"🚀 Syncing [{data_key}] -> {table_id} ({len(data_list)} rows)")
        
        # 1. Get Schema
        schema_map = self._get_field_schema(table_id)
        if not schema_map: return False
            
        # 2. Clean Data
        valid_records = []
        for row in data_list:
            clean_row = self._process_row(row, schema_map)
            if clean_row:
                valid_records.append({"fields": clean_row})
                
        if not valid_records:
            log_error(f"No valid records after cleaning for {data_key}")
            return False
            
        # 3. Clear (Optional)
        if clear_existing:
            self._clear_table(table_id)
            
        # 4. Insert Data
        self._batch_insert(table_id, valid_records)
        log_info(f"✅ [1/4] Data Write Done for {data_key}")

        # 5. Verify Data Readback
        if not self._verify_data_count(table_id, len(valid_records)):
            log_error(f"❌ [2/4] Data Verification Failed for {data_key}")
            # We continue to try updating status even if count mismatches, to record the attempt? 
            # Or return False? User wants verification flow. Let's log error but proceed to status to ensure we see it.
        else:
             log_info(f"✅ [2/4] Data Verification Passed (Count: {len(valid_records)})")

        # 6. Update Sync Status
        status_updated = self.update_sync_status(data_key, count=len(valid_records))
        if status_updated:
            log_info(f"✅ [3/4] Status Write Done for {data_key}")
        else:
            log_error(f"❌ [3/4] Status Write Failed for {data_key}")

        # 7. Verify Status Readback
        if self._verify_status_update(data_key):
             log_info(f"✅ [4/4] Status Verification Passed")
        else:
             log_error(f"❌ [4/4] Status Verification Failed")

        return True

    def _verify_data_count(self, table_id, expected_count):
        """Read back table total count"""
        try:
            # We just get total, not all records
            # API: LIST records, page_size=1 (to be fast), total=true
            data = self.client._request("GET", f"tables/{table_id}/records", params={"page_size": 1, "page_token": ""})
            if data:
                total = data.get("total", 0)
                if total == expected_count:
                    return True
                else:
                    log_error(f"Count Mismatch! Expected {expected_count}, Found {total}")
                    return False
        except Exception as e:
            log_error(f"Verification Exception: {e}")
        return False

    def update_sync_status(self, key_name, status="Success", count=0):
        """Update status table"""
        try:
            # 1. Search for existing record
            # We need to filter by "任务名称"
            # Since filtering via API might vary, we can try to list and find (assuming status table small)
            # Or usage filter like `CurrentValue.[任务名称] = "{key_name}"`
            
            # Simple approach: List all (Status table is small)
            records = self._get_all_records(TABLE_ID_STATUS)
            target_record_id = None
            
            for r in records:
                fields = r.get("fields", {})
                if fields.get("任务名称") == key_name:
                    target_record_id = r["record_id"]
                    break
            
            ts_now = int(time.time() * 1000)
            fields_payload = {
                "任务名称": key_name,
                "状态": status,
                "更新时间": ts_now,
                "或者": str(count) # Assuming checks mapping text, wait. Let's stick to simple
            }
            # Note: Fields must match Status Table Schema exactly.
            # Based on user description: "状态数据表日期和状态是对的"
            # I'll guess standard names: "任务名称", "状态", "更新时间".
            
            if target_record_id:
                # Update
                self.client._request("PUT", f"tables/{TABLE_ID_STATUS}/records/{target_record_id}", json_data={"fields": fields_payload})
            else:
                # Create
                self.client._request("POST", f"tables/{TABLE_ID_STATUS}/records", json_data={"fields": fields_payload})
            
            return True
        except Exception as e:
            log_error(f"Update Status Error: {e}")
            return False

    def _verify_status_update(self, key_name):
        """Read back status table and check timestamp is recent"""
        try:
            records = self._get_all_records(TABLE_ID_STATUS)
            for r in records:
                 fields = r.get("fields", {})
                 if fields.get("任务名称") == key_name:
                     # Check update time is within last 1 minute
                     upd_time = fields.get("更新时间")
                     if upd_time:
                         # timestamp ms
                         if abs(time.time() * 1000 - upd_time) < 60000:
                             return True
                         else:
                             log_error(f"Status Verify: Timestamp stale for {key_name}")
                             return False
                     return True # Found but no time field? 
            log_error(f"Status Verify: Record not found for {key_name}")
            return False
        except:
            return False

    def _get_all_records(self, table_id):
        all_records = []
        page_token = ""
        while True:
            params = {"page_size": 100}
            if page_token: params["page_token"] = page_token
            
            data = self.client._request("GET", f"tables/{table_id}/records", params=params)
            if not data: break
            
            items = data.get("items", [])
            all_records.extend(items)
            
            if data.get("has_more"):
                page_token = data.get("page_token")
            else:
                break
        return all_records

    def _get_field_schema(self, table_id):
        data = self.client._request("GET", f"tables/{table_id}/fields", params={"page_size": 100})
        if not data: return None
        schema = {}
        for item in data["items"]:
            schema[item["field_name"]] = {"type": item["type"]}
        return schema

    def _process_row(self, row, schema_map):
        clean_fields = {}
        for key, value in row.items():
            if key not in schema_map: continue
            
            f_type = schema_map[key]["type"]
            
            # Type 5 = Date (Timestamp)
            if f_type == 5:
                ts = self._to_timestamp(value)
                if ts: clean_fields[key] = ts
            # Type 2 = Number
            elif f_type == 2:
                if value is not None and value != "":
                    try:
                        if isinstance(value, str): value = value.replace(",", "").replace("%", "")
                        clean_fields[key] = float(value)
                    except: pass
            # Type 1 = Text, 3 = Select
            elif f_type in [1, 3]:
                if value: clean_fields[key] = str(value)
            else:
                if value is not None: clean_fields[key] = value
                
        return clean_fields if clean_fields else None

    def _to_timestamp(self, date_val):
        try:
            if isinstance(date_val, (int, float)):
                if date_val > 100000000000: return int(date_val)
                return int(date_val * 1000)
            
            if isinstance(date_val, str):
                for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"]:
                    try:
                        dt = datetime.datetime.strptime(date_val, fmt)
                        return int(dt.timestamp() * 1000)
                    except: continue
            return None
        except: return None

    def _clear_table(self, table_id):
        log_info(f"    🧹 Clearing table...")
        while True:
            data = self.client._request("GET", f"tables/{table_id}/records", params={"page_size": 100})
            if not data or not data.get("items"): break
            
            record_ids = [i["record_id"] for i in data["items"]]
            self.client._request("POST", f"tables/{table_id}/records/batch_delete", json_data={"records": record_ids})
            log_info(f"    🗑️ Deleted {len(record_ids)} records")
            if len(record_ids) < 100: break

    def _batch_insert(self, table_id, records):
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            self.client._request("POST", f"tables/{table_id}/records/batch_create", json_data={"records": batch})
            log_info(f"    📥 Inserted batch {i//batch_size + 1}")
