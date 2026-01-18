# -*- coding:utf-8 -*-
import sys
import os
import datetime
from zoneinfo import ZoneInfo
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import OUTPUT_JSON_NAME, STATUS_LOG_NAME, TZ_CN
from Collectors.market_collector import MarketCollector
from Processors.data_processor import DataProcessor
from Formatters.json_formatter import JsonFormatter
from MessageBus.email_service import EmailService

# Email Config (Load from Env or Config)
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD")
RECEIVER_EMAIL = os.environ.get("RECEIVER_EMAIL")
SMTP_SERVER = "smtp.qq.com"
SMTP_PORT = 465

def print_banner():
    print(r"""
   __  __            _        _   ____          _            
  |  \/  | __ _ _ __| | _____| |_|  _ \ __ _ __| | __ _ _ __ 
  | |\/| |/ _` | '__| |/ / _ \ __| |_) / _` / _` |/ _` | '__|
  | |  | | (_| | |  |   <  __/ |_|  _ < (_| (_| | (_| | |   
  |_|  |_|\__,_|_|  |_|\_\___|\__|_| \_\__,_\__,_|\__,_|_|   
    """)

def main():
    print_banner()
    print(f"🚀 MarketRadar Started at {datetime.datetime.now(TZ_CN)}")
    
    # 1. Collect Data
    collector = MarketCollector()
    data, logs = collector.collect_all()
    
    # 2. Process / Merge (Already partial in Collector, but finalize here)
    # The collector returns "final_data" structure directly.
    # We might want to perform additional "final merge" if needed
    # e.g. adding meta info
    data["meta"]["generated_at"] = datetime.datetime.now(TZ_CN).strftime("%Y-%m-%d %H:%M:%S")
    data["meta"]["description"] = "MarketRadar Refactored Report (OpenBB + Akshare)"
    
    # 3. Format & Save
    JsonFormatter.save_compact_json(data, OUTPUT_JSON_NAME)
    
    # Save Logs
    with open(STATUS_LOG_NAME, 'w', encoding='utf-8') as f:
        for log in logs:
            status = "PASS" if log['status'] else "FAIL"
            msg = f"[{status}] {log['name']}"
            if log.get('error'):
                msg += f" | {log['error']}"
            f.write(msg + "\n")
            
    # 4. Send Email
    if SENDER_EMAIL and RECEIVER_EMAIL:
        subject = f"MarketRadar Report {datetime.datetime.now(TZ_CN).strftime('%Y-%m-%d')}"
        
        # Generate Summary Body
        pass_count = sum(1 for l in logs if l['status'])
        fail_count = len(logs) - pass_count
        body = f"""MarketRadar Execution Summary
---------------------------
Total Tasks: {len(logs)}
Success: {pass_count}
Failed: {fail_count}

Report generated at: {data['meta']['generated_at']}
        """
        
        email_service = EmailService(SMTP_SERVER, SMTP_PORT, SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAIL)
        email_service.send_email(subject, body, [OUTPUT_JSON_NAME, STATUS_LOG_NAME])
    else:
        print("⚠️ Email configuration missing, skipping email.")

    print("✅ Done.")

if __name__ == "__main__":
    main()