# main_refactored.py
from config.settings import ConfigManager
from src.utils.bus import MessageBus
from src.collectors.manager import MarketCollector
from src.formatters.json_fmt import JsonFormatter
from src.utils.notifier import Notifier
import datetime
import time

def main():
    start_time = time.time()
    
    # 1. Init Layer
    bus = MessageBus()
    cfg = ConfigManager()
    
    print(">>> MarketRadar Refactored (Layered Architecture)")
    
    # 2. Collector Layer
    collector = MarketCollector(cfg, bus)
    
    # Storage for gathered data
    collected_data = {
        "groups": {}, # Store by category name e.g. "Indices": [...]
        "ma_general": [],
        "ma_commodities": [],
        "macro_usa": {},
        "macro_china": {},
        # ...
    }
    
    # 3. Execution - K-Lines & Components
    print("   [1/3] Collection K-Lines...")
    
    # Iterate all categories in config
    for category in cfg.TARGETS_KLINES.keys():
        k_res, ma_res = collector.collect_klines(category)
        
        # Convert dict {name: data} to list of records [ {name:..., data:...} ] match legacy format?
        # Legacy format: "data": { "指数": [ {name:..., ...}, ... ] }
        # collectors.collect_klines returns {name: [rows...]}
        
        # We need to store it so formatter can map it.
        collected_data["groups"][category] = k_res
        
        # Categorize MA data
        if category == "Commodities":
            collected_data["ma_commodities"].extend(ma_res)
        else:
            collected_data["ma_general"].extend(ma_res)
    
    # Others...
    # (In full implementation, loop through all categories in Config)
    
    # 4. Execution - Macro (FRED)
    print("   [2/3] Collection FRED Data...")
    fred_data = collector.collect_fred_data()
    collected_data["macro_usa"].update(fred_data)
    
    # 5. Format & Output
    print("   [3/3] Formatting Report...")
    formatter = JsonFormatter()
    
    metadata = {
        "generated_at": datetime.datetime.now(cfg.TZ_CN).strftime("%Y-%m-%d %H:%M:%S"),
        "description": "MarketRadar Layered Output"
    }
    
    final_report = formatter.structure_final_report(collected_data, metadata)
    formatter.save_to_file(final_report, "MarketRadar_Refactored_Report.json")
    
    # Summary
    summary_text = bus.get_summary()
    print("\n" + summary_text)
    
    # 6. Notification
    print("   [4/4] Sending Notification...")
    notifier = Notifier(cfg, bus)
    notifier.send_email(
        subject=f"MarketRadar Report {datetime.datetime.now().strftime('%Y-%m-%d')}",
        body=summary_text,
        attachment_files=["MarketRadar_Refactored_Report.json"]
    )
    
    print(f"✨ Done in {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    main()
