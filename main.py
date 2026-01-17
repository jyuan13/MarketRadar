# main_refactored.py
from config.settings import ConfigManager
from src.utils.bus import MessageBus
from src.collectors.manager import MarketCollector
from src.formatters.json_fmt import JsonFormatter
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
        "klines": {},
        "ma_general": [],
        "ma_commodities": [],
        "macro_usa": {},
        "macro_china": {},
        # ...
    }
    
    # 3. Execution - K-Lines
    print("   [1/3] Collection K-Lines...")
    
    # Indices
    k_idx, ma_idx = collector.collect_klines("Indices")
    collected_data["klines"].update(k_idx)
    collected_data["ma_general"].extend(ma_idx)
    
    # Commodities
    k_com, ma_com = collector.collect_klines("Commodities")
    collected_data["klines"].update(k_com)
    collected_data["ma_commodities"].extend(ma_com) # Separate logic?
    
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
    print("\n" + bus.get_summary())
    print(f"✨ Done in {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    main()
