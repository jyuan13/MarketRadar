# main.py
import sys
import os
import time
import datetime

# Ensure legacy folder is in path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
legacy_dir = os.path.join(current_dir, "legacy")
if legacy_dir not in sys.path:
    sys.path.append(legacy_dir)

from config.settings import ConfigManager
from src.utils.bus import MessageBus
from src.collectors.manager import MarketCollector
from src.formatters.json_fmt import JsonFormatter
from src.utils.notifier import Notifier

# Legacy imports
try:
    import scrape_economy_selenium
except ImportError:
    print("WARNING: Could not import scrape_economy_selenium from legacy.")
    scrape_economy_selenium = None

try:
    # 尝试导入 fetch_data (Legacy) 获取 FX/Bond 数据
    # fetch_data.py is in legacy or root? List dir showed it in legacy + root?
    # Step 405 showed fetch_data.py in legacy folder? No, list_dir legacy showed 6 files.
    # Step 332 Ref/MarketRadar had fetch_data.py.
    # Step 405 legacy dir: main_old.py, fetch_data.py etc.
    import fetch_data
except ImportError:
    fetch_data = None

def main():
    start_time = time.time()
    
    # 1. Init Layer
    bus = MessageBus()
    cfg = ConfigManager()
    
    print(">>> MarketRadar Restored (Hybrid Architecture)")
    
    # 2. Collector Layer
    collector = MarketCollector(cfg, bus)
    
    # Storage for gathered data matching JsonFormatter requirements
    collected_data = {
        "groups": {},
        "ma_general": [],
        "ma_commodities": [],
        "macro_fx": {},
        "macro_china": {},
        "macro_usa": {},
        "macro_japan": {},
        "macro_hk": {},
        "star50": {},
        "crypto": {}
    }
    
    # 3. Execution - K-Lines
    print("   [1/5] Collection K-Lines...")
    for category in cfg.TARGETS_KLINES.keys():
        k_res, ma_res = collector.collect_klines(category)
        collected_data["groups"][category] = k_res
        
        if category == "Commodities":
            collected_data["ma_commodities"].extend(ma_res)
        else:
            collected_data["ma_general"].extend(ma_res)

    # 4. Execution - Market Details (Star50, HSTECH 60m)
    print("   [2/5] Collection Market Details (Star50/HK)...")
    details = collector.collect_market_details()
    
    # Map details to correct buckets
    # Star50 details -> "star50"
    star50_keys = ["科创50_60分钟K线", "科创50估值", "科创50融资融券", "科创50实时快照"]
    for k in star50_keys:
        if k in details:
            collected_data["star50"][k] = details[k]
            
    # HSTECH 60m -> "macro_hk" (or handled by formatter mapping if placed correctly)
    if "恒生科技指数_60m" in details:
        collected_data["macro_hk"]["恒生科技指数_60m"] = details["恒生科技指数_60m"]
        
    # 5. Execution - FRED
    print("   [3/5] Collection FRED Data...")
    fred_data = collector.collect_fred_data()
    # Merge FRED into macro_usa
    collected_data["macro_usa"].update(fred_data)
    
    # 6. Execution - Selenium (Legacy)
    print("   [4/5] Collection Macro/FX (Legacy Modules)...")
    
    # 6a. FX/Bonds (Legacy fetch_data)
    if fetch_data and hasattr(fetch_data, 'get_market_fx_and_bonds'):
        try:
            base_macro, logs_fx = fetch_data.get_market_fx_and_bonds()
            # Merge base_macro keys: market_fx, china, usa, japan
            collected_data["macro_fx"].update(base_macro.get("market_fx", {}))
            collected_data["macro_china"].update(base_macro.get("china", {}))
            collected_data["macro_usa"].update(base_macro.get("usa", {}))
            collected_data["macro_japan"].update(base_macro.get("japan", {}))
        except Exception as e:
            print(f"     Legacy FX Fetch Error: {e}")

    # 6b. Selenium Macro
    if scrape_economy_selenium:
        try:
            selenium_macro, logs_sel = scrape_economy_selenium.get_macro_data()
            # Merge selenium_macro keys (same structure: china, usa, etc.)
            for k, v in selenium_macro.items():
                target_key = f"macro_{k}" if k != "market_fx" else "macro_fx"
                # Handle special keys if any, otherwise direct map
                if target_key in collected_data:
                    collected_data[target_key].update(v)
                elif k in ["china", "usa", "japan", "hk"]:
                     collected_data[f"macro_{k}"].update(v)
                else:
                    # Fallback or ignore
                    pass
        except Exception as e:
            print(f"     Legacy Selenium Fetch Error: {e}")
            
    # 7. Format & Output
    print("   [5/5] Formatting Report...")
    formatter = JsonFormatter()
    
    metadata = {
        "generated_at": datetime.datetime.now(cfg.TZ_CN).strftime("%Y-%m-%d %H:%M:%S"),
        "description": "MarketRadar Restored Output",
        "date_range": f"{datetime.datetime.now().strftime('%Y-%m-%d')}"
    }
    
    final_report = formatter.structure_final_report(collected_data, metadata)
    output_file = "MarketRadar_Restored_Report.json"
    formatter.save_to_file(final_report, output_file)
    
    # Summary & Notify
    summary_text = bus.get_summary()
    print("\n" + summary_text)
    
    # Check "恒生科技" presence
    hstech_found = False
    if "恒生科技" in final_report.get("market_klines", {}):
        if final_report["market_klines"]["恒生科技"]:
            hstech_found = True
            print("[OK] HSTECH data found in report.")
    
    if not hstech_found:
        print("[WARN] HSTECH data missing or empty.")
    
    print(f"[DONE] in {time.time() - start_time:.2f}s. Saved to {output_file}")

if __name__ == "__main__":
    main()
