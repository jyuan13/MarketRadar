# -*- coding:utf-8 -*-
"""
Feishu Dispatcher
Routes MarketRadar-Original data structure to Feishu Tables.
"""
import logging
from feishu_connector import FeishuDataWriter

# logger = logging.getLogger("FeishuDispatcher")

def sync_data(final_data):
    """
    Main entry point to sync the 'final_data' dict from main.py to Feishu.
    """
    print("⚡ Starting Feishu Sync Dispatch... (Debug Mode)")
    writer = FeishuDataWriter()
    
    # ==========================================
    # 1. Sync Macro Data (China, US, Japan)
    # ==========================================
    # China Macro
    cn_data = final_data.get("china", {})
    _sync_macro_section(writer, cn_data, "CN")
    
    # US Macro
    us_data = final_data.get("usa", {})
    _sync_macro_section(writer, us_data, "US")
    
    # Japan Macro
    jp_data = final_data.get("japan", {})
    _sync_macro_section(writer, jp_data, "JP")
    
    # Market FX (Not strictly mapped in table_config yet, skipping or could map if needed)
    
    # ==========================================
    # 2. Sync K-Lines (Transactions) & MA Data
    # ==========================================
    # MarketRadar structure:
    # "market_klines": { "Group Name": { "Stock Name": [List of Dicts] } }
    # "技术分析": { "指数+个股日均线": [], "大宗商品": [] }
    
    klines_groups = final_data.get("market_klines", {})
    ma_lists = final_data.get("技术分析", {})
    
    # --- 2.1 Sync MA Data (Technical Analysis) ---
    # We need to filter the MA list by group/type to send to correct tables
    
    # General List (Indices, HK, US Stocks, Star50)
    general_ma = ma_lists.get("指数+个股日均线", [])
    _dispatch_ma_list(writer, general_ma)
    _dispatch_tech_list(writer, general_ma) # [New] Dispatch Tech
    
    # Commodities List
    comm_ma = ma_lists.get("大宗商品", [])
    if comm_ma:
        writer.write_data("GOLD_MA", comm_ma, clear_existing=True)
        writer.write_data("GOLD_TECH", comm_ma, clear_existing=True) # [New] Dispatch Tech

    # --- 2.2 Sync Transaction Data (30 Days) ---
    # We iterate through groups and dispatch based on known group names
    
    for group_name, stocks_dict in klines_groups.items():
        _dispatch_transaction_group(writer, group_name, stocks_dict)
        
    print("✨ Feishu Sync Complete!")


def _sync_macro_section(writer, data_dict, prefix):
    """
    Syncs macro data where keys match "CN_MACRO_..." pattern logic.
    Ref Project structure might differ, so we map manually if needed.
    """
    if not data_dict: return

    # Mapping Ref Project keys to Feishu Keys
    # Ref Project Keys Example: "money_supply", "treasury", "lpr"
    
    # Common mappings
    key_map = {
        "treasury": f"{prefix}_MACRO_YIELD",
        "money_supply": f"{prefix}_MACRO_MONEY",
        "lpr": f"{prefix}_MACRO_LPR",
        "pmi": f"{prefix}_MACRO_PMI", # Logic might need adjustment based on valid keys
        "cpi": f"{prefix}_MACRO_CPI",
        "ppi": f"{prefix}_MACRO_PPI",
        "southbound": f"{prefix}_MACRO_FLOWS",
        "Liquidity": f"{prefix}_MACRO_RRP" if prefix == "US" else None # Partial match
    }
    
    for local_key, feishu_key in key_map.items():
        if local_key in data_dict and data_dict[local_key]:
            # Special handling for US Liquidity which might be a dict with RRP/TGA
            if local_key == "Liquidity" and isinstance(data_dict[local_key], dict):
                 rrp = data_dict[local_key].get("RRP")
                 tga = data_dict[local_key].get("TGA")
                 if rrp: writer.write_data("US_MACRO_RRP", rrp, clear_existing=True)
                 if tga: writer.write_data("US_MACRO_TGA", tga, clear_existing=True)
            else:
                 writer.write_data(feishu_key, data_dict[local_key], clear_existing=True)


def _dispatch_ma_list(writer, ma_list):
    """
    Splits the big 'general' MA list into specific tables based on Name/Type.
    Using Name heuristics since 'Type' might not be in the MA output dict directly.
    """
    # Buckets
    buckets = {
        "US_INDEX_MA": [], "US_STOCK_MA": [],
        "HK_INDEX_MA": [], "HK_STOCK_MA": [],
        "STAR50_INDEX_MA": [], "STAR50_STOCK_MA": [],
        "DRUG_INDEX_MA": [], "DRUG_STOCK_MA": []
    }
    
    for item in ma_list:
        name = item.get("名称", item.get("name", ""))
        
        # Heuristics
        if name in ["纳斯达克", "标普500", "VNM(ETF)"]:
            buckets["US_INDEX_MA"].append(item)
        elif name in ["恒生科技", "恒生指数"]:
            buckets["HK_INDEX_MA"].append(item)
        elif "科创50" in name and "ETF" in name:
            buckets["STAR50_INDEX_MA"].append(item)
        elif name == "港股创新药ETF":
             buckets["DRUG_INDEX_MA"].append(item)
        elif _is_us_stock(name):
            buckets["US_STOCK_MA"].append(item)
        elif _is_hk_drug(name):
             buckets["DRUG_STOCK_MA"].append(item)
        elif _is_star50_stock(name):
             buckets["STAR50_STOCK_MA"].append(item)
        else:
             # Default fallback: HK Tech Stock or mix
             # If it's pure HK stock not in Drug/Star list
             buckets["HK_STOCK_MA"].append(item)
             
    # Write Buckets
    for key, data in buckets.items():
        if data:
            writer.write_data(key, data, clear_existing=True)

def _dispatch_tech_list(writer, ma_list):
    """
    Splits the 'general' list (which contains Tech Indicators too) into specific TECH tables.
    Also handles converting 'Signals' list to string.
    """
    # Buckets
    buckets = {
        "US_INDEX_TECH": [], "US_STOCK_TECH": [],
        "HK_INDEX_TECH": [], "HK_STOCK_TECH": [],
        "STAR50_INDEX_TECH": [], "STAR50_STOCK_TECH": [],
        "DRUG_INDEX_TECH": [], "DRUG_STOCK_TECH": []
    }
    
    for item in ma_list:
        name = item.get("名称", item.get("name", ""))
        
        # Process Signals (Convert list to comma-string)
        # We create a COPY to avoid modifying the original item used by MA dispatch (though safely it's fine)
        # Actually simplest is to modify or use helper
        tech_item = item.copy()
        signals = tech_item.get("Signals", [])
        if isinstance(signals, list):
            tech_item["Signals"] = ", ".join(signals)
            
        # Ensure MACD/KDJ fields exist (market_core provides them, but safe check)
        
        # Heuristics (Same as MA)
        if name in ["纳斯达克", "标普500", "VNM(ETF)"]:
            buckets["US_INDEX_TECH"].append(tech_item)
        elif name in ["恒生科技", "恒生指数"]:
            buckets["HK_INDEX_TECH"].append(tech_item)
        elif "科创50" in name and "ETF" in name:
            buckets["STAR50_INDEX_TECH"].append(tech_item)
        elif name == "港股创新药ETF":
             buckets["DRUG_INDEX_TECH"].append(tech_item)
        elif _is_us_stock(name):
            buckets["US_STOCK_TECH"].append(tech_item)
        elif _is_hk_drug(name):
             buckets["DRUG_STOCK_TECH"].append(tech_item)
        elif _is_star50_stock(name):
             buckets["STAR50_STOCK_TECH"].append(tech_item)
        else:
             buckets["HK_STOCK_TECH"].append(tech_item)
             
    # Write Buckets
    for key, data in buckets.items():
        if data:
            writer.write_data(key, data, clear_existing=True)


def _dispatch_transaction_group(writer, group_name, group_data_list):
    """
    Routes 'market_klines' groups to Transaction Tables (TRANS_30D).
    Input `group_data_list` is a flat list of records: [{name:..., date:..., close:...}, ...]
    """
    if not isinstance(group_data_list, list):
        logger.warning(f"⚠️ expected list for group {group_name}, got {type(group_data_list)}")
        return

    if group_name == "指数":
        # Mixed Group: US Indices, HK Indices. Need split?
        # Yes, US_INDEX_TRANS_30D vs HK_INDEX_TRANS_30D
        _split_and_sync_indices(writer, group_data_list)
        return
        
    target_key = None
    if group_name == "大宗商品":
        target_key = "GOLD_TRANS_30D"
    elif group_name == "恒生科技":
        target_key = "HK_STOCK_TRANS_30D" # Mostly stocks in this group list (Top20)
    elif group_name == "美股七巨头+台积电&博通&美光":
        target_key = "US_STOCK_TRANS_30D"
    elif group_name == "港股创新药":
        target_key = "DRUG_STOCK_TRANS_30D"
    elif group_name == "科创50ETF":
        target_key = "STAR50_INDEX_TRANS_30D"
    elif group_name == "科创50持仓":
        target_key = "STAR50_STOCK_TRANS_30D"
        
    if target_key:
        # group_data_list is already the list of records we need
        writer.write_data(target_key, group_data_list, clear_existing=True)


def _split_and_sync_indices(writer, records_list):
    us_records = []
    hk_records = []
    
    for r in records_list:
        name = r.get("name", "")
        if name in ["纳斯达克", "标普500", "VNM(ETF)"]:
            us_records.append(r)
        else:
            hk_records.append(r)
            
    writer.write_data("US_INDEX_TRANS_30D", us_records, clear_existing=True)
    writer.write_data("HK_INDEX_TRANS_30D", hk_records, clear_existing=True)


# --- Helper Checks ---
def _is_us_stock(name):
    # Quick check for known US stocks in our list
    us_names = ["苹果", "微软", "谷歌", "亚马逊", "英伟达", "Meta", "特斯拉", "台积电", "博通", "美光", "XBI", "铀"]
    return any(n in name for n in us_names)

def _is_hk_drug(name):
    drug_names = ["信达", "百济", "药明", "康方", "中国生物", "石药", "三生", "翰森", "科伦"]
    return any(n in name for n in drug_names)

def _is_star50_stock(name):
    # Logic: usually 688xxx but here we check name or just default to False if unknown
    # But usually passed via "科创50持仓" group which is handled separately.
    # This is for MA list dispatch.
    star_names = ["中芯国际", "海光", "寒武纪", "澜起", "中微", "联影", "金山", "芯原", "石头", "传音", "沪硅", "华海", "晶晨", "拓荆", "恒玄", "中控", "佰维", "思特威", "芯联", "百利"]
    # Note: "中芯国际" is also in HK Tech. If strictly 688, it's Star.
    # But in MA list, name is "中芯国际" or "中芯国际(A)".
    if "(A)" in name: return True
    if name == "中芯国际": return False # Assume HK for duplicate name unless specified
    return any(n in name for n in star_names)
