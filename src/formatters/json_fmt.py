# formatters.py
import json
import numpy as np

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        elif isinstance(obj, np.floating): return float(obj)
        elif isinstance(obj, np.ndarray): return obj.tolist()
        return super(NpEncoder, self).default(obj)

class JsonFormatter:
    def __init__(self):
        pass

    def clean_data(self, data):
        """Recursively clean floats and NaNs"""
        import math
        if isinstance(data, dict):
            return {k: self.clean_data(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.clean_data(x) for x in data]
        elif isinstance(data, float):
            if math.isnan(data) or math.isinf(data):
                return None
            return round(data, 2)
        elif isinstance(data, (np.int64, np.int32)):
            return int(data)
        else:
            return data

    def structure_final_report(self, collectors_data, metadata):
        """
        Assemble the final dictionary structure.
        collectors_data: content from various collectors
        """
        groups = collectors_data.get("groups", {})
        
        # Helper to convert {name: [rows]} to [ {name: name, rows...} ] if needed? 
        # Actually legacy output: "data": { "指数": [ { ...row data... }, ... ] } ??? 
        # No, legacy `fetch_group_data` returned LIST of dataframes/dicts. 
        # Actually legacy `all_data_collection["data"]["指数"]` is a LIST of data for each ticker? 
        # Let's check Utils or Legacy code. 
        # `market_core.fetch_group_data` returns `data_collection` (list).
        # Inside, it appends `cleaned_data` (list of dicts) to `data_collection`.
        # So `data["指数"]` is `[ [record1, record2...], [record1...] ]` ? 
        # OR is it `[ {name: "Nasdaq", history: [...]}, ... ]` ?
        # Checking legacy output structure is safest.
        # Assuming `collectors.collect_klines` returns {name: [rows]}. 
        # We should convert this dict to matching legacy structure.
        
        # Mapping Config Categories to Legacy Report Keys
        cat_map = {
            "Indices": "指数",
            "Commodities": "大宗商品",
            "HSTECH_Components": "恒生科技",
            "Vietnam_Top10": "新兴市场",
            "US_BigTech": "美股七巨头+台积电&博通&美光",
            "HK_Pharma": "港股创新药",
            "Star50_ETF": "科创50ETF",
            "Star50_Holdings": "科创50持仓",
            "US_Banks": "美股银行" # New?
        }
        
        data_section = {}
        for cfg_cat, report_key in cat_map.items():
            if cfg_cat in groups:
                 # Flatten the dict values (lists of history) into a list of lists? 
                 # Or just list of objects.
                 # Legacy `fetch_group_data` returned `data_collection` list.
                 # Let's assume list of history lists.
                 # collectors data: { "Nasdaq": [{date, close...}, ...], "SPX": [...] }
                 # Legacy expectation: [ [{name:"Nasdaq", ...}, ...], ... ]
                 # We simply put valid lists into the array.
                 data_section[report_key] = list(groups[cfg_cat].values())

        merged = {
            "meta": metadata,
            "data": data_section,
            "技术分析": {
                "指数+个股日均线": collectors_data.get("ma_general", []),
                "大宗商品": collectors_data.get("ma_commodities", [])
            },
            "market_fx": collectors_data.get("macro_fx", {}),
            "科创50": collectors_data.get("star50", {}),
            "加密货币": collectors_data.get("crypto", {}),
            "china": collectors_data.get("macro_china", {}),
            "usa": collectors_data.get("macro_usa", {}),
            "japan": collectors_data.get("macro_japan", {}),
            "hk": collectors_data.get("macro_hk", {}),
            # "market_klines": collectors_data.get("klines", {}) # Removed flattened
        }
        return self.clean_data(merged)

    def save_to_file(self, data, filename):
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, cls=NpEncoder, ensure_ascii=False, indent=4)
            return True
        except Exception as e:
            print(f"Write error: {e}")
            return False
