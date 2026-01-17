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
        # Map back to legacy structure
        # keys: "技术分析", "market_fx", "科创50", "加密货币", "china", "usa", "japan", "hk", "market_klines"
        
        merged = {
            "meta": metadata,
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
            "market_klines": collectors_data.get("klines", {})
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
