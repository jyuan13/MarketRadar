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
        Assemble the final dictionary structure matching legacy MarketRadar format.
        """
        groups = collectors_data.get("groups", {})
        
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
            "US_Banks": "美股银行"
        }
        
        data_section = {}
        for cfg_cat, report_key in cat_map.items():
            if cfg_cat in groups:
                # Flatten the dictionary {ticker: [rows]} into a single list of rows
                # Legacy format: Flat list of all records for the category, sorted by Date DESC, Name ASC
                flat_rows = []
                for ticker_rows in groups[cfg_cat].values():
                    if isinstance(ticker_rows, list):
                        flat_rows.extend(ticker_rows)
                
                # Sort: Date (desc), Name (asc)
                # Ensure date is comparable (string is fine if format is YYYY-MM-DD)
                try:
                    flat_rows.sort(key=lambda x: (x.get('date', ''), x.get('name', '')), reverse=False)
                    # Legacy sort was: df.sort_values(by=['date', 'name'], ascending=[False, True])
                    # So primary key Date is Descending, Name is Ascending.
                    # Python sort is stable. We can sort by Name (Asc) first, then Date (Desc).
                    flat_rows.sort(key=lambda x: x.get('name', '')) # Ascending Name
                    flat_rows.sort(key=lambda x: x.get('date', ''), reverse=True) # Descending Date
                except Exception as e:
                    print(f"Sort error for {cfg_cat}: {e}")
                
                data_section[report_key] = flat_rows
            else:
                 data_section[report_key] = []

        merged = {
            "meta": metadata,
            "data": data_section,
            "ma_data": { # Reverted from "技术分析"
                "general": collectors_data.get("ma_general", []),      # Reverted from "指数+个股日均线"
                "commodities": collectors_data.get("ma_commodities", [])
            },
            # Extra data collected by new scrapers/APIs can stay or be hidden if strict adherence is required.
            # User asked "is there any omission?", implying extras are fine but defaults must be there.
            # We keep these as they provide value, but ensure core structure is identical.
            "market_fx": collectors_data.get("macro_fx", {}),
            "china": collectors_data.get("macro_china", {}),
            "usa": collectors_data.get("macro_usa", {}),
            "japan": collectors_data.get("macro_japan", {}),
            "hk": collectors_data.get("macro_hk", {}),
            "科创50": collectors_data.get("star50", {}),
            "加密货币": collectors_data.get("crypto", {})
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
