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
            "US_Banks": "美股银行" # Legacy didn't strictly have this in top grouping but we can add or merge? 
            # In legacy `main.py`, US Banks were just fetched but maybe not in a specific named group? 
            # Actually legacy `main.py` did NOT add US Banks to `all_data_collection` via `market_core` groups?
            # Wait, `fetch_us_banks_daily` was called separately in `main.py` and added to `kline_data_dict["data"]`.
            # So "美股银行" key is probably fine or we just let "US_Banks" map to whatever.
        }
        
        market_klines = {}
        
        for cfg_cat, ticker_rows in groups.items():
            report_key = cat_map.get(cfg_cat, cfg_cat) # Fallback to config name
            
            # Flatten the dictionary {ticker: [rows]} into a single list of rows
            flat_rows = []
            if isinstance(ticker_rows, dict):
                for rows in ticker_rows.values():
                    if isinstance(rows, list):
                        flat_rows.extend(rows)
            elif isinstance(ticker_rows, list):
                 flat_rows.extend(ticker_rows)
            
            # Sort: Name (asc), Date (desc)
            try:
                flat_rows.sort(key=lambda x: x.get('name', '')) # Ascending Name
                flat_rows.sort(key=lambda x: x.get('date', ''), reverse=True) # Descending Date
            except Exception as e:
                print(f"Sort error for {cfg_cat}: {e}")
            
            market_klines[report_key] = flat_rows

        merged = {
            "meta": metadata,
            # Legacy Key: market_klines (was data_section)
            "market_klines": market_klines,
            
            # Legacy Key: 技术分析
            "技术分析": {
                "指数+个股日均线": collectors_data.get("ma_general", []),
                "大宗商品": collectors_data.get("ma_commodities", [])
            },
            
            # Legacy Keys for Macro/Detail sections
            "market_fx": collectors_data.get("macro_fx", {}),
            "china": collectors_data.get("macro_china", {}),
            "usa": collectors_data.get("macro_usa", {}),
            "japan": collectors_data.get("macro_japan", {}),
            "hk": collectors_data.get("macro_hk", {}),
            "科创50": collectors_data.get("star50", {}),
            
            # Optional extra
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
