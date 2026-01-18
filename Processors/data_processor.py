# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime

class DataProcessor:
    """
    Data Processing Layer
    Handles data cleaning, moving average calculation, and merging.
    """
    
    @staticmethod
    def clean_and_round(data, decimals=2):
        """
        Recursively clean and round data.
        """
        if isinstance(data, dict):
            return {k: DataProcessor.clean_and_round(v, decimals) for k, v in data.items()}
        elif isinstance(data, list):
            return [DataProcessor.clean_and_round(v, decimals) for v in data]
        elif isinstance(data, float):
            if pd.isna(data) or np.isinf(data):
                return None
            return round(data, decimals)
        elif isinstance(data, (np.integer, np.int64, int)):
            return int(data)
        elif isinstance(data, (np.floating, np.float64)):
            if pd.isna(data) or np.isinf(data):
                return None
            return round(float(data), decimals)
        return data

    @staticmethod
    def calculate_ma(data_list, days=[5, 10, 20, 60, 120, 200]):
        """
        Calculate Moving Averages for a list of daily data.
        Input: list of dicts [{'date':..., 'close':...}, ...] sorted by date desc or asc?
        We assume input is a list of dicts. We convert to DF for calc.
        """
        if not data_list:
            return {}
        
        df = pd.DataFrame(data_list)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
        
        if 'close' not in df.columns:
             return {}

        results = {}
        # Calculate MA
        for d in days:
            col_name = f"MA{d}"
            # Rolling mean
            ma_series = df['close'].rolling(window=d).mean()
            # Get the latest value (last row)
            if not ma_series.empty:
                val = ma_series.iloc[-1]
                if not pd.isna(val):
                    results[col_name] = round(float(val), 2)
                else:
                    results[col_name] = None
        
        # Also return latest close, change etc if needed
        latest = df.iloc[-1]
        results['latest_close'] = round(float(latest['close']), 2)
        results['date'] = latest['date'].strftime('%Y-%m-%d')
        
        return results

    @staticmethod
    def deep_merge(target, source):
        """
        Deep merge two dictionaries.
        """
        for k, v in source.items():
            if isinstance(v, dict) and k in target and isinstance(target[k], dict):
                DataProcessor.deep_merge(target[k], v)
            else:
                target[k] = v
        return target
