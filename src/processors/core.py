# processors.py
import pandas as pd
from src.utils.tools import calculate_ma as _legacy_calc_ma # Re-use legacy logic initially

class DataProcessor:
    def __init__(self):
        pass

    def clean_kline_data(self, df_or_list):
        # Standardize to list of dicts with keys: date, open, close, high, low, volume, name...
        if isinstance(df_or_list, pd.DataFrame):
            # Ensure date is string YYYY-MM-DD
            if 'date' in df_or_list.columns:
                 df_or_list['date'] = pd.to_datetime(df_or_list['date']).dt.strftime('%Y-%m-%d')
            return df_or_list.to_dict(orient="records")
        return df_or_list

    def calculate_ma(self, df, name):
        """Wrapper for MA calculation"""
        if df.empty: return []
        df['name'] = name # Ensure name is present for legacy util
        return _legacy_calc_ma(df)

    def process_star50_hourly(self, df):
        if df is None or df.empty: return []
        
        # Volume Ratio Logic
        try:
            df['hour_str'] = df['date'].dt.strftime('%H:%M')
            df['avg_vol_5d'] = df.groupby('hour_str')['volume'].transform(
                lambda x: x.shift(1).rolling(window=5, min_periods=1).mean()
            )
            df['volume_ratio'] = df['volume'] / df['avg_vol_5d']
            df['volume_ratio'] = df['volume_ratio'].fillna(0.0).replace([float('inf')], 0.0)
            df['volume_ratio'] = df['volume_ratio'].apply(lambda x: round(x, 2))
        except:
             pass
             
        # Format
        df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M')
        return df.to_dict(orient="records")
