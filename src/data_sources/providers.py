import akshare as ak
import yfinance as yf
from openbb import obb
import pandas as pd
import pandas as pd
import requests
import os
import time
import random
import datetime
from zoneinfo import ZoneInfo

# 设置北京时间
TZ_CN = ZoneInfo("Asia/Shanghai")

class BaseSource:
    def __init__(self, message_bus=None, fetch_start_date=None, end_date=None):
        self.fetch_start_date = fetch_start_date
        self.end_date = end_date
        self.bus = message_bus

class OpenBBSource(BaseSource):
    def fetch_historical(self, symbol, days=365):
        try:
            end_date = datetime.datetime.now()
            days_to_fetch = days + 20
            start_date = end_date - datetime.timedelta(days=days_to_fetch)
            
            # Handle Crypto-like symbols if needed, but yfinance provider handles most
            df = obb.equity.price.historical(symbol=symbol, start_date=start_date.strftime('%Y-%m-%d'), provider="yfinance").to_df()
            
            if df is None or df.empty: return None, "Empty Data"
            
            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
            
            date_col = next((c for c in df.columns if "date" in c), None)
            if not date_col: return None, "No date column"
            
            df = df.rename(columns={date_col: "date"})
            df["date"] = pd.to_datetime(df["date"])
            
            # Ensure columns
            for c in ['open', 'high', 'low', 'close', 'volume']:
                 if c not in df.columns: df[c] = 0.0
                 
            return df, None
        except Exception as e:
            return None, str(e)

class FredSource(BaseSource):
    def __init__(self, fetch_start_date=None, end_date=None, api_key=None):
        super().__init__(fetch_start_date, end_date)
        self.api_key = api_key

    def fetch_series(self, series_id, days=60):
        """Fetch series data from FRED API"""
        # Try direct arg, then env var
        key = self.api_key or os.environ.get("FRED_API_KEY")
        if not key:
            print(f"   [FRED] Error: No API Key found for {series_id}")
            return [], "No API Key"
            
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": key,
            "file_type": "json",
            "observation_start": self.fetch_start_date,
            "observation_end": self.end_date,
            "sort_order": "desc",
            "limit": days 
        }
        
        try:
            r = requests.get(url, params=params, timeout=15)
            data = r.json()
            if "observations" in data:
                # Format: date, value
                res = []
                for obs in data["observations"]:
                    try:
                        val = float(obs["value"])
                        res.append({
                            "date": obs["date"],
                            "value": val
                        })
                    except:
                        continue
                return res, None
            else:
                err_msg = data.get("error_message", "Unknown FRED error")
                return [], err_msg
        except Exception as e:
            return [], str(e)

class AkShareSource(BaseSource):
    def fetch_index_daily(self, symbol):
        """A股/港股/美股指数日线"""
        try:
            # 港股指数 (如 HSTECH)
            # 尝试多种接口
            if symbol == "HSTECH" or symbol == "HSI":
                try:
                    df = ak.stock_hk_index_daily_sina(symbol=symbol)
                    if not df.empty: return df, None
                except:
                    pass

            if str(symbol).startswith('.'): # 美股指数 .NDX
                df = ak.index_us_stock_sina(symbol=symbol)
                return df, None
            
            # 默认尝试 A 股指数
            df = ak.stock_zh_index_daily_em(symbol=symbol)
            return df, None
        except Exception as e:
            print(f"   [AkShare] Index Error {symbol}: {e}")
            return None, str(e)

    def fetch_future_daily(self, symbol):
        """期货日线 (内盘/外盘)"""
        try:
            # 上海金 (au0) - 使用新浪主力连续接口
            if symbol == "au0":
                df = ak.futures_main_sina(symbol="au0")
                return df, None

            # 外盘期货 (如 COMEX 黄金)
            if symbol in ["GC", "SI", "HG", "CL"]:
                df = ak.futures_foreign_hist(symbol=symbol)
                return df, None
            
            # 其他内盘期货
            try:
                df = ak.futures_zh_daily_sina(symbol=symbol)
                return df, None
            except:
                pass
                
            return None, "Empty"
        except Exception as e:
            print(f"   [AkShare] Future Error {symbol}: {e}")
            return None, str(e)

    def fetch_stock_daily(self, symbol, market="cn"):
        """个股数据"""
        try:
            if market == "hk":
                return ak.stock_hk_daily(symbol=symbol, adjust="qfq"), None
            elif market == "us":
                return ak.stock_us_daily(symbol=symbol, adjust="qfq"), None
            elif market == "vn":
                return ak.stock_vn_hist(symbol=symbol), None
            else:
                # A股
                start_date_clean = self.fetch_start_date.replace("-", "") if self.fetch_start_date else "20200101"
                end_date_clean = self.end_date.replace("-", "") if self.end_date else datetime.datetime.now().strftime("%Y%m%d")
                return ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date_clean, end_date=end_date_clean, adjust="qfq"), None
        except Exception as e:
            print(f"   [AkShare] Stock Error {symbol}: {e}")
            return None, str(e)

    def fetch_etf_daily(self, symbol):
        """ETF日线"""
        try:
            start_date_clean = self.fetch_start_date.replace("-", "") if self.fetch_start_date else "20200101"
            end_date_clean = self.end_date.replace("-", "") if self.end_date else datetime.datetime.now().strftime("%Y%m%d")
            return ak.fund_etf_hist_em(symbol=symbol, period="daily", start_date=start_date_clean, end_date=end_date_clean, adjust="qfq"), None
        except Exception as e:
            print(f"   [AkShare] ETF Error {symbol}: {e}")
            return None, str(e)

    # --- 新增 科创50 相关方法 ---
    
    def fetch_star50_valuation(self):
        """获取科创50指数估值 (PE/PB)"""
        try:
            df = ak.stock_zh_index_value_csindex(symbol="000688")
            if df.empty: return []
            
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values('日期')
            # 截取最近180天
            cutoff = datetime.datetime.now() - datetime.timedelta(days=180)
            df = df[df['日期'] >= cutoff]
            df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')
            
            data = []
            for _, row in df.iterrows():
                item = {"日期": row['日期']}
                # 模糊匹配列名
                for col in df.columns:
                    if "市盈率" in col and "1" in col: item["PE"] = row[col]
                    elif "市净率" in col and "1" in col: item["PB"] = row[col]
                # Fallback
                if "PE" not in item:
                    for col in df.columns: 
                        if "市盈率" in col: item["PE"] = row[col]; break
                if "PB" not in item:
                    for col in df.columns:
                        if "市净率" in col: item["PB"] = row[col]; break
                data.append(item)
            return sorted(data, key=lambda x: x["日期"], reverse=True)
        except Exception as e:
            print(f"Error fetch_star50_valuation: {e}")
            return []

    def fetch_star50_margin(self):
        """获取科创50ETF融资融券 (Loop Date)"""
        target_symbol = "588000"
        data_list = []
        current = datetime.datetime.now()
        days_found = 0
        days_checked = 0
        
        while days_found < 5 and days_checked < 20:
            date_str = current.strftime("%Y%m%d")
            if current.weekday() < 5:
                try:
                    df = ak.stock_margin_detail_sse(date=date_str)
                    if not df.empty:
                        df['标的证券代码'] = df['标的证券代码'].astype(str)
                        row = df[df['标的证券代码'] == target_symbol]
                        if not row.empty:
                            r = row.iloc[0]
                            data_list.append({
                                "日期": current.strftime("%Y-%m-%d"),
                                "融资余额": r.get('融资余额'),
                                "融券余额": r.get('融券余额'),
                                "融资买入额": r.get('融资买入额')
                            })
                            days_found += 1
                except:
                    pass
            current -= datetime.timedelta(days=1)
            days_checked += 1
            time.sleep(0.2)
        return data_list

    def fetch_star50_realtime_vol_ratio(self):
        """获取科创50ETF实时量比"""
        try:
            df = ak.fund_etf_spot_em()
            if df.empty: return None, "Spot data empty"
            target = df[df['代码'] == '588000']
            if target.empty: return None, "588000 not found"
            
            row = target.iloc[0]
            return {
                "代码": row['代码'],
                "名称": row['名称'],
                "最新价": row['最新价'],
                "量比": row['量比'],
                "更新时间": datetime.datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')
            }, None
            return df, None
        except Exception as e:
            return None, str(e)

    def fetch_hstech_60m_proxy(self):
        """获取恒生科技指数 60分钟K线 (Forced Proxy: ETF 3033.HK)"""
        try:
            # Use yf directly for specific proxy logic
            t = yf.Ticker("3033.HK")
            hist = t.history(interval="60m", period="1mo")
            if hist.empty: return [], "No data for 3033.HK"
            
            hist = hist.reset_index()
            # Handle timezone
            if isinstance(hist['Datetime'].dtype, pd.DatetimeTZDtype):
                 hist['date'] = hist['Datetime'].dt.tz_convert(TZ_CN).dt.tz_localize(None)
            else:
                 hist['date'] = pd.to_datetime(hist['Datetime'])
                 
            hist = hist.rename(columns={"Volume": "volume", "Close": "close"})
            hist = hist.sort_values('date')
            
            # Simple Volume Ratio Calculation
            hist['hour_str'] = hist['date'].dt.strftime('%H:%M')
            hist['avg_vol'] = hist.groupby('hour_str')['volume'].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
            hist['volume_ratio'] = hist['volume'] / hist['avg_vol']
            hist['volume_ratio'] = hist['volume_ratio'].fillna(0).round(2)
            
            # Slice last 35
            df_slice = hist.iloc[-35:].copy()
            df_slice['date'] = df_slice['date'].dt.strftime('%Y-%m-%d %H:%M')
            
            res = []
            for _, row in df_slice.iterrows():
                res.append({
                    "date": row['date'],
                    "volume": row['volume'],
                    "amount": 0.0, # YF has no amount
                    "volume_ratio": row['volume_ratio'],
                    "close": row['close'],
                    "note": "Source: ETF 3033.HK"
                })
            return res, None
        except Exception as e:
            return [], str(e)

    def fetch_kcb50_60m(self):
        """获取科创50 60分钟K线"""
        try:
            df = None
            try:
                df = ak.stock_zh_a_hist_min_em(symbol="588000", period="60", adjust="qfq")
            except:
                pass
                
            if df is None or df.empty:
                if hasattr(ak, 'fund_etf_hist_min_em'):
                    try: 
                        df = ak.fund_etf_hist_min_em(symbol="588000", period="60", adjust="qfq")
                    except: pass
            
            if df is None or df.empty: return [], "No data"
            
            # 映射列名
            df.rename(columns={
                "时间": "date", "成交量": "volume", "成交额": "amount", 
                "开盘": "open", "收盘": "close", "最高": "high", "最低": "low"
            }, inplace=True)
            
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # 计算量比 (简化版)
            df['hour_str'] = df['date'].dt.strftime('%H:%M')
            # 简单 shift mean for proxy
            df['avg_vol'] = df.groupby('hour_str')['volume'].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
            df['volume_ratio'] = df['volume'] / df['avg_vol']
            df['volume_ratio'] = df['volume_ratio'].fillna(0).round(2)
            
            df_slice = df.iloc[-30:].copy()
            df_slice['date'] = df_slice['date'].dt.strftime('%Y-%m-%d %H:%M')
            
            res = []
            for _, row in df_slice.iterrows():
                res.append({
                    "date": row['date'],
                    "volume": row['volume'],
                    "amount": row['amount'],
                    "volume_ratio": row['volume_ratio'],
                    "close": row['close']
                })
            return res, None
        except Exception as e:
            return [], str(e)

# --- 独立辅助函数: HSTECH 60m proxy ---
def fetch_hstech_60m_proxy():
    """获取恒生科技指数 60分钟K线 (Forced Proxy: ETF 3033.HK)"""
    try:
        t = yf.Ticker("3033.HK")
        hist = t.history(interval="60m", period="1mo")
        if hist.empty: return [], "No data for 3033.HK"
        
        hist = hist.reset_index()
        # 处理时区
        if isinstance(hist['Datetime'].dtype, pd.DatetimeTZDtype):
             hist['date'] = hist['Datetime'].dt.tz_convert(TZ_CN).dt.tz_localize(None)
        else:
             hist['date'] = pd.to_datetime(hist['Datetime'])
             
        hist.rename(columns={"Volume": "volume", "Close": "close"}, inplace=True)
        hist = hist.sort_values('date')
        
        # 量比
        hist['hour_str'] = hist['date'].dt.strftime('%H:%M')
        # 简单计算
        hist['avg_vol'] = hist.groupby('hour_str')['volume'].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
        hist['volume_ratio'] = hist['volume'] / hist['avg_vol']
        hist['volume_ratio'] = hist['volume_ratio'].fillna(0).round(2)
        
        df_slice = hist.iloc[-35:].copy()
        df_slice['date'] = df_slice['date'].dt.strftime('%Y-%m-%d %H:%M')
        
        res = []
        for _, row in df_slice.iterrows():
            res.append({
                "date": row['date'],
                "volume": row['volume'],
                "amount": 0,
                "volume_ratio": row['volume_ratio'],
                "close": row['close'],
                "note": "Source: ETF 3033.HK"
            })
        return res, None
    except Exception as e:
        return [], str(e)
