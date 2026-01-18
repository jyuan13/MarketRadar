# -*- coding:utf-8 -*-
import datetime
import time
import pandas as pd
import akshare as ak
try:
    from config.settings import AKSHARE_SYMBOLS, TZ_CN
except ImportError:
    from zoneinfo import ZoneInfo
    TZ_CN = ZoneInfo("Asia/Shanghai")
    AKSHARE_SYMBOLS = {}

class AkshareSource:
    """
    Akshare Data Source Adapter
    Wraps Akshare functions for China market data.
    """
    
    def fetch_southbound_flow(self, days=30):
        """
        获取南向资金净流入 (包含重试机制)
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                df = ak.stock_hsgt_hist_em(symbol="南向资金")
                if df.empty:
                    raise ValueError("Empty dataframe")
                
                if '日期' not in df.columns or '当日成交净买额' not in df.columns:
                    raise ValueError(f"Unexpected columns: {df.columns.tolist()}")
                
                df['日期'] = pd.to_datetime(df['日期'])
                df = df.sort_values('日期')
                
                cutoff = datetime.datetime.now() - datetime.timedelta(days=days)
                df = df[df['日期'] >= cutoff]
                
                results = []
                for _, row in df.iterrows():
                    results.append({
                        "date": row['日期'].strftime('%Y-%m-%d'),
                        "value": row['当日成交净买额'] # 单位：亿元
                    })
                
                results.sort(key=lambda x: x['date'], reverse=True)
                return results, None
                
            except Exception as e:
                if attempt == max_retries - 1:
                    return [], str(e)
                time.sleep(1)
        return [], "Max retries exceeded"

    def fetch_ashare_indices(self, days=30):
        """
        获取A股主要指数日线
        """
        results = {}
        error_msg = []
        
        targets = [
            {"name": "上证指数", "symbol": AKSHARE_SYMBOLS.get("sh_index", "sh000001")},
            {"name": "深证成指", "symbol": AKSHARE_SYMBOLS.get("sz_index", "sz399001")},
            {"name": "创业板指", "symbol": AKSHARE_SYMBOLS.get("cyb_index", "sz399006")},
            {"name": "沪深300", "symbol": AKSHARE_SYMBOLS.get("hs300", "sh000300")},
        ]
        
        for t in targets:
            try:
                df = ak.stock_zh_index_daily_em(symbol=t["symbol"])
                if df.empty:
                    error_msg.append(f"{t['name']}: Empty")
                    continue
                
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                df = df.iloc[-days:]
                
                data_list = []
                for _, row in df.iterrows():
                    data_list.append({
                        "date": row['date'].strftime('%Y-%m-%d'),
                        "open": row['open'],
                        "close": row['close'],
                        "high": row['high'],
                        "low": row['low'],
                        "volume": row['volume'],
                        "amount": row.get('amount', 0),
                        "name": t['name']
                    })
                results[t['name']] = data_list
            except Exception as e:
                error_msg.append(f"{t['name']}: {e}")
        
        return results, "; ".join(error_msg) if error_msg else None

    def fetch_star50_daily(self, days=30):
        """科创50指数"""
        try:
            symbol = AKSHARE_SYMBOLS.get("kc50", "000688")
            df = ak.stock_zh_index_daily_em(symbol=symbol)
            if df.empty: return [], "Empty"
            
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').iloc[-days:]
            
            data = []
            for _, row in df.iterrows():
                data.append({
                    "date": row['date'].strftime('%Y-%m-%d'),
                    "close": row['close'],
                    "volume": row['volume'],
                })
            return data, None
        except Exception as e:
            return [], str(e)

    def fetch_star50_valuation(self, days=180):
        """科创50估值 (PE/PB)"""
        try:
            df = ak.stock_zh_index_value_csindex(symbol=AKSHARE_SYMBOLS.get("kc50", "000688"))
            if df.empty: return [], "Empty"
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values('日期')
            cutoff = datetime.datetime.now() - datetime.timedelta(days=days)
            df = df[df['日期'] >= cutoff]
            
            data = []
            for _, row in df.iterrows():
                pe = row.get("市盈率1", row.get("市盈率2", 0))
                pb = row.get("市净率1", row.get("市净率2", 0))
                data.append({
                    "date": row['日期'].strftime('%Y-%m-%d'),
                    "PE": pe,
                    "PB": pb
                })
            data.sort(key=lambda x: x['date'], reverse=True)
            return data, None
        except Exception as e:
            return [], str(e)

    def fetch_star50_margin(self, days=20):
        """科创50ETF融资融券"""
        # 需循环查询，比较耗时，此处简化，沿用逻辑
        # 省略详细实现，建议在 Collector 中处理循环逻辑，或在此处完整实现
        # 为保持 Source 简单，这里仅提供单日查询或简版
        # 鉴于原始逻辑复杂，这里暂留 Interface
        return [], "Not Fully Implemented in Source (Complex Loop)"

    def fetch_scfi(self):
        """
        上海出口集装箱运价指数 (SCFI)
        """
        # 尝试 ak.index_scfi() 如果存在
        # 备选: ak.index_shipping_yangshan()
        try:
            # 假设接口
            if hasattr(ak, 'index_scfi'):
                 df = ak.index_scfi()
                 # Process df
            # 查阅源码发现 ak.transport_container_index
            return [], "Interface Pending"
        except:
            return [], "Not supported"
    
    def fetch_dr007(self, days=30):
        """
        DR007 银行间7天回购利率
        """
        try:
             # ak.bond_china_close_return_map?
             # ak.rate_interbank(market="上海银行间同业拆放市场", symbol="Shibor人民币", indicator="1周") -> SHIBOR 1W != DR007
             # DR007: 存款类金融机构7天质押式回购 weighted average
             # ak.bond_repo_zh_tick(symbol="DR007") -> tick data?
             return [], "Interface Pending"
        except:
             return [], "Error"

    def fetch_star50_realtime_vol(self):
         """科创50实时量比"""
         try:
             df = ak.fund_etf_spot_em()
             target = df[df['代码'] == AKSHARE_SYMBOLS.get("kc50_etf", "588000")]
             if target.empty: return None, "Not found"
             row = target.iloc[0]
             return {
                 "code": row['代码'],
                 "name": row['名称'],
                 "price": row['最新价'],
                 "vol_ratio": row['量比'],
                 "time": datetime.datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')
             }, None
         except Exception as e:
             return None, str(e)
