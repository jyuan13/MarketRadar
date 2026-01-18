# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np

class TechnicalAnalysis:
    """
    Technical Analysis Processor
    """
    
    @staticmethod
    def calculate_signals(df_input):
        """
        Generate signals for a DataFrame using MyTT.
        """
        if df_input is None or df_input.empty or len(df_input) < 30:
            return ["数据不足"]
            
        try:
            from . import MyTT
        except ImportError:
            try:
                import MyTT
            except ImportError:
                return ["MyTT模块缺失"]

        # Prepare data for MyTT (numpy arrays)
        CLOSE = df_input['close'].values
        HIGH = df_input['high'].values
        LOW = df_input['low'].values
        OPEN = df_input['open'].values
        
        # 1. MACD
        dif, dea, macd_bar = MyTT.MACD(CLOSE)
        
        # 2. KDJ
        k, d, j = MyTT.KDJ(CLOSE, HIGH, LOW)
        
        # 3. RSI
        rsi6 = MyTT.RSI(CLOSE, 6)
        
        signals = []
        
        # Check signals for the last day vs previous day
        if len(dif) > 1:
            if dif[-2] < dea[-2] and dif[-1] > dea[-1]:
                signals.append("MACD金叉")
            elif dif[-2] > dea[-2] and dif[-1] < dea[-1]:
                signals.append("MACD死叉")
                
        if len(k) > 1:
            if k[-2] < d[-2] and k[-1] > d[-1]:
                signals.append("KDJ金叉")
                
        if len(rsi6) > 0:
            if rsi6[-1] > 80:
                signals.append("RSI超买")
            elif rsi6[-1] < 20:
                signals.append("RSI超卖")
                
        if not signals:
            signals.append("无特殊技术形态")
            
        metrics = {
            "MACD": round(float(macd_bar[-1]), 4) if len(macd_bar) > 0 else 0,
            "DIF": round(float(dif[-1]), 4) if len(dif) > 0 else 0,
            "DEA": round(float(dea[-1]), 4) if len(dea) > 0 else 0,
            "K": round(float(k[-1]), 2) if len(k) > 0 else 0,
            "D": round(float(d[-1]), 2) if len(d) > 0 else 0,
            "J": round(float(j[-1]), 2) if len(j) > 0 else 0,
            "RSI6": round(float(rsi6[-1]), 2) if len(rsi6) > 0 else 0,
            "Signals": signals
        }
        
        return metrics

    @staticmethod
    def analyze_market_breadth(df_input):
        # Implementation for market breadth if data available
        pass
