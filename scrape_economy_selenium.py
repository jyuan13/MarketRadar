# scrape_economy_selenium.py
# -----------------------------------------------------------------------------
# DeepSeek Finance Project - Macro Data Scraper (Concurrent Version)
# 功能描述:
# 1. 使用 Selenium (Headless Chrome) 并发抓取东方财富网的多项宏观经济数据。
# 2. [对外接口] 提供 get_macro_data() 供 MarketRadar 主程序调用。
# 3. [稳定性] 增加重试机制：单个任务失败时自动重试3次。
# 4. [定制逻辑] 针对“中国_南向资金”仅获取近30天数据；其他数据保持近180天。
# 5. [修复] 增加页面加载超时限制(45s)和降低并发数(2)。
# 6. [修复] 修复南向资金 .dt 报错。
# 7. [新增] 返回详细状态日志供主程序生成 Log 文件。
# -----------------------------------------------------------------------------

import time
import json
import pandas as pd
import datetime
import os
import numpy as np
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class MacroDataScraper:
    def __init__(self):
        # 目标数据源配置
        self.targets = {
            "中国_CPI": "https://data.eastmoney.com/cjsj/cpi.html",
            "中国_PMI": "https://data.eastmoney.com/cjsj/pmi.html",
            "中国_PPI": "https://data.eastmoney.com/cjsj/ppi.html",
            "中国_货币供应量": "https://data.eastmoney.com/cjsj/hbgyl.html",
            "中国_LPR": "https://data.eastmoney.com/cjsj/globalRateLPR.html",
            "中国_南向资金": "https://data.eastmoney.com/hsgtV2/hsgtDetail/scgkDetail_nx.html", 
            "美国_ISM制造业PMI": "https://data.eastmoney.com/cjsj/foreign_0_0.html",
            "美国_ISM非制造业指数": "https://data.eastmoney.com/cjsj/foreign_0_1.html",
            "美国_非农就业": "https://data.eastmoney.com/cjsj/foreign_0_2.html",
            "美国_核心零售销售月率": "https://data.eastmoney.com/cjsj/foreign_0_9.html",
            "美国_利率决议": "https://data.eastmoney.com/cjsj/foreign_8_0.html",
            "日本_央行利率决议": "https://data.eastmoney.com/cjsj/foreign_3_0.html",
        }

        # 输出结构映射表
        self.key_mapping = {
            "中国_CPI": ("china", "CPI"),
            "中国_PMI": ("china", "PMI_制造业"),
            "中国_PPI": ("china", "PPI"),
            "中国_货币供应量": ("china", "货币供应量"),
            "中国_LPR": ("china", "LPR"),
            "中国_南向资金": ("china", "南向资金净流入"), 
            "美国_ISM制造业PMI": ("usa", "ISM_制造业PMI"),
            "美国_ISM非制造业指数": ("usa", "ISM_非制造业PMI"),
            "美国_非农就业": ("usa", "非农就业人数"),
            "美国_核心零售销售月率": ("usa", "零售销售月率"),
            "美国_利率决议": ("usa", "利率决议"),
            "日本_央行利率决议": ("japan", "央行利率")
        }
        
        # 结果存储字典
        self.results = {}
        self.status_logs = []
        
        # Chrome 浏览器配置
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--log-level=3")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36')
        
        # 独立运行时输出文件路径
        self.output_path = "OnlineReport.json"

    def clean_date(self, date_str):
        try:
            date_str = str(date_str).strip()
            if "年" in date_str and "月" in date_str:
                clean_str = date_str.replace("月份", "").replace("月", "").replace("年", "-")
                if clean_str.count("-") == 1:
                    clean_str += "-01"
                return pd.to_datetime(clean_str)
            return pd.to_datetime(date_str)
        except Exception:
            return pd.NaT

    def fetch_single_source(self, name, url):
        max_retries = 3
        # 南向资金仅需30天，其他数据保持180天
        days_to_keep = 30 if "南向资金" in name else 180
        last_error = None

        for attempt in range(1, max_retries + 1):
            print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium)...")
            driver = None
            try:
                driver = webdriver.Chrome(options=self.chrome_options)
                
                # 强制设置超时，防止页面加载卡死
                driver.set_page_load_timeout(45) 
                driver.set_script_timeout(45)
                
                driver.get(url)
                # 等待表格加载
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                html = driver.page_source
                dfs = pd.read_html(StringIO(html))
                
                if not dfs:
                    raise ValueError("页面解析为空，未找到表格数据")

                df = max(dfs, key=lambda x: len(x))
                
                # 处理 MultiIndex
                if isinstance(df.columns, pd.MultiIndex):
                    new_cols = []
                    for col in df.columns:
                        valid_parts = [str(c) for c in col if "Unnamed" not in str(c) and str(c).strip() != ""]
                        seen = set()
                        unique_parts = [x for x in valid_parts if not (x in seen or seen.add(x))]
                        new_cols.append("".join(unique_parts))
                    df.columns = new_cols
                
                df.columns = [str(c).replace(" ", "").replace("\n", "").strip() for c in df.columns]
                
                possible_date_cols = ['月份', '时间', '日期', '发布日期', '公布日期']
                date_col = next((col for col in df.columns if any(x in str(col) for x in possible_date_cols)), None)
                
                if date_col:
                    df['_std_date'] = df[date_col].apply(self.clean_date)
                    df = df.dropna(subset=['_std_date'])
                    
                    # [修复] 强制转换为 datetime 类型
                    df['_std_date'] = pd.to_datetime(df['_std_date'])
                    
                    cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_to_keep)
                    df = df[df['_std_date'] >= cutoff_date]
                    
                    df['_std_date'] = df['_std_date'].dt.strftime('%Y-%m-%d')
                    df = df.replace({'-': None, 'nan': None})
                    df = df.where(pd.notnull(df), None)

                    if name == "中国_南向资金":
                        keep_cols = ['_std_date']
                        for c in df.columns:
                            if "净买额" in c and "当日" in c:
                                keep_cols.append(c)
                            elif "成交笔数" in c:
                                keep_cols.append(c)
                        df = df[keep_cols]
                        df.rename(columns={'_std_date': '日期'}, inplace=True)
                    
                    # 统一增加 '日期' 字段用于合并
                    if '日期' not in df.columns and '_std_date' in df.columns:
                        df['日期'] = df['_std_date']

                    records = df.to_dict('records')
                    print(f"✅ [{name}] 抓取成功! 获得 {len(records)} 条记录")
                    return name, records, None # Success
                else:
                    raise ValueError(f"未找到日期列: {df.columns.tolist()}")

            except Exception as e:
                last_error = str(e)
                print(f"❌ [{name}] 失败: {last_error[:200]}") 
                if attempt < max_retries:
                    time.sleep(2)
            finally:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
        
        # 最终失败
        return name, [], last_error

    def run_concurrent(self):
        print("🚀 [Scraper] 正在并发抓取宏观数据 (Workers=2)...")
        # 清空之前的日志
        self.status_logs = []
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_name = {
                executor.submit(self.fetch_single_source, name, url): name 
                for name, url in self.targets.items()
            }
            for future in as_completed(future_to_name):
                name, data, error_msg = future.result()
                if not error_msg:
                    self.results[name] = data
                    self.status_logs.append({'name': name, 'status': True, 'error': None})
                else:
                    self.results[name] = []
                    self.status_logs.append({'name': name, 'status': False, 'error': error_msg})
                    
        return self.results, self.status_logs

    def organize_data(self):
        """
        将扁平的 results 转换为嵌套的字典结构
        """
        nested_data = {
            "china": {},
            "usa": {},
            "japan": {}
        }
        
        for old_key, data_list in self.results.items():
            if not data_list:
                continue
            if old_key in self.key_mapping:
                country_key, metric_key = self.key_mapping[old_key]
                nested_data[country_key][metric_key] = data_list
        
        return nested_data

    def get_data_dict(self):
        """
        对外主接口：运行抓取并返回 (data_dict, status_logs)
        """
        self.run_concurrent()
        return self.organize_data(), self.status_logs

    def save_custom_json(self):
        data, _ = self.get_data_dict() # 独立运行时忽略日志返回
        try:
            with open(self.output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            print(f"💾 独立运行数据已写入: {self.output_path}")
        except Exception as e:
            print(f"❌ 写入文件失败: {e}")

# 对外暴露的便捷函数
def get_macro_data():
    scraper = MacroDataScraper()
    return scraper.get_data_dict()

if __name__ == "__main__":
    scraper = MacroDataScraper()
    scraper.save_custom_json()