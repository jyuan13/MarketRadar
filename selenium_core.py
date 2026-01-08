# selenium_core.py
# -----------------------------------------------------------------------------
# DeepSeek Finance Project - Selenium Scraper Core Logic
# -----------------------------------------------------------------------------

import time
import json
import pandas as pd
import re
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
        # [修改] 已移除 "中国_南向资金" 以避免冗余和 0 记录问题
        self.targets = {
            "中国_CPI": "https://data.eastmoney.com/cjsj/cpi.html",
            "中国_PMI": "https://data.eastmoney.com/cjsj/pmi.html",
            "中国_PPI": "https://data.eastmoney.com/cjsj/ppi.html",
            "中国_货币供应量": "https://data.eastmoney.com/cjsj/hbgyl.html",
            "中国_LPR": "https://data.eastmoney.com/cjsj/globalRateLPR.html",
            "美国_ISM制造业PMI": "https://data.eastmoney.com/cjsj/foreign_0_0.html",
            "美国_ISM非制造业指数": "https://data.eastmoney.com/cjsj/foreign_0_1.html",
            "美国_非农就业": "https://data.eastmoney.com/cjsj/foreign_0_2.html",
            "美国_核心零售销售月率": "https://data.eastmoney.com/cjsj/foreign_0_9.html",
            "美国_利率决议": "https://data.eastmoney.com/cjsj/foreign_8_0.html",
            "日本_央行利率决议": "https://data.eastmoney.com/cjsj/foreign_3_0.html",
            "恒生医疗保健指数": "https://cn.investing.com/indices/hang-seng-healthcare-historical-data"
        }

        self.key_mapping = {
            "中国_CPI": ("china", "CPI"),
            "中国_PMI": ("china", "PMI_制造业"),
            "中国_PPI": ("china", "PPI"),
            "中国_货币供应量": ("china", "货币供应量"),
            "中国_LPR": ("china", "LPR"),
            "美国_ISM制造业PMI": ("usa", "ISM_制造业PMI"),
            "美国_ISM非制造业指数": ("usa", "ISM_非制造业PMI"),
            "美国_非农就业": ("usa", "非农就业人数"),
            "美国_核心零售销售月率": ("usa", "零售销售月率"),
            "美国_利率决议": ("usa", "利率决议"),
            "日本_央行利率决议": ("japan", "央行利率"),
            "恒生医疗保健指数": ("hk", "恒生医疗保健指数")
        }
        
        self.results = {}
        self.status_logs = []
        
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--log-level=3")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        self.chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36')
        
        self.chrome_options.page_load_strategy = 'eager'

        prefs = {"profile.managed_default_content_settings.images": 2}
        self.chrome_options.add_experimental_option("prefs", prefs)
        
        self.output_path = "OnlineReport.json"

    def clean_date(self, date_str):
        try:
            date_str = str(date_str).strip()
            if "年" in date_str:
                clean_str = date_str.replace("月份", "").replace("月", "").replace("日", "").replace("年", "-")
                if clean_str.count("-") == 1:
                    clean_str += "-01"
                return pd.to_datetime(clean_str)
            return pd.to_datetime(date_str)
        except Exception:
            return pd.NaT

    def _clean_investing_date(self, date_str):
        try:
            date_str = str(date_str).strip()
            if "年" in date_str:
                clean_str = date_str.replace("年", "-").replace("月份", "").replace("月", "-").replace("日", "")
                clean_str = re.sub(r'-+', '-', clean_str)
                if clean_str.count("-") == 1:
                    clean_str += "-01"
                return pd.to_datetime(clean_str)
            return pd.to_datetime(date_str)
        except Exception:
            return pd.NaT

    def parse_volume(self, vol_str):
        if not isinstance(vol_str, str):
            return vol_str
        
        vol_str = vol_str.upper().strip()
        if vol_str in ['-', '', 'NAN', 'NONE']:
            return 0.0
            
        multiplier = 1.0
        if 'K' in vol_str:
            multiplier = 1000.0
            vol_str = vol_str.replace('K', '')
        elif 'M' in vol_str:
            multiplier = 1000000.0
            vol_str = vol_str.replace('M', '')
        elif 'B' in vol_str:
            multiplier = 1000000000.0
            vol_str = vol_str.replace('B', '')
            
        try:
            return float(vol_str) * multiplier
        except:
            return 0.0

    def parse_percentage(self, pct_str):
        if not isinstance(pct_str, str):
            return pct_str
        try:
            return float(pct_str.replace('%', '').replace(',', ''))
        except:
            return 0.0

    def fetch_investing_source(self, name, url):
        max_retries = 5
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - Investing专线)...")
            driver = None
            try:
                driver = webdriver.Chrome(options=self.chrome_options)
                
                driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                    "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
                })

                driver.set_page_load_timeout(60)
                driver.set_script_timeout(60)
                
                driver.get(url)
                
                try:
                    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                except:
                    print(f"⚠️ [{name}] 等待表格超时，尝试继续解析源码...")
                
                html = driver.page_source
                dfs = pd.read_html(StringIO(html))
                
                if not dfs:
                    raise ValueError("页面解析为空，未找到表格数据")

                target_df = None
                for df in dfs:
                    cols = [str(c).replace(" ", "").replace("\n", "").strip() for c in df.columns]
                    if all(k in cols for k in ['日期', '收盘', '交易量']):
                        df.columns = cols 
                        target_df = df
                        break
                
                if target_df is None:
                    for df in dfs:
                        cols = [str(c).replace(" ", "").replace("\n", "").strip() for c in df.columns]
                        if '日期' in cols and '收盘' in cols:
                            df.columns = cols
                            target_df = df
                            break

                if target_df is None:
                     raise ValueError(f"未找到符合 Investing 格式的表格 (检查列名: 日期/收盘/交易量)")

                df = target_df.copy()
                
                df['_std_date'] = df['日期'].apply(self._clean_investing_date)
                
                if df['_std_date'].isna().all() and not df.empty:
                    print(f"❌ [{name}] 日期解析全部失败! 原始数据示例: {df['日期'].iloc[0]}")
                
                df = df.dropna(subset=['_std_date'])
                df['_std_date'] = pd.to_datetime(df['_std_date'])
                
                cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=180)
                df = df[df['_std_date'] >= cutoff_date]
                
                df['_std_date'] = df['_std_date'].dt.strftime('%Y-%m-%d')
                
                rename_map = {
                    '日期': '日期', 
                    '收盘': 'close', 
                    '开盘': 'open',
                    '高': 'high', 
                    '低': 'low', 
                    '交易量': 'volume', 
                    '涨跌幅': 'change_pct'
                }
                
                available_map = {k: v for k, v in rename_map.items() if k in df.columns}
                df = df.rename(columns=available_map)
                
                if 'volume' in df.columns:
                    df['volume'] = df['volume'].apply(self.parse_volume)
                
                for col in ['close', 'open', 'high', 'low']:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        
                if 'change_pct' in df.columns:
                    df['change_pct'] = df['change_pct'].apply(self.parse_percentage)

                keep_cols = ['_std_date'] + list(available_map.values())
                keep_cols = list(dict.fromkeys(keep_cols))
                final_cols = [c for c in keep_cols if c in df.columns]
                
                df = df[final_cols]
                df.rename(columns={'_std_date': '日期'}, inplace=True)
                
                records = df.to_dict('records')
                print(f"✅ [{name}] 抓取成功! 获得 {len(records)} 条记录")
                return name, records, None 

            except Exception as e:
                last_error = str(e)
                print(f"❌ [{name}] 失败: {str(e)[:100]}")
                if attempt < max_retries:
                    time.sleep(2)
            finally:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
        
        return name, [], last_error

    def fetch_single_source(self, name, url):
        if name == "恒生医疗保健指数":
            return self.fetch_investing_source(name, url)

        max_retries = 5
        days_to_keep = 30 if "南向资金" in name else 180
        last_error = None

        for attempt in range(1, max_retries + 1):
            print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium)...")
            driver = None
            try:
                driver = webdriver.Chrome(options=self.chrome_options)
                
                driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                    "source": """
                        Object.defineProperty(navigator, 'webdriver', {
                            get: () => undefined
                        })
                    """
                })

                driver.set_page_load_timeout(30)
                driver.set_script_timeout(30)
                
                driver.get(url)
                
                try:
                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                except Exception:
                    print(f"⚠️ [{name}] 等待表格超时，尝试继续解析源码...")
                
                html = driver.page_source
                dfs = pd.read_html(StringIO(html))
                
                if not dfs:
                    raise ValueError("页面解析为空，未找到表格数据")

                target_df = None
                
                for df in dfs:
                    df.columns = [str(c).replace(" ", "").replace("\n", "").strip() for c in df.columns]
                    
                    possible_date_cols = ['月份', '时间', '日期', '发布日期', '公布日期']
                    if any(x in str(col) for x in df.columns for col in possible_date_cols):
                        if target_df is None or len(df) > len(target_df):
                            target_df = df
                
                if target_df is None:
                    target_df = max(dfs, key=lambda x: len(x))

                df = target_df
                
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
                    
                    df['_std_date'] = pd.to_datetime(df['_std_date'])
                    
                    cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_to_keep)
                    df = df[df['_std_date'] >= cutoff_date]
                    
                    df['_std_date'] = df['_std_date'].dt.strftime('%Y-%m-%d')
                    df = df.replace({'-': None, 'nan': None})
                    
                    # [修改] 南向资金逻辑已从 targets 移除，这里的清理逻辑仅为防御性保留
                    if name == "中国_南向资金":
                        df = df.where(pd.notnull(df), None)
                        keep_cols = ['_std_date']
                        for c in df.columns:
                            if "净买额" in c and "当日" in c:
                                keep_cols.append(c)
                            elif "成交笔数" in c:
                                keep_cols.append(c)
                        df = df[keep_cols]
                        df.rename(columns={'_std_date': '日期'}, inplace=True)
                    else:
                        df = df.where(pd.notnull(df), None)
                        if '日期' not in df.columns and '_std_date' in df.columns:
                            df['日期'] = df['_std_date']

                    records = df.to_dict('records')
                    print(f"✅ [{name}] 抓取成功! 获得 {len(records)} 条记录")
                    return name, records, None
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
        
        return name, [], last_error

    def run_concurrent(self):
        print("🚀 [Scraper] 正在并发抓取宏观数据 (Workers=2)...")
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
        nested_data = {
            "china": {},
            "usa": {},
            "japan": {},
            "hk": {}
        }
        
        for old_key, data_list in self.results.items():
            if not data_list:
                continue
            if old_key in self.key_mapping:
                country_key, metric_key = self.key_mapping[old_key]
                if country_key not in nested_data:
                    nested_data[country_key] = {}
                nested_data[country_key][metric_key] = data_list
        
        return nested_data

    def get_data_dict(self):
        self.run_concurrent()
        return self.organize_data(), self.status_logs