# selenium_scrapers_investing.py
# -----------------------------------------------------------------------------
# DeepSeek Finance Project - Investing.com Scrapers
# -----------------------------------------------------------------------------

import time
import pandas as pd
import re
from io import StringIO
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import selenium_utils

def fetch_investing_source(name, url, chrome_options, days_to_keep=180):
    """
    通用 Investing.com 历史数据抓取
    支持中文/英文表头，支持页面滚动懒加载
    """
    max_retries = 5
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - Investing专线)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })

            driver.set_page_load_timeout(60)
            driver.set_script_timeout(60)
            driver.get(url)
            
            # [关键] 滚动页面以触发懒加载 (特别是对于 ICE/BDI/SKEW)
            try:
                driver.execute_script("window.scrollBy(0, 500);")
                time.sleep(2)
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except:
                pass
            
            html = driver.page_source
            dfs = pd.read_html(StringIO(html))
            
            if not dfs:
                raise ValueError("页面解析为空，未找到表格数据")

            target_df = None
            
            # 增强表头匹配逻辑
            for df in dfs:
                cols = [str(c).replace(" ", "").replace("\n", "").strip() for c in df.columns]
                # Check for Chinese Headers
                if all(k in cols for k in ['日期', '收盘']):
                    target_df = df
                    break
                # Check for English Headers
                if all(k in cols for k in ['Date', 'Price']):
                    target_df = df
                    break
            
            if target_df is None:
                # Fallback: check only date/close partials
                for df in dfs:
                    cols = [str(c).strip() for c in df.columns]
                    if ('日期' in cols and '收盘' in cols) or ('Date' in cols and 'Price' in cols):
                        target_df = df
                        break

            if target_df is None:
                    raise ValueError(f"未找到符合 Investing 格式的表格")

            df = target_df.copy()
            
            # Standardize Column Names
            rename_map = {
                '日期': '日期', '收盘': 'close', '开盘': 'open',
                '高': 'high', '低': 'low', '交易量': 'volume', '涨跌幅': 'change_pct',
                'Date': '日期', 'Price': 'close', 'Open': 'open',
                'High': 'high', 'Low': 'low', 'Vol.': 'volume', 'Change %': 'change_pct'
            }
            
            actual_cols = {}
            for col in df.columns:
                clean_col = str(col).strip()
                if clean_col in rename_map:
                    actual_cols[col] = rename_map[clean_col]
            
            df = df.rename(columns=actual_cols)
            
            df['_std_date'] = df['日期'].apply(selenium_utils.clean_investing_date)
            df = df.dropna(subset=['_std_date'])
            df['_std_date'] = pd.to_datetime(df['_std_date'])
            
            # [修改] 数据回退机制：如果按日期过滤后为空，但原始数据不为空（说明数据过旧），则强制返回最新 N 条
            df = df.sort_values(by='_std_date', ascending=False)
            
            cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_to_keep)
            filtered_df = df[df['_std_date'] >= cutoff_date]
            
            if filtered_df.empty and not df.empty:
                latest_date_str = df.iloc[0]['_std_date'].strftime('%Y-%m-%d')
                print(f"⚠️ [{name}] 数据过旧 (Latest: {latest_date_str})，超出 {days_to_keep} 天范围。自动回退: 返回最新 5 条。")
                df = df.head(5)
            else:
                df = filtered_df
            
            df['_std_date'] = df['_std_date'].dt.strftime('%Y-%m-%d')
            
            if 'volume' in df.columns:
                df['volume'] = df['volume'].apply(selenium_utils.parse_volume)
            for col in ['close', 'open', 'high', 'low']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            if 'change_pct' in df.columns:
                df['change_pct'] = df['change_pct'].apply(selenium_utils.parse_percentage)

            keep_cols = ['_std_date'] + list(set(rename_map.values()))
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

def fetch_investing_economic_calendar(name, url, chrome_options, days_to_keep=150):
    """
    抓取 Investing.com 财经日历数据
    """
    max_retries = 3
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - Calendar)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })
            driver.set_page_load_timeout(45)
            driver.get(url)
            
            try:
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except:
                pass
            
            html = driver.page_source
            dfs = pd.read_html(StringIO(html))
            
            target_df = None
            for df in dfs:
                cols = [str(c).lower() for c in df.columns]
                if any("release date" in c for c in cols) and any("actual" in c for c in cols):
                    target_df = df
                    break
            
            if target_df is None:
                raise ValueError("未找到财经日历数据表格")
            
            df = target_df.copy()
            new_cols = {}
            for c in df.columns:
                c_str = str(c).strip()
                if "Release Date" in c_str: new_cols[c] = "Release Date"
                elif "Actual" in c_str: new_cols[c] = "Actual"
                elif "Forecast" in c_str: new_cols[c] = "Forecast"
                elif "Previous" in c_str: new_cols[c] = "Previous"
            
            df.rename(columns=new_cols, inplace=True)
            
            def parse_calendar_date(x):
                try:
                    x = re.sub(r'\(.*?\)', '', str(x)).strip()
                    return pd.to_datetime(x, format='%b %d, %Y')
                except:
                    return pd.NaT

            if 'Release Date' not in df.columns:
                raise ValueError("列名识别失败")

            df['std_date'] = df['Release Date'].apply(parse_calendar_date)
            df = df.dropna(subset=['std_date'])
            
            cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_to_keep)
            df = df[df['std_date'] >= cutoff_date]
            
            records = []
            for _, row in df.iterrows():
                records.append({
                    "日期": row['std_date'].strftime('%Y-%m-%d'),
                    "实际值": str(row.get('Actual', '')).strip(),
                    "预测值": str(row.get('Forecast', '')).strip(),
                    "前值": str(row.get('Previous', '')).strip()
                })
            
            print(f"✅ [{name}] 抓取成功! 获得 {len(records)} 条记录 (近 {days_to_keep} 天)")
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

def fetch_fed_rate_monitor(name, url, chrome_options):
    """
    抓取 Investing.com Fed Rate Monitor Tool
    """
    max_retries = 3
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - FedRate)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })
            driver.set_page_load_timeout(45)
            driver.get(url)
            
            try:
                WebDriverWait(driver, 20).until(
                    EC.text_to_be_present_in_element((By.TAG_NAME, "body"), "Fed Interest Rate Decision")
                )
            except:
                pass

            body_text = driver.find_element(By.TAG_NAME, "body").text
            normalized_text = re.sub(r'\s+', ' ', body_text).strip()
            
            # 解析日期
            meeting_date = "Unknown"
            date_match = re.search(r"Meeting Time:\s*([A-Za-z]{3}\s\d{1,2},\s\d{4})", normalized_text)
            if not date_match:
                date_match = re.search(r"Fed Interest Rate Decision\s*([A-Za-z]{3}\s\d{1,2},\s\d{4})", normalized_text)
            if date_match:
                meeting_date = date_match.group(1).strip()
            
            # 解析概率表
            table_pattern = r"(\d+\.\d+\s*-\s*\d+\.\d+)\s+([\d\.]+%)\s+([\d\.]+%)\s+([\d\.]+%)(?:\s|$)"
            matches = re.findall(table_pattern, normalized_text)
            
            if not matches:
                raise ValueError("未匹配到利率概率表数据")

            records = []
            fetch_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            
            for m in matches:
                records.append({
                    "抓取日期": fetch_date,
                    "会议日期": meeting_date,
                    "目标利率区间": m[0],
                    "当前概率": m[1],
                    "前一日概率": m[2],
                    "前一周概率": m[3]
                })
            
            print(f"✅ [{name}] 抓取成功! 会议: {meeting_date}, 获得 {len(records)} 个区间数据")
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