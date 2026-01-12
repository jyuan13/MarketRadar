# selenium_scrapers.py
# -----------------------------------------------------------------------------
# DeepSeek Finance Project - Specific Scraper Implementations
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

def fetch_cnn_fear_greed(name, url, chrome_options):
    """
    专门抓取 CNN Fear & Greed Index
    """
    max_retries = 5
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - CNN)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })

            driver.set_window_size(1920, 1080)
            driver.set_page_load_timeout(45)
            driver.get(url)

            try:
                # 滚动到底部
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3) 
            except:
                pass
            
            try:
                WebDriverWait(driver, 15).until(
                    EC.text_to_be_present_in_element((By.TAG_NAME, "body"), "Timeline")
                )
            except:
                pass 
            
            body_text = driver.find_element(By.TAG_NAME, "body").text
            normalized_text = re.sub(r'\s+', ' ', body_text).strip()
            
            # 1. 当前值
            current_val = None
            match_header = re.search(r"Fear & Greed Index\s+(\d+)", normalized_text, re.IGNORECASE)
            if match_header:
                current_val = int(match_header.group(1))
            else:
                match_timeline = re.search(r"Timeline\s+(\d+)", normalized_text, re.IGNORECASE)
                if match_timeline:
                    current_val = int(match_timeline.group(1))

            # 2. 历史值
            prev_close = 0
            week_ago = 0
            month_ago = 0
            
            m_prev = re.search(r"Previous close\s+(\d+)", normalized_text, re.IGNORECASE)
            if m_prev: prev_close = int(m_prev.group(1))
            
            m_week = re.search(r"1 week ago\s+(\d+)", normalized_text, re.IGNORECASE)
            if m_week: week_ago = int(m_week.group(1))
            
            m_month = re.search(r"1 month ago\s+(\d+)", normalized_text, re.IGNORECASE)
            if m_month: month_ago = int(m_month.group(1))
            
            if current_val is not None:
                record = {
                    "日期": pd.Timestamp.now().strftime('%Y-%m-%d'),
                    "最新值": current_val,
                    "前值": prev_close,
                    "一周前": week_ago,
                    "一月前": month_ago,
                    "description": "CNN Fear & Greed Index"
                }
                print(f"✅ [{name}] 抓取成功! 当前值: {current_val}")
                return name, [record], None
            else:
                raise ValueError("无法解析当前恐惧贪婪指数数值")

        except Exception as e:
            last_error = str(e)
            print(f"❌ [{name}] 失败: {str(e)[:100]}")
            if attempt < max_retries:
                time.sleep(3)
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
                    
    return name, [], last_error

def fetch_cboe_data(name, url, chrome_options):
    """
    抓取 CBOE Options Market Statistics
    """
    max_retries = 3
    last_error = None
    
    target_keys = [
        "TOTAL PUT/CALL RATIO",
        "INDEX PUT/CALL RATIO",
        "EXCHANGE TRADED PRODUCTS PUT/CALL RATIO",
        "EQUITY PUT/CALL RATIO",
        "CBOE VOLATILITY INDEX (VIX) PUT/CALL RATIO",
        "SPX + SPXW PUT/CALL RATIO",
        "OEX PUT/CALL RATIO",
        "MRUT PUT/CALL RATIO",
        "MXEA PUT/CALL RATIO",
        "MXEF PUT/CALL RATIO",
        "MXACW PUT/CALL RATIO",
        "MXWLD PUT/CALL RATIO",
        "MXUSA PUT/CALL RATIO",
        "CBTX PUT/CALL RATIO",
        "MBTX PUT/CALL RATIO",
        "SPEQX PUT/CALL RATIO",
        "SPEQW PUT/CALL RATIO",
        "MGTN PUT/CALL RATIO",
        "MGTNW PUT/CALL RATIO"
    ]

    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - CBOE)...")
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
                    EC.text_to_be_present_in_element((By.TAG_NAME, "body"), "TOTAL PUT/CALL RATIO")
                )
            except:
                pass

            body_text = driver.find_element(By.TAG_NAME, "body").text
            normalized_text = re.sub(r'\s+', ' ', body_text).strip()
            
            records = []
            current_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            
            # 解析日期
            date_match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", normalized_text)
            if date_match:
                try:
                    y, m, d = date_match.groups()
                    current_date = f"{y}-{int(m):02d}-{int(d):02d}"
                except:
                    pass
            
            data_dict = {"日期": current_date}
            
            found_count = 0
            for key in target_keys:
                pattern = re.escape(key) + r"\s+([\d\.]+)"
                match = re.search(pattern, normalized_text)
                if match:
                    data_dict[key] = float(match.group(1))
                    found_count += 1
                else:
                    data_dict[key] = None
            
            if found_count > 0:
                records.append(data_dict)
                print(f"✅ [{name}] 抓取成功! 获得 {found_count} 个指标, 日期: {current_date}")
                return name, records, None
            else:
                raise ValueError("未匹配到任何 Put/Call Ratio 数据")

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

def fetch_ccfi_data(name, url, chrome_options):
    """
    抓取中国出口集装箱运价指数 (CCFI)
    [修复] 
    1. 增加第一行数据检查逻辑，防止表头解析错误
    2. 增加页面滚动和显式等待，确保表格加载
    """
    max_retries = 3
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - CCFI)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })
            driver.set_page_load_timeout(45)
            driver.get(url)
            
            # [新增] 页面交互，确保加载
            try:
                driver.execute_script("window.scrollTo(0, 300);")
                time.sleep(2)
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except:
                print(f"⚠️ [{name}] 等待表格超时，尝试继续解析...")

            html = driver.page_source
            dfs = pd.read_html(StringIO(html))
            
            if not dfs:
                raise ValueError("未找到表格数据")
            
            target_df = None
            
            # 策略：遍历所有表格，寻找包含 "航线" 的表头 OR 第一行
            for df in dfs:
                # 1. 检查 Headers
                header_str = ""
                if isinstance(df.columns, pd.MultiIndex):
                    header_str = " ".join([str(c) for col in df.columns for c in col])
                else:
                    header_str = " ".join([str(c) for c in df.columns])
                
                if "航线" in header_str:
                    target_df = df
                    break
                
                # 2. 检查第一行数据 (若 header 解析失败)
                if not df.empty:
                    first_row_str = " ".join([str(x) for x in df.iloc[0].values])
                    if "航线" in first_row_str:
                        # 将第一行提升为表头
                        new_header = df.iloc[0]
                        df = df[1:]
                        df.columns = new_header
                        target_df = df
                        break
            
            if target_df is None:
                raise ValueError("未找到包含 '航线' 的表格")

            # 提取日期
            prev_date = None
            curr_date = None
            
            # 辅助函数：展平列名
            flat_cols = []
            if isinstance(target_df.columns, pd.MultiIndex):
                for col in target_df.columns:
                    flat_cols.append(" ".join([str(c) for c in col]))
            else:
                flat_cols = [str(c) for c in target_df.columns]

            for col_str in flat_cols:
                if "上期" in col_str:
                    match = re.search(r"(\d{4}-\d{2}-\d{2})", col_str)
                    if match: prev_date = match.group(1)
                if "本期" in col_str:
                    match = re.search(r"(\d{4}-\d{2}-\d{2})", col_str)
                    if match: curr_date = match.group(1)
            
            if not curr_date:
                curr_date = pd.Timestamp.now().strftime('%Y-%m-%d')

            records = []
            for _, row in target_df.iterrows():
                try:
                    # 假设前四列是核心数据: 航线, 上期, 本期, 涨跌
                    if len(row) < 4: continue
                    
                    route_name = str(row.iloc[0]).strip()
                    if "航线" in route_name or route_name == "nan" or route_name == "": continue
                    
                    def clean_val(x):
                        return float(str(x).replace(',', '').replace('nan', '0'))

                    prev_val = clean_val(row.iloc[1])
                    curr_val = clean_val(row.iloc[2])
                    
                    change_str = str(row.iloc[3]).replace('%', '').replace(',', '')
                    change_pct = float(change_str) if change_str != 'nan' else 0.0
                    
                    records.append({
                        "日期": curr_date,
                        "航线": route_name,
                        "本期指数": curr_val,
                        "上期指数": prev_val,
                        "上期日期": prev_date,
                        "涨跌幅(%)": change_pct
                    })
                except:
                    continue 

            if not records:
                raise ValueError("表格解析后未获得有效数据")

            print(f"✅ [{name}] 抓取成功! 日期: {curr_date}, 获得 {len(records)} 条航线数据")
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

def fetch_investing_source(name, url, chrome_options, days_to_keep=180):
    """
    通用 Investing.com 历史数据抓取
    [新增] 页面滚动逻辑，确保懒加载表格被渲染（解决 ICE/BDI 数据缺失问题）
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
            
            cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_to_keep)
            df = df[df['_std_date'] >= cutoff_date]
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

def fetch_gurufocus_insider_ratio(name, url, chrome_options):
    """
    抓取 GuruFocus Insider Buy/Sell Ratio - Historical Data Table
    """
    max_retries = 5
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - GuruFocus)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })
            driver.set_page_load_timeout(60)
            driver.get(url)
            
            try:
                WebDriverWait(driver, 20).until(
                    EC.text_to_be_present_in_element((By.TAG_NAME, "body"), "Historical Data")
                )
            except:
                print(f"⚠️ [{name}] 等待页面关键字 'Historical Data' 超时...")

            html = driver.page_source
            dfs = pd.read_html(StringIO(html))
            
            if not dfs:
                raise ValueError("页面解析为空，未找到表格数据")

            target_df = None
            for df in dfs:
                cols = [str(c).strip() for c in df.columns]
                if "Date" in cols and "Value" in cols and any("YOY" in c for c in cols):
                    target_df = df
                    break
            
            if target_df is None:
                raise ValueError("未找到 'Historical Data' 表格 (需包含 Date/Value/YOY)")

            records = []
            for _, row in target_df.iterrows():
                try:
                    date_str = str(row['Date']).strip()
                    val_str = str(row['Value']).strip()
                    yoy_col = next(c for c in target_df.columns if "YOY" in str(c))
                    yoy_str = str(row[yoy_col]).strip()
                    
                    if not re.match(r"\d{4}-\d{2}-\d{2}", date_str):
                        continue

                    records.append({
                        "日期": date_str,
                        "Value": float(val_str.replace(',', '')),
                        "YOY": yoy_str
                    })
                except:
                    continue
            
            if not records:
                raise ValueError("未提取到有效数据行")

            print(f"✅ [{name}] 抓取成功! 获得 {len(records)} 条记录")
            return name, records, None

        except Exception as e:
            last_error = str(e)
            print(f"❌ [{name}] 失败: {str(e)[:100]}")
            if attempt < max_retries:
                time.sleep(3)
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    return name, [], last_error

def fetch_generic_source(name, url, chrome_options, days_to_keep=180):
    """
    通用数据源抓取 (Eastmoney 等)
    """
    max_retries = 5
    last_error = None

    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(30)
            driver.get(url)
            
            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except Exception:
                pass
            
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
                df['_std_date'] = df[date_col].apply(selenium_utils.clean_date)
                df = df.dropna(subset=['_std_date'])
                df['_std_date'] = pd.to_datetime(df['_std_date'])
                
                cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_to_keep)
                df = df[df['_std_date'] >= cutoff_date]
                
                df['_std_date'] = df['_std_date'].dt.strftime('%Y-%m-%d')
                df = df.replace({'-': None, 'nan': None})
                
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