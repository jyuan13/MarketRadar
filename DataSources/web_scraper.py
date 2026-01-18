# -*- coding:utf-8 -*-
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
try:
    from config.settings import URLS, USER_AGENT
except ImportError:
    URLS = {}
    USER_AGENT = "Mozilla/5.0"

from . import selenium_scrapers_investing
from . import selenium_scrapers_misc

class WebScraper:
    """
    Selenium/Requests Web Scraper
    Fetches data from Investing.com and other sites requiring JS rendering.
    """
    
    def _get_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument(f"user-agent={USER_AGENT}")
        # Add more options for stability
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        return webdriver.Chrome(options=chrome_options)

    def fetch_investing_table(self, url, table_id=None, look_for_cols=None):
        """
        Generic fetcher for Investing.com historical data tables.
        """
        driver = self._get_driver()
        try:
            driver.get(url)
            # Wait for table
            wait = WebDriverWait(driver, 15)
            if table_id:
                wait.until(EC.presence_of_element_located((By.ID, table_id)))
            else:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            
            html = driver.page_source
            dfs = pd.read_html(html)
            
            target_df = None
            for df in dfs:
                if look_for_cols:
                    # Check if columns match generic keywords
                    cols = [str(c).lower() for c in df.columns]
                    if any(x in cols for x in look_for_cols):
                        target_df = df
                        break
            
            if target_df is None and dfs:
                target_df = dfs[0] # Default to first if specific not found
                
            return target_df, None
            
        except Exception as e:
            return None, str(e)
        finally:
            driver.quit()

    def fetch_korea_exports(self):
        """
        Fetch South Korea Exports YoY
        URL: https://www.investing.com/economic-calendar/south-korean-export-growth-1316
        """
        url = URLS.get("korea_exports")
        if not url: return [], "URL not configured"
        
        # This page usually has a "History" tab or similar. scraping economic-calendar pages on Investing.com
        # usually requires finding the table with ID 'eventHistoryTable1316' (ID suffix matches event ID)
        
        event_id = url.split("-")[-1]
        table_id = f"eventHistoryTable{event_id}"
        
        df, err = self.fetch_investing_table(url, table_id=table_id)
        if df is None: return [], err
        
        # Process DF
        # Cols: Release Date, Time, Actual, Forecast, Previous
        # We need Release Date (or ref period?) and Actual.
        # Note: Economic Calendar usually shows "Release Date" but the data is for the previous month.
        # We'll just capture Release Date and Actual for now.
        
        data = []
        try:
            for _, row in df.iterrows():
                # Clean Actual
                act = str(row.get('Actual', '')).replace('%', '').strip()
                prev = str(row.get('Previous', '')).replace('%', '').strip()
                
                # Date parsing
                # Investing calendar dates are like "Jan 01, 2024" or "Jan 01, 2024 (Dec)"
                # We need to parse carefully.
                
                date_str = str(row.get('Release Date', ''))
                # Remove (Month) suffix if present
                if '(' in date_str:
                    date_str = date_str.split('(')[0].strip()
                
                try:
                    dt = datetime.datetime.strptime(date_str, "%b %d, %Y")
                    date_fmt = dt.strftime("%Y-%m-%d")
                except:
                    date_fmt = date_str
                
                if act:
                    data.append({
                        "date": date_fmt,
                        "value": act,
                        "previous": prev
                    })
            return data, None
        except Exception as e:
            return [], str(e)

    def fetch_vn_fdi(self):
         """
         Fetch Vietnam FDI
         """
         url = URLS.get("vn_fdi")
         if not url: return [], "URL not configured"
         event_id = url.split("-")[-1]
         table_id = f"eventHistoryTable{event_id}"
         
         df, err = self.fetch_investing_table(url, table_id=table_id)
         if df is None: return [], err
         
         data = []
         try:
            for _, row in df.iterrows():
                val = str(row.get('Actual', '')).replace('B', '').strip() # Billion
                data.append({
                    "date": str(row.get('Release Date', '')),
                    "value": val
                })
            return data, None
         except Exception as e:
            return [], str(e)

import datetime
