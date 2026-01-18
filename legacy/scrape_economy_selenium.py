# scrape_economy_selenium.py
# -----------------------------------------------------------------------------
# DeepSeek Finance Project - Macro Data Scraper (Interface)
# 核心逻辑已移至 selenium_core.py
# -----------------------------------------------------------------------------

import selenium_core_legacy as selenium_core
import json

def get_macro_data():
    scraper = selenium_core.MacroDataScraper()
    return scraper.get_data_dict()

if __name__ == "__main__":
    scraper = selenium_core.MacroDataScraper()
    data, _ = scraper.get_data_dict()
    try:
        with open("OnlineReport.json", 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"💾 独立运行数据已写入: OnlineReport.json")
    except Exception as e:
        print(f"❌ 写入文件失败: {e}")