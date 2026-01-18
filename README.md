# MarketRadar Refactored - 全球金融市场数据聚合雷达

MarketRadar 是一个自动化的金融数据聚合工具，旨在为投资者提供全方位的市场快照。它现在采用分层架构，集成了 **OpenBB**, **Akshare** 和 **Web Scraping** 技术，提供更稳健的数据获取和更丰富的宏观指标。

## 🎯 核心功能

### 1. 全球市场 K线与技术分析
自动抓取全球主要投资标的的 **K线数据（OHLCV）**，并计算 **5日、10日、20日、60日、120日、250日（年线）** 移动平均线，以及 **MACD, KDJ, RSI** 技术指标。

* **全球指数**: 纳斯达克, 标普500, 恒生指数, 恒生科技, 越南胡志明指数。
* **大宗商品**: 黄金, 白银, 铜, 上海金, 原油, 铀。
* **核心个股**: 美股七巨头, 港股科技 Top 20, 港股创新药, 新兴市场龙头。

### 2. 宏观经济指标 (Macro Data)
* **OpenBB / FRED**: 
    * TIPS 收益率, 期限溢价 (Term Premium)
    * TGA 账户余额, 逆回购 (ON RRP)
    * 全球流动性代理 (Global Liquidity)
    * 信用利差 (High Yield OAS)
* **Investing.com 爬虫**: 
    * 韩国出口同比增长
    * 越南外商直接投资 (FDI)
* **Akshare**: 
    * 南向资金流向
    * A股主要指数

### 3. 系统架构 (Refactored)

项目已重构为分层架构，以提高可维护性和扩展性：

* **`config/`**: 配置中心，管理 API Key (FRED) 和全局参数。
* **`DataSources/`**: 数据源适配器层。
    * `openbb_source.py`: OpenBB 接口封装 (美股, 美债, FRED)。
    * `akshare_source.py`: Akshare 接口封装 (A股, 港股通)。
    * `web_scraper.py`: Selenium/Requests 爬虫 (Investing.com)。
* **`Collectors/`**: 数据采集层。
    * `market_collector.py`: 核心采集器，并发调度各个数据源。
* **`Processors/`**: 数据处理层。
    * `data_processor.py`: 数据清洗、均线计算。
    * `technical_analysis.py`: 技术指标计算 (MyTT)。
* **`Formatters/`**: 格式化层。
    * `json_formatter.py`: JSON 报告生成。
* **`MessageBus/`**: 消息总线。
    * `email_service.py`: 邮件发送服务。
* **`main.py`**: 程序入口。

## 🚀 快速开始

1. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```
   *注意：OpenBB 包较大，建议在虚拟环境中安装。*

2. **配置环境**
   请设置以下环境变量 (或修改 `config/settings.py`)：
   * `FRED_API_KEY`: FRED 数据接口 Key (必须)。
   * `SENDER_EMAIL`: 发件人邮箱 (QQ邮箱)。
   * `SENDER_PASSWORD`: 邮箱授权码。
   * `RECEIVER_EMAIL`: 收件人邮箱。

3. **运行**
   ```bash
   python main.py
   ```

4. **产出**
   * `MarketRadar_Report.json`: 包含所有数据的结构化报告。
   * `market_data_status.txt`: 运行状态日志。