# MarketRadar - 全球宏观与市场数据雷达

这是一个基于 Python 的自动化市场数据获取与分析工具。它结合了 **AkShare** (针对中国市场) 和 **OpenBB SDK** (针对全球市场) 的强大功能，为您提供每日最新的关键金融数据报告。

## 核心功能

1.  **全球宏观数据 (OpenBB/FRED)**
    *   美元指数、VIX 恐慌指数
    *   美国/日本国债收益率 (10年/2年等)
    *   关键宏观指标: TIPS 实际利率, ON RRP, TGA 余额, 高收益债利差, 金融条件指数等 (New)

2.  **中国市场数据 (AkShare)**
    *   **A股指数**: 上证、深证、创业板、沪深300 (日线/均线)
    *   **科创50**: 实时量比、融资融券、指数估值、60分钟K线
    *   **南向资金**: 每日净流入趋势
    *   **货币与经济**: DR007, M1/M2 剪刀差 (New)

3.  **其他市场**
    *   **港股**: 恒生科技指数 (60分钟K线/日线)
    *   **越南**: 越南胡志明指数 (日线)
    *   **加密货币**: Bitcoin 价格与涨跌幅 (New)
    *   **美股银行**: 六大行股价趋势

4.  **自动化报告**
    *   生成 JSON 格式的结构化数据报告
    *   支持邮件发送日报 (HTML 格式)

## 技术架构更新 (2025)

本项目已完成重构，从原本的 `yfinance` + `selenium` 混合模式迁移至更稳定、专业的 API 组合：

*   **OpenBB Platform (SDK v4)**: 接管所有海外数据（美股、美债、外汇、宏观）。通过统一接口调用 Yahoo Finance, FRED, FMP 等数据源。
*   **AkShare**: 继续作为A股和中国宏观数据的核心数据源。
*   **Selenium**: 仅保留极少数 API 无法覆盖的特殊数据抓取。

## 安装与使用

1.  **安装依赖**
    ```bash
    pip install -r requirements.txt
    ```
    *确保已安装 Python 3.9+ 以支持 OpenBB v4*

2.  **运行程序**
    ```bash
    python main.py
    ```

3.  **产出物**
    *   `MarketRadar_Report.json`: 包含所有指标的完整数据文件
    *   `market_data_status.txt`: 运行日志与状态检查

## 目录结构
*   `fetch_data_core.py`: 数据获取核心逻辑 (OpenBB & AkShare)
*   `main.py`: 主程序流程控制与报告生成
*   `utils.py`: 通用工具函数 (均线计算等)
*   `Ref/`: 接口文档参考

## 近期更新
*   [2026-01-17] 迁移至 OpenBB SDK，移除 yfinance 直接依赖。新增 M1/M2, TIPS, Bitcoin 等监控指标。