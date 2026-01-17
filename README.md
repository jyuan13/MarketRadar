# MarketRadar - 全球宏观与金融市场数据雷达

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
[![OpenBB](https://img.shields.io/badge/OpenBB-SDK%20v4-orange)](https://openbb.co/)
[![AkShare](https://img.shields.io/badge/AkShare-Latest-red)](https://akshare.xyz/)

**MarketRadar** 是一个模块化的金融数据采集与分析系统，旨在帮助投资者快速获取全球宏观经济、股市、债市、大宗商品及加密货币的核心数据，并生成整合后的每日简报。

本项目已采用**分层架构 (Layered Architecture)** 重构，支持更灵活的扩展与维护。

---

## 🚀 核心功能

1.  **多源数据采集**:
    *   **OpenBB**: 负责美股 (七巨头/银行)、外汇、美债收益率、比特币、全球大宗商品 (黄金/铜/原油) 等。
    *   **AkShare**: 负责 A 股指数 (上证/创业板/科创50)、ETF (科创50/恒生科技)、中国国债、南向资金、中国宏观 (M1/M2) 等。
    *   **FRED (美联储)**: 负责美国深层宏观数据 (TIPS 实际利率, TGA 账户, ON RRP)。
    *   **Selenium 爬虫**: 负责补充 Investing.com 和 Eastmoney 上的特定经济数据 (如韩国出口、越南 FDI、CPI、PMI)。

2.  **分层架构**:
    *   严格分离配置、数据源、采集逻辑与数据处理，便于维护。

3.  **每日报告**:
    *   输出标准 JSON 格式全量报告。
    *   实时监控数据获取状态，支持日志追踪。

---

## 📂 项目结构

```text
MarketRadar/
├── config/                 # 配置层
│   └── settings.py         # 核心配置文件 (目标列表, API配置, 时间范围)
│
├── src/                    # 源码层
│   ├── data_sources/       # 数据源层 (API/爬虫封装)
│   │   ├── providers.py    # OpenBB, AkShare, FRED 包装类
│   │   └── selenium_*.py   # Selenium 爬虫脚本
│   ├── collectors/         # 采集层 (业务调度)
│   │   └── manager.py      # 采集总控
│   ├── processors/         # 处理层 (清洗/计算)
│   │   └── core.py         # 均线计算, 数据标准化
│   ├── formatters/         # 格式化层 (输出)
│   │   └── json_fmt.py     # JSON 组装
│   └── utils/              # 工具库 (日志/算法)
│
├── legacy/                 # 旧版代码归档 (参考用)
├── main.py                 # 程序唯一入口
├── requirements.txt        # 依赖列表
└── README.md               # 本文档
```

---

## 🛠️ 安装与使用

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置说明

*   **API Keys**:
    *   `FRED_API_KEY`: 若需获取美联储 FRED 数据，请在环境变量中设置。
*   **邮箱通知**:
    *   在环境变量中配置以下字段以启用邮件推送 (默认使用QQ邮箱SMTP/465端口)：
        *   `SENDER_EMAIL`: 发件人邮箱
        *   `SENDER_PASSWORD`: 邮箱授权码
        *   `RECEIVER_EMAIL`: 收件人邮箱
*   **采集目标**:
    *   所有监控的股票、指数、宏观指标均在 `config/settings.py` 中定义。如需增加新的监控标的，直接修改该文件即可，无需改动核心代码。

### 3. 运行

```bash
python main.py
```

程序运行结束后，会生成最新的 JSON 数据报告。

---

## 📊 支持指标概览

| 类别 | 包含内容 | 数据源 |
| :--- | :--- | :--- |
| **全球指数** | 纳斯达克, 标普500, 恒生科技, 越南VN30, A股主要指数 | OpenBB / AkShare |
| **宏观经济** | 中国 M1/M2/社融/CPI, 美国非农/CPI/PMI, 韩国出口 | AkShare / Selenium |
| **美债/货币** | 10年期美债, TIPS实际利率, TGA账户, 逆回购ON RRP | OpenBB / FRED |
| **大宗商品** | 黄金, 白银, 铜, 原油, 铀 | OpenBB / AkShare |
| **加密货币** | 比特币 (BTC-USD) | OpenBB |
| **行业数据** | 港股创新药, 科创50融资融券/量比, 半导体ETF | AkShare |

---

## 📝 开发者指南

*   **新增数据源**: 在 `src/data_sources` 中继承 `BaseSource` 实现新类。
*   **修改清洗规则**: 在 `src/processors/core.py` 中调整逻辑。
*   **旧代码**: `legacy/` 目录下保留了重构前的原始脚本，可做逻辑对照参考。