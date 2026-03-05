# KDB+ Equity Trading Analytics Platform

> A comprehensive KDB+/Q project demonstrating production-grade tick data architecture, real-time analytics, and trading desk tools used in front office environments.

## Overview

This project simulates the core data infrastructure found on every institutional trading desk. It covers the full lifecycle of market data: ingestion, real-time processing, historical storage, and analytics — using the same architecture deployed at Goldman Sachs, JP Morgan, Morgan Stanley, and every major hedge fund.

## Architecture

```
+-------------------+
|  Market Data Feed |  (Yahoo Finance historical / simulated real-time)
+--------+----------+
         |
         v
+-------------------+     +------------------+
|   TICKERPLANT     | --> |  TP LOG FILE     |
|   (tick.q)        |     |  (recovery/audit)|
+--------+----------+     +------------------+
         |
    +----+----+
    |         |
    v         v
+------+  +----------+
|  RDB |  | ALERTING |  (alerts.q - volume spikes, price moves)
+------+  +----------+
    |
    | (end-of-day write-down)
    v
+------+
|  HDB |  (date-partitioned on disk)
+------+
    |
    +---> analytics.q   (VWAP, volatility, moving averages)
    +---> tca.q         (Transaction Cost Analysis)
    +---> risk.q        (VaR, Greeks, exposure)
    +---> reports.q     (PM/desk-level reporting)
```

## Concepts Covered

| # | Concept | File(s) | Description |
|---|---------|---------|-------------|
| 1 | **Q Language Fundamentals** | `src/schema.q`, `src/utils.q` | Atoms, lists, dictionaries, tables, keyed tables, data types |
| 2 | **qSQL Queries** | `src/analytics.q`, `src/reports.q` | select/where/by, aggregations, functional select, exec |
| 3 | **Asof Join (aj)** | `src/tca.q` | Trade-to-quote matching for execution quality analysis |
| 4 | **Window Join (wj)** | `src/tca.q` | Aggregate quotes within time windows around trades |
| 5 | **Left/Inner/Union Joins** | `src/analytics.q`, `src/risk.q` | Reference data enrichment, cross-table analysis |
| 6 | **Tick Architecture** | `src/tick.q`, `src/rdb.q` | Tickerplant, RDB, pub/sub, log files |
| 7 | **HDB (Historical DB)** | `src/hdb_setup.q` | Date-partitioned on-disk database |
| 8 | **Sym File & Enumeration** | `src/hdb_setup.q` | .Q.en, symbol enumeration, on-disk format |
| 9 | **Splayed Tables** | `src/hdb_setup.q` | Column-as-file storage, .d file, memory mapping |
| 10 | **Time-Series Analytics** | `src/analytics.q` | VWAP, TWAP, rolling volatility, moving averages, xbar bucketing |
| 11 | **Anomaly Detection** | `src/alerts.q` | Volume spikes, price gaps, volatility breakouts |
| 12 | **Transaction Cost Analysis** | `src/tca.q` | Spread analysis, slippage, market impact, implementation shortfall |
| 13 | **Risk Analytics** | `src/risk.q` | VaR (Historical & Parametric), sector exposure, correlation matrix |
| 14 | **Functions & Lambdas** | All files | Custom functions, projections, composition, error handling |
| 15 | **IPC (Inter-Process Comm)** | `src/tick.q`, `src/rdb.q` | Handle connections, publish/subscribe, async messaging |

## Project Structure

```
kdb-trading-analytics/
├── README.md                    # This file
├── .gitignore                   # Ignore data/db directories
├── scripts/
│   └── download_data.py         # Download historical equity data (Yahoo Finance)
├── src/
│   ├── schema.q                 # Table schemas (trades, quotes, orders, reference)
│   ├── utils.q                  # Utility functions, logging, timer helpers
│   ├── load_data.q              # CSV loader → partitioned HDB builder
│   ├── hdb_setup.q              # HDB creation, enumeration, partition management
│   ├── tick.q                   # Tickerplant simulator (pub/sub, logging)
│   ├── rdb.q                    # Real-time database (subscribe, in-memory, EOD writedown)
│   ├── analytics.q              # Core analytics: VWAP, TWAP, volatility, moving averages
│   ├── tca.q                    # Transaction Cost Analysis: aj, spread, slippage, impact
│   ├── risk.q                   # Risk: VaR, correlation, sector exposure, PnL
│   ├── alerts.q                 # Alerting engine: volume/price/volatility anomalies
│   └── reports.q                # Business reports: PM dashboard, desk summary, rankings
├── tests/
│   └── test_analytics.q         # Validation queries and sanity checks
└── docs/
    └── interview_guide.md       # Quick reference for interview prep
```

## Getting Started

### Prerequisites
- **KDB+ Personal Edition** (free): Download from [kx.com](https://kx.com/kdb-personal-edition-download/)
- **Python 3.8+** with `yfinance` and `pandas`: `pip install yfinance pandas`

### Step 1: Download Market Data
```bash
cd scripts
python download_data.py
```
This downloads 3 years of daily OHLCV data for 20 equities across 6 sectors, plus generates simulated intraday tick data.

### Step 2: Build the Historical Database
```bash
cd ..
q src/load_data.q
```
This loads CSVs, enumerates symbols, and creates a date-partitioned HDB.

### Step 3: Run Analytics
```bash
q
\l src/analytics.q
\l src/tca.q
\l src/risk.q
\l src/alerts.q
\l src/reports.q
```

### Step 4: Explore
```q
/ See what tables exist
tables[]

/ Run VWAP
calcVWAP[`AAPL; 2024.01.01; 2024.12.31]

/ Transaction Cost Analysis
runTCA[`AAPL; 2024.06.01; 2024.06.30]

/ Generate alerts
generateAlerts[]

/ PM dashboard
pmDashboard[`AAPL`MSFT`GOOG; 2024.01.01; 2024.12.31]
```

## Key Queries to Practice

```q
/ === BASIC qSQL ===
select from trades where sym=`AAPL
select avg price, sum size by sym from trades
select vwap:(sum price*size) % sum size by sym, date from trades

/ === TIME BUCKETING ===
select high:max price, low:min price, open:first price, close:last price
    by 5 xbar time.minute, sym from trades

/ === ASOF JOIN (trade-to-quote matching) ===
aj[`sym`time; trades; quotes]

/ === WINDOW JOIN (avg bid in 5s window before each trade) ===
wj[-00:00:05 00:00:00+\:trades`time; `sym`time#trades; (quotes;(avg;`bid);(avg;`ask))]

/ === MOVING AVERAGES ===
update ma20:mavg[20;close], ma50:mavg[50;close] by sym from dailyOHLC

/ === VOLATILITY ===
select sym, vol20:dev ret, annVol:(dev ret)*sqrt 252.0 by sym from returns
```

## What This Demonstrates to Interviewers

1. **You understand tick architecture** — not just queries, but how data flows from exchange to database
2. **You can write production-quality Q** — functions with parameters, error handling, logging
3. **You know the joins** — aj for TCA, lj for reference enrichment, wj for windowed analytics
4. **You understand storage** — partitioning, enumeration, splayed tables, sym files
5. **You can translate business questions to code** — VWAP, TCA, risk, alerts are all real trading desk tools
6. **You built something end-to-end** — not just isolated queries, but a complete system

## Technologies

- **KDB+ 4.0** — Column-oriented time-series database
- **Q** — KDB+ query and programming language
- **Python** — Data acquisition (yfinance)
- **Git** — Version control

## License

MIT — Free to use for learning and portfolio purposes.

## Author

[Your Name] — Aspiring Business Analyst | Trading Technology

---

*Built as a portfolio project demonstrating KDB+/Q proficiency for front office trading roles.*
