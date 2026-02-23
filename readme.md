# Dynamic FX-Conditional Beta Rotation Strategy on Nigerian Equities 

## Overview

This project builds a full quantitative research pipeline to study the impact of currency depreciation on equity beta dynamics in a frontier market.

Specifically, it tests whether Nigerian stock betas change during Naira depreciation regimes and whether these changes can be exploited via a long-only beta rotation strategy.


## Architecture

## Project structure

```
ngx-trading_engine/
├── data/                        # Put investing.com CSVs here (git-ignored)
│   ├── DANGCEM_historical.csv
│   └── GTCO_historical.csv
├── src/
│   ├── main.rs                  # CLI: load-csv | update | stats
│   ├── config/                  # AppConfig (TOML + env)
│   ├── models/                  # Ticker, DailyBar, RawCsvRow
│   ├── loader/                  # CSV parser for investing.com
│   ├── scraper/                 # Web scraper + cleaner (for updates)
│   ├── storage/                 # DuckDB repo (upserts, queries)
│   ├── pipeline/                # Orchestrator
│   └── utils/                   # Timer, fmt_number
└── config/
    └── default.toml
└── research/
    └── research.md
```

---
## 1. Data ingestion(Rust)
- parses historical EOD equities data
- parses USD/NGN FX rates
- stores data in duckdb

## Data source
## [investing.com](https://www.investing.com)


## 2. CLI reference

```bash
# Bulk-import all CSVs from data/ folder
cargo run --release -- load-csv

# Import from custom directory
cargo run --release -- load-csv --dir /path/to/csvs

# Daily incremental update (scraper/API mode)
# cargo run --release -- update (will be released later)

# Show DB stats (row counts, date range)
cargo run --release -- stats

# List all ticker symbols in DB
cargo run --release -- symbols

# Apply schema migrations only
cargo run --release -- migrate

# Verbose logging
cargo run --release -- -v load-csv
```

---


## 3. Strategy logic: research phase logic deifned, backtesting in progress
- Compute stock log return of equities(for both NGN and USD-adjusted)

```sql
-- Stock log return for NGN
SELECT 
    symbol,
    date,
    close,
    LN(close / LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)) AS log_return
FROM daily_bars
WHERE symbol = 'DANGCEM'
ORDER BY date DESC
LIMIT 20;
```
- compute fx log return
```sql
SELECT
    pair,
    date,
    close,
    LN(close / LAG(close, 1) OVER (PARTITION BY pair ORDER BY date)) AS log_return
FROM fx_rates
WHERE pair = 'USDNGN'
ORDER BY date;
```
we woud also compute log return for NGX 30 market index

## Compute beta for market index and fx

## Estimate rolling 90-day regression:
    r_i = α + β_m r_m + β_fx r_fx

-  Identify stocks with highest positive β_fx

-  Long top N stocks

-  Rebalance monthly

## 4. Backtesting
- Walk-forward validation
- No lookahead bias
- Rolling window recalibration
- Out-of-sample regime testing
- Performance metrics:
    - Annualized return
    - Volatility
    - Sharpe ratio
    - Max drawdown