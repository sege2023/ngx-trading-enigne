# Dynamic FX-Conditional Beta Rotation Strategy on Nigerian Equities 

## Overview

This project builds a full quantitative research pipeline to study the impact of currency depreciation on equity beta dynamics in a frontier market.

Specifically, it tests whether Nigerian stock betas change during Naira depreciation regimes and whether these changes can be exploited via a long-only beta rotation strategy.

## Architecture
1. ## Data ingestion(Rust)

## Data source
## [investing.com](https://www.investing.com)
## CLI reference

```bash
# Bulk-import all CSVs from data/ folder
cargo run --release -- load-csv

# Import from custom directory
cargo run --release -- load-csv --dir /path/to/csvs

# Daily incremental update (scraper/API mode)
cargo run --release -- update

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

---

## Project structure

```
ngx-etl/
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

