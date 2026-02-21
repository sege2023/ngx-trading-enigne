use crate::models::{DailyBar, Ticker, FxRate};
use anyhow::{Context, Result};
use chrono::Utc;
use duckdb::{params, Connection};
use std::path::Path;
use tracing::info;

// ── Schema ────────────────────────────────────────────────────────────────────

const DDL: &str = r#"
CREATE TABLE IF NOT EXISTS tickers (
    symbol      VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL DEFAULT '',
    sector      VARCHAR,
    board       VARCHAR,
    isin        VARCHAR,
    scraped_at  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_bars (
    symbol      VARCHAR  NOT NULL,
    date        DATE     NOT NULL,
    -- Always NULL from kwayisi (reserved for paid feed)
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    -- Always present
    close       DOUBLE   NOT NULL,
    change      DOUBLE,
    change_pct  DOUBLE,
    volume      BIGINT,
    deals       BIGINT,
    scraped_at  TIMESTAMP NOT NULL,
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS fx_rates (
    pair        VARCHAR  NOT NULL,
    date        DATE     NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE   NOT NULL,
    change_pct  DOUBLE,
    source      VARCHAR,
    scraped_at  TIMESTAMP NOT NULL,
    PRIMARY KEY (pair, date)
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id                  INTEGER PRIMARY KEY,
    started_at          TIMESTAMP NOT NULL,
    finished_at         TIMESTAMP,
    status              VARCHAR NOT NULL DEFAULT 'running',
    tickers_processed   INTEGER DEFAULT 0,
    bars_inserted       INTEGER DEFAULT 0,
    error_msg           VARCHAR
);

CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  TIMESTAMP NOT NULL
);
"#;

const INDEXES: &str = r#"
CREATE INDEX IF NOT EXISTS idx_bars_date   ON daily_bars (date);
CREATE INDEX IF NOT EXISTS idx_bars_symbol ON daily_bars (symbol);
CREATE INDEX IF NOT EXISTS idx_fx_date     ON fx_rates (date);
CREATE INDEX IF NOT EXISTS idx_fx_pair     ON fx_rates (pair);
"#;

// ── Repository ────────────────────────────────────────────────────────────────

pub struct Repository {
    conn: Connection,
}

impl Repository {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Could not create dir {:?}", parent))?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open DuckDB at {:?}", path))?;
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> Result<Self> {
        Ok(Self { conn: Connection::open_in_memory()? })
    }

    pub fn run_migrations(&self) -> Result<()> {
        info!("Running migrations…");
        self.conn.execute_batch(DDL).context("DDL failed")?;
        self.conn.execute_batch(INDEXES).context("Index creation failed")?;
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES (1, ?)",
            params![Utc::now().naive_utc()],
        )?;
        info!("Migrations done.");
        Ok(())
    }

    // ── Tickers ───────────────────────────────────────────────────────────────

    pub fn upsert_tickers(&self, tickers: &[Ticker]) -> Result<usize> {
        let tx = self.conn.unchecked_transaction()?;
        for t in tickers {
            tx.execute(
                r#"INSERT INTO tickers (symbol, name, sector, board, isin, scraped_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT (symbol) DO UPDATE SET
                       name = excluded.name,
                       sector = COALESCE(excluded.sector, tickers.sector),
                       board  = COALESCE(excluded.board,  tickers.board),
                       isin   = COALESCE(excluded.isin,   tickers.isin),
                       scraped_at = excluded.scraped_at"#,
                params![t.symbol, t.name, t.sector, t.board, t.isin, t.scraped_at],
            ).with_context(|| format!("upsert ticker {}", t.symbol))?;
        }
        tx.commit()?;
        Ok(tickers.len())
    }

    pub fn list_symbols(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT symbol FROM tickers ORDER BY symbol")?;
        let syms: Vec<String> = stmt
            .query_map([], |r| r.get(0))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(syms)
    }

    // ── Daily bars ────────────────────────────────────────────────────────────

    /// Upsert bars — idempotent, safe to re-run on same data.
    pub fn upsert_daily_bars(&self, bars: &[DailyBar]) -> Result<usize> {
        if bars.is_empty() { return Ok(0); }

        let tx = self.conn.unchecked_transaction()?;
        let sql = r#"
            INSERT INTO daily_bars
                (symbol, date, open, high, low, close, change, change_pct, volume, deals, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, date) DO UPDATE SET
                open        = COALESCE(excluded.open,       daily_bars.open),
                high        = COALESCE(excluded.high,       daily_bars.high),
                low         = COALESCE(excluded.low,        daily_bars.low),
                close       = excluded.close,
                change      = COALESCE(excluded.change,     daily_bars.change),
                change_pct  = COALESCE(excluded.change_pct, daily_bars.change_pct),
                volume      = COALESCE(excluded.volume,     daily_bars.volume),
                deals       = COALESCE(excluded.deals,      daily_bars.deals),
                scraped_at  = excluded.scraped_at
        "#;

        for bar in bars {
            tx.execute(sql, params![
                bar.symbol, bar.date,
                bar.open, bar.high, bar.low,
                bar.close, bar.change, bar.change_pct,
                bar.volume, bar.deals,
                bar.scraped_at,
            ]).with_context(|| format!("insert bar {} {}", bar.symbol, bar.date))?;
        }

        tx.commit()?;
        Ok(bars.len())
    }

    /// Latest date stored for a symbol — used to log scrape coverage.
    pub fn latest_date_for_symbol(&self, symbol: &str) -> Result<Option<chrono::NaiveDate>> {
        let mut stmt = self.conn.prepare(
            "SELECT MAX(date) FROM daily_bars WHERE symbol = ?"
        )?;
        let date: Option<chrono::NaiveDate> = stmt
            .query_row(params![symbol], |r| r.get(0))
            .ok()
            .flatten();
        Ok(date)
    }

    pub fn bar_count(&self) -> Result<i64> {
        let mut s = self.conn.prepare("SELECT COUNT(*) FROM daily_bars")?;
        Ok(s.query_row([], |r| r.get(0))?)
    }

    pub fn ticker_count(&self) -> Result<i64> {
        let mut s = self.conn.prepare("SELECT COUNT(*) FROM tickers")?;
        Ok(s.query_row([], |r| r.get(0))?)
    }

    pub fn date_range(&self) -> Result<(Option<chrono::NaiveDate>, Option<chrono::NaiveDate>)> {
        let mut s = self.conn.prepare("SELECT MIN(date), MAX(date) FROM daily_bars")?;
        Ok(s.query_row([], |r| Ok((r.get(0)?, r.get(1)?)))?)
    }

    // ── FX rates ──────────────────────────────────────────────────────────────

    pub fn upsert_fx_rates(&self, rates: &[FxRate]) -> Result<usize> {
        if rates.is_empty() {
            return Ok(0);
        }

        let tx = self.conn.unchecked_transaction()?;
        let sql = r#"
            INSERT INTO fx_rates
                (pair, date, open, high, low, close, change_pct, source, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (pair, date) DO UPDATE SET
                open       = COALESCE(excluded.open, fx_rates.open),
                high       = COALESCE(excluded.high, fx_rates.high),
                low        = COALESCE(excluded.low, fx_rates.low),
                close      = excluded.close,
                change_pct = COALESCE(excluded.change_pct, fx_rates.change_pct),
                source     = COALESCE(excluded.source, fx_rates.source),
                scraped_at = excluded.scraped_at
        "#;

        for rate in rates {
            tx.execute(
                sql,
                params![
                    rate.pair,
                    rate.date,
                    rate.open,
                    rate.high,
                    rate.low,
                    rate.close,
                    rate.change_pct,
                    rate.source,
                    rate.scraped_at,
                ],
            )
            .with_context(|| format!("insert fx {} {}", rate.pair, rate.date))?;
        }

        tx.commit()?;
        Ok(rates.len())
    }

    pub fn fx_count(&self) -> Result<i64> {
        let mut s = self.conn.prepare("SELECT COUNT(*) FROM fx_rates")?;
        Ok(s.query_row([], |r| r.get(0))?)
    }

    pub fn fx_date_range(&self) -> Result<(Option<chrono::NaiveDate>, Option<chrono::NaiveDate>)> {
        let mut s = self
            .conn
            .prepare("SELECT MIN(date), MAX(date) FROM fx_rates")?;
        Ok(s.query_row([], |r| Ok((r.get(0)?, r.get(1)?)))?)
    }


    // ── Scrape run log ────────────────────────────────────────────────────────

    pub fn begin_scrape_run(&self) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO scrape_runs (started_at, status) VALUES (?, 'running')",
            params![Utc::now().naive_utc()],
        )?;
        let id: i64 = self.conn.query_row(
            "SELECT last_insert_rowid()", [], |r| r.get(0),
        )?;
        Ok(id)
    }

    pub fn finish_scrape_run(
        &self, run_id: i64, tickers: usize, bars: usize, error: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            r#"UPDATE scrape_runs SET
               finished_at = ?, status = ?,
               tickers_processed = ?, bars_inserted = ?, error_msg = ?
               WHERE id = ?"#,
            params![
                Utc::now().naive_utc(),
                if error.is_none() { "success" } else { "error" },
                tickers as i64, bars as i64, error, run_id,
            ],
        )?;
        Ok(())
    }
}