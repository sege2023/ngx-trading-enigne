use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};

// ── Ticker ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Ticker {
    pub symbol: String,
    pub name: String,
    pub sector: Option<String>,
    pub industry: Option<String>,
    pub exchange: Option<String>,  // Lagos, Abuja
    pub scraped_at: NaiveDateTime,
}

// ── Equity daily bar ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DailyBar {
    pub symbol: String,
    pub date: NaiveDate,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: f64,
    pub change_pct: Option<f64>,
    pub volume: Option<i64>,
    pub scraped_at: NaiveDateTime,
}

// ── FX rate ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FxRate {
    pub pair: String,      // "USDNGN", "EURNGN", etc.
    pub date: NaiveDate,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: f64,        // settlement/EOD rate
    pub change_pct: Option<f64>,
    pub source: Option<String>,  // "investing.com", "cbn", etc.
    pub scraped_at: NaiveDateTime,
}

// ── Raw CSV rows ──────────────────────────────────────────────────────────────

/// investing.com equity CSV: Date, Price, Open, High, Low, Volume, Change%
#[derive(Debug, Clone, Default)]
pub struct RawCsvRow {
    pub date: Option<String>,
    pub price: Option<String>,      // close
    pub open: Option<String>,
    pub high: Option<String>,
    pub low: Option<String>,
    pub volume: Option<String>,
    pub change_pct: Option<String>,
}

/// investing.com FX CSV: Date, Price, Open, High, Low, Change%
/// (no volume — forex is OTC)
#[derive(Debug, Clone, Default)]
pub struct RawFxCsvRow {
    pub date: Option<String>,
    pub price: Option<String>,      
    pub open: Option<String>,
    pub high: Option<String>,
    pub low: Option<String>,
    pub change_pct: Option<String>,
}

/// Ticker metadata CSV: symbol, name, sector, industry, exchange
#[derive(Debug, Clone, Default)]
pub struct RawTickerRow {
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub sector: Option<String>,
    pub industry: Option<String>,
    pub exchange: Option<String>,
}

// ── Legacy raw types (scraper compatibility) ──────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct RawEquityRow {
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub price: Option<String>,
    pub change: Option<String>,
    pub change_pct: Option<String>,
    pub volume: Option<String>,
    pub deals: Option<String>,
    pub sector: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RawHistoricalRow {
    pub date: Option<String>,
    pub open: Option<String>,
    pub high: Option<String>,
    pub low: Option<String>,
    pub close: Option<String>,
    pub change: Option<String>,
    pub volume: Option<String>,
    pub deals: Option<String>,
}